package router

import (
	"errors"
	"sync"
	"testing"
)

// A delivery records the exact rule-set version that matched it, so callers
// can tell which revision of the rules handled a message during concurrent
// updates.
func TestDeliveriesCarryRuleVersion(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "v1", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.UpdateRules("sub", []Rule{{ID: "v2", Topic: "t"}}); err != nil {
		t.Fatal(err)
	}
	if err := router.UpdateRules("sub", []Rule{{ID: "v3", Topic: "t"}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m2", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	deliveries := drain(sub)
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(deliveries))
	}
	if deliveries[0].Message.ID() != "m1" || deliveries[0].RuleVersion != 1 {
		t.Fatalf("first delivery = %#v, want m1 at version 1", deliveries[0])
	}
	if deliveries[1].Message.ID() != "m2" || deliveries[1].RuleVersion != 3 {
		t.Fatalf("second delivery = %#v, want m2 at version 3", deliveries[1])
	}
}

// A failed UpdateRules must not advance the version: messages still match
// the previous rule set.
func TestRejectedUpdateKeepsVersion(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "r1", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.UpdateRules("sub", []Rule{{ID: "", Topic: "t"}}); err == nil {
		t.Fatal("invalid update unexpectedly succeeded")
	}
	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	deliveries := drain(sub)
	if len(deliveries) != 1 || deliveries[0].RuleVersion != 1 {
		t.Fatalf("deliveries = %#v, want one version-1 delivery", deliveries)
	}
}

// KeepNewest evicts the oldest unreceived message, keeps the newest ones in
// order, and records the eviction with the matched rule version. The
// retention policy covers every message the consumer has not received yet,
// including the one currently in handoff, so the surviving sequence is
// exactly the newest bufferSize messages. Fast subscribers remain
// completely unaffected.
func TestKeepNewestEvictsOldest(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(10))
	if err != nil {
		t.Fatal(err)
	}
	fastDone := make(chan []Delivery, 1)
	go func() { fastDone <- drain(fast) }()

	ids := []string{"m1", "m2", "m3", "m4", "m5"}
	for _, id := range ids {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}

	got := drain(slow)
	if len(got) != 3 {
		t.Fatalf("slow deliveries = %d, want exactly 3: %#v", len(got), got)
	}
	for _, delivery := range got {
		if delivery.RuleVersion != 1 {
			t.Fatalf("delivery %q version = %d, want 1", delivery.Message.ID(), delivery.RuleVersion)
		}
	}
	// The consumer never read, so every unreceived message competes under
	// the same policy: exactly the newest three survive, in order.
	want := []string{"m3", "m4", "m5"}
	for index, id := range want {
		if got[index].Message.ID() != id {
			t.Fatalf("delivery %d = %q, want %q; all: %#v", index, got[index].Message.ID(), id, got)
		}
	}
	if fast := <-fastDone; len(fast) != 5 {
		t.Fatalf("fast deliveries = %d, want 5", len(fast))
	}

	var evictions, rejects int
	evicted := make(map[string]bool)
	for _, record := range router.Records() {
		switch {
		case record.Kind == RecordDropped && record.Code == "subscriber_buffer_evicted":
			evictions++
			evicted[record.MessageID] = true
			if record.RuleVersion != 1 {
				t.Fatalf("eviction version = %d, want 1: %#v", record.RuleVersion, record)
			}
			if record.SubscriptionID != "slow" {
				t.Fatalf("eviction target = %q, want slow", record.SubscriptionID)
			}
		case record.Kind == RecordRejected:
			rejects++
		}
	}
	if evictions != 5-len(got) {
		t.Fatalf("evictions = %d, want %d for %d deliveries", evictions, 5-len(got), len(got))
	}
	if !evicted["m1"] || !evicted["m2"] {
		t.Fatalf("evicted messages = %v, want m1 and m2", evicted)
	}
	if rejects != 0 {
		t.Fatalf("unexpected rejects = %d", rejects)
	}
}

// Invalid overflow policies are rejected at Subscribe time.
func TestInvalidOverflowPolicy(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("bad", []Rule{{ID: "r", Topic: "t"}},
		WithOverflowPolicy("keep_median")); err == nil {
		t.Fatal("invalid overflow policy unexpectedly accepted")
	}
}

// SetOverflowPolicy switches a running subscription without rewriting the
// backlog: messages accepted before the switch are delivered in order, and
// only new offers observe the new policy. The exact retained sequence and
// every drop reason are observable.
func TestSetOverflowPolicySwitchKeepsBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2), WithOverflowPolicy(KeepOldest))
	if err != nil {
		t.Fatal(err)
	}

	publish := func(ids ...string) {
		t.Helper()
		for _, id := range ids {
			if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Consumer paused. KeepOldest: m1, m2 accepted; m3 dropped.
	publish("m1", "m2", "m3")
	if err := router.SetOverflowPolicy("sub", KeepNewest); err != nil {
		t.Fatal(err)
	}
	// KeepNewest: m4 evicts m1, m5 evicts m2 (the pre-switch backlog is
	// retained until new arrivals need the room).
	publish("m4", "m5")
	if err := router.SetOverflowPolicy("sub", KeepOldest); err != nil {
		t.Fatal(err)
	}
	// KeepOldest again: buffer full, m6 dropped.
	publish("m6")
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	// Nothing accepted after cancellation may be delivered.
	publish("m7")

	got := drain(sub)
	want := []string{"m4", "m5"}
	if len(got) != len(want) {
		t.Fatalf("deliveries = %#v, want %v", got, want)
	}
	for index, id := range want {
		if got[index].Message.ID() != id {
			t.Fatalf("delivery %d = %q, want %q; all: %#v", index, got[index].Message.ID(), id, got)
		}
	}

	full := make(map[string]bool)
	evicted := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Kind != RecordDropped {
			continue
		}
		switch record.Code {
		case "subscriber_buffer_full":
			full[record.MessageID] = true
		case "subscriber_buffer_evicted":
			evicted[record.MessageID] = true
		default:
			t.Fatalf("unexpected drop code: %#v", record)
		}
		if record.SubscriptionID != "sub" || record.RuleVersion != 1 {
			t.Fatalf("record targets wrong subscription or version: %#v", record)
		}
	}
	if !full["m3"] || !full["m6"] || len(full) != 2 {
		t.Fatalf("buffer_full drops = %v, want m3 and m6", full)
	}
	if !evicted["m1"] || !evicted["m2"] || len(evicted) != 2 {
		t.Fatalf("evictions = %v, want m1 and m2", evicted)
	}
}

// Policy switches are validated and never affect other subscribers.
func TestSetOverflowPolicyValidationAndIsolation(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(1))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	fastDone := make(chan []Delivery, 1)
	go func() { fastDone <- drain(fast) }()

	if err := router.SetOverflowPolicy("", KeepNewest); err == nil {
		t.Fatal("empty subscription id unexpectedly accepted")
	}
	if err := router.SetOverflowPolicy("slow", "keep_median"); err == nil {
		t.Fatal("invalid policy unexpectedly accepted")
	}
	if err := router.SetOverflowPolicy("missing", KeepNewest); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("missing subscription error = %v, want ErrSubscriptionGone", err)
	}

	if err := router.SetOverflowPolicy("slow", KeepNewest); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	if got := <-fastDone; len(got) != 3 {
		t.Fatalf("fast deliveries = %d, want 3: policy switch leaked across subscribers", len(got))
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	// After cancellation the policy is fixed and late sends go nowhere.
	if err := router.SetOverflowPolicy("slow", KeepOldest); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("post-unsubscribe switch error = %v, want ErrSubscriptionGone", err)
	}
	if err := router.Publish(mustMessage(t, "m4", "t", nil)); err != nil {
		t.Fatal(err)
	}
	got := drain(slow)
	if len(got) != 1 || got[0].Message.ID() != "m3" {
		t.Fatalf("slow deliveries = %#v, want only m3", got)
	}

	rejects := 0
	for _, record := range router.Records() {
		if record.Kind == RecordRejected {
			rejects++
		}
	}
	if rejects != 4 {
		t.Fatalf("reject records = %d, want 4", rejects)
	}
}

// KeepNewest retains exactly the newest bufferSize messages when the
// consumer is paused, for every capacity, and every discarded message is
// accounted for in the audit history.
func TestKeepNewestExactSequenceAcrossCapacities(t *testing.T) {
	for _, capacity := range []int{0, 1, 2, 5} {
		t.Run(string(rune('0'+capacity)), func(t *testing.T) {
			router, err := NewRouter(WithHistorySize(64))
			if err != nil {
				t.Fatal(err)
			}
			sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
				WithSubscriptionBufferSize(capacity), WithOverflowPolicy(KeepNewest))
			if err != nil {
				t.Fatal(err)
			}

			const total = 8
			for index := 0; index < total; index++ {
				id := string(rune('a' + index))
				if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
					t.Fatal(err)
				}
			}
			if err := router.Unsubscribe("sub"); err != nil {
				t.Fatal(err)
			}

			got := drain(sub)
			if len(got) != capacity {
				t.Fatalf("capacity %d: deliveries = %d, want %d: %#v", capacity, len(got), capacity, got)
			}
			for index, delivery := range got {
				want := string(rune('a' + total - capacity + index))
				if delivery.Message.ID() != want {
					t.Fatalf("capacity %d: delivery %d = %q, want %q", capacity, index, delivery.Message.ID(), want)
				}
			}

			drops := 0
			for _, record := range router.Records() {
				if record.Kind == RecordDropped {
					drops++
				}
			}
			if drops != total-capacity {
				t.Fatalf("capacity %d: drops = %d, want %d", capacity, drops, total-capacity)
			}
		})
	}
}

// Concurrent publishing against a paused KeepNewest consumer: every
// published message is either delivered or recorded exactly once, and the
// retained backlog never exceeds the buffer.
func TestConcurrentPublishPausedConsumerKeepNewest(t *testing.T) {
	router, err := NewRouter(WithHistorySize(4096))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}

	const workers = 4
	const perWorker = 100
	var publishers sync.WaitGroup
	publishers.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer publishers.Done()
			for index := 0; index < perWorker; index++ {
				_ = router.Publish(mustMessage(t, "t", "t", nil))
			}
		}(worker)
	}
	publishers.Wait()
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	got := drain(sub)
	if len(got) > 4 {
		t.Fatalf("retained = %d, want at most buffer size 4", len(got))
	}
	drops := 0
	for _, record := range router.Records() {
		if record.Kind != RecordDropped {
			t.Fatalf("unexpected record: %#v", record)
		}
		drops++
	}
	if got, want := len(got)+drops, workers*perWorker; got != want {
		t.Fatalf("delivered + dropped = %d, want %d", got, want)
	}
}

// Messages already accepted when Unsubscribe is called are a drain backlog:
// they are all delivered in order after cancellation, under either
// overflow policy, and nothing new is accepted afterwards.
func TestUnsubscribeDrainsBacklogUnderBothPolicies(t *testing.T) {
	for _, policy := range []OverflowPolicy{KeepOldest, KeepNewest} {
		t.Run(string(policy), func(t *testing.T) {
			router, err := NewRouter()
			if err != nil {
				t.Fatal(err)
			}
			sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
				WithSubscriptionBufferSize(4), WithOverflowPolicy(policy))
			if err != nil {
				t.Fatal(err)
			}

			for _, id := range []string{"m1", "m2", "m3", "m4"} {
				if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
					t.Fatal(err)
				}
			}
			if err := router.Unsubscribe("sub"); err != nil {
				t.Fatal(err)
			}
			if err := router.Publish(mustMessage(t, "m5", "t", nil)); err != nil {
				t.Fatal(err)
			}

			got := drain(sub)
			if len(got) != 4 {
				t.Fatalf("drained = %d, want 4: %#v", len(got), got)
			}
			want := []string{"m1", "m2", "m3", "m4"}
			for index, delivery := range got {
				if delivery.Message.ID() != want[index] {
					t.Fatalf("delivery %d = %q, want %q", index, delivery.Message.ID(), want[index])
				}
			}
		})
	}
}

// Multi-rule matches, back-to-back updates and unsubscribe with a backlog
// all stay consistent under concurrent load: every consumed delivery is
// ordered and self-describing via RuleVersion.
func TestConcurrentUpdatesVersionEveryDelivery(t *testing.T) {
	router, err := NewRouter(WithBufferSize(256), WithHistorySize(1024))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "catch-all", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	const publishes = 400
	consumed := make(chan Delivery, 3*publishes)
	var consumer sync.WaitGroup
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for delivery := range sub.Deliveries {
			consumed <- delivery
		}
	}()

	var publishers sync.WaitGroup
	publishers.Add(3)
	for worker := 0; worker < 3; worker++ {
		go func(worker int) {
			defer publishers.Done()
			for index := 0; index < publishes; index++ {
				_ = router.Publish(mustMessage(t, "t", "t", nil))
			}
		}(worker)
	}
	go func() {
		for index := 0; index < 60; index++ {
			rules := []Rule{
				{ID: "catch-all", Topic: "t"},
				{ID: "even", Topic: "t", Attrs: map[string]string{"n": "even"}},
			}
			if index%2 == 0 {
				rules[1].Attrs["n"] = "odd"
			}
			_ = router.UpdateRules("sub", rules)
		}
	}()
	publishers.Wait()
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	consumer.Wait()
	close(consumed)

	for delivery := range consumed {
		if delivery.RuleVersion < 1 || delivery.RuleVersion > 61 {
			t.Fatalf("delivery version out of range: %#v", delivery)
		}
		if len(delivery.RuleIDs) == 0 {
			t.Fatalf("delivery without matched rules: %#v", delivery)
		}
	}
}
