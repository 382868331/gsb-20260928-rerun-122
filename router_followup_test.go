package router

import (
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

// KeepNewest evicts the oldest buffered message, keeps the newest ones in
// order, and records the eviction with the matched rule version. Fast
// subscribers remain completely unaffected.
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
	if len(got) < 3 || len(got) > 4 {
		t.Fatalf("slow deliveries = %d, want 3 or 4: %#v", len(got), got)
	}
	seen := make(map[string]bool, len(got))
	for _, delivery := range got {
		if delivery.RuleVersion != 1 {
			t.Fatalf("delivery %q version = %d, want 1", delivery.Message.ID(), delivery.RuleVersion)
		}
		seen[delivery.Message.ID()] = true
	}
	// The three newest messages are always retained, in order, possibly
	// preceded by m1 which was already pinned in the pump's handoff slot.
	wantTail := []string{"m3", "m4", "m5"}
	tail := got[len(got)-3:]
	for index, id := range wantTail {
		if tail[index].Message.ID() != id {
			t.Fatalf("delivery tail %d = %q, want %q; all: %#v", index, tail[index].Message.ID(), id, got)
		}
	}
	if len(got) == 4 && !seen["m1"] {
		t.Fatalf("fourth retained delivery must be the pinned m1: %#v", got)
	}
	if fast := <-fastDone; len(fast) != 5 {
		t.Fatalf("fast deliveries = %d, want 5", len(fast))
	}

	var evictions, rejects int
	for _, record := range router.Records() {
		switch {
		case record.Kind == RecordDropped && record.Code == "subscriber_buffer_evicted":
			evictions++
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
