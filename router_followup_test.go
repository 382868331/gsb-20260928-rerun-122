package router

import (
	"errors"
	"fmt"
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
	if len(got) != 3 {
		t.Fatalf("slow deliveries = %d, want exactly 3: %#v", len(got), got)
	}
	for _, delivery := range got {
		if delivery.RuleVersion != 1 {
			t.Fatalf("delivery %q version = %d, want 1", delivery.Message.ID(), delivery.RuleVersion)
		}
	}
	// The consumer never read anything, so every message -- including the one
	// parked in the pump's handoff -- participates in KeepNewest eviction:
	// the consumer sees exactly the newest N messages, in FIFO order.
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
	if evictions != 2 {
		t.Fatalf("evictions = %d, want 2", evictions)
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

func TestUpdateOverflowPolicyAppliesToFutureOffersAndVersionsBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(3))
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

	publish := func(id string) {
		t.Helper()
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		publish(id)
	}
	if err := router.UpdateOverflowPolicy("slow", KeepNewest); err != nil {
		t.Fatal(err)
	}
	publish("m4")
	if err := router.UpdateOverflowPolicy("slow", KeepOldest); err != nil {
		t.Fatal(err)
	}
	publish("m5")
	if err := router.UpdateOverflowPolicy("slow", KeepOldest); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}

	got := drain(slow)
	wantIDs := []string{"m2", "m3", "m4"}
	wantVersions := []int{1, 1, 2}
	if len(got) != len(wantIDs) {
		t.Fatalf("slow deliveries = %d, want %d: %#v", len(got), len(wantIDs), got)
	}
	for index := range wantIDs {
		if got[index].Message.ID() != wantIDs[index] || got[index].PolicyVersion != wantVersions[index] {
			t.Fatalf("delivery %d = %#v, want %s at policy version %d", index, got[index], wantIDs[index], wantVersions[index])
		}
	}
	if fast := <-fastDone; len(fast) != 5 {
		t.Fatalf("fast deliveries = %d, want all 5", len(fast))
	}

	var evictions, fullDrops int
	for _, record := range router.Records() {
		switch record.Code {
		case "subscriber_buffer_evicted":
			evictions++
			if record.MessageID != "m1" || record.PolicyVersion != 1 || record.SubscriptionID != "slow" {
				t.Fatalf("unexpected eviction: %#v", record)
			}
		case "subscriber_buffer_full":
			fullDrops++
			if record.MessageID != "m5" || record.PolicyVersion != 3 || record.SubscriptionID != "slow" {
				t.Fatalf("unexpected full drop: %#v", record)
			}
		}
	}
	if evictions != 1 || fullDrops != 1 {
		t.Fatalf("evictions = %d, full drops = %d, want 1 each", evictions, fullDrops)
	}
}

func TestUpdateOverflowPolicyFromNewestToOldestKeepsCurrentBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}

	for index := 1; index <= 5; index++ {
		if err := router.Publish(mustMessage(t, fmt.Sprintf("m%d", index), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.UpdateOverflowPolicy("sub", KeepOldest); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m6", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	got := drain(sub)
	if len(got) != 3 {
		t.Fatalf("deliveries = %d, want 3: %#v", len(got), got)
	}
	for index, id := range []string{"m3", "m4", "m5"} {
		if got[index].Message.ID() != id || got[index].PolicyVersion != 1 {
			t.Fatalf("delivery %d = %#v, want %s retained by policy version 1", index, got[index], id)
		}
	}
	records := router.Records()
	if len(records) != 3 {
		t.Fatalf("records = %d, want two evictions and one full drop: %#v", len(records), records)
	}
	var m6 *Record
	for index := range records {
		if records[index].MessageID == "m6" {
			m6 = &records[index]
		}
	}
	if m6 == nil || m6.Code != "subscriber_buffer_full" || m6.PolicyVersion != 2 {
		t.Fatalf("m6 record = %#v, want policy version 2 full drop", m6)
	}
}

func TestZeroCapacityDropsEveryPausedDelivery(t *testing.T) {
	for _, policy := range []OverflowPolicy{KeepOldest, KeepNewest} {
		t.Run(string(policy), func(t *testing.T) {
			router, err := NewRouter()
			if err != nil {
				t.Fatal(err)
			}
			sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
				WithSubscriptionBufferSize(0), WithOverflowPolicy(policy))
			if err != nil {
				t.Fatal(err)
			}
			if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
				t.Fatal(err)
			}
			if err := router.Unsubscribe("sub"); err != nil {
				t.Fatal(err)
			}
			if got := drain(sub); len(got) != 0 {
				t.Fatalf("deliveries = %#v, want none for zero capacity", got)
			}
			records := router.Records()
			if len(records) != 1 || records[0].Code != "subscriber_buffer_full" || records[0].PolicyVersion != 1 {
				t.Fatalf("records = %#v, want one version-1 full drop", records)
			}
		})
	}
}

func TestZeroCapacityDeliversWhenReceiverIsReady(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(0), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Delivery, 1)
	go func() {
		received <- <-sub.Deliveries
	}()

	var delivered bool
	for attempt := 0; attempt < 1000 && !delivered; attempt++ {
		if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
			t.Fatal(err)
		}
		select {
		case delivery := <-received:
			if delivery.Message.ID() != "m1" {
				t.Fatalf("delivery = %#v, want m1", delivery)
			}
			delivered = true
		default:
		}
	}
	if !delivered {
		t.Fatal("zero-capacity subscription never hand-delivered to a ready receiver")
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	drain(sub)
}

func TestUpdateOverflowPolicyRejectsInvalidPolicyAndGoneSubscription(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.UpdateOverflowPolicy("sub", "keep-middle"); err == nil {
		t.Fatal("invalid policy update unexpectedly succeeded")
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.UpdateOverflowPolicy("sub", KeepNewest); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("gone subscription policy update error = %v, want ErrSubscriptionGone", err)
	}
	drain(sub)
}

func TestConcurrentPausedPublishersKeepExactlyOneBoundedWindow(t *testing.T) {
	for _, policy := range []OverflowPolicy{KeepOldest, KeepNewest} {
		t.Run(string(policy), func(t *testing.T) {
			router, err := NewRouter(WithHistorySize(1024))
			if err != nil {
				t.Fatal(err)
			}
			sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
				WithSubscriptionBufferSize(3), WithOverflowPolicy(policy))
			if err != nil {
				t.Fatal(err)
			}

			const workers = 3
			const perWorker = 20
			var publishers sync.WaitGroup
			publishers.Add(workers)
			for worker := 0; worker < workers; worker++ {
				go func(worker int) {
					defer publishers.Done()
					for index := 0; index < perWorker; index++ {
						id := fmt.Sprintf("m%d-%d", index, worker)
						if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
							t.Errorf("Publish(%s): %v", id, err)
							return
						}
					}
				}(worker)
			}
			publishers.Wait()
			if err := router.Unsubscribe("sub"); err != nil {
				t.Fatal(err)
			}

			got := drain(sub)
			if len(got) != 3 {
				t.Fatalf("deliveries = %d, want bounded window of 3: %#v", len(got), got)
			}
			lastByWorker := make(map[int]int)
			seen := make(map[string]bool)
			for _, delivery := range got {
				var sequence, worker int
				if _, err := fmt.Sscanf(delivery.Message.ID(), "m%d-%d", &sequence, &worker); err != nil {
					t.Fatalf("delivery id %q: %v", delivery.Message.ID(), err)
				}
				if seen[delivery.Message.ID()] {
					t.Fatalf("duplicate delivery %q: %#v", delivery.Message.ID(), got)
				}
				seen[delivery.Message.ID()] = true
				if previous, ok := lastByWorker[worker]; ok && sequence <= previous {
					t.Fatalf("worker %d deliveries are not FIFO: %#v", worker, got)
				}
				lastByWorker[worker] = sequence
			}
			dropped := 0
			for _, record := range router.Records() {
				if record.Kind != RecordDropped || record.RuleVersion != 1 || record.PolicyVersion != 1 {
					t.Fatalf("unexpected audit record: %#v", record)
				}
				dropped++
			}
			if dropped != workers*perWorker-3 {
				t.Fatalf("dropped = %d, want %d", dropped, workers*perWorker-3)
			}
		})
	}
}
