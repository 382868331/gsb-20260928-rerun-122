package router

import (
	"sync"
	"testing"
)

// TestDeliveriesCarryRuleVersion walks a subscription through consecutive
// rule updates and checks each delivery is stamped with the exact rule
// edition that matched it, including multi-rule hits within one edition.
func TestDeliveriesCarryRuleVersion(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{
		{ID: "v1-all", Topic: "t"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Version 1: one rule matches everything.
	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}

	// Version 2: two rules can hit the same message simultaneously.
	if err := router.UpdateRules("sub", []Rule{
		{ID: "v2-all", Topic: "t"},
		{ID: "v2-critical", Topic: "t", Attrs: map[string]string{"severity": "critical"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m2", "t", map[string]string{"severity": "critical"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "t", map[string]string{"severity": "info"})); err != nil {
		t.Fatal(err)
	}

	// Version 3: only one rule remains.
	if err := router.UpdateRules("sub", []Rule{{ID: "v3-critical", Topic: "t", Attrs: map[string]string{"severity": "critical"}}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m4", "t", map[string]string{"severity": "critical"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m5", "t", map[string]string{"severity": "info"})); err != nil {
		t.Fatal(err)
	}

	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	deliveries := drain(sub)

	want := []struct {
		id      string
		version int
		rules   []string
	}{
		{"m1", 1, []string{"v1-all"}},
		{"m2", 2, []string{"v2-all", "v2-critical"}},
		{"m3", 2, []string{"v2-all"}},
		{"m4", 3, []string{"v3-critical"}},
	}
	if len(deliveries) != len(want) {
		t.Fatalf("got %d deliveries, want %d: %#v", len(deliveries), len(want), deliveries)
	}
	for index, expected := range want {
		got := deliveries[index]
		if got.Message.ID() != expected.id {
			t.Fatalf("delivery %d id = %q, want %q", index, got.Message.ID(), expected.id)
		}
		if got.RuleVersion != expected.version {
			t.Fatalf("delivery %s version = %d, want %d", expected.id, got.RuleVersion, expected.version)
		}
		if len(got.RuleIDs) != len(expected.rules) {
			t.Fatalf("delivery %s rules = %v, want %v", expected.id, got.RuleIDs, expected.rules)
		}
		for ruleIndex, ruleID := range expected.rules {
			if got.RuleIDs[ruleIndex] != ruleID {
				t.Fatalf("delivery %s rules = %v, want %v", expected.id, got.RuleIDs, expected.rules)
			}
		}
	}
}

// TestOverflowKeepOldestRetainsBufferedMessages confirms the default policy:
// when the buffer is full the new message is dropped and the oldest buffered
// messages survive until consumed.
func TestOverflowKeepOldestRetainsBufferedMessages(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2))
	if err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 5; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	deliveries := drain(slow)
	if len(deliveries) != 2 || deliveries[0].Message.ID() != "a" || deliveries[1].Message.ID() != "b" {
		t.Fatalf("deliveries = %#v, want only a and b retained", deliveries)
	}

	records := router.Records()
	if len(records) != 3 {
		t.Fatalf("got %d drop records, want 3", len(records))
	}
	for _, record := range records {
		if record.Kind != RecordDropped || record.Code != DropReasonBufferFull {
			t.Fatalf("unexpected record: %#v", record)
		}
		if record.RuleVersion != 1 {
			t.Fatalf("drop record version = %d, want 1: %#v", record.RuleVersion, record)
		}
	}
}

// TestOverflowKeepNewestEvictsOldest walks a slow subscription message by
// message: every overflow must evict exactly the oldest buffered message,
// the newest messages must remain consumable, and each audit record must name
// the evicted message and the rule version in effect.
func TestOverflowKeepNewestEvictsOldest(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2),
		WithSubscriptionOverflowPolicy(OverflowKeepNewest))
	if err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 5; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	deliveries := drain(slow)
	if len(deliveries) != 2 || deliveries[0].Message.ID() != "d" || deliveries[1].Message.ID() != "e" {
		t.Fatalf("deliveries = %#v, want only d and e retained", deliveries)
	}

	records := router.Records()
	wantEvicted := []string{"a", "b", "c"}
	if len(records) != len(wantEvicted) {
		t.Fatalf("got %d drop records, want %d: %#v", len(records), len(wantEvicted), records)
	}
	for index, evictedID := range wantEvicted {
		record := records[index]
		if record.Kind != RecordDropped || record.Code != DropReasonOldestEvicted {
			t.Fatalf("record %d = %#v, want oldest eviction", index, record)
		}
		if record.EvictedMessageID != evictedID {
			t.Fatalf("record %d evicted = %q, want %q", index, record.EvictedMessageID, evictedID)
		}
		if record.MessageID != string(rune('c'+index)) {
			t.Fatalf("record %d retained message = %q, want %q", index, record.MessageID, string(rune('c'+index)))
		}
		if record.SubscriptionID != "slow" || record.RuleVersion != 1 {
			t.Fatalf("record %d metadata mismatch: %#v", index, record)
		}
	}
}

// TestKeepNewestSlowSubscriberDoesNotAffectFastSubscriber proves the eviction
// policy is local to one subscription: the slow consumer loses messages while
// the fast consumer still receives every published message.
func TestKeepNewestSlowSubscriberDoesNotAffectFastSubscriber(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(1),
		WithSubscriptionOverflowPolicy(OverflowKeepNewest))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	fastDone := make(chan []Delivery, 1)
	go func() { fastDone <- drain(fast) }()

	for index := 0; index < 10; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	fastDeliveries := <-fastDone
	if len(fastDeliveries) != 10 {
		t.Fatalf("fast deliveries = %d, want all 10", len(fastDeliveries))
	}

	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	slowDeliveries := drain(slow)
	if len(slowDeliveries) != 1 || slowDeliveries[0].Message.ID() != "j" {
		t.Fatalf("slow deliveries = %#v, want only the newest j", slowDeliveries)
	}

	evictions := 0
	for _, record := range router.Records() {
		if record.Kind == RecordDropped && record.SubscriptionID == "slow" && record.Code == DropReasonOldestEvicted {
			evictions++
		}
	}
	if evictions != 9 {
		t.Fatalf("slow evictions = %d, want 9", evictions)
	}
}

// TestConcurrentRuleUpdatesStampCoherentVersion runs publishes against
// continuously changing rules and verifies every delivery behaved exactly
// like its stamped RuleVersion: odd editions use a broad rule, even editions
// use a narrow attribute rule. A delivery can therefore never come from a
// mixture of two rule editions.
func TestConcurrentRuleUpdatesStampCoherentVersion(t *testing.T) {
	router, err := NewRouter(WithBufferSize(256))
	if err != nil {
		t.Fatal(err)
	}
	// Version 1 is the broad rule. Each update toggles broad <-> narrow.
	sub, err := router.Subscribe("sub", []Rule{{ID: "broad", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	const publishes = 400
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < publishes; index++ {
			attrs := map[string]string{"region": "cn"}
			if index%2 == 1 {
				attrs["region"] = "us"
			}
			_ = router.Publish(mustMessage(t, "msg", "t", attrs))
		}
	}()
	go func() {
		defer wg.Done()
		for index := 0; index < 100; index++ {
			if index%2 == 0 {
				_ = router.UpdateRules("sub", []Rule{{ID: "narrow", Topic: "t", Attrs: map[string]string{"region": "cn"}}})
			} else {
				_ = router.UpdateRules("sub", []Rule{{ID: "broad", Topic: "t"}})
			}
		}
	}()
	wg.Wait()

	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	count := 0
	for delivery := range sub.Deliveries {
		count++
		switch {
		case delivery.RuleVersion%2 == 1:
			if len(delivery.RuleIDs) != 1 || delivery.RuleIDs[0] != "broad" {
				t.Fatalf("odd version %d delivery matched %v, want broad only", delivery.RuleVersion, delivery.RuleIDs)
			}
		case delivery.RuleVersion%2 == 0:
			if region, _ := delivery.Message.Attribute("region"); region != "cn" {
				t.Fatalf("even version delivered a message the narrow rule does not match: %#v", delivery)
			}
			if len(delivery.RuleIDs) != 1 || delivery.RuleIDs[0] != "narrow" {
				t.Fatalf("even version %d delivery matched %v, want narrow only", delivery.RuleVersion, delivery.RuleIDs)
			}
		}
	}
	if count == 0 {
		t.Fatal("expected at least one delivery under concurrent updates")
	}
}
