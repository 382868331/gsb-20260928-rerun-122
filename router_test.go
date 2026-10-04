package router

import (
	"errors"
	"sync"
	"testing"
)

func mustMessage(t *testing.T, id, topic string, attrs map[string]string) Message {
	t.Helper()
	message, err := NewMessage(id, topic, attrs)
	if err != nil {
		t.Fatalf("NewMessage(%q): %v", id, err)
	}
	return message
}

func drain(sub Subscription) []Delivery {
	var deliveries []Delivery
	for delivery := range sub.Deliveries {
		deliveries = append(deliveries, delivery)
	}
	return deliveries
}

func TestRoutesByTopicAndAttributes(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}

	sub, err := router.Subscribe("alerts", []Rule{
		{ID: "all-alerts", Topic: "alerts"},
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
		{ID: "other", Topic: "other"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Publish(mustMessage(t, "m1", "alerts", map[string]string{"severity": "critical"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m2", "alerts", map[string]string{"severity": "info"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "ignored", nil)); err != nil {
		t.Fatal(err)
	}

	var deliveries []Delivery
	for index := 0; index < 2; index++ {
		deliveries = append(deliveries, <-sub.Deliveries)
	}
	if len(deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2: %#v", len(deliveries), deliveries)
	}
	if got := deliveries[0].RuleIDs; len(got) != 2 || got[0] != "all-alerts" || got[1] != "critical" {
		t.Fatalf("matched rules = %v, want both rules once", got)
	}
	if got := deliveries[1].RuleIDs; len(got) != 1 || got[0] != "all-alerts" {
		t.Fatalf("matched rules = %v, want topic rule only", got)
	}
	if err := router.Unsubscribe("alerts"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-sub.Deliveries; open {
		t.Fatal("delivery channel remains open after unsubscribe")
	}
}

func TestSlowSubscriberDoesNotBlockOthersAndRecordsDrop(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}}, WithSubscriptionBufferSize(1))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}}, WithSubscriptionBufferSize(10))
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 10; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	// The fast consumer receives everything despite the slow one.
	if got := len(receiveAll(t, fast, 10)); got != 10 {
		t.Fatalf("fast deliveries = %d, want 10", got)
	}
	// The slow consumer receives exactly the oldest message; everything
	// else was dropped while its buffer stayed full.
	if got := <-slow.Deliveries; got.Message.ID() != "a" {
		t.Fatalf("slow first delivery = %q, want a", got.Message.ID())
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-slow.Deliveries; open {
		t.Fatal("slow channel remains open after unsubscribe")
	}

	records := router.Records()
	if len(records) != 9 {
		t.Fatalf("got %d records, want 9: %#v", len(records), records)
	}
	for _, record := range records {
		if record.Kind != RecordDropped || record.Code != "subscriber_buffer_full" || record.SubscriptionID != "slow" {
			t.Fatalf("unexpected record: %#v", record)
		}
	}
}

func TestUpdateRulesAreAtomicAndRejectedInvalidInput(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "old", Topic: "old"}})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.UpdateRules("sub", []Rule{{ID: "new", Topic: "new", Attrs: map[string]string{"env": "test"}}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m1", "old", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m2", "new", map[string]string{"env": "test"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "new", map[string]string{"env": "prod"})); err != nil {
		t.Fatal(err)
	}

	err = router.UpdateRules("sub", []Rule{{ID: "bad", Topic: ""}})
	if !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("invalid update error = %v, want ErrInvalidRule", err)
	}
	err = router.UpdateRules("missing", []Rule{{ID: "rule", Topic: "new"}})
	if !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("missing update error = %v, want ErrSubscriptionGone", err)
	}

	delivery := <-sub.Deliveries
	if delivery.Message.ID() != "m2" {
		t.Fatalf("delivery = %#v, want only m2", delivery)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	rejects := 0
	for _, record := range router.Records() {
		if record.Kind == RecordRejected {
			rejects++
		}
	}
	if rejects != 2 {
		t.Fatalf("reject records = %d, want 2", rejects)
	}
}

// Cancellation boundary: once Unsubscribe returns, the channel is already
// closed and no queued message is ever delivered. Messages the consumer
// received before the boundary remain valid; everything still buffered is
// discarded and recorded with code "subscription_cancelled".
func TestUnsubscribeDiscardsBacklogAndClosesChannel(t *testing.T) {
	router, err := NewRouter(WithBufferSize(2))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m2", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if _, open := <-sub.Deliveries; open {
		t.Fatal("delivery channel remains open after unsubscribe returned")
	}

	cancelled := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Kind == RecordDropped && record.Code == "subscription_cancelled" {
			cancelled[record.MessageID] = true
			if record.SubscriptionID != "sub" || record.RuleVersion != 1 {
				t.Fatalf("cancel record targets wrong subscription or version: %#v", record)
			}
		}
	}
	if !cancelled["m1"] || !cancelled["m2"] || len(cancelled) != 2 {
		t.Fatalf("cancelled backlog = %v, want m1 and m2", cancelled)
	}
}

// A message the consumer received before Unsubscribe is never revoked:
// the boundary only discards what was still unreceived.
func TestUnsubscribeKeepsAlreadyReceivedMessages(t *testing.T) {
	router, err := NewRouter(WithBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	received := <-sub.Deliveries
	if received.Message.ID() != "m1" {
		t.Fatalf("received = %q, want m1", received.Message.ID())
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-sub.Deliveries; open {
		t.Fatal("delivery channel remains open after unsubscribe")
	}
	// m1 was handed to the consumer and stays valid; m2/m3 were still
	// unreceived and are accounted for as cancelled.
	cancelled := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Code == "subscription_cancelled" {
			cancelled[record.MessageID] = true
		}
	}
	if cancelled["m1"] {
		t.Fatal("received message m1 must not be recorded as cancelled")
	}
	if !cancelled["m2"] || !cancelled["m3"] {
		t.Fatalf("cancelled = %v, want m2 and m3", cancelled)
	}
}

// Re-subscribing under the same ID starts a fresh subscription: the old
// backlog is never moved into the new subscription.
func TestResubscribeDoesNotInheritBacklog(t *testing.T) {
	router, err := NewRouter(WithBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	first, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-first.Deliveries; open {
		t.Fatal("old channel remains open after unsubscribe")
	}

	second, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "t", nil)); err != nil {
		t.Fatal(err)
	}
	delivery := <-second.Deliveries
	if delivery.Message.ID() != "m3" || delivery.RuleVersion != 1 {
		t.Fatalf("new subscription first delivery = %#v, want only m3 at version 1", delivery)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-second.Deliveries; open {
		t.Fatal("new channel remains open after unsubscribe")
	}
}

func TestConcurrentRuleUpdatesAndPublishes(t *testing.T) {
	router, err := NewRouter(WithBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "initial", Topic: "topic"}})
	if err != nil {
		t.Fatal(err)
	}

	const publishes = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < publishes; index++ {
			_ = router.Publish(mustMessage(t, "topic-message", "topic", nil))
		}
	}()
	go func() {
		defer wg.Done()
		for index := 0; index < 50; index++ {
			rules := []Rule{{ID: "topic", Topic: "topic"}}
			if index%2 == 0 {
				rules[0].Attrs = map[string]string{"n": string(rune('0' + index%10))}
			}
			_ = router.UpdateRules("sub", rules)
		}
	}()
	wg.Wait()

	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	for range sub.Deliveries {
	}
}

func TestMessageAttributesAreCopied(t *testing.T) {
	attrs := map[string]string{"region": "cn"}
	message, err := NewMessage("m1", "t", attrs)
	if err != nil {
		t.Fatal(err)
	}
	attrs["region"] = "us"
	if value, _ := message.Attribute("region"); value != "cn" {
		t.Fatalf("attribute = %q, want cn", value)
	}
}
