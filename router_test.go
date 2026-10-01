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
	if err := router.Unsubscribe("alerts"); err != nil {
		t.Fatal(err)
	}

	deliveries := drain(sub)
	if len(deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2: %#v", len(deliveries), deliveries)
	}
	if got := deliveries[0].RuleIDs; len(got) != 2 || got[0] != "all-alerts" || got[1] != "critical" {
		t.Fatalf("matched rules = %v, want both rules once", got)
	}
	if got := deliveries[1].RuleIDs; len(got) != 1 || got[0] != "all-alerts" {
		t.Fatalf("matched rules = %v, want topic rule only", got)
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
	fastDone := make(chan []Delivery, 1)
	go func() {
		fastDone <- drain(fast)
	}()

	for index := 0; index < 10; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	slowGot := drain(slow)
	if len(slowGot) != 1 || slowGot[0].Message.ID() != "a" {
		t.Fatalf("slow deliveries = %#v, want only oldest message a", slowGot)
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	if got := len(<-fastDone); got != 10 {
		t.Fatalf("fast deliveries = %d, want 10", got)
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
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	deliveries := drain(sub)
	if len(deliveries) != 1 || deliveries[0].Message.ID() != "m2" {
		t.Fatalf("deliveries = %#v, want only m2", deliveries)
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

func TestUnsubscribeFinishesInFlightAndDeliversQueuedMessages(t *testing.T) {
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
	if _, open := <-sub.Deliveries; !open {
		t.Fatal("delivery channel closed before queued delivery")
	}
	if _, open := <-sub.Deliveries; !open {
		t.Fatal("delivery channel closed before queued delivery")
	}
	if _, open := <-sub.Deliveries; open {
		t.Fatal("delivery channel remains open after unsubscribe")
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
