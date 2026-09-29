package messagerouter

import (
	"strconv"
	"sync"
	"testing"
)

func TestMultipleMatchingRulesDeliverOnce(t *testing.T) {
	router := NewRouter(4)
	rules := []Rule{
		{ID: "region", Topic: "orders", Attributes: Attributes{"region": "cn"}},
		{ID: "priority", Topic: "orders", Attributes: Attributes{"priority": "high"}},
		{ID: "other-topic", Topic: "payments"},
	}
	sub, err := router.Subscribe("alerts", rules)
	if err != nil {
		t.Fatal(err)
	}

	message := Message{
		ID:         "m1",
		Topic:      "orders",
		Attributes: Attributes{"region": "cn", "priority": "high"},
	}
	if err := router.Publish(message); err != nil {
		t.Fatal(err)
	}

	delivery := <-sub.C
	if delivery.Message.ID != "m1" {
		t.Fatalf("got message %q, want m1", delivery.Message.ID)
	}
	wantRules := []string{"priority", "region"}
	if len(delivery.RuleIDs) != len(wantRules) {
		t.Fatalf("got rules %v, want %v", delivery.RuleIDs, wantRules)
	}
	for index := range wantRules {
		if delivery.RuleIDs[index] != wantRules[index] {
			t.Fatalf("got rules %v, want %v", delivery.RuleIDs, wantRules)
		}
	}
}

func TestSlowSubscriberDoesNotBlockOthersAndRejectsAreRecorded(t *testing.T) {
	router := NewRouter(1)
	slow, err := router.Subscribe("slow", []Rule{{ID: "all", Topic: "events"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "all", Topic: "events"}}, 4)
	if err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 4; index++ {
		if err := router.Publish(Message{ID: "slow-" + string(rune('1'+index)), Topic: "events"}); err != nil {
			t.Fatal(err)
		}
	}

	received := 0
	for range fast.C {
		received++
		if received == 4 {
			break
		}
	}
	if received != 4 {
		t.Fatalf("fast subscriber received %d messages, want 4", received)
	}
	if _, open := <-slow.C; !open {
		t.Fatal("slow subscription was unexpectedly closed")
	}
	rejected := 0
	for _, record := range router.Records() {
		if record.Kind == RecordRejected && record.SubscriptionID == "slow" {
			rejected++
		}
	}
	if rejected != 3 {
		t.Fatalf("got %d rejection records, want 3", rejected)
	}
}

func TestUnsubscribeDrainsPendingMessagesAndClosesChannel(t *testing.T) {
	router := NewRouter(4)
	sub, err := router.Subscribe("done", []Rule{{ID: "all", Topic: "events"}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := router.Publish(Message{ID: "cancel-" + string(rune('1'+index)), Topic: "events"}); err != nil {
			t.Fatal(err)
		}
	}

	sub.Unsubscribe()

	if _, open := <-sub.C; open {
		t.Fatal("delivery channel remained open after unsubscribe")
	}
	if err := router.Publish(Message{ID: "after", Topic: "events"}); err != nil {
		t.Fatal(err)
	}
	select {
	case delivery, ok := <-sub.C:
		if ok {
			t.Fatalf("delivered after unsubscribe: %+v", delivery)
		}
	default:
		t.Fatal("closed channel should report not open")
	}
	discarded := 0
	for _, record := range router.Records() {
		if record.Kind == RecordDiscarded && record.SubscriptionID == "done" {
			discarded++
		}
	}
	if discarded != 3 {
		t.Fatalf("got %d discard records, want 3", discarded)
	}
}

func TestConcurrentRuleReplacementAndPublishing(t *testing.T) {
	router := NewRouter(128)
	sub, err := router.Subscribe("dynamic", []Rule{{ID: "v1", Topic: "events"}}, 128)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < 200; index++ {
			rules := []Rule{{ID: "v1", Topic: "events"}}
			if index%2 == 0 {
				rules = append(rules, Rule{ID: "v2", Topic: "events", Attributes: Attributes{"kind": "important"}})
			}
			_ = sub.ReplaceRules(rules...)
		}
	}()
	go func() {
		defer wg.Done()
		for index := 0; index < 200; index++ {
			message := Message{ID: "rule-race-" + strconv.Itoa(index), Topic: "events"}
			if index%2 == 0 {
				message.Attributes = Attributes{"kind": "important"}
			}
			_ = router.Publish(message)
		}
	}()
	wg.Wait()

	done := make(chan struct{})
	go func() {
		for range sub.C {
		}
		close(done)
	}()
	sub.Unsubscribe()
	<-done
}

func TestUnsubscribeIsSafeDuringConcurrentPublishing(t *testing.T) {
	router := NewRouter(256)
	sub, err := router.Subscribe("race", []Rule{{ID: "all", Topic: "events"}}, 256)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := 0; index < 500; index++ {
			_ = router.Publish(Message{ID: "cancel-race-" + strconv.Itoa(index), Topic: "events"})
		}
	}()
	go func() {
		defer wg.Done()
		sub.Unsubscribe()
	}()
	wg.Wait()

	if _, open := <-sub.C; open {
		t.Fatal("delivery channel remained open")
	}
}
