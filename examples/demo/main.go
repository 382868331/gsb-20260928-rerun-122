// Command demo is a self-contained walkthrough of the message-router:
// real consumers read every delivery, multi-rule matches, successive rule
// updates with version stamps, slow subscribers under both overflow
// policies, and draining a backlog after unsubscribe.
package main

import (
	"fmt"

	router "message-router"
)

func mustMsg(id, topic string, attrs map[string]string) router.Message {
	msg, err := router.NewMessage(id, topic, attrs)
	if err != nil {
		panic(err)
	}
	return msg
}

func consume(name string, sub router.Subscription) []router.Delivery {
	var got []router.Delivery
	for delivery := range sub.Deliveries {
		fmt.Printf("  [%s] consume %-3s rules=%v ruleVersion=%d\n",
			name, delivery.Message.ID(), delivery.RuleIDs, delivery.RuleVersion)
		got = append(got, delivery)
	}
	return got
}

func main() {
	// 1) Multi-rule matches plus successive rule updates. Each delivery
	//    carries the exact rule-set version that produced it.
	fmt.Println("== multi-rule matches and rule versions ==")
	r, err := router.NewRouter()
	if err != nil {
		panic(err)
	}
	sub, err := r.Subscribe("alerts", []router.Rule{
		{ID: "all-alerts", Topic: "alerts"},
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
	})
	if err != nil {
		panic(err)
	}

	_ = r.Publish(mustMsg("m1", "alerts", map[string]string{"severity": "critical"})) // hits v1: both rules
	if err := r.UpdateRules("alerts", []router.Rule{{ID: "only-critical", Topic: "alerts",
		Attrs: map[string]string{"severity": "critical"}}}); err != nil { // v2
		panic(err)
	}
	if err := r.UpdateRules("alerts", []router.Rule{{ID: "only-info", Topic: "alerts",
		Attrs: map[string]string{"severity": "info"}}}); err != nil { // v3
		panic(err)
	}
	_ = r.Publish(mustMsg("m2", "alerts", map[string]string{"severity": "critical"})) // v3: no match
	_ = r.Publish(mustMsg("m3", "alerts", map[string]string{"severity": "info"}))     // v3: match

	if err := r.Unsubscribe("alerts"); err != nil {
		panic(err)
	}
	deliveries := consume("alerts", sub)
	fmt.Printf("  consumed %d deliveries, channel closed after drain\n\n", len(deliveries))

	// 2) Slow subscribers: KeepOldest drops the newest messages, while a
	//    fast subscriber is never affected.
	fmt.Println("== slow subscriber, KeepOldest (default) ==")
	slowOld, err := r.Subscribe("slow-old", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(2))
	if err != nil {
		panic(err)
	}
	fast, err := r.Subscribe("fast", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(8))
	if err != nil {
		panic(err)
	}
	fastDone := make(chan int, 1)
	go func() {
		fastDone <- len(consume("fast", fast))
	}()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		_ = r.Publish(mustMsg(id, "t", nil))
	}
	_ = r.Unsubscribe("fast")
	if got := <-fastDone; got != 5 {
		panic(fmt.Sprintf("fast got %d, want 5", got))
	}
	_ = r.Unsubscribe("slow-old")
	got := consume("slow-old", slowOld)
	fmt.Printf("  slow retained %v (oldest kept)\n\n", ids(got))

	// 3) Slow subscribers: KeepNewest evicts the oldest buffered messages
	//    so the newest batch survives; each eviction is recorded with the
	//    matched rule version.
	fmt.Println("== slow subscriber, KeepNewest ==")
	slowNew, err := r.Subscribe("slow-new", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(2), router.WithOverflowPolicy(router.KeepNewest))
	if err != nil {
		panic(err)
	}
	for _, id := range []string{"f", "g", "h", "i", "j"} {
		_ = r.Publish(mustMsg(id, "t", nil))
	}
	// Unsubscribe while messages are still buffered: the retained backlog
	// is fully drained before the channel closes.
	_ = r.Unsubscribe("slow-new")
	got = consume("slow-new", slowNew)
	fmt.Printf("  slow retained %v (exactly the newest 2: i, j)\n\n", ids(got))

	// 3b) Switch the overflow policy while the subscription is running. The
	//     backlog is kept; only new deliveries observe the new policy.
	fmt.Println("== runtime overflow policy switch ==")
	switcher, err := r.Subscribe("switcher", []router.Rule{{ID: "t", Topic: "sw"}},
		router.WithSubscriptionBufferSize(2))
	if err != nil {
		panic(err)
	}
	for _, id := range []string{"s1", "s2", "s3"} { // KeepOldest: s3 dropped
		_ = r.Publish(mustMsg(id, "sw", nil))
	}
	if err := r.SetOverflowPolicy("switcher", router.KeepNewest); err != nil {
		panic(err)
	}
	for _, id := range []string{"s4", "s5"} { // KeepNewest: evicts s1, s2
		_ = r.Publish(mustMsg(id, "sw", nil))
	}
	_ = r.Unsubscribe("switcher")
	got = consume("switcher", switcher)
	fmt.Printf("  retained %v after switching to keep_newest\n\n", ids(got))

	// 4) Audit trail: every drop/eviction states the reason and the rule
	//    version the message had matched.
	fmt.Println("== drop / eviction records ==")
	for _, rec := range r.Records() {
		if rec.Kind != router.RecordDropped {
			continue
		}
		fmt.Printf("  %s sub=%s msg=%s code=%s ruleVersion=%d\n",
			rec.Kind, rec.SubscriptionID, rec.MessageID, rec.Code, rec.RuleVersion)
	}
}

func ids(deliveries []router.Delivery) []string {
	out := make([]string, len(deliveries))
	for index, delivery := range deliveries {
		out[index] = delivery.Message.ID()
	}
	return out
}
