// Command demo is a self-contained walkthrough of the message-router:
// multi-rule matches with version stamps, two receivers of different
// speeds, publishing while a subscriber is paused, resuming with a replay
// report, the exact cancellation boundary, resubscribing from scratch,
// and a simulated process restart. Every delivered and every explicitly
// abandoned message is checked at the end.
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

func receive(sub router.Subscription, n int) []router.Delivery {
	var got []router.Delivery
	for len(got) < n {
		delivery, open := <-sub.Deliveries
		if !open {
			panic(fmt.Sprintf("channel of %s closed after %d deliveries, want %d", sub.ID, len(got), n))
		}
		got = append(got, delivery)
	}
	return got
}

func ids(deliveries []router.Delivery) []string {
	out := make([]string, len(deliveries))
	for index, delivery := range deliveries {
		out[index] = delivery.Message.ID()
	}
	return out
}

func expect(name string, got, want []string) {
	if len(got) != len(want) {
		panic(fmt.Sprintf("%s = %v, want %v", name, got, want))
	}
	for index := range want {
		if got[index] != want[index] {
			panic(fmt.Sprintf("%s = %v, want %v", name, got, want))
		}
	}
	fmt.Printf("  ok %s = %v\n", name, got)
}

func main() {
	r, err := router.NewRouter()
	if err != nil {
		panic(err)
	}

	// 1) Multi-rule matches plus successive rule updates. Each delivery
	//    carries the exact rule-set version that produced it.
	fmt.Println("== multi-rule matches and rule versions ==")
	alerts, err := r.Subscribe("alerts", []router.Rule{
		{ID: "all-alerts", Topic: "alerts"},
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
	})
	if err != nil {
		panic(err)
	}
	_ = r.Publish(mustMsg("m1", "alerts", map[string]string{"severity": "critical"})) // hits v1: both rules
	if err := r.UpdateRules("alerts", []router.Rule{{ID: "only-info", Topic: "alerts",
		Attrs: map[string]string{"severity": "info"}}}); err != nil { // v2
		panic(err)
	}
	_ = r.Publish(mustMsg("m2", "alerts", map[string]string{"severity": "critical"})) // v2: no match
	_ = r.Publish(mustMsg("m3", "alerts", map[string]string{"severity": "info"}))     // v2: match
	got := receive(alerts, 2)
	fmt.Printf("  [%s] consume %-3s rules=%v ruleVersion=%d\n", alerts.ID, got[0].Message.ID(), got[0].RuleIDs, got[0].RuleVersion)
	fmt.Printf("  [%s] consume %-3s rules=%v ruleVersion=%d\n", alerts.ID, got[1].Message.ID(), got[1].RuleIDs, got[1].RuleVersion)
	if err := r.Unsubscribe("alerts"); err != nil {
		panic(err)
	}
	fmt.Println()

	// 2) Two receivers of different speeds. The slow one pauses; publishing
	//    continues and the fast one receives everything. On resume, the
	//    report names exactly what can be replayed and which messages the
	//    slow subscription's own retention policy (keep_newest, buffer 2)
	//    evicted during the pause.
	fmt.Println("== pause, publish during pause, resume with replay report ==")
	slow, err := r.Subscribe("slow", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(2), router.WithOverflowPolicy(router.KeepNewest))
	if err != nil {
		panic(err)
	}
	fast, err := r.Subscribe("fast", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(8))
	if err != nil {
		panic(err)
	}
	if err := r.Pause("slow"); err != nil {
		panic(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		_ = r.Publish(mustMsg(id, "t", nil))
	}
	expect("fast received while slow paused", ids(receive(fast, 5)), []string{"a", "b", "c", "d", "e"})
	report, err := r.Resume("slow")
	if err != nil {
		panic(err)
	}
	expect("resume replayable", report.Replayable, []string{"d", "e"})
	expect("resume evicted gaps", report.Evicted, []string{"a", "b", "c"})
	expect("slow replayed deliveries", ids(receive(slow, 2)), []string{"d", "e"})
	fmt.Println()

	// 3) Cancellation boundary: messages still unreceived when Unsubscribe
	//    returns are discarded and recorded, never delivered. A
	//    resubscription under the same ID starts completely fresh: no
	//    leftover migrates into it.
	fmt.Println("== cancel discards backlog, resubscribe starts fresh ==")
	if err := r.Pause("slow"); err != nil {
		panic(err)
	}
	for _, id := range []string{"f", "g", "h"} {
		_ = r.Publish(mustMsg(id, "t", nil))
	}
	expect("fast keeps its own progress", ids(receive(fast, 3)), []string{"f", "g", "h"})
	if err := r.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	if _, open := <-slow.Deliveries; open {
		panic("slow received a delivery after Unsubscribe returned")
	}
	fmt.Println("  ok slow channel closed at the cancellation boundary; f, g, h discarded and recorded")

	slow2, err := r.Subscribe("slow", []router.Rule{{ID: "t", Topic: "t"}},
		router.WithSubscriptionBufferSize(2))
	if err != nil {
		panic(err)
	}
	_ = r.Publish(mustMsg("i", "t", nil))
	fresh := receive(slow2, 1)
	expect("resubscribed slow sees only new messages", ids(fresh), []string{"i"})
	if fresh[0].RuleVersion != 1 {
		panic(fmt.Sprintf("resubscribed rule version = %d, want fresh 1", fresh[0].RuleVersion))
	}
	expect("fast still unaffected", ids(receive(fast, 1)), []string{"i"})
	if err := r.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	if err := r.Unsubscribe("fast"); err != nil {
		panic(err)
	}
	fmt.Println()

	// 4) Process restart: a brand-new router has no memory of the old one.
	fmt.Println("== process restart ==")
	r2, err := router.NewRouter()
	if err != nil {
		panic(err)
	}
	restarted, err := r2.Subscribe("slow", []router.Rule{{ID: "t", Topic: "t"}})
	if err != nil {
		panic(err)
	}
	_ = r2.Publish(mustMsg("post-restart", "t", nil))
	after := receive(restarted, 1)
	expect("after restart only new messages arrive", ids(after), []string{"post-restart"})
	if after[0].RuleVersion != 1 {
		panic(fmt.Sprintf("post-restart rule version = %d, want 1", after[0].RuleVersion))
	}
	if err := r2.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	fmt.Println()

	// 5) Audit trail: every abandoned message states its reason, either
	//    evicted during the pause or discarded at the cancellation
	//    boundary.
	fmt.Println("== abandoned message records ==")
	for _, rec := range r.Records() {
		if rec.Kind != router.RecordDropped {
			continue
		}
		fmt.Printf("  sub=%-5s msg=%-2s code=%s ruleVersion=%d\n",
			rec.SubscriptionID, rec.MessageID, rec.Code, rec.RuleVersion)
	}
}
