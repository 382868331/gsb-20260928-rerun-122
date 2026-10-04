// Command demo walks through the cancellation boundary and pause/resume
// semantics of the message-router with two receivers of different speeds:
//
//   - a fast receiver that consumes every message as it arrives;
//   - a slow receiver that is paused while publishing continues, then
//     resumed (with a redelivery report), cancelled, re-subscribed, and
//     finally re-created after a simulated process restart.
//
// Every published message is checked item by item: it is either actually
// delivered to a receiver or explicitly accounted for as abandoned
// (dropped, evicted, or discarded at cancellation).
package main

import (
	"fmt"
	"sort"
	"time"

	router "message-router"
)

func mustMsg(id string) router.Message {
	msg, err := router.NewMessage(id, "ticks", nil)
	if err != nil {
		panic(err)
	}
	return msg
}

func publishAll(r *router.Router, ids ...string) {
	for _, id := range ids {
		if err := r.Publish(mustMsg(id)); err != nil {
			panic(err)
		}
	}
}

func check(label string, ok bool) {
	status := "ok"
	if !ok {
		status = "MISMATCH"
	}
	fmt.Printf("    [%s] %s\n", status, label)
	if !ok {
		panic(label)
	}
}

func equalSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for index := range as {
		if as[index] != bs[index] {
			return false
		}
	}
	return true
}

// ledger records, for one subscription, which messages were delivered and
// which were explicitly abandoned, then verifies the two sets exactly
// partition everything ever published to it.
type ledger struct {
	delivered []string
	abandoned []string
}

func (l *ledger) verify(name string, published []string) {
	seen := make(map[string]bool)
	for _, id := range append(append([]string(nil), l.delivered...), l.abandoned...) {
		if seen[id] {
			panic(fmt.Sprintf("%s: %s accounted twice", name, id))
		}
		seen[id] = true
	}
	all := append(append([]string(nil), l.delivered...), l.abandoned...)
	check(fmt.Sprintf("%s: delivered %v + abandoned %v == published %v",
		name, l.delivered, l.abandoned, published), equalSet(all, published))
}

// receive reads exactly count deliveries, simulating a slow consumer.
func receive(name string, sub router.Subscription, count int) []string {
	var ids []string
	for len(ids) < count {
		delivery, open := <-sub.Deliveries
		if !open {
			panic(fmt.Sprintf("%s: channel closed after %d deliveries, want %d", name, len(ids), count))
		}
		time.Sleep(5 * time.Millisecond) // slow receiver
		fmt.Printf("    [%s] received %s (rules=%v v%d)\n",
			name, delivery.Message.ID(), delivery.RuleIDs, delivery.RuleVersion)
		ids = append(ids, delivery.Message.ID())
	}
	return ids
}

func gapIDs(report router.ResumeReport) []string {
	ids := make([]string, len(report.Gaps))
	for index, gap := range report.Gaps {
		ids[index] = gap.MessageID
	}
	return ids
}

func main() {
	r, err := router.NewRouter(router.WithHistorySize(256))
	if err != nil {
		panic(err)
	}

	fast, err := r.Subscribe("fast", []router.Rule{{ID: "all", Topic: "ticks"}})
	if err != nil {
		panic(err)
	}
	slow, err := r.Subscribe("slow", []router.Rule{{ID: "all", Topic: "ticks"}},
		router.WithSubscriptionBufferSize(3), router.WithOverflowPolicy(router.KeepNewest))
	if err != nil {
		panic(err)
	}

	fastLedger := &ledger{}
	slowLedger := &ledger{}

	// 1) Pause the slow receiver, then publish t1..t8. The fast receiver
	//    keeps up; the paused slow one retains only what its own buffer
	//    policy allows, and Publish never waits for it.
	fmt.Println("== 1) publish t1..t8 while the slow receiver is paused ==")
	if err := r.Pause("slow"); err != nil {
		panic(err)
	}
	publishAll(r, "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8")
	fastLedger.delivered = append(fastLedger.delivered, receive("fast", fast, 8)...)
	select {
	case d := <-slow.Deliveries:
		panic(fmt.Sprintf("slow received %q while paused", d.Message.ID()))
	case <-time.After(20 * time.Millisecond):
		fmt.Println("    [slow] received nothing while paused")
	}

	// 2) Resume: the report states exactly what can be redelivered and
	//    which messages were evicted during the pause, before the backlog
	//    starts flowing again.
	fmt.Println("== 2) resume: redelivery report, then backlog flows ==")
	report, err := r.Resume("slow")
	if err != nil {
		panic(err)
	}
	fmt.Printf("    redeliverable: %v\n", report.Redeliverable)
	for _, gap := range report.Gaps {
		fmt.Printf("    gap: %s (%s)\n", gap.MessageID, gap.Code)
	}
	check("redeliverable is exactly the retained newest 3",
		equalSet(report.Redeliverable, []string{"t6", "t7", "t8"}))
	check("gaps are exactly the evicted t1..t5",
		equalSet(gapIDs(report), []string{"t1", "t2", "t3", "t4", "t5"}))
	slowLedger.delivered = append(slowLedger.delivered, receive("slow", slow, 3)...)

	// 3) Cancel the slow subscription with t9, t10 still unreceived: the
	//    cancellation boundary discards them, the channel is closed by the
	//    time Unsubscribe returns, and nothing is delivered afterwards.
	fmt.Println("== 3) cancel with an unreceived backlog ==")
	publishAll(r, "t9", "t10")
	fastLedger.delivered = append(fastLedger.delivered, receive("fast", fast, 2)...)
	if err := r.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	if _, open := <-slow.Deliveries; open {
		panic("slow channel still open after Unsubscribe returned")
	}
	fmt.Println("    [slow] channel already closed when Unsubscribe returned")

	// 4) Re-subscribe under the same ID: a fresh subscription that never
	//    inherits the old backlog. t11 is the only message it can get.
	fmt.Println("== 4) re-subscribe under the same ID ==")
	slow2, err := r.Subscribe("slow", []router.Rule{{ID: "all", Topic: "ticks"}},
		router.WithSubscriptionBufferSize(3), router.WithOverflowPolicy(router.KeepNewest))
	if err != nil {
		panic(err)
	}
	publishAll(r, "t11")
	fastLedger.delivered = append(fastLedger.delivered, receive("fast", fast, 1)...)
	slow2Ledger := &ledger{delivered: receive("slow", slow2, 1)}
	check("re-subscribed slow received only t11", equalSet(slow2Ledger.delivered, []string{"t11"}))
	if err := r.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	if err := r.Unsubscribe("fast"); err != nil {
		panic(err)
	}

	// 5) Simulated process restart: a brand-new router has no memory of
	//    the old subscriptions; the same IDs start empty.
	fmt.Println("== 5) process restart: new router, same subscription IDs ==")
	r2, err := router.NewRouter()
	if err != nil {
		panic(err)
	}
	fast2, err := r2.Subscribe("fast", []router.Rule{{ID: "all", Topic: "ticks"}})
	if err != nil {
		panic(err)
	}
	slow3, err := r2.Subscribe("slow", []router.Rule{{ID: "all", Topic: "ticks"}})
	if err != nil {
		panic(err)
	}
	publishAll(r2, "n1", "n2")
	restartFast := receive("fast", fast2, 2)
	restartSlow := receive("slow", slow3, 2)
	check("restarted fast received only n1,n2", equalSet(restartFast, []string{"n1", "n2"}))
	check("restarted slow received only n1,n2", equalSet(restartSlow, []string{"n1", "n2"}))
	if err := r2.Unsubscribe("fast"); err != nil {
		panic(err)
	}
	if err := r2.Unsubscribe("slow"); err != nil {
		panic(err)
	}

	// 6) Item-by-item reconciliation: every message published on the first
	//    router is either delivered or explicitly abandoned, per receiver.
	fmt.Println("== 6) itemized reconciliation ==")
	for _, record := range r.Records() {
		if record.Kind != router.RecordDropped {
			continue
		}
		fmt.Printf("    abandoned: %s sub=%s code=%s\n",
			record.MessageID, record.SubscriptionID, record.Code)
		switch record.SubscriptionID {
		case "slow":
			slowLedger.abandoned = append(slowLedger.abandoned, record.MessageID)
		case "fast":
			fastLedger.abandoned = append(fastLedger.abandoned, record.MessageID)
		}
	}
	fastLedger.verify("fast", []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11"})
	slowLedger.verify("slow (first)", []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"})
	slow2Ledger.verify("slow (re-subscribed)", []string{"t11"})
	fmt.Println("all checks passed")
}
