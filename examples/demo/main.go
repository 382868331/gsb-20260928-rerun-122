// Command demo is a standalone, runnable walkthrough of the message routing
// framework. It actually consumes every delivery it produces (it never exits
// right after subscribing) and prints the matched rule version for each one,
// then prints dropped/evicted audit records with their reasons.
//
// Run with:
//
//	go run ./examples/demo
package main

import (
	"fmt"

	routerpkg "message-router"
)

func mustMessage(id, topic string, attrs map[string]string) routerpkg.Message {
	message, err := routerpkg.NewMessage(id, topic, attrs)
	if err != nil {
		panic(err)
	}
	return message
}

func main() {
	demoMultiRuleHitsAndVersioning()
	demoKeepOldestBacklogOnUnsubscribe()
	demoKeepNewestDoesNotAffectFastSubscriber()
}

// demoMultiRuleHitsAndConsecutiveUpdates publishes through three consecutive
// rule editions and consumes all deliveries, showing that each delivery names
// the exact rule version and rule set that matched it.
func demoMultiRuleHitsAndVersioning() {
	fmt.Println("== 多规则同时命中 + 连续更新规则 ==")
	r, err := routerpkg.NewRouter()
	if err != nil {
		panic(err)
	}

	sub, err := r.Subscribe("alerts", []routerpkg.Rule{
		{ID: "all-alerts", Topic: "alerts"},
	})
	if err != nil {
		panic(err)
	}

	// Rule version 2: a critical message now matches two rules at once.
	if err := r.UpdateRules("alerts", []routerpkg.Rule{
		{ID: "all-alerts", Topic: "alerts"},
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
	}); err != nil {
		panic(err)
	}
	_ = r.Publish(mustMessage("m-critical", "alerts", map[string]string{"severity": "critical"}))
	_ = r.Publish(mustMessage("m-info", "alerts", map[string]string{"severity": "info"}))

	// Rule version 3: only critical messages match.
	if err := r.UpdateRules("alerts", []routerpkg.Rule{
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
	}); err != nil {
		panic(err)
	}
	_ = r.Publish(mustMessage("m-critical-2", "alerts", map[string]string{"severity": "critical"}))
	_ = r.Publish(mustMessage("m-info-2", "alerts", map[string]string{"severity": "info"}))

	// Cancel while messages are buffered: queued deliveries are still drained.
	if err := r.Unsubscribe("alerts"); err != nil {
		panic(err)
	}
	consumed := 0
	for delivery := range sub.Deliveries {
		consumed++
		fmt.Printf("  收到 %-13s 规则版本=%d 命中规则=%v\n", delivery.Message.ID(), delivery.RuleVersion, delivery.RuleIDs)
	}
	fmt.Printf("  取消后排空完成，实际消费 %d 条，通道已关闭\n\n", consumed)
}

// demoKeepOldestBacklogOnUnsubscribe fills a slow subscriber's buffer with
// the default keep-oldest policy, cancels with a backlog, then drains it.
func demoKeepOldestBacklogOnUnsubscribe() {
	fmt.Println("== 慢订阅者保留旧消息，取消时仍有积压并排空 ==")
	r, err := routerpkg.NewRouter()
	if err != nil {
		panic(err)
	}
	slow, err := r.Subscribe("slow", []routerpkg.Rule{{ID: "topic", Topic: "t"}},
		routerpkg.WithSubscriptionBufferSize(2))
	if err != nil {
		panic(err)
	}

	for index := 0; index < 5; index++ {
		_ = r.Publish(mustMessage(fmt.Sprintf("old-%d", index), "t", nil))
	}
	if err := r.Unsubscribe("slow"); err != nil {
		panic(err)
	}
	for delivery := range slow.Deliveries {
		fmt.Printf("  排空旧消息 %s（按规则版本 %d 匹配）\n", delivery.Message.ID(), delivery.RuleVersion)
	}
	printDropRecords(r)
}

// demoKeepNewestDoesNotAffectFastSubscriber runs a keep-newest slow
// subscriber next to a fast subscriber that consumes concurrently and proves
// the fast one still receives every message.
func demoKeepNewestDoesNotAffectFastSubscriber() {
	fmt.Println("== 慢订阅者保留新消息，不影响快速订阅者 ==")
	r, err := routerpkg.NewRouter()
	if err != nil {
		panic(err)
	}
	slow, err := r.Subscribe("slow", []routerpkg.Rule{{ID: "topic", Topic: "t"}},
		routerpkg.WithSubscriptionBufferSize(2),
		routerpkg.WithSubscriptionOverflowPolicy(routerpkg.OverflowKeepNewest))
	if err != nil {
		panic(err)
	}
	fast, err := r.Subscribe("fast", []routerpkg.Rule{{ID: "topic", Topic: "t"}},
		routerpkg.WithSubscriptionBufferSize(16))
	if err != nil {
		panic(err)
	}

	fastConsumed := make(chan []routerpkg.Delivery, 1)
	go func() {
		var deliveries []routerpkg.Delivery
		for delivery := range fast.Deliveries {
			deliveries = append(deliveries, delivery)
		}
		fastConsumed <- deliveries
	}()

	for index := 0; index < 6; index++ {
		_ = r.Publish(mustMessage(fmt.Sprintf("new-%d", index), "t", nil))
	}
	_ = r.Unsubscribe("fast")
	fastDeliveries := <-fastConsumed

	_ = r.Unsubscribe("slow")
	var slowDeliveries []routerpkg.Delivery
	for delivery := range slow.Deliveries {
		slowDeliveries = append(slowDeliveries, delivery)
	}

	fmt.Printf("  快速订阅者消费 %d 条（一条不丢）：", len(fastDeliveries))
	for _, delivery := range fastDeliveries {
		fmt.Printf(" %s", delivery.Message.ID())
	}
	fmt.Println()
	fmt.Printf("  慢速订阅者只保留最新 %d 条：", len(slowDeliveries))
	for _, delivery := range slowDeliveries {
		fmt.Printf(" %s", delivery.Message.ID())
	}
	fmt.Println()
	printDropRecords(r)
}

func printDropRecords(r *routerpkg.Router) {
	records := r.Records()
	if len(records) == 0 {
		fmt.Println()
		return
	}
	fmt.Println("  丢弃审计：")
	for _, record := range records {
		switch record.Code {
		case routerpkg.DropReasonOldestEvicted:
			fmt.Printf("    [%s] 订阅=%s 按规则版本=%d 命中=%v 保留新消息 %s，丢弃旧消息 %s\n",
				record.Code, record.SubscriptionID, record.RuleVersion, record.RuleIDs,
				record.MessageID, record.EvictedMessageID)
		case routerpkg.DropReasonBufferFull:
			fmt.Printf("    [%s] 订阅=%s 按规则版本=%d 命中=%v 保留积压旧消息，丢弃新消息 %s\n",
				record.Code, record.SubscriptionID, record.RuleVersion, record.RuleIDs,
				record.MessageID)
		default:
			fmt.Printf("    [%s] %s: %s\n", record.Code, record.SubscriptionID, record.Reason)
		}
	}
	fmt.Println()
}
