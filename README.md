# 消息路由框架（message-router）

本地进程内的消息路由库：发布者按主题 + 属性发布消息，订阅者用一组规则声明关心哪些消息。
路由是有界缓冲、非阻塞的：任何慢订阅者都不会卡住发布者，也不会影响其他订阅者。

## 快速上手

```go
package main

import (
	"fmt"

	"message-router"
)

func main() {
	r, _ := router.NewRouter()

	sub, _ := r.Subscribe("alerts", []router.Rule{
		{ID: "all-alerts", Topic: "alerts"},
		{ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
	})

	msg, _ := router.NewMessage("m1", "alerts", map[string]string{"severity": "critical"})
	_ = r.Publish(msg)

	_ = r.Unsubscribe("alerts") // 停止接收新消息，排空后关闭通道
	for delivery := range sub.Deliveries {
		fmt.Println(delivery.Message.ID(), delivery.RuleVersion, delivery.RuleIDs)
	}
}
```

完整、可运行的过程式示例见 `examples/demo`，它会真正消费所有投递结果：

```bash
go run ./examples/demo
```

## 核心 API

- `NewRouter(opts ...Option) (*Router, error)`：创建路由器。
  - `WithBufferSize(n)`：新订阅的默认缓冲容量（默认 16）。
  - `WithHistorySize(n)`：审计记录环形缓冲容量（默认 128，0 表示不记录）。
  - `WithOverflowPolicy(p)`：新订阅的默认溢出策略。
- `NewMessage(id, topic, attrs)`：构造不可变消息；属性表会被复制，外部修改不影响消息。
- `Subscribe(id, rules, opts...) (Subscription, error)`：注册订阅，返回只读投递通道。
  - `WithSubscriptionBufferSize(n)`：本订阅缓冲容量（0 = 无缓冲）。
  - `WithSubscriptionOverflowPolicy(p)`：本订阅溢出策略，不影响其他订阅。
- `Publish(message)`：按各订阅当前规则快照做一次匹配并非阻塞投递，永不等待慢消费者。
- `UpdateRules(subID, rules)`：原子替换某订阅的整组规则，规则版本 +1。
- `Unsubscribe(id)`：停止新投递，等待在途发送结束，然后关闭通道。
- `Records()`：按时间顺序返回拒绝/丢弃审计记录的副本。

## 匹配规则

一条规则在「主题相等」且「规则中的每个属性都以相同值出现在消息上」时命中。
同一条消息可以同时命中一个订阅的多条规则，命中的规则 ID 按字典序放在
`Delivery.RuleIDs` 中。规则列表不能为空，规则 ID 在同一次设置中不能重复。

## 规则版本与并发换规则

每个订阅维护一个单调递增的规则版本：

- `Subscribe` 成功时版本为 **1**；
- 每次 `UpdateRules` 成功后版本 **+1**；非法输入或订阅不存在时更新被整体拒绝，版本不变。

`Publish` 对每个订阅只取一次不可变的规则快照。因此与 `UpdateRules` 并发时，
一条消息要么完全按旧版匹配，要么完全按新版匹配，绝不会出现「半新半旧」的匹配结果。
每条投递与每条丢弃记录都带有 `RuleVersion`，事后可以明确判断这条消息究竟按哪版规则命中：

```go
delivery := <-sub.Deliveries
fmt.Println(delivery.RuleVersion, delivery.RuleIDs) // 例如 3 [critical]
```

## 慢订阅者：保留旧消息或保留新消息

订阅缓冲满时的行为由溢出策略决定，且只影响该订阅自身：

- `OverflowKeepOldest`（默认）：**保留旧消息**，丢弃新来的消息。发布不阻塞。
- `OverflowKeepNewest`：**保留新消息**，挤出缓冲中最旧的一条为新消息腾位。

两种情况下 `Publish` 都不等待，且其他订阅照常全量收到消息（见示例第三段与
`TestKeepNewestSlowSubscriberDoesNotAffectFastSubscriber`）。

## 取消（Unsubscribe）后的排空语义

`Unsubscribe` 的保证可以直接依赖，无需从测试推断：

1. 订阅立即从路由表移除，之后的 `Publish` 不再向它投递；
2. 等待所有已经开始的在途发送完成（含挤出旧消息这类内部操作）；
3. 然后关闭投递通道。关闭前已进入有界缓冲的消息**一条不少**，仍可从通道读出，
   读完后通道才返回关闭；
4. 因为取消与缓冲满而没能进入缓冲的消息不会出现，它们在丢弃审计记录里可查；
5. 重复取消或取消不存在的订阅返回 `ErrSubscriptionGone`。

消费方的标准写法是取消后继续 `range` 通道直到关闭，完成积压排空：

```go
_ = r.Unsubscribe("alerts")
for delivery := range sub.Deliveries { // 先读完积压，再收到 close
	handle(delivery)
}
```

## 丢弃原因与审计记录

`Record` 同时覆盖被拒绝的操作（`RecordRejected`）和被丢弃的投递（`RecordDropped`）。
丢弃原因用 `Record.Code` 区分：

| Code | 触发条件 | MessageID 含义 | EvictedMessageID 含义 |
| --- | --- | --- | --- |
| `subscriber_buffer_full` | 保留旧消息策略下缓冲已满，新消息被丢弃 | 被丢弃的新消息 | 空 |
| `subscriber_oldest_evicted` | 保留新消息策略下挤出最旧消息 | 得以保留的新消息 | 被挤出的旧消息 |

每条丢弃记录还带有 `SubscriptionID`、命中的 `RuleIDs` 和匹配时的 `RuleVersion`，
因此可以同时回答三个问题：丢了谁、为什么丢、按哪版规则判定的。常量
`router.DropReasonBufferFull` 和 `router.DropReasonOldestEvicted` 可直接用于比较。

## 测试

```bash
go test ./...        # 全部功能测试
go test -race ./...  # 含并发换规则/发布竞态检测
```

关键核对用例：

- `TestDeliveriesCarryRuleVersion`：连续三版规则、多规则同时命中时逐条核对版本与命中集合；
- `TestOverflowKeepOldestRetainsBufferedMessages`：保留旧消息时只留最旧的两条且原因可查；
- `TestOverflowKeepNewestEvictsOldest`：逐条核对每次挤出的旧消息 ID、保留的新消息与版本；
- `TestKeepNewestSlowSubscriberDoesNotAffectFastSubscriber`：慢订阅丢弃的同时快订阅全量收到；
- `TestConcurrentRuleUpdatesStampCoherentVersion`：并发换规则下每条投递与所盖版本完全一致；
- `TestUnsubscribeFinishesInFlightAndDeliversQueuedMessages`：取消后积压仍可排空。
