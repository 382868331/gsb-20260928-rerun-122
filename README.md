# 消息路由框架（message-router）

一个本地进程内的消息路由库：发布者按主题与属性发布不可变消息，订阅者用多条规则匹配并从 Go channel 消费。发布永不阻塞，慢订阅者不会影响其他订阅者。取消订阅有明确的生效边界：Unsubscribe 返回后不再向该订阅交付任何消息；订阅者也可以暂停接收、之后恢复，恢复时先拿到一份“可补发内容 + 已淘汰缺口”的报告。

## 快速开始

```go
router, _ := router.NewRouter()

sub, _ := router.Subscribe("alerts", []router.Rule{
    {ID: "all-alerts", Topic: "alerts"},
    {ID: "critical", Topic: "alerts", Attrs: map[string]string{"severity": "critical"}},
})

msg, _ := router.NewMessage("m1", "alerts", map[string]string{"severity": "critical"})
_ = router.Publish(msg)

d := <-sub.Deliveries // 必须真正消费
fmt.Println(d.Message.ID(), d.RuleIDs, d.RuleVersion)

_ = router.Unsubscribe("alerts") // 返回时 channel 已关闭，不会再有任何交付
_, open := <-sub.Deliveries      // open == false
```

可直接运行的完整示例见 `examples/demo/main.go`：

```sh
go run ./examples/demo
```

## 匹配规则

- 规则在主题相等、且规则中的每个属性在消息上同值存在时命中；规则没有属性时只匹配主题。
- 一次发布对每个订阅者只取一份原子的规则快照，因此一条消息要么完全按某版规则匹配，要么完全按另一版，不会出现“半套规则”。
- 一个订阅者可同时命中多条规则，命中结果按规则 ID 排序后随交付一起给出（`Delivery.RuleIDs`）。
- 消息和规则在构造、订阅时都会复制属性 map，外部后续修改不会影响路由器内部状态。

## 规则版本

每个订阅者持有单调递增的规则版本号：

- `Subscribe` 时的初始规则集为 **版本 1**。
- 每次成功的 `UpdateRules` 使版本加 1；失败的更新（非法规则、订阅已不存在）**不会**推进版本，匹配仍按上一版规则进行。
- 每条交付携带 `Delivery.RuleVersion`，明确指出该消息究竟按哪一版规则匹配——即使发布与 `UpdateRules` 并发执行。

## 慢订阅者与溢出策略

每个订阅者有独立的有界缓冲（默认 16，可用 `WithSubscriptionBufferSize(n)` 覆盖；`WithBufferSize` 设置路由器默认值）。缓冲满时只影响该订阅者，其他订阅者照常接收，`Publish` 也绝不等待慢消费者。通过 `WithOverflowPolicy` 选择满时行为：

- `KeepOldest`（默认）：保留缓冲中的旧消息，**丢弃新来的消息**，记录 `Code = "subscriber_buffer_full"`。
- `KeepNewest`：淘汰缓冲中最旧的消息为新消息腾位，始终保留最新的一批，被淘汰消息记录 `Code = "subscriber_buffer_evicted"`。

保留策略统一作用于**所有尚未被消费者接收的消息**——包括正在交接（handoff）中的那一条。`KeepNewest` 下缓冲满时淘汰最旧的未接收消息（交接中的消息若最旧也会被放弃），因此消费者看到的恰好是“最新的 N 条”，顺序保持先进先出。若淘汰与消费者的接收恰好并发竞争，以实际结果为准：已被消费者接收的消息不会再被记为淘汰。

两种策略只作用于选择它的订阅者，快速订阅者永远不受影响。

## 运行中切换溢出策略

`SetOverflowPolicy(id, policy)` 在订阅运行期间切换溢出策略：

- 新策略对调用返回之后的每次投递立即生效；正在进行的并发投递可能仍按旧策略执行。
- 切换本身**不改写积压**：已接收进缓冲（含交接中）的消息全部保留并按序交付。切到 `KeepNewest` 后，新的溢出会淘汰最旧的未接收消息——那可能正是切换前积压里的消息；切到 `KeepOldest` 后，缓冲满时新到消息被丢弃。
- 切换只影响目标订阅者；取消订阅后再调用返回 `ErrSubscriptionGone`，取消后迟到的发送也不会再交付。
- 非法策略或空订阅 ID 会被拒绝并记入审计（`invalid_overflow_policy` / `invalid_subscription`），原策略保持不变。

## 取消的生效边界

`Unsubscribe(id)` 返回后，不再向该订阅交付任何消息：

1. 订阅者立即从路由表移除，之后的发布不会再投递给它；
2. 等待当前正在进行的投递（`Publish` 持有快照后的那一步）完成，因此取消与发布并发时不会有 goroutine 向已关闭 channel 发送；
3. **已经交给接收者的消息**（消费者已从 channel 收到的）归消费者所有，路由器绝不回收或重投；
4. **尚未被接收的消息**（缓冲中及正在交接中的）在取消边界处被丢弃，逐条记入审计（`Code = "subscription_cancelled"`）；
5. channel 在 `Unsubscribe` 返回前已经关闭——返回后再读只会得到“已关闭”。

重复取消或取消不存在的订阅返回 `ErrSubscriptionGone`。取消只影响目标订阅者，其他订阅的进度不受任何影响。之后用同一 ID 重新 `Subscribe` 会得到一个全新的订阅：旧订阅的剩余消息**不会**被搬到新订阅，规则版本也从 1 重新开始。进程重启（新建 `Router`）同理：路由器不持久化任何状态，同名订阅从空开始。

## 暂停与恢复

`Pause(id)` 让订阅者暂停接收而不取消订阅；`Resume(id)` 恢复交付：

- 暂停期间消费者收不到任何消息；匹配到的消息仍按**该订阅自己的保留约定**（缓冲大小 + 溢出策略）进入有界缓冲，`Publish` 绝不因暂停的订阅者而阻塞，其他订阅者照常接收。
- 暂停期间被保留策略淘汰或丢弃的消息会被逐条记下。
- `Resume` 在恢复交付**之前**返回 `ResumeReport`：`Redeliverable` 是仍被保留、即将按序补发的消息 ID 列表；`Gaps` 是暂停期间被丢弃的消息（含丢弃原因码、命中规则与规则版本），这些消息永远不会再补发给该订阅。
- `Resume` 返回后，保留的积压按原顺序补发，随后恢复正常交付。
- 暂停中可以切换溢出策略（`SetOverflowPolicy`）：只影响之后到达的消息如何取舍，不改写已保留的积压。
- 暂停中取消订阅：按取消边界处理，保留的积压被丢弃并记录，不会留给后来的同名订阅。
- 重复暂停返回 `ErrSubscriptionPaused`；恢复未暂停的订阅返回 `ErrSubscriptionNotPaused`；两者对已取消的订阅都返回 `ErrSubscriptionGone`。

## 审计记录

`Records()` 返回环形审计历史（容量由 `WithHistorySize` 控制，默认 128），每条记录包含：

- `Kind`：`rejected`（非法消息/规则/订阅）或 `dropped`（因慢消费者丢弃或淘汰）；
- `Code` / `Reason`：机器可读原因码与文字说明，例如 `subscriber_buffer_full`、`subscriber_buffer_evicted`、`subscription_cancelled`；
- `MessageID`、`Topic`、`SubscriptionID`：定位具体消息与订阅者；
- `RuleIDs`、`RuleVersion`：该消息当时命中的规则与规则版本，丢弃与淘汰记录同样携带，方便在并发换规则后还原“它本来会按哪版规则交给谁”。

## API 概览

| 符号 | 说明 |
| --- | --- |
| `NewMessage(id, topic, attrs)` | 构造不可变消息，属性会被复制 |
| `NewRouter(opts...)` | 创建路由器（`WithBufferSize`、`WithHistorySize`） |
| `Subscribe(id, rules, opts...)` | 订阅，规则版本从 1 开始；返回消费 channel |
| `UpdateRules(id, rules)` | 原子替换规则集，成功后版本加 1 |
| `SetOverflowPolicy(id, policy)` | 运行中切换溢出策略；积压保留，新策略对后续投递生效 |
| `Pause(id)` | 暂停交付；消息仍按该订阅自己的保留约定进入缓冲 |
| `Resume(id)` | 恢复交付；先返回可补发列表与已淘汰缺口的 `ResumeReport` |
| `Publish(msg)` | 非阻塞分发；慢订阅者按溢出策略丢弃并记录 |
| `Unsubscribe(id)` | 取消边界：返回后不再交付；未接收的积压丢弃并记录，channel 已关闭 |
| `Records()` | 读取拒绝/丢弃/淘汰审计记录 |
| `WithOverflowPolicy` | `KeepOldest`（默认）或 `KeepNewest` |
