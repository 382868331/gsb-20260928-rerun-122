# 消息路由框架（message-router）

一个本地进程内的消息路由库：发布者按主题与属性发布不可变消息，订阅者用多条规则匹配并从 Go channel 消费。发布永不阻塞，慢订阅者不会影响其他订阅者，取消订阅时已接收的积压消息会排空（drain）后再关闭 channel。

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

_ = router.Unsubscribe("alerts")
for d := range sub.Deliveries { // 取消后排空积压，channel 随后关闭
    _ = d
}
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

容量为 0 时没有可积压空间：消费者正阻塞等待则立即交接，否则本条立即按 `subscriber_buffer_full` 丢弃；`Publish` 仍然不等待。

- `KeepOldest`（默认）：保留缓冲中的旧消息，**丢弃新来的消息**，记录 `Code = "subscriber_buffer_full"`。
- `KeepNewest`：淘汰缓冲中最旧的消息为新消息腾位，始终保留最新的一批，被淘汰消息记录 `Code = "subscriber_buffer_evicted"`。

容量按“消费者尚未接收”的全部消息计算，包括 pump 正在向 channel 交接、但消费者尚未真正接收的那一条。进入交接位置不会把消息移出保留策略：暂停接收时，`KeepNewest` 仍会淘汰它，消费者最终看到且只看到最新 N 条，顺序保持先进先出。交接与消费者接收并发时，只有消费者已经接收才不可撤回；否则淘汰生效。

可在订阅运行中调用 `UpdateOverflowPolicy(id, policy)` 原子切换策略：

- 成功返回后，之后进入该订阅者的消息按新策略处理；已经完成接纳或丢弃的消息不追溯。
- 切换本身不清空积压：现有消息保持原 FIFO 位置。切到 `KeepOldest` 后，满缓冲中的新消息被丢弃；切到 `KeepNewest` 后，下一条新消息到达时淘汰当前最旧的未接收消息。
- 初始策略版本为 1；只有实际切换到不同策略才递增。切换为相同策略是成功 no-op，版本不变；非法策略或订阅已取消返回错误。
- `Delivery.PolicyVersion` 与 `Record.PolicyVersion` 标明每条交付、丢弃、淘汰采用的策略版本。

两种策略只作用于选择它的订阅者，快速订阅者永远不受影响。

## 取消与排空语义

`Unsubscribe(id)` 的保证：

1. 订阅者立即从路由表移除，之后的发布不会再投递给它；
2. 等待当前正在进行的投递（`Publish` 持有快照后的那一步）完成，因此取消与发布并发时不会有 goroutine 向已关闭 channel 发送；
3. **不会**丢弃已经进入有界缓冲的消息：消费者继续按顺序读完全部积压，channel 在最后一条之后关闭（`range` 自然结束）。

重复取消或取消不存在的订阅返回 `ErrSubscriptionGone`。

## 审计记录

`Records()` 返回环形审计历史（容量由 `WithHistorySize` 控制，默认 128），每条记录包含：

- `Kind`：`rejected`（非法消息/规则/订阅）或 `dropped`（因慢消费者丢弃或淘汰）；
- `Code` / `Reason`：机器可读原因码与文字说明，例如 `subscriber_buffer_full`、`subscriber_buffer_evicted`；
- `MessageID`、`Topic`、`SubscriptionID`：定位具体消息与订阅者；
- `RuleIDs`、`RuleVersion`：该消息当时命中的规则与规则版本，丢弃与淘汰记录同样携带，方便在并发换规则后还原“它本来会按哪版规则交给谁”；
- `PolicyVersion`：该消息被接纳、丢弃或淘汰时订阅者采用的溢出策略版本。

## API 概览

| 符号 | 说明 |
| --- | --- |
| `NewMessage(id, topic, attrs)` | 构造不可变消息，属性会被复制 |
| `NewRouter(opts...)` | 创建路由器（`WithBufferSize`、`WithHistorySize`） |
| `Subscribe(id, rules, opts...)` | 订阅，规则版本从 1 开始；返回消费 channel |
| `UpdateRules(id, rules)` | 原子替换规则集，成功后版本加 1 |
| `UpdateOverflowPolicy(id, policy)` | 运行中原子切换该订阅者的溢出策略，实际改变后策略版本加 1 |
| `Publish(msg)` | 非阻塞分发；慢订阅者按溢出策略丢弃并记录 |
| `Unsubscribe(id)` | 停止新投递，排空积压后关闭 channel |
| `Records()` | 读取拒绝/丢弃/淘汰审计记录 |
| `WithOverflowPolicy` | `KeepOldest`（默认）或 `KeepNewest` |
