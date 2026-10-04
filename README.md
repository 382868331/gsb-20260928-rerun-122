# 消息路由框架（message-router）

一个本地进程内的消息路由库：发布者按主题与属性发布不可变消息，订阅者用多条规则匹配并从 Go channel 消费。发布永不阻塞，慢订阅者不会影响其他订阅者。订阅者可以暂停接收并在之后接续；取消订阅是硬边界——取消返回后不再交付任何消息，未接收的积压被明确放弃并记入审计。

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

_ = router.Unsubscribe("alerts") // 取消是硬边界：未接收的积压被放弃并记录
for d := range sub.Deliveries {  // 只能读到取消前已交付的消息，channel 随即关闭
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

## 取消语义（硬边界）

`Unsubscribe(id)` 的保证：

1. 订阅者立即从路由表移除，之后的发布不会再投递给它；
2. 等待当前正在进行的投递（`Publish` 持有快照后的那一步）完成，因此取消与发布并发时不会有 goroutine 向已关闭 channel 发送；
3. **取消返回后不再向该订阅交付任何消息**：所有尚未被消费者接收的消息（缓冲积压与交接中的那一条）在取消边界被放弃，逐条记入审计（`Code = "subscription_cancelled"`），channel 随即关闭；
4. **已经交给接收者的消息**归接收者所有，不会被回收或重复计数——取消只放弃“未接收”的部分；
5. 其他订阅的进度完全不受影响；同名重新订阅是全新订阅（规则版本回到 1），旧订阅的剩余消息绝不会迁移到新订阅。

重复取消或取消不存在的订阅返回 `ErrSubscriptionGone`。

## 暂停与恢复

`Pause(id)` 让订阅者暂停接收：pump 停止向消费者交接，但发布照常进行，匹配到的消息仍进入该订阅的有界缓冲，**保留范围由该订阅自己的约定决定**（`KeepOldest` / `KeepNewest` 与各自的缓冲大小）。暂停期间发布者从不被拖住，其他订阅者不受影响。

`Resume(id)` 恢复交付，并返回 `ResumeReport` 先说明结果：

- `Replayable`：仍可补发的消息 ID（按即将交付的顺序），恢复后它们就在原 channel 上按序到达；
- `Evicted`：暂停期间被保留策略淘汰的消息 ID，即明确的缺口。

暂停、取消与 `SetOverflowPolicy` 可以任意交错：暂停中切换保留策略只影响后续投递；暂停中取消则积压按取消语义放弃；已暂停的订阅不能重复暂停（`ErrSubscriptionPaused`），未暂停的订阅不能恢复（`ErrSubscriptionNotPaused`），目标不存在时两者都返回 `ErrSubscriptionGone`。

## 可恢复的订阅状态

`WithStateFile(path)` 让订阅状态可跨进程恢复。路由器在每次状态变化时把每个活跃订阅的规则、规则版本、溢出策略、暂停标记、**未接收**积压与暂停期间产生的缺口原子地写入状态文件（临时文件 + 重命名，崩溃不会写坏上一份状态）。用同一状态文件创建新 `Router` 即完成恢复：

- `RecoveryReports()` 逐个说明恢复出的订阅：`Replayable` 是仍可补发的消息 ID（按交付顺序），`Gaps` 是已因保留约定淘汰的消息，每个缺口都保留**产生时**的原因码、文字说明、命中的规则与规则版本、以及当时的溢出策略——暂停期间切换保留约定不会改写历史缺口。
- 已暂停的订阅恢复后仍保持暂停，`Resume` 返回的报告与恢复报告一致，补发在原有 channel 语义上进行；用 `Subscription(id)` 获取恢复订阅的消费 channel。
- **已交给接收者的消息不会被重放**：交接完成后立即从持久化积压中移除，重开后只补发仍在队列中的消息。
- 取消是跨重启的硬边界：`Unsubscribe` 返回后订阅即从状态文件移除，重开不会复活它；同名重新订阅是全新订阅（规则版本回到 1），不继承旧积压。
- 恢复出的订阅保留原规则版本与溢出策略，后续发布按恢复后的状态继续匹配。

## 审计记录

`Records()` 返回环形审计历史（容量由 `WithHistorySize` 控制，默认 128），每条记录包含：

- `Kind`：`rejected`（非法消息/规则/订阅）或 `dropped`（因慢消费者丢弃或淘汰）；
- `Code` / `Reason`：机器可读原因码与文字说明，例如 `subscriber_buffer_full`、`subscriber_buffer_evicted`、`subscription_cancelled`（取消边界放弃的未接收消息）；
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
| `Publish(msg)` | 非阻塞分发；慢订阅者按溢出策略丢弃并记录 |
| `Unsubscribe(id)` | 硬边界取消：未接收的积压被放弃并记录，返回后不再交付 |
| `Pause(id)` | 暂停接收；发布照常，保留范围由该订阅自己的溢出策略决定 |
| `Resume(id)` | 恢复交付，返回 `ResumeReport` 说明可补发内容与已淘汰缺口 |
| `WithStateFile(path)` | 持久化订阅状态，重开后按原进度恢复 |
| `RecoveryReports()` | 启动时恢复出的订阅报告：可补发内容与已淘汰缺口（含原因与规则） |
| `Subscription(id)` | 获取活跃（含恢复出的）订阅的消费 channel |
| `Records()` | 读取拒绝/丢弃/淘汰审计记录 |
| `WithOverflowPolicy` | `KeepOldest`（默认）或 `KeepNewest` |
