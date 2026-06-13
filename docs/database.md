# 数据库引擎

数据库核心位于 [`src/db.rs`](../src/db.rs)，提供 **KV 存储**、**TTL 过期**和 **Pub/Sub 频道**三大功能。

## 分层结构

### DbDropGuard — 生命周期管理

[`src/db.rs:13`](../src/db.rs#L13) — 封装 `Db`，在 Drop 时通知后台清理任务退出：

```rust
impl Drop for DbDropGuard {
    fn drop(&mut self) { self.db.shutdown_purge_task(); }
}
```

`Listener` 持有 `DbDropGuard`，`Handler` 通过 `db_holder.db()` 获取 `Db` 克隆。

### Db — 共享句柄

[`src/db.rs:32`](../src/db.rs#L32)

```rust
pub(crate) struct Db {
    shared: Arc<Shared>,
}
```

`Clone` 只增加引用计数，所有 `Handler` 共享同一份数据。

### Shared — 实际状态

[`src/db.rs:39`](../src/db.rs#L39) — 使用 `std::sync::Mutex` 保护的内部状态：

```rust
struct Shared {
    state: Mutex<State>,
    background_task: Notify,   // 唤醒 TTL 后台任务
}

struct State {
    entries: HashMap<String, Entry>,
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    expirations: BTreeSet<(Instant, String)>,
    shutdown: bool,
}
```

## KV 存储

### GET

`Db::get()` — 加锁 → `entries.get(key)` → 克隆 `Bytes`（浅克隆，引用计数共享） → 解锁。

若 key 不存在返回 `None`，客户端收到 `Null` 帧。

### SET

`Db::set()` — `src/db.rs:158`:

1. 计算过期时间，判断是否为"最早过期 key"（需通知后台任务）
2. 插入/覆盖 `entries`
3. 清理旧的 expiration 记录
4. 插入新的 expiration 记录
5. 解锁后，按需通知后台任务

## TTL 过期

### 后台清理任务

`purge_expired_tasks()` — `src/db.rs:346`，`Db::new()` 中 `tokio::spawn` 启动：

```
loop:
    1. 尝试清理已过期 key
    2. 若有下一个过期时间 → sleep_until 该时间
    3. 若无待过期 key → notified() 等待
    4. 被 Notify 唤醒后重新检查
```

### 唤醒优化

`set()` 仅在"新 key 的过期时间早于当前最早过期 key"时调用 `background_task.notify_one()`，避免频繁唤醒。

### 过期数据结构

- `BTreeSet<(Instant, String)>` — 按过期时间排序，便于查找下一个要过期的 key
- 删除 entry 时同步移除其 expiration 记录

## Pub/Sub

### SUBSCRIBE

`Db::subscribe()` — `src/db.rs:225`:

- 查找 `pub_sub` HashMap，key 为频道名
- 若频道不存在，创建 `broadcast::channel(1024)`
- 返回 `broadcast::Receiver<Bytes>`

### PUBLISH

`Db::publish()` — `src/db.rs:256`:

- 从 `pub_sub` 获取对应 `broadcast::Sender`
- `tx.send(value)` 返回接收者数量
- 失败（无接收者）返回 0

### 注意事项

- **独立命名空间**：KV 和 Pub/Sub 使用独立的 HashMap，`SET foo` 与 `PUBLISH foo` 互不影响
- **容量限制**：broadcast channel 容量 1024，慢消费者会导致消息丢弃
- **Lagged 处理**：消费者通过 `RecvError::Lagged` 跳过追赶不上的消息（见 `cmd/subscribe.rs`）

## 关键设计决策

- **std::sync::Mutex**：临界区极短（无 `.await`），用 tokio mutex 反而增加开销
- **先解锁再通知**：`set()` 中 `drop(state)` 后再 `background_task.notify_one()`，减少锁竞争
- **shutdown 优雅退出**：`shutdown = true` 时后台任务立即退出，不保留僵尸任务

## 相关文档

- [服务器架构](server.md) — Listener 持有 DbDropGuard，Handler 持有 Db
- [命令系统](commands.md) — GET/SET/PUBLISH/SUBSCRIBE 如何调用 Db 方法
- [优雅关闭](shutdown.md) — 后台任务在 DbDropGuard drop 时被通知退出