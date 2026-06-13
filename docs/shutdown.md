# 优雅关闭机制

mini-redis 实现了双层优雅关闭：**信号广播**和**连接排空**。

## Shutdown 结构

[`src/shutdown.rs`](../src/shutdown.rs)

```rust
pub(crate) struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}
```

- `is_shutdown` — 标记是否已收到关闭信号
- `notify` — broadcast receiver，监听服务端关闭

### 方法

- `is_shutdown()` — 检查是否已关闭，用于 `Handler::run` 的 while 条件
- `recv()` — await 关闭信号，幂等（已关闭时立即返回）

## 关闭流程

### 触发

`server::run()` 通过 `tokio::select!` 同时运行监听循环和外部 shutdown future。当 `shutdown`（如 `ctrl_c()`）完成时，进入关闭流程。

### 第一步：广播信号

```rust
// server.rs
let Listener { notify_shutdown, shutdown_complete_tx, .. } = server;
drop(notify_shutdown);       // 触发所有 Handler 的 Shutdown
drop(shutdown_complete_tx);  // 准备等待 Handler 完成
```

`broadcast::Sender` 被 drop 时，所有 `Receiver` 收到 `recv()` 完成，所有 `Handler::run` 中的 `shutdown.recv()` 返回。

### 第二步：连接排空

```rust
shutdown_complete_rx.recv().await;
```

- `Listener` 持有 `shutdown_complete_tx` 的一个 `Sender`
- 每个 `Handler` 持有 `shutdown_complete_tx` 的克隆 `Sender`
- 所有 `Sender` 被 drop 后，`Receiver` 收到 `None`
- 此时所有 Handler 已完成当前请求的处理

### Handler 的关闭响应

在 `Handler::run()` 中：

```
每轮循环：select!
  ├── read_frame() → 继续处理
  └── shutdown.recv() → return Ok(()) 立即退出
```

这意味着：
- 正在等待读帧 → 立即退出
- 正在执行命令 → 执行完成后下一轮循环退出
- 不会中断正在进行的命令处理

## 数据库后台任务的关闭

`DbDropGuard` 的 `Drop` 实现：

```rust
impl Drop for DbDropGuard {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}
```

`shutdown_purge_task` 设置 `shutdown = true` 并通知后台任务，后台任务下次唤醒时检查标记并退出。

## 关闭时序图

```
ctrl_c() 信号
    │
    ▼
Listener.run() 的 select! 触发
    │
    ├── drop(notify_shutdown) → broadcast 关闭
    │       │
    │       ├── Handler 1 shutdown.recv() → return
    │       ├── Handler 2 shutdown.recv() → return
    │       └── ...
    │
    ├── drop(shutdown_complete_tx)
    │
    └── shutdown_complete_rx.recv() → None → 退出进程
         │
         └── 所有 Handler 的 Sender 已 drop
```

## 关键设计决策

- **broadcast channel**：一推多收，适合通知所有连接
- **mpsc channel**：计数所有存活 Handler，等待最后一个完成
- **幂等 Shutdown**：`is_shutdown` 标志确保多次调用安全
- **不乱丢数据**：当前帧处理完成后才退出

## 相关文档

- [服务器架构](server.md) — Listener 持有 notify_shutdown，Handler 持有 Shutdown
- [数据库引擎](database.md) — DbDropGuard 和后台清理任务的关闭