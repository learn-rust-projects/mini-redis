# 怎么理解 Listener 的层层封装

`Listener` 的层层封装实际上是在解决一个核心问题：**一个 TCP 服务器在运行时需要管理哪些全局资源**。

来看 `Listener` 的定义（`src/server.rs:18`）：

```rust
struct Listener {
    db_holder: DbDropGuard,          // 层1: 数据
    listener: TcpListener,            // 层2: 网络
    limit_connections: Arc<Semaphore>, // 层3: 并发控制
    notify_shutdown: broadcast::Sender<()>,  // 层4: 关闭信号
    shutdown_complete_tx: mpsc::Sender<()>,  // 层5: 关闭完成追踪
}
```

每一层解决一个独立的问题：

**第1层 — 数据层（`db_holder`）**：整个服务器共享的数据。所有连接的 Handler 都在操作同一个 `Db`。`DbDropGuard` 比裸 `Db` 多了一层生命周期管理——它 drop 时会通知后台过期任务退出。

**第2层 — 网络层（`listener`）**：TCP 监听 socket。职责只有一个：`accept()` 产生新的 `TcpStream`。

**第3层 — 并发控制层（`limit_connections`）**：信号量，限制同时活跃的连接数为 250。每一个 Handler 任务持有一个 permit，任务结束 permit 自动归还。

**第4层 — 关闭信号层（`notify_shutdown`）**：`broadcast::Sender`。当服务器收到 `ctrl_c` 时，通过这个通道一推多收，通知所有 Handler 开始关闭。

**第5层 — 关闭完成追踪层（`shutdown_complete_tx`）**：`mpsc::Sender`。每个 Handler 持有一个克隆，通过计数引用判断所有任务是否已结束。

关键的设计美感在于这些层的**协作方式**——`run()` 函数最终把 Listener 拆解开：

```rust
let Listener { shutdown_complete_tx, notify_shutdown, .. } = server;
drop(notify_shutdown);        // 触发了层4 → 所有 Handler 收到信号
drop(shutdown_complete_tx);   // 断开层5 → recv() 能完成
shutdown_complete_rx.recv().await;  // 等所有 Handler 的 Sender 都 drop
```

这里的精妙之处是：`Listener` 的每个字段在 `run()` 结束后都被**重新利用**（destructure），而不是简单地丢弃。关闭信号的发送者被 drop 来触发广播，完成追踪的发送者被 drop 来让 `recv()` 返回。这个模式的本质是**用变量的生命周期来表达协议**——"drop 这个变量"就是"发送关闭信号"。

简单总结：层层封装不是过度设计，而是把 5 个正交的职责（数据、网络、并发控制、信号发送、信号确认）拆成 5 个字段，每个字段有清晰的类型和生命周期，最后在关闭时利用 Rust 的所有权机制（drop）来编排关闭流程。