# 服务器架构

服务端核心位于 [`src/server.rs`](../src/server.rs)，入口为 `pub async fn run(listener, shutdown)`。设计上分为两层结构：**`Listener`**（全局监听器）和 **`Handler`**（单连接处理器）。

## 启动流程

```rust
// bin/server.rs — 二进制入口
let listener = TcpListener::bind("127.0.0.1:6379").await?;
server::run(listener, signal::ctrl_c()).await;
```

`Clap` 解析 CLI 参数（可选 port），绑定 TCP 监听，传入 `ctrl_c` 作为关闭信号，启动服务端。

## Listener — 全局状态

[`src/server.rs:18`](../src/server.rs#L18) — 持有以下资源：

| 字段 | 类型 | 作用 |
|------|------|------|
| `db_holder` | `DbDropGuard` | 共享数据库句柄，封装 `Arc<Db>`，确保背景任务在 `Listener` 销毁时清理 |
| `listener` | `TcpListener` | TCP 监听器 |
| `limit_connections` | `Arc<Semaphore>` | 最大 250 并发连接，用信号量限制 |
| `notify_shutdown` | `broadcast::Sender<()>` | 广播关闭信号给所有 Handler |
| `shutdown_complete_tx` | `mpsc::Sender<()>` | 用于等待所有 Handler 完成 |

### 连接接收循环

`Listener::run()` — `src/server.rs:216`:

1. 从信号量获取 permit（控制并发数）
2. 调用 `accept()` 接受新连接，含**指数退避重试**（1s → 2s → 4s → ... → 64s 上限）
3. 为每个连接创建 `Handler`，`tokio::spawn` 异步处理
4. permit 随 task 生命周期自动归还到信号量

## Handler — 单连接处理

[`src/server.rs:69`](../src/server.rs#L69) — 每个 TCP 连接对应一个 `Handler`：

```rust
struct Handler {
    db: Db,                    // 共享数据库
    connection: Connection,    // 帧级读写层
    shutdown: Shutdown,        // 关闭信号监听
    _shutdown_complete: mpsc::Sender<()>,  // 生命周期标记
}
```

### 请求循环

`Handler::run()` — `src/server.rs:318`:

```
while !shutdown 且连接存活：
    1. select! 等待 read_frame() 或 shutdown 信号
    2. Frame → Command::from_frame() 解析
    3. cmd.apply(&db, &mut connection, &mut shutdown) 执行
```

每次循环通过 `tokio::select!` 同时等待新帧或关闭信号，确保关闭时能及时响应。

### 并发控制

- **信号量**：`MAX_CONNECTIONS = 250`，超过时 `accept()` 阻塞
- **每连接单任务**：一个 TCP 连接由一个 tokio task 处理，不支持 pipeline
- **错误隔离**：单连接错误只影响该连接，不影响其他 Handler

## 关键设计决策

- **std::sync::Mutex** 而非 tokio::sync::Mutex——临界区无 `.await` 点，且操作极快
- **exponential backoff** 处理 `accept()` 的瞬态错误（如 fd 耗尽），最多重试 6 次
- `server::run` 通过 `tokio::select!` 并行运行监听循环和关闭信号等待

## 相关文档

- [优雅关闭机制](shutdown.md) — shutdown 信号如何传播
- [数据库引擎](database.md) — Handler 中 db 字段的实现
- [命令系统](commands.md) — cmd.apply() 的分发过程