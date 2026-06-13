# 客户端实现

客户端位于 [`src/clients/`](../src/clients/)，提供三种使用模式：**异步 Client**、**阻塞 BlockingClient** 和 **缓冲 BufferedClient**。

## 异步 Client

[`src/clients/client.rs`](../src/clients/client.rs)

```rust
pub struct Client {
    connection: Connection,
}
```

### 连接建立

`Client::connect(addr)`:

```
TcpStream::connect(addr) → Connection::new(socket) → Client
```

### 命令执行模式

每个命令（`get`、`set`、`ping`、`publish`）遵循统一模式：

1. 创建命令对象 `Get::new(key)`
2. 调用 `into_frame()` 转为 RESP Frame
3. `connection.write_frame()` 发送
4. `connection.read_frame()` 等待响应
5. 匹配期望的帧类型，转换返回值

### Subscriber — 订阅模式

调用 `subscribe()` 后 `Client` 消费自身返回 `Subscriber`，确保订阅状态无法执行非 pub/sub 命令：

```rust
pub struct Subscriber {
    client: Client,
    subscribed_channels: Vec<String>,
}
```

提供 `next_message()` 和 `into_stream()` 两个消费模式：

- `next_message()` — 逐条 await 消息，返回 `Option<Message>`
- `into_stream()` — 转换为 `Stream<Item = Result<Message>>`，使用 `async_stream::try_stream!` 宏实现

## BlockingClient

[`src/clients/blocking_client.rs`](../src/clients/blocking_client.rs) — 为同步代码场景提供非异步接口。

```rust
pub struct BlockingClient {
    inner: crate::clients::Client,  // 内部异步客户端
    rt: Runtime,                     // current_thread 运行时
}
```

### 实现方式

每个方法都通过 `rt.block_on(inner.method())` 执行：

```rust
pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
    self.rt.block_on(self.inner.get(key))
}
```

`BlockingSubscriber` 额外提供 `into_iter()` 方法，通过 `Iterator` trait 实现逐条阻塞读取消息。

### 适用场景

- 不需要 async/await 的同步上下文
- 测试或简单的脚本场景

## BufferedClient

[`src/clients/buffered_client.rs`](../src/clients/buffered_client.rs) — 通过消息传递实现跨任务共享连接。

### 问题

`Client` 要求 `&mut self`，无法直接被多个 tokio task 共享。

### 解决方案

Actor 模式——专用 task 持有 `Client`，其他 task 通过 channel 发送命令：

```rust
struct BufferedClient { tx: Sender<Message> }
// Message = (Command, oneshot::Sender<Result>)
```

### 工作流

```
Task A → tx.send((Get("foo"), tx_a)) ─┐
                                      ▼
                              ┌────────────────┐
                              │  run() 后台任务  │ → Client → Server
                              └────────────────┘
Task B → tx.send((Set("bar"), tx_b)) ─┘       │
                                         oneshot 回复
                                              ▼
                                        Task A/B 收到结果
```

`BufferedClient` 实现 `Clone`（仅克隆 `Sender`），可以在多个 task 间自由传递。

### 限制

当前只支持 `GET` 和 `SET` 两个命令的缓冲，是简化版本的教学示例。

## 三种客户端对比

| 特性 | Client | BlockingClient | BufferedClient |
|------|--------|---------------|----------------|
| API | async | 同步 | async |
| 跨任务共享 | ❌ (&mut self) | ❌ | ✅ (Clone) |
| 适用场景 | 异步应用 | 同步代码 | 多 task 共享连接 |
| 支持命令 | 全部 | 全部 | GET + SET |

## 相关文档

- [RESP 协议](protocol.md) — Connection 层的帧读写
- [命令系统](commands.md) — 客户端命令的 into_frame 和响应解析