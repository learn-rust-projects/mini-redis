# RESP 协议实现

RESP（REdis Serialization Protocol）是 Redis 客户端与服务器之间的通信协议。mini-redis 实现了 RESP 的帧编解码层，拆分为 **帧定义**（`frame.rs`）和 **帧传输**（`connection.rs`）两层。

## Frame — 协议数据类型

[`src/frame.rs:13`](../src/frame.rs#L13)

```rust
pub enum Frame {
    Simple(String),    // +OK\r\n
    Error(String),     // -ERR msg\r\n
    Integer(u64),      // :1\r\n
    Bulk(Bytes),       // $5\r\nhello\r\n
    Null,              // $-1\r\n
    Array(Vec<Frame>), // *2\r\n$3\r\nget\r\n$3\r\nkey\r\n
}
```

### 编解码流程

**解码（读取）** 分两步：

1. `Frame::check(&mut Cursor)` — 检查数据是否足够解析一个完整帧（轻量，不分配）
2. `Frame::parse(&mut Cursor)` — 消耗数据构造 `Frame` 实例

两步分离的设计允许 `Connection` 快速判断"数据是否够"，避免不必要的分配。

**编码（写入）** — `Connection::write_value()` 根据帧类型写入对应的前缀字节、内容、`\r\n`。

### 各类型格式

| 类型 | 格式 | 示例 |
|------|------|------|
| Simple String | `+<string>\r\n` | `+OK\r\n` |
| Error | `-<string>\r\n` | `-ERR unknown command\r\n` |
| Integer | `:<number>\r\n` | `:1\r\n` |
| Bulk String | `$<len>\r\n<data>\r\n` | `$5\r\nhello\r\n` |
| Null | `$-1\r\n` | `$-1\r\n` |
| Array | `*<len>\r\n...entries...` | `*2\r\n$3\r\nget\r\n$3\r\nkey\r\n` |

## Connection — 帧传输层

[`src/connection.rs`](../src/connection.rs) — 基于 `TcpStream` 的帧读写。

```rust
pub struct Connection {
    stream: BufWriter<TcpStream>,  // 带写缓冲
    buffer: BytesMut,             // 4KB 读缓冲
}
```

### 读帧 — `read_frame()`

```
loop:
    1. parse_frame() 尝试从 buffer 解析帧
    2. 若成功，返回 Frame，advance buffer
    3. 若 Incomplete，从 socket 读更多数据到 buffer
    4. 读到 0 字节 → 连接关闭，检查 buffer 是否为空
```

`parse_frame()` 内部调用 `Frame::check()` 做快速长度检测，再调用 `Frame::parse()` 做完整解析。

### 写帧 — `write_frame()`

```
对于 Array → 写 *<len>\r\n，递归写每个元素
对于其他类型 → 调用 write_value()
最后 flush() 确保数据发送
```

写端使用 `BufWriter` 缓冲，减少系统调用。

### 关键设计决策

- **读缓冲复用**：`BytesMut` 作为读缓冲，已解析的数据通过 `advance()` 丢弃，减少内存分配
- **Cursor 机制**：解析时使用 `Cursor<&[u8]>` 跟踪解析位置，不修改原始 buffer
- **非递归写数组**：`write_value` 中对 `Frame::Array` 标记 `unreachable!()`，由外层 `write_frame` 迭代处理，避免异步递归

## 相关文档

- [服务器架构](server.md) — Connection 在 Handler 中的使用
- [命令系统](commands.md) — Frame → Command 的转换
- [客户端](client.md) — 客户端侧的帧读写