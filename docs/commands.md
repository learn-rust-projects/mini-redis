# 命令系统

命令系统分为 **命令分发**（`cmd/mod.rs`）、**参数解析**（`parse.rs`）和 **各命令实现**（`cmd/*.rs`）三部分。

## 命令枚举与分发

[`src/cmd/mod.rs:25`](../src/cmd/mod.rs#L25)

```rust
pub enum Command {
    Get(Get),          Publish(Publish),      Set(Set),
    Subscribe(Subscribe), Unsubscribe(Unsubscribe),
    Ping(Ping),        Unknown(Unknown),
}
```

### from_frame — 帧到命令的转换

[`src/cmd/mod.rs:44`](../src/cmd/mod.rs#L44):

1. `Parse::new(frame)` 将 `Frame::Array` 转为迭代器
2. 读取第一个字符串（命令名），转为小写
3. 按命令名分配到对应的 `parse_frames()`
4. `parse.finish()` 检查是否有未消费的字段

### apply — 命令执行

[`src/cmd/mod.rs:90`](../src/cmd/mod.rs#L90):

```rust
match self {
    Get(cmd) => cmd.apply(db, dst).await,
    Set(cmd) => cmd.apply(db, dst).await,
    Publish(cmd) => cmd.apply(db, dst).await,
    Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
    Ping(cmd) => cmd.apply(dst).await,
    Unknown(cmd) => cmd.apply(dst).await,
    Unsubscribe(_) => Err(...),  // 只能在 subscribe 上下文中出现
}
```

## Parse — 参数解析器

[`src/parse.rs`](../src/parse.rs) — 基于 `vec::IntoIter<Frame>` 的光标式 API：

| 方法 | 用途 |
|------|------|
| `next_string()` | 读取 Simple 或 Bulk 帧为 String |
| `next_bytes()` | 读取为 Bytes |
| `next_int()` | 读取 Integer 帧或解析字符串为整数 |
| `finish()` | 确保所有字段已消费 |

## 各命令实现

### GET — `cmd/get.rs`

- 解析：读取一个字符串 key
- 执行：`db.get(&key)` → Bulk(数据) 或 Null(nil)
- 客户端 `into_frame`：`["get", key]`

### SET — `cmd/set.rs`

- 解析：读取 key, value, 可选 `EX seconds` / `PX milliseconds`
- 执行：`db.set(key, value, expire)` → Simple("OK")
- 客户端 `into_frame`：`["set", key, value, ("px", ms)?]`

### PING — `cmd/ping.rs`

- 解析：可选消息参数
- 执行：无参数 → Simple("PONG")，有参数 → Bulk(消息)

### PUBLISH — `cmd/publish.rs`

- 解析：channel, message
- 执行：`db.publish(&channel, message)` → Integer(订阅者数)

### SUBSCRIBE / UNSUBSCRIBE — `cmd/subscribe.rs`

[详细实现](../src/cmd/subscribe.rs) — 最复杂的命令。

SUBSCRIBE 进入**订阅模式**后，不再执行普通命令，而是：

```
loop:
    1. 对新频道调用 subscribe_to_channel()
       - db.subscribe() 获取 broadcast::Receiver
       - 封装为 async_stream 以处理 Lagged 错误
       - 插入 StreamMap
    2. select!:
       - 从 StreamMap 接收消息 → 写 Frame 给客户端
       - 读客户端新帧 → 处理 subscribe/unsubscribe
       - shutdown 信号 → 退出
```

在订阅模式中，只有 `SUBSCRIBE` 和 `UNSUBSCRIBE` 被允许，其他命令通过 `Unknown` 返回错误。

`Unsubscribe` 的 `apply` 被标记为不支持——它只能在 `Subscribe::apply` 的上下文中通过 `handle_command` 处理。

### Unknown — `cmd/unknown.rs`

返回 `Frame::Error("ERR unknown command '...'")`。

## 命令扩展模式

增加新命令只需：

1. 新建 `cmd/my_command.rs`，定义 `struct MyCommand`
2. 实现 `parse_frames()`、`apply()`、`into_frame()`
3. 在 `cmd/mod.rs` 注册模块，在 `Command` 枚举加变体，更新 `from_frame` 和 `apply`

## 相关文档

- [RESP 协议](protocol.md) — Frame 作为命令的载体
- [数据库引擎](database.md) — 命令操作的实际目标
- [服务器架构](server.md) — Handler 调用 `cmd.apply()` 的上下文