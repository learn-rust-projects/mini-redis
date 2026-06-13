# mini-redis 核心架构

mini-redis 是一个基于 Tokio 异步运行时的 Redis 教学实现，完整实现了 RESP 协议、KV 存储、Pub/Sub 以及客户端/服务器通信。本文档从整体架构出发，梳理各模块的设计与职责。

## 项目结构

```
src/
├── bin/
│   ├── server.rs        # 服务端入口（二进制）
│   └── cli.rs           # 客户端入口（二进制）
├── lib.rs               # 库入口，导出公共类型
├── server.rs            # 服务端连接管理核心
├── db.rs                # KV 存储 + Pub/Sub + TTL 过期
├── frame.rs             # RESP 协议帧定义与编解码
├── connection.rs        # 基于 TcpStream 的帧读写层
├── parse.rs             # 命令参数解析器
├── shutdown.rs          # 优雅关闭信号
├── cmd/
│   ├── mod.rs           # 命令分发
│   ├── get.rs           # GET
│   ├── set.rs           # SET
│   ├── ping.rs          # PING
│   ├── publish.rs       # PUBLISH
│   ├── subscribe.rs     # SUBSCRIBE / UNSUBSCRIBE
│   └── unknown.rs       # 未知命令
└── clients/
    ├── mod.rs           # 客户端导出
    ├── client.rs        # 异步客户端
    ├── blocking_client.rs  # 同步阻塞客户端
    └── buffered_client.rs # 带缓冲的客户端
```

## 核心模块

| 模块 | 职责 | 关键文件 |
|------|------|---------|
| [**服务器骨架**](server.md) | TCP listener、连接接收、并发控制、优雅关闭 | `server.rs`, `bin/server.rs` |
| [**RESP 协议**](protocol.md) | 帧类型定义、序列化/反序列化 | `frame.rs`, `connection.rs` |
| [**数据库引擎**](database.md) | KV 存储、TTL 过期、Pub/Sub 频道 | `db.rs` |
| [**命令系统**](commands.md) | 命令解析、派发、各命令实现 | `cmd/*`, `parse.rs` |
| [**客户端**](client.md) | 异步客户端、阻塞客户端、缓冲客户端 | `clients/*` |
| [**优雅关闭**](shutdown.md) | 信号广播、连接排空、资源清理 | `shutdown.rs` |

## 数据流

```
TCP 字节流
    │
    ▼
┌──────────────┐
│  Connection  │  ──  读/写 RESP Frame
│  (frame.rs + │
│   connection.rs)
└──────┬───────┘
       │ Frame
       ▼
┌──────────────┐
│  Command     │  ──  解析 Frame → 具体命令
│  (cmd/mod.rs)│      执行命令操作 Db
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Db          │  ──  HashMap KV 存储
│  (db.rs)     │      broadcast Pub/Sub
└──────────────┘
```

## 依赖关系

- `lib.rs` 是根模块，公开了 `server`、`frame`、`cmd`、`clients` 等模块
- `server.rs` 依赖 `db`、`connection`、`shutdown`、`cmd`
- `cmd/*` 依赖 `frame`、`connection`、`db`、`parse`
- `clients/*` 依赖 `connection`、`cmd`