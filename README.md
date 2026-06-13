# mini-redis

`mini-redis` 是一个不完整的、符合 Rust 惯用风格的 [Redis](https://redis.io) 客户端和服务器实现，基于 [Tokio](https://tokio.rs) 构建。

本项目的目的是提供一个更大型的 Tokio 应用编写示例。

**免责声明** 请勿在生产环境中使用 mini-redis。本项目旨在作为学习资源，并且省略了 Redis 协议的某些部分，因为实现它们不会引入任何新概念。我们不会因为你需要在项目中使用新功能而添加它们——请改用那些功能完备的替代方案。

## 为什么选择 Redis

本项目的主要目标是教授 Tokio。这需要一个功能范围广泛且注重实现简洁性的项目。Redis，一个内存数据库，提供了广泛的功能，并使用简单的线路协议。广泛的功能允许在"真实世界"的上下文中演示许多 Tokio 模式。

Redis 线路协议文档可以在 [这里](https://redis.io/topics/protocol) 找到。

Redis 提供的命令集可以在 [这里](https://redis.io/commands) 找到。

## 运行

该仓库提供了一个服务器、一个客户端库，以及一些用于与服务器交互的客户端可执行文件。

启动服务器:

```
RUST_LOG=debug cargo run --bin mini-redis-server
```

[`tracing`](https://github.com/tokio-rs/tracing) crate 用于提供结构化日志。你可以将 `debug` 替换为所需的 [日志级别][level]。

[level]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives

然后，在另一个终端窗口中，可以执行各种客户端 [示例](examples)。例如:

```
cargo run --example hello_world
```

此外，还提供了一个 CLI 客户端，用于从终端运行任意命令。在服务器运行的情况下，以下命令有效:

```
cargo run --bin mini-redis-cli set foo bar

cargo run --bin mini-redis-cli get foo
```

## OpenTelemetry

如果你正在运行多个应用实例（例如在开发云服务时通常如此），你需要一种方法将所有追踪数据从主机中提取到集中式存储中。这里有很多选项，例如 Prometheus、Jaeger、DataDog、Honeycomb、AWS X-Ray 等。

我们利用 OpenTelemetry，因为它是一个开放标准，允许在上述所有选项（以及更多选项）中使用单一数据格式。这消除了供应商锁定的风险，因为如果需要，你可以在提供商之间切换。

### AWS X-Ray 示例

要启用向 X-Ray 发送追踪数据，请使用 `otel` 特性:
```
RUST_LOG=debug cargo run --bin mini-redis-server --features otel
```

这将使 `tracing` 切换到使用 `tracing-opentelemetry`。你需要在同一主机上运行一份 AWSOtelCollector 的副本。

出于演示目的，你可以按照 https://github.com/aws-observability/aws-otel-collector/blob/main/docs/developers/docker-demo.md#run-a-single-aws-otel-collector-instance-in-docker 记录的步骤进行设置。

## 支持的命令

`mini-redis` 目前支持以下命令。

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

Redis 线路协议规范可以在 [这里](https://redis.io/topics/protocol) 找到。

目前尚不支持持久化。

## Tokio 模式

该项目演示了许多有用的模式，包括:

### TCP 服务器

[`server.rs`](src/server.rs) 启动一个 TCP 服务器，接受连接，并为每个连接生成一个新任务。它能够优雅地处理 `accept` 错误。

### 客户端库

[`client.rs`](src/clients/client.rs) 展示了如何建模一个异步客户端。各种能力以 `async` 方法的形式公开。

### 跨 socket 共享状态

服务器维护一个可从所有已连接连接访问的 [`Db`] 实例。[`Db`] 实例管理键值状态以及 pub/sub 功能。

[`Db`]: src/db.rs

### 成帧

[`connection.rs`](src/connection.rs) 和 [`frame.rs`](src/frame.rs) 展示了如何以惯用风格实现线路协议。该协议使用一个中间表示——`Frame` 结构体——来建模。`Connection` 接收一个 `TcpStream` 并公开一个发送和接收 `Frame` 值的 API。

### 优雅关闭

服务器实现了优雅关闭。[`tokio::signal`] 用于监听 SIGINT 信号。一旦收到信号，关闭过程开始。服务器停止接受新连接。现有连接被通知优雅地关闭。正在执行的工作完成，然后连接被关闭。

[`tokio::signal`]: https://docs.rs/tokio/*/tokio/signal/

### 并发连接限制

服务器使用 [`Semaphore`] 限制最大并发连接数。一旦达到限制，服务器停止接受新连接，直到现有连接终止。

[`Semaphore`]: https://docs.rs/tokio/*/tokio/sync/struct.Semaphore.html

### 发布/订阅

服务器实现了非平凡的 pub/sub 功能。客户端可以订阅多个频道，并随时更新其订阅。服务器通过为每个频道使用一个[广播通道][broadcast] 并为每个连接使用一个 [`StreamMap`] 来实现此功能。客户端可以向服务器发送订阅命令来更新活动的订阅。

[broadcast]: https://docs.rs/tokio/*/tokio/sync/broadcast/index.html
[`StreamMap`]: https://docs.rs/tokio-stream/*/tokio_stream/struct.StreamMap.html

### 在异步应用中使用 `std::sync::Mutex`

服务器使用 `std::sync::Mutex` **而不是** Tokio 互斥锁来同步对共享状态的访问。更多详情请参见 [`db.rs`](src/db.rs)。

### 测试依赖时间的异步代码

在 [`tests/server.rs`](tests/server.rs) 中，有针对键过期的测试。这些测试依赖于时间的流逝。为了使测试具有确定性，使用 Tokio 的测试工具模拟了时间。

## 贡献

欢迎对 `mini-redis` 做出贡献。请记住，本项目的目标 **不是** 达到与真正 Redis 的功能对等，而是展示使用 Tokio 的异步 Rust 模式。

只有当添加命令或其他功能有助于展示新模式时，才应添加。

贡献应附带针对 Tokio 新手的详细注释。

仅关注澄清和改进注释的贡献非常受欢迎。

## 许可

本项目采用 [MIT 许可](LICENSE)。

### 贡献

除非你明确声明，否则你有意提交以包含在 `mini-redis` 中的任何贡献，均应按照 MIT 许可授权，不附加任何额外条款或条件。