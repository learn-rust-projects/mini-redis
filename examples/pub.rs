//! 向 Redis 频道发布消息的示例。
//!
//! 一个简单的客户端，连接到 mini-redis 服务器，
//! 并在 `foo` 频道上发布一条消息。
//!
//! 你可以通过以下方式测试:
//!
//!     cargo run --bin mini-redis-server
//!
//! 然后在另一个终端中运行:
//!
//!     cargo run --example sub
//!
//! 然后在另一个终端中运行:
//!
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use mini_redis::{clients::Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // 打开到 mini-redis 地址的连接。
    let mut client = Client::connect("127.0.0.1:6379").await?;

    // 在 foo 频道上发布消息 `bar`。
    client.publish("foo", "bar".into()).await?;

    Ok(())
}