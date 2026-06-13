//! 订阅 Redis 频道的示例。
//!
//! 一个简单的客户端，连接到 mini-redis 服务器，
//! 订阅 "foo" 和 "bar" 频道，并等待在这些频道上发布的消息。
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
pub async fn main() -> Result<()> {
    // 打开到 mini-redis 地址的连接。
    let client = Client::connect("127.0.0.1:6379").await?;

    // 订阅频道 foo。
    let mut subscriber = client.subscribe(vec!["foo".into()]).await?;

    // 等待频道 foo 上的消息。
    if let Some(msg) = subscriber.next_message().await? {
        println!(
            "got message from the channel: {}; message = {:?}",
            msg.channel, msg.content
        );
    }

    Ok(())
}