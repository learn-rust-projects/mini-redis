//! 极简阻塞式 Redis 客户端实现。
//!
//! 提供阻塞式的 connect 和发出支持的命令的方法。

use bytes::Bytes;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

/// 与 Redis 服务器建立的连接。
///
/// 由单个 `TcpStream` 支持，`BlockingClient` 提供基本的网络客户端
/// 功能（无连接池、重试等）。连接使用 [`connect`](fn@connect) 函数建立。
///
/// 请求使用 `Client` 的各种方法发出。
pub struct BlockingClient {
    /// 异步 `Client` 的内部实现。
    inner: crate::clients::Client,

    /// 用于以阻塞方式在异步客户端上执行操作的 `current_thread` 运行时。
    rt: Runtime,
}

/// 已进入 pub/sub 模式的客户端。
///
/// 一旦客户端订阅了频道，就只能执行 pub/sub 相关的命令。
/// `BlockingClient` 类型被转换为 `BlockingSubscriber` 类型，
/// 以防止调用非 pub/sub 方法。
pub struct BlockingSubscriber {
    /// 异步 `Subscriber` 的内部实现。
    inner: crate::clients::Subscriber,

    /// 用于以阻塞方式在异步 `Subscriber` 上执行操作的 `current_thread` 运行时。
    rt: Runtime,
}

/// `Subscriber::into_iter` 返回的迭代器。
struct SubscriberIterator {
    /// 异步 `Subscriber` 的内部实现。
    inner: crate::clients::Subscriber,

    /// 用于以阻塞方式在异步 `Subscriber` 上执行操作的 `current_thread` 运行时。
    rt: Runtime,
}

impl BlockingClient {
    /// 与位于 `addr` 的 Redis 服务器建立连接。
    ///
    /// `addr` 可以是任何可以异步转换为 `SocketAddr` 的类型。
    /// 这包括 `SocketAddr` 和字符串。`ToSocketAddrs` trait
    /// 是 Tokio 版本，不是 `std` 版本。
    ///
    /// # 示例
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let client = match BlockingClient::connect("localhost:6379") {
    ///     Ok(client) => client,
    ///     Err(_) => panic!("failed to establish connection"),
    /// };
    /// # drop(client);
    /// ```
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let inner = rt.block_on(crate::clients::Client::connect(addr))?;

        Ok(BlockingClient { inner, rt })
    }

    /// 获取键的值。
    ///
    /// 如果键不存在，返回特殊值 `None`。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// let val = client.get("foo").unwrap();
    /// println!("Got = {val:?}");
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    /// 将 `key` 设置为持有给定的 `value`。
    ///
    /// `value` 与 `key` 关联，直到被下一次 `set` 调用覆盖或被移除。
    ///
    /// 如果键已经持有值，则覆盖它。成功执行 SET 操作后，
    /// 与该键关联的任何先前生存时间（TTL）都将被丢弃。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// client.set("foo", "bar".into()).unwrap();
    ///
    /// // 立即获取值能成功。
    /// let val = client.get("foo").await.unwrap().unwrap();
    /// assert_eq!(val, "bar");
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    /// 将 `key` 设置为持有给定的 `value`。该值在 `expiration` 后过期。
    ///
    /// `value` 与 `key` 关联，直到以下情况之一发生:
    /// - 它过期了。
    /// - 它被下一次 `set` 调用覆盖。
    /// - 它被移除了。
    ///
    /// 如果键已经持有值，则覆盖它。成功执行 SET 操作后，
    /// 与该键关联的任何先前生存时间（TTL）都将被丢弃。
    ///
    /// # 示例
    ///
    /// 演示基本用法。这个示例不能 **保证** 总是有效，
    /// 因为它依赖于基于时间的逻辑，并假设客户端和服务器
    /// 保持相对同步的时间。现实世界通常不会如此乐观。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let ttl = Duration::from_millis(500);
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// client.set_expires("foo", "bar".into(), ttl).unwrap();
    ///
    /// // 立即获取值能成功。
    /// let val = client.get("foo").unwrap().unwrap();
    /// assert_eq!(val, "bar");
    ///
    /// // 等待 TTL 过期。
    /// thread::sleep(ttl);
    ///
    /// let val = client.get("foo").unwrap();
    /// assert!(val.is_none());
    /// ```
    pub fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        self.rt
            .block_on(self.inner.set_expires(key, value, expiration))
    }

    /// 向给定的 `channel` 发布 `message`。
    ///
    /// 返回当前在频道上监听的订阅者数量。
    /// 不保证这些订阅者都能收到消息，因为他们可能随时断开连接。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// let val = client.publish("foo", "bar".into()).unwrap();
    /// println!("Got = {val:?}");
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    /// 将客户端订阅到指定的频道。
    ///
    /// 一旦客户端发出 subscribe 命令，就不能再发出任何非 pub/sub 命令。
    /// 该函数消费 `self` 并返回一个 `BlockingSubscriber`。
    ///
    /// `BlockingSubscriber` 值用于接收消息以及管理客户端订阅的频道列表。
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    /// 返回当前订阅的频道集合。
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// 接收在订阅频道上发布的下一条消息，必要时等待。
    ///
    /// `None` 表示订阅已终止。
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    /// 将订阅者转换为 `Iterator`，生成在订阅频道上发布的新消息。
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    /// 订阅一个新频道列表。
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    /// 取消订阅一个新频道列表。
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;

    fn next(&mut self) -> Option<crate::Result<Message>> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}