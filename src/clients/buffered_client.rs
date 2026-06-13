use crate::clients::Client;
use crate::Result;

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

// 用于从 `BufferedClient` 句柄传递请求命令的枚举。
#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
}

// 通过通道发送到连接任务的消息类型。
//
// `Command` 是要转发到连接的命令。
//
// `oneshot::Sender` 是一种发送 **单个** 值的通道类型。
// 这里用于将从连接接收到的响应发送回原始请求者。
type Message = (Command, oneshot::Sender<Result<Option<Bytes>>>);

/// 接收通过通道发送的命令并将其转发给客户端。
/// 响应通过 `oneshot` 返回给调用者。
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    // 重复从通道中弹出消息。返回 `None`
    // 表示所有 `BufferedClient` 句柄都已丢弃，通道中
    // 再也不会有消息发送。
    while let Some((cmd, tx)) = rx.recv().await {
        // 命令被转发到连接。
        let response = match cmd {
            Command::Get(key) => client.get(&key).await,
            Command::Set(key, value) => client.set(&key, value).await.map(|_| None),
        };

        // 将响应发送回调用者。
        //
        // 发送消息失败表示 `rx` 端在收到消息之前
        // 就已经丢弃了。这是正常的运行时事件。
        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// 创建一个新的客户端请求缓冲区。
    ///
    /// `Client` 直接在 TCP 连接上执行 Redis 命令。一次只能有
    /// 一个正在进行的请求，并且操作需要 `Client` 句柄的可变访问。
    /// 这阻止了从多个 Tokio 任务使用单个 Redis 连接。
    ///
    /// 处理这类问题的策略是生成一个专用的 Tokio 任务来管理
    /// Redis 连接，并使用"消息传递"来操作连接。
    /// 命令被推入通道。连接任务从通道中弹出命令，
    /// 并将其应用到 Redis 连接。当收到响应时，
    /// 它被转发回原始请求者。
    ///
    /// 返回的 `BufferedClient` 句柄可以在传递给不同任务之前被克隆。
    pub fn buffer(client: Client) -> BufferedClient {
        // 将消息限制设置为硬编码值 32。在实际应用中，
        // 缓冲区大小应该是可配置的，但我们这里不需要这样做。
        let (tx, rx) = channel(32);

        // 生成一个任务来处理连接请求。
        tokio::spawn(async move { run(client, rx).await });

        // 返回 `BufferedClient` 句柄。
        BufferedClient { tx }
    }

    /// 获取键的值。
    ///
    /// 与 `Client::get` 相同，但请求被 **缓冲**，
    /// 直到关联的连接能够发送请求。
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        // 初始化一个新的 `Get` 命令，通过通道发送。
        let get = Command::Get(key.into());

        // 初始化一个新的 oneshot 通道，用于从连接接收响应。
        let (tx, rx) = oneshot::channel();

        // 发送请求。
        self.tx.send((get, tx)).await?;

        // 等待响应。
        match rx.await {
            Ok(res) => res,
            Err(err) => Err(err.into()),
        }
    }

    /// 将 `key` 设置为持有给定的 `value`。
    ///
    /// 与 `Client::set` 相同，但请求被 **缓冲**，
    /// 直到关联的连接能够发送请求。
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        // 初始化一个新的 `Set` 命令，通过通道发送。
        let set = Command::Set(key.into(), value);

        // 初始化一个新的 oneshot 通道，用于从连接接收响应。
        let (tx, rx) = oneshot::channel();

        // 发送请求。
        self.tx.send((set, tx)).await?;

        // 等待响应。
        match rx.await {
            Ok(res) => res.map(|_| ()),
            Err(err) => Err(err.into()),
        }
    }
}