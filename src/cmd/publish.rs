use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

/// 向给定的频道发布消息。
///
/// 将消息发送到频道，无需了解个体消费者。
/// 消费者可以订阅频道以接收消息。
///
/// 频道名称与键值命名空间无关。在名为"foo"的频道上发布
/// 与设置"foo"键无关。
#[derive(Debug)]
pub struct Publish {
    /// 应该发布消息的频道名称。
    channel: String,

    /// 要发布的消息。
    message: Bytes,
}

impl Publish {
    /// 创建一个新的 `Publish` 命令，在 `channel` 上发送 `message`。
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// 从接收到的帧中解析 `Publish` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从 socket 接收完毕。
    ///
    /// `PUBLISH` 字符串已被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Publish` 值。如果帧格式错误，返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含三个条目的数组帧。
    ///
    /// ```text
    /// PUBLISH channel message
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // `PUBLISH` 字符串已被消费。从帧中提取 `channel`
        // 和 `message` 值。
        //
        // `channel` 必须是有效的字符串。
        let channel = parse.next_string()?;

        // `message` 是任意字节。
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /// 将 `Publish` 命令应用到指定的 `Db` 实例。
    ///
    /// 响应被写入 `dst`。服务器调用此方法来执行接收到的命令。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 共享状态包含所有活动频道的 `tokio::sync::broadcast::Sender`。
        // 调用 `db.publish` 将消息分派到相应的频道。
        //
        // 返回当前在频道上监听的订阅者数量。这并不意味着
        // `num_subscriber` 个频道都会收到消息。订阅者可能在收到消息之前
        // 断开连接。因此，`num_subscribers` 应仅作为"提示"使用。
        let num_subscribers = db.publish(&self.channel, self.message);

        // 订阅者数量作为发布请求的响应返回。
        let response = Frame::Integer(num_subscribers as u64);

        // 将帧写入客户端。
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码要发送到服务器的 `Publish` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}