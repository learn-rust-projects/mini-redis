use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// 如果没有提供参数，返回 PONG，否则将参数作为 bulk 字符串返回。
///
/// 此命令通常用于测试连接是否仍然存活，或测量延迟。
#[derive(Debug, Default)]
pub struct Ping {
    /// 可选的要返回的消息。
    msg: Option<Bytes>,
}

impl Ping {
    /// 创建一个新的 `Ping` 命令，带有可选的 `msg`。
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// 从接收到的帧中解析 `Ping` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从 socket 接收完毕。
    ///
    /// `PING` 字符串已被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ping` 值。如果帧格式错误，返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含 `PING` 和可选消息的数组帧。
    ///
    /// ```text
    /// PING [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// 应用 `Ping` 命令并返回消息。
    ///
    /// 响应被写入 `dst`。服务器调用此方法来执行接收到的命令。
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        // 将响应写回客户端。
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码要发送到服务器的 `Ping` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}