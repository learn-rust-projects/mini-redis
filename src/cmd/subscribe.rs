use crate::cmd::{Parse, ParseError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// 将客户端订阅到一个或多个频道。
///
/// 一旦客户端进入订阅状态，就不应该发出任何其他命令，
/// 除了额外的 SUBSCRIBE、PSUBSCRIBE、UNSUBSCRIBE、PUNSUBSCRIBE、
/// PING 和 QUIT 命令。
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 将客户端从一个或多个频道取消订阅。
///
/// 当未指定频道时，客户端将从所有之前订阅的频道取消订阅。
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// 消息流。流从 `broadcast::Receiver` 接收消息。
/// 我们使用 `stream!` 来创建一个消费消息的 `Stream`。
/// 由于 `stream!` 值不能被命名，我们使用 trait 对象对流进行 box。
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// 创建一个新的 `Subscribe` 命令，监听指定的频道。
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// 从接收到的帧中解析 `Subscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从 socket 接收完毕。
    ///
    /// `SUBSCRIBE` 字符串已被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Subscribe` 值。如果帧格式错误，返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含两个或更多条目的数组帧。
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // `SUBSCRIBE` 字符串已被消费。此时，
        // `parse` 中剩余一个或多个字符串。这些表示
        // 要订阅的频道。
        //
        // 提取第一个字符串。如果没有，帧格式错误，
        // 错误被向上传递。
        let mut channels = vec![parse.next_string()?];

        // 现在，消费帧的其余部分。每个值必须是字符串，
        // 否则帧格式错误。一旦帧中的所有值都被消费，
        // 命令解析完成。
        loop {
            match parse.next_string() {
                // 从 `parse` 消费了一个字符串，将其加入频道列表。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据要解析。
                Err(EndOfStream) => break,
                // 所有其他错误被向上传递，导致连接被终止。
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// 将 `Subscribe` 命令应用到指定的 `Db` 实例。
    ///
    /// 此函数是入口点，包括要订阅的初始频道列表。
    /// 来自客户端的额外 `subscribe` 和 `unsubscribe` 命令
    /// 可能被接收，并且订阅列表相应更新。
    ///
    /// [这里]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // 每个单独的频道订阅使用 `sync::broadcast` 频道处理。
        // 消息然后被分发到所有当前订阅该频道的客户端。
        //
        // 单个客户端可以订阅多个频道，并且可以动态地
        // 添加和移除其订阅集中的频道。为了处理这个，
        // 使用 `StreamMap` 来跟踪活动的订阅。
        // `StreamMap` 在接收消息时合并来自各个广播频道的消息。
        let mut subscriptions = StreamMap::new();

        loop {
            // `self.channels` 用于跟踪要订阅的额外频道。
            // 在执行 `apply` 期间收到新的 `SUBSCRIBE` 命令时，
            // 新的频道被推入此 vec。
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待以下事件之一发生:
            //
            // - 从订阅的频道之一接收消息。
            // - 从客户端接收订阅或取消订阅命令。
            // - 服务器关闭信号。
            select! {
                // 从订阅的频道接收消息。
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // 这发生在远程客户端已断开连接时。
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码要发送到服务器的 `Subscribe` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // 订阅频道。
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // 如果我们在消费消息时滞后了，只需继续。
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // 在此客户端的订阅集中跟踪订阅。
    subscriptions.insert(channel_name.clone(), rx);

    // 使用成功订阅进行响应。
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// 处理在 `Subscribe::apply` 内部收到的命令。在此上下文中，
/// 只允许 subscribe 和 unsubscribe 命令。
///
/// 任何新的订阅被追加到 `subscribe_to` 而不是直接修改 `subscriptions`。
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // 从客户端接收到了一个命令。
    //
    // 在此上下文中，只允许 `SUBSCRIBE` 和 `UNSUBSCRIBE` 命令。
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // `apply` 方法将订阅我们添加到这个 vec 中的频道。
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // 如果未指定频道，这请求从 **所有** 频道取消订阅。
            // 为了实现这个，`unsubscribe.channels` vec
            // 被填充为当前订阅的频道列表。
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// 创建订阅请求的响应。
///
/// 所有这些函数都将 `channel_name` 作为 `String` 而不是 `&str`，
/// 因为 `Bytes::from` 可以重用 `String` 中的分配，而使用 `&str`
/// 则需要复制数据。这允许调用者决定是克隆频道名称还是直接使用。
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建取消订阅请求的响应。
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建一个消息帧，通知客户端在其订阅的频道上有新消息。
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// 创建一个新的 `Unsubscribe` 命令，带有给定的 `channels`。
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// 从接收到的帧中解析 `Unsubscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从 socket 接收完毕。
    ///
    /// `UNSUBSCRIBE` 字符串已被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Unsubscribe` 值。如果帧格式错误，返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含至少一个条目的数组帧。
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // 可能没有列出频道，所以从一个空的 vec 开始。
        let mut channels = vec![];

        // 帧中的每个条目必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消费，命令解析完成。
        loop {
            match parse.next_string() {
                // 从 `parse` 消费了一个字符串，将其
                // 推入要取消订阅的频道列表。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据要解析。
                Err(EndOfStream) => break,
                // 所有其他错误被向上传递，导致连接被终止。
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码要发送到服务器的 `Unsubscribe` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}