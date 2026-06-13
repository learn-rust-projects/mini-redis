mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/// 支持的 Redis 命令的枚举。
///
/// 在 `Command` 上调用的方法被委托给具体的命令实现。
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// 从接收到的帧中解析命令。
    ///
    /// `Frame` 必须表示 `mini-redis` 支持的 Redis 命令，
    /// 并且必须是数组变体。
    ///
    /// # 返回值
    ///
    /// 成功时返回命令值，否则返回 `Err`。
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // 帧值被装饰为 `Parse`。`Parse` 提供了一个类似"游标"的 API，
        // 使得解析命令更加容易。
        //
        // 帧值必须是数组变体。任何其他帧变体都会导致返回错误。
        let mut parse = Parse::new(frame)?;

        // 所有 Redis 命令都以命令名称作为字符串开头。
        // 读取名称并转换为小写以进行大小写不敏感匹配。
        let command_name = parse.next_string()?.to_lowercase();

        // 匹配命令名称，将剩余解析委托给具体的命令。
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                // 命令不被识别，返回 Unknown 命令。
                //
                // 这里使用 `return` 来跳过下面的 `finish()` 调用。
                // 由于命令不被识别，`Parse` 实例中很可能
                // 存在未消耗的字段。
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // 检查 `Parse` 值中是否还有剩余未消耗的字段。
        // 如果还有字段剩余，表示意外的帧格式并返回错误。
        parse.finish()?;

        // 命令已成功解析。
        Ok(command)
    }

    /// 将命令应用到指定的 `Db` 实例。
    ///
    /// 响应被写入 `dst`。服务器调用此方法以执行接收到的命令。
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` 不能在这里应用。它只能在 `Subscribe` 命令的
            // 上下文中接收。
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    /// 返回命令名称。
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "publish",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}