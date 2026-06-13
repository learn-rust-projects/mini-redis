use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// 用于解析命令的工具。
///
/// 命令表示为数组帧。帧中的每个条目是一个"token"。
/// `Parse` 用数组帧初始化，并提供一个类似游标的 API。
/// 每个命令结构体包含一个 `parse_frame` 方法，使用 `Parse` 来提取其字段。
#[derive(Debug)]
pub(crate) struct Parse {
    /// 数组帧迭代器。
    parts: vec::IntoIter<Frame>,
}

/// 解析帧时遇到的错误。
///
/// 只有 `EndOfStream` 错误在运行时被处理。所有其他错误
/// 都会导致连接被终止。
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 尝试提取值失败，因为帧已被完全消费。
    EndOfStream,

    /// 所有其他错误。
    Other(crate::Error),
}

impl Parse {
    /// 创建一个新的 `Parse` 来解析 `frame` 的内容。
    ///
    /// 如果 `frame` 不是数组帧，返回 `Err`。
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {frame:?}").into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// 返回下一个条目。数组帧是帧的数组，因此下一个条目是一个帧。
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 将下一个条目作为字符串返回。
    ///
    /// 如果下一个条目不能表示为 String，则返回错误。
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可能是字符串。
            // 字符串被解析为 UTF-8。
            //
            // 虽然错误存储为字符串，但它们被认为是独立的类型。
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {frame:?}"
            )
            .into()),
        }
    }

    /// 将下一个条目作为原始字节返回。
    ///
    /// 如果下一个条目不能表示为原始字节，则返回错误。
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可能是原始字节。
            //
            // 虽然错误存储为字符串并且可以表示为原始字节，
            // 但它们被认为是独立的类型。
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {frame:?}"
            )
            .into()),
        }
    }

    /// 将下一个条目作为整数返回。
    ///
    /// 这包括 `Simple`、`Bulk` 和 `Integer` 帧类型。
    /// `Simple` 和 `Bulk` 帧类型会被解析。
    ///
    /// 如果下一个条目不能表示为整数，则返回错误。
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // Integer 帧类型已经存储为整数。
            Frame::Integer(v) => Ok(v),
            // Simple 和 bulk 帧必须被解析为整数。如果解析失败，
            // 则返回错误。
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {frame:?}").into()),
        }
    }

    /// 确保数组中没有更多条目。
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}