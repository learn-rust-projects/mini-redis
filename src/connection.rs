use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 从远程对等端发送和接收 `Frame` 值。
///
/// 实现网络协议时，该协议上的消息通常由几个较小的消息（称为帧）组成。
/// `Connection` 的目的就是在底层 `TcpStream` 上读写帧。
///
/// 为了读取帧，`Connection` 使用内部缓冲区，该缓冲区被填充
/// 直到有足够的字节来创建一个完整的帧。一旦发生这种情况，
/// `Connection` 创建帧并将其返回给调用者。
///
/// 发送帧时，帧首先被编码到写缓冲区中。
/// 然后写缓冲区的内容被写入 socket。
#[derive(Debug)]
pub struct Connection {
    // `TcpStream`。它装饰了 `BufWriter`，提供了写级别的缓冲。
    // Tokio 提供的 `BufWriter` 实现足以满足我们的需求。
    stream: BufWriter<TcpStream>,

    // 用于读取帧的缓冲区。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个新的 `Connection`，由 `socket` 支持。
    /// 读写缓冲区被初始化。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 默认使用 4KB 读缓冲区。对于 mini-redis 的用例，
            // 这没问题。然而，实际应用需要根据具体用例调整这个值。
            // 更大的读缓冲区可能效果更好。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从底层流中读取单个 `Frame` 值。
    ///
    /// 该函数会等待直到获取到足够的数据来解析一个帧。
    /// 帧被解析后读缓冲区中的任何剩余数据会保留给下一次
    /// `read_frame` 调用。
    ///
    /// # 返回值
    ///
    /// 成功时，返回接收到的帧。如果 `TcpStream`
    /// 以不中断半帧的方式关闭，则返回 `None`。
    /// 否则返回错误。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓冲数据中解析一个帧。如果足够的数据
            // 已被缓冲，则返回该帧。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 没有足够的缓冲数据来读取一个帧。尝试
            // 从 socket 读取更多数据。
            //
            // 成功时，返回读取的字节数。`0` 表示"流结束"。
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远程端关闭了连接。为了干净地关闭，
                // 读缓冲区中应该没有数据。如果有，
                // 表示对端在发送帧时关闭了 socket。
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// 尝试从缓冲区解析一个帧。如果缓冲区包含足够的数据，
    /// 则返回该帧并将数据从缓冲区中移除。如果没有足够的数据
    /// 被缓冲，返回 `Ok(None)`。如果缓冲的数据不表示一个
    /// 有效的帧，则返回 `Err`。
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 使用 Cursor 来跟踪缓冲区中的"当前位置"。
        // Cursor 也实现了 `bytes` crate 中的 `Buf` trait，
        // 提供了许多有用的工具来处理字节。
        let mut buf = Cursor::new(&self.buffer[..]);

        // 第一步是检查是否已缓冲足够的数据来解析一个单帧。
        // 这一步通常比完整解析帧要快得多，并且允许我们
        // 跳过分配数据结构来保存帧数据，直到我们知道
        // 完整的帧已经被接收到。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // `check` 函数已将游标推进到帧的末尾。
                // 由于游标在调用 `Frame::check` 之前位置被设置为零，
                // 我们通过检查游标位置来获取帧的长度。
                let len = buf.position() as usize;

                // 在将游标传递给 `Frame::parse` 之前将位置重置为零。
                buf.set_position(0);

                // 从缓冲区解析帧。这将分配必要的数据结构
                // 来表示帧并返回帧值。
                //
                // 如果编码的帧表示无效，则返回错误。
                // 这应该终止 **当前** 连接，
                // 但不影响任何其他连接的客户端。
                let frame = Frame::parse(&mut buf)?;

                // 从读缓冲区中丢弃已解析的数据。
                //
                // 当对读缓冲区调用 `advance` 时，所有数据
                // 一直到 `len` 都被丢弃。具体工作方式的细节
                // 由 `BytesMut` 处理。这通常通过移动内部
                // 游标来实现，但也可能通过重新分配和复制数据来实现。
                self.buffer.advance(len);

                // 将解析后的帧返回给调用者。
                Ok(Some(frame))
            }
            // 读缓冲区中没有足够的数据来解析一个单帧。
            // 我们必须等待从 socket 接收更多数据。
            // 读取 socket 的操作将在此 `match` 之后的语句中执行。
            //
            // 我们不想从这里返回 `Err`，因为这个"错误"是一个
            // 预期的运行时情况。
            Err(Incomplete) => Ok(None),
            // 解析帧时遇到错误。连接现在处于无效状态。
            // 从这里返回 `Err` 将导致连接被关闭。
            Err(e) => Err(e.into()),
        }
    }

    /// 向底层流写入单个 `Frame` 值。
    ///
    /// `Frame` 值使用各种 `write_*` 函数写入 socket。
    /// 直接在 `TcpStream` 上调用这些函数是 **不** 推荐的，
    /// 因为这将导致大量的系统调用。然而，在 *缓冲的*
    /// 写流上调用这些函数是可以的。数据将被写入缓冲区。
    /// 一旦缓冲区满了，它会被刷新到底层 socket。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // 数组通过编码每个条目来编码。所有其他帧类型
        // 被视为字面量。目前，mini-redis 无法编码递归帧结构。
        // 详见下文。
        match frame {
            Frame::Array(val) => {
                // 编码帧类型前缀。对于数组，它是 `*`。
                self.stream.write_u8(b'*').await?;

                // 编码数组的长度。
                self.write_decimal(val.len() as u64).await?;

                // 迭代并编码数组中的每个条目。
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 帧类型是字面量。直接编码该值。
            _ => self.write_value(frame).await?,
        }

        // 确保编码的帧被写入 socket。上面的调用是
        // 针对缓冲流的写入操作。调用 `flush` 将缓冲区中
        // 剩余的内容写入 socket。
        self.stream.flush().await
    }

    /// 向流中写入一个帧字面量。
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 从值内部编码 `Array` 不能使用递归策略。
            // 一般来说，异步函数不支持递归。
            // Mini-redis 目前不需要编码嵌套数组，
            // 所以暂时跳过。
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 向流中写入一个十进制帧。
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // 将值转换为字符串。
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{val}")?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}