//! Redis 服务器和客户端的一个极简（即, 非常不完整）实现。
//!
//! 本项目的目的是提供一个基于 Tokio 构建的异步 Rust 项目的较大示例。
//! 请勿在生产环境中运行……说真的。
//!
//! # 布局
//!
//! 库的结构设计为可以与指南配合使用。有些模块被设为公开，
//! 在"真正的"redis 客户端库中可能不会公开。
//!
//! 主要组件包括:
//!
//! * `server`: Redis 服务器实现。包含一个 `run` 函数，
//!   它接收 `TcpListener` 并开始接受 Redis 客户端连接。
//!
//! * `clients/client`: 一个异步 Redis 客户端实现。演示了如何使用 Tokio 构建客户端。
//!
//! * `cmd`: 支持的 Redis 命令的实现。
//!
//! * `frame`: 表示单个 Redis 协议帧。帧被用作
//!    "命令"和字节表示之间的中间表示。

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

/// Redis 服务器监听的默认端口。
///
/// 如果没有指定端口则使用此值。
pub const DEFAULT_PORT: u16 = 6379;

/// 大多数函数返回的错误类型。
///
/// 在编写实际应用时，可能会考虑使用专门的错误处理库
/// 或将错误类型定义为原因的 `enum`。
/// 然而，对于我们的示例，使用 boxed 的 `std::error::Error` 就足够了。
///
/// 出于性能考虑，在热点路径中避免使用 box 技术。例如，
/// 在 `parse` 中，定义了一个自定义的错误 `enum`。这是因为当在
/// socket 上接收到部分帧时，该错误在正常执行期间会被命中并处理。
/// `std::error::Error` 为 `parse::Error` 实现了，
/// 使其可以转换为 `Box<dyn std::error::Error>`。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// mini-redis 操作的专用 `Result` 类型。
///
/// 作为便利定义。
pub type Result<T> = std::result::Result<T, Error>;