use tokio::sync::broadcast;

/// 监听服务器关闭信号。
///
/// 关闭通过 `broadcast::Receiver` 发出信号。只发送一个值。
/// 一旦通过广播通道发送了一个值，服务器应该关闭。
///
/// `Shutdown` 结构体监听信号并跟踪信号是否已被接收。
/// 调用者可以查询是否已收到关闭信号。
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// 如果已收到关闭信号，则为 `true`。
    is_shutdown: bool,

    /// 用于监听关闭的接收端通道。
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 创建一个新的 `Shutdown`，由给定的 `broadcast::Receiver` 支持。
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// 如果已收到关闭信号，返回 `true`。
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 接收关闭通知，必要时等待。
    pub(crate) async fn recv(&mut self) {
        // 如果关闭信号已经收到，则立即返回。
        if self.is_shutdown {
            return;
        }

        // 不会收到"滞后错误"（lag error），因为只发送一个值。
        let _ = self.notify.recv().await;

        // 记住信号已被接收。
        self.is_shutdown = true;
    }
}