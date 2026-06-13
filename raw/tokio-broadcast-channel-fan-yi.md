# Tokio broadcast::channel 文档翻译

```rust
/// 创建一个有界的、多生产者多消费者的通道，其中每个发送的值
/// 都会广播给所有活跃的接收者。
///
/// 在 [`Sender`] 上发送的所有数据将以相同的发送顺序
/// 在每个活跃的 [`Receiver`] 上可用。
///
/// `Sender` 可以被克隆，以便从进程中的多个位置向同一通道 `send`，
/// 或者可以通过 `Arc` 并发使用。新的 `Receiver` 句柄通过调用
/// [`Sender::subscribe`] 创建。
///
/// 如果所有 [`Receiver`] 句柄都被丢弃，`send` 方法将返回
/// [`SendError`]。类似地，如果所有 [`Sender`] 句柄都被丢弃，
/// [`recv`] 方法将返回 [`RecvError`]。
///
/// # 示例
///
/// ```
/// use tokio::sync::broadcast;
///
/// #[tokio::main]
/// async fn main() {
///     let (tx, mut rx1) = broadcast::channel(16);
///     let mut rx2 = tx.subscribe();
///
///     tokio::spawn(async move {
///         assert_eq!(rx1.recv().await.unwrap(), 10);
///         assert_eq!(rx1.recv().await.unwrap(), 20);
///     });
///
///     tokio::spawn(async move {
///         assert_eq!(rx2.recv().await.unwrap(), 10);
///         assert_eq!(rx2.recv().await.unwrap(), 20);
///     });
///
///     tx.send(10).unwrap();
///     tx.send(20).unwrap();
/// }
/// ```
///
/// # Panics
///
/// 如果 `capacity` 等于 `0` 或大于 `usize::MAX / 2`，此函数将 panic。
```