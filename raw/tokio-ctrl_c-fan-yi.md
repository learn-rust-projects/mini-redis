# Tokio signal::ctrl_c() 翻译

来自 Tokio 源码。

```rust
/// 当向进程发送"ctrl-c"通知时完成。
///
/// 虽然 Unix 和 Windows 对信号的处理方式非常不同，
/// 但两个平台都支持接收"ctrl-c"上的信号。此函数为接收此通知提供了
/// 可移植的 API。
///
/// 一旦轮询返回的 future，就会注册一个监听器。该 future
/// 将在首次调用 `Future::poll` 或 `.await` **之后**，
/// 第一次收到 `ctrl-c` 时完成。
///
/// # 注意事项
///
/// 在 Unix 平台上，第一次为特定信号类型注册 `Signal` 实例时，
/// 会安装一个操作系统信号处理程序，**在整个进程的生命周期内**，
/// 该处理程序将替换收到该信号时的默认平台行为。
///
/// 例如，Unix 系统默认会在终端收到"CTRL+C"产生的信号时终止进程。
/// 但是，当创建一个监听此信号的 `ctrl_c` 流时，信号到达时
/// 将被转换为流事件，进程将继续执行。
/// **即使此 `Signal` 实例被丢弃，后续的 SIGINT 传递仍将被 Tokio 捕获，
/// 并且默认的平台行为将不会恢复**。
///
/// 因此，应用程序应注意确保在监听特定信号后，
/// 预期的信号行为按预期发生。
///
/// # 示例
///
/// ```rust,no_run
/// use tokio::signal;
///
/// #[tokio::main]
/// async fn main() {
///     println!("等待 ctrl-c");
///
///     signal::ctrl_c().await.expect("监听事件失败");
///
///     println!("收到 ctrl-c 事件");
/// }
/// ```
///
/// 在后台监听:
///
/// ```rust,no_run
/// tokio::spawn(async move {
///     tokio::signal::ctrl_c().await.unwrap();
///     // 你的处理程序放在这里
/// });
/// ```
pub async fn ctrl_c() -> io::Result<()> {
    os_impl::ctrl_c()?.recv().await;
    Ok(())
}
```

几个值得注意的点：

- `#[cfg(unix)]` / `#[cfg(windows)]` — 条件编译，不同平台用不同实现
- `os_impl` — 重命名导入，统一接口名，屏蔽平台差异
- `recv().await` — 阻塞直到收到信号，然后返回 `Ok(())`
- **Caveats 部分很重要**：一旦用 Tokio 捕获了 SIGINT，即使你 drop 了 `Signal` 实例，默认的进程终止行为也不会恢复。这是操作系统级别的信号处理程序替换，影响整个进程生命周期。