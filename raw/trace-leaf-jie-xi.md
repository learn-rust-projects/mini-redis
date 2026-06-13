# trace_leaf 解析

```rust
ready!(crate::trace::trace_leaf(cx));
```

## 这行到底干了什么？

分两种情况：**taskdump 功能启用**和**未启用**。

### 未启用时（默认）

```rust
cfg_not_taskdump! {
    #[inline(always)]
    pub(crate) fn trace_leaf(_: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        std::task::Poll::Ready(())
    }
}
```

**什么也不做。** 直接返回 `Ready(())`，`ready!` 宏直接通过。这行相当于不存在，零开销。

### 启用时（cfg_taskdump）

当启用了 Tokio 的 taskdump 功能（`tokio_unstable` + `tokio-taskdump` feature），`trace_leaf` 是一个真正的追踪函数：

```rust
pub(crate) fn trace_leaf(cx: &mut task::Context<'_>) -> Poll<()> {
    let did_trace = unsafe {
        Context::try_with_current(|context_cell| {
            if let Some(mut collector) = context_cell.collector.take() {
                // ... 收集当前任务的调用栈信息 ...
            }
        })
    };
    // ...
}
```

它做的事情是：**把当前执行位置标记为一个"叶子 Future"，并收集从 root 到这里的调用栈帧。**

### taskdump 是什么？

Tokio 的 `taskdump` 是一个诊断工具（类似 `SIGQUIT` 对 Java 的 thread dump），允许你在运行时抓取所有任务的调用栈，用来排查死锁或任务卡住的问题。

`trace_leaf` 标记了 async 调用链中的"叶子节点"——那些真正在等待某个条件（I/O、锁、channel）的 Future。taskdump 时，你可以看到每个任务从入口到叶子节点的完整调用栈。

## 为什么用 ready! 包一层？

```rust
ready!(crate::trace::trace_leaf(cx));
```

`trace_leaf` 返回 `Poll<()>`，`ready!` 展开后：

```rust
match crate::trace::trace_leaf(cx) {
    Poll::Ready(()) => {},  // 继续执行
    Poll::Pending => return Poll::Pending,  // 这里不会发生
}
```

在未启用 taskdump 时，永远返回 `Ready(())`，所以 `ready!` 永远直接通过。在启用时，如果 tracing 正在收集调用栈，可能返回 `Pending`——这样 Future 就不会继续执行 poll，确保在正确的时机捕获调用栈快照。

## 一句话总结

**`ready!(trace_leaf(cx))` 默认是零开销的 no-op，只在 taskdump 诊断启用时才生效，用于标记叶子 Future 并收集调用栈信息。**

| 状态 | 行为 | 开销 |
|------|------|------|
| 默认（未启用 taskdump） | 直接返回 Ready | 零（inline + 空函数） |
| 启用 taskdump | 标记叶子位置，收集调用栈 | 仅在 dump 时 |