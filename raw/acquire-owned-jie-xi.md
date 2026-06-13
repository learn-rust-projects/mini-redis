# Semaphore::acquire_owned 解析

## 签名

```rust
pub async fn acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit, AcquireError>
```

注意 `self: Arc<Self>` 而不是 `&self`——调用者必须传一个 `Arc<Semaphore>` 的克隆。

## 为什么需要 Arc？

因为 `acquire_owned` 返回的 `OwnedSemaphorePermit` 需要**持有 `Semaphore` 的所有权**，这样才能跨 `spawn` 传递：

```rust
// acquire() 返回 SemaphorePermit<'_, Semaphore>，有生命周期
let permit = semaphore.acquire().await.unwrap();
// 不能跨 spawn，因为 permit 借用了 semaphore

// acquire_owned() 返回 OwnedSemaphorePermit，没有生命周期
let permit = semaphore.clone().acquire_owned().await.unwrap();
tokio::spawn(async move {
    // permit 通过 Arc 持有 semaphore，可以自由移动
    drop(permit); // drop 时自动归还许可
});
```

`SemaphorePermit` 借用了 `&Semaphore`，受生命周期约束。`OwnedSemaphorePermit` 通过 `Arc` 共享所有权，不受生命周期约束。

## 函数体

```rust
    #[cfg(not(all(tokio_unstable, feature = "tracing")))]
    let inner = self.ll_sem.acquire(1);

    inner.await?;
    Ok(OwnedSemaphorePermit {
        sem: self,
        permits: 1,
    })
```

分三步：

1. **`self.ll_sem.acquire(1)`** — 调用底层信号量的 `acquire`，请求 1 个许可。返回一个 Future（`inner`）
2. **`inner.await?`** — await 这个 Future。如果有可用许可，立即返回；如果没有，异步等待直到拿到许可或信号量关闭
3. **`Ok(OwnedSemaphorePermit { sem: self, permits: 1 })`** — 成功拿到许可后，构造一个 `OwnedSemaphorePermit`，持有 `Semaphore` 的 Arc 和许可数量

## Cancel safety

注释提到：

> Cancelling a call to `acquire_owned` makes you lose your place in the queue.

如果你 `await` 到一半取消了（比如 `select!` 跳走了），你就**失去排队位置**了。下次再 `acquire_owned`，你得重新排队到末尾。

这是公平 FIFO 的自然结果——如果你退出队列，后面的人往前移。Tokio 不会帮你保留位置。

## tracing 分支

```rust
    #[cfg(all(tokio_unstable, feature = "tracing"))]
    let inner = trace::async_op(
        || self.ll_sem.acquire(1),
        self.resource_span.clone(),
        "Semaphore::acquire_owned",
        "poll",
        true,
    );
```

当启用 `tokio_unstable` + `tracing` feature 时，acquire 操作会被追踪——记录它的 span 信息，便于诊断和性能分析。普通情况下这段代码被编译消除，零开销。

## 总结

| | acquire() | acquire_owned() |
|--|-----------|----------------|
| 参数 | `&self` | `Arc<Self>` |
| 返回 | `SemaphorePermit<'_, Semaphore>` | `OwnedSemaphorePermit` |
| 生命周期 | 有（借用 Semaphore） | 无（通过 Arc 共享所有权） |
| 跨 spawn | 不行 | 可以 |
| 取消后排队位置 | 丢失 | 丢失 |