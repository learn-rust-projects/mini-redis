# broadcast 通道为什么需要 T: Clone

```rust
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let tx = unsafe { Sender::new_with_receiver_count(1, capacity) };
    let rx = Receiver {
        shared: tx.shared.clone(),
        next: 0,
    };
    (tx, rx)
}
```

因为 broadcast 通道是**一发多收**——一个 `send()` 的数据要让所有活跃的 `Receiver` 各自读一次。

Rust 的所有权规则决定了：一份数据不能同时被多个所有者持有。当每个 Receiver 调用 `recv()` 时，它需要拿到属于自己的那份数据，所以必须**克隆**。

如果去掉 `T: Clone` 约束，可以改成内部存 `Arc<T>`，但这样：

- `send(42)` 这种小整数也要走一次堆分配 + 原子引用计数
- 每个 `recv()` 都要 `Arc::clone()`（原子操作递增）
- 调用者没法自己选择——哪怕他知道他的类型克隆很便宜（比如 `i32`、`Vec<u8>`），也得承受 `Arc` 的开销

当前的设计把选择权交给调用者：

```rust
// 小类型，直接克隆
let (tx, rx) = broadcast::channel::<i32>(16);

// 大类型，调用者自己包 Arc
let (tx, rx) = broadcast::channel::<Arc<BigData>>(16);
```

作为对比，`mpsc` 通道不需要 `Clone`，因为它是一发一收——数据直接 move 过去就行，所有权转移就好了。