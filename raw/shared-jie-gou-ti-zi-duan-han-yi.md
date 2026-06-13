# Shared 结构中各字段的含义

```rust
let shared = Arc::new(Shared {
    buffer: buffer.into_boxed_slice(),
    mask: capacity - 1,
    tail: Mutex::new(Tail {
        pos: 0,
        rx_cnt: receiver_count,
        closed: false,
        waiters: LinkedList::new(),
    }),
    num_tx: AtomicUsize::new(1),
});
```

这是 broadcast 通道的核心状态初始化。分解来看：

```rust
Arc::new(Shared {
    buffer: buffer.into_boxed_slice(),  // 环形缓冲区，固定大小
    mask: capacity - 1,                 // 位掩码，替代取模运算
    tail: Mutex::new(Tail {
        pos: 0,                // 下一条消息的写入位置（逻辑序号）
        rx_cnt: receiver_count, // 当前活跃的 receiver 数量
        closed: false,          // 通道是否已关闭
        waiters: LinkedList::new(), // 等待读取的 receiver 队列
    }),
    num_tx: AtomicUsize::new(1), // Sender 的数量（初始为 1）
})
```

各字段用途：

- **`mask`** — `capacity - 1`。因为 capacity 是 2 的幂，所以 `pos & mask` 等价于 `pos % capacity`。比如 `capacity = 4`，`mask = 3`，`pos = 5` → `5 & 3 = 1`，就是槽位 1。

- **`tail.pos`** — 写入序号。每次 `send()` 递增。读端根据这个值知道"最新的消息是哪条"。

- **`tail.rx_cnt`** — 活跃 receiver 数量。内部用这个计数判断"某个槽位是否可以覆盖"。`rem`（每个 slot 的剩余未读计数）会基于这个值初始化。

- **`num_tx`** — 存活的 Sender 数量。所有 Sender 都 drop 后，通道自动关闭，receiver 会收到 `RecvError::Closed`。

- **`waiters`** — 当 receiver 想读但还没有新消息时（lag 为 0 但没新数据），receiver 把自己加入这个链表等待通知。`send()` 完成后会唤醒链表中的等待者。

这些字段都是 `Mutex` 或 `Atomic` 说明 broadcast 通道的核心路径是有锁的（不像 mpsc 通道可以做到完全无锁）。