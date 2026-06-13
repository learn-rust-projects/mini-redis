# 初始化 Slot 时 pos 为什么是负数

```rust
for i in 0..capacity {
    buffer.push(RwLock::new(Slot {
        rem: AtomicUsize::new(0),
        pos: (i as u64).wrapping_sub(capacity as u64),
        val: UnsafeCell::new(None),
    }));
}
```

逐行解释：

```rust
for i in 0..capacity {
    buffer.push(RwLock::new(Slot {
        rem: AtomicUsize::new(0),           // 还剩几个 receiver 没读这条消息
        pos: (i as u64).wrapping_sub(capacity as u64), // 槽位的"逻辑位置"
        val: UnsafeCell::new(None),         // 存储的实际数据
    }));
}
```

关键在 `pos` 这行：

```
capacity = 4 时:

i=0 → 0 - 4 =  wrapping_sub → 18446744073709551612  (即 -4 的补码)
i=1 → 1 - 4 =  wrapping_sub → 18446744073709551613  (即 -3 的补码)
i=2 → 2 - 4 =  wrapping_sub → 18446744073709551614  (即 -2 的补码)
i=3 → 3 - 4 =  wrapping_sub → 18446744073709551615  (即 -1 的补码)
```

为什么要初始化为负数？

因为 broadcast 通道用 `pos` 来区分"这个槽位存的是第几轮写入的数据"。初始化为负数表示"这些槽位都还没写过有效数据"。当第一次写入时，`pos` 从 0 开始递增，写入位置是 `pos % capacity`，并更新对应槽位的 `pos`。

举个例子，容量 4 的通道写入第 0 条消息：

- 写入槽位 `0 % 4 = 0`
- 槽位 0 的 `pos` 从 `-4` 更新为 `0`
- receiver 读的时候比较自己的 `pos` 和槽位的 `pos`：如果自己的 `pos` < 槽位的 `pos`，说明这条消息还没读过

整个 `pos` 的加减比较构成了一种**不需要额外锁的、原子化的"消息序号 vs 已读位置"跟踪机制**。配合 `rem`（还剩几个 receiver 没读），就能确定一个槽位是否可以安全覆盖——当 `rem` 降到 0 且有新消息需要写入时，新数据就可以覆盖这个槽位。

简单理解：`pos` 是消息的全局序号，负数初始值标记"空槽位"，后续通过序号比较来判断读写进度。