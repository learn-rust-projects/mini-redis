# `capacity.next_power_of_two()` 的含义

```rust
// Round to a power of two
capacity = capacity.next_power_of_two();
```

这行代码把容量值向上取整到**最近的 2 的幂**。

例子：

| 原始值 | `next_power_of_two()` 后 |
|--------|------------------------|
| 16     | 16                      |
| 3      | 4                       |
| 5      | 8                       |
| 100    | 128                     |
| 17     | 32                      |

为什么 broadcast 通道要做这个？因为内部用**位运算**（`& mask`）替代**取模**（`%`）来计算数组下标。当容量是 2 的幂时：

```rust
index = pos % capacity         // 慢
index = pos & (capacity - 1)   // 快 —— 等价，但只需要一条 CPU 指令
```

这是环形缓冲区（ring buffer）的经典优化。你传入 `channel(3)`，内部实际分配 4 个槽位。这个细节是实现层面的性能优化，不影响你在应用层使用——`channel(3)` 对外表现为容量 3（存 3 条消息才会阻塞发送者），只是底层多浪费了一个槽位。