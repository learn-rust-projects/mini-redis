# 为什么必须先释放 slot 写锁，再释放 tail 锁？

## 背景

```rust
let mut slot = self.shared.buffer[idx].write().unwrap();
// 写入数据
drop(slot);                           // 1. 先释放 slot 写锁
self.shared.notify_rx(tail);          // 2. 通知 receiver
// tail 在此函数末尾自动释放        // 3. 释放 tail 互斥锁
```

注释原文：

> Notify and release the mutex. This must happen after the slot lock is released, otherwise the writer lock bit could be cleared while another thread is in the critical section.

## 为什么顺序不能反过来？

如果先释放 `tail`，再释放 `slot`：

```
Thread A: lock(tail) → lock(slot[0]) → 写入 → unlock(tail) → ... → unlock(slot[0]) → 清除写锁位
                                                                         ↑
Thread B:                     lock(tail) → lock(slot[0]) → 自旋等待写锁释放
```

1. Thread A 释放 tail 后，Thread B 立即拿到 `tail` 锁进入临界区
2. Thread B 尝试 `lock(slot[0])`，发现写锁位被 Thread A 占着，**自旋等待**
3. Thread A 此时 `drop(slot)`，清除了写锁位
4. Thread B 看到写锁位被清，以为自己可以安全拿锁了

## 带来的问题

1. **通知丢失**：Thread A 已经释放了 tail，无法再调用 `notify_rx` 来唤醒等待的 receiver——这条消息的通知就这么丢了
2. **竞态条件**：Thread A 刚清除写锁位时，Thread B 在同一瞬间开始拿锁。两个线程对同一个 slot 的状态操作可能交织，出现未定义行为

## 一句话总结

**先释放 slot 再释放 tail，保证释放 slot 时不会有其他线程处于临界区中。** 反过来，另一个线程可能在 slot 还锁着的时候就拿到 tail 锁进入临界区，导致通知丢失和竞态。

## 锁释放顺序对比

| 顺序 | 结果 |
|------|------|
| 先释放 slot，再释放 tail | 安全。释放 slot 时无其他线程在临界区 |
| 先释放 tail，再释放 slot | 竞态。另一个线程进入临界区后，与当前线程的 slot 写锁释放交织 |