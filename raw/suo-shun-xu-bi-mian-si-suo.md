# broadcast 中的锁顺序：为什么 recv_ref 必须先释放 slot 锁

## 背景

broadcast channel 中有两把锁：

- **`tail` 锁**（`Mutex<Tail>`）— 保护写入位置、receiver 数量、等待队列
- **`slot` 锁**（`RwLock<Slot<T>>`）— 保护每个槽位的读写

两个操作需要同时持有这两把锁：

| 操作 | 锁获取顺序 |
|------|-----------|
| `send2`（发送消息） | 先 `lock(tail)`，再 `write(slot)` |
| `recv_ref`（读取消息，不匹配分支） | 先 `read(slot)`，再 `lock(tail)` |

## 死锁是怎么发生的

如果 `recv_ref` 不先释放 slot 锁，两个线程可能这样交错执行：

```
Thread A (send2):          Thread B (recv_ref)
─────────────────────      ─────────────────────
lock(tail) ✅
                           read(slot[0]) ✅
write(slot[0]) ❌ 等...    lock(tail) ❌ 等...
```

- Thread A 持有 tail，等 slot
- Thread B 持有 slot，等 tail

**互相等对方释放锁，死锁。** 两个线程永远不会继续执行。

## 解决方案

`recv_ref` 在发现 `slot.pos != self.next` 后，**先释放 slot 读锁，再去获取 tail 锁，最后重新获取 slot 读锁**：

```rust
// recv_ref 中
if slot.pos != self.next {
    drop(slot);                          // 1. 释放 slot 锁
    let mut tail = self.shared.tail.lock(); // 2. 获取 tail 锁
    slot = self.shared.buffer[idx].read().unwrap(); // 3. 重新获取 slot 锁
    // ...
}
```

### 释放后再获取的时间线

```
Thread A (send2):          Thread B (recv_ref)
─────────────────────      ─────────────────────
lock(tail) ✅
write(slot[0]) ✅                           先读 slot[0]
unlock(slot[0])                             发现不匹配
                            read(slot[0]) ✅  (短暂获取)
                            drop(slot)       (释放)
                            lock(tail) ❌ 等...  (B 等 A 的 tail)
unlock(tail)               ← A 释放 tail
                           lock(tail) ✅   (B 拿到)
                           read(slot[0]) ✅ (重新获取)
```

顺序始终是 **tail → slot**，不会交叉，死锁解除。

## 如果释放后 slot 数据变了怎么办？

释放 slot 读锁后，另一个线程可能写入了这个 slot。所以重新获取 slot 后需要**再次检查 `slot.pos != self.next`**：

```rust
if slot.pos != self.next {
    // 再次比较，确认数据没有被中间改写
}
```

如果第二次检查发现 `slot.pos == self.next`，说明在释放锁期间 send2 正好写入了期望的消息——直接读就行了。`recv_ref` 处理了这个分支（不在上面的代码段中，但在完整实现中有）。

## 总结

| | 做法 | 原因 |
|--|------|------|
| send2 | lock(tail) → write(slot) | 先拿 tail 再写槽位 |
| recv_ref（匹配） | read(slot) 直接读 | 无需 tail 锁，不会死锁 |
| recv_ref（不匹配） | **drop(slot)** → lock(tail) → read(slot) | 避免与 send2 形成交叉死锁 |

**关键规则：多个锁必须严格按固定顺序获取，否则必然死锁。** 这里强制统一为 `tail → slot` 的顺序，`recv_ref` 通过释放 slot 来适应这个顺序。