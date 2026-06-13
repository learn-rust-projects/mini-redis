# 为什么释放锁后要重新检查 slot.pos？

## 代码

```rust
drop(slot);
let mut tail = self.shared.tail.lock();
slot = self.shared.buffer[idx].read().unwrap();

// Make sure the position did not change. This could happen in the
// unlikely event that the buffer is wrapped between dropping the
// read lock and acquiring the tail lock.
if slot.pos != self.next {
```

## 这段话在问什么

`recv_ref` 先读 slot，发现不匹配，然后：
1. `drop(slot)` — 释放了 slot 读锁
2. `lock(tail)` — 获取 tail 锁
3. `read(slot)` — 重新获取 slot 读锁

**问题：释放锁到重新获取锁之间，slot 可能被改写了。** 所以需要再次检查 `slot.pos != self.next`。

## 具体场景

以 `capacity = 4` 为例：

```
初始状态：tail.pos = 10, rx.next = 10
槽位 idx = (10 & 3) = 2，里面存的是上一次的消息 pos=6
```

### 正常流程（没有并发写入）

```
recv_ref:
  1. read(slot[2]) → slot.pos = 6, self.next = 10
     slot.pos(6) != self.next(10) → 不匹配
  2. drop(slot) → 释放读锁
  3. lock(tail) → 获取 tail 锁
  4. read(slot[2]) → slot.pos = 6 → 还是 6
  5. 再次检查 slot.pos(6) != self.next(10) → 还是不匹配
  6. 进一步判断：slot.pos(6) + capacity(4) = 10 == self.next(10) → Empty
```

正常的，没有新消息，进入等待注册流程。

### 并发场景：释放锁期间 send2 写入了

```
Thread A (recv_ref):            Thread B (send2):
───────────────────────────     ───────────────────────
1. read(slot[2]) → pos=6
   self.next=10, 不匹配
2. drop(slot) ✅
                                  3. lock(tail) ✅
                                  4. write(slot[2]) → pos=10, val=X
                                  5. unlock(slot)
                                  6. unlock(tail)
7. lock(tail) ✅
8. read(slot[2]) → 此时此刻！
```

第 8 步读到的 `slot.pos` 是 10 还是 6？

**取决于 Thread B 是否已经写入了。** 如果 Thread B 在 Thread A 的 `drop(slot)` 和 `lock(tail)` 之间写入了 slot[2]，那么第 8 步读到的就是 `pos=10`。

此时 `slot.pos(10) == self.next(10)` → 等于！

如果 `recv_ref` 不重新检查直接进入旧的分支逻辑，就会错误地认为"没新数据"而去注册等待——明明槽位里已经有一条正是 receiver 要的消息。这会导致：

1. receiver 注册等待，返回 Pending
2. Sender 已经发过消息了，不会再 notify
3. **receiver 永远醒不过来**

### 重新检查保证安全

```rust
if slot.pos != self.next {
    // 确实是没匹配，进入 Empty/Lagged 判断
} else {
    // 中间被写入了！！slot.pos == self.next 了
    // 直接走下面的正常读取路径
}
// ...
self.next = self.next.wrapping_add(1);
Ok(RecvGuard { slot })
```

如果第二次检查发现 `slot.pos == self.next`，说明发送者已经在这个间隙把数据写入了——`recv_ref` 直接按正常读取流程走，不进入等待注册逻辑。

## 一句话总结

**释放锁到重新获取锁的间隙中，另一个线程可能恰好写入了这个槽位。如果不重新检查就直接认为"不匹配"，会导致 receiver 去注册等待而错过了已经就绪的消息，造成虚假的永久挂起。**