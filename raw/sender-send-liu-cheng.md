# Sender::send 执行流程

```rust
pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
```

## 第1步：检查是否有接收者

```rust
let mut tail = self.shared.tail.lock();

if tail.rx_cnt == 0 {
    return Err(SendError(value));
}
```

如果没有任何 receiver，直接返回错误，数据原路退回。

## 第2步：计算写入位置

```rust
let pos = tail.pos;                    // 当前写入序号（从0开始递增）
let rem = tail.rx_cnt;                 // 当前活跃 receiver 数量
let idx = (pos & self.shared.mask as u64) as usize;  // 实际槽位索引
```

`pos & mask` 就是位运算版的 `pos % capacity`。例如 capacity=4，mask=3，pos=5 → idx=1。

## 第3步：更新写入序号

```rust
tail.pos = tail.pos.wrapping_add(1);
```

**先读旧值，再更新**。这很关键——后续其他调用者看到的 `tail.pos` 已经指向"下一个空位"。

## 第4步：写入槽位

```rust
let mut slot = self.shared.buffer[idx].write().unwrap();

slot.pos = pos;                      // 记录这条消息的序号
slot.rem.with_mut(|v| *v = rem);     // 设置"还有几个 receiver 没读"
slot.val = UnsafeCell::new(Some(value));  // 存入数据

drop(slot);  // 释放写锁
```

写入三个字段：消息序号、待读计数、数据本身。写完后立即释放锁——通知 receiver 时不需要持有着槽位锁。

## 第5步：通知接收者

```rust
self.shared.notify_rx(tail);

Ok(rem)
```

`notify_rx` 唤醒所有正在 `recv()` 上等待的 receiver。`tail` 的 Mutex 在这里释放（函数参数拿走所有权）。

## 完整流程图

```
send(value)
  │
  ├─ lock(tail)
  ├─ rx_cnt == 0? ──── Yes ──→ return Err(value)
  │
  ├─ pos = tail.pos          // 当前写入位置
  ├─ idx = pos & mask        // 算槽位
  ├─ tail.pos += 1           // 移动写入指针（下一个数据写别的位置）
  │
  ├─ lock(slot[idx])         // 写锁
  ├─ slot.pos = pos          // 标记消息序号
  ├─ slot.rem = rx_cnt       // 标记待读人数
  ├─ slot.val = value        // 存数据
  ├─ unlock(slot)            // 释放写锁
  │
  ├─ notify_rx(tail)         // 唤醒等待的 receiver，释放 tail 锁
  └─ return Ok(rem)
```

## 为什么 `slot.rem` 记录的是写入时的 rx_cnt

因为 `rx_cnt` 可能随时变化（新 receiver subscribe 或老 receiver drop）。`send` 时记下当时的 `rx_cnt`，每个 receiver 读完这条消息就把 `rem` 减 1。当 `rem` 降到 0 时，说明所有当时在线的 receiver 都读过了，这个槽位可以被覆盖。

但如果某个 receiver 读得太慢，新数据又来了怎么办？这会导致新数据覆盖旧数据——这就是 broadcast 的**滞后（lagging）**机制：慢 receiver 会收到 `RecvError::Lagged`，跳过错过的消息，从最新位置开始读。