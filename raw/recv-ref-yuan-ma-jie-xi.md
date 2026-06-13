# Recv Future 与 recv_ref 逐行解析

## Recv Future

```rust
impl<'a, T> Future for Recv<'a, T>
where
    T: Clone,
{
    type Output = Result<T, RecvError>;
```

`Recv` 是 `Receiver::recv()` 返回的 Future。当 poll 时，要么拿到数据返回 `Ready(Ok(T))`，要么返回错误（Lagged 或 Closed），要么暂无数据返回 `Pending`。

---

```rust
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        ready!(crate::trace::trace_leaf(cx));
```

`trace_leaf` 是 Tokio 内部追踪用的，`ready!` 让它先执行，如果是叶子节点就直接 return。

---

```rust
        let (receiver, waiter) = self.project();
```

把 `Pin<&mut Self>` 拆成 `receiver` 和 `waiter` 两个可变引用。

---

```rust
        let guard = match receiver.recv_ref(Some((waiter, cx.waker()))) {
            Ok(value) => value,
            Err(TryRecvError::Empty) => return Poll::Pending,
            Err(TryRecvError::Lagged(n)) => return Poll::Ready(Err(RecvError::Lagged(n))),
            Err(TryRecvError::Closed) => return Poll::Ready(Err(RecvError::Closed)),
        };
```

- `recv_ref(Some((waiter, cx.waker())))` — 尝试读消息，同时传入 waiter 和 waker，如果没数据就把自己注册到等待队列
- `Ok(RecvGuard)` — 有数据可读，往下走
- `Empty` — 没数据且已注册等待，返回 `Pending`
- `Lagged(n)` / `Closed` — 直接返回

```rust
        Poll::Ready(guard.clone_value().ok_or(RecvError::Closed))
    }
```

从 `RecvGuard` 中 clone 出值返回。`ok_or` 是因为如果 channel 在读取间隙被 close 了，`RecvGuard` 的值可能已经是 None。

---

## project 方法

```rust
    fn project(self: Pin<&mut Self>) -> (&mut Receiver<T>, &UnsafeCell<Waiter>) {
        unsafe {
            is_unpin::<&mut Receiver<T>>();
            let me = self.get_unchecked_mut();
            (me.receiver, &me.waiter)
        }
    }
```

Recv 的结构大致是：

```rust
struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    waiter: UnsafeCell<Waiter>,  // 不能用 pin-project-lite，因为需要自定义 drop
}
```

为什么不用 `pin-project-lite`？因为注释写了"a custom drop implementation is needed"——`Recv` 在 drop 时需要：如果 waiter 还在等待队列中，要把它移除。

`is_unpin::<&mut Receiver<T>>()` 是一个编译期检查：如果 `Receiver<T>` 不是 `Unpin`，这行会编译报错。这是在用类型系统确保 `get_unchecked_mut` 安全。

---

## recv_ref

核心函数，处理"尝试读消息 → 没消息就注册等待 → 有消息就返回"的完整逻辑。

```rust
    fn recv_ref(
        &mut self,
        waiter: Option<(&UnsafeCell<Waiter>, &Waker)>,
    ) -> Result<RecvGuard<'_, T>, TryRecvError> {
        let idx = (self.next & self.shared.mask as u64) as usize;
        let mut slot = self.shared.buffer[idx].read().unwrap();
```

计算要读的槽位索引，获取读锁。

---

```rust
        if slot.pos != self.next {
```

**核心检查：槽位里的消息序号是否等于 receiver 期望的序号？**

- `slot.pos == self.next` → 正好是期望的消息，直接读
- `slot.pos != self.next` → 不匹配，需要进一步判断

### 什么情况下 slot.pos != self.next？

以 `capacity = 4` 为例，看槽位 `idx = 0` 随时间的变化：

```
时间线         槽位0的内容              tail.pos    rx.next（这次读）
───────────────────────────────────────────────────────────
send(A)       pos=0, val=A              1          0
send(B)       pos=1, val=B              2          0
send(C)       pos=2, val=C              3          0
send(D)       pos=3, val=D              4          0
send(E)       pos=4, val=E              5          0

此时 rx.next=0，槽位0的 pos=4 → slot.pos != self.next
```

这产生了三种可能的情况：

**情况 1：跟上了最新消息（Empty）**

```
send(A) → send(B) → send(C) → send(D) → 发完了

r.next=4，槽位4（idx=0）的 pos=4 → 正好匹配
```

读完之后 `rx.next=5`。如果此时 sender 还没发新消息：

```
r.next=5，槽位1（idx=1）的 pos=1 ≠ 5 → 不匹配

slot.pos(1) + capacity(4) = 5 == r.next(5) → 没新数据
```

槽位 1 里存的是之前 send(B) 的数据，不是 receiver 要的 pos=5。由于还没有发 send(F)，槽位 1 还没被覆盖，`slot.pos + capacity == self.next`——说明 receiver 已经追上了最新的消息。这就是 Empty。

**情况 2：读得太慢，错过了消息（Lagged）**

接上面的例子，receiver 一直没读，sender 继续发：

```
send(E) → 槽位0的 pos=4
send(F) → 槽位1的 pos=5
send(G) → 槽位2的 pos=6
send(H) → 槽位3的 pos=7
send(I) → 槽位0的 pos=8  ← 覆盖了 pos=4

此时 rx.next=5，槽位1的 pos=5 → 正好匹配？不：
r.next=5，槽位1（idx=1）的 pos=5 = self.next → 能正常读到 send(F)
```

但如果 receiver 在 send(I) 之后才读：

```
r.next=5，槽位1（idx=1）的 pos=5 = self.next → 正常读 F
r.next=6，槽位2（idx=2）的 pos=6 = self.next → 正常读 G
r.next=7，槽位3（idx=3）的 pos=7 = self.next → 正常读 H

r.next=8，槽位0（idx=0）的 pos=8 = self.next → 正常读 I
```

好像不会错过？等等——如果 receiver 是在 send(F) 之前读的呢？

```
r.next=0，槽位0的 pos=4 ≠ 0 → 不匹配

最旧可读位置 = tail.pos(5) - capacity(4) = 1
missed = 1 - 0 = 1 → Lagged(1)
r.next 调整为 1

再读时 r.next=1，槽位1的 pos=5 ≠ 1 → 又错过？
最旧可读位置 = tail.pos(5) - capacity(4) = 1
missed = 1 - 1 = 0 → 没错过！直接读？
```

等等，这里 `missed = 0` 但 slot.pos(5) != self.next(1)，进的是这段代码：

```rust
if missed == 0 {
    self.next = self.next.wrapping_add(1);  // self.next = 2
    return Ok(RecvGuard { slot });          // 返回槽位1的数据 = send(F)
}
```

所以即使 `slot.pos != self.next`，只要 `missed == 0`，就把 `self.next` 加 1 后读这个槽位。实际上 receiver 错过了 A，但 B/C/D/E 也被覆盖了——只报告错过了 1 条？不对。

让我用更清晰的例子说明。

### 真实场景：慢 receiver

```
capacity = 4
send A: 槽位0 (pos=0)
send B: 槽位1 (pos=1)
send C: 槽位2 (pos=2)
send D: 槽位3 (pos=3)
send E: 槽位0 (pos=4)  → 覆盖 A
send F: 槽位1 (pos=5)  → 覆盖 B
send G: 槽位2 (pos=6)  → 覆盖 C
send H: 槽位3 (pos=7)  → 覆盖 D
send I: 槽位0 (pos=8)  → 覆盖 E

此时 rx 开始读，rx.next = 0:

第一次 recv:
  idx = 0 & 3 = 0
  slot[0].pos = 8, self.next = 0
  8 != 0, 进入不匹配分支
  已发 9 条，capacity = 4
  最旧可读 = tail.pos(9) - 4 = 5
  missed = 5 - 0 = 5
  → Lagged(5), rx.next = 5

第二次 recv:
  idx = 5 & 3 = 1
  slot[1].pos = 5, self.next = 5
  5 == 5 → 正常读，拿到 send(F)
  rx.next = 6
```

所以 receiver 确实错过了 A-E 共 5 条消息。错过了 `capacity + (lag 出现时的消息序号差)` 范围内的数据。但最终结果就是 `Lagged(n)` 中的 n = 最旧可读位置 - 当前 next。

### 错过消息的根本原因

**环形缓冲区没有历史消息的概念。** 每个槽位只能存一条消息。当 sender 写入新消息时，如果槽位已经被占，旧消息直接被覆盖。

- 如果 receiver 在旧消息被覆盖之后才读，那就已经错过了
- `slot.pos` 记录的是槽位当前消息的序号，receiver 通过比较 `slot.pos` 和 `self.next` 来发现自己错过了
- 缓冲区越大，在同样写入速率下能保留的历史消息越多，receiver 越不容易错过

---

### 分支一：已释放 slot 读锁，获取 tail 锁

```rust
            drop(slot);
            let mut old_waker = None;
            let mut tail = self.shared.tail.lock();
            slot = self.shared.buffer[idx].read().unwrap();
```

为什么先 drop slot 再 lock tail，然后再重新获取 slot？

**防止死锁。** 注释解释了：

> This is required because `send2` acquires the tail lock first followed by the slot lock. Acquiring the locks in reverse order here would result in a potential deadlock.

```
send2:        lock(tail) → lock(slot)
recv_ref:     lock(slot) → lock(tail)  ← 死锁！
```

如果两个线程分别持有其中一个锁，互相等对方释放，就死锁了。所以 recv_ref 必须：**释放 slot → lock tail → 重新 lock slot**，保证锁获取顺序一致。

---

```rust
            if slot.pos != self.next {
                let next_pos = slot.pos.wrapping_add(self.shared.buffer.len() as u64);

                if next_pos == self.next {
```

**情景 A：没有新消息**

### `slot.pos` 是什么

`slot.pos` 是**当前这个槽位里存的消息的全局序号**。每次 `send()` 写入时，会把 `tail.pos`（当前写入序号）存到 `slot.pos` 中：

```rust
// send2 中的写入
slot.pos = pos;  // pos = tail.pos（写入时的全局序号）
```

### `buffer.len()` 是什么

`buffer.len()` 是环形缓冲区的总长度，也就是 `capacity`（经过 `next_power_of_two` 调整后的容量）。比如 `channel(3)` → capacity = 4，`buffer.len()` = 4。

### 为什么 `slot.pos + buffer.len()`？

环形缓冲区中，每个槽位的写入序号是固定的等差数列：

```
capacity = 4（buffer.len() = 4）

槽位 0: 存过 pos=0, 4, 8, 12 ...  每隔 4 次写入回到这个槽位
槽位 1: 存过 pos=1, 5, 9, 13 ...
槽位 2: 存过 pos=2, 6, 10, 14 ...
槽位 3: 存过 pos=3, 7, 11, 15 ...
```

**同一个槽位的两次写入之间，全局序号恰好增加 `capacity`（即 `buffer.len()`），因为写满一整圈才会回到这个槽位。**

### `next_pos` 是什么

`next_pos = slot.pos + buffer.len()` 就是**这个槽位下一次被写入时的新消息序号**。

比如槽位 0 当前存的是 `pos=8`，那么下一次写入槽位 0 的消息序号就是 `8 + 4 = 12`。

### 用 next_pos 判断 Empty 的原理

回到 `recv_ref` 的代码：

```rust
let idx = (self.next & self.shared.mask as u64) as usize;
let mut slot = self.shared.buffer[idx].read().unwrap();
```

`idx` 是根据 `self.next` 计算出来的——`receiver` 期望的消息应该在这个槽位里。如果期望的消息已经写入了但没被覆盖，那么 `slot.pos == self.next`。

现在，来看 `slot.pos != self.next` 的情况。三种可能：

```
capacity = 4, self.next = 7, idx = 7 & 3 = 3（槽位 3）

         slot[3].pos    next_pos = pos+4    结论
         ───────────    ─────────────────    ──────────────
情况 A:       7                —            slot.pos == self.next → 正好匹配，直接读
情况 B:       3            3+4=7            next_pos(7) == self.next(7) → Empty
情况 C:      11           11+4=15           next_pos(15) != self.next(7) → Lagged
```

- **情况 B**：槽位 3 当前存的是 `pos=3`（旧数据）。下一个写入槽位 3 的消息是 `pos=7`。`next_pos(7) == self.next(7)` → receiver 要等的正是槽位 3 的下一次写入 → 说明还没写，没有新数据。

- **情况 C**：槽位 3 当前存的是 `pos=11`。槽位 3 的下一次写入是 `pos=15`。`next_pos(15) != self.next(7)` → 注意 `slot.pos(11) > self.next(7)`，说明槽位 3 已经被覆盖了好几次，`pos=7` 早就过去了 → receiver 严重滞后 → Lagged。

### 为什么不用 tail.pos 直接判断？

你可能想问：直接看 `tail.pos` 和 `self.next` 不就知道有没新数据了吗？但 `tail.pos` 是 Mutex 保护的，获取它需要拿锁。而 `slot.pos + capacity` 这个判断只需要已经持有的 slot 读锁——不需要额外获取 tail 锁就能在大多数情况下确定"没数据了"。

只有 `slot.pos != self.next` **且** `next_pos != self.next` 时才需要去获取 tail 锁进一步判断。这是一个**两阶段判断优化**。

```rust
                    if tail.closed {
                        return Err(TryRecvError::Closed);
                    }
```

channel 已关闭 → 返回 Closed。

---

```rust
                    if let Some((waiter, waker)) = waiter {
                        unsafe {
                            waiter.with_mut(|ptr| {
                                match (*ptr).waker {
                                    Some(ref w) if w.will_wake(waker) => {}
                                    _ => {
                                        old_waker = std::mem::replace(
                                            &mut (*ptr).waker,
                                            Some(waker.clone()),
                                        );
                                    }
                                }

                                if !(*ptr).queued {
                                    (*ptr).queued = true;
                                    tail.waiters.push_front(NonNull::new_unchecked(&mut *ptr));
                                }
                            });
                        }
                    }
```

**注册等待：**

1. **更新 waker** — 检查当前 waiter 存的 waker 是否和本次的 waker 是同一个任务（`will_wake`）。如果是同一个，不用换；如果不是，替换成新的。这是因为 `recv()` 可能被不同 waker poll 多次（比如任务被移动），需要更新通知方式。

2. **入队** — 如果 `queued` 为 false（还没在等待队列中），设置为 true 并插入 `tail.waiters` 链表头部，之后 sender 发消息时会从尾部弹出。

```rust
                    drop(slot);
                    drop(tail);
                    drop(old_waker);
                    return Err(TryRecvError::Empty);
```

释放所有锁后返回 Empty。注意释放顺序：先 slot 再 tail，和之前讲过的锁释放顺序一致。

---

```rust
                let next = tail.pos.wrapping_sub(self.shared.buffer.len() as u64);
                let missed = next.wrapping_sub(self.next);
                drop(tail);
```

**情景 B：错过太多了**

`slot.pos != self.next` 而且 `next_pos != self.next`，说明 slot 已经被写入了好几次（环形缓冲区转了一圈以上），receiver 漏掉了至少 capacity 条消息。

### 为什么 `tail.pos - capacity` 是最旧的可读位置？

**`tail.pos` 是下一次写入的位置，所以最新一条已写入的消息是 `tail.pos - 1`。**

环形缓冲区只有 `capacity` 个槽位。最新写入了 `tail.pos - 1`，再往前推 `capacity - 1` 条，就是缓冲区里能读到的最旧消息。

```
capacity = 4, tail.pos = 10（下一次写入 pos=10）

缓冲区中的消息（按写入顺序）:
  pos=6(pos=10-4)  7  8  9(pos=10-1)
    ↑ oldest                         ↑ newest
    └ tail.pos - capacity            └ tail.pos - 1

总共有 4 条消息：6, 7, 8, 9
```

`pos=5` 和更早的消息已经被覆盖了。`pos=6` 就是还能读到的最旧消息 = `tail.pos - capacity`。

用具体的环形缓冲区布局看看：

```
capacity = 4，tail.pos = 10

槽位索引:    0      1      2      3
           ┌──────┬──────┬──────┬──────┐
当前内容:   │pos=8 │pos=9 │pos=6 │pos=7 │
           └──────┴──────┴──────┴──────┘

tail.pos=10 的下一次写入 → idx = 10 & 3 = 2 → 覆盖 pos=6

最旧可读 = tail.pos(10) - 4 = 6 → 正是 pos=6
```

**所以：`tail.pos - capacity` = 缓冲区中还在的最早那条消息的序号。**

如果 receiver 的 `self.next` 比这个值还小，说明它要读的消息已经被覆盖了。直接跳到这个位置，然后告诉它错过了多少条。

```rust
                if missed == 0 {
                    self.next = self.next.wrapping_add(1);
                    return Ok(RecvGuard { slot });
                }
```

如果 `missed == 0`（实际不可能发生，这里是防御性代码），就当正常读。

```rust
                self.next = next;
                return Err(TryRecvError::Lagged(missed));
```

调整 `self.next` 到最旧的可读位置，返回 Lagged(n) 告诉调用者错过了多少条。

---

### 分支二：slot.pos == self.next，正常读取

```rust
        self.next = self.next.wrapping_add(1);
        Ok(RecvGuard { slot })
```

正好匹配，`self.next` 前进，返回 `RecvGuard`。

---

## 总结

```
slot.pos vs self.next（同一槽位 idx = self.next & mask）
─────────────────────────────────────────────────
等于         slot.pos == self.next       → 直接读
落后 1 轮    slot.pos + cap == self.next → Empty（还没发出来，去注册等待）
落后 N 轮    slot.pos > self.next        → Lagged（跳到最旧可读位置）
```

### 为什么只有这三种？

因为 `idx = self.next & mask`，所以 `slot.pos` 和 `self.next` 在同一槽位相遇的条件是：`slot.pos ≡ self.next (mod capacity)`。也就是说它们的差必须是 capacity 的整数倍——不可能出现只差 2 或只差 7 的"中间态"。

### 换个角度看：环形缓冲区的时间线

```
同一槽位的写入时间线（capacity=4）:
  pos=0      4       8       12      16      ...
     ├───────┼───────┼───────┼───────┼───────▶ 时间
     │       │       │       │       │
     write0  write1  write2  write3  write4  ← 每次写入覆盖旧数据

receiver 读到这个槽位时可能的位置（self.next 是 8）:
  slot.pos=8  → 等于，直接读
  slot.pos=4  → 4+4=8=elf.next → Empty（消息8还没写入，等）
  slot.pos=12 → 12>8 → Lagged（消息8已经被覆盖了）
```

| 情况 | 条件 | 结果 |
|------|------|------|
| 正好匹配 | `slot.pos == self.next` | 直接读 |
| 没新数据 | `next_pos == self.next` | 注册等待，返回 Empty/Pending |
| 错过太多 | 其他 | 调整位置，返回 Lagged(n) |
| channel 关闭 | `tail.closed` | 返回 Closed |

`RecvGuard` 是一个持有 slot 读锁的 RAII 包装，`clone_value()` 从 guard 中 clone 出值，然后 guard drop 时释放读锁。