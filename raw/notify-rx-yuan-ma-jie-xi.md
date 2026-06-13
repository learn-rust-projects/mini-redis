# notify_rx 源码逐行解析

```rust
impl<T> Shared<T> {
    fn notify_rx<'a, 'b: 'a>(&'b self, mut tail: MutexGuard<'a, Tail>) {
```

接收一个已经锁住的 `tail`，函数结束后 `MutexGuard` 自动释放。生命周期 `'b: 'a` 表示 `self` 活得比 guard 长。

---

```rust
        let guard = Waiter::new();
        pin!(guard);
```

在**栈上**创建一个临时的 `Waiter` 节点并固定（pin）住。这个 guard 节点用于构建`GuardedLinkedList`——一个循环链表，guard 连接 head 和 tail。

**为什么需要 pin？** GuardedLinkedList 的节点指针不能移动，否则链表中的指针就悬空了。`pin!` 保证 guard 在栈上不动。

---

```rust
        let mut list = WaitersList::new(
            std::mem::take(&mut tail.waiters),
            guard.as_ref(),
            self,
        );
```

`std::mem::take(&mut tail.waiters)` — 把 `tail.waiters` 链表**原子性地换成一个空链表**。原来的所有 waiter 节点被移到 `list` 中。

`WaitersList` 是一个包装类型：
- 内部用 `GuardedLinkedList` 组织所有 waiter
- 保证只能在持有 `tail` 锁时修改链表
- **析构时自动清空链表**——这是关键的安全保证。如果函数中途 panic，`WaitersList` 的 drop 会清理所有指向栈上 guard 的指针，防止悬空指针

---

```rust
        let mut wakers = WakeList::new();
        'outer: loop {
            while wakers.can_push() {
                match list.pop_back_locked(&mut tail) {
                    Some(mut waiter) => {
                        let waiter = unsafe { waiter.as_mut() };
                        assert!(waiter.queued);
                        waiter.queued = false;
                        if let Some(waker) = waiter.waker.take() {
                            wakers.push(waker);
                        }
                    }
                    None => {
                        break 'outer;
                    }
                }
            }
```

`WakeList` 是一个固定大小的 waker 数组（默认 32 个）。`wakers.can_push()` 返回 false 时说明满了，需要先唤醒一批。

对每个 waiter：

```rust
let waiter = unsafe { waiter.as_mut() };
assert!(waiter.queued);
waiter.queued = false;
if let Some(waker) = waiter.waker.take() {
    wakers.push(waker);
}
```

**`assert!(waiter.queued)`** — 断言这个 waiter 当前确实在等待队列中。这是 debug 下的安全检查，防止重复弹出同一个 waiter 或对已经出队的 waiter 误操作。

**`waiter.queued = false`** — 标记为"已出队"。

这个标记为什么重要？考虑一个场景：receiver 在调用 `recv()` 等待消息时设置了超时或取消：

1. receiver 把自己插入 waiters 链表 `waiter.queued = true`
2. 超时了，receiver 尝试从链表中自我移除
3. 与此同时 sender 发消息，`notify_rx` 正在遍历链表准备唤醒

有了 `queued` 标记，receiver 可以检查自己是否还在队列中：
- `queued = true` → 还在队列中，需要自我移除，然后手动处理超时
- `queued = false` → `notify_rx` 已经处理了我，waker 已经被取走，等待 `wake()` 即可

这个标记解决的是**链表自我移除和外部遍历之间的竞态**，是 intrusive 链表中典型的并发处理模式。

**`waiter.waker.take()`** — 取出 waiter 保存的 Waker（用 `take` 留下 `None`，避免后续重复使用）。

取出的 Waker 加入 `WakeList`。如果 `WakeList` 满了，跳出内层循环到外层进行批量唤醒。

---

```rust
            drop(tail);
            wakers.wake_all();
            tail = self.tail.lock();
```

**释放锁后再唤醒**，这是核心设计：

```rust
drop(tail);           // 1. 释放 tail 锁
wakers.wake_all();    // 2. 唤醒这批 waiter
tail = self.tail.lock(); // 3. 重新获取锁
```

为什么一定要先释放锁再唤醒？看一个具体的时间线：

### 场景：锁未释放就 wake

假设不 drop tail，直接在持有锁时调用 `waker.wake()`：

```
Thread A (notify_rx):   lock(tail) → pop waiter → waker.wake() → ...
                                              ↓
Thread B (waiter):               被唤醒 → 尝试 lock(tail) → 阻塞！拿不到锁
```

Thread B 被唤醒后，第一件事可能就是去拿 tail 锁（比如要重新排队或自我移除）——但锁还在 Thread A 手里，Thread B 只能阻塞。这就是经典的**锁传递（lock convoy）**问题：唤醒者和被唤醒者争同一把锁，毫无并行性。

### 释放锁之后发生了什么

`drop(tail)` 释放锁到 `self.tail.lock()` 重新拿到锁，中间有一段**无锁窗口**。在这期间：

**1. 刚被唤醒的 waiter 可能把自己从链表中移除**

之前说 waiter 可能因为超时/取消而自我移除，但那是在被唤醒前。这里说的是另一种情况：

Thread A 已经从 `WaitersList` 中 pop 出了 waiter、取走了 waker、调用了 `wake()`。waiter 被唤醒后，如果它的业务逻辑决定不再等待（比如 `recv()` 返回了数据），它需要清理自己的状态——而这个清理可能包括"把自己从链表里移除"。

但等等，waiter 已经被 pop 出来了啊？不——**注意这里 pop 的是 waiter 的 waker，不是 waiter 节点本身！** waiter 节点还留在链表里。所以被唤醒的 waiter 可能会尝试从 `WaitersList` 中移除自己的节点。

**2. 新的 waiter 可能加入链表**

新 receiver 调用 `recv()`，发现没有新消息，于是 `lock(tail)` → 把自己插入 `tail.waiters` → `unlock(tail)`。

注意 `notify_rx` 刚才用的是 `std::mem::take(&mut tail.waiters)`——把原链表全移走了。所以新 waiter 插入的是**新的空链表**，跟 Thread A 正在处理的 `WaitersList` 是两回事。它们互不干扰。

**3. 都没关系：唤醒非本批的 waiter 也没事**

重新获取锁后，`tail.waiters` 中可能有新加入的 waiter。这批新 waiter 不在 `WaitersList` 中，本轮不会被唤醒。

但这没关系——它们的 waker 还在自己身上。当下一次 `send()` 发生时，`notify_rx` 会再次被调用，它们自然会被唤醒。最多就是多等一轮 `send`，不会丢失。

---

```rust
        }

        drop(tail);
        wakers.wake_all();
    }
}
```

循环结束后，还有一批 waker 没唤醒（最后一批不满 32 个）。释放锁，再唤醒这批。

---

## 完整流程总结

```
notify_rx 被调用（tail 已锁住）
  │
  ├─ 1. 创建栈上 guard，pin 住
  ├─ 2. take(&mut tail.waiters) → 把所有 waiter 移到 WaitersList
  ├─ 3. 循环 pop waiter，收集 waker
  │      ├─ waker 满了 → drop(tail) → wake_all() → lock(tail) → 继续
  │      └─ 所有 waiter 处理完 → 跳出循环
  ├─ 4. drop(tail) → wake_all() → 唤醒最后一批
  └─ 结束
```

## 关键设计决策

| 设计 | 原因 |
|------|------|
| `std::mem::take` 清空原链表 | 原子性操作，保证不会漏掉或重复处理 waiter |
| 用 WaitersList 包装 | 析构时自动清理，panic 安全；防止悬空指针 |
| 分批唤醒（WakeList） | 避免一次唤醒大量任务造成惊群 |
| 唤醒前释放 tail 锁 | 被唤醒的 waiter 不会因锁阻塞；允许安全地自我移除 |
| 栈上 guard + GuardedLinkedList | waiter 可以安全地从链表中自我移除（如超时取消）|