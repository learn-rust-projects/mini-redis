# pop_back_locked 逐行解析

```rust
/// Removes the last element from the guarded list. Modifying this list
/// requires an exclusive access to the main list in `Notify`.
fn pop_back_locked(&mut self, _tail: &mut Tail) -> Option<NonNull<Waiter>> {
    let result = self.list.pop_back();
    if result.is_none() {
        // Save information about emptiness to avoid waiting for lock
        // in the destructor.
        self.is_empty = true;
    }
    result
}
```

## 参数 `_tail: &mut Tail`

注意参数名有下划线前缀——表示**函数体里根本不使用它**。

那为什么还要传？因为 `&mut Tail` 意味着调用者必须持有 `tail` 锁。Rust 的所有权系统在编译期保证：没有 `MutexGuard<Tail>`，就无法调用这个方法。

这是一种**类型级的安全守卫**：用类型系统代替运行时检查。

## 函数体

```rust
let result = self.list.pop_back();
```

从内部的 `GuardedLinkedList` 弹出最后一个 waiter（FIFO 队列）。

```rust
if result.is_none() {
    self.is_empty = true;
}
```

如果链表已经空了，设置 `is_empty = true` 标记。

```rust
result
```

返回弹出的 waiter 指针。

## 为什么需要 is_empty 标记？

回到 `WaitersList` 的析构函数（伪代码）：

```rust
impl Drop for WaitersList<...> {
    fn drop(&mut self) {
        if self.is_empty {
            return; // 已空，不需要锁
        }
        // 否则需要获取 tail 锁，把剩余节点归还到原链表
        let tail = self.shared.tail.lock();
        // ... 归还逻辑 ...
    }
}
```

如果没有 `is_empty` 标记，析构函数不知道链表是否为空，**每次 drop 都要去 acquire tail 锁**。

在 `notify_rx` 的循环中：

```rust
'outer: loop {
    while wakers.can_push() {
        match list.pop_back_locked(&mut tail) {
            Some(mut waiter) => { ... }
            None => { break 'outer; }  // pop 到空了才跳出
        }
    }
    drop(tail);
    wakers.wake_all();
    tail = self.tail.lock();
}
```

循环退出时，链表一定已经空了。`is_empty = true` 后，析构函数无需拿锁，直接跳过清理。

## 一句话总结

**`is_empty` 是一个零成本优化标记**——既然 `notify_rx` 已经把所有 waiter 都 pop 完了，析构时没必要多拿一次锁。而 `_tail: &mut Tail` 参数是用类型系统保证线程安全：没锁就别想碰这个链表。