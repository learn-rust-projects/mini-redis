# into_guarded 解析：普通链表如何转为守卫循环链表

## WaitersList::new

```rust
fn new(
    unguarded_list: LinkedList<Waiter, <Waiter as linked_list::Link>::Target>,
    guard: Pin<&'a Waiter>,
    shared: &'a Shared<T>,
) -> Self {
    let guard_ptr = NonNull::from(guard.get_ref());
    let list = unguarded_list.into_guarded(guard_ptr);
    WaitersList {
        list,
        is_empty: false,
        shared,
    }
}
```

- `unguarded_list` — 从 `tail.waiters` 中 `take` 出来的普通链表
- `guard` — 栈上 pin 住的 guard 节点
- `guard_ptr` — 获取 guard 的裸指针
- `into_guarded(guard_ptr)` — 把普通链表和 guard 节点"缝合"成循环链表

---

## into_guarded

```rust
pub(crate) fn into_guarded(self, guard_handle: L::Handle) -> GuardedLinkedList<L, L::Target> {
    let guard = L::as_raw(&guard_handle);

    unsafe {
        if let Some(head) = self.head {
```

情况一：**链表不为空**。以 head 存在分支。

---

```rust
            debug_assert!(L::pointers(head).as_ref().get_prev().is_none());
            L::pointers(head).as_mut().set_prev(Some(guard));
            L::pointers(guard).as_mut().set_next(Some(head));
```

**`debug_assert!` 这一行：** 在把 guard 接上去之前，先断言 head 的 prev 确实是 `None`。这是对"当前链表确实是普通链表"的**防御性检查**——如果因为某个 bug，head 已经有 prev 节点了（说明链表已经是循环的），在 debug 模式下会直接 panic 捕获这个异常。`debug_assert!` 只会在 debug 编译下生效，release 中完全消除，零开销。

普通链表中，head 的 prev 是 None。现在：

```
guard.next → head
head.prev → guard
```

---

```rust
            let tail = self.tail.unwrap();
            debug_assert!(L::pointers(tail).as_ref().get_next().is_none());
            L::pointers(tail).as_mut().set_next(Some(guard));
            L::pointers(guard).as_mut().set_prev(Some(tail));
```

**`debug_assert!` 同理：** 断言 tail 的 next 确实是 `None`，确保链表结构符合预期。`unwrap()` 这里也安全——因为前面已经确认 head 存在，链表不为空，tail 必然不是 None。

普通链表中，tail 的 next 是 None。现在：

```
tail.next → guard
guard.prev → tail
```

**效果：链表变成循环的，guard 像一座桥连接 head 和 tail。**

```
                   ┌──────────────────────────────┐
                   ↓                              │
  guard.next ──→ head ↔ node1 ↔ node2 ↔ tail ────┘
  guard.prev ←───────────────────────────────────┘
```

---

```rust
        } else {
            // The list is empty.
            L::pointers(guard).as_mut().set_prev(Some(guard));
            L::pointers(guard).as_mut().set_next(Some(guard));
        }
    }

    GuardedLinkedList { guard, _marker: PhantomData }
}
```

情况二：**链表为空**。guard 的 prev 和 next 都指向自己：

```
  guard.next ──┐
               ↓
              guard
               ↑
  guard.prev ──┘
```

这样即使链表为空，所有节点的指针也不为 None。

---

## 为什么注释说 `guard_handle` 不需要关心 drop

```rust
// `guard_handle` is a NonNull pointer, we don't have to care about dropping it.
```

`Guard<L, T>` 中 `Handle = NonNull<Waiter>`。`NonNull` 没有实现 `Drop`，所以 `GuardedLinkedList` 析构时不会去释放 guard 节点。guard 是栈上变量，由 Rust 的正常出栈流程管理。

## 总结

| 操作 | 前（普通链表） | 后（守卫链表） |
|------|---------------|---------------|
| head.prev | None | → guard |
| tail.next | None | → guard |
| guard.prev | — | → tail |
| guard.next | — | → head |
| 空链表时 guard | — | 指向自己 |

核心目标：**消除所有 None 指针**，让每个节点都能在不知道 head/tail 的情况下安全地自我移除。