# Tokio 侵入式链表 LinkedList 源码分析

这是 Tokio 内部使用的侵入式双链表，用于 `broadcast` 的 `waiters`、`Notify` 等场景。

## 什么是侵入式链表

普通链表：节点是独立分配的，包含指向数据的指针。

```
Vec<LinkedList> → Node { data, prev, next } → 实际数据
```

侵入式链表：链表指针**嵌入在数据结构内部**，数据本身就是链表节点。

```
struct Waiter {
    pointers: Pointers<Waiter>,  // 链表指针嵌在这里
    ...其他字段...
}
```

Tokio 的 `broadcast` 中，等待消息的 receiver 就是通过这种链表串联起来的。

## 核心结构

### LinkedList

```rust
pub(crate) struct LinkedList<L, T> {
    head: Option<NonNull<T>>,
    tail: Option<NonNull<T>>,
    _marker: PhantomData<*const L>,
}
```

最简单的双链表：只记录头尾指针。

### Link trait

```rust
pub(crate) unsafe trait Link {
    type Handle;    // 用户持有的句柄类型
    type Target;    // 实际节点类型

    fn as_raw(handle: &Self::Handle) -> NonNull<Self::Target>;
    unsafe fn from_raw(ptr: NonNull<Self::Target>) -> Self::Handle;
    unsafe fn pointers(target: NonNull<Self::Target>) -> NonNull<Pointers<Self::Target>>;
}
```

解耦了"链表逻辑"和"节点如何存储指针"。不同的类型可以实现 `Link` 来定义自己的句柄和指针访问方式。

## 关键设计：避免 noalias

### PointersInner 的 #[repr(C)]

```rust
#[repr(C)]
struct PointersInner<T> {
    prev: Option<NonNull<T>>,
    next: Option<NonNull<T>>,
    _pin: PhantomPinned,
}
```

注释解释了为什么这么设计：

- Rust 编译器会对 `&mut T` 加 `noalias` 属性（LLVM 层面的别名分析优化）
- 如果通过 `&mut` 访问 `prev` 和 `next` 字段，编译器可能做出错误的优化假设
- 解决方案：用**裸指针偏移（pointer offset）** 来读写字段，避免创建引用

```rust
fn get_prev(&self) -> Option<NonNull<T>> {
    // prev 是 PointersInner 的第一个字段
    unsafe {
        let inner = self.inner.get();                    // UnsafeCell 裸指针
        let prev = inner as *const Option<NonNull<T>>;  // 转为指向第一个字段的指针
        ptr::read(prev)                                  // 直接读
    }
}

fn set_next(&mut self, value: Option<NonNull<T>>) {
    unsafe {
        let inner = self.inner.get();
        let prev = inner as *mut Option<NonNull<T>>;
        let next = prev.add(1);                          // 偏移到第二个字段
        ptr::write(next, value);
    }
}
```

`#[repr(C)]` 保证字段顺序就是内存布局顺序，所以 `prev.add(1)` 一定指向 `next`。

## 几种链表变体

### LinkedList — 标准双链表

`push_front` + `pop_back` 实现了 FIFO 队列（先进先出）。

```rust
push_front: 新节点插入到 head 前面
pop_back: 从 tail 弹出
```

### CountedLinkedList — 带计数

```rust
pub(crate) struct CountedLinkedList<L: Link, T> {
    list: LinkedList<L, T>,
    count: usize,  // 额外记录元素数量
}
```

在每个操作（push_front、pop_back、remove）中维护 count。需要 O(1) 获取长度时使用。

### GuardedLinkedList — 循环守卫链表

```rust
pub(crate) struct GuardedLinkedList<L, T> {
    guard: NonNull<T>,      // 指向守卫节点
    _marker: PhantomData<*const L>,
}
```

把普通链表转为**循环链表**：守卫节点（guard）连接 head 和 tail。链表为空时，guard 的 prev 和 next 都指向自己。

优势：所有节点的指针都不为 None，一些操作可以简化。用于 Tokio 的 `Notify`。

### DrainFilter — 条件删除迭代器

```rust
impl<T: Link> LinkedList<T, T::Target> {
    pub(crate) fn drain_filter<F>(&mut self, filter: F) -> DrainFilter<'_, T, F>
    where
        F: FnMut(&T::Target) -> bool,
    {
        // ...
    }
}
```

遍历链表，对匹配 filter 的节点调用 `remove`。迭代器持有链表可变引用，逐个检查并移除。

## 操作细节

### push_front

```
val → ManuallyDrop（防止中途丢弃）
ptr = as_raw(val)
ptr.next = head
ptr.prev = None
if head存在: head.prev = ptr
head = ptr
if tail为空: tail = ptr
```

### pop_back

```
last = tail?
tail = last.prev
if last.prev存在: last.prev.next = None
else: head = None
last.prev = None
last.next = None
from_raw(last) → Handle
```

### remove

需要传入节点指针，从链表中摘除。**必须是链表中的节点**，否则返回 None。

```rust
if prev存在:
    prev.next = node.next
else:
    head = node.next

if next存在:
    next.prev = node.prev
else:
    tail = node.prev

node.prev = None
node.next = None
```

### 安全检查

- `push_front` 有 `assert_ne!(self.head, Some(ptr))` — 防止重复插入
- `remove` 检查 `self.head` 和 `self.tail` 是否匹配 — 防止从不属于该链表的节点中移除

## 在 broadcast 中的使用场景

```rust
struct Tail {
    waiters: LinkedList<Waiter, <Waiter as Link>::Target>,
    // ...
}
```

当 receiver 调用 `recv()` 但还没有新消息时，它把自己作为一个节点插入到 `waiters` 链表中。当 sender 调用 `send()` 写入新数据后，通过 `notify_rx` 遍历这个链表，唤醒所有等待的 receiver。

## 总结

| 特点 | 说明 |
|------|------|
| 侵入式 | 链表指针嵌入节点，无需额外分配 |
| 无 noalias | 用 `#[repr(C)]` + 裸指针偏移避免编译器错误优化 |
| unsafe | 大部分 API 是 unsafe，调用者需保证节点确实在链表中 |
| 多种变体 | 普通 / 计数 / 守卫循环 / 条件删除 |
| 使用场景 | broadcast waiters、Notify 等内部同步原语 |