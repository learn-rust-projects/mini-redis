# Waker 到底唤醒了什么？

## 一句话

**`waker.wake()` 把等待的任务重新提交给 Tokio 调度器，告诉它"这个任务可以继续执行了"。**

## 完整流程

1. 某个 task 调用 `rx.recv().await`：

```
Task A 执行 recv()
  → 没有新消息
  → 创建一个 Waker（指向 Task A 自己）
  → 把 Waker 存入自己的 waiter 节点
  → 把自己插入 waiters 链表
  → yield（暂停执行）
```

2. Sender 发消息后，`notify_rx` 从 waiter 中取出这个 Waker，调用 `waker.wake()`：

   这是把 **Task A** 提交给 Tokio 调度器，告诉它："Task A 现在可以继续执行了，请把它放回就绪队列。"

3. Tokio 调度器在合适的时机重新运行 Task A，Task A 从 `recv().await` 暂停的地方恢复：

```
Task A 恢复执行
  → 检查 slot，发现新消息
  → Ok(data) ← 拿到数据了
```

## 比喻：咖啡店

| 角色 | 比喻 | 说明 |
|------|------|------|
| Task | 顾客 | 执行任务的异步代码 |
| recv().await | "咖啡好了叫我" | 暂停等待某个条件 |
| Waiter 节点 | 等号本上的名字 | 链表中的等待记录 |
| Waker | 顾客的手机号 | 如何联系这个任务 |
| waker.wake() | 打电话"你的咖啡好了" | 通知任务可以继续了 |
| Tokio 调度器 | 顾客本人走过来 | 在合适的时机恢复执行 |

## 为什么是 waker 而不是直接执行？

两种方式的区别：

- **直接调用** = 咖啡师跑到顾客座位上，直接把咖啡塞到手里（打断顾客正在做的事）
- **waker.wake()** = 叫号"0987号！"——顾客听到后自己过来拿（协作式调度）

Tokio 使用**协作式调度**：`wake()` 只是把任务标记为"可运行"，调度器在合适的时机来 poll 这个任务。这样不会出现一个任务被另一个任务强行抢占的情况，也不会出现"唤醒者和被唤醒者抢同一把锁"的锁传递问题。

## 代码中的对应关系

```rust
// waiter 的结构
struct Waiter {
    waker: Option<Waker>,  // 存的是"怎么叫醒我"的方式
    queued: bool,           // 我是否还在等待队列里
    pointers: Pointers<Waiter>,  // 链表指针
}

// notify_rx 做的事
let waker = waiter.waker.take();  // 取出手机号
wakers.push(waker);              // 记在待拨打列表
// ...
wakers.wake_all();               // 批量拨打：全部提交给调度器
```