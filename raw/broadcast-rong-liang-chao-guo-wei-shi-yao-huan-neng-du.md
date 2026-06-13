# broadcast 容量超过为什么还能正常读到

代码：

```rust
let (tx, mut rx) = broadcast::channel(1);

tx.send(1).unwrap();
tx.send(2).unwrap();  // 覆盖槽位

// 输出:
// Err(Lagged(1))
// Ok(2)
println!("收到: {:?}", rx.recv().await);
println!("收到: {:?}", rx.recv().await);
```

**核心理解：`channel(1)` 的容量 1 不是说"只能发一条消息"，而是"缓冲区只能暂存 1 条消息"。**

`send()` 永远不会因为缓冲区满而阻塞或失败——它直接覆盖旧数据。调用两次 `send()` 都成功写入了，只是第一条被覆盖了。

## 两次 recv 分别发生了什么

```
初始: rx.next = 0, tail.pos = 0, 槽位空

tx.send(1): 写入 pos=0, tail.pos → 1
tx.send(2): 覆盖同一槽位 pos=1, tail.pos → 2

第一次 rx.recv():
  rx.next = 0, slot.pos = 1
  lag = 1 - 0 = 1 > 0
  → Err(Lagged(1)), 并将 rx.next 调整到"最旧的可读位置"

  "最旧的可读位置" = tail.pos - capacity = 2 - 1 = 1
  rx.next = 1

第二次 rx.recv():
  rx.next = 1, slot.pos = 1
  rx.next >= slot.pos → 可以读
  → Ok(2), rx.next = 2
```

第三次调用 `rx.recv()` 时：`rx.next=2` 而 `tail.pos=2`，没有新数据，会**阻塞等待**。

## 三次 send（用户实际运行）

```rust
let (tx, mut rx) = broadcast::channel(1);

tx.send(1).unwrap();
tx.send(2).unwrap();
tx.send(3).unwrap();

// 输出:
// Err(Lagged(2))
// Ok(3)
// （第三次 recv 阻塞）
println!("收到: {:?}", rx.recv().await);
println!("收到: {:?}", rx.recv().await);
println!("收到: {:?}", rx.recv().await);  // ← 阻塞
```

追踪：

```
初始: rx.next = 0, tail.pos = 0

send(1): 写入 pos=0, tail.pos → 1
send(2): 覆盖 pos=1, tail.pos → 2  (值1被丢弃)
send(3): 覆盖 pos=2, tail.pos → 3  (值2被丢弃)

第一次 recv:
  rx.next = 0, slot.pos = 2
  lag = 2 - 0 = 2  → 错过的消息比上次更多了
  → Err(Lagged(2))
  rx.next 调整到"最旧的可读位置" = tail.pos - capacity = 3 - 1 = 2

第二次 recv:
  rx.next = 2, slot.pos = 2
  → Ok(3), rx.next = 3

第三次 recv:
  rx.next = 3, tail.pos = 3 → 无新数据 → 阻塞
```

## 核心原因：lag 是计数器，不是缓冲区剩余条数

`Lagged(n)` 的 `n` 是**自从你上次读取以来，你错过了多少条消息**，而不是"缓冲区还剩多少条给你读"。

```
send 1次 → Lagged(1)  → 错过了消息1
send 2次 → Lagged(2)  → 错过了消息1和消息2
send N次 → Lagged(N)  → 错过了消息1到消息N
```

无论 capacity 是多少，lag 只增不减（如果你一直不读）。capacity 决定的是"缓冲区能保留几条历史消息"，而不是"你能错过几条"。

## 为什么 Lagged(N) 后还能读到 Ok

因为虽然你错过了 N 条消息，但**最新一条数据就在槽位里**。`Err(Lagged)` 只是告诉你"你漏了一些"，然后帮你调整位置到当前可读的最新消息——你下一次 `recv()` 就能拿到它。

## 一句话总结

**`Lagged(n)` 是历史欠账计数器，不是缓冲区空位指示器。** 容量 1 只限制缓冲区深度，不限制 lag 值。你发 100 次才读，就是 `Lagged(99)`，但第 100 条消息仍然能正常读到 `Ok(100)`。