# send 是什么时候写入的？

代码：

```rust
let (tx, mut rx) = broadcast::channel(1);

tx.send(1).unwrap();
match tx.send(2) {
    Err(e) => println!("{:?}", e),
    Ok(_) => {}
}
println!("收到: {:?}", rx.recv().await);
```

调用 `tx.send(2)` 时，数据**立即写入**缓冲区，不是在 recv 时才写。

## 实际行为

`send()` 从不因为缓冲区满而返回 Err。`send()` 返回 Err 的唯一条件是 `rx_cnt == 0`（没有任何 receiver）。缓冲区满时它直接覆盖旧数据，慢 receiver 会收到 `Lagged`。

对 `broadcast::channel(1)`：

```rust
tx.send(1)  // 写入槽位 pos=0，返回 Ok(1)  （有1个receiver）
tx.send(2)  // 覆盖同一槽位 pos=1，返回 Ok(1)  （不是Err！）
rx.recv()   // 发现跳过了1条消息，返回 Err(Lagged(1))
```

## send(2) 的写入流程

```
send(2):
  1. lock(tail)         → rx_cnt=1 > 0，继续
  2. pos = tail.pos     → pos=1
  3. tail.pos += 1      → 写入序号+1
  4. lock(slot[0])      → 容量为1，只有这一个槽位
  5. slot.pos = 1       → 覆盖旧消息序号
  6. slot.val = Some(2) → 覆盖旧数据
  7. unlock(slot)
  8. notify_rx(tail)    → 唤醒 receiver
  9. return Ok(1)       → 成功
```

数据**已经写进缓冲区了**。

## 关键区分

- **`send` 返回 Err** = 没有任何 receiver（数据退回）
- **`recv` 返回 Err(Lagged(n))** = 读得太慢，错过了 n 条消息

正确的代码应该是：

```rust
let (tx, mut rx) = broadcast::channel(1);

tx.send(1).unwrap();          // Ok(1)，写入 pos=0
tx.send(2).unwrap();          // Ok(1)，覆盖写入 pos=1（同一个槽位）

match rx.recv().await {
    Err(e) => println!("收到: {:?}", e), // Err(Lagged(1))
    Ok(v) => println!("收到: {:?}", v),
}

// 再读一遍才能拿到 2
println!("收到: {:?}", rx.recv().await); // Ok(2)
```