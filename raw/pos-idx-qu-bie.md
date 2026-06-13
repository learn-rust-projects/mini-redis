# 为什么 pos 不直接存 idx，每次都用 `pos & mask` 计算？

```rust
let pos = tail.pos;
let rem = tail.rx_cnt;
let idx = (pos & self.shared.mask as u64) as usize;
```

**`pos` 是全局唯一的消息序号，`idx` 是环形缓冲区的位置，两者用途不同。**

如果只存 `idx`，receiver 没法区分"槽位里是新消息还是上次那条旧消息"。以 `capacity=4` 为例：

```
send 5 条消息后:

        pos:  0  1  2  3  4
        idx:  0  1  2  3  0   ← idx 重复了
```

receiver 上次读到 `pos=0`，现在看到 `idx=0` 的槽位里有数据——它怎么知道这是 `pos=4` 的新消息还是 `pos=0` 的旧消息？无法判断。

有了 `pos`，receiver 只需要比较 `rx.next` 和 `slot.pos`：

- `rx.next=0`, `slot.pos=4` → 差了 4 条 → `Lagged(4)`
- `rx.next=4`, `slot.pos=4` → 无 lag → 可以读

而且 `pos & mask` 是一条位运算指令，成本可以忽略不计。

## 一句话总结

**`pos` 是消息的身份证号，`idx` 是它在环形缓冲区里的座位号。** 身份证号用来追查你是否错过了消息，座位号用来找到数据实际放在哪里。两者缺一不可。