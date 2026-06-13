# broadcast 为什么要用 into_boxed_slice()

```rust
let shared = Arc::new(Shared {
    buffer: buffer.into_boxed_slice(),
    ...
});
```

`Vec<T>` 内部有 3 个字段：

```
Vec<T> → (pointer, length, capacity)  = 24 字节
Box<[T]> → (pointer, length)          = 16 字节
```

broadcast 的缓冲区在初始化后就**永远不会扩容或缩容**，capacity 字段完全浪费。`into_boxed_slice()` 丢弃多余的 capacity，只保留实际使用的内存。

### 语义更准确

`Vec` 暗示"可以 push/pop、可以扩容"。而 broadcast 的缓冲区是固定大小的环形缓冲区——`Box<[T]>` 更准确地表达了"这玩意儿不会变长"。

```rust
// Vec 版本：看到 Vec 你会想"能 push 吗？"
buffer: Vec<RwLock<Slot<T>>>

// Box<[T]> 版本：一看就知道是固定大小的
buffer: Box<[RwLock<Slot<T>]>
```

### 总结

**不需要的东西就不该存在。** `Box<[T]>` 比 `Vec<T>` 小 8 字节、语义更准确、且防止误用。