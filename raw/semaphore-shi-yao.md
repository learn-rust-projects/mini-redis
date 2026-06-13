# Tokio Semaphore 是什么

```
/// 一个计数信号量，用于异步获取许可。
///
/// 信号量维护一组许可。许可用于同步对共享资源的访问。
/// 信号量与互斥锁的区别在于，它允许同时有多个并发调用者访问共享资源。
///
/// 当调用 `acquire` 且信号量还有剩余许可时，函数立即返回一个许可。
/// 但如果已经没有剩余许可，`acquire` 会（异步地）等待，直到某个持有的许可被 drop。
/// 此时，被释放的许可会分配给调用者。
///
/// 这个 `Semaphore` 是公平的，意味着许可是按照请求顺序分配的。
/// 这种公平性同样适用于 `acquire_many`。如果队列前面的 `acquire_many`
/// 请求的许可数超过了当前可用数，它会阻塞后面的 `acquire` 调用，
/// 即使信号量有足够完成 `acquire` 的许可也不行。
///
/// 要在 poll 函数中使用 `Semaphore`，可以使用 [`PollSemaphore`] 工具。
```

原文翻译，也是文档的第一段。对应到停车场比喻：车位就是许可，车就是调用者，先到的车先停。

## 什么叫"公平"？

"公平"在这里是 **FIFO（先进先出）** 的意思——许可以请求到达的顺序分配，而不是谁运气好就先给谁。

### 场景：公平带来的"看似不公平"

```
当前剩余 2 个许可

等待队列（按请求时间排序）:
  [1号: 申请 10 个] → [2号: 申请 1 个] → [3号: 申请 3 个]

有人归还了 5 个许可，现在剩余 7 个:
  1号要 10 个 → 还不够，继续等
  2号要 1 个 → 但 1 号在前面！2 号也得等
  3号要 3 个 → 1 号在前面，继续等
```

**即使有 7 个许可、2 号只需要 1 个，2 号也得等 1 号先拿到许可。** 这就是注释说的"如果队列前面的 `acquire_many` 请求的许可数超过了当前可用数，它会阻塞后面的 `acquire` 调用，即使信号量有足够完成 `acquire` 的许可也不行。"

### 如果不公平会怎样？

不公平的信号量会"跳队"：

```
当前剩余 2 个许可

等待队列:
  [1号: 申请 10 个] → [2号: 申请 1 个]

有人归还 1 个，现在有 3 个:
  不公平实现 → 跳过 1 号，先给 2 号（因为 2 号要得少）
  2 号拿到许可走了
  1 号继续等
```

这对 1 号不公平——明明你先来的，却让别人插队了。

### 公平的代价

公平保证**请求顺序**，代价是**吞吐量**——大的请求会阻塞后面所有请求，即使当前有足够的资源完成小请求。

| | 公平实现（Tokio） | 不公平实现 |
|--|------------------|-----------|
| 分配顺序 | FIFO | 谁先能满足就给谁 |
| 大请求 | 不会饿死 | 可能永远被小请求插队 |
| 吞吐量 | 可能因队头阻塞降低 | 小请求可以绕过，总体高 |
| 适用场景 | 所有请求同等重要 | 优先完成小/快速请求 |

Tokio 选择了公平，确保最大的请求不会饿死。

## 一句话

**Semaphore 是一个异步信号量，用来限制对共享资源的并发访问数。**

## 比喻：停车场

```
Semaphore(3) = 一个只有 3 个车位的停车场

车 A 来了 → 有空位（3→2），直接进
车 B 来了 → 还有空位（2→1），直接进
车 C 来了 → 还有空位（1→0），直接进

车 D 来了 → 没空位了 → 在门口排队等
车 E 来了 → 没空位了 → 在 D 后面排队等

车 A 走了 → 空出 1 个位（0→1），叫 D 进来
车 D 走了 → 空出 1 个位（1→2），叫 E 进来
```

对应到代码：

| 停车场 | Semaphore |
|--------|----------|
| 车位总数 | `Semaphore::new(3)` — 初始许可数 |
| 车进场 | `acquire().await` — 取走一个许可 |
| 车出场 | `drop(permit)` — 归还许可 |
| 排队 | 自动的 FIFO 等待队列 |

## Semaphore vs Mutex

| Mutex | Semaphore |
|-------|-----------|
| 一次只允许 1 个访问 | 一次允许 N 个访问 |
| 锁 | 许可（Permit） |
| 必须由持有者解锁 | 任何任务都可以 drop permit |
| 保护共享数据 | 控制访问速率 / 限制并发数 |

## 两种 acquire

```rust
// 拿 1 个许可
let permit = semaphore.acquire().await.unwrap();

// 拿 N 个许可
let permits = semaphore.acquire_many(5).await.unwrap();

// 不阻塞的版本
let permit = semaphore.try_acquire();
```

## acquire_owned — 跨任务传递许可

```rust
let semaphore = Arc::new(Semaphore::new(3));

// acquire_owned 返回 OwnedPermit，不依赖生命周期
let permit = semaphore.clone().acquire_owned().await.unwrap();

tokio::spawn(async move {
    // 在这个任务里持有 permit
    do_work().await;
    drop(permit); // 显式归还
});
```

`acquire()` 返回的 `SemaphorePermit<'_, Semaphore>` 有生命周期约束，不能跨 `spawn` 发送。`acquire_owned()` 返回的 `OwnedPermit` 没有生命周期，可以跨任务传递。

## 公平性

注释说：

> This `Semaphore` is fair, which means that permits are given out in the order they were requested.

FIFO 队列。如果前面有人申请 10 个许可但不够，后面的人即使只需要 1 个也得等——即使当前有足够的许可完成这个小请求。

```
当前剩余 2 个许可

等待队列: [申请 10 个] → [申请 1 个] → [申请 3 个]

来了 5 个新许可（有人归还了）：
  还不够 10 个 → 继续等，后面的人也得等
```

这是"公平"的代价——大的请求会阻塞后面的小请求，但保证了请求顺序的公平性。

## 在 mini-redis 中的使用

mini-redis 用 `Semaphore` 做**连接数限制**：

```rust
// server.rs
let limiter = Arc::new(Semaphore::new(max_connections));

// 每个新连接
let permit = limiter.clone().acquire_owned().await?;
// permit 在这个连接的 handler 中持有
// 连接关闭时 permit drop，释放一个许可
```

这样保证了同时处理的连接数不超过 `max_connections`。

## 总结

- **Semaphore = 异步许可计数器**，限制并发访问数
- **acquire** 没许可时排队等，有许可时减 1 返回
- **drop(permit)** 归还许可，唤醒队列中的下一个
- **公平 FIFO**：按请求顺序分配
- mini-redis 用它限制最大连接数