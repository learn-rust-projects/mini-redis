# raw — 目录

> 阅读 mini-redis 和 Tokio 源码过程中的问答笔记

## mini-redis 源码理解

- [Listener 的层层封装](listener-de-cc-ceng-feng-zhuang.md) — Listener 五层职责拆解，关闭流程如何利用 drop 语义
- [Server 架构图](server-jia-gou-tu.md) — Listener → Handler → 关闭流程的完整架构图
- [读源码的最佳方式](du-yuan-ma-de-zui-jia-fang-shi.md) — "抓两头，堵中间"的源码阅读方法论

## Tokio 源码分析

- [Tokio 侵入式链表 LinkedList 源码分析](tokio-intrusive-linkedlist-fen-xi.md)
- [signal::ctrl_c 翻译](tokio-ctrl_c-fan-yi.md) — 平台兼容的信号监听 API，Caveats 说明
- [broadcast::channel 翻译](tokio-broadcast-channel-fan-yi.md) — 一发多收的通道，Panics 条件
- [Sender::new_with_receiver_count 翻译](tokio-sender-new-with-receiver-count-fan-yi.md) — 底层构造方法，Safety 要求
- [Sender::send 翻译](tokio-sender-send-fan-yi.md) — 发送语义，Ok/Err 的注意点

## Tokio broadcast 原理

- [capacity.next_power_of_two() 含义](capacity-next-power-of-two.md) — 环形缓冲区位运算优化
- [Slot 初始化 pos 为什么是负数](slot-chu-shi-hua-pos-wei-shi-yao-shi-fu-shu.md) — 全局序号 vs 已读位置的跟踪机制
- [Shared 结构体字段含义](shared-jie-gou-ti-zi-duan-han-yi.md) — mask、tail、num_tx、waiters 等字段解读
- [Sender::send 执行流程](sender-send-liu-cheng.md) — 写入序号、槽位填充、通知 receiver 的完整路径
- [send 是什么时候写入的](sender-send-shi-yao-shi-hou-xie-ru.md) — send 立即写入缓冲区，从不因满返回 Err；慢 receiver 得到 Lagged
- [容量超过了为什么还能正常读](broadcast-rong-liang-chao-guo-wei-shi-yao-huan-neng-du.md) — broadcast 是环形缓冲区，send 覆盖旧数据，最新消息永远可读
- [为什么用 into_boxed_slice](broadcast-wei-shi-yao-yong-into-boxed-slice.md) — 省掉 Vec 多余的 capacity 字段，减重 8 字节，语义更准确
- [为什么 pos 不直接存 idx](pos-idx-qu-bie.md) — 全局消息序号与缓冲区位置的区别，两者缺一不可
- [为什么必须先释放 slot 写锁，再释放 tail 锁](suo-shi-fang-shun-xu.md) — 两个锁释放顺序的竞态分析与正确理由
- [notify_rx 源码逐行解析](notify-rx-yuan-ma-jie-xi.md) — 批量唤醒 waiter 的完整流程与设计决策
- [pop_back_locked 逐行解析](pop-back-locked-jie-xi.md) — is_empty 优化标记与类型级安全守卫
- [into_guarded 解析](into-guarded-jie-xi.md) — 普通链表转为守卫循环链表的完整过程
- [类型级权限守卫](lei-xing-ji-quan-xian-shou-wei.md) — \_tail 参数用编译器保证锁持有，零运行时开销
- [Waker 到底唤醒了什么](waker-huan-xing-le-shi-yao.md) — waker.wake() 把任务提交给调度器恢复执行
- [Recv Future 与 recv_ref 逐行解析](recv-ref-yuan-ma-jie-xi.md) — 从注册等待到读取消息的完整路径
- [trace_leaf 解析](trace-leaf-jie-xi.md) — ready!(trace_leaf(cx)) 默认零开销，仅在 taskdump 时生效
- [锁顺序如何避免死锁](suo-shun-xu-bi-mian-si-suo.md) — recv_ref 先释放 slot 再获取 tail 防止交叉死锁
- [释放锁后为什么要重新检查 slot.pos](suo-shi-fang-hou-chong-xin-jian-cha.md) — 防止并发写入导致 receiver 永远挂起
- [Tokio Semaphore 是什么](semaphore-shi-yao.md) — 异步信号量，限制共享资源并发访问数
- [acquire_owned 解析](acquire-owned-jie-xi.md) — Arc 版本 acquire，返回 OwnedPermit 可跨 spawn 传递
- [最大连接数的限制](zui-da-lian-jie-shu-xian-zhi.md) — accept 队列、fd、内存、内核参数如何决定连接上限
- [broadcast 为什么需要 T: Clone](broadcast-wei-shi-yao-yao-qiu-T-Clone.md) — 一发多收的数据分发与所有权