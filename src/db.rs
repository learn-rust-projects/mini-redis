use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// 一个 `Db` 实例的包装器。存在是为了实现有序清理，
/// 当此结构体被丢弃时，通过通知后台清理任务关闭。
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// 当此 `DbDropGuard` 结构体被丢弃时将关闭的 `Db` 实例。
    db: Db,
}

/// 所有连接共享的服务器状态。
///
/// `Db` 包含一个存储键/值数据的 `HashMap` 以及所有活动 pub/sub 通道的
/// `broadcast::Sender` 值。
///
/// `Db` 实例是共享状态的句柄。克隆 `Db` 是浅拷贝，
/// 只增加原子引用计数。
///
/// 当创建 `Db` 值时，会生成一个后台任务。该任务用于
/// 在请求的持续时间过后使值过期。该任务一直运行到所有 `Db` 实例
/// 都被丢弃，此时任务终止。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 共享状态的句柄。后台任务也会有一个 `Arc<Shared>`。
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由互斥锁保护。这里使用 `std::sync::Mutex` 而不是
    /// Tokio 互斥锁，因为在持有锁期间没有执行异步操作。
    /// 此外，临界区非常小。
    ///
    /// Tokio 互斥锁主要用于需要在 `.await` 暂停点跨越锁的持有期的情况。
    /// 其他情况通常最好使用 std 互斥锁。如果临界区不包含任何异步操作
    /// 但耗时较长（CPU 密集型或执行阻塞操作），则整个操作（包括等待互斥锁）
    /// 被认为是"阻塞"操作，应使用 `tokio::task::spawn_blocking`。
    state: Mutex<State>,

    /// 通知处理条目过期的后台任务。后台任务在此等待通知，
    /// 然后检查过期值或关闭信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 键值数据。我们不打算做任何花哨的事情，所以
    /// `std::collections::HashMap` 就足够了。
    entries: HashMap<String, Entry>,

    /// pub/sub 键空间。Redis 使用 **独立** 的键空间来存储键值
    /// 和 pub/sub。`mini-redis` 通过使用独立的 `HashMap` 来处理。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 跟踪键的 TTL。
    ///
    /// 使用 `BTreeSet` 来维护按过期时间排序的到期记录。
    /// 这允许后台任务迭代此映射以查找下一个过期的值。
    ///
    /// 虽然极不可能，但可能在同一个时刻创建多个到期记录。
    /// 因此，`Instant` 不足以作为键。使用唯一键（`String`）来打破这些平局。
    expirations: BTreeSet<(Instant, String)>,

    /// 当所有 `Db` 值都丢弃时，设置为 true。设置此值为 `true` 表示
    /// 后台任务退出。当 Db 实例关闭时为真。
    shutdown: bool,
}

/// 键值存储中的条目
#[derive(Debug)]
struct Entry {
    /// 存储的数据
    data: Bytes,

    /// 条目过期并从数据库中删除的时刻。
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个新的 `DbDropGuard`，包装一个 `Db` 实例。当这个被丢弃时，
    /// `Db` 的清理任务将被关闭。
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享数据库。内部是一个 `Arc`，因此克隆只增加引用计数。
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 通知 `Db` 实例关闭负责清理过期键的任务。
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建新的空 `Db` 实例。分配共享状态并生成一个
    /// 后台任务来管理键过期。
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 启动后台任务。
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 获取与键关联的值。
    ///
    /// 如果没有与该键关联的值，返回 `None`。可能是因为从未给键赋值，
    /// 或者之前赋的值已过期。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取锁，获取条目并克隆值。
        //
        // 因为数据使用 `Bytes` 存储，这里的克隆是浅克隆。
        // 数据不会被复制。
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置与键关联的值，以及与可选的过期 Duration。
    ///
    /// 如果键已经关联了一个值，该值会被移除。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // 如果这个 `set` 成为 **下一个** 过期的键，则需要通知
        // 后台任务以更新其状态。
        //
        // 在 `set` 过程中计算是否需要通知任务。
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // 键过期的 `Instant`。
            let when = Instant::now() + duration;

            // 仅当新插入的过期时间是 **下一个** 要驱逐的键时才
            // 通知工作线程。在这种情况下，需要唤醒工作线程以更新其状态。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // 将条目插入 `HashMap`。
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // 如果以前有值与这个键关联 **并且** 它有过期时间。
        // `expirations` 映射中的关联条目也必须被移除。这避免了数据泄漏。
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // 清除过期记录。
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // 跟踪过期时间。如果我们在移除之前插入，当当前 `(when, key)`
        // 等于之前的 `(when, key)` 时会导致错误。先移除后插入可以避免此问题。
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // 在通知后台任务之前释放互斥锁。这有助于减少竞争，
        // 因为后台任务被唤醒后可能因为此函数仍持有锁而无法获取锁。
        drop(state);

        if notify {
            // 最后，仅当后台任务需要更新其状态以反映新的过期时间时
            // 才通知它。
            self.shared.background_task.notify_one();
        }
    }

    /// 返回请求的频道的 `Receiver`。
    ///
    /// 返回的 `Receiver` 用于接收由 `PUBLISH` 命令广播的值。
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取互斥锁。
        let mut state = self.shared.state.lock().unwrap();

        // 如果请求的频道没有条目，则创建一个新的广播通道
        // 并将其与键关联。如果已经存在，则返回关联的接收器。
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // 广播通道尚不存在，因此创建一个。
                //
                // 通道容量为 `1024` 条消息。一条消息会一直存储在
                // 通道中，直到 **所有** 订阅者都看到了它。这意味着
                // 慢速订阅者可能导致消息被无限期保存。
                //
                // 当通道容量满时，发布将导致旧消息被丢弃。这可以防止
                // 慢速消费者阻塞整个系统。
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 向频道发布一条消息。返回监听该频道的订阅者数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 在广播通道上成功发送消息时，返回订阅者数量。
            // 错误表示没有接收者，此时应返回 `0`。
            .map(|tx| tx.send(value).unwrap_or(0))
            // 如果频道键没有条目，则没有订阅者。此时返回 `0`。
            .unwrap_or(0)
    }

    /// 通知清理后台任务关闭。由 `DbShutdown` 的 `Drop` 实现调用。
    fn shutdown_purge_task(&self) {
        // 必须通知后台任务关闭。通过设置 `State::shutdown` 为 `true`
        // 并通知任务来实现。
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 在通知后台任务之前释放锁。这有助于减少锁竞争，
        // 确保后台任务不会被唤醒后却无法获取互斥锁。
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// 清除所有过期的键，并返回 **下一个** 键过期的 `Instant`。
    /// 后台任务将休眠直到此时刻。
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // 数据库正在关闭。共享状态的所有句柄都已丢弃。
            // 后台任务应该退出。
            return None;
        }

        // 这是为了让借用检查器满意。简而言之，`lock()` 返回一个 `MutexGuard`
        // 而不是 `&mut State`。借用检查器无法"看穿"互斥锁保护，
        // 确定同时可变访问 `state.expirations` 和 `state.entries` 是安全的，
        // 所以我们在线程外获取 `State` 的"真正"可变引用。
        let state = &mut *state;

        // 查找所有计划在 **现在之前** 过期的键。
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // 清理完成，`when` 是下一个键过期的时刻。
                // 工作线程将等待直到此时刻。
                return Some(when);
            }

            // 键已过期，移除它。
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// 如果数据库正在关闭，返回 `true`。
    ///
    /// 当所有 `Db` 值都已被丢弃时，`shutdown` 标志被设置，
    /// 表明共享状态不能再被访问。
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// 由后台任务执行的例程。
///
/// 等待被通知。收到通知后，从共享状态句柄中清除任何过期键。
/// 如果设置了 `shutdown`，则终止任务。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果关闭标志被设置，则任务应该退出。
    while !shared.is_shutdown() {
        // 清除所有已过期的键。该函数返回 **下一个** 键过期的
        // 时刻。工作线程应等待到此时刻过去，然后再次清理。
        if let Some(when) = shared.purge_expired_keys() {
            // 等待直到下一个键过期 **或** 直到后台任务被通知。
            // 如果任务被通知，则必须重新加载其状态，因为新的键已
            // 被设置为提前过期。这是通过循环来实现的。
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 未来没有键要过期。等待直到任务被通知。
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}