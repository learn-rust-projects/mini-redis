# 类型级权限守卫：_tail 参数的真正作用

```rust
fn pop_back_locked(&mut self, _tail: &mut Tail) -> Option<NonNull<Waiter>> {
    let result = self.list.pop_back();
    if result.is_none() {
        self.is_empty = true;
    }
    result
}
```

## _tail 在函数体里完全没用

注意参数名前缀 `_`——Rust 编译器不会警告"未使用"。它在函数体中一次都没有被引用过。

**那为什么要传？**

## 类型级权限守卫

`_tail: &mut Tail` 的语义不是"函数需要这个参数"，而是"**调用者必须持有 `tail` 锁才能调我**"。

`MutexGuard<'_, Tail>` 实现了 `DerefMut<Target = Tail>`，所以调用方传的是：

```rust
list.pop_back_locked(&mut tail)  // tail: MutexGuard<'_, Tail>
```

如果调用者没持有 tail 锁，就没有 `MutexGuard`，就拿不到 `&mut Tail`，编译直接拒绝：

```
error[E0308]: mismatched types
  → ... pop_back_locked(&mut something_else)
                        doesn't have type &mut Tail
```

## 运行时 vs 编译期

| 方案 | 做法 | 问题 |
|------|------|------|
| 运行时检查 | 内部放一个 `bool locked`，操作前判断 | 可能忘了设标记、并发 bug |
| 类型守卫 | 用 `&mut Tail` 要求调用者持有锁 | 编译器保证，零开销 |

## Tokio 中的同类模式

- `PointerInner` 用 `#[repr(C)]` + 裸指针偏移**避免编译器 noalias 优化错误**
- `&mut Tail` 参数用类型系统**在编译期强制执行锁持有**

思路一致：**能用编译器解决的问题，绝不留到运行时。**