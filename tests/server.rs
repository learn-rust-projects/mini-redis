use mini_redis::server;

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};

/// 一个基本的"hello world"风格的测试。在后台任务中启动一个服务器实例。
/// 然后建立客户端 TCP 连接，并向服务器发送原始 redis 命令。
/// 在字节级别评估响应。
#[tokio::test]
async fn key_value_get_set() {
    let addr = start_server().await;

    // 建立到服务器的连接。
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 获取一个键，数据不存在。
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // 读取 nil 响应。
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);

    // 设置一个键。
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    // 读取 OK。
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // 获取这个键，数据存在。
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // 关闭写入端。
    stream.shutdown().await.unwrap();

    // 读取 "world" 响应。
    let mut response = [0; 11];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nworld\r\n", &response);

    // 接收 `None`。
    assert_eq!(0, stream.read(&mut response).await.unwrap());
}

/// 类似于基本的键值测试，但是这次将测试超时。该测试演示了如何测试
/// 与时间相关的行为。
///
/// 编写测试时，消除非确定性来源是很有用的。时间就是非确定性的来源。
/// 在这里，我们使用 `time::pause()` 函数来"暂停"时间。
/// 此函数通过 `test-util` 特性标志可用。这使我们能够以确定性的方式
/// 控制时间对应用程序的推进方式。
#[tokio::test]
async fn key_value_timeout() {
    tokio::time::pause();

    let addr = start_server().await;

    // 建立到服务器的连接。
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 设置一个键。
    stream
        .write_all(
            b"*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n\
                     +EX\r\n:1\r\n",
        )
        .await
        .unwrap();

    let mut response = [0; 5];

    // 读取 OK。
    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+OK\r\n", &response);

    // 获取这个键，数据存在。
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // 读取 "world" 响应。
    let mut response = [0; 11];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"$5\r\nworld\r\n", &response);

    // 等待键过期。
    time::advance(Duration::from_secs(1)).await;

    // 获取一个键，数据不存在。
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // 读取 nil 响应。
    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"$-1\r\n", &response);
}

#[tokio::test]
async fn pub_sub() {
    let addr = start_server().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();

    // 发布一条消息，还没有订阅者，因此服务器将返回 `0`。
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":0\r\n", &response);

    // 创建一个订阅者。该订阅者只订阅 `hello` 频道。
    let mut sub1 = TcpStream::connect(addr).await.unwrap();
    sub1.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // 读取订阅响应。
    let mut response = [0; 34];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
        &response[..]
    );

    // 发布一条消息，现在有一个订阅者。
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // 第一个订阅者收到了消息。
    let mut response = [0; 39];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..],
        &response[..]
    );

    // 创建第二个订阅者。
    //
    // 该订阅者将同时订阅 `hello` 和 `foo`。
    let mut sub2 = TcpStream::connect(addr).await.unwrap();
    sub2.write_all(b"*3\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    // 读取订阅响应。
    let mut response = [0; 34];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
        &response[..]
    );
    let mut response = [0; 32];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:2\r\n"[..],
        &response[..]
    );

    // 在 `hello` 上发布另一条消息，现在有两个订阅者。
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\njazzy\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":2\r\n", &response);

    // 在 `foo` 上发布一条消息，只有一个订阅者。
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    publisher.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // 第一个订阅者收到了消息。
    let mut response = [0; 39];
    sub1.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\njazzy\r\n"[..],
        &response[..]
    );

    // 第二个订阅者收到了消息。
    let mut response = [0; 39];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\njazzy\r\n"[..],
        &response[..]
    );

    // 第一个订阅者 **没有** 收到第二条消息。
    let mut response = [0; 1];
    time::timeout(Duration::from_millis(100), sub1.read(&mut response))
        .await
        .unwrap_err();

    // 第二个订阅者 **确实** 收到了消息。
    let mut response = [0; 35];
    sub2.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..],
        &response[..]
    );
}

#[tokio::test]
async fn manage_subscription() {
    let addr = start_server().await;

    let mut sub = TcpStream::connect(addr).await.unwrap();

    // 订阅到 foo 频道。
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    // 阅读订阅确认信息。
    let mut response = [0; 32];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"[..],
        &response[..]
    );

    // 取消订阅 foo 频道。
    sub.write_all(b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    // 阅读取消订阅确认信息。
    let mut response = [0; 34];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n"[..],
        &response[..]
    );

    // 现在重新订阅。
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    // 阅读订阅确认信息。
    let mut response = [0; 32];
    sub.read_exact(&mut response).await.unwrap();
    assert_eq!(
        &b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"[..],
        &response[..]
    );
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}