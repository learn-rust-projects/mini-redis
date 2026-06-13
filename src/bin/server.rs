//! mini-redis 服务器。
//!
//! 这个文件是库中实现的服务器的入口点。
//! 它执行命令行解析并将参数传递给 `mini_redis::server`。
//!
//! `clap` crate 用于解析参数。

use mini_redis::{server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(feature = "otel")]
// 为了能够设置 XrayPropagator。
use opentelemetry::global;
#[cfg(feature = "otel")]
// 为了配置某些选项，如采样率。
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// 为了跨服务传递相同的 XrayId。
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
// `Ext` trait 用于允许 Registry 接受 OpenTelemetry 特定的类型
// （例如 `OpenTelemetryLayer`）。
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 绑定一个 TCP 监听器。
    let listener = TcpListener::bind(&format!("127.0.0.1:{port}")).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    // 有关更多信息，请参见 https://docs.rs/tracing。
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    // 将全局传播器设置为 X-Ray 传播器。
    // 注意：如果需要在同一跟踪中跨服务传递 x-amzn-trace-id，
    // 则需要这一行。然而，这需要此处未展示的额外代码。
    // 关于使用 hyper 的完整示例，请参见:
    // https://github.com/open-telemetry/opentelemetry-rust/blob/v0.19.0/examples/aws-xray/src/server.rs#L14-L26
    global::set_text_map_propagator(XrayPropagator::default());

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                // 需要将跟踪 ID 转换为 Xray 兼容的格式。
                .with_id_generator(sdktrace::XrayIdGenerator::default()),
        )
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    // 使用配置的跟踪器创建一个 tracing 层。
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // 从 `RUST_LOG` 环境变量中解析 `EnvFilter` 配置。
    let filter = EnvFilter::from_default_env();

    // 使用 tracing subscriber `Registry` 或任何其他实现了 `LookupSpan` 的 subscriber。
    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}