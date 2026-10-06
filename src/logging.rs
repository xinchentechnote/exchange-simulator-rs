use crate::config::LoggingConf;
use tracing_subscriber::EnvFilter;

/// 初始化全局日志：优先 RUST_LOG 环境变量，否则取配置的 level（默认 info）。
pub fn init(conf: Option<&LoggingConf>) {
    let level = conf
        .and_then(|c| c.level.clone())
        .unwrap_or_else(|| "info".to_string());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
