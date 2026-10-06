use anyhow::{Context, Result};
use matching_core::core::exchange::{ExchangeConfig, ProducerType, WaitStrategyType};
use serde::Deserialize;
use std::fs;

/// 全进程配置，结构与 docs/DEVELOPMENT_DESIGN.md §4.5 一致。
#[derive(Debug, Deserialize)]
pub struct Config {
    pub logging: Option<LoggingConf>,
    pub sse: ExchangeConf,
    pub szse: ExchangeConf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConf {
    pub level: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConf {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// HTTP 下单接口监听端口
    pub server_port: u16,
    /// 协议网关端口
    #[serde(default)]
    pub gateway_port: Option<u16>,
    /// 协议网关类型："sse"（默认）| "szse"
    #[serde(default)]
    pub gateway_protocol: Option<String>,
    pub matcher: MatcherConf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MatcherConf {
    pub symbol_info_path: String,
    pub account_info_path: String,
    #[serde(default)]
    pub core: CoreConf,
    #[serde(default)]
    pub persistence: PersistenceConf,
}

/// 持久化配置（T3.2）：WAL 故障恢复。
/// 注意：matching-core v0.2.2 的快照仅在 startup 前可用（serialize_state 在
/// startup 后 panic），故运行期不打快照，恢复采用纯 WAL 全量重放。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PersistenceConf {
    /// WAL 文件路径；缺省关闭持久化
    pub wal_path: Option<String>,
}

/// 撮合核心参数，映射 matching-core 的 ExchangeConfig，缺省值与其 Default 一致。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct CoreConf {
    pub ring_buffer_size: Option<usize>,
    pub matching_engines_num: Option<usize>,
    pub risk_engines_num: Option<usize>,
    pub producer_type: Option<String>,
    pub wait_strategy: Option<String>,
}

fn default_true() -> bool {
    true
}

impl CoreConf {
    pub fn to_exchange_config(&self) -> Result<ExchangeConfig> {
        let mut cfg = ExchangeConfig::default();
        if let Some(v) = self.ring_buffer_size {
            cfg.ring_buffer_size = v;
        }
        if let Some(v) = self.matching_engines_num {
            cfg.matching_engines_num = v;
        }
        if let Some(v) = self.risk_engines_num {
            cfg.risk_engines_num = v;
        }
        if let Some(p) = &self.producer_type {
            cfg.producer_type = match p.as_str() {
                "Single" => ProducerType::Single,
                "Multi" => ProducerType::Multi,
                other => anyhow::bail!("未知 producer_type: {other}（可选 Single/Multi）"),
            };
        }
        if let Some(w) = &self.wait_strategy {
            cfg.wait_strategy = match w.as_str() {
                "BusySpin" => WaitStrategyType::BusySpin,
                "Yielding" => WaitStrategyType::Yielding,
                "Blocking" => WaitStrategyType::Blocking,
                "Sleeping" => WaitStrategyType::Sleeping,
                other => anyhow::bail!(
                    "未知 wait_strategy: {other}（可选 BusySpin/Yielding/Blocking/Sleeping）"
                ),
            };
        }
        Ok(cfg)
    }
}

/// 解析命令行参数，返回配置文件路径（--config <path>，默认 conf/config.toml）。
pub fn parse_args() -> String {
    let mut config_path = "conf/config.toml".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next().unwrap_or_else(|| {
                    eprintln!("--config 需要指定配置文件路径");
                    std::process::exit(2);
                });
            }
            other => {
                eprintln!("未知参数: {other}（用法: exchange-simulator-rs [--config <path>]）");
                std::process::exit(2);
            }
        }
    }
    config_path
}

pub fn load_config(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path).with_context(|| format!("读取配置失败: {path}"))?;
    toml::from_str(&content).with_context(|| format!("解析配置失败: {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_path(rel: &str) -> String {
        format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel)
    }

    #[test]
    fn test_load_config() {
        let config = load_config(&manifest_path("conf/config.toml")).unwrap();
        assert!(config.sse.enabled);
        assert_eq!(config.sse.server_port, 8080);
        assert_eq!(config.sse.gateway_port, Some(9010));
        assert_eq!(config.sse.gateway_protocol.as_deref(), Some("sse"));
        assert_eq!(
            config.sse.matcher.symbol_info_path,
            "data/sse/symbol_data.csv"
        );
        assert_eq!(
            config.sse.matcher.persistence.wal_path.as_deref(),
            Some("data/sse/wal.log")
        );
        // 阶段三：SZSE 市场启用（HTTP 8081 / 网关 9011）
        assert!(config.szse.enabled);
        assert_eq!(config.szse.server_port, 8081);
        assert_eq!(config.szse.gateway_port, Some(9011));
        assert_eq!(config.szse.gateway_protocol.as_deref(), Some("szse"));
        assert_eq!(
            config.szse.matcher.persistence.wal_path.as_deref(),
            Some("data/szse/wal.log")
        );
    }

    #[test]
    fn test_core_conf_defaults_match_matching_core() {
        let cfg = CoreConf::default().to_exchange_config().unwrap();
        let def = ExchangeConfig::default();
        assert_eq!(cfg.ring_buffer_size, def.ring_buffer_size);
        assert_eq!(cfg.matching_engines_num, def.matching_engines_num);
        assert_eq!(cfg.risk_engines_num, def.risk_engines_num);
    }

    #[test]
    fn test_core_conf_mapping() {
        let cfg = CoreConf {
            ring_buffer_size: Some(1024),
            matching_engines_num: Some(2),
            risk_engines_num: Some(2),
            producer_type: Some("Multi".into()),
            wait_strategy: Some("Blocking".into()),
        }
        .to_exchange_config()
        .unwrap();
        assert_eq!(cfg.ring_buffer_size, 1024);
        assert_eq!(cfg.matching_engines_num, 2);
        assert!(matches!(cfg.producer_type, ProducerType::Multi));
        assert!(matches!(cfg.wait_strategy, WaitStrategyType::Blocking));
    }

    #[test]
    fn test_invalid_producer_type_rejected() {
        let result = CoreConf {
            producer_type: Some("Nope".into()),
            ..Default::default()
        }
        .to_exchange_config();
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_missing_file_fails() {
        assert!(load_config("/nonexistent/config.toml").is_err());
    }
}
