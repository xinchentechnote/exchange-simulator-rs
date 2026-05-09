use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub sse: ExchangeConf,
    pub szse: ExchangeConf,
}

#[derive(Debug, Deserialize)]
pub struct ExchangeConf {
    pub server_port: u16,
    pub matcher: MatcherConf,
}

#[derive(Debug, Deserialize)]
pub struct MatcherConf {
    pub symbol_info_path: String,
    pub account_info_path: String,
}

pub fn load_config(path: &str) -> Config {
    let content = fs::read_to_string(path).expect("Failed to read config file");
    toml::from_str(&content).expect("Failed to parse config file")
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_load_config() {
        let config = super::load_config("conf/config.toml");
        println!("{:#?}", config);
        assert_eq!(config.sse.server_port, 9010);
        assert_eq!(config.szse.server_port, 9011);
    }
}
