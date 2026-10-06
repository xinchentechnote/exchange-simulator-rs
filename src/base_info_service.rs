use anyhow::{Context, Result, bail};
use matching_core::{CoreSymbolSpecification, SymbolType};
use std::collections::HashSet;
use std::fs;
use std::str::FromStr;

/// CSV 标的类型映射：0 现货 / 1 期货 / 2 看涨期权 / 3 看跌期权。
pub struct MySymbolType(pub SymbolType);

impl FromStr for MySymbolType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "0" => Ok(MySymbolType(SymbolType::CurrencyExchangePair)),
            "1" => Ok(MySymbolType(SymbolType::FuturesContract)),
            "2" => Ok(MySymbolType(SymbolType::CallOption)),
            "3" => Ok(MySymbolType(SymbolType::PutOption)),
            _ => Err(format!("未知 symbolType: {value}（可选 0/1/2/3）")),
        }
    }
}

/// 读取 CSV 有效行：跳过空行与 #、// 注释行，返回 (行号, 行内容)。
/// `str::lines` 已兼容 \n 与 \r\n。
fn csv_rows(path: &str) -> Result<Vec<(usize, String)>> {
    let content = fs::read_to_string(path).with_context(|| format!("读取文件失败: {path}"))?;
    Ok(content
        .lines()
        .enumerate()
        .map(|(i, line)| (i + 1, line.to_string()))
        .filter(|(_, line)| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#') && !line.starts_with("//")
        })
        .collect())
}

fn parse_field<T: FromStr>(raw: &str, path: &str, line_no: usize, column: &str) -> Result<T> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| anyhow::anyhow!("{path}:{line_no} 列 {column} 解析失败: \"{raw}\""))
}

/// 加载标的规定，任何解析/校验错误均直接返回 Err（fail-fast）。
/// 格式：symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell
pub fn load_symbol_info(path: &str) -> Result<Vec<CoreSymbolSpecification>> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for (line_no, line) in csv_rows(path)? {
        if line.trim().starts_with("symbolId") {
            continue; // 表头
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        if fields.len() != 10 {
            bail!(
                "{path}:{line_no} 列数应为 10，实际 {}：\"{line}\"",
                fields.len()
            );
        }
        let symbol_id = parse_field(fields[0], path, line_no, "symbolId")?;
        if !seen.insert(symbol_id) {
            bail!("{path}:{line_no} 重复的 symbolId: {symbol_id}");
        }
        let symbol_type = fields[1]
            .parse::<MySymbolType>()
            .map_err(|e| anyhow::anyhow!("{path}:{line_no} 列 type {e}"))?
            .0;
        let spec = CoreSymbolSpecification {
            symbol_id,
            symbol_type,
            base_currency: parse_field(fields[2], path, line_no, "baseCurrency")?,
            quote_currency: parse_field(fields[3], path, line_no, "quoteCurrency")?,
            base_scale_k: parse_field(fields[4], path, line_no, "baseScaleK")?,
            quote_scale_k: parse_field(fields[5], path, line_no, "quoteScaleK")?,
            taker_fee: parse_field(fields[6], path, line_no, "takerFee")?,
            maker_fee: parse_field(fields[7], path, line_no, "makerFee")?,
            margin_buy: parse_field(fields[8], path, line_no, "marginBuy")?,
            margin_sell: parse_field(fields[9], path, line_no, "marginSell")?,
        };
        if spec.base_currency <= 0 || spec.quote_currency <= 0 {
            bail!("{path}:{line_no} 币种代码必须大于 0");
        }
        if spec.base_scale_k <= 0 || spec.quote_scale_k <= 0 {
            bail!("{path}:{line_no} scaleK 必须大于 0");
        }
        result.push(spec);
    }
    Ok(result)
}

/// 账户资金记录（uid,currency,amount）。
#[derive(Debug)]
pub struct UserBalance {
    pub uid: u64,
    pub currency: i32,
    pub amount: i64,
}

/// 加载账户资金，任何解析/校验错误均直接返回 Err（fail-fast）。
pub fn load_account_info(path: &str) -> Result<Vec<UserBalance>> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for (line_no, line) in csv_rows(path)? {
        if line.trim().starts_with("uid") {
            continue; // 表头
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        if fields.len() != 3 {
            bail!(
                "{path}:{line_no} 列数应为 3，实际 {}：\"{line}\"",
                fields.len()
            );
        }
        let uid = parse_field(fields[0], path, line_no, "uid")?;
        if !seen.insert(uid) {
            bail!("{path}:{line_no} 重复的 uid: {uid}");
        }
        let balance = UserBalance {
            uid,
            currency: parse_field(fields[1], path, line_no, "currency")?,
            amount: parse_field(fields[2], path, line_no, "amount")?,
        };
        if balance.currency <= 0 {
            bail!("{path}:{line_no} 币种代码必须大于 0");
        }
        if balance.amount < 0 {
            bail!("{path}:{line_no} 初始金额不能为负");
        }
        result.push(balance);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static SEQ: AtomicU32 = AtomicU32::new(0);

    fn manifest_path(rel: &str) -> String {
        format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel)
    }

    /// 在临时目录写 fixture，返回文件路径。测试不依赖工作目录。
    fn fixture(name: &str, content: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "exsim_test_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path: PathBuf = dir.join(name);
        fs::write(&path, content).unwrap();
        path.to_string_lossy().into_owned()
    }

    const SYMBOL_HEADER: &str = "symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell\n";
    const ACCOUNT_HEADER: &str = "uid,currency,amount\n";

    #[test]
    fn test_load_symbol_info_from_repo_data() {
        let result = load_symbol_info(&manifest_path("data/sse/symbol_data.csv")).unwrap();
        assert_eq!(result.len(), 41);
        assert!(result.iter().all(|s| s.symbol_id > 0));
    }

    #[test]
    fn test_load_account_info_from_repo_data() {
        let result = load_account_info(&manifest_path("data/sse/account_data.csv")).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn test_comment_before_header() {
        let path = fixture(
            "symbols.csv",
            &format!("# 注释在最前\n{SYMBOL_HEADER}600000,0,840,840,100,1,3,2,0,0\n"),
        );
        let result = load_symbol_info(&path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].symbol_id, 600000);
    }

    #[test]
    fn test_crlf_line_endings() {
        let path = fixture(
            "accounts.csv",
            "uid,currency,amount\r\n10001,840,1000000\r\n",
        );
        let result = load_account_info(&path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].amount, 1_000_000);
    }

    #[test]
    fn test_bad_field_fails_with_context() {
        let path = fixture(
            "symbols.csv",
            &format!("{SYMBOL_HEADER}600000,0,840,840,x,1,3,2,0,0\n"),
        );
        let err = format!("{}", load_symbol_info(&path).unwrap_err());
        assert!(err.contains("baseScaleK"), "错误信息应包含列名: {err}");
    }

    #[test]
    fn test_wrong_column_count_fails() {
        let path = fixture(
            "symbols.csv",
            &format!("{SYMBOL_HEADER}600000,0,840,840,100,1\n"),
        );
        assert!(load_symbol_info(&path).is_err());
    }

    #[test]
    fn test_duplicate_symbol_id_fails() {
        let path = fixture(
            "symbols.csv",
            &format!(
                "{SYMBOL_HEADER}600000,0,840,840,100,1,3,2,0,0\n600000,0,840,840,100,1,3,2,0,0\n"
            ),
        );
        let err = format!("{}", load_symbol_info(&path).unwrap_err());
        assert!(err.contains("重复"), "{err}");
    }

    #[test]
    fn test_duplicate_uid_fails() {
        let path = fixture(
            "accounts.csv",
            &format!("{ACCOUNT_HEADER}10001,840,1\n10001,840,2\n"),
        );
        assert!(load_account_info(&path).is_err());
    }

    #[test]
    fn test_negative_amount_fails() {
        let path = fixture("accounts.csv", &format!("{ACCOUNT_HEADER}10001,840,-1\n"));
        assert!(load_account_info(&path).is_err());
    }

    #[test]
    fn test_unknown_symbol_type_fails() {
        let path = fixture(
            "symbols.csv",
            &format!("{SYMBOL_HEADER}600000,9,840,840,100,1,3,2,0,0\n"),
        );
        assert!(load_symbol_info(&path).is_err());
    }

    #[test]
    fn test_empty_file_yields_empty() {
        let path = fixture("symbols.csv", "");
        assert!(load_symbol_info(&path).unwrap().is_empty());
    }

    #[test]
    fn test_missing_file_fails() {
        assert!(load_symbol_info("/nonexistent/symbols.csv").is_err());
    }
}
