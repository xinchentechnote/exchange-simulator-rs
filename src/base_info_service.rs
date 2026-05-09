use matching_core::{CoreSymbolSpecification, SymbolType};
use std::str::FromStr;

pub struct MySymbolType(pub SymbolType);
impl FromStr for MySymbolType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "0" => Ok(MySymbolType(SymbolType::CurrencyExchangePair)),
            "1" => Ok(MySymbolType(SymbolType::FuturesContract)),
            "2" => Ok(MySymbolType(SymbolType::CallOption)),
            "3" => Ok(MySymbolType(SymbolType::PutOption)),
            _ => Err(format!("Unknown symbol type: {}", value)),
        }
    }
}

pub fn load_symbol_info(path: &str) -> Vec<CoreSymbolSpecification> {
    let content = std::fs::read_to_string(path).expect("Failed to read symbol info file");
    let mut result = Vec::new();
    //symbolId,type,baseCurrency,quoteCurrency,baseScaleK,
    //quoteScaleK,takerFee,makerFee,marginBuy,marginSell
    let lines = content.split("\n");
    let mut index = 0;
    for line in lines {
        index = index + 1;
        if index == 1 {
            continue;
        }
        if line.starts_with("#") || line.starts_with("//") {
            continue;
        }
        let fields: Vec<&str> = line.split(",").collect();
        // Process the fields and create a CoreSymbolSpecification instance
        // This is a simplified example - you would need to parse each field appropriately
        let symbol_spec = CoreSymbolSpecification {
            symbol_id: fields[0].parse().unwrap_or(0),
            symbol_type: fields[1]
                .parse()
                .unwrap_or(MySymbolType(SymbolType::CurrencyExchangePair))
                .0,
            base_currency: fields[2].parse().unwrap_or(0),
            quote_currency: fields[3].parse().unwrap_or(0),
            base_scale_k: fields[4].parse().unwrap_or(0),
            quote_scale_k: fields[5].parse().unwrap_or(0),
            taker_fee: fields[6].parse().unwrap_or(0),
            maker_fee: fields[7].parse().unwrap_or(0),
            margin_buy: fields[8].parse().unwrap_or(0),
            margin_sell: fields[9].parse().unwrap_or(0),
        };
        result.push(symbol_spec);
    }
    result
}

#[derive(Debug)]
pub struct UserBalance {
    //uid,currency,amount
    pub uid: u64,
    pub balance: i32,
    pub amount: i64,
}

pub fn load_account_info(path: &str) -> Vec<UserBalance> {
    let content = std::fs::read_to_string(path).expect("Failed to read user info file");
    let mut result = Vec::new();
    let lines = content.split("\n");
    let mut index = 0;
    for line in lines {
        index = index + 1;
        if index == 1 {
            continue;
        }
        if line.starts_with("#") || line.starts_with("//") {
            continue;
        }
        let fields: Vec<&str> = line.split(",").collect();
        let user_balance = UserBalance {
            uid: fields[0].parse().unwrap_or(0),
            balance: fields[1].parse().unwrap_or(0),
            amount: fields[2].parse().unwrap_or(0),
        };
        result.push(user_balance);
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::base_info_service::{MySymbolType, load_account_info, load_symbol_info};

    #[test]
    fn test_load_symbol_info() {
        let result = load_symbol_info("data/sse/symbol_data.csv");
        assert_eq!(result.len(), 41);
        println!("{:#?}", result);
    }

    #[test]
    fn test_load_account_info() {
        let result = load_account_info("data/sse/account_data.csv");
        assert_eq!(result.len(), 10);
        println!("{:#?}", result);
    }
}
