use serde::Deserialize;
use validator::Validate;

/// POST /place 请求体。
/// 字段语义与校验规则见 docs/DEVELOPMENT_DESIGN.md §4.2 / §5.1。
#[derive(Debug, Deserialize, Validate)]
pub struct OrderRequest {
    #[validate(range(min = 1, message = "order_id 必须大于 0"))]
    pub order_id: u64,

    #[validate(range(min = 1, message = "user_id 必须大于 0"))]
    pub user_id: u64,

    pub action: String, // 1=买(Bid) 2=卖(Ask)，其余值返回 400

    pub order_type: String, // GTC/IOC/FOK/FOK_BUDGET/IOC_BUDGET/POST_ONLY/DAY/GTD:<unix_ts>

    #[validate(range(min = 1, message = "price 必须大于 0"))]
    pub price: i64,

    #[validate(range(min = 1, message = "size 必须大于 0"))]
    pub size: i64,

    #[validate(range(min = 1, message = "symbol 必须大于 0"))]
    pub symbol: i32,

    pub reserve_price: Option<u64>, // 买单资金冻结预留价；缺省或 0 表示按 price 冻结
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request() -> OrderRequest {
        serde_json::from_str(
            r#"{"order_id":1,"user_id":10001,"action":"1","order_type":"GTC",
                "price":1000,"size":100,"symbol":600000,"reserve_price":1000}"#,
        )
        .unwrap()
    }

    #[test]
    fn test_valid_request_passes() {
        assert!(base_request().validate().is_ok());
    }

    #[test]
    fn test_missing_reserve_price_defaults_to_none() {
        let req: OrderRequest = serde_json::from_str(
            r#"{"order_id":1,"user_id":10001,"action":"1","order_type":"GTC",
                "price":1000,"size":100,"symbol":600000}"#,
        )
        .unwrap();
        assert_eq!(req.reserve_price, None);
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_non_positive_size_rejected() {
        let mut req = base_request();
        req.size = 0;
        let errors = req.validate().unwrap_err();
        assert!(errors.field_errors().contains_key("size"));
    }

    #[test]
    fn test_zero_user_id_rejected() {
        let mut req = base_request();
        req.user_id = 0;
        let errors = req.validate().unwrap_err();
        assert!(errors.field_errors().contains_key("user_id"));
    }
}
