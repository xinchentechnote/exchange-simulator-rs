use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRequest {
    pub order_id: u64,

    pub user_id: u64,

    pub action: String,     // 1=买(Bid) 2=卖(Ask)，其余值返回 400
    pub order_type: String, // GTC/IOC/FOK/FOK_BUDGET/IOC_BUDGET/POST_ONLY/DAY/GTD:<unix_ts>

    pub price: i64,

    pub size: i64,

    pub symbol: i32,

    pub reserve_price: u64, // 买单资金冻结预留价；0 表示按 price 冻结
}
