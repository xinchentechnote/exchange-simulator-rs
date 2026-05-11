use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRequest {
    pub order_id: u64,

    pub user_id: u64,

    pub action: String,     // ASK / BID
    pub order_type: String, // GTC / IOC / FOK_BUDGET

    pub price: i64,

    pub size: i64,

    pub symbol: i32,

    pub reserve_price: u64,
}
