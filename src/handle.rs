use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use matching_core::{OrderAction, OrderCommand, OrderCommandType, OrderType};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::model::OrderRequest;

/// action 语义与 SSE 协议惯例一致：1=买入(Bid)、2=卖出(Ask)，其余值非法。
fn parse_action(action: &str) -> Option<OrderAction> {
    match action {
        "1" => Some(OrderAction::Bid),
        "2" => Some(OrderAction::Ask),
        _ => None,
    }
}

/// 订单类型映射，返回 (OrderType, expire_time)。
/// 裸 "GTD"（缺时间戳）与 STOP_LIMIT/STOP_MARKET/ICEBERG（需要请求中尚不存在的
/// stop_price/visible_size 字段）暂不支持，返回 None 由调用方拒绝。
fn parse_order_type(order_type: &str) -> Option<(OrderType, Option<i64>)> {
    let value = order_type.trim().to_ascii_uppercase();
    if let Some(ts) = value.strip_prefix("GTD:").and_then(|s| s.parse::<i64>().ok()) {
        return Some((OrderType::Gtd(ts), Some(ts)));
    }
    match value.as_str() {
        "GTC" => Some((OrderType::Gtc, None)),
        "IOC" => Some((OrderType::Ioc, None)),
        "FOK" => Some((OrderType::Fok, None)),
        "FOK_BUDGET" => Some((OrderType::FokBudget, None)),
        "IOC_BUDGET" => Some((OrderType::IocBudget, None)),
        "POST_ONLY" => Some((OrderType::PostOnly, None)),
        "DAY" => Some((OrderType::Day, None)),
        _ => None,
    }
}

pub async fn place_order(
    State(tx): State<Arc<mpsc::Sender<OrderCommand>>>,
    Json(request): Json<OrderRequest>,
) -> Result<Json<bool>, StatusCode> {
    info!("收到委托请求: {:?}", request);

    let Some(action) = parse_action(&request.action) else {
        warn!(action = %request.action, "非法 action，应为 1(买) 或 2(卖)");
        return Err(StatusCode::BAD_REQUEST);
    };

    let Some((order_type, expire_time)) = parse_order_type(&request.order_type) else {
        warn!(order_type = %request.order_type, "非法或暂不支持的订单类型");
        return Err(StatusCode::BAD_REQUEST);
    };

    // reserve_price 是买单资金冻结预留价；未填（0）时按委托价冻结，
    // 否则冻结额不足会绕过风控并在成交结算时把余额打成负数
    let reserve_price = if request.reserve_price == 0 {
        request.price
    } else {
        request.reserve_price as i64
    };

    let command = OrderCommand {
        command: OrderCommandType::PlaceOrder,
        uid: request.user_id,
        order_id: request.order_id,
        symbol: request.symbol,
        price: request.price,
        reserve_price,
        size: request.size,
        action,
        order_type,
        expire_time,
        ..Default::default()
    };

    if tx.send(command).await.is_err() {
        error!("命令通道已关闭，撮合核心不可用");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(true))
}
