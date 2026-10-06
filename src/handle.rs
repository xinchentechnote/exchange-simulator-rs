use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use matching_core::{OrderAction, OrderCommand, OrderCommandType, OrderType};
use tracing::info;
use validator::Validate;

use crate::core_bridge::CoreBridge;
use crate::errors::SimError;
use crate::model::OrderRequest;

/// HTTP 路由。接口规格见 docs/DEVELOPMENT_DESIGN.md §5.1。
pub fn router(bridge: Arc<CoreBridge>) -> Router {
    Router::new()
        .route("/place", post(place_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/accounts/{uid}/balances", get(get_balances))
        .route("/trades", get(get_trades))
        .route("/admin/state", get(get_admin_state))
        .with_state(bridge)
        .layer(axum::middleware::from_fn(trace_middleware))
}

async fn trace_middleware(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let response = next.run(req).await;
    info!(
        method = %method,
        path = %path,
        status = %response.status(),
        elapsed_ms = start.elapsed().as_millis() as u64,
        "http 请求"
    );
    response
}

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
    if let Some(ts) = value
        .strip_prefix("GTD:")
        .and_then(|s| s.parse::<i64>().ok())
    {
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

async fn place_order(
    State(bridge): State<Arc<CoreBridge>>,
    Json(request): Json<OrderRequest>,
) -> Result<Json<crate::core_bridge::ExecutionReport>, SimError> {
    info!("收到委托请求: {:?}", request);
    request.validate().map_err(SimError::from_validation)?;

    let Some(action) = parse_action(&request.action) else {
        return Err(SimError::bad_request(format!(
            "action 必须为 1(买) 或 2(卖)，当前为 {:?}",
            request.action
        )));
    };
    let Some((order_type, expire_time)) = parse_order_type(&request.order_type) else {
        return Err(SimError::bad_request(format!(
            "order_type 非法或暂不支持: {:?}",
            request.order_type
        )));
    };

    // reserve_price 是买单资金冻结预留价；缺省/0 按委托价冻结，
    // 否则冻结额不足会绕过风控并在成交结算时把余额打成负数
    let reserve_price = match request.reserve_price {
        Some(0) | None => request.price,
        Some(rp) if action == OrderAction::Bid && rp < request.price as u64 => {
            return Err(SimError::bad_request("买单 reserve_price 不能低于委托价"));
        }
        Some(rp) => rp as i64,
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

    let report = bridge.execute(command).await?;
    Ok(Json(report))
}

async fn get_order(State(bridge): State<Arc<CoreBridge>>, Path(order_id): Path<u64>) -> Response {
    match bridge.order_record(order_id) {
        Some(record) => Json(record).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "order_not_found" })),
        )
            .into_response(),
    }
}

async fn get_balances(
    State(bridge): State<Arc<CoreBridge>>,
    Path(uid): Path<u64>,
) -> Json<BTreeMap<i32, i64>> {
    Json(bridge.balances_of(uid))
}

#[derive(Debug, serde::Deserialize)]
struct TradesQuery {
    symbol: Option<i32>,
    limit: Option<usize>,
}

/// 最近成交流水（内存环形缓冲，按时间正序；limit 截取最近 N 笔，上限 1000）
async fn get_trades(
    State(bridge): State<Arc<CoreBridge>>,
    axum::extract::Query(q): axum::extract::Query<TradesQuery>,
) -> Json<Vec<crate::core_bridge::TradeView>> {
    let limit = q.limit.unwrap_or(100).min(1000);
    Json(bridge.recent_trades(q.symbol, limit))
}

/// 运行汇总（对账用）
async fn get_admin_state(State(bridge): State<Arc<CoreBridge>>) -> Json<serde_json::Value> {
    let s = bridge.state_summary();
    Json(serde_json::json!({
        "orders_total": s.orders_total,
        "trades_total": s.trades_total,
        "registry_size": s.registry_size,
        "balance_sums": s.balance_sums,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_action() {
        assert!(matches!(parse_action("1"), Some(OrderAction::Bid)));
        assert!(matches!(parse_action("2"), Some(OrderAction::Ask)));
        assert!(parse_action("9").is_none());
        assert!(parse_action("").is_none());
        assert!(parse_action("buy").is_none());
    }

    #[test]
    fn test_parse_order_type() {
        assert!(matches!(
            parse_order_type("GTC"),
            Some((OrderType::Gtc, None))
        ));
        assert!(matches!(
            parse_order_type("ioc"),
            Some((OrderType::Ioc, None))
        ));
        assert!(matches!(
            parse_order_type(" FOK "),
            Some((OrderType::Fok, None))
        ));
        assert!(matches!(
            parse_order_type("FOK_BUDGET"),
            Some((OrderType::FokBudget, None))
        ));
        assert!(matches!(
            parse_order_type("IOC_BUDGET"),
            Some((OrderType::IocBudget, None))
        ));
        assert!(matches!(
            parse_order_type("POST_ONLY"),
            Some((OrderType::PostOnly, None))
        ));
        assert!(matches!(
            parse_order_type("DAY"),
            Some((OrderType::Day, None))
        ));

        let (t, expire) = parse_order_type("GTD:1735689600").unwrap();
        assert!(matches!(t, OrderType::Gtd(1735689600)));
        assert_eq!(expire, Some(1735689600));

        // 非法/暂不支持
        assert!(parse_order_type("XYZ").is_none());
        assert!(parse_order_type("GTD").is_none(), "裸 GTD 缺时间戳");
        assert!(parse_order_type("GTD:not-a-number").is_none());
        assert!(parse_order_type("STOP_LIMIT").is_none());
        assert!(parse_order_type("STOP_MARKET").is_none());
        assert!(parse_order_type("ICEBERG").is_none());
    }
}
