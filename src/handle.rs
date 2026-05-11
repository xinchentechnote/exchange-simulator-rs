use std::sync::Arc;
use crate::model::OrderRequest;
use axum::{Json, extract::State, http::StatusCode};
use matching_core::core::exchange::ExchangeCore;
use matching_core::{OrderAction, OrderCommand, OrderCommandType, OrderType};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info};

pub async fn place_order(
    State(tx): State<Arc<mpsc::Sender<OrderCommand>>>,
    Json(request): Json<OrderRequest>,
) -> Result<Json<bool>, StatusCode> {
    info!("收到委托请求: {:?}", request);
    let action = match request.action.as_str() {
        "1" => OrderAction::Ask,
        _ => OrderAction::Bid,
    };

    tx.send(OrderCommand {
        command: OrderCommandType::PlaceOrder,
        uid: request.user_id,
        order_id: request.order_id,
        symbol: request.symbol,
        price: request.price,
        size: request.size,
        action,
        order_type: OrderType::Gtc,
        ..Default::default()
    }).await.expect("TODO: panic message");
    Ok(Json(true))
}
