pub mod base_info_service;
pub mod config;
pub mod handle;
pub mod model;

use axum::{routing::post, Router};
use matching_core::core::exchange::ExchangeConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use matching_core::{OrderCommand, OrderCommandType};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let local = LocalSet::new();

        local
            .run_until(async {
                let config = ExchangeConfig::default();
                let mut core = matching_core::core::exchange::ExchangeCore::new(config);

                core.set_result_consumer(Arc::new(|cmd| {
                    println!("result: {:?} - {:?}", cmd.command, cmd.result_code);
                }));

                // 加载标的
                crate::base_info_service::load_symbol_info("data/sse/symbol_data.csv")
                    .into_iter()
                    .for_each(|spec| core.add_symbol(spec));

                // 加载账户
                let user_balances =
                    crate::base_info_service::load_account_info("data/sse/account_data.csv");

                for ub in &user_balances {
                    core.submit_command(OrderCommand {
                        command: OrderCommandType::AddUser,
                        uid: ub.uid,
                        ..Default::default()
                    });

                    core.submit_command(OrderCommand {
                        command: OrderCommandType::BalanceAdjustment,
                        uid: ub.uid,
                        currency: ub.currency.clone(),
                        amount: ub.amount.clone(),
                        ..Default::default()
                    });
                }

                let (tx, mut rx) = mpsc::channel::<OrderCommand>(1024);

                tokio::task::spawn_local(async move {
                    core.startup();
                    while let Some(cmd) = rx.recv().await {
                        core.submit_command(cmd);
                    }
                });

                let app = Router::new()
                    .route("/place", post(handle::place_order))
                    .with_state(Arc::new(tx));

                let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
                println!("Server listening on {}", addr);

                let listener = TcpListener::bind(addr).await.unwrap();
                axum::serve(listener, app).await.unwrap();
            })
            .await;
    });
}