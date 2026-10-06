//! SZSE 二进制协议网关：面向 gt-auto 等 OMS 模拟端的 TCP 接入层。
//!
//! 与 SSE 网关（`sse_gateway`）架构一致，差异在帧格式与报文类型/字段映射；
//! 采纳"先复制后改造"策略，待两套协议稳定后再评估抽象合并。

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use matching_core::OrderCommand;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::core_bridge::CoreBridge;

pub mod adapter;
pub mod codec;
pub mod report;
pub mod session;

/// 网关订单注册表容量上限，超出按先进先出淘汰
const REGISTRY_CAPACITY: usize = 100_000;

/// 网关共享状态：订单跟踪表、撤单上下文与连接编号
pub struct GatewayShared {
    pub bridge: Arc<CoreBridge>,
    pub registry: Mutex<HashMap<u64, report::GatewayOrderRecord>>,
    registry_order: Mutex<VecDeque<u64>>,
    pub pending_cancels: Mutex<HashMap<u64, report::CancelMeta>>,
    /// 连接编号
    pub conn_seq: AtomicU64,
}

impl GatewayShared {
    fn new(bridge: Arc<CoreBridge>) -> Self {
        Self {
            bridge,
            registry: Mutex::new(HashMap::new()),
            registry_order: Mutex::new(VecDeque::new()),
            pending_cancels: Mutex::new(HashMap::new()),
            conn_seq: AtomicU64::new(0),
        }
    }
}

/// 注册表 FIFO 淘汰（结果路由与会话任务都会调用）
pub(crate) fn push_registry_order(shared: &GatewayShared, order_id: u64) {
    let mut order = shared.registry_order.lock().unwrap();
    order.push_back(order_id);
    while order.len() > REGISTRY_CAPACITY {
        if let Some(oldest) = order.pop_front()
            && oldest != order_id
        {
            shared.registry.lock().unwrap().remove(&oldest);
        }
    }
}

/// 绑定端口并启动网关（accept 循环 + 结果订阅任务）。须在 LocalSet 内调用。
pub fn spawn(
    port: u16,
    bridge: Arc<CoreBridge>,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(%addr, error = %e, "SZSE 网关端口绑定失败");
                return;
            }
        };
        info!(%addr, "SZSE 协议网关已启动");
        let _ = spawn_listener(listener, bridge, shutdown).await;
    })
}

/// 在已绑定的 listener 上启动网关（测试用任意端口）。
pub fn spawn_listener(
    listener: TcpListener,
    bridge: Arc<CoreBridge>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let shared = Arc::new(GatewayShared::new(bridge.clone()));
    tokio::task::spawn_local(async move {
        // 结果订阅任务：终态广播 → 回报生成
        tokio::task::spawn_local(result_loop(shared.clone(), bridge.subscribe_results()));
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                accepted = listener.accept() => match accepted {
                    Ok((socket, peer)) => {
                        let conn_id = shared.conn_seq.fetch_add(1, Ordering::Relaxed) + 1;
                        info!(conn_id, %peer, "SZSE 会话接入");
                        tokio::task::spawn_local(session::run_connection(shared.clone(), conn_id, socket));
                    }
                    Err(e) => {
                        warn!(error = %e, "accept 失败，网关退出");
                        break;
                    }
                }
            }
        }
        info!("SZSE 网关停止接受新连接");
    })
}

/// 消费结果广播，驱动回报生成。
async fn result_loop(shared: Arc<GatewayShared>, mut results: broadcast::Receiver<OrderCommand>) {
    loop {
        match results.recv().await {
            Ok(cmd) => match cmd.command {
                matching_core::OrderCommandType::PlaceOrder => {
                    report::on_place_result(&shared, &cmd);
                }
                matching_core::OrderCommandType::CancelOrder => {
                    report::on_cancel_result(&shared, &cmd);
                }
                _ => {}
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(n, "网关结果订阅滞后，部分终态被跳过");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
