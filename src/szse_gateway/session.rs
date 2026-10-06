//! 会话状态机（SZSE）：AwaitingLogon → Active → Closed。
//!
//! 与 SSE 网关的差异：
//! - **宽松登录**：gt-auto 的 SZSE 参考场景不含 Logon 步骤，业务报文不强制先登录；
//!   收到 Logon 则幂等应答（sender/target 对调、其余回显）；
//! - 心跳：回应 Heartbeat，`heart_btint` 秒内无任何入站报文判定超时断开；
//! - 序列号：SZSE 帧无序列号字段，无校验；
//! - 订单/撤单报文在独立任务中处理，读循环不因撮合等待而阻塞。

use std::sync::Arc;
use std::time::Duration;

use binary_codec::BinaryCodec;
use bytes::BytesMut;
use matching_core::OrderCommand;
use szse_binary::heartbeat::Heartbeat;
use szse_binary::logon::Logon;
use szse_binary::logout::Logout;
use szse_binary::new_order::NewOrder;
use szse_binary::order_cancel_request::OrderCancelRequest;
use szse_binary::szse_binary::{SzseBinary, SzseBinaryBodyEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::errors::SimError;

use super::GatewayShared;
use super::adapter;
use super::codec::{self, FrameError};
use super::report::{
    self, GatewayOrderRecord, REJ_DUPLICATE_ORDER_ID, REJ_INTERNAL, SessionHandle,
};

/// 未携带心跳间隔时的默认超时（秒）
const DEFAULT_HEARTBTINT_SECS: i32 = 30;
/// 会话出站队列容量
const OUT_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleResult {
    Continue,
    Close,
}

struct ConnState {
    shared: Arc<GatewayShared>,
    handle: SessionHandle,
    logged_on: bool,
    heart_btint: i32,
}

impl ConnState {
    fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.heart_btint.unsigned_abs()).max(1))
    }

    async fn handle_message(&mut self, msg: SzseBinary) -> HandleResult {
        match msg.body {
            SzseBinaryBodyEnum::Logon(logon) => self.on_logon(logon),
            SzseBinaryBodyEnum::Heartbeat(_) => {
                self.handle
                    .send(SzseBinaryBodyEnum::Heartbeat(Heartbeat {}));
                HandleResult::Continue
            }
            SzseBinaryBodyEnum::Logout(logout) => {
                info!(session_status = logout.session_status, "对端登出，关闭会话");
                self.handle.send(SzseBinaryBodyEnum::Logout(Logout {
                    session_status: 0,
                    text: String::new(),
                }));
                HandleResult::Close
            }
            SseLikeBody::NewOrder(nos) => {
                self.on_new_order(nos);
                HandleResult::Continue
            }
            SseLikeBody::OrderCancelRequest(ocr) => {
                self.on_cancel(ocr);
                HandleResult::Continue
            }
            other => {
                warn!(
                    msg_type = codec::msg_type_of(&other),
                    "暂不处理的报文类型，忽略"
                );
                HandleResult::Continue
            }
        }
    }

    fn on_logon(&mut self, logon: Logon) -> HandleResult {
        if self.logged_on {
            // 幂等应答：gt-auto 可能跨用例复用连接再次登录
            info!("会话已登录，重复 Logon 仍应答");
        } else {
            info!(
                sender = %logon.sender_comp_id,
                target = %logon.target_comp_id,
                heart_btint = logon.heart_btint,
                "会话登录"
            );
            self.logged_on = true;
        }
        self.heart_btint = if logon.heart_btint == 0 {
            DEFAULT_HEARTBTINT_SECS
        } else {
            logon.heart_btint
        };
        // 应答 Logon：sender/target 对调，其余字段回显
        self.handle.send(SzseBinaryBodyEnum::Logon(Logon {
            sender_comp_id: logon.target_comp_id.clone(),
            target_comp_id: logon.sender_comp_id.clone(),
            heart_btint: logon.heart_btint,
            password: logon.password.clone(),
            default_appl_ver_id: logon.default_appl_ver_id.clone(),
        }));
        HandleResult::Continue
    }

    fn on_new_order(&mut self, nos: NewOrder) {
        let shared = self.shared.clone();
        let handle = self.handle.clone();
        match adapter::new_order(&nos) {
            Err(reason) => {
                warn!(cl_ord_id = %nos.cl_ord_id, reason, "新订单适配失败，拒绝");
                report::send_business_reject_for_nos(&handle, &nos, reason);
            }
            Ok(adapted) => {
                let order_id = adapted.cmd.order_id;
                {
                    let mut registry = shared.registry.lock().unwrap();
                    if registry.contains_key(&order_id) {
                        warn!(order_id, "重复订单号，拒绝");
                        report::send_business_reject(
                            &handle,
                            &adapted.meta,
                            matching_core::CommandResultCode::Success,
                            REJ_DUPLICATE_ORDER_ID,
                        );
                        return;
                    }
                    // 先登记再提交，保证结果路由时记录可见
                    registry.insert(
                        order_id,
                        GatewayOrderRecord {
                            order_id,
                            order_qty: adapted.meta.order_qty,
                            filled_qty: 0,
                            leaves_qty: adapted.meta.order_qty,
                            status: crate::core_bridge::OrderStatus::New,
                            confirm_index: 0,
                            report_index: 0,
                            session: handle.clone(),
                            meta: adapted.meta.clone(),
                        },
                    );
                    super::push_registry_order(&shared, order_id);
                }
                tokio::task::spawn_local(async move {
                    match shared.bridge.execute(adapted.cmd).await {
                        // ExecutionConfirm/ExecutionReport 由结果广播（result_loop）驱动发送
                        Ok(_) => {}
                        Err(SimError::DuplicateOrderId) => {
                            shared.registry.lock().unwrap().remove(&order_id);
                            report::send_business_reject(
                                &handle,
                                &adapted.meta,
                                matching_core::CommandResultCode::Success,
                                REJ_DUPLICATE_ORDER_ID,
                            );
                        }
                        Err(e) => {
                            warn!(order_id, error = ?e, "订单提交失败");
                            shared.registry.lock().unwrap().remove(&order_id);
                            report::send_business_reject(
                                &handle,
                                &adapted.meta,
                                matching_core::CommandResultCode::Success,
                                REJ_INTERNAL,
                            );
                        }
                    }
                });
            }
        }
    }

    fn on_cancel(&mut self, ocr: OrderCancelRequest) {
        let shared = self.shared.clone();
        let handle = self.handle.clone();
        match adapter::cancel_order(&ocr) {
            Err(reason) => {
                warn!(orig_cl_ord_id = %ocr.orig_cl_ord_id, reason, "撤单适配失败，拒绝");
                report::send_cancel_reject_for_ocr(&handle, &ocr, reason);
            }
            Ok((cmd, fields)) => {
                // SZSE 撤单报文无账户字段，uid 从网关订单记录回查
                let uid = shared
                    .registry
                    .lock()
                    .unwrap()
                    .get(&cmd.order_id)
                    .map(|rec| rec.meta.account_id.trim().parse::<u64>().unwrap_or(0))
                    .unwrap_or(0);
                let cmd = OrderCommand { uid, ..cmd };
                if !shared.registry.lock().unwrap().contains_key(&cmd.order_id) {
                    warn!(order_id = cmd.order_id, "撤单目标订单不存在");
                    report::send_cancel_reject_for_ocr(&handle, &ocr, report::REJ_UNKNOWN_ORDER);
                    return;
                }
                let meta = report::CancelMeta {
                    session: handle.clone(),
                    appl_id: fields.appl_id,
                    pbu: fields.pbu,
                    cl_ord_id: fields.cl_ord_id,
                    orig_cl_ord_id: fields.orig_cl_ord_id,
                    security_id: fields.security_id,
                    security_id_source: fields.security_id_source,
                    owner_type: fields.owner_type,
                    clearing_firm: fields.clearing_firm,
                    side: fields.side,
                    user_info: fields.user_info,
                    transact_time: fields.transact_time,
                };
                shared
                    .pending_cancels
                    .lock()
                    .unwrap()
                    .insert(cmd.order_id, meta);
                let order_id = cmd.order_id;
                tokio::task::spawn_local(async move {
                    // 撤单结果经结果广播回传（on_cancel_result）
                    if shared.bridge.submit(cmd).await.is_err() {
                        warn!(order_id, "撤单提交失败：核心不可用");
                        shared.pending_cancels.lock().unwrap().remove(&order_id);
                    }
                });
            }
        }
    }
}

// 与 SSE 版对齐的可读性别名：避免 match 分支过长
use SzseBinaryBodyEnum as SseLikeBody;

/// 单个连接的处理入口：写任务 + 带心跳超时的读循环。
pub(crate) async fn run_connection(shared: Arc<GatewayShared>, conn_id: u64, socket: TcpStream) {
    let (socket_rd, mut socket_wr) = socket.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<SzseBinary>(OUT_QUEUE_CAPACITY);

    let writer = tokio::task::spawn_local(async move {
        while let Some(msg) = out_rx.recv().await {
            let mut buf = BytesMut::new();
            msg.encode(&mut buf);
            if socket_wr.write_all(&buf).await.is_err() {
                break;
            }
        }
    });

    let mut state = ConnState {
        shared: shared.clone(),
        handle: SessionHandle::new(out_tx.clone()),
        logged_on: false,
        heart_btint: DEFAULT_HEARTBTINT_SECS,
    };
    let mut rd = BufReader::new(socket_rd);
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let read = tokio::time::timeout(state.heartbeat_timeout(), rd.read_buf(&mut buf));
        match read.await {
            Err(_) => {
                info!(
                    conn_id,
                    heart_btint = state.heart_btint,
                    "心跳超时，断开会话"
                );
                break;
            }
            Ok(Err(e)) => {
                warn!(conn_id, error = %e, "读错误，断开会话");
                break;
            }
            Ok(Ok(0)) => {
                info!(conn_id, "对端关闭连接");
                break;
            }
            Ok(Ok(_)) => {}
        }
        let mut closed = false;
        loop {
            match codec::decode_frame(&mut buf) {
                Ok(None) => break,
                Ok(Some(msg)) => {
                    if state.handle_message(msg).await == HandleResult::Close {
                        closed = true;
                        break;
                    }
                }
                Err(FrameError::UnknownMessageType { msg_type }) => {
                    warn!(conn_id, msg_type, "未知报文类型，跳过该帧");
                }
                Err(FrameError::BodyTooLarge { body_len }) => {
                    warn!(conn_id, body_len, "超长报文，断开会话");
                    closed = true;
                    break;
                }
                Err(FrameError::MalformedBody) => {
                    warn!(conn_id, "报文体解析异常，断开会话");
                    closed = true;
                    break;
                }
            }
        }
        if closed {
            break;
        }
    }

    drop(state);
    drop(out_tx); // 关闭出站队列，写任务排空后退出
    let _ = writer.await;
    info!(conn_id, "会话结束");
}
