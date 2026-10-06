//! 回报生成与网关订单跟踪表（SZSE 二进制协议）。
//!
//! Confirm（受理确认）在订单受理时发送；每个 Trade 事件生成买卖双方各一条
//! ExecutionReport(200115)；拒绝发 BusinessReject(4)；撤单成功发
//! ExecutionConfirm(exec_type=4)、失败发 CancelReject(290008)。
//!
//! 字段回填规则与 SSE 网关一致（依 gt-auto 模板校验校准的结论）：
//! ReportIndex 按「订单 × 报文类别」从 0 递增；流水 ID 留空；成交回报仅回填
//! 成交与订单核心要素。exec_type/ord_status 码值为设计值（待与协议细则校准）。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use matching_core::{CommandResultCode, MatcherEventType, OrderCommand};
use szse_binary::business_reject::BusinessReject;
use szse_binary::cancel_reject::CancelReject;
use szse_binary::execution_confirm::ExecutionConfirm;
use szse_binary::execution_report::ExecutionReport;
use szse_binary::extend_200102::Extend200102;
use szse_binary::extend_200115::Extend200115;
use szse_binary::new_order::NewOrder;
use szse_binary::order_cancel_request::OrderCancelRequest;
use szse_binary::szse_binary::{SzseBinary, SzseBinaryBodyEnum};
use tokio::sync::mpsc;
use tracing::warn;

use crate::core_bridge::OrderStatus;

use super::GatewayShared;
use super::codec;

// ---- 回报拒绝原因码（模拟器自定义，与 SSE 网关一致，见开发设计文档 §6.4）----
pub const REJ_USER_NOT_FOUND: u32 = 1001;
pub const REJ_INSUFFICIENT_BALANCE: u32 = 1002;
pub const REJ_INVALID_SYMBOL: u32 = 1003;
pub const REJ_INVALID_SIZE: u32 = 1004;
pub const REJ_UNKNOWN_ORDER: u32 = 1005;
pub const REJ_DUPLICATE_ORDER_ID: u32 = 1006;
pub const REJ_INVALID_FIELD: u32 = 1007;
pub const REJ_NOT_LOGGED_IN: u32 = 1008;
pub const REJ_INTERNAL: u32 = 1999;

// ---- exec_type / ord_status 码值（设计值，待确认）----
pub const EXEC_TYPE_NEW: &str = "0";
pub const EXEC_TYPE_CANCELLED: &str = "4";
pub const EXEC_TYPE_TRADE: &str = "F";
pub const ORD_STATUS_NORMAL: &str = "0";
pub const ORD_STATUS_CANCELLED: &str = "4";

/// SZSE 场景：分区号（模板参考值 1）
const PARTITION_NO: i32 = 1;

/// 核心结果码 → `business_reject_reason`/`cxl_rej_reason`
pub(crate) fn reject_reason(code: CommandResultCode) -> u32 {
    match code {
        CommandResultCode::AuthInvalidUser => REJ_USER_NOT_FOUND,
        CommandResultCode::RiskNsf => REJ_INSUFFICIENT_BALANCE,
        CommandResultCode::InvalidSymbol | CommandResultCode::MatchingInvalidOrderBookId => {
            REJ_INVALID_SYMBOL
        }
        CommandResultCode::MatchingInvalidOrderSize => REJ_INVALID_SIZE,
        CommandResultCode::MatchingUnknownOrderId => REJ_UNKNOWN_ORDER,
        _ => REJ_INTERNAL,
    }
}

/// 会话出站句柄：网关订单/撤单上下文通过它把回报发回所属连接。
/// 出站队列满时丢弃该条回报并告警（写阻塞不应拖垮结果路由）。
#[derive(Clone)]
pub struct SessionHandle {
    out: mpsc::Sender<SzseBinary>,
    out_seq: Arc<AtomicU64>,
}

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("out_seq", &self.out_seq.load(Ordering::Relaxed))
            .finish()
    }
}

impl SessionHandle {
    pub(crate) fn new(out: mpsc::Sender<SzseBinary>) -> Self {
        Self {
            out,
            out_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn send(&self, body: SzseBinaryBodyEnum) {
        let msg = SzseBinary {
            msg_type: codec::msg_type_of(&body),
            body_length: 0,
            body,
            checksum: 0,
        };
        if self.out.try_send(msg).is_err() {
            warn!("会话出站队列满或已关闭，丢弃回报");
        }
    }
}

/// NewOrder 回填字段：生成 ExecutionConfirm/ExecutionReport 时原样回传
#[derive(Debug, Clone)]
pub struct NsMeta {
    pub appl_id: String,
    pub submitting_pbuid: String,
    pub security_id: String,
    pub security_id_source: String,
    pub owner_type: u16,
    pub clearing_firm: String,
    pub transact_time: i64,
    pub user_info: String,
    pub cl_ord_id: String,
    pub account_id: String,
    pub branch_id: String,
    pub order_restrictions: String,
    pub side: String,
    pub ord_type: String,
    pub trade_date: u32,
    pub price: i64,
    pub order_qty: i64,
}

/// 撤单请求上下文：结果到达时用于回填 CancelReject / 撤单 ExecutionConfirm
#[derive(Debug, Clone)]
pub struct CancelMeta {
    pub session: SessionHandle,
    pub appl_id: String,
    pub pbu: String,
    pub cl_ord_id: String,
    pub orig_cl_ord_id: String,
    pub security_id: String,
    pub security_id_source: String,
    pub owner_type: u16,
    pub clearing_firm: String,
    pub side: String,
    pub user_info: String,
    pub transact_time: i64,
}

/// 网关订单跟踪表记录：订单受理前登记，成交/撤单时更新
#[derive(Debug, Clone)]
pub struct GatewayOrderRecord {
    pub order_id: u64,
    pub order_qty: i64,
    pub filled_qty: i64,
    pub leaves_qty: i64,
    pub status: OrderStatus,
    /// Confirm 类报文序号（按订单从 0 递增）
    pub confirm_index: u64,
    /// Report 类报文序号（按订单从 0 递增）
    pub report_index: u64,
    pub session: SessionHandle,
    pub meta: NsMeta,
}

impl GatewayOrderRecord {
    fn next_confirm_index(&mut self) -> i64 {
        let v = self.confirm_index;
        self.confirm_index += 1;
        v as i64
    }

    fn next_report_index(&mut self) -> i64 {
        let v = self.report_index;
        self.report_index += 1;
        v as i64
    }
}

/// PlaceOrder 终态 → ExecutionConfirm / ExecutionReport×N / BusinessReject
pub(crate) fn on_place_result(shared: &GatewayShared, cmd: &OrderCommand) {
    let mut registry = shared.registry.lock().unwrap();
    let Some(mut rec) = registry.get(&cmd.order_id).cloned() else {
        return; // 非网关渠道下单（如 HTTP），无会话可回报
    };

    if cmd.result_code != CommandResultCode::Success {
        rec.status = OrderStatus::Rejected;
        registry.insert(cmd.order_id, rec.clone());
        drop(registry);
        let reason = reject_reason(cmd.result_code);
        warn!(
            order_id = cmd.order_id,
            reason, "订单被拒，发送 BusinessReject"
        );
        send_business_reject(&rec.session, &rec.meta, cmd.result_code, reason);
        return;
    }

    // 1) 受理确认（LeavesQty 为受理时的全额，CumQty=0）
    let confirm_index = rec.next_confirm_index();
    rec.session.send(SzseBinaryBodyEnum::ExecutionConfirm(
        build_execution_confirm(
            &rec.meta,
            confirm_index,
            rec.order_qty,
            0,
            EXEC_TYPE_NEW,
            ORD_STATUS_NORMAL,
        ),
    ));

    // 2) 逐事件生成成交流水（本方 + 对手方）
    for event in &cmd.matcher_events {
        match event.event_type {
            MatcherEventType::Trade => {
                rec.filled_qty += event.size;
                rec.leaves_qty = (rec.order_qty - rec.filled_qty).max(0);
                rec.status = if rec.leaves_qty == 0 {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
                let report_index = rec.next_report_index();
                rec.session
                    .send(SzseBinaryBodyEnum::ExecutionReport(build_execution_report(
                        &rec.meta,
                        report_index,
                        rec.leaves_qty,
                        rec.filled_qty,
                        event.size,
                        event.price,
                    )));
                // 对手方（maker）：其成交来自本 taker 事件
                if let Some(mut maker) = registry.get(&event.matched_order_id).cloned() {
                    maker.filled_qty += event.size;
                    maker.leaves_qty = (maker.order_qty - maker.filled_qty).max(0);
                    maker.status = if maker.leaves_qty == 0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    let maker_report_index = maker.next_report_index();
                    maker.session.send(SzseBinaryBodyEnum::ExecutionReport(
                        build_execution_report(
                            &maker.meta,
                            maker_report_index,
                            maker.leaves_qty,
                            maker.filled_qty,
                            event.size,
                            event.price,
                        ),
                    ));
                    registry.insert(event.matched_order_id, maker);
                }
            }
            MatcherEventType::Reject | MatcherEventType::Reduce => {
                // IOC/FOK 未成交部分被拒：剩余量清零（余额镜像由 core_bridge 处理）
                rec.leaves_qty = rec.leaves_qty.saturating_sub(event.size);
                rec.status = if rec.leaves_qty == 0 {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
            }
        }
    }
    registry.insert(cmd.order_id, rec);
}

/// CancelOrder 终态 → 撤单 ExecutionConfirm(exec_type=4) 或 CancelReject
pub(crate) fn on_cancel_result(shared: &GatewayShared, cmd: &OrderCommand) {
    let Some(meta) = shared.pending_cancels.lock().unwrap().remove(&cmd.order_id) else {
        return;
    };
    if cmd.result_code != CommandResultCode::Success {
        warn!(order_id = cmd.order_id, reason = ?cmd.result_code, "撤单失败，发送 CancelReject");
        meta.session
            .send(SzseBinaryBodyEnum::CancelReject(CancelReject {
                partition_no: PARTITION_NO,
                report_index: 0,
                appl_id: meta.appl_id.clone(),
                reporting_pbuid: String::new(),
                submitting_pbuid: meta.pbu.clone(),
                security_id: meta.security_id.clone(),
                security_id_source: meta.security_id_source.clone(),
                owner_type: meta.owner_type,
                clearing_firm: meta.clearing_firm.clone(),
                transact_time: meta.transact_time,
                user_info: meta.user_info.clone(),
                cl_ord_id: meta.cl_ord_id.clone(),
                orig_cl_ord_id: meta.orig_cl_ord_id.clone(),
                side: meta.side.clone(),
                ord_status: ORD_STATUS_CANCELLED.to_string(),
                cxl_rej_reason: reject_reason(cmd.result_code) as u16,
                reject_text: String::new(),
                order_id: String::new(),
            }));
        return;
    }

    let mut registry = shared.registry.lock().unwrap();
    let (filled_before, confirm_index, rec_meta) = match registry.get_mut(&cmd.order_id) {
        Some(rec) => {
            let filled = rec.filled_qty;
            rec.leaves_qty = 0;
            rec.status = OrderStatus::Filled;
            (filled, rec.next_confirm_index(), Some(rec.meta.clone()))
        }
        None => (0, 0, None),
    };
    drop(registry);

    // 撤单确认：订单要素回填，transact_time 取撤单请求值
    let session = meta.session.clone();
    if let Some(rec_meta) = &rec_meta {
        session.send(SzseBinaryBodyEnum::ExecutionConfirm(build_cancel_confirm(
            rec_meta,
            &meta,
            filled_before,
            confirm_index,
        )));
    }
}

/// 直接发送 BusinessReject（适配失败/重复订单号等前置拒绝）
pub(crate) fn send_business_reject(
    handle: &SessionHandle,
    meta: &NsMeta,
    code: CommandResultCode,
    reason: u32,
) {
    handle.send(SzseBinaryBodyEnum::BusinessReject(BusinessReject {
        appl_id: meta.appl_id.clone(),
        transact_time: meta.transact_time,
        submitting_pbuid: meta.submitting_pbuid.clone(),
        security_id: meta.security_id.clone(),
        security_id_source: meta.security_id_source.clone(),
        ref_seq_num: 0,
        ref_msg_type: codec::MSG_NEW_ORDER,
        business_reject_ref_id: meta.cl_ord_id.clone(),
        business_reject_reason: reason as u16,
        business_reject_text: format!("{code:?}"),
    }));
}

/// 未适配成功的 NewOrder 直接拒绝（字段不完整，尽力回填）
pub(crate) fn send_business_reject_for_nos(handle: &SessionHandle, nos: &NewOrder, reason: u32) {
    handle.send(SzseBinaryBodyEnum::BusinessReject(BusinessReject {
        appl_id: nos.appl_id.clone(),
        transact_time: nos.transact_time,
        submitting_pbuid: nos.submitting_pbuid.clone(),
        security_id: nos.security_id.clone(),
        security_id_source: nos.security_id_source.clone(),
        ref_seq_num: 0,
        ref_msg_type: codec::MSG_NEW_ORDER,
        business_reject_ref_id: nos.cl_ord_id.clone(),
        business_reject_reason: reason as u16,
        business_reject_text: String::new(),
    }));
}

/// 适配失败的 OrderCancelRequest 直接发 CancelReject
pub(crate) fn send_cancel_reject_for_ocr(
    handle: &SessionHandle,
    ocr: &OrderCancelRequest,
    reason: u32,
) {
    handle.send(SzseBinaryBodyEnum::CancelReject(CancelReject {
        partition_no: PARTITION_NO,
        report_index: 0,
        appl_id: ocr.appl_id.clone(),
        reporting_pbuid: String::new(),
        submitting_pbuid: ocr.submitting_pbuid.clone(),
        security_id: ocr.security_id.clone(),
        security_id_source: ocr.security_id_source.clone(),
        owner_type: ocr.owner_type,
        clearing_firm: ocr.clearing_firm.clone(),
        transact_time: ocr.transact_time,
        user_info: ocr.user_info.clone(),
        cl_ord_id: ocr.cl_ord_id.clone(),
        orig_cl_ord_id: ocr.orig_cl_ord_id.clone(),
        side: ocr.side.clone(),
        ord_status: ORD_STATUS_CANCELLED.to_string(),
        cxl_rej_reason: reason as u16,
        reject_text: String::new(),
        order_id: String::new(),
    }));
}

fn zero_confirm_ext() -> szse_binary::execution_confirm::ExecutionConfirmApplExtendEnum {
    szse_binary::execution_confirm::ExecutionConfirmApplExtendEnum::Extend200102(Extend200102 {
        stop_px: 0,
        min_qty: 0,
        max_price_levels: 0,
        time_in_force: String::new(),
        cash_margin: String::new(),
    })
}

fn build_execution_confirm(
    meta: &NsMeta,
    report_index: i64,
    leaves_qty: i64,
    cum_qty: i64,
    exec_type: &str,
    ord_status: &str,
) -> ExecutionConfirm {
    ExecutionConfirm {
        partition_no: PARTITION_NO,
        report_index,
        appl_id: meta.appl_id.clone(),
        reporting_pbuid: String::new(),
        submitting_pbuid: meta.submitting_pbuid.clone(),
        security_id: meta.security_id.clone(),
        security_id_source: meta.security_id_source.clone(),
        owner_type: meta.owner_type,
        clearing_firm: meta.clearing_firm.clone(),
        transact_time: meta.transact_time,
        user_info: meta.user_info.clone(),
        order_id: String::new(),
        cl_ord_id: meta.cl_ord_id.clone(),
        quote_msg_id: String::new(),
        orig_cl_ord_id: String::new(),
        exec_id: String::new(),
        exec_type: exec_type.to_string(),
        ord_status: ord_status.to_string(),
        ord_rej_reason: 0,
        leaves_qty,
        cum_qty,
        side: meta.side.clone(),
        ord_type: meta.ord_type.clone(),
        order_qty: meta.order_qty,
        price: meta.price,
        account_id: meta.account_id.clone(),
        branch_id: meta.branch_id.clone(),
        order_restrictions: meta.order_restrictions.clone(),
        appl_extend: zero_confirm_ext(),
    }
}

/// 撤单确认：订单要素回填，transact_time/user_info 取撤单请求值
fn build_cancel_confirm(
    order: &NsMeta,
    cancel: &CancelMeta,
    filled_before: i64,
    report_index: i64,
) -> ExecutionConfirm {
    let mut confirm = build_execution_confirm(
        order,
        report_index,
        0,
        filled_before,
        EXEC_TYPE_CANCELLED,
        ORD_STATUS_CANCELLED,
    );
    confirm.orig_cl_ord_id = cancel.orig_cl_ord_id.clone();
    confirm.transact_time = cancel.transact_time;
    confirm.user_info = cancel.user_info.clone();
    confirm
}

/// 成交流水：仅回填成交与订单核心要素（流水 ID 留空）
#[allow(clippy::too_many_arguments)]
fn build_execution_report(
    meta: &NsMeta,
    report_index: i64,
    leaves_qty: i64,
    cum_qty: i64,
    last_qty: i64,
    last_px: i64,
) -> ExecutionReport {
    ExecutionReport {
        partition_no: PARTITION_NO,
        report_index,
        appl_id: meta.appl_id.clone(),
        reporting_pbuid: String::new(),
        submitting_pbuid: meta.submitting_pbuid.clone(),
        security_id: meta.security_id.clone(),
        security_id_source: meta.security_id_source.clone(),
        owner_type: meta.owner_type,
        clearing_firm: meta.clearing_firm.clone(),
        transact_time: meta.transact_time,
        user_info: meta.user_info.clone(),
        order_id: String::new(),
        cl_ord_id: meta.cl_ord_id.clone(),
        quote_msg_id: String::new(),
        exec_id: String::new(),
        exec_type: EXEC_TYPE_TRADE.to_string(),
        ord_status: ORD_STATUS_NORMAL.to_string(),
        last_px,
        last_qty,
        leaves_qty,
        cum_qty,
        side: meta.side.clone(),
        account_id: meta.account_id.clone(),
        branch_id: meta.branch_id.clone(),
        appl_extend: szse_binary::execution_report::ExecutionReportApplExtendEnum::Extend200115(
            Extend200115 {
                cash_margin: String::new(),
            },
        ),
    }
}
