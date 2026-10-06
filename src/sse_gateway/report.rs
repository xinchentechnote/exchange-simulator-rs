//! 回报生成与网关订单跟踪表。
//!
//! Confirm（受理确认）在订单受理时发送；每个 Trade 事件生成买卖双方各一条
//! Report(103)；拒绝发 OrderReject(204)；撤单成功发 Confirm(exec_type=4)、
//! 失败发 CancelReject(59)。
//!
//! 字段回填规则（依据 gt-auto 场景模板校验结果，sse_32/sse_103）：
//! - ReportIndex：按"订单 × 报文类别"从 0 递增（Confirm/Report 各自独立计数）；
//! - Confirm 回显订单要素；OrdCnfmId 留空；
//! - Report 仅回填成交要素（ClOrdID/SecurityID/Account/Side/Price/LastPx/LastQty/
//!   LeavesQty 等），OwnerType/OrderEntryTime/GrossTradeAmt/ClearingFirm/流水 ID 为 0/空。
//!
//! exec_type/ord_status 码值为设计值（待与协议细则校准）。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use matching_core::{CommandResultCode, MatcherEventType, OrderCommand};
use sse_binary::cancel_reject::CancelReject;
use sse_binary::confirm::Confirm;
use sse_binary::new_order_single::NewOrderSingle;
use sse_binary::order_cancel::OrderCancel;
use sse_binary::order_reject::OrderReject;
use sse_binary::report::Report;
use sse_binary::sse_binary::{SseBinary, SseBinaryBodyEnum};
use tokio::sync::mpsc;
use tracing::warn;

use crate::core_bridge::OrderStatus;

use super::GatewayShared;
use super::codec;

// ---- 回报拒绝原因码（模拟器自定义，见开发设计文档 §6.4）----
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

/// 核心结果码 → `ord_rej_reason`/`cxl_rej_reason`
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
    out: mpsc::Sender<SseBinary>,
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
    pub(crate) fn new(out: mpsc::Sender<SseBinary>) -> Self {
        Self {
            out,
            out_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_seq(&self) -> u64 {
        self.out_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn send(&self, body: SseBinaryBodyEnum) {
        let msg = SseBinary {
            msg_type: codec::msg_type_of(&body),
            msg_seq_num: self.next_seq(),
            msg_body_len: 0,
            body,
            checksum: 0,
        };
        if self.out.try_send(msg).is_err() {
            warn!("会话出站队列满或已关闭，丢弃回报");
        }
    }
}

/// NewOrderSingle 回填字段：生成 Confirm/Report 时原样回传
#[derive(Debug, Clone)]
pub struct NsMeta {
    pub biz_id: u32,
    pub biz_pbu: String,
    pub cl_ord_id: String,
    pub security_id: String,
    pub account: String,
    pub owner_type: u8,
    pub side: String,
    pub ord_type: String,
    pub time_in_force: String,
    pub credit_tag: String,
    pub clearing_firm: String,
    pub branch_id: String,
    pub user_info: String,
    pub transact_time: u64,
    pub trade_date: u32,
    pub price: i64,
    pub order_qty: i64,
}

/// 撤单请求上下文：结果到达时用于回填 CancelReject / 撤单 Confirm
#[derive(Debug, Clone)]
pub struct CancelMeta {
    pub session: SessionHandle,
    pub biz_id: u32,
    pub pbu: String,
    pub cl_ord_id: String,
    pub orig_cl_ord_id: String,
    pub security_id: String,
    pub branch_id: String,
    pub user_info: String,
    pub trade_date: u32,
    pub transact_time: u64,
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
    fn next_confirm_index(&mut self) -> u64 {
        let v = self.confirm_index;
        self.confirm_index += 1;
        v
    }

    fn next_report_index(&mut self) -> u64 {
        let v = self.report_index;
        self.report_index += 1;
        v
    }
}

/// PlaceOrder 终态 → Confirm / Report×N / OrderReject
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
            reason, "订单被拒，发送 OrderReject"
        );
        send_order_reject(&rec.session, &rec.meta, reason);
        return;
    }

    // 1) 受理确认（LeavesQty 为受理时的全额，与场景 sse_32 预期一致）
    let confirm_index = rec.next_confirm_index();
    rec.session.send(SseBinaryBodyEnum::Confirm(build_confirm(
        &rec.meta,
        confirm_index,
        rec.order_qty,
        0,
        EXEC_TYPE_NEW,
        ORD_STATUS_NORMAL,
    )));

    // 2) 逐事件生成成交回报（本方 + 对手方）
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
                rec.session.send(SseBinaryBodyEnum::Report(build_report(
                    &rec.meta,
                    report_index,
                    rec.leaves_qty,
                    rec.order_qty,
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
                    maker.session.send(SseBinaryBodyEnum::Report(build_report(
                        &maker.meta,
                        maker_report_index,
                        maker.leaves_qty,
                        maker.order_qty,
                        event.size,
                        event.price,
                    )));
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

/// CancelOrder 终态 → 撤单 Confirm(exec_type=4) 或 CancelReject
pub(crate) fn on_cancel_result(shared: &GatewayShared, cmd: &OrderCommand) {
    let Some(meta) = shared.pending_cancels.lock().unwrap().remove(&cmd.order_id) else {
        return;
    };
    if cmd.result_code != CommandResultCode::Success {
        warn!(order_id = cmd.order_id, reason = ?cmd.result_code, "撤单失败，发送 CancelReject");
        meta.session
            .send(SseBinaryBodyEnum::CancelReject(CancelReject {
                pbu: meta.pbu.clone(),
                set_id: 0,
                report_index: 0,
                biz_id: meta.biz_id,
                biz_pbu: meta.pbu.clone(),
                cl_ord_id: meta.cl_ord_id.clone(),
                security_id: meta.security_id.clone(),
                orig_cl_ord_id: meta.orig_cl_ord_id.clone(),
                branch_id: meta.branch_id.clone(),
                cxl_rej_reason: reject_reason(cmd.result_code),
                trade_date: meta.trade_date,
                transact_time: meta.transact_time,
                user_info: meta.user_info.clone(),
            }));
        return;
    }

    let mut registry = shared.registry.lock().unwrap();
    let (leaves_before, confirm_index, rec_meta) = match registry.get_mut(&cmd.order_id) {
        Some(rec) => {
            let leaves = rec.leaves_qty;
            rec.leaves_qty = 0;
            rec.status = OrderStatus::Filled;
            (leaves, rec.next_confirm_index(), Some(rec.meta.clone()))
        }
        None => (0, 0, None),
    };
    drop(registry);

    // 撤单确认：订单要素回填，transact_time/trade_date 取撤单请求值
    let session = meta.session.clone();
    if let Some(rec_meta) = &rec_meta {
        session.send(SseBinaryBodyEnum::Confirm(build_cancel_confirm(
            rec_meta,
            &meta,
            leaves_before,
            confirm_index,
        )));
    }
}

/// 直接发送 OrderReject（适配失败/未登录/重复订单号等前置拒绝）
pub(crate) fn send_order_reject(handle: &SessionHandle, meta: &NsMeta, reason: u32) {
    handle.send(SseBinaryBodyEnum::OrderReject(OrderReject {
        biz_id: meta.biz_id,
        biz_pbu: meta.biz_pbu.clone(),
        cl_ord_id: meta.cl_ord_id.clone(),
        security_id: meta.security_id.clone(),
        ord_rej_reason: reason,
        trade_date: meta.trade_date,
        transact_time: meta.transact_time,
        user_info: meta.user_info.clone(),
    }));
}

/// 未适配成功的 NewOrderSingle 直接拒绝（字段不完整，尽力回填）
pub(crate) fn send_order_reject_for_nos(handle: &SessionHandle, nos: &NewOrderSingle, reason: u32) {
    handle.send(SseBinaryBodyEnum::OrderReject(OrderReject {
        biz_id: nos.biz_id,
        biz_pbu: nos.biz_pbu.clone(),
        cl_ord_id: nos.cl_ord_id.clone(),
        security_id: nos.security_id.clone(),
        ord_rej_reason: reason,
        trade_date: (nos.transact_time / 1_000_000) as u32,
        transact_time: nos.transact_time,
        user_info: nos.user_info.clone(),
    }));
}

/// 适配失败的 OrderCancel 直接发 CancelReject
pub(crate) fn send_cancel_reject_for_oc(handle: &SessionHandle, oc: &OrderCancel, reason: u32) {
    handle.send(SseBinaryBodyEnum::CancelReject(CancelReject {
        pbu: oc.biz_pbu.clone(),
        set_id: 0,
        report_index: 0,
        biz_id: oc.biz_id,
        biz_pbu: oc.biz_pbu.clone(),
        cl_ord_id: oc.cl_ord_id.clone(),
        security_id: oc.security_id.clone(),
        orig_cl_ord_id: oc.orig_cl_ord_id.clone(),
        branch_id: oc.branch_id.clone(),
        cxl_rej_reason: reason,
        trade_date: (oc.transact_time / 1_000_000) as u32,
        transact_time: oc.transact_time,
        user_info: oc.user_info.clone(),
    }));
}

fn build_confirm(
    meta: &NsMeta,
    report_index: u64,
    leaves_qty: i64,
    cxl_qty: i64,
    exec_type: &str,
    ord_status: &str,
) -> Confirm {
    Confirm {
        pbu: meta.biz_pbu.clone(),
        set_id: 0,
        report_index,
        biz_id: meta.biz_id,
        exec_type: exec_type.to_string(),
        biz_pbu: meta.biz_pbu.clone(),
        cl_ord_id: meta.cl_ord_id.clone(),
        security_id: meta.security_id.clone(),
        account: meta.account.clone(),
        owner_type: meta.owner_type,
        side: meta.side.clone(),
        price: meta.price,
        order_qty: meta.order_qty,
        leaves_qty,
        cxl_qty,
        ord_type: meta.ord_type.clone(),
        time_in_force: meta.time_in_force.clone(),
        ord_status: ord_status.to_string(),
        credit_tag: meta.credit_tag.clone(),
        orig_cl_ord_id: String::new(),
        clearing_firm: meta.clearing_firm.clone(),
        branch_id: meta.branch_id.clone(),
        ord_rej_reason: 0,
        ord_cnfm_id: String::new(),
        orig_ord_cnfm_id: String::new(),
        trade_date: meta.trade_date,
        transact_time: meta.transact_time,
        user_info: meta.user_info.clone(),
    }
}

/// 撤单确认：原订单要素回填，transact_time/trade_date/user_info 取撤单请求值
fn build_cancel_confirm(
    order: &NsMeta,
    cancel: &CancelMeta,
    leaves_before: i64,
    report_index: u64,
) -> Confirm {
    Confirm {
        pbu: order.biz_pbu.clone(),
        set_id: 0,
        report_index,
        biz_id: order.biz_id,
        exec_type: EXEC_TYPE_CANCELLED.to_string(),
        biz_pbu: order.biz_pbu.clone(),
        cl_ord_id: order.cl_ord_id.clone(),
        security_id: order.security_id.clone(),
        account: order.account.clone(),
        owner_type: order.owner_type,
        side: order.side.clone(),
        price: order.price,
        order_qty: order.order_qty,
        leaves_qty: 0,
        cxl_qty: leaves_before,
        ord_type: order.ord_type.clone(),
        time_in_force: order.time_in_force.clone(),
        ord_status: ORD_STATUS_CANCELLED.to_string(),
        credit_tag: order.credit_tag.clone(),
        orig_cl_ord_id: cancel.orig_cl_ord_id.clone(),
        clearing_firm: order.clearing_firm.clone(),
        branch_id: order.branch_id.clone(),
        ord_rej_reason: 0,
        ord_cnfm_id: String::new(),
        orig_ord_cnfm_id: String::new(),
        trade_date: cancel.trade_date,
        transact_time: cancel.transact_time,
        user_info: cancel.user_info.clone(),
    }
}

/// 成交回报：仅回填成交与订单核心要素（OwnerType/OrderEntryTime/GrossTradeAmt/
/// ClearingFirm/流水 ID 按场景模板置 0/空）
#[allow(clippy::too_many_arguments)]
fn build_report(
    meta: &NsMeta,
    report_index: u64,
    leaves_qty: i64,
    order_qty: i64,
    last_qty: i64,
    last_px: i64,
) -> Report {
    Report {
        pbu: meta.biz_pbu.clone(),
        set_id: 0,
        report_index,
        biz_id: meta.biz_id,
        exec_type: EXEC_TYPE_TRADE.to_string(),
        biz_pbu: meta.biz_pbu.clone(),
        cl_ord_id: meta.cl_ord_id.clone(),
        security_id: meta.security_id.clone(),
        account: meta.account.clone(),
        owner_type: 0,
        order_entry_time: 0,
        last_px,
        last_qty,
        gross_trade_amt: 0,
        side: meta.side.clone(),
        order_qty,
        leaves_qty,
        ord_status: ORD_STATUS_NORMAL.to_string(),
        credit_tag: meta.credit_tag.clone(),
        clearing_firm: String::new(),
        branch_id: meta.branch_id.clone(),
        trd_cnfm_id: String::new(),
        ord_cnfm_id: String::new(),
        trade_date: meta.trade_date,
        transact_time: meta.transact_time,
        user_info: meta.user_info.clone(),
    }
}
