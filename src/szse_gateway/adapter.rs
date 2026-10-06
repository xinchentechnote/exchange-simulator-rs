//! 入站报文 → 撮合命令适配（SZSE 二进制协议）。
//!
//! 字段映射规则见 docs/DEVELOPMENT_DESIGN.md §4.7.3（SZSE 差异：无 Account 账户字段，
//! 以 `AccountID` 数字化后作为 uid；NewOrder 顶层无 TimeInForce，统一按限价 GTC 处理）。

use matching_core::{OrderAction, OrderCommand, OrderCommandType, OrderType};
use szse_binary::new_order::NewOrder;
use szse_binary::order_cancel_request::OrderCancelRequest;

use super::report::NsMeta;

pub const SIDE_BUY: &str = "1";
pub const SIDE_SELL: &str = "2";
pub const ORD_TYPE_LIMIT: &str = "1";

/// ClOrdID/AccountID → 数字 ID：跳过非数字前缀后取数值
pub(crate) fn parse_ref(ref_id: &str) -> Option<u64> {
    ref_id
        .trim()
        .trim_start_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .ok()
}

fn parse_side(side: &str) -> Option<OrderAction> {
    match side {
        SIDE_BUY => Some(OrderAction::Bid),
        SIDE_SELL => Some(OrderAction::Ask),
        _ => None,
    }
}

/// NewOrder → PlaceOrder 命令；Err 为 BusinessReject 的 `business_reject_reason`（见 §6.4）。
#[derive(Debug)]
pub(crate) struct AdaptedNewOrder {
    pub cmd: OrderCommand,
    pub meta: NsMeta,
}

pub(crate) fn new_order(msg: &NewOrder) -> Result<AdaptedNewOrder, u32> {
    use super::report::{REJ_INVALID_FIELD, REJ_INVALID_SIZE};

    let order_id = parse_ref(&msg.cl_ord_id).ok_or(REJ_INVALID_FIELD)?;
    let action = parse_side(&msg.side).ok_or(REJ_INVALID_FIELD)?;
    let symbol: i32 = msg
        .security_id
        .trim()
        .parse()
        .map_err(|_| REJ_INVALID_FIELD)?;
    let uid: u64 = msg
        .account_id
        .trim()
        .parse()
        .map_err(|_| REJ_INVALID_FIELD)?;
    if msg.order_qty <= 0 {
        return Err(REJ_INVALID_SIZE);
    }
    if msg.price <= 0 {
        return Err(REJ_INVALID_FIELD);
    }

    let meta = NsMeta {
        appl_id: msg.appl_id.clone(),
        submitting_pbuid: msg.submitting_pbuid.clone(),
        security_id: msg.security_id.clone(),
        security_id_source: msg.security_id_source.clone(),
        owner_type: msg.owner_type,
        clearing_firm: msg.clearing_firm.clone(),
        transact_time: msg.transact_time,
        user_info: msg.user_info.clone(),
        cl_ord_id: msg.cl_ord_id.clone(),
        account_id: msg.account_id.clone(),
        branch_id: msg.branch_id.clone(),
        order_restrictions: msg.order_restrictions.clone(),
        side: msg.side.clone(),
        ord_type: msg.ord_type.clone(),
        // 协议报文无交易日字段，按 TransactTime 前 8 位推导（yyyymmddhhmmss → yyyymmdd）
        trade_date: (msg.transact_time / 1_000_000) as u32,
        price: msg.price,
        order_qty: msg.order_qty,
    };

    let cmd = OrderCommand {
        command: OrderCommandType::PlaceOrder,
        uid,
        order_id,
        symbol,
        price: msg.price,
        // 网关冻结策略：预留价 = 委托价（限价撮合不会以劣于限价的价格成交）
        reserve_price: msg.price,
        size: msg.order_qty,
        action,
        // SZSE NewOrder 顶层无 TimeInForce，统一按限价 GTC 处理
        order_type: OrderType::Gtc,
        ..Default::default()
    };
    Ok(AdaptedNewOrder { cmd, meta })
}

/// 撤单请求上下文字段（会话句柄由调用方补齐后组成 CancelMeta）
pub(crate) struct CancelFields {
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

/// OrderCancelRequest → CancelOrder 命令；目标订单以 `orig_cl_ord_id` 标识。
/// 注意：SZSE 撤单报文无账户字段，uid 由调用方从订单注册表回查补齐。
pub(crate) fn cancel_order(msg: &OrderCancelRequest) -> Result<(OrderCommand, CancelFields), u32> {
    use super::report::REJ_INVALID_FIELD;

    let order_id = parse_ref(&msg.orig_cl_ord_id).ok_or(REJ_INVALID_FIELD)?;
    let symbol: i32 = msg
        .security_id
        .trim()
        .parse()
        .map_err(|_| REJ_INVALID_FIELD)?;

    let cmd = OrderCommand {
        command: OrderCommandType::CancelOrder,
        order_id,
        symbol,
        ..Default::default()
    };
    let fields = CancelFields {
        appl_id: msg.appl_id.clone(),
        pbu: msg.submitting_pbuid.clone(),
        cl_ord_id: msg.cl_ord_id.clone(),
        orig_cl_ord_id: msg.orig_cl_ord_id.clone(),
        security_id: msg.security_id.clone(),
        security_id_source: msg.security_id_source.clone(),
        owner_type: msg.owner_type,
        clearing_firm: msg.clearing_firm.clone(),
        side: msg.side.clone(),
        user_info: msg.user_info.clone(),
        transact_time: msg.transact_time,
    };
    Ok((cmd, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_nos() -> NewOrder {
        NewOrder {
            appl_id: "010".into(),
            submitting_pbuid: "b0001".into(),
            security_id: "000001".into(),
            security_id_source: "102".into(),
            owner_type: 1,
            clearing_firm: "1".into(),
            transact_time: 20250101120000,
            user_info: "u0001".into(),
            cl_ord_id: "c0001".into(),
            account_id: "20001".into(),
            branch_id: "b01".into(),
            order_restrictions: "o01".into(),
            side: SIDE_BUY.into(),
            ord_type: ORD_TYPE_LIMIT.into(),
            order_qty: 1000,
            price: 100,
            appl_extend: szse_binary::new_order::NewOrderApplExtendEnum::Extend100101(
                szse_binary::extend_100101::Extend100101 {
                    stop_px: 0,
                    min_qty: 0,
                    max_price_levels: 0,
                    time_in_force: String::new(),
                    cash_margin: String::new(),
                },
            ),
        }
    }

    #[test]
    fn test_parse_ref() {
        assert_eq!(parse_ref("c0001"), Some(1));
        assert_eq!(parse_ref("20001"), Some(20001));
        assert_eq!(parse_ref("abc"), None);
        assert_eq!(parse_ref(""), None);
    }

    #[test]
    fn test_new_order_mapping() {
        let adapted = new_order(&sample_nos()).unwrap();
        assert_eq!(adapted.cmd.order_id, 1);
        assert_eq!(adapted.cmd.uid, 20001);
        assert_eq!(adapted.cmd.symbol, 1);
        assert_eq!(adapted.cmd.price, 100);
        assert_eq!(adapted.cmd.size, 1000);
        assert_eq!(adapted.cmd.reserve_price, 100);
        assert!(matches!(adapted.cmd.action, OrderAction::Bid));
        assert!(matches!(adapted.cmd.order_type, OrderType::Gtc));
        assert_eq!(adapted.meta.trade_date, 20250101);
        assert_eq!(adapted.meta.account_id, "20001");
    }

    #[test]
    fn test_new_order_field_errors() {
        let mut nos = sample_nos();
        nos.cl_ord_id = "no-digits".into();
        assert_eq!(
            new_order(&nos).unwrap_err(),
            super::super::report::REJ_INVALID_FIELD
        );

        let mut nos = sample_nos();
        nos.side = "9".into();
        assert_eq!(
            new_order(&nos).unwrap_err(),
            super::super::report::REJ_INVALID_FIELD
        );

        let mut nos = sample_nos();
        nos.order_qty = 0;
        assert_eq!(
            new_order(&nos).unwrap_err(),
            super::super::report::REJ_INVALID_SIZE
        );
    }

    #[test]
    fn test_cancel_mapping() {
        let mut ocr = OrderCancelRequest {
            appl_id: "010".into(),
            submitting_pbuid: "b0001".into(),
            security_id: "000001".into(),
            security_id_source: "102".into(),
            owner_type: 1,
            clearing_firm: "1".into(),
            transact_time: 20250101130000,
            user_info: "u0001".into(),
            cl_ord_id: "cx0001".into(),
            orig_cl_ord_id: "c0001".into(),
            side: SIDE_BUY.into(),
            order_id: String::new(),
            order_qty: 1000,
        };
        let (cmd, fields) = cancel_order(&ocr).unwrap();
        assert!(matches!(cmd.command, OrderCommandType::CancelOrder));
        assert_eq!(cmd.order_id, 1);
        assert_eq!(fields.orig_cl_ord_id, "c0001");

        ocr.orig_cl_ord_id = "x".into();
        assert!(cancel_order(&ocr).is_err());
    }
}
