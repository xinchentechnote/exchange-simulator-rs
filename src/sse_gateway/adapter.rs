//! 入站报文 → 撮合命令适配。
//!
//! 字段映射规则见 docs/DEVELOPMENT_DESIGN.md §4.7.3；`ord_type`/`time_in_force`
//! 等协议码值为设计值（待与协议细则校准），集中在常量定义。

use matching_core::{OrderAction, OrderCommand, OrderCommandType, OrderType};
use sse_binary::new_order_single::NewOrderSingle;
use sse_binary::order_cancel::OrderCancel;

use super::report::NsMeta;

pub const SIDE_BUY: &str = "1";
pub const SIDE_SELL: &str = "2";
/// SSE Binary v0.57：OrdType 2=限价；"1"（市转撤）为历史约定兼容值，均按限价处理
pub const ORD_TYPE_LIMIT: &str = "2";
/// 历史/兼容取值（gt-auto 用例沿用），同样按限价处理
pub const ORD_TYPE_LIMIT_COMPAT: &str = "1";
pub const TIF_DAY: &str = "0";
pub const TIF_IOC: &str = "1";
pub const TIF_FOK: &str = "2";

/// ClOrdID → 数字订单号：跳过非数字前缀后取数值（如 "c10001" → 10001）
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

fn order_type_of(ord_type: &str, tif: &str) -> Option<(OrderType, &'static str)> {
    if ord_type != ORD_TYPE_LIMIT && ord_type != ORD_TYPE_LIMIT_COMPAT {
        return None; // 仅支持限价
    }
    match tif {
        TIF_DAY => Some((OrderType::Gtc, TIF_DAY)),
        TIF_IOC => Some((OrderType::Ioc, TIF_IOC)),
        TIF_FOK => Some((OrderType::Fok, TIF_FOK)),
        _ => None,
    }
}

/// 由报文生成撮合命令；Err 为 OrderReject 的 `ord_rej_reason`（见 §6.4）。
#[derive(Debug)]
pub(crate) struct AdaptedNewOrder {
    pub cmd: OrderCommand,
    pub meta: NsMeta,
}

pub(crate) fn new_order(msg: &NewOrderSingle) -> Result<AdaptedNewOrder, u32> {
    use super::report::{REJ_INTERNAL, REJ_INVALID_FIELD, REJ_INVALID_SIZE};

    let order_id = parse_ref(&msg.cl_ord_id).ok_or(REJ_INVALID_FIELD)?;
    let action = parse_side(&msg.side).ok_or(REJ_INVALID_FIELD)?;
    let symbol: i32 = msg
        .security_id
        .trim()
        .parse()
        .map_err(|_| REJ_INVALID_FIELD)?;
    let uid: u64 = msg.account.trim().parse().map_err(|_| REJ_INVALID_FIELD)?;
    if msg.order_qty <= 0 {
        return Err(REJ_INVALID_SIZE);
    }
    if msg.price <= 0 {
        return Err(REJ_INVALID_FIELD);
    }
    let (order_type, tif) = order_type_of(&msg.ord_type, &msg.time_in_force).ok_or(REJ_INTERNAL)?;

    let meta = NsMeta {
        biz_id: msg.biz_id,
        biz_pbu: msg.biz_pbu.clone(),
        cl_ord_id: msg.cl_ord_id.clone(),
        security_id: msg.security_id.clone(),
        account: msg.account.clone(),
        owner_type: msg.owner_type,
        side: msg.side.clone(),
        ord_type: msg.ord_type.clone(),
        time_in_force: tif.to_string(),
        credit_tag: msg.credit_tag.clone(),
        clearing_firm: msg.clearing_firm.clone(),
        branch_id: msg.branch_id.clone(),
        user_info: msg.user_info.clone(),
        transact_time: msg.transact_time,
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
        order_type,
        ..Default::default()
    };
    Ok(AdaptedNewOrder { cmd, meta })
}

/// 撤单请求上下文字段（会话句柄由调用方补齐后组成 CancelMeta）
pub(crate) struct CancelFields {
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

/// 撤单报文 → CancelOrder 命令；目标订单以 `orig_cl_ord_id` 标识。
pub(crate) fn cancel_order(msg: &OrderCancel) -> Result<(OrderCommand, CancelFields), u32> {
    use super::report::REJ_INVALID_FIELD;

    let order_id = parse_ref(&msg.orig_cl_ord_id).ok_or(REJ_INVALID_FIELD)?;
    let uid: u64 = msg.account.trim().parse().map_err(|_| REJ_INVALID_FIELD)?;
    let symbol: i32 = msg
        .security_id
        .trim()
        .parse()
        .map_err(|_| REJ_INVALID_FIELD)?;

    let cmd = OrderCommand {
        command: OrderCommandType::CancelOrder,
        uid,
        order_id,
        symbol,
        ..Default::default()
    };
    let fields = CancelFields {
        biz_id: msg.biz_id,
        pbu: msg.biz_pbu.clone(),
        cl_ord_id: msg.cl_ord_id.clone(),
        orig_cl_ord_id: msg.orig_cl_ord_id.clone(),
        security_id: msg.security_id.clone(),
        branch_id: msg.branch_id.clone(),
        user_info: msg.user_info.clone(),
        trade_date: (msg.transact_time / 1_000_000) as u32,
        transact_time: msg.transact_time,
    };
    Ok((cmd, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_nos() -> NewOrderSingle {
        NewOrderSingle {
            biz_id: 10,
            biz_pbu: "pbu001".into(),
            cl_ord_id: "c10001".into(),
            security_id: "600000".into(),
            account: "10001".into(),
            owner_type: 1,
            side: SIDE_BUY.into(),
            price: 1000,
            order_qty: 100,
            ord_type: ORD_TYPE_LIMIT.into(),
            time_in_force: TIF_DAY.into(),
            transact_time: 20250106101530,
            credit_tag: "1".into(),
            clearing_firm: "1001".into(),
            branch_id: "b001".into(),
            user_info: "u000123".into(),
        }
    }

    #[test]
    fn test_parse_ref() {
        assert_eq!(parse_ref("c10001"), Some(10001));
        assert_eq!(parse_ref("10001"), Some(10001));
        assert_eq!(parse_ref("order-9"), Some(9));
        assert_eq!(parse_ref("abc"), None);
        assert_eq!(parse_ref(""), None);
    }

    #[test]
    fn test_new_order_mapping() {
        let adapted = new_order(&sample_nos()).unwrap();
        assert_eq!(adapted.cmd.order_id, 10001);
        assert_eq!(adapted.cmd.uid, 10001);
        assert_eq!(adapted.cmd.symbol, 600000);
        assert_eq!(adapted.cmd.price, 1000);
        assert_eq!(adapted.cmd.size, 100);
        assert_eq!(adapted.cmd.reserve_price, 1000);
        assert!(matches!(adapted.cmd.action, OrderAction::Bid));
        assert!(matches!(adapted.cmd.order_type, OrderType::Gtc));
        assert_eq!(adapted.meta.trade_date, 20250106);
        assert_eq!(adapted.meta.biz_pbu, "pbu001");
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

        let mut nos = sample_nos();
        nos.ord_type = "3".to_string(); // 市转限等非限价暂不支持
        assert_eq!(
            new_order(&nos).unwrap_err(),
            super::super::report::REJ_INTERNAL
        );
    }

    #[test]
    fn test_cancel_mapping() {
        let mut oc = OrderCancel {
            biz_id: 10,
            biz_pbu: "pbu001".into(),
            cl_ord_id: "cx0001".into(),
            security_id: "600000".into(),
            account: "10001".into(),
            owner_type: 1,
            side: SIDE_BUY.into(),
            orig_cl_ord_id: "c10001".into(),
            transact_time: 20250106102000,
            branch_id: "b001".into(),
            user_info: "u000123".into(),
        };
        let (cmd, fields) = cancel_order(&oc).unwrap();
        assert!(matches!(cmd.command, OrderCommandType::CancelOrder));
        assert_eq!(cmd.order_id, 10001);
        assert_eq!(fields.orig_cl_ord_id, "c10001");

        oc.orig_cl_ord_id = "x".into();
        assert!(cancel_order(&oc).is_err());
    }
}
