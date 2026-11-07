use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use matching_core::core::exchange::{ExchangeCore, ResultConsumer};
use matching_core::{
    CommandResultCode, CoreSymbolSpecification, MatcherEventType, OrderAction, OrderCommand,
    OrderCommandType, OrderType, SymbolId, UserId,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::base_info_service;
use crate::config::ExchangeConf;

/// 等待撮合结果的超时时间
pub const EXECUTE_TIMEOUT: Duration = Duration::from_secs(5);
/// 结果通道容量；满时丢弃并告警（等待方有超时兜底，迟到结果仍会写入订单注册表）
const RESULT_CHANNEL_CAPACITY: usize = 4096;
/// 结果广播容量（网关等旁路订阅者）；订阅者消费过慢产生 Lagged 时自行告警
const BROADCAST_CHANNEL_CAPACITY: usize = 1024;
/// 命令通道容量（对上游形成背压）
const COMMAND_CHANNEL_CAPACITY: usize = 1024;
/// 订单注册表容量上限，超出按先进先出淘汰；order_id 终态后不复用
const REGISTRY_CAPACITY: usize = 100_000;
/// 内存成交流水环形缓冲容量（供 /trades 查询；完整流水见 JSONL 审计文件）
const TRADE_BUFFER_CAPACITY: usize = 10_000;

/// 单笔成交（来自撮合事件 Trade）
#[derive(Debug, Clone, serde::Serialize)]
pub struct TradeFill {
    pub size: i64,
    pub price: i64,
    pub counterparty_order_id: UserId,
}

/// 成交流水视图：内存环形缓冲 + JSONL 审计共用
#[derive(Debug, Clone, serde::Serialize)]
pub struct TradeView {
    /// 成交时间（unix 毫秒）
    pub ts: i64,
    pub symbol: SymbolId,
    /// 吃单方订单号
    pub taker_order_id: u64,
    /// 挂单方订单号
    pub maker_order_id: u64,
    /// 吃单方方向
    pub taker_side: OrderAction,
    pub price: i64,
    pub size: i64,
}

/// JSONL 审计日志：每条订单终态（受理/拒绝/撤单）追加一行 JSON。
/// 写失败仅告警不阻塞撮合（模拟器语义；对账以文件为准，允许极端情况下缺行）。
pub struct AuditWriter {
    writer: Mutex<Option<std::io::BufWriter<std::fs::File>>>,
}

impl AuditWriter {
    pub fn open(path: &str) -> Arc<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path);
        let writer = match file {
            Ok(f) => Some(std::io::BufWriter::new(f)),
            Err(e) => {
                warn!(path = %path, error = %e, "审计日志打开失败，持久化已停用");
                None
            }
        };
        Arc::new(Self {
            writer: Mutex::new(writer),
        })
    }

    fn write_line(&self, line: &str) {
        use std::io::Write;
        let mut guard = self.writer.lock().unwrap();
        let Some(w) = guard.as_mut() else { return };
        if let Err(e) = writeln!(w, "{line}").and_then(|_| w.flush()) {
            warn!(error = %e, "审计日志写入失败，本行丢弃");
        }
    }
}

/// 运行汇总（/admin/state）
#[derive(Debug, serde::Serialize)]
pub struct StateSummary {
    pub orders_total: u64,
    pub trades_total: u64,
    pub registry_size: usize,
    /// 各币种余额总和（全部账户合计，对账用）
    pub balance_sums: BTreeMap<i32, i64>,
}

/// 命令的最终处理结果：受理（含挂单/成交）或拒绝
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionReport {
    pub order_id: u64,
    pub result_code: CommandResultCode,
    pub accepted: bool,
    pub leaves_qty: i64,
    pub trades: Vec<TradeFill>,
}

/// 撤单结果：核心返回码 + 撤单后的订单注册表快照。
/// 目标订单已被淘汰或非本渠道下单时 `status`/`leaves_qty` 为空。
#[derive(Debug, Clone, serde::Serialize)]
pub struct CancelReport {
    pub order_id: u64,
    pub result_code: CommandResultCode,
    pub accepted: bool,
    pub status: Option<OrderStatus>,
    pub leaves_qty: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// 挂单中
    New,
    /// 部分成交（剩余已撤销/拒绝）
    PartiallyFilled,
    /// 已全部成交
    Filled,
    /// 被拒绝，未进入订单簿
    Rejected,
}

/// 订单注册表记录：写入于命令下达，更新于结果路由
#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderRecord {
    pub order_id: u64,
    pub uid: UserId,
    pub symbol: SymbolId,
    pub action: OrderAction,
    pub order_type: OrderType,
    pub price: i64,
    /// 买单冻结预留价（撤单解冻计算用）
    pub reserve_price: i64,
    pub size: i64,
    pub filled_qty: i64,
    pub leaves_qty: i64,
    pub status: OrderStatus,
    pub result_code: CommandResultCode,
    pub trades: Vec<TradeFill>,
}

#[derive(Default)]
struct CoreShared {
    /// order_id → 结果回传通道（下单时插入，结果到达或超时后移除）
    pending: HashMap<u64, tokio::sync::oneshot::Sender<ExecutionReport>>,
    /// 被撤订单 order_id → 撤单结果回传通道（HTTP /cancel 等待；网关撤单走结果广播）
    cancel_pending: HashMap<u64, tokio::sync::oneshot::Sender<CancelReport>>,
    /// order_id → 订单最新状态（终态保留，直至容量淘汰）
    registry: HashMap<u64, OrderRecord>,
    registry_order: VecDeque<u64>,
    /// (uid, currency) → 余额镜像：初始资金叠加结果事件流水推算
    balances: HashMap<(UserId, i32), i64>,
    /// 成交流水环形缓冲（供 /trades 查询）
    trades: VecDeque<TradeView>,
    /// 终态订单计数（含拒绝）
    orders_total: u64,
    /// 成交笔数（Trade 事件数）
    trades_total: u64,
}

/// 撮合核心集成层：对上层提供"提交命令并等待结果"的同步语义，
/// 并维护订单注册表与余额镜像（均由结果事件驱动，只读供查询）。
pub struct CoreBridge {
    command_tx: mpsc::Sender<OrderCommand>,
    result_tx: mpsc::Sender<OrderCommand>,
    /// 结果广播：供 SSE 网关等旁路订阅者接收全部命令终态（含 matcher_events）
    result_broadcast: tokio::sync::broadcast::Sender<OrderCommand>,
    shared: Arc<Mutex<CoreShared>>,
    specs: Arc<Mutex<HashMap<SymbolId, CoreSymbolSpecification>>>,
    /// JSONL 审计日志（未配置时为 None）
    audit: Option<Arc<AuditWriter>>,
}

impl CoreBridge {
    /// 创建桥接层，返回 (桥, 命令接收端, 结果接收端)。
    /// 两个接收端分别交给 `spawn_core_task` / `spawn_result_task`；
    /// 测试场景可主动丢弃接收端以模拟核心不可用。
    pub fn new(
        audit: Option<Arc<AuditWriter>>,
    ) -> (
        Arc<Self>,
        mpsc::Receiver<OrderCommand>,
        mpsc::Receiver<OrderCommand>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = mpsc::channel(RESULT_CHANNEL_CAPACITY);
        let (result_broadcast, _) = tokio::sync::broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        let bridge = Arc::new(Self {
            command_tx,
            result_tx,
            result_broadcast,
            shared: Arc::new(Mutex::new(CoreShared::default())),
            specs: Arc::new(Mutex::new(HashMap::new())),
            audit,
        });
        (bridge, command_rx, result_rx)
    }

    /// 订阅全部命令终态（PlaceOrder/CancelOrder 等）。无订阅者时结果直接丢弃。
    pub fn subscribe_results(&self) -> tokio::sync::broadcast::Receiver<OrderCommand> {
        self.result_broadcast.subscribe()
    }

    /// 注册到撮合核心的结果消费者（运行在 Disruptor 消费线程，须非阻塞）。
    pub fn result_consumer(&self) -> ResultConsumer {
        let tx = self.result_tx.clone();
        Arc::new(move |cmd: &OrderCommand| {
            if tx.try_send(cmd.clone()).is_err() {
                warn!(order_id = cmd.order_id, "结果通道已满，丢弃结果");
            }
        })
    }

    /// 登记标的规定（引导阶段调用，供结果结算换算使用）。
    pub fn add_symbol_spec(&self, spec: CoreSymbolSpecification) {
        self.specs.lock().unwrap().insert(spec.symbol_id, spec);
    }

    /// 提交 PlaceOrder 命令并等待最终结果（含风控/撮合拒绝）。
    pub async fn execute(
        &self,
        cmd: OrderCommand,
    ) -> Result<ExecutionReport, crate::errors::SimError> {
        let order_id = cmd.order_id;
        let (report_tx, report_rx) = tokio::sync::oneshot::channel();
        {
            let mut shared = self.shared.lock().unwrap();
            if shared.registry.contains_key(&order_id) || shared.pending.contains_key(&order_id) {
                return Err(crate::errors::SimError::DuplicateOrderId);
            }
            shared.pending.insert(order_id, report_tx);
        }
        if self.command_tx.send(cmd).await.is_err() {
            self.shared.lock().unwrap().pending.remove(&order_id);
            return Err(crate::errors::SimError::CoreUnavailable);
        }
        match tokio::time::timeout(EXECUTE_TIMEOUT, report_rx).await {
            Ok(Ok(report)) => Ok(report),
            // 等待方未超时而通道被丢弃：桥接层正在停机
            Ok(Err(_)) => Err(crate::errors::SimError::CoreUnavailable),
            Err(_) => {
                // 否则该 order_id 会永久留在 pending 里，后续复用同一订单号会被误判为重复
                self.shared.lock().unwrap().pending.remove(&order_id);
                Err(crate::errors::SimError::CoreTimeout)
            }
        }
    }

    /// 提交命令但不等待结果（撤单等由结果广播回传的命令）。
    pub async fn submit(&self, cmd: OrderCommand) -> Result<(), crate::errors::SimError> {
        self.command_tx
            .send(cmd)
            .await
            .map_err(|_| crate::errors::SimError::CoreUnavailable)
    }

    /// 提交 CancelOrder 并等待撤单终态（cmd.order_id 为被撤订单号）。
    /// 等待方超时或停机时清理登记，同一订单不会被后续撤单误判为重复。
    pub async fn cancel(&self, cmd: OrderCommand) -> Result<CancelReport, crate::errors::SimError> {
        let order_id = cmd.order_id;
        let (report_tx, report_rx) = tokio::sync::oneshot::channel();
        {
            let mut shared = self.shared.lock().unwrap();
            if shared.cancel_pending.contains_key(&order_id) {
                return Err(crate::errors::SimError::CancelInFlight);
            }
            shared.cancel_pending.insert(order_id, report_tx);
        }
        if self.command_tx.send(cmd).await.is_err() {
            self.shared.lock().unwrap().cancel_pending.remove(&order_id);
            return Err(crate::errors::SimError::CoreUnavailable);
        }
        match tokio::time::timeout(EXECUTE_TIMEOUT, report_rx).await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(_)) => Err(crate::errors::SimError::CoreUnavailable),
            Err(_) => {
                self.shared.lock().unwrap().cancel_pending.remove(&order_id);
                Err(crate::errors::SimError::CoreTimeout)
            }
        }
    }

    /// 核心命令通道是否仍可写入（接收端随核心任务退出而关闭，用于就绪探针）
    pub fn core_available(&self) -> bool {
        !self.command_tx.is_closed()
    }

    /// 查询订单当前状态
    pub fn order_record(&self, order_id: u64) -> Option<OrderRecord> {
        self.shared.lock().unwrap().registry.get(&order_id).cloned()
    }

    /// 查询账户各币种余额（镜像值，可能与核心内部状态存在极小窗口的延迟）
    pub fn balances_of(&self, uid: UserId) -> BTreeMap<i32, i64> {
        self.shared
            .lock()
            .unwrap()
            .balances
            .iter()
            .filter(|((u, _), _)| *u == uid)
            .map(|((_, currency), amount)| (*currency, *amount))
            .collect()
    }

    /// 查询最近的成交流水（按时间正序），可按标的过滤；limit 截取最近的 N 笔
    pub fn recent_trades(&self, symbol: Option<SymbolId>, limit: usize) -> Vec<TradeView> {
        let shared = self.shared.lock().unwrap();
        let mut filtered = shared
            .trades
            .iter()
            .filter(|t| symbol.is_none_or(|s| t.symbol == s))
            .cloned()
            .collect::<Vec<_>>();
        if filtered.len() <= limit {
            filtered
        } else {
            filtered.split_off(filtered.len() - limit)
        }
    }

    /// 运行汇总（对账用）
    pub fn state_summary(&self) -> StateSummary {
        let shared = self.shared.lock().unwrap();
        let mut balance_sums: BTreeMap<i32, i64> = BTreeMap::new();
        for ((_, currency), amount) in &shared.balances {
            *balance_sums.entry(*currency).or_insert(0) += amount;
        }
        StateSummary {
            orders_total: shared.orders_total,
            trades_total: shared.trades_total,
            registry_size: shared.registry.len(),
            balance_sums,
        }
    }

    /// 结果路由：由 result_task 逐条调用。记账逻辑与 matching-core RiskEngine
    /// 的 R1 冻结 / R2 结算保持一致（镜像推算）。
    pub fn handle_result(&self, cmd: &OrderCommand) {
        route_result(&self.shared, &self.specs, self.audit.as_deref(), cmd);
    }
}

type SharedState = Arc<Mutex<CoreShared>>;
type SpecMap = Arc<Mutex<HashMap<SymbolId, CoreSymbolSpecification>>>;

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 结果路由（自由函数）：result_task 只持有 shared/specs/audit，
/// 不持有 CoreBridge，避免 core_task ↔ result_task 经由命令/结果通道的存活环。
fn route_result(
    shared: &SharedState,
    specs: &SpecMap,
    audit: Option<&AuditWriter>,
    cmd: &OrderCommand,
) {
    match cmd.command {
        OrderCommandType::PlaceOrder => on_place_result(shared, specs, audit, cmd),
        OrderCommandType::BalanceAdjustment => {
            if cmd.result_code == CommandResultCode::Success {
                let mut shared = shared.lock().unwrap();
                *shared.balances.entry((cmd.uid, cmd.currency)).or_insert(0) += cmd.amount;
            }
        }
        OrderCommandType::CancelOrder => on_cancel_result(shared, specs, audit, cmd),
        _ => {}
    }
}

/// 撤单结果处理：先按业务语义解冻记账，再回传给等待撤单终态的调用方。
/// 回传必须在任何返回码下都执行（含拒绝），否则 HTTP /cancel 会白等到超时。
fn on_cancel_result(
    shared: &SharedState,
    specs: &SpecMap,
    audit: Option<&AuditWriter>,
    cmd: &OrderCommand,
) {
    unfreeze_on_cancel(shared, specs, audit, cmd);
    let mut shared = shared.lock().unwrap();
    if let Some(report_tx) = shared.cancel_pending.remove(&cmd.order_id) {
        let record = shared.registry.get(&cmd.order_id);
        let accepted = cmd.result_code == CommandResultCode::Success;
        let _ = report_tx.send(CancelReport {
            order_id: cmd.order_id,
            result_code: cmd.result_code,
            accepted,
            status: record.map(|r| r.status),
            leaves_qty: record.map(|r| r.leaves_qty),
        });
    }
}

/// 撤单解冻镜像：按业务语义全额解冻剩余量（本金 + 手续费）。
///
/// 注意：matching-core v0.2.2 撤单产出的 Reject 事件 `bidder_hold_price=0`，
/// 其 R2 结算对买单只退手续费不退本金（上游缺陷）；镜像按正确语义计算，
/// 因此余额镜像可能高于核心内部冻结口径（核心偏保守，风险方向安全）。
fn unfreeze_on_cancel(
    shared: &SharedState,
    specs: &SpecMap,
    audit: Option<&AuditWriter>,
    cmd: &OrderCommand,
) {
    if cmd.result_code != CommandResultCode::Success {
        return;
    }
    let spec = specs.lock().unwrap().get(&cmd.symbol).cloned();
    let mut shared = shared.lock().unwrap();
    let Some(spec) = &spec else { return };
    let (uid, currency, remaining, refund) = {
        let Some(rec) = shared.registry.get_mut(&cmd.order_id) else {
            return; // 非网关/HTTP 渠道订单或已淘汰
        };
        let remaining = rec.leaves_qty;
        let refund = match rec.action {
            OrderAction::Ask => remaining * spec.base_scale_k,
            OrderAction::Bid => {
                let reserve =
                    if matches!(rec.order_type, OrderType::FokBudget | OrderType::IocBudget) {
                        rec.price
                    } else {
                        rec.reserve_price
                    };
                remaining * reserve * spec.quote_scale_k + remaining * spec.taker_fee
            }
        };
        let currency = match rec.action {
            OrderAction::Bid => spec.quote_currency,
            OrderAction::Ask => spec.base_currency,
        };
        rec.leaves_qty = 0;
        rec.status = OrderStatus::Filled;
        (rec.uid, currency, remaining, refund)
    };
    *shared.balances.entry((uid, currency)).or_insert(0) += refund;
    drop(shared);
    if let Some(audit) = audit {
        audit.write_line(
            &serde_json::json!({
                "ts": unix_millis(),
                "kind": "cancel",
                "order_id": cmd.order_id,
                "uid": cmd.uid,
                "symbol": cmd.symbol,
                "result_code": cmd.result_code,
                "leaves_before": remaining,
                "refund": refund,
            })
            .to_string(),
        );
    }
    info!(order_id = cmd.order_id, refund, "撤单解冻");
}

fn on_place_result(
    shared: &SharedState,
    specs: &SpecMap,
    audit: Option<&AuditWriter>,
    cmd: &OrderCommand,
) {
    let spec = specs.lock().unwrap().get(&cmd.symbol).cloned();
    let accepted = cmd.result_code == CommandResultCode::Success;
    let mut shared = shared.lock().unwrap();

    // 1) 冻结镜像：仅在风控通过（进入撮合并 Success 终态）时发生
    if accepted && let Some(spec) = &spec {
        let hold = match cmd.action {
            OrderAction::Bid => {
                let reserve =
                    if matches!(cmd.order_type, OrderType::FokBudget | OrderType::IocBudget) {
                        cmd.price
                    } else {
                        cmd.reserve_price
                    };
                cmd.size * reserve * spec.quote_scale_k + cmd.size * spec.taker_fee
            }
            OrderAction::Ask => cmd.size * spec.base_scale_k,
        };
        let currency = match cmd.action {
            OrderAction::Bid => spec.quote_currency,
            OrderAction::Ask => spec.base_currency,
        };
        *shared.balances.entry((cmd.uid, currency)).or_insert(0) -= hold;
    }

    // 2) 事件结算镜像（taker 来自命令本身，maker 来自事件中的对手方信息）
    let mut trades = Vec::new();
    let mut filled_qty = 0i64;
    let mut has_reject = false;
    for event in &cmd.matcher_events {
        match event.event_type {
            MatcherEventType::Trade => {
                filled_qty += event.size;
                trades.push(TradeFill {
                    size: event.size,
                    price: event.price,
                    counterparty_order_id: event.matched_order_id,
                });
                // 成交流水：环形缓冲（/trades 查询）+ 计数
                shared.trades_total += 1;
                shared.trades.push_back(TradeView {
                    ts: unix_millis(),
                    symbol: cmd.symbol,
                    taker_order_id: cmd.order_id,
                    maker_order_id: event.matched_order_id,
                    taker_side: cmd.action,
                    price: event.price,
                    size: event.size,
                });
                while shared.trades.len() > TRADE_BUFFER_CAPACITY {
                    shared.trades.pop_front();
                }
                // maker 侧订单注册表同步更新（其成交来自本 taker 命令的事件）
                if let Some(maker) = shared.registry.get_mut(&event.matched_order_id) {
                    maker.filled_qty += event.size;
                    maker.leaves_qty = (maker.size - maker.filled_qty).max(0);
                    maker.status = if maker.leaves_qty == 0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    maker.trades.push(TradeFill {
                        size: event.size,
                        price: event.price,
                        counterparty_order_id: cmd.order_id,
                    });
                }
                if let Some(spec) = &spec {
                    let taker_sell = cmd.action == OrderAction::Ask;
                    if taker_sell {
                        let income = event.size * event.price * spec.quote_scale_k
                            - event.size * spec.taker_fee;
                        *shared
                            .balances
                            .entry((cmd.uid, spec.quote_currency))
                            .or_insert(0) += income;
                        let refund = event.size
                            * (event.bidder_hold_price - event.price)
                            * spec.quote_scale_k;
                        *shared
                            .balances
                            .entry((event.matched_order_uid, spec.quote_currency))
                            .or_insert(0) += refund;
                        *shared
                            .balances
                            .entry((event.matched_order_uid, spec.base_currency))
                            .or_insert(0) += event.size * spec.base_scale_k;
                    } else {
                        let refund = event.size
                            * (event.bidder_hold_price - event.price)
                            * spec.quote_scale_k;
                        *shared
                            .balances
                            .entry((cmd.uid, spec.quote_currency))
                            .or_insert(0) += refund;
                        *shared
                            .balances
                            .entry((cmd.uid, spec.base_currency))
                            .or_insert(0) += event.size * spec.base_scale_k;
                        let income = event.size * event.price * spec.quote_scale_k
                            - event.size * spec.maker_fee;
                        *shared
                            .balances
                            .entry((event.matched_order_uid, spec.quote_currency))
                            .or_insert(0) += income;
                    }
                }
                debug!(
                    order_id = cmd.order_id,
                    size = event.size,
                    price = event.price,
                    "成交"
                );
            }
            MatcherEventType::Reject | MatcherEventType::Reduce => {
                has_reject = true;
                if let Some(spec) = &spec {
                    if cmd.action == OrderAction::Ask {
                        *shared
                            .balances
                            .entry((cmd.uid, spec.base_currency))
                            .or_insert(0) += event.size * spec.base_scale_k;
                    } else {
                        let refund = event.size * event.bidder_hold_price * spec.quote_scale_k
                            + event.size * spec.taker_fee;
                        *shared
                            .balances
                            .entry((cmd.uid, spec.quote_currency))
                            .or_insert(0) += refund;
                    }
                }
            }
        }
    }

    // 3) 订单注册表 + 结果回传 + 审计
    let leaves_qty = (cmd.size - filled_qty).max(0);
    let status = if !accepted {
        OrderStatus::Rejected
    } else if leaves_qty == 0 {
        OrderStatus::Filled
    } else if has_reject {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::New
    };
    let reserve_price = if matches!(cmd.order_type, OrderType::FokBudget | OrderType::IocBudget) {
        cmd.price
    } else {
        cmd.reserve_price
    };
    let record = OrderRecord {
        order_id: cmd.order_id,
        uid: cmd.uid,
        symbol: cmd.symbol,
        action: cmd.action,
        order_type: cmd.order_type,
        price: cmd.price,
        reserve_price,
        size: cmd.size,
        filled_qty,
        leaves_qty,
        status,
        result_code: cmd.result_code,
        trades: trades.clone(),
    };
    shared.registry.insert(cmd.order_id, record);
    shared.registry_order.push_back(cmd.order_id);
    while shared.registry_order.len() > REGISTRY_CAPACITY {
        if let Some(oldest) = shared.registry_order.pop_front()
            && oldest != cmd.order_id
        {
            shared.registry.remove(&oldest);
        }
    }
    shared.orders_total += 1;
    if let Some(audit) = audit {
        audit.write_line(
            &serde_json::json!({
                "ts": unix_millis(),
                "kind": "place",
                "order_id": cmd.order_id,
                "uid": cmd.uid,
                "symbol": cmd.symbol,
                "action": cmd.action,
                "order_type": cmd.order_type,
                "price": cmd.price,
                "size": cmd.size,
                "result_code": cmd.result_code,
                "status": status,
                "filled_qty": filled_qty,
                "leaves_qty": leaves_qty,
                "trades": trades,
            })
            .to_string(),
        );
    }
    let report = ExecutionReport {
        order_id: cmd.order_id,
        result_code: cmd.result_code,
        accepted,
        leaves_qty,
        trades,
    };
    if let Some(report_tx) = shared.pending.remove(&cmd.order_id) {
        let _ = report_tx.send(report);
    }
    drop(shared);

    if accepted {
        info!(
            order_id = cmd.order_id,
            uid = cmd.uid,
            symbol = cmd.symbol,
            "订单受理"
        );
    } else {
        warn!(
            order_id = cmd.order_id,
            uid = cmd.uid,
            symbol = cmd.symbol,
            result = ?cmd.result_code,
            "订单被拒"
        );
    }
}

/// 撮合核心宿主任务：startup 后循环消费命令通道。通道关闭即退出。
/// 不持有 CoreBridge——否则其内部的 command_tx 会让本任务永远等不到通道关闭。
fn spawn_core_task(
    mut core: ExchangeCore,
    mut command_rx: mpsc::Receiver<OrderCommand>,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        core.startup();
        info!("撮合核心已启动（Disruptor 就绪）");
        while let Some(cmd) = command_rx.recv().await {
            core.submit_command(cmd);
        }
        // 通知 Disruptor 消费线程停止自旋，释放其闭包持有的结果通道发送端；
        // 否则结果通道永不关闭，result_task 只能等 5s 超时退出
        core.submit_command(OrderCommand {
            command: OrderCommandType::ShutdownSignal,
            ..Default::default()
        });
        info!("命令通道已关闭，撮合核心任务退出");
    })
}

/// 结果路由任务：桥接 Disruptor 消费线程与 LocalSet 上的等待方。
fn spawn_result_task(
    shared: SharedState,
    specs: SpecMap,
    audit: Option<Arc<AuditWriter>>,
    result_broadcast: tokio::sync::broadcast::Sender<OrderCommand>,
    mut result_rx: mpsc::Receiver<OrderCommand>,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(mut cmd) = result_rx.recv().await {
            route_result(&shared, &specs, audit.as_deref(), &cmd);
            // 旁路广播（网关回报生成）；无订阅者时报错被忽略
            cmd.matcher_events.truncate(64);
            let _ = result_broadcast.send(cmd);
        }
        info!("结果路由任务退出");
    })
}

/// 引导一个交易所实例：加载数据（fail-fast）→ 注册标的与账户 → 启动任务。
/// 引导命令在 core.startup() 之前提交，走同步路径，结果码可就地校验。
pub async fn bootstrap_exchange(
    name: &str,
    conf: &ExchangeConf,
) -> Result<(Arc<CoreBridge>, JoinHandle<()>, JoinHandle<()>)> {
    let core_config = conf
        .matcher
        .core
        .to_exchange_config()
        .context("撮合核心配置无效")?;
    let mut core = ExchangeCore::new(core_config);

    let symbols = base_info_service::load_symbol_info(&conf.matcher.symbol_info_path)
        .with_context(|| format!("加载标的数据失败: {}", conf.matcher.symbol_info_path))?;
    let accounts = base_info_service::load_account_info(&conf.matcher.account_info_path)
        .with_context(|| format!("加载账户数据失败: {}", conf.matcher.account_info_path))?;
    info!(
        exchange = name,
        symbols = symbols.len(),
        accounts = accounts.len(),
        "基础数据加载完成"
    );

    let audit = conf
        .matcher
        .persistence
        .orders_log_path
        .as_deref()
        .map(AuditWriter::open);
    let (bridge, command_rx, result_rx) = CoreBridge::new(audit.clone());
    // 结果消费者必须在引导命令之前注册，BalanceAdjustment 才能进入余额镜像
    core.set_result_consumer(bridge.result_consumer());
    for spec in &symbols {
        bridge.add_symbol_spec(spec.clone());
    }
    // 标的为静态配置（不进 WAL），恢复与全新启动都需要重新注册
    for spec in symbols {
        core.add_symbol(spec);
    }

    // WAL 故障恢复（T3.2）：先开启 journaling（append 模式，重放不产生写入），
    // 若 WAL 存在且非空则全量重放恢复状态并跳过账户引导（账户/订单都在 WAL 中）。
    let mut recovered = false;
    if let Some(wal) = conf.matcher.persistence.wal_path.as_deref() {
        core.enable_journaling(wal)
            .with_context(|| format!("开启 WAL 失败: {wal}"))?;
        let wal_non_empty = std::path::Path::new(wal)
            .metadata()
            .map(|m| m.len() > 0)
            .unwrap_or(false);
        if wal_non_empty {
            info!(exchange = name, wal = %wal, "发现 WAL，重放恢复状态");
            core.replay_journal(wal)
                .with_context(|| format!("WAL 重放失败: {wal}"))?;
            recovered = true;
            info!(exchange = name, "WAL 重放完成，跳过账户引导");
        }
    }

    if !recovered {
        for account in &accounts {
            let done = core.submit_command(OrderCommand {
                command: OrderCommandType::AddUser,
                uid: account.uid,
                ..Default::default()
            });
            if done.result_code != CommandResultCode::Success {
                anyhow::bail!(
                    "[{name}] AddUser 失败 uid={} result={:?}",
                    account.uid,
                    done.result_code
                );
            }
            let done = core.submit_command(OrderCommand {
                command: OrderCommandType::BalanceAdjustment,
                uid: account.uid,
                currency: account.currency,
                amount: account.amount,
                ..Default::default()
            });
            if done.result_code != CommandResultCode::Success {
                anyhow::bail!(
                    "[{name}] BalanceAdjustment 失败 uid={} result={:?}",
                    account.uid,
                    done.result_code
                );
            }
        }
        info!(exchange = name, accounts = accounts.len(), "账户引导完成");
    }

    let core_task = spawn_core_task(core, command_rx);
    let result_task = spawn_result_task(
        bridge.shared.clone(),
        bridge.specs.clone(),
        audit,
        bridge.result_broadcast.clone(),
        result_rx,
    );
    Ok((bridge, core_task, result_task))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_reports_unavailable_when_core_dropped() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (bridge, command_rx, _result_rx) = CoreBridge::new(None);
            drop(command_rx);
            let cmd = OrderCommand {
                command: OrderCommandType::PlaceOrder,
                order_id: 1,
                ..Default::default()
            };
            let result = bridge.execute(cmd).await;
            assert!(matches!(
                result,
                Err(crate::errors::SimError::CoreUnavailable)
            ));
        });
    }

    #[test]
    fn test_duplicate_order_id_rejected() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(tokio::task::LocalSet::new().run_until(async {
            let (bridge, command_rx, _result_rx) = CoreBridge::new(None);
            let _keep_alive = command_rx; // 通道保持开启
            let cmd = |id: u64| OrderCommand {
                command: OrderCommandType::PlaceOrder,
                order_id: id,
                ..Default::default()
            };
            // 第一条命令挂起等待结果，第二条同号命令应立即被幂等层拒绝
            let task_bridge = bridge.clone();
            let first = tokio::task::spawn_local(async move { task_bridge.execute(cmd(7)).await });
            // 显式让出，确保第一条命令已进入 pending（其 send 因通道空闲不会自行让出）
            tokio::task::yield_now().await;
            let second = bridge.execute(cmd(7)).await;
            assert!(matches!(
                second,
                Err(crate::errors::SimError::DuplicateOrderId)
            ));
            first.abort();
        }));
    }
}
