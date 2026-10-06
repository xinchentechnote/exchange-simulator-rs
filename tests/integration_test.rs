//! L2 集成测试：真实撮合核心 + 桥接层的端到端行为验证。
//! 用例设计见 docs/DEVELOPMENT_DESIGN.md §8.3。

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use exchange_simulator_rs::config::{CoreConf, ExchangeConf, MatcherConf};
use exchange_simulator_rs::core_bridge::{self, CoreBridge, OrderStatus};
use exchange_simulator_rs::errors::SimError;
use matching_core::{CommandResultCode, OrderAction, OrderCommand, OrderCommandType};
use tokio::task::JoinHandle;

static SEQ: AtomicU32 = AtomicU32::new(0);

const SYMBOL_HEADER: &str = "symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell\n";
const ACCOUNT_HEADER: &str = "uid,currency,amount\n";

fn fixture_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exsim_l2_{}_{}_{}",
        std::process::id(),
        name,
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn test_conf(dir: &std::path::Path) -> ExchangeConf {
    ExchangeConf {
        enabled: true,
        server_port: 0,
        gateway_port: None,
        gateway_protocol: Some("sse".into()),
        matcher: MatcherConf {
            symbol_info_path: dir.join("symbols.csv").to_string_lossy().into_owned(),
            account_info_path: dir.join("accounts.csv").to_string_lossy().into_owned(),
            core: CoreConf::default(),
        },
    }
}

fn write_fixtures(dir: &std::path::Path) {
    fs::write(
        dir.join("symbols.csv"),
        format!("{SYMBOL_HEADER}600000,0,840,840,100,1,3,2,0,0\n"),
    )
    .unwrap();
    fs::write(
        dir.join("accounts.csv"),
        format!("{ACCOUNT_HEADER}10001,840,10000000\n10002,840,10000000\n10009,840,200000\n"),
    )
    .unwrap();
}

fn place_cmd(order_id: u64, uid: u64, action: OrderAction, price: i64, size: i64) -> OrderCommand {
    OrderCommand {
        command: OrderCommandType::PlaceOrder,
        uid,
        order_id,
        symbol: 600000,
        price,
        reserve_price: price,
        size,
        action,
        order_type: matching_core::OrderType::Gtc,
        ..Default::default()
    }
}

/// 在 LocalSet 上引导一个真实撮合核心，供各用例使用
async fn bootstrap(name: &str) -> (Arc<CoreBridge>, JoinHandle<()>, JoinHandle<()>) {
    let dir = fixture_dir(name);
    write_fixtures(&dir);
    core_bridge::bootstrap_exchange(name, &test_conf(&dir))
        .await
        .unwrap()
}

fn run_local<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(tokio::task::LocalSet::new().run_until(fut))
}

/// 用例 1：无对手盘的挂单 —— Success、leaves=size、余额扣除冻结额
#[test]
fn case1_resting_order_freezes_balance() {
    run_local(async {
        let (bridge, core_task, result_task) = bootstrap("case1").await;
        let report = bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 1000, 100))
            .await
            .unwrap();

        assert_eq!(report.result_code, CommandResultCode::Success);
        assert!(report.accepted);
        assert!(report.trades.is_empty());
        assert_eq!(report.leaves_qty, 100);

        // 冻结 100×1000×1 + 100×3 = 100,300
        assert_eq!(bridge.balances_of(10001).get(&840), Some(&9_899_700));

        let record = bridge.order_record(1).unwrap();
        assert_eq!(record.status, OrderStatus::New);
        assert_eq!(record.filled_qty, 0);

        core_task.abort();
        result_task.abort();
    });
}

/// 用例 2：成交结算 —— taker 收款扣费、maker 退差价收 base、订单状态流转
#[test]
fn case2_crossing_order_settles_both_sides() {
    run_local(async {
        let (bridge, core_task, result_task) = bootstrap("case2").await;
        bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 1000, 100))
            .await
            .unwrap();
        let taker = bridge
            .execute(place_cmd(2, 10002, OrderAction::Ask, 900, 100))
            .await
            .unwrap();

        // 以 maker 价 1000 成交
        assert_eq!(taker.result_code, CommandResultCode::Success);
        assert_eq!(taker.trades.len(), 1);
        assert_eq!(taker.trades[0].price, 1000);
        assert_eq!(taker.trades[0].size, 100);
        assert_eq!(taker.trades[0].counterparty_order_id, 1);
        assert_eq!(taker.leaves_qty, 0);

        // taker 10002（卖）：冻结 100×100=10,000，收入 100×1000−100×3=99,700 → 10,089,700
        assert_eq!(bridge.balances_of(10002).get(&840), Some(&10_089_700));
        // maker 10001（买）：退差价 0，收入 base 100×100=10,000 → 9,899,700+10,000
        assert_eq!(bridge.balances_of(10001).get(&840), Some(&9_909_700));

        let record = bridge.order_record(1).unwrap();
        assert_eq!(record.status, OrderStatus::Filled);
        assert_eq!(record.filled_qty, 100);
        assert_eq!(record.leaves_qty, 0);

        core_task.abort();
        result_task.abort();
    });
}

/// 用例 3：资金不足 —— RiskNsf、余额不变
#[test]
fn case3_insufficient_balance_rejected() {
    run_local(async {
        let (bridge, core_task, result_task) = bootstrap("case3").await;
        let report = bridge
            .execute(place_cmd(1, 10009, OrderAction::Bid, 1000, 1000))
            .await
            .unwrap();

        assert_eq!(report.result_code, CommandResultCode::RiskNsf);
        assert!(!report.accepted);
        assert_eq!(report.trades.len(), 0);
        assert_eq!(bridge.balances_of(10009).get(&840), Some(&200_000));

        let record = bridge.order_record(1).unwrap();
        assert_eq!(record.status, OrderStatus::Rejected);

        core_task.abort();
        result_task.abort();
    });
}

/// 用例 4：order_id 幂等 —— 终态后同号订单被拒绝
#[test]
fn case4_duplicate_order_id_rejected() {
    run_local(async {
        let (bridge, core_task, result_task) = bootstrap("case4").await;
        bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 1000, 100))
            .await
            .unwrap();
        let second = bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 1000, 100))
            .await;
        assert!(matches!(second, Err(SimError::DuplicateOrderId)));

        core_task.abort();
        result_task.abort();
    });
}

/// 用例 5：核心不可用 —— 命令通道关闭时返回 503 语义错误
#[test]
fn case5_core_unavailable() {
    run_local(async {
        let (bridge, command_rx, _result_rx) = CoreBridge::new();
        drop(command_rx);
        let result = bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 1000, 100))
            .await;
        assert!(matches!(result, Err(SimError::CoreUnavailable)));
    });
}

/// 用例 6：撤单解冻 —— 挂单后撤单，冻结资金应全额退回
#[test]
fn case6_cancel_restores_frozen_balance() {
    run_local(async {
        let (bridge, core_task, result_task) = bootstrap("case6").await;
        bridge
            .execute(place_cmd(1, 10001, OrderAction::Bid, 500, 100))
            .await
            .unwrap();
        // 冻结 100×500×1 + 100×3 = 50,300（fixture 起始 10,000,000）
        assert_eq!(bridge.balances_of(10001).get(&840), Some(&9_949_700));

        let cancel = OrderCommand {
            command: OrderCommandType::CancelOrder,
            uid: 10001,
            order_id: 1,
            symbol: 600000,
            ..Default::default()
        };
        bridge.submit(cancel).await.unwrap();
        // 等待撤单结果经广播结算（最多 2s）
        let mut restored = false;
        for _ in 0..200 {
            if bridge.balances_of(10001).get(&840) == Some(&10_000_000) {
                restored = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            restored,
            "撤单后冻结资金应全额解冻，实际 {:?}",
            bridge.balances_of(10001)
        );

        core_task.abort();
        result_task.abort();
    });
}
