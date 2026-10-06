//! SZSE 协议网关端到端测试：以 TCP 客户端模拟 gt-auto 的 binary-szse 场景流程
//! （登录回执 / 新订单确认 / 双边成交流水 / 撤单 / 业务拒绝）。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use binary_codec::BinaryCodec;
use bytes::BytesMut;
use exchange_simulator_rs::config::{CoreConf, ExchangeConf, MatcherConf, PersistenceConf};
use exchange_simulator_rs::core_bridge::{self, CoreBridge};
use exchange_simulator_rs::szse_gateway::codec::{decode_frame, msg_type_of};
use exchange_simulator_rs::szse_gateway::spawn_listener;
use szse_binary::execution_confirm::ExecutionConfirm;
use szse_binary::execution_report::ExecutionReport;
use szse_binary::heartbeat::Heartbeat;
use szse_binary::logon::Logon;
use szse_binary::new_order::NewOrder;
use szse_binary::new_order::NewOrderApplExtendEnum;
use szse_binary::order_cancel_request::OrderCancelRequest;
use szse_binary::szse_binary::{SzseBinary, SzseBinaryBodyEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

static SEQ: AtomicU32 = AtomicU32::new(0);

const SYMBOL_HEADER: &str = "symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell\n";
const ACCOUNT_HEADER: &str = "uid,currency,amount\n";

fn fixture_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exsim_szse_{}_{}_{}",
        std::process::id(),
        name,
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn bootstrap_with_gateway(
    name: &str,
) -> (
    Arc<CoreBridge>,
    SocketAddr,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let dir = fixture_dir(name);
    std::fs::write(
        dir.join("symbols.csv"),
        format!("{SYMBOL_HEADER}000001,0,840,840,100,1,3,2,0,0\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join("accounts.csv"),
        format!("{ACCOUNT_HEADER}20001,840,1000000\n20002,840,500000\n"),
    )
    .unwrap();

    let conf = ExchangeConf {
        enabled: true,
        server_port: 0,
        gateway_port: None,
        gateway_protocol: Some("szse".into()),
        matcher: MatcherConf {
            symbol_info_path: dir.join("symbols.csv").to_string_lossy().into_owned(),
            account_info_path: dir.join("accounts.csv").to_string_lossy().into_owned(),
            core: CoreConf::default(),
            persistence: PersistenceConf::default(),
        },
    };
    let (bridge, core_task, result_task) =
        core_bridge::bootstrap_exchange(name, &conf).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_listener(listener, bridge.clone(), shutdown_rx);
    (bridge, addr, shutdown_tx, core_task, result_task)
}

fn run_local<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(tokio::task::LocalSet::new().run_until(fut))
}

/// 模拟 gt-auto 的最小 SZSE 协议客户端
struct TestClient {
    stream: TcpStream,
    buf: BytesMut,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    async fn send(&mut self, body: SzseBinaryBodyEnum) {
        let msg = SzseBinary {
            msg_type: msg_type_of(&body),
            body_length: 0,
            body,
            checksum: 0,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);
        self.stream.write_all(&buf).await.unwrap();
    }

    /// 接收下一条指定类型的报文（跳过其他类型），5s 超时视为失败
    async fn recv(&mut self, want_type: u32) -> SzseBinary {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match decode_frame(&mut self.buf) {
                Ok(Some(msg)) => {
                    if msg.msg_type == want_type {
                        return msg;
                    }
                }
                Ok(None) => {}
                Err(e) => panic!("帧解码失败: {e:?}"),
            }
            let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
            let n = tokio::time::timeout(remain, self.stream.read_buf(&mut self.buf))
                .await
                .expect("等待回报超时")
                .unwrap();
            assert!(n > 0, "连接被对端关闭");
        }
    }
}

fn logon_body() -> SzseBinaryBodyEnum {
    SzseBinaryBodyEnum::Logon(Logon {
        sender_comp_id: "szoms01".into(),
        target_comp_id: "szsim01".into(),
        heart_btint: 10,
        password: String::new(),
        default_appl_ver_id: String::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn nos_body(
    cl_ord_id: &str,
    account_id: &str,
    side: &str,
    price: i64,
    qty: i64,
) -> SzseBinaryBodyEnum {
    SzseBinaryBodyEnum::NewOrder(NewOrder {
        appl_id: "010".into(),
        submitting_pbuid: "b0001".into(),
        security_id: "000001".into(),
        security_id_source: "102".into(),
        owner_type: 1,
        clearing_firm: "1".into(),
        transact_time: 20250101120000,
        user_info: "u0001".into(),
        cl_ord_id: cl_ord_id.into(),
        account_id: account_id.into(),
        branch_id: "b01".into(),
        order_restrictions: "o01".into(),
        side: side.into(),
        ord_type: "1".into(),
        order_qty: qty,
        price,
        appl_extend: NewOrderApplExtendEnum::Extend100101(
            szse_binary::extend_100101::Extend100101 {
                stop_px: 0,
                min_qty: 0,
                max_price_levels: 0,
                time_in_force: String::new(),
                cash_margin: String::new(),
            },
        ),
    })
}

fn cancel_body(cl_ord_id: &str, orig_cl_ord_id: &str) -> SzseBinaryBodyEnum {
    SzseBinaryBodyEnum::OrderCancelRequest(OrderCancelRequest {
        appl_id: "010".into(),
        submitting_pbuid: "b0001".into(),
        security_id: "000001".into(),
        security_id_source: "102".into(),
        owner_type: 1,
        clearing_firm: "1".into(),
        transact_time: 20250101130000,
        user_info: "u0001".into(),
        cl_ord_id: cl_ord_id.into(),
        orig_cl_ord_id: orig_cl_ord_id.into(),
        side: "1".into(),
        order_id: String::new(),
        order_qty: 0,
    })
}

fn confirm_of(msg: &SzseBinary) -> &ExecutionConfirm {
    let SzseBinaryBodyEnum::ExecutionConfirm(c) = &msg.body else {
        panic!("期望 ExecutionConfirm")
    };
    c
}

fn report_of(msg: &SzseBinary) -> &ExecutionReport {
    let SzseBinaryBodyEnum::ExecutionReport(r) = &msg.body else {
        panic!("期望 ExecutionReport")
    };
    r
}

/// SZSE 场景 1：Logon 应答 → 新订单受理确认 → 两边成交流水
#[test]
fn szse_001_logon_order_confirm_trade() {
    run_local(async {
        let (bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("szse001").await;
        let mut a = TestClient::connect(addr).await; // c0001 买方
        let mut b = TestClient::connect(addr).await; // c0002 卖方

        // 登录应答：sender/target 对调
        a.send(logon_body()).await;
        let reply = a.recv(1).await;
        let SzseBinaryBodyEnum::Logon(logon) = &reply.body else {
            panic!("期望 Logon 应答")
        };
        assert_eq!(logon.sender_comp_id, "szsim01");
        assert_eq!(logon.target_comp_id, "szoms01");

        b.send(logon_body()).await;
        b.recv(1).await;

        // 买 1000@100（挂单）→ ExecutionConfirm
        a.send(nos_body("c0001", "20001", "1", 100, 1000)).await;
        let confirm = a.recv(200102).await;
        let c = confirm_of(&confirm);
        assert_eq!(c.cl_ord_id, "c0001");
        assert_eq!(c.exec_type, "0");
        assert_eq!(c.ord_status, "0");
        assert_eq!(c.leaves_qty, 1000);
        assert_eq!(c.cum_qty, 0);
        assert_eq!(c.report_index, 0);
        assert_eq!(c.side, "1");

        // 卖 1000@90（全成）→ 自己的 Confirm + 成交流水；买方成交流水
        b.send(nos_body("c0002", "20002", "2", 90, 1000)).await;
        let confirm_b = b.recv(200102).await;
        assert_eq!(confirm_of(&confirm_b).cl_ord_id, "c0002");

        let rpt_b = b.recv(200115).await;
        let r = report_of(&rpt_b);
        assert_eq!(r.cl_ord_id, "c0002");
        assert_eq!(r.exec_type, "F");
        assert_eq!(r.last_px, 100);
        assert_eq!(r.last_qty, 1000);
        assert_eq!(r.leaves_qty, 0);
        assert_eq!(r.cum_qty, 1000);
        assert_eq!(r.side, "2");

        let rpt_a = a.recv(200115).await;
        let ra = report_of(&rpt_a);
        assert_eq!(ra.cl_ord_id, "c0001");
        assert_eq!(ra.exec_type, "F");
        assert_eq!(ra.last_px, 100);
        assert_eq!(ra.side, "1");

        // 余额断言（与撮合结算算术一致）：
        // 20001 maker 买：冻结 1000×100+1000×3=103,000，成交后退差价 0、收 base 1000×100=100,000
        assert_eq!(bridge.balances_of(20001).get(&840), Some(&997_000));
        // 20002 taker 卖：冻结 1000×100=100,000，收入 1000×100−1000×3=97,000
        assert_eq!(bridge.balances_of(20002).get(&840), Some(&497_000));

        core_task.abort();
        result_task.abort();
    });
}

/// SZSE 场景 2：撤单成功 → ExecutionConfirm(exec_type=4)；撤不存在单 → CancelReject
#[test]
fn szse_002_cancel_and_reject() {
    run_local(async {
        let (_bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("szse002").await;
        let mut c = TestClient::connect(addr).await;

        // 宽松登录策略：不登录也能直接下单（gt-auto SZSE 参考场景无 Logon 步骤）
        c.send(nos_body("c0003", "20001", "1", 50, 1000)).await;
        c.recv(200102).await;

        // 撤单成功 → ExecutionConfirm(exec_type=4)
        c.send(cancel_body("cx0001", "c0003")).await;
        let confirm = c.recv(200102).await;
        let cf = confirm_of(&confirm);
        assert_eq!(cf.exec_type, "4");
        assert_eq!(cf.ord_status, "4");
        assert_eq!(cf.leaves_qty, 0);
        assert_eq!(cf.cum_qty, 0);
        assert_eq!(cf.orig_cl_ord_id, "c0003");

        // 撤销不存在的订单 → CancelReject(290008)
        c.send(cancel_body("cx0002", "c99999")).await;
        let msg = c.recv(290008).await;
        let SzseBinaryBodyEnum::CancelReject(cr) = &msg.body else {
            panic!("期望 CancelReject")
        };
        assert_eq!(cr.cxl_rej_reason, 1005);
        assert_eq!(cr.orig_cl_ord_id, "c99999");

        core_task.abort();
        result_task.abort();
    });
}

/// SZSE 场景 3：账户不存在 → BusinessReject(reason=1001)；心跳应答
#[test]
fn szse_003_business_reject_and_heartbeat() {
    run_local(async {
        let (_bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("szse003").await;
        let mut c = TestClient::connect(addr).await;

        c.send(SzseBinaryBodyEnum::Heartbeat(Heartbeat {})).await;
        c.recv(3).await;

        c.send(nos_body("c50001", "99999", "1", 100, 1000)).await;
        let msg = c.recv(4).await;
        let SzseBinaryBodyEnum::BusinessReject(br) = &msg.body else {
            panic!("期望 BusinessReject")
        };
        assert_eq!(br.business_reject_reason, 1001);
        assert_eq!(br.business_reject_ref_id, "c50001");

        core_task.abort();
        result_task.abort();
    });
}
