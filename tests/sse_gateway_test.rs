//! SSE 协议网关端到端测试：以 TCP 客户端模拟 gt-auto 的场景流程
//! （对应 docs/DEVELOPMENT_DESIGN.md §8.4 的 sse_001~004）。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use binary_codec::BinaryCodec;
use bytes::BytesMut;
use exchange_simulator_rs::config::{CoreConf, ExchangeConf, MatcherConf};
use exchange_simulator_rs::core_bridge::{self, CoreBridge};
use exchange_simulator_rs::sse_gateway::codec::{decode_frame, msg_type_of};
use exchange_simulator_rs::sse_gateway::spawn_listener;
use sse_binary::heartbeat::Heartbeat;
use sse_binary::logon::Logon;
use sse_binary::new_order_single::NewOrderSingle;
use sse_binary::order_cancel::OrderCancel;
use sse_binary::sse_binary::{SseBinary, SseBinaryBodyEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

static SEQ: AtomicU32 = AtomicU32::new(0);

const SYMBOL_HEADER: &str = "symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell\n";
const ACCOUNT_HEADER: &str = "uid,currency,amount\n";

fn fixture_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "exsim_sse_{}_{}_{}",
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
        format!("{SYMBOL_HEADER}600000,0,840,840,100,1,3,2,0,0\n"),
    )
    .unwrap();
    std::fs::write(
        dir.join("accounts.csv"),
        format!("{ACCOUNT_HEADER}10001,840,1000000\n10002,840,500000\n"),
    )
    .unwrap();

    let conf = ExchangeConf {
        enabled: true,
        server_port: 0,
        gateway_port: None, // 测试内手动启动网关（任意端口）
        matcher: MatcherConf {
            symbol_info_path: dir.join("symbols.csv").to_string_lossy().into_owned(),
            account_info_path: dir.join("accounts.csv").to_string_lossy().into_owned(),
            core: CoreConf::default(),
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

/// 模拟 gt-auto 的最小协议客户端
struct TestClient {
    stream: TcpStream,
    buf: BytesMut,
    out_seq: u64,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self {
            stream,
            buf: BytesMut::new(),
            out_seq: 0,
        }
    }

    async fn send(&mut self, body: SseBinaryBodyEnum) {
        self.out_seq += 1;
        let msg = SseBinary {
            msg_type: msg_type_of(&body),
            msg_seq_num: self.out_seq,
            msg_body_len: 0,
            body,
            checksum: 0,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);
        self.stream.write_all(&buf).await.unwrap();
    }

    /// 接收下一条指定类型的报文（跳过其他类型），5s 超时视为失败
    async fn recv(&mut self, want_type: u32) -> SseBinary {
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

fn logon_body() -> SseBinaryBodyEnum {
    SseBinaryBodyEnum::Logon(Logon {
        sender_comp_id: "gt001".into(),
        target_comp_id: "sim001".into(),
        heart_bt_int: 10,
        prtcl_version: "v1".into(),
        trade_date: 20260501,
        q_size: 2,
    })
}

#[allow(clippy::too_many_arguments)]
fn nos_body(
    cl_ord_id: &str,
    account: &str,
    side: &str,
    price: i64,
    qty: i64,
    tif: &str,
) -> SseBinaryBodyEnum {
    SseBinaryBodyEnum::NewOrderSingle(NewOrderSingle {
        biz_id: 10,
        biz_pbu: "pbu001".into(),
        cl_ord_id: cl_ord_id.into(),
        security_id: "600000".into(),
        account: account.into(),
        owner_type: 1,
        side: side.into(),
        price,
        order_qty: qty,
        ord_type: "1".into(),
        time_in_force: tif.into(),
        transact_time: 20250106101530,
        credit_tag: "1".into(),
        clearing_firm: "1001".into(),
        branch_id: "b001".into(),
        user_info: "u000123".into(),
    })
}

fn cancel_body(cl_ord_id: &str, orig_cl_ord_id: &str) -> SseBinaryBodyEnum {
    SseBinaryBodyEnum::OrderCancel(OrderCancel {
        biz_id: 10,
        biz_pbu: "pbu001".into(),
        cl_ord_id: cl_ord_id.into(),
        security_id: "600000".into(),
        account: "10001".into(),
        owner_type: 1,
        side: "1".into(),
        orig_cl_ord_id: orig_cl_ord_id.into(),
        transact_time: 20250106102000,
        branch_id: "b001".into(),
        user_info: "u000123".into(),
    })
}

/// 场景 sse_001：Logon → 两笔新订单 → 双方 Confirm + 成交 Report
#[test]
fn sse_001_logon_order_confirm_report() {
    run_local(async {
        let (bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("sse001").await;
        let mut a = TestClient::connect(addr).await; // c10001 买方会话
        let mut b = TestClient::connect(addr).await; // c10002 卖方会话

        // 登录应答：sender/target 对调
        a.send(logon_body()).await;
        let reply = a.recv(40).await;
        let SseBinaryBodyEnum::Logon(logon) = &reply.body else {
            panic!("期望 Logon 应答")
        };
        assert_eq!(logon.sender_comp_id, "sim001");
        assert_eq!(logon.target_comp_id, "gt001");

        b.send(logon_body()).await;
        b.recv(40).await;

        // 买 100@1000（挂单）→ Confirm
        a.send(nos_body("c10001", "10001", "1", 1000, 100, "0"))
            .await;
        let confirm = a.recv(32).await;
        let SseBinaryBodyEnum::Confirm(c) = &confirm.body else {
            panic!("期望 Confirm")
        };
        assert_eq!(c.cl_ord_id, "c10001");
        assert_eq!(c.exec_type, "0");
        assert_eq!(c.ord_status, "0");
        assert_eq!(c.leaves_qty, 100);
        assert_eq!(c.side, "1");

        // 卖 100@900（IOC 语义下立即成交；这里用 GTC 限价同样全成）
        b.send(nos_body("c10002", "10002", "2", 900, 100, "0"))
            .await;
        let confirm_b = b.recv(32).await;
        let SseBinaryBodyEnum::Confirm(cb) = &confirm_b.body else {
            panic!("期望 Confirm")
        };
        assert_eq!(cb.cl_ord_id, "c10002");
        assert_eq!(cb.leaves_qty, 100);

        // 卖方成交回报
        let report_b = b.recv(103).await;
        let SseBinaryBodyEnum::Report(rb) = &report_b.body else {
            panic!("期望 Report")
        };
        assert_eq!(rb.cl_ord_id, "c10002");
        assert_eq!(rb.exec_type, "F");
        assert_eq!(rb.last_px, 1000);
        assert_eq!(rb.last_qty, 100);
        assert_eq!(rb.leaves_qty, 0);
        assert_eq!(rb.gross_trade_amt, 0); // 模板约定：成交额字段为 0
        assert_eq!(rb.side, "2");
        assert_eq!(rb.report_index, 0); // 按订单从 0 计数

        // 买方成交回报（maker 侧）
        let report_a = a.recv(103).await;
        let SseBinaryBodyEnum::Report(ra) = &report_a.body else {
            panic!("期望 Report")
        };
        assert_eq!(ra.cl_ord_id, "c10001");
        assert_eq!(ra.exec_type, "F");
        assert_eq!(ra.last_px, 1000);
        assert_eq!(ra.last_qty, 100);
        assert_eq!(ra.leaves_qty, 0);
        assert_eq!(ra.side, "1");

        // 余额断言（与撮合结算算术一致）
        assert_eq!(bridge.balances_of(10001).get(&840), Some(&909_700));
        assert_eq!(bridge.balances_of(10002).get(&840), Some(&589_700));

        core_task.abort();
        result_task.abort();
    });
}

/// 场景 sse_002：下单 → 撤单成功 → Confirm(exec_type=4)；撤不存在单 → CancelReject
#[test]
fn sse_002_order_cancel_and_reject() {
    run_local(async {
        let (_bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("sse002").await;
        let mut c = TestClient::connect(addr).await;
        c.send(logon_body()).await;
        c.recv(40).await;

        // 挂单（价格远离盘口，不会成交）
        c.send(nos_body("c10003", "10001", "1", 500, 100, "0"))
            .await;
        c.recv(32).await;

        // 撤单成功 → Confirm(exec_type=4)
        c.send(cancel_body("cx0001", "c10003")).await;
        let confirm = c.recv(32).await;
        let SseBinaryBodyEnum::Confirm(cf) = &confirm.body else {
            panic!("期望撤单 Confirm")
        };
        assert_eq!(cf.exec_type, "4");
        assert_eq!(cf.ord_status, "4");
        assert_eq!(cf.leaves_qty, 0);
        assert_eq!(cf.cxl_qty, 100);

        // 撤销不存在的订单 → CancelReject(59)
        c.send(cancel_body("cx0002", "c99999")).await;
        let reject = c.recv(59).await;
        let SseBinaryBodyEnum::CancelReject(cr) = &reject.body else {
            panic!("期望 CancelReject")
        };
        assert_eq!(cr.cxl_rej_reason, 1005);
        assert_eq!(cr.orig_cl_ord_id, "c99999");

        core_task.abort();
        result_task.abort();
    });
}

/// 场景 sse_003：账户不存在 → OrderReject(204, reason=1001)
#[test]
fn sse_003_unknown_account_rejected() {
    run_local(async {
        let (_bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("sse003").await;
        let mut c = TestClient::connect(addr).await;
        c.send(logon_body()).await;
        c.recv(40).await;

        c.send(nos_body("c20001", "99999", "1", 1000, 100, "0"))
            .await;
        let msg = c.recv(204).await;
        let SseBinaryBodyEnum::OrderReject(or) = &msg.body else {
            panic!("期望 OrderReject")
        };
        assert_eq!(or.cl_ord_id, "c20001");
        assert_eq!(or.ord_rej_reason, 1001);

        core_task.abort();
        result_task.abort();
    });
}

/// 场景 sse_004：心跳应答 + 重复订单号拒绝(1006)
#[test]
fn sse_004_heartbeat_and_duplicate_order() {
    run_local(async {
        let (_bridge, addr, _shutdown, core_task, result_task) =
            bootstrap_with_gateway("sse004").await;
        let mut c = TestClient::connect(addr).await;
        c.send(logon_body()).await;
        c.recv(40).await;

        // 心跳应答
        c.send(SseBinaryBodyEnum::Heartbeat(Heartbeat {})).await;
        c.recv(33).await;

        // 重复订单号：第一次受理，第二次 OrderReject(1006)
        c.send(nos_body("c30001", "10001", "1", 500, 100, "0"))
            .await;
        c.recv(32).await;
        c.send(nos_body("c30001", "10001", "1", 500, 100, "0"))
            .await;
        let msg = c.recv(204).await;
        let SseBinaryBodyEnum::OrderReject(or) = &msg.body else {
            panic!("期望 OrderReject")
        };
        assert_eq!(or.ord_rej_reason, 1006);

        core_task.abort();
        result_task.abort();
    });
}
