use std::net::SocketAddr;
use std::time::Duration;

use exchange_simulator_rs::{config, core_bridge, handle, logging};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::LocalSet;
use tracing::{error, info};

fn main() {
    let config_path = config::parse_args();
    let cfg = config::load_config(&config_path).unwrap_or_else(|e| {
        eprintln!("加载配置失败 ({config_path}): {e:#}");
        std::process::exit(1);
    });
    logging::init(cfg.logging.as_ref());
    info!(config = %config_path, "配置加载完成");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("构建 tokio 运行时失败");
    rt.block_on(async {
        LocalSet::new().run_until(run(cfg)).await;
    });
}

async fn run(cfg: config::Config) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut bridges = Vec::new();
    let mut core_tasks = Vec::new();
    let mut result_tasks = Vec::new();
    let mut server_tasks = Vec::new();
    let mut gateway_tasks = Vec::new();

    for (name, conf) in [("sse", &cfg.sse), ("szse", &cfg.szse)] {
        if !conf.enabled {
            info!(exchange = name, "未启用，跳过");
            continue;
        }
        let (bridge, core_task, result_task) = core_bridge::bootstrap_exchange(name, conf)
            .await
            .unwrap_or_else(|e| {
                error!(exchange = name, error = %e, "交易所引导失败");
                std::process::exit(1);
            });

        let app = handle::router(bridge.clone());
        let addr: SocketAddr = format!("127.0.0.1:{}", conf.server_port)
            .parse()
            .unwrap_or_else(|e| panic!("监听地址无效 ({}): {e}", conf.server_port));
        let listener = TcpListener::bind(addr)
            .await
            .unwrap_or_else(|e| panic!("绑定 {addr} 失败: {e}"));
        info!(exchange = name, %addr, "HTTP 服务已启动");

        let mut server_shutdown = shutdown_rx.clone();
        server_tasks.push(tokio::task::spawn_local(async move {
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = server_shutdown.changed().await;
                })
                .await
            {
                error!("HTTP 服务异常退出: {e}");
            }
        }));

        // SSE 二进制协议网关（阶段二）
        if let Some(gateway_port) = conf.gateway_port {
            let gateway_shutdown = shutdown_rx.clone();
            gateway_tasks.push(exchange_simulator_rs::sse_gateway::spawn(
                gateway_port,
                bridge.clone(),
                gateway_shutdown,
            ));
        }

        bridges.push(bridge);
        core_tasks.push(core_task);
        result_tasks.push(result_task);
    }

    info!("就绪，等待订单（Ctrl-C 停止）");
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!("监听 Ctrl-C 失败: {e}");
    }
    info!("收到停止信号，开始优雅停机");

    // 1. 停止接受新请求/新会话并等待接入层任务退出
    let _ = shutdown_tx.send(true);
    for task in server_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    for task in gateway_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    // 2. 关闭命令通道，撮合核心排空存量命令后退出
    bridges.clear();
    for task in core_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    // 3. 结果路由任务随结果通道关闭而退出
    for task in result_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    info!("已退出");
}
