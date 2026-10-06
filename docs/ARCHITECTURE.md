# exchange-simulator-rs 架构与开发设计文档

> 版本：v0.1（对应代码版本 v0.1.0 / main@a60b32b）
> 更新日期：2026-10-06
> 适用读者：本项目开发与维护者、接入本模拟器做联调测试的 OMS/网关开发人员
> 配套文档：[DEVELOPMENT_DESIGN.md](./DEVELOPMENT_DESIGN.md)（模块级详细开发设计：数据字典、接口规格、错误码、测试设计与任务拆解）

---

## 目录

1. [项目定位与目标](#1-项目定位与目标)
2. [系统上下文](#2-系统上下文)
3. [技术选型与依赖](#3-技术选型与依赖)
4. [总体架构](#4-总体架构)
5. [运行时与线程模型](#5-运行时与线程模型)
6. [核心概念：命令与流水线](#6-核心概念命令与流水线)
7. [模块设计说明（本仓库代码）](#7-模块设计说明本仓库代码)
8. [基础数据与数据模型](#8-基础数据与数据模型)
9. [对外接口](#9-对外接口)
10. [关键流程](#10-关键流程)
11. [配置管理](#11-配置管理)
12. [测试体系](#12-测试体系)
13. [代码走查发现的问题](#13-代码走查发现的问题)
14. [演进路线图](#14-演进路线图)

---

## 1. 项目定位与目标

**exchange-simulator-rs 是一个交易所模拟器（Simulator/Matcher）**：在本地或测试环境中模拟真实交易所的核心行为——账户资金管理、订单接收、风控校验、订单簿撮合、成交结算，供上游系统（OMS、交易网关、自动化测试工具）进行集成测试与验收测试。

设计目标：

| 目标 | 说明 |
|------|------|
| **行为仿真** | 复现真实交易所的下单/成交/回报链路语义（含资金冻结、手续费、部分成交、撤单等） |
| **高性能** | 撮合核心基于 Disruptor 环形缓冲区 + 单线程流水线，全整数运算、零锁竞争 |
| **可测试** | 支持脚本化测试用例驱动（`testcase/` 目录 + `gt-auto` 测试工具），支持批量标的与账户初始化 |
| **协议可扩展** | 当前提供 HTTP 简单接口；预留上交所（SSE）/深交所（SZSE）二进制协议网关接入能力 |

**非目标（当前阶段）**：不做行情发布、不做真实清算交收、不做多节点分布式部署、不面向生产交易。

---

## 2. 系统上下文

```
                    ┌────────────────────────────┐
                    │        测试驱动方           │
                    │  (OMS / gt-auto 自动化工具) │
                    └──────────┬─────────────────┘
                               │ ①HTTP POST /place（当前已实现）
                               │ ②SSE/SZSE 二进制协议（规划中）
                               ▼
        ┌──────────────────────────────────────────────┐
        │           exchange-simulator-rs              │
        │  ┌────────────┐   ┌───────────────────────┐  │
        │  │ HTTP 接入层 │──▶│  撮合核心 ExchangeCore │  │
        │  └────────────┘   │  (matching-core crate) │  │
        │  ┌────────────┐   └───────────────────────┘  │
        │  │ 基础数据加载 │     ▲ 启动时加载             │
        │  └─────┬──────┘                             │
        └────────┼────────────────────────────────────┘
                 ▼
        data/sse/*.csv  （标的规定 + 账户资金 CSV）
```

- **上游**：被测系统（OMS 或网关）。当前通过 HTTP 提交订单；规划通过 SSE（上交所 step 二进制类协议）接入。
- **测试工具**：`gt-auto`（外部工具，见 `autotest.sh`）按 `testcase/sse/*.csv` 场景脚本驱动报文的发送与接收校验。
- **依赖仓库**（均为 GitHub tag 固定版本，构建时需可访问）：
  - `matching-core`（撮合核心，v0.2.2）
  - `sse-binary` / `binary-codec`（SSE 二进制协议编解码，v0.1.0，**尚未接线使用**）

---

## 3. 技术选型与依赖

| 依赖 | 版本 | 用途 | 状态 |
|------|------|------|------|
| `matching-core` | git tag v0.2.2 | 撮合核心：风控引擎、撮合引擎、订单簿、Disruptor 流水线、WAL/快照 | ✅ 核心依赖 |
| `axum` | 0.8.9 | HTTP 服务框架（`/place` 下单接口） | ✅ 使用中 |
| `tokio` | 1.52.3 | 异步运行时（current_thread + LocalSet） | ✅ 使用中 |
| `serde` / `toml` | 1.0 / 1.1 | JSON 请求解析 / TOML 配置解析 | ✅ 使用中 |
| `tracing` | 0.1.44 | 结构化日志 | ⚠️ 已引入但**未初始化 subscriber**（见问题 P0-2） |
| `sse-binary` | git tag v0.1.0 | SSE 二进制协议报文编解码（Logon/NewOrderSingle/Confirm/Report…） | 📦 预留未使用 |
| `binary-codec` | git tag v0.1.0 | sse-binary 的底层编解码运行时 | 📦 预留未使用 |
| `bincode` / `bigdecimal` / `validator` | — | — | 📦 预留未使用（可考虑移除） |

**语言版本**：Rust edition 2024（需 rustc 1.85+）。

> ⚠️ 构建前提：本机需能访问 `github.com` 拉取三个 git 依赖；若使用代理/SSH 改写，出现
> `failed to authenticate` 时可设置 `CARGO_NET_GIT_FETCH_WITH_CLI=true` 重试。

---

## 4. 总体架构

### 4.1 分层视图

```
┌───────────────────────────────────────────────────────────────────────┐
│                        exchange-simulator-rs                          │
│                                                                       │
│  ┌─────────────────────────┐      ┌────────────────────────────────┐  │
│  │  接入层 (src/handle.rs) │      │  基础数据层                     │  │
│  │  axum Router            │      │  (src/base_info_service.rs)    │  │
│  │  POST /place            │      │  load_symbol_info()            │  │
│  │  OrderRequest → 命令    │      │  load_account_info()           │  │
│  └───────────┬─────────────┘      └──────────────┬─────────────────┘  │
│              │                                    │ 启动引导           │
│              │ tokio::mpsc(1024)                  ▼                    │
│              ▼                            ┌──────────────────────┐    │
│  ┌─────────────────────────┐             │  ExchangeCore        │    │
│  │      命令通道            │────────────▶│  (matching-core)     │    │
│  └─────────────────────────┘  submit_    │                      │    │
│                               command    │ startup() 后由       │    │
│                                          │ Disruptor 消费线程   │    │
│                                          │ 顺序处理命令         │    │
│                                          └──────────┬───────────┘    │
│                                                     ▼                │
│                                          ┌──────────────────────┐    │
│                                          │ 结果消费者(回调)      │    │
│                                          │ 目前: println! 打印   │    │
│                                          └──────────────────────┘    │
└───────────────────────────────────────────────────────────────────────┘
```

### 4.2 撮合核心内部流水线（matching-core）

每条 `OrderCommand` 按以下顺序经过流水线（`Pipeline::handle_event`）：

```
RingBuffer(64K, 单生产者, BusySpin)
        │
        ▼
┌──────────────────┐   ┌──────────────────────┐   ┌──────────────────┐   ┌───────────────┐
│ RiskEngine R1    │──▶│ MatchingEngineRouter │──▶│ RiskEngine R2    │──▶│ ResultConsumer│
│ (预处理/资金冻结) │   │ (路由到订单簿撮合)    │   │ (后处理/成交结算) │   │ (结果回调)     │
└──────────────────┘   └──────────────────────┘   └──────────────────┘   └───────────────┘
```

1. **R1 预处理**
   - `AddUser`：注册用户档案；重复注册返回 `UserMgmtUserAlreadyExists`。
   - `BalanceAdjustment`：按 `currency`/`amount` 入金（`tid` 幂等标识）。
   - `PlaceOrder`：**资金冻结**
     - 买单：冻结 `size × reserve_price × quote_scale_k + size × taker_fee`（预算单 FokBudget/IocBudget 用 `price` 代替 `reserve_price`），扣 `quote_currency`；
     - 卖单：冻结 `size × base_scale_k`，扣 `base_currency`；
     - 余额不足 → `result_code = RiskNsf`（订单不会进入撮合）；
     - 校验通过 → `result_code = ValidForMatchingEngine`。
2. **撮合引擎**：按 `symbol` 分片路由到对应订单簿。
   - 仅处理 `result_code == ValidForMatchingEngine` 的 `PlaceOrder`（`Success` 的命令直接跳过）；
   - 未注册标的 → `MatchingInvalidOrderBookId`；
   - 产生的成交/拒绝/减单事件写入 `cmd.matcher_events`（`MatcherTradeEvent`）。
3. **R2 后处理（结算）**：遍历 `matcher_events`
   - 成交（Trade）：taker/maker 双边记账——卖方收 `quote`（扣手续费），买方收 `base` 并退差价（`bidder_hold_price − price`）；
   - 拒绝/减单（Reject/Reduce）：全额解冻对应冻结金额。
   - 最终置 `result_code = Success`。
4. **结果消费**：回调 `Arc<dyn Fn(&OrderCommand)>`，由本仓库注册（当前仅打印）。

> **设计要点**：所有金额、价格、数量均为 `i64` 整数，通过 `base_scale_k`/`quote_scale_k`（数量级乘数，如 A 股一手 = 100 股 → `base_scale_k = 100`）换算，避免浮点误差；风险引擎与撮合引擎均支持按 `uid`/`symbol` 位掩码分片（当前配置均为 1 片）。

**订单簿实现**：`add_symbol` 默认创建 `DirectOrderBook`（另有 Naive/DirectOptimized/Advanced 四种实现可切换，见 matching-core `orderbook` 模块），支持价格/时间优先撮合与 L2 行情查询。

---

## 5. 运行时与线程模型

`src/main.rs` 的并发模型是理解本项目的关键：

```
OS 主线程
├── tokio current_thread runtime
│    └── LocalSet::run_until(async { ... })
│         ├── ExchangeCore 引导（同步、阻塞执行，见 §10.1）
│         ├── spawn_local(core_task)      ← 撮合核心宿主任务（!Send，不能跨线程）
│         │     ├── core.startup()        ← 创建 Disruptor 环形缓冲区
│         │     └── loop { rx.recv() → core.submit_command() }
│         └── axum::serve(...)            ← HTTP 服务（同一线程内协作调度）
│
└── Disruptor 消费线程（由 matching-core startup() 创建，BusySpin 自旋等待）
         └── Pipeline.handle_event(cmd)  ← 风控→撮合→结算→结果回调
```

设计动机与约束：

1. **为什么用 `LocalSet` + `spawn_local`**：`ExchangeCore`（含 Disruptor 生产者句柄与流水线）不是 `Send` 的，无法放入普通的 `tokio::spawn`；因此核心被放在 `LocalSet` 的本地任务中，与 axum 同处一个单线程运行时，通过 **`mpsc::channel::<OrderCommand>(1024)`** 把 axum handler（`Send` 世界）的命令搬到核心任务里。
2. **两条命令处理路径**（`ExchangeCore::submit_command` 内部切换）：
   - **启动前（producer 为空）**：直接同步调用 `pipeline.handle_event`，命令在调用线程立即处理完毕——用于启动引导阶段批量注册标的与账户；
   - **启动后**：命令发布进 Disruptor 环形缓冲区，由 Disruptor 专用消费线程异步处理。**这意味着引导必须在 `startup()` 之前完成**；同时 `startup()` 会把 `pipeline` move 走，**启动后不可再调用 `add_symbol`（会静默无效）**。
3. **背压**：mpsc 容量 1024，通道满时 `tx.send().await` 挂起，天然对上游限流。
4. **当前为单进程单交易所实例**：一个 `ExchangeCore` 承载全部标的；SSE/SZSE 双市场多实例是规划项（见 §14）。

---

## 6. 核心概念：命令与流水线

### 6.1 OrderCommand（命令模型）

matching-core 采用**命令即事件**的设计：同一条 `OrderCommand` 结构贯穿"请求 → 处理 → 结果"，`result_code` 字段在流水线各阶段被逐步覆写为最终结果。主要字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `command` | `OrderCommandType` | 命令类型（见下表） |
| `uid` / `order_id` / `symbol` | u64/u64/i32 | 用户 / 订单 / 标的标识 |
| `price` / `size` | i64 | 委托价 / 委托量 |
| `reserve_price` | i64 | 买单**风险冻结预留价**（可高于委托价，防穿仓；结算时退差价） |
| `action` | `OrderAction` | Bid（买）/ Ask（卖） |
| `order_type` | `OrderType` | Gtc / Ioc / Fok / FokBudget / IocBudget / PostOnly / StopLimit / StopMarket / Iceberg / Day / Gtd |
| `currency` / `amount` / `tid` | i32/i64/i64 | `BalanceAdjustment` 专用 |
| `matcher_events` | `Vec<MatcherTradeEvent>` | 撮合产出的事件（成交/拒绝/减单），R2 结算与下游回报的依据 |

支持的命令类型：`PlaceOrder / MoveOrder / CancelOrder / ReduceOrder / OrderBookRequest / AddUser / BalanceAdjustment / SuspendUser / ResumeUser / Reset / Persist* / ShutdownSignal` 等。

### 6.2 结果码（CommandResultCode）

`New → ValidForMatchingEngine → Success` 为正常路径；异常路径包括 `AuthInvalidUser`（用户未注册）、`RiskNsf`（资金不足）、`InvalidSymbol`、`MatchingInvalidOrderBookId`、`MatchingUnknownOrderId`、`MatchingInvalidOrderSize`、`UserMgmtUserAlreadyExists` 等。下游可通过 `result_code` 判定受理结果。

---

## 7. 模块设计说明（本仓库代码）

```
src/
├── main.rs              # 进程入口：运行时装配、核心引导、HTTP 服务启动
├── handle.rs            # axum 请求处理器（/place）
├── model.rs             # 对外 DTO（OrderRequest）
├── base_info_service.rs # CSV 基础数据加载（标的规格、账户资金）
└── config.rs            # TOML 配置结构（预留，main 尚未使用）
```

### 7.1 `main.rs` — 装配与生命周期

职责（按执行顺序）：

1. 构建 `current_thread` tokio runtime 并进入 `LocalSet`；
2. 创建 `ExchangeCore`（`ExchangeConfig::default()`：ring buffer 64K、单生产者、BusySpin、风控/撮合各 1 分片）；
3. 注册**结果消费者**（当前打印 `command` 与 `result_code`）；
4. **引导**：加载 `data/sse/symbol_data.csv`（41 个标的）→ `core.add_symbol()`；加载 `data/sse/account_data.csv`（10 个账户）→ 对每个账户提交 `AddUser` + `BalanceAdjustment` 命令（此时尚未 `startup()`，走同步路径）；
5. 创建 mpsc 命令通道（容量 1024），`spawn_local` 核心宿主任务：`startup()` 后循环消费命令；
6. 启动 axum，监听 **`127.0.0.1:8080`**（当前硬编码），路由 `POST /place`。

### 7.2 `handle.rs` — HTTP 接入

- 入参 `OrderRequest`（JSON），映射为 `OrderCommand::PlaceOrder` 后 `tx.send().await` 投递；
- `action` 字段映射：`"1" → Ask`，**其余一切值 → Bid（静默兜底）**；
- `order_type` 固定 `Gtc`；`reserve_price` 原样透传；
- 返回 `Json(true)`——**受理即返回，不含撮合结果**（fire-and-forget，结果只能从服务端 stdout 观察）。

### 7.3 `base_info_service.rs` — 基础数据加载

- `load_symbol_info`：解析标的规定 CSV（格式见 §8.1），`MySymbolType` 将 `type` 列的 `"0"~"3"` 映射为 `CurrencyExchangePair/FuturesContract/CallOption/PutOption`；
- `load_account_info`：解析账户 CSV 为 `UserBalance { uid, currency, amount }`；
- 解析容错策略：字段解析失败**静默取 0**（`unwrap_or(0)`）——测试数据可控时尚可，正式使用需改为报错（见问题 P1-3）；
- 附带单元测试（依赖工作目录为 crate 根）。

### 7.4 `config.rs` — 配置结构（预留）

定义了 `[sse]` / `[szse]` 双交易所配置（`server_port` + 数据文件路径），带测试。**`main.rs` 目前未调用**，监听端口与数据路径仍为硬编码；这是"多市场实例化"改造的预留接口（见 §14）。

---

## 8. 基础数据与数据模型

### 8.1 标的规定 `data/sse/symbol_data.csv`

```csv
symbolId,type,baseCurrency,quoteCurrency,baseScaleK,quoteScaleK,takerFee,makerFee,marginBuy,marginSell
600000,0,840,840,100,1,3,2,0,0
```

| 列 | 含义 | 示例说明 |
|----|------|----------|
| `symbolId` | 标的 ID（`SymbolId: i32`） | 直接使用证券代码，如 600000 |
| `type` | 标的类型：0 现货 / 1 期货 / 2 看涨期权 / 3 看跌期权 | A 股股票填 0 |
| `baseCurrency` | 基础货币（卖单冻结币种，成交收货币种） | 840 |
| `quoteCurrency` | 报价货币（买单冻结币种，成交收款币种） | 840 |
| `baseScaleK` | 基础数量乘数 | 100 = 一手 100 股 |
| `quoteScaleK` | 价格数量乘数 | 1 = 价格即最小货币单位 |
| `takerFee` / `makerFee` | 吃单/挂单手续费（按股数收取的整数费用） | 3 / 2 |
| `marginBuy` / `marginSell` | 保证金比例（现货场景为 0） | 0 |

> 数据现值：41 个沪市标的（600xxx/601xxx 等），`baseCurrency == quoteCurrency == 840`（单币种股票买卖模拟；840 为 ISO 4217 的 USD 代码，此处仅作为币种 ID 使用，语义上可理解为 CNY）。`data/szse/` 目录已放置同构数据文件，尚未被加载。

### 8.2 账户数据 `data/sse/account_data.csv`

```csv
uid,currency,amount
10001,840,1000000
```

当前 10 个账户（uid 10001~10010），币种均为 840，金额单位为最小货币单位（分）。

---

## 9. 对外接口

### 9.1 已实现：HTTP 下单接口

```
POST /place
Content-Type: application/json

{
  "order_id": 1,          // u64，客户端自定义订单号（无去重校验）
  "user_id": 10001,       // u64，账户（必须已在账户 CSV 中引导）
  "action": "1",          // 1=买(Bid)、2=卖(Ask)，其他值返回 400（P0-1 已修复）
  "order_type": "GTC",    // GTC/IOC/FOK/FOK_BUDGET/IOC_BUDGET/POST_ONLY/DAY/GTD:<unix_ts>；
                          // 非法值 400，STOP_LIMIT/STOP_MARKET/ICEBERG 暂不支持（P0-4 已修复）
  "price": 1000,          // i64，限价
  "size": 100,            // i64，数量（股）
  "symbol": 600000,       // i32，标的代码
  "reserve_price": 1000   // u64 可选，买单资金冻结预留价；缺省或 0 表示按委托价冻结
}

响应（200，同步返回撮合终态结果，`result_code` 为 CommandResultCode 枚举名）：

```json
{ "order_id": 1, "result_code": "Success", "accepted": true, "leaves_qty": 100,
  "trades": [ {"size": 100, "price": 1000, "counterparty_order_id": 2} ] }
```

错误响应：400 invalid_request（action/order_type/reserve_price 非法）、409 duplicate_order_id、422 validation（字段校验）、503 core_unavailable、504 core_timeout。

其他接口：

| 方法/路径 | 说明 |
|-----------|------|
| `GET /orders/{order_id}` | 订单当前状态（New/PartiallyFilled/Filled/Rejected + 成交明细，含对手方回填），不存在返回 404 |
| `GET /accounts/{uid}/balances` | 账户各币种余额（由结果事件流水推算的镜像值） |
```

> 观察结果的补充方式：服务端 stdout 仍打印 `result: <command> - <result_code>`；HTTP 响应与查询接口是主要途径。

### 9.2 规划中：SSE 二进制协议网关

`Cargo.toml` 已引入 `sse-binary` 编解码库（fin-protoc 生成），配套 `testcase/sse/` 场景与 `gt-auto` 测试工具。规划架构：

```
gt-auto (测试工具)                     exchange-simulator-rs
┌──────────────┐   TCP/二进制协议    ┌──────────────────────────┐
│ OMS 模拟端    │ ◄───────────────► │  SSE 协议网关（规划）      │
│ gw-auto-sse  │  localhost:9010   │  报文 ↔ OrderCommand 适配  │
└──────────────┘                   └──────────────────────────┘
```

协议报文 `SseBinary` 帧：`msg_type(u32) + msg_seq_num(u64) + msg_body_len(u32) + body + checksum(u32)`，消息类型映射：

| msg_type | 报文 | 方向 | 对应 OrderCommand / 事件 |
|----------|------|------|--------------------------|
| 33 | Heartbeat 心跳 | 双向 | — |
| 40 | Logon 登录 | 双向 | 会话建立 |
| 41 | Logout 登出 | 双向 | 会话关闭 |
| 58 | NewOrderSingle 新订单 | 收 | `PlaceOrder` |
| 61 | OrderCancel 撤单 | 收 | `CancelOrder` |
| 32 | Confirm 订单确认 | 发 | 受理回执（对应 `result_code`） |
| 59 | CancelReject 撤单被拒 | 发 | 撤单失败 |
| 103 | Report 成交报告 | 发 | `matcher_events`（Trade） |
| 204 | OrderReject 订单被拒 | 发 | `RiskNsf` 等拒绝 |
| 209 | PlatformState 平台状态 | 发 | 交易所状态模拟 |
| 208/206/207/210 | ExecRpt Info/Sync/SyncRsp/EndOfStream | 发 | 执行报告同步/对账流 |

测试场景编排（`testcase/sse/sse_test_case.csv`）按 `case_id/step_id/action_type(Send|Receive)/msg_type` 驱动，字段模板在 `sse_40.csv`（Logon）、`sse_58.csv`（新订单：ClOrdID/SecurityID/Side/Price/OrderQty…）、`sse_32.csv`（确认：OrdStatus/LeavesQty…）、`sse_103.csv`（成交：LastPx/LastQty/GrossTradeAmt…）。

---

## 10. 关键流程

### 10.1 启动引导流程

```
main
 │ 1. 构建 runtime + LocalSet
 │ 2. ExchangeCore::new(默认配置)
 │ 3. set_result_consumer(打印回调)
 │ 4. load_symbol_info → 逐个 add_symbol        ← 同步写入流水线
 │ 5. load_account_info → 逐个 AddUser + BalanceAdjustment
 │        （startup 之前 → submit_command 走 pipeline.handle_event 同步路径）
 │ 6. 创建 mpsc(1024) 通道
 │ 7. spawn_local: core.startup() → Disruptor 就绪（消费线程开始自旋）
 │                 loop { rx.recv() → submit_command（走环形缓冲区） }
 │ 8. axum::serve 绑定 127.0.0.1:8080（阻塞，进程常驻）
 ▼
服务就绪
```

### 10.2 下单与成交时序

```
Client         axum(handle.rs)        mpsc         core task        Disruptor 消费线程
  │ POST /place      │                  │               │                  │
  ├─────────────────▶│ OrderRequest     │               │                  │
  │                  ├─ OrderCommand ──▶│               │                  │
  │◀─ 200 报告 ───────┤ (同步等待结果，5s 超时)    ├── recv() ────▶│                  │
  │                  │                            │               │                  │
  │                  │                            │               ├─ publish(cmd) ──▶│
  │                  │                            │               │                  ├─ R1: 冻结资金/校验
  │                  │                            │               │                  ├─ Matching: 撮合, 产出 events
  │                  │                            │               │                  ├─ R2: 双边记账/退差价
  │                  │                            │◀──────────────┴──────────────────┤ ResultConsumer: 结果路由
  │                  │                            │  （受理/拒绝报告、订单注册表、余额镜像）│
```

> 阶段一更新：结果不再只打印，而是经 core_bridge 结果路由回传——`/place` 同步返回终态报告，订单/余额可查询（详见开发设计文档 §4.6）。

**资金安全示例**（买单全成）：冻结 `size×reserve_price×quote_scale_k + size×taker_fee` → 撮合成交价 `price` → R2 退差价 `(reserve_price−price)×size×quote_scale_k`，入 `base` 币 `size×base_scale_k`。预留价机制保证即使成交价上穿委托价也不会扣穿账户。

---

## 11. 配置管理

**现状**：配置已全面接管（阶段一 T1.3 完成），`main.rs` 支持 `--config <path>`（默认 `conf/config.toml`）：

| 配置项 | 来源 | 说明 |
|--------|------|------|
| HTTP 监听端口 | `[sse].server_port`（当前 8080） | 绑定 127.0.0.1 |
| SSE 网关端口 | `[sse].gateway_port`（当前 9010，预留） | 阶段二启用 |
| 数据文件路径 | `[sse].matcher.*_info_path` | fail-fast 加载 |
| 撮合核心参数 | `[sse].matcher.core` | ring/分片/生产者/等待策略，缺省同 matching-core 默认 |
| 日志级别 | `[logging].level`（可被 `RUST_LOG` 覆盖） | 默认 info |
| 市场开关 | `[sse]/[szse].enabled` | 阶段一仅启用 sse，szse 关闭（阶段三扩展为多实例） |

**持久化**：matching-core 提供 WAL（`enable_journaling`，rkyv 长度前缀帧）与快照（`enable_snapshotting`/`take_snapshot`/`load_latest_snapshot`）能力，本仓库**尚未启用**；当前进程为纯内存态，重启即回到初始 CSV 状态。

---

## 12. 测试体系

| 层级 | 内容 | 运行方式 |
|------|------|----------|
| 单元测试 | `config`（解析/核心参数映射）、`base_info_service`（CSV 正常/注释/CRLF/坏字段/重复/空文件，共 12 例）、`model`（validator 校验 4 例）、`handle`（action/order_type 映射）、`errors`（状态码映射）、`core_bridge`（不可用/幂等）——共 27 例 | `cargo test`（fixture 写入临时目录，不依赖工作目录） |
| 集成测试 | 真实撮合核心端到端 5 例：挂单冻结、成交双边结算（余额精确断言）、资金不足 RiskNsf、order_id 幂等、核心不可用 | `cargo test --test integration_test` |
| 场景测试 | `testcase/sse/sse_test_case.csv` 定义 case sse_001（Logon → 新订单 → 确认 → 成交报告） | `./autotest.sh` → 调用外部 `gt-auto` 工具（需另行安装），针对阶段二的协议网关（9010 端口） |
| CI | GitHub Actions：`cargo fmt --check` → `clippy -D warnings` → `test` → `build --release` | `.github/workflows/ci.yml` |

---

## 13. 代码走查发现的问题

以下按优先级列出本次走查发现的问题，供后续迭代排期。

### P0（影响正确性/可观测性）

> **2026-10-06 更新：P0-1 ~ P0-4 已全部修复**（`src/handle.rs` 重写 + `main.rs` 日志初始化，经冒烟验证），下表保留作为变更记录。

| # | 位置 | 问题 | 建议 |
|---|------|------|------|
| P0-1 | `src/handle.rs:14-17` | `action` 字段非 `"1"` 时**静默按买单处理**，测试数据写错方向不易察觉；且当前 `"1"→Ask` 与 SSE 惯例（1=买入/2=卖出）相反 | 显式映射 `"1"→Bid（买）`、`"2"→Ask（卖）`，其余返回 400（详细设计 §4.2，属接口语义修正需通告调用方） |
| P0-2 | `src/handle.rs:7` + `main.rs` | 引入 `tracing` 但从未初始化 subscriber，`info!` **不产生任何输出** | `main` 中初始化 `tracing_subscriber`（需加依赖）或改用 `log`+`env_logger` |
| P0-3 | `src/handle.rs:27-29` | `tx.send().await.expect(...)` 在通道关闭（核心任务崩溃/退出）时 panic 整个 handler 线程 | 改为匹配错误并返回 `StatusCode::SERVICE_UNAVAILABLE` |
| P0-4 | `src/handle.rs:27` | `order_type` 请求字段被**完全忽略**，固定 `Gtc`；`IOC/FOK` 等类型无法测试 | 解析 `order_type` 字符串映射到 `OrderType`，未识别时拒绝 |

### P1（健壮性 / 语义一致性）

> **2026-10-06 更新：P1-1 ~ P1-7 已随阶段一（T1.1~T1.6）全部修复**；P1-8 的引导顺序约束已封装进 `core_bridge::bootstrap_exchange`（结果消费者先于引导命令注册，启动后再调 `add_symbol` 的路径已不存在）。下表保留作为变更记录。

| # | 位置 | 问题 | 建议 |
|---|------|------|------|
| P1-1 | `src/model.rs` | `OrderRequest` 无字段校验：`size<=0`、`price<=0`、`user_id=0` 等直接透传给核心 | 启用 `validator` 派生或在 handler 内校验，返回 422 |
| P1-2 | `src/main.rs:34-40` | 数据文件路径、监听地址硬编码；`config.rs` 与 `conf/config.toml` 成死代码 | `main` 接入 `load_config`（支持 `--config` 参数），按配置启动 SSE 实例 |
| P1-3 | `src/base_info_service.rs:38-50` | CSV 字段解析失败静默 `unwrap_or(0)`，会生成 `symbol_id=0` 的脏规格或 0 金额账户 | 解析失败即返回错误（`anyhow`），fail-fast |
| P1-4 | `src/base_info_service.rs:24-33` | 按行号跳表头（`index==1`），若文件以注释开头会把注释当表头跳过、表头当数据解析；未处理 `\r\n` | 先判断首行是否以 `symbolId/uid` 开头再决定跳过；`trim_end` 处理 `\r` |
| P1-5 | `src/handle.rs:19-28` | `order_id` 由客户端提供且无去重/幂等保护，重复提交会污染订单簿 | 核心或接入层增加 order_id 唯一性校验 |
| P1-6 | `src/main.rs:29-31` | 结果消费者仅 `println!`，HTTP 侧无法拿到撮合结果（受理/拒绝/成交） | 为命令引入关联 ID，结果经 `mpsc`/`DashMap<order_id, oneshot::Sender>` 回推，使 `/place` 可同步返回结果码；或提供订单状态查询接口 |
| P1-7 | `src/main.rs` | 无优雅停机（`ctrl-c` 直接终止），核心任务与 HTTP 不做 join/shutdown | 接入 `tokio::signal`，触发 `ShutdownSignal` 命令与 listener 优雅关闭 |
| P1-8 | `src/main.rs:26-36` | `startup()` 后 `add_symbol` 静默失效（pipeline 已被 move），生命周期约束仅存在于隐式约定 | 在 `ExchangeCore` 使用侧以类型状态/注释固化"先引导后启动"顺序，并在 startup 后调用 add_symbol 时告警 |

### P2（整洁度 / 可维护性）

> **2026-10-06 更新：P2-1/P2-2/P2-3/P2-5/P2-6 已随阶段一（T1.4/T1.7）修复**（bincode/bigdecimal 移除、toml 版本号规范、警告清零、MySymbolType fail-fast、测试改用 CARGO_MANIFEST_DIR/临时目录、关键 expect 带上下文）；**P2-4（币种代码 840 vs 156）保留待需求方确认**。

| # | 位置 | 问题 | 建议 |
|---|------|------|------|
| P2-1 | `Cargo.toml` | `bincode`、`bigdecimal`、`validator`（及配套的 `sse-binary`/`binary-codec` 若短期不接入）未使用，拉长编译时间 | 未接线前移除或挪到 `[workspace.dependencies]` 注释保留 |
| P2-2 | `src/handle.rs:4-7`、`Cargo.toml` | 未使用导入：`ExchangeCore`、`Mutex`、`error`；`toml` 版本号含被忽略的 semver 元数据 `+spec-1.1.0`（共 4 条警告） | `cargo fix` 清理导入，`toml = "1.1.0"` 去掉元数据 |
| P2-3 | `src/base_info_service.rs:5-17` | `MySymbolType` 的 `FromStr` 失败时被调用方 `unwrap_or` 兜底为现货；且未覆盖 `PerpetualSwap` | 返回错误而非默认值；映射表补全 |
| P2-4 | `data/sse/*.csv` | 币种代码使用 840（USD 的 ISO 4217 码）表示人民币场景 | 若 840 无特殊含义，建议改用 156（CNY）避免误读，或在文档注明为内部币种 ID |
| P2-5 | 单元测试 | 依赖 CWD 相对路径，IDE/子目录执行会失败 | 用 `CARGO_MANIFEST_DIR` 拼绝对路径 |
| P2-6 | `src/main.rs:16-19` | `unwrap()` 多处（runtime/绑定/解析），模拟器可接受，但至少绑定失败应输出可读错误 | 关键失败点带上下文信息 `expect("bind 127.0.0.1:8080")` |

---

## 14. 演进路线图

结合现有代码骨架与预留接口，建议按以下阶段演进：

### 阶段一：夯实现有 HTTP 链路（对应 P0/P1）✅ 已完成（2026-10-06）

1. ~~修复 `action` 映射、`order_type` 透传、通道断连错误处理（P0-1/3/4）~~；
2. ~~初始化日志、接入 `conf/config.toml` 驱动端口与数据路径（P0-2、P1-2）~~；
3. ~~结果回传：`/place` 同步返回 `result_code` 与成交明细，新增订单/余额查询接口（P1-6）~~；
4. ~~优雅停机与启动自检（标的/账户加载结果日志汇总、Ctrl-C 三阶段停机）~~；
5. ~~单测/集成测试补齐（32 例）+ CI 流水线 + 依赖清理~~。

### 阶段二：SSE 二进制协议网关
1. 新增 `sse_gateway` 模块：tokio TCP server（按配置监听 9010）→ 帧解析（`sse_binary::SseBinary`）→ 会话管理（Logon/心跳/Logout、序列号校验）；
2. 协议适配层：`NewOrderSingle → PlaceOrder`（Side/OrdType/价格数量字段映射）、结果事件 → `Confirm/Report/OrderReject` 回报，注意 `ClOrdID ↔ order_id`、`Account ↔ uid` 的映射与 `reserve_price` 策略（网关默认按委托价 + 安全上浮冻结）；
3. 与 `gt-auto` 场景测试打通（sse_001 全流程绿色）。

### 阶段三：多市场与状态持久化
1. 按 `conf/config.toml` 实例化多个交易所（SSE/SZSE 各一个 `ExchangeCore` 或分片），统一进程内路由；
2. 启用 WAL + 快照，实现"重启恢复至最新状态"（matching-core 能力已在，需接 `enable_journaling/enable_snapshotting` 与启动重放）；
3. 订单/成交持久化存储，支持对账查询接口。

### 阶段四：仿真增强
- 行情模拟（L2 快照发布、`OrderBookRequest` 查询接入）；
- 平台状态/交易时段模拟（`PlatformState`）、异常场景注入（延迟、拒单率、断线重连）；
- 压测基线（Disruptor BusySpin 模式下的吞吐/延迟指标输出）。

---

## 附录 A：快速开始

```bash
# 构建 & 单测（需可访问 GitHub 拉取 git 依赖；如遇认证失败：
#   export CARGO_NET_GIT_FETCH_WITH_CLI=true）
cargo build
cargo test

# 启动（监听 127.0.0.1:8080，加载 data/sse/*.csv，结果打印到 stdout）
cargo run

# 下单示例（账户 10001 买 600000，价格 1000 分 x 100 股；action：1=买入、2=卖出）
curl -X POST http://127.0.0.1:8080/place \
  -H 'Content-Type: application/json' \
  -d '{"order_id":1,"user_id":10001,"action":"1","order_type":"GTC",
       "price":1000,"size":100,"symbol":600000,"reserve_price":1000}'

# 观察结果：服务端 stdout 输出 result: PlaceOrder - Success
```

## 附录 B：术语表

| 术语 | 含义 |
|------|------|
| Taker / Maker | 吃单方（主动成交）/ 挂单方（被动成交） |
| `reserve_price` | 买单风控冻结预留价，结算时按实际成交价退差价 |
| `scale_k` | 数量级乘数，用于整数化金额换算（如一手 100 股 → 100） |
| Disruptor / RingBuffer | 无锁环形缓冲区并发框架，命令队列的底层实现 |
| WAL | Write-Ahead Log，命令预写日志，用于故障恢复 |
| SSE / SZSE | 上海证券交易所 / 深圳证券交易所（本模拟器的目标协议） |
| gt-auto | 配套的外部自动化测试工具，按 CSV 场景驱动协议报文收发与校验 |
