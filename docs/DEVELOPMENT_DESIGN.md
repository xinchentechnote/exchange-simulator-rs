# exchange-simulator-rs 详细开发设计文档

> 版本：v0.1（对应代码版本 v0.1.0 / main@a60b32b）
> 更新日期：2026-10-06
> 配套文档：[ARCHITECTURE.md](./ARCHITECTURE.md)（系统架构与演进路线，本文不重复其内容）
> 适用读者：本项目的开发实现者。本文回答"每个模块怎么写、接口长什么样、错误怎么定义、怎么测"。

---

## 目录

1. [文档约定](#1-文档约定)
2. [工程结构与模块清单](#2-工程结构与模块清单)
3. [数据字典](#3-数据字典)
4. [模块详细设计](#4-模块详细设计)
5. [接口规格](#5-接口规格)
6. [错误处理设计](#6-错误处理设计)
7. [日志与可观测性设计](#7-日志与可观测性设计)
8. [测试设计](#8-测试设计)
9. [开发规范与工程约定](#9-开发规范与工程约定)
10. [任务拆解（WBS）](#10-任务拆解wbs)

---

## 1. 文档约定

- **【现状】** 表示当前代码的实际行为（作为改造基线）；**【设计】** 表示本次详细设计定义的目标行为（待实现部分明确标注阶段，阶段编号对应 ARCHITECTURE.md §14 的路线图）。
- 所有设计中的字段表、枚举值均已对照依赖源码核实：
  - matching-core：`api/types.rs`、`api/commands.rs`、`api/events.rs`、`core/exchange.rs`、`core/pipeline.rs`、`core/processors/{risk_engine,matching_engine}.rs`（tag v0.2.2）
  - sse-binary：`src/sse_binary.rs` 及各报文定义文件（tag v0.1.0）
- 标注 **（待确认）** 的取值需要与协议细则/需求方核对后固化，实现时以常量集中在 `protocol` 模块定义，禁止散落硬编码。

---

## 2. 工程结构与模块清单

### 2.1 现状结构

| 文件 | 行数 | 职责 | 设计章节 |
|------|------|------|----------|
| `src/main.rs` | 79 | 进程入口：运行时装配、核心引导、HTTP 启动 | §4.1 |
| `src/handle.rs` | 31 | axum 请求处理器 | §4.3 |
| `src/model.rs` | 19 | 对外 DTO | §4.2 |
| `src/base_info_service.rs` | 107 | CSV 基础数据加载 | §4.4 |
| `src/config.rs` | 37 | TOML 配置结构（未接线） | §4.5 |

### 2.2 目标结构（阶段一~二完成后的 src 布局）

> **落地状态**：阶段一部分已于 2026-10-06 实现（lib.rs 拆分、core_bridge/errors/logging 新增），`sse_gateway/` 待阶段二。

```
src/
├── main.rs                 # 入口：参数解析 → 配置 → 日志 → 装配（§4.1）
├── config.rs               # 配置模型与加载（含 CLI 参数，§4.5）
├── model.rs                # HTTP DTO 与校验（§4.2）
├── handle.rs               # axum 路由与 handler（§4.3）
├── base_info_service.rs    # 基础数据加载（fail-fast，§4.4）
├── core_bridge.rs          # 【新增】撮合核心集成层：核心宿主任务、结果路由（§4.6）
├── errors.rs               # 【新增】SimError / ResultCode 映射（§6）
├── logging.rs              # 【新增】tracing 初始化（§7）
└── sse_gateway/            # 【新增·阶段二】SSE 二进制协议网关（§4.7）
    ├── mod.rs              #   网关装配与监听
    ├── codec.rs            #   帧编解码（length/checksum）
    ├── session.rs          #   会话状态机与心跳
    ├── adapter.rs          #   报文 ↔ OrderCommand 适配
    └── report.rs           #   回报生成（Confirm/Report/OrderReject）+ OrderRegistry
```

> 拆分原则：`main.rs` 只做装配；每个模块可独立单测；网关与 HTTP 接入互不依赖，共享 `core_bridge` 与 `errors`。

---

## 3. 数据字典

### 3.1 全局类型别名（matching-core 定义，全系统统一）

| 别名 | Rust 类型 | 语义 | 取值约定 |
|------|-----------|------|----------|
| `UserId` | u64 | 账户 ID | 账户 CSV 的 uid，如 10001 |
| `OrderId` | u64 | 订单 ID | 接入方提供，全局唯一（见 §4.6 幂等设计） |
| `SymbolId` | i32 | 标的 ID | 直接使用证券代码，如 600000 |
| `Currency` | i32 | 币种 ID | 当前统一 840（见 ARCHITECTURE §8.1 说明） |
| `Price` | i64 | 价格 | 最小价格单位（分） |
| `Size` | i64 | 数量 | 股数 |

### 3.2 OrderCommand 字段表

| 字段 | 类型 | PlaceOrder | CancelOrder | AddUser | BalanceAdjustment | 说明 |
|------|:----:|:----------:|:-----------:|:-------:|:-----------------:|------|
| `command` | enum | ✅ | ✅ | ✅ | ✅ | 命令类型 |
| `uid` | u64 | ✅ | ✅ | ✅ | ✅ | 账户 |
| `order_id` | u64 | ✅ | ✅（填被撤单号） | — | — | 订单号 |
| `symbol` | i32 | ✅ | ✅ | — | — | 标的 |
| `price` | i64 | ✅ | — | — | — | 委托价 |
| `reserve_price` | i64 | ✅（买） | — | — | — | 冻结预留价；**FokBudget/IocBudget 时风控改用 `price`** |
| `size` | i64 | ✅ | — | — | — | 委托量 |
| `action` | enum | ✅ | ✅ | — | — | Bid/Ask |
| `order_type` | enum | ✅ | — | — | — | 见 §3.3 |
| `currency` | i32 | — | — | — | ✅ | 入金币种 |
| `amount` | i64 | — | — | — | ✅ | 入金金额（可为负） |
| `tid` | i64 | — | — | — | ✅ | 入金幂等号 |
| `matcher_events` | Vec | 输出 | 输出 | — | — | 撮合事件，见 §3.5 |
| `result_code` | enum | 输出 | 输出 | 输出 | 输出 | 流水线覆写为最终结果 |
| `stop_price` / `visible_size` / `expire_time` | Option | 止损/冰山/GTD 用 | — | — | — | v0.2.2 新增扩展字段 |

### 3.3 OrderType 枚举与接入映射

| OrderType | 含义 | HTTP `order_type` 串（设计） | SSE `ord_type`/`time_in_force`（设计，待确认） |
|-----------|------|------------------------------|------------------------------------------------|
| `Gtc` | 直到撤销 | `GTC` | ord_type=`1`（限价），TIF=`0` |
| `Ioc` | 立即成交剩余撤销 | `IOC` | TIF=`1` |
| `Fok` | 全成或全撤 | `FOK` | TIF=`2` |
| `FokBudget` | FOK+预算 | `FOK_BUDGET` | —（暂不开放） |
| `IocBudget` | IOC+预算 | `IOC_BUDGET` | —（暂不开放） |
| `PostOnly` | 只做 Maker | `POST_ONLY` | — |
| `StopLimit` / `StopMarket` | 止损单 | `STOP_LIMIT` / `STOP_MARKET` | — |
| `Iceberg` | 冰山单 | `ICEBERG`（配 `visible_size`） | — |
| `Day` | 当日有效 | `DAY` | — |
| `Gtd(i64)` | 到期失效 | `GTD:<unix_ts>` | — |

### 3.4 CommandResultCode 全集与分类

| 结果码 | 分类 | 产生环节 | 含义 |
|--------|------|----------|------|
| `New` | 过程 | 初始 | 命令初始态 |
| `ValidForMatchingEngine` | 过程 | 风控 R1 | 风控通过，待撮合 |
| `Success` | 终态 | 撮合/R2 | 受理成功（挂单进入订单簿也算 Success） |
| `Accepted` | 终态 | — | 已受理（预留） |
| `AuthInvalidUser` | 拒绝 | 风控 R1 | 账户未注册 |
| `RiskNsf` | 拒绝 | 风控 R1 | 资金不足 |
| `RiskInvalidReserveBidPrice` / `RiskAskPriceLowerThanFee` / `RiskMarginTradingDisabled` | 拒绝 | 风控 R1 | 预留价非法 / 卖价低于手续费 / 保证金交易未开 |
| `InvalidSymbol` | 拒绝 | 风控 R1 | 标的未注册 |
| `MatchingInvalidOrderBookId` | 拒绝 | 撮合 | 订单簿不存在 |
| `MatchingUnknownOrderId` | 拒绝 | 撮合 | 撤单/改单时订单不存在 |
| `MatchingUnsupportedCommand` | 拒绝 | 撮合 | 命令不支持 |
| `MatchingMoveFailedPriceOverRiskLimit` / `MatchingReduceFailedWrongSize` / `MatchingInvalidOrderSize` | 拒绝 | 撮合 | 改单超限 / 减单数量错 / 数量非法 |
| `UserMgmtUserAlreadyExists` | 拒绝 | 风控 R1 | 重复注册 |
| `StatePersist*Failed` / `BinaryCommandFailed` | 系统 | — | 持久化/二进制命令失败 |

> **流转规则**：`PlaceOrder` 正常路径 `New → ValidForMatchingEngine → Success`；风控拒绝的命令带拒绝码流过撮合引擎（撮合仅处理 `ValidForMatchingEngine`），最终码即拒绝码。

### 3.5 MatcherTradeEvent（撮合事件）

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_type` | enum | `Trade` 成交 / `Reject` 拒绝 / `Reduce` 减量 |
| `size` / `price` | i64 | 本次成交量 / 成交价（对手方挂单价） |
| `matched_order_id` / `matched_order_uid` | u64 | 对手方（Maker）订单号 / 账户 |
| `bidder_hold_price` | i64 | 本单若为买方时的冻结预留价（用于退差价计算） |

### 3.6 CoreSymbolSpecification 与 CSV 字段字典

结构体字段与 CSV 列一一对应（列序见 ARCHITECTURE §8.1）：

| 列 | 字段 | 类型 | 校验规则（设计） |
|----|------|------|------------------|
| symbolId | `symbol_id` | i32 | >0 且全表唯一 |
| type | `symbol_type` | enum | ∈ {0,1,2,3}，见 §3.7 |
| baseCurrency | `base_currency` | i32 | >0；卖单冻结/成交收货币种 |
| quoteCurrency | `quote_currency` | i32 | >0；买单冻结/成交收货币种 |
| baseScaleK | `base_scale_k` | i64 | >0；卖单冻结/成交收入 base 币的每股系数（`size × base_scale_k`） |
| quoteScaleK | `quote_scale_k` | i64 | >0；买单冻结/成交金额的价格系数（`size × price × quote_scale_k`） |
| takerFee / makerFee | `taker_fee` / `maker_fee` | i64 | ≥0；按股数收取 |
| marginBuy / marginSell | `margin_buy` / `margin_sell` | i64 | ≥0；现货场景 0 |

账户 CSV（uid,currency,amount）：`uid` >0 唯一；`currency` >0；`amount` ≥0。

### 3.7 SymbolType 映射（`MySymbolType::from_str`）

| CSV 值 | SymbolType | 说明 |
|--------|------------|------|
| `0` | `CurrencyExchangePair` | 现货（当前全部标的） |
| `1` | `FuturesContract` | 期货 |
| `2` | `CallOption` | 看涨期权 |
| `3` | `PutOption` | 看跌期权 |
| 其他 | **报错**（设计；现状静默兜底为 0，见 ARCHITECTURE P2-3） | — |

---

## 4. 模块详细设计

### 4.1 `main.rs` — 进程装配

**职责**：解析参数 → 加载配置 → 初始化日志 → 构建 runtime → 装配交易所实例 → 启动服务 → 优雅停机。除装配外不含业务逻辑。

【现状】硬编码路径/端口，无停机处理；引导逻辑与装配混在一起。

【设计】目标流程（阶段一）：

```rust
fn main() {
    // 1. CLI: --config <path>（默认 conf/config.toml）
    let args = config::parse_args();
    let cfg = config::load_config(&args.config).expect("config 加载失败");   // fail-fast
    logging::init(&cfg.logging);                                            // tracing subscriber

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        LocalSet::new().run_until(async {
            // 2. 每个启用的交易所一个实例（阶段一仅 sse；阶段三扩展为遍历 cfg）
            let core = bootstrap_exchange(&cfg.sse).await;   // 见下
            // 3. 接入层：HTTP（阶段一） + SSE 网关（阶段二，按 cfg.sse.gateway 启用）
            let http = http_serve(cfg.sse.server_port, core.command_tx());
            // 4. 优雅停机
            tokio::signal::ctrl_c().await.ok();
            shutdown(http, core).await;   // 停止 accept → 发 ShutdownSignal → join 核心任务
        }).await;
    });
}

async fn bootstrap_exchange(conf: &ExchangeConf) -> ExchangeHandle {
    let mut core = ExchangeCore::new(to_core_config(&conf.matcher));   // §4.5 映射
    core.set_result_consumer(core_bridge::result_consumer());          // §4.6
    let symbols = base_info_service::load_symbol_info(&conf.symbol_info_path)?;   // fail-fast，日志汇总条数
    let accounts = base_info_service::load_account_info(&conf.account_info_path)?;
    symbols.into_iter().for_each(|s| core.add_symbol(s));              // 必须在 startup() 前
    for ub in &accounts {                                              // 同步路径（startup 前）
        core.submit_command(cmd!(AddUser, uid: ub.uid));
        core.submit_command(cmd!(BalanceAdjustment, uid: ub.uid,
                                 currency: ub.currency, amount: ub.amount, tid: 0));
    }
    core_bridge::spawn_core_task(core)   // startup() + rx 循环，返回句柄
}
```

**生命周期约束**（固化到 `core_bridge`，防止误用）：

| 约束 | 依据 |
|------|------|
| `add_symbol` / 结果消费者注册 / 引导命令必须在 `startup()` 之前 | `startup()` 会 move 走 pipeline，此后 `add_symbol` 静默无效 |
| 引导命令走同步路径（无 Disruptor），结果码可就地检查 | `submit_command` 在 producer 为空时直接调 `pipeline.handle_event` |
| 引导失败（如 `AddUser` 返回非 Success）→ 进程启动失败退出 | fail-fast，避免带病运行 |

### 4.2 `model.rs` — HTTP DTO

【现状】`OrderRequest` 7+1 字段无校验，`action`/`order_type` 为裸 String。

【设计】保留字段名兼容现有调用方，增加派生校验（阶段一，`validator` 已在依赖中）：

```rust
#[derive(Debug, Deserialize, Validate)]
pub struct OrderRequest {
    #[validate(range(min = 1))]
    pub order_id: u64,          // >0；唯一性由 core_bridge 幂等层保证（§4.6）
    #[validate(range(min = 1))]
    pub user_id: u64,
    #[validate(length(equal = 1))]
    pub action: String,         // "1"=买(Bid) "2"=卖(Ask)，其余 400（§5.1 修正说明）
    pub order_type: String,     // 缺省按 "GTC"；合法值见 §3.3
    #[validate(range(min = 1))]
    pub price: i64,             // >0（市价单暂不支持，price 必填）
    #[validate(range(min = 1))]
    pub size: i64,
    #[validate(range(min = 1))]
    pub symbol: i32,
    pub reserve_price: Option<u64>, // 【修改】改为 Option；None 时买单按 price 冻结（§4.3）
}
```

设计决策：

1. **action 语义修正**：按 SSE 惯例 `1=买入 / 2=卖出`。✅ 已按此语义修复（P0-1，2026-10-06）；**接口语义变更需通告调用方**。完整字段校验（validator 派生、Option 化）仍在阶段一（T1.1）。
2. **reserve_price 策略**：None → 取 `price`（限价单冻结额即 `size×price×quote_scale_k+fee`，因撮合只会以不劣于限价的价格成交，安全）；显式给定时必须 `>= price`（买单），否则 400。预算单（FOK_BUDGET 等）语义相反（price 即预算上限），校验规则随之调整。**P0 修复已先以「字段为 0 时按委托价冻结」的等价规则落地**（DTO 仍是必填 u64，Option 化调整在 T1.1）。
3. `order_id`/`user_id`/`symbol` 的存在性校验（账户已注册、标的已加载）不在 DTO 层做——由核心风控给出权威结果码，接入层透传。

【现状（2026-10-07 更新）】上述设计已落地（validator 派生校验 + `reserve_price: Option<u64>`，`order_type` 仍为裸 String 由 §4.3 映射）。另新增 `CancelRequest { order_id, user_id }` 供 `POST /cancel` 使用——标的代码由订单注册表回查，故不入请求体。

### 4.3 `handle.rs` — axum 路由与 handler

【现状】单 handler `place_order`：解析→构造命令→`tx.send().await.expect()`→返回 `true`。

【设计】阶段一目标：

```rust
pub async fn place_order(
    State(app): State<Arc<AppState>>,          // 含 command_tx + pending registry（§4.6）
    Json(req): Json<OrderRequest>,
) -> Result<Json<PlaceOrderResponse>, ApiError> {
    req.validate()?;                            // 422 + 字段级错误明细
    let cmd = adapter::from_http(&req)?;        // action/order_type/reserve_price 映射，非法值 400
    let rpt = app.core_bridge.execute(cmd, EXECUTE_TIMEOUT).await?;   // 同步等结果，5s 超时→504
    Ok(Json(rpt.into()))                        // 200 {order_id, result_code, trades?}
}
```

- `ApiError` 实现 `IntoResponse`，统一错误体（§6.3）；通道已断 → 503。
- 路由表（阶段一新增两个查询接口，服务测试与联调）：

| 方法/路径 | 说明 | 返回 |
|-----------|------|------|
| `POST /place` | 下单（同步受理结果） | `PlaceOrderResponse` |
| `GET /orders/:order_id` | 订单当前状态（来自 OrderRegistry/结果缓存） | 状态 + 成交明细 |
| `GET /accounts/:uid/balances` | 账户各币种余额（联调对账用） | `Map<currency, amount>` |

【现状（2026-10-07 更新）】已实现的路由为 7 条：上表三条 + `POST /cancel`（撤单，同步返回终态，§5.1）+ `GET /trades`、`GET /admin/state`（T3.3 对账）+ `GET /health`（就绪探针，核心命令通道关闭时 503）。

三点与上述设计稿的差异：

1. **提取器失败也返回统一 JSON 错误体**：handler 以 `Result<Json<T>, JsonRejection>`（及 `PathRejection`/`QueryRejection`）接收参数并转成 `SimError`，保留 axum 原状态码（400/415/422），避免畸形 JSON、路径参数非数字等场景返回纯文本；
2. **路由状态是 `AppState { bridge, exchange }`**：`exchange` 市场名仅用于 `/health` 标注探针归属，`router(bridge, exchange)` 由 `main.rs` 按市场实例分别构建；
3. **撤单不等价于「fire-and-forget」**：网关撤单仍走结果广播回报，HTTP `/cancel` 则在桥接层按被撤订单号登记一次性回传通道（`CoreBridge::cancel`），任何返回码都必须回传，否则等待方白等到超时。

> 余额查询的实现：`core_bridge` 维护 `result_consumer` 回推的余额快照（`BalanceAdjustment`/成交结算后由结果事件推算），**不直接读核心**（核心无查询命令）。若精度不可接受，改为在引导时镜像一份初始余额并叠加结果事件流水（阶段一先实现后者，简单可靠）。

### 4.4 `base_info_service.rs` — 基础数据加载

【现状】行号跳表头、`unwrap_or(0)` 静默容错、不处理 CRLF。

【设计】改为 fail-fast + 显式校验（签名变更，阶段一）：

```rust
pub fn load_symbol_info(path: &str) -> anyhow::Result<Vec<CoreSymbolSpecification>>
pub fn load_account_info(path: &str) -> anyhow::Result<Vec<UserBalance>>
```

解析算法（两个函数共用一套行迭代辅助函数 `csv_rows(path) -> impl Iterator<Item=(usize /*line_no*/, String)>`）：

1. 逐行读取，`trim_end_matches(['\r','\n'])`；空行、`#`/`//` 开头行跳过；
2. **表头识别**：第一个非跳过行若以 `symbolId`/`uid` 开头则跳过（不依赖行号，兼容注释在最前的文件）；
3. 字段按 `,` 切分，**列数不符 → `Err`（含行号与行内容）**；每个字段 `parse` 失败 → `Err(行号, 列名, 原始值)`；
4. 全表校验：`symbol_id`/`uid` 唯一（重复报错）、`symbol_type` ∈ 0..=3（`MySymbolType::from_str` 返回 Err 而非兜底）、scale_k/币种按 §3.6 规则；
5. 返回并在启动日志汇总 `loaded N symbols from <path>`。

错误示例：`failed to parse symbol_data.csv:56 baseScaleK: invalid digit found in string, line: "600000,0,840,840,x,1,3,2,0,0"`。

### 4.5 `config.rs` — 配置模块

【现状】结构已定义双市场，未被 main 使用。

【设计】

```toml
# conf/config.toml（目标格式，向后兼容现有键）
[logging]
level = "info"            # tracing EnvFilter 语法，支持 RUST_LOG 覆盖

[sse]
server_port = 8080        # 【语义变更】HTTP 监听端口（阶段一）；阶段二另加 gateway_port = 9010
enabled = true
[sse.matcher]
symbol_info_path = "data/sse/symbol_data.csv"
account_info_path = "data/sse/account_data.csv"
[sse.matcher.core]        # 映射 matching-core ExchangeConfig，缺省同其 Default
ring_buffer_size = 65536
matching_engines_num = 1
risk_engines_num = 1
producer_type = "Single"  # Single | Multi
wait_strategy = "BusySpin"

[szse]                    # 阶段三启用
enabled = false
```

加载规则：`--config` 参数 > 默认路径 `conf/config.toml`；解析失败/必填缺失 → 启动报错退出。`server_port` 现值 9010/9011 规划给协议网关，HTTP 用 8080——阶段一改造时**同步更新 config.toml**，避免 9010 被 sse_32 测试工具占用。

### 4.6 `core_bridge.rs` — 撮合核心集成层（新增，核心模块）

> **落地状态（2026-10-06，阶段一 T1.5/T1.6）**：已实现。与设计稿的差异：
> 1. 命令通道直接传 `OrderCommand`（未引入 `CoreRequest` 包装——结果等待由 `execute` 内部的 pending 表+oneshot 完成，无需 reply 字段）；
> 2. 结果消费者以 `try_send` 投递至 tokio mpsc（容量 4096），满则丢弃并告警（§4.6.5 的首选方案）；
> 3. 注册表淘汰为**先进先出**（非严格 LRU），终态 order_id 不复用、容量 10 万；
> 4. `spawn_core_task`/`spawn_result_task` 为私有函数，只持有各自所需状态（结果任务仅持 shared/specs 两个 Arc），**不持有 CoreBridge**——否则 core_task 与 result_task 会经命令/结果通道形成存活环，导致优雅停机无法收敛（冒烟阶段发现并修复）；
> 5. 结果消费者在 `bootstrap_exchange` 中、于引导命令提交**之前**注册，保证 BalanceAdjustment 进入余额镜像；
> 6. taker 成交会同步更新 maker 侧的订单注册表记录（filled/leaves/status/成交明细回填）。

**职责**：持有 `ExchangeCore` 宿主任务；对上层提供"提交命令并等待结果"的同步语义；把 Disruptor 线程产生的结果路由回等待方；维护订单/余额查询用的只读快照。

#### 4.6.1 结构

```rust
pub struct CoreBridge {
    command_tx: mpsc::Sender<CoreRequest>,          // 宿主任务入口（容量 1024）
    pending: Arc<Mutex<HashMap<OrderId, oneshot::Sender<ExecutionReport>>>>,
    registry: Arc<Mutex<OrderRegistry>>,            // order_id → 订单快照（§4.7.5 共用）
    balances: Arc<Mutex<HashMap<(UserId, Currency), i64>>>,   // 余额镜像
}
pub struct CoreRequest { cmd: OrderCommand, reply: Option<oneshot::Sender<OrderCommand>> }
```

#### 4.6.2 结果路由（解决"fire-and-forget 拿不到结果"问题，ARCHITECTURE P1-6）

```
axum handler                    LocalSet 宿主任务                Disruptor 消费线程
    │ execute(cmd)                     │                              │
    ├─ pending.insert(order_id, tx)    │                              │
    ├─ command_tx.send(req) ──────────▶│ core.submit_command(cmd) ───▶│ 环形缓冲区
    │                                   │                              ├─ Pipeline 处理
    │                                   │◀─ result_tx(std mpsc) ───────┤ result_consumer(cmd)
    │◀─ result_task: result_rx.recv()   │                              │
    │   → pending.remove(order_id).tx.send(report)                     │
    ├─ oneshot.await（timeout 5s）      │                              │
    ◀─ ExecutionReport ────────────────┤                              │
```

要点：

1. `result_consumer` 运行在 **Disruptor 消费线程**（非 async 上下文），用 **`std::sync::mpsc`**（无锁队列语义足够，非阻塞 `try_send`，满则丢弃并计数告警——结果通道容量 4096）；
2. `result_task` 是 LocalSet 上的常驻任务，负责把结果分发到 `pending` 的 oneshot、更新 `registry`/`balances` 快照；
3. 每条命令都以 `Success` 或拒绝码终态（见 §3.4 流转规则），**所有** PlaceOrder 都能等到结果；撤单同理（`MatchingUnknownOrderId` 也是终态）；HTTP `/cancel` 复用同一结果路由，按**被撤订单号**在 `cancel_pending` 登记一次性回传通道（回传 `CancelReport`），网关撤单仍走结果广播生成协议回报，两者互不影响；
4. 超时兜底：等待方 5s 超时返回 504，并从 `pending` 移除；迟到的结果写入 `registry` 供 `GET /orders/:id` 补查。

#### 4.6.3 ExecutionReport（结果模型）

```rust
pub struct ExecutionReport {
    pub order_id: OrderId,
    pub result_code: CommandResultCode,     // 终态码
    pub trades: Vec<TradeFill>,             // 由 matcher_events 过滤 Trade 而来
    pub leaves_qty: i64,                    // 剩余量（挂单=size；部分成交递减）
}
pub struct TradeFill { pub size: i64, pub price: i64, pub counterparty_order_id: OrderId }
```

#### 4.6.4 幂等与 order_id 管理

- `registry` 中已存在的 `order_id` 再次 PlaceOrder → 直接拒绝（HTTP 409 / SSE OrderReject reason=重复订单号），不进核心；
- 订单从簿中消失（全成/全撤）后 `order_id` **不复用**：registry 保留终态记录（容量上限 10 万条，LRU 淘汰）。

#### 4.6.5 宿主任务

```rust
pub(crate) async fn core_task(mut core: ExchangeCore, mut rx: mpsc::Receiver<CoreRequest>,
                              result_rx: std_mpsc::Receiver<OrderCommand>) {
    core.startup();                                    // 之后 add_symbol 失效
    loop { tokio::select! {
        Some(req) = rx.recv() => { core.submit_command(req.cmd); req.reply 转发/忽略 }
        Ok(cmd) = result_rx.recv() => route_result(cmd).await,
        else => break,
    } }
}
```

> `select!` 中的 `std_mpsc::Receiver` 用 `tokio::task::spawn_blocking` 或轮询桥接有开销；实现时优先方案：result_consumer 直接用 `tokio::sync::mpsc::try_send`（容量大 + 单线程 LocalSet 下等待方活跃，阻塞概率低），并保留丢弃计数器。

### 4.7 `sse_gateway/` — SSE 二进制协议网关（阶段二，规划模块）

> **落地状态（2026-10-06，T2.1~T2.5 完成）**：已全部实现并通过 gt-auto 真实联调
> （4 场景 14 个 Receive 校验步骤全部 ✅）+ 4 个 Rust 端到端协议测试。与设计稿的差异/补充：
> 1. **校验和**：binary_codec 全局已注册 `SsebinChecksumService`（字节累加 mod 256），编码自动计算，无需手工处理；
> 2. **登录应答**：只回 Logon（sender/target 对调、其余回显），不主动发 PlatformState（避免干扰场景校验）；Logon 幂等应答（跨用例复用连接时重复登录仍回执）；gt-auto 客户端库连接时会先发一个空字段握手 Logon，服务端按普通登录处理；
> 3. **字段回填规则（依据 gt-auto 模板校验校准）**：
>    - `ReportIndex` 按「订单 × 报文类别」从 **0** 递增（Confirm/Report 各自独立计数）；
>    - `OrdCnfmId`/`TrdCnfmId` 留空；
>    - Report 仅回填成交与订单核心要素，`OwnerType=0`、`OrderEntryTime=0`、`GrossTradeAmt=0`、`ClearingFirm` 空（Confirm 则完整回显订单要素）；
>    - 撤单成功回报 Confirm(exec_type="4")，字段回填原订单要素 + `OrigClOrdID`，时间字段取撤单请求值；
> 4. **回报触发**：core_bridge 新增结果广播（`subscribe_results`，tokio broadcast），网关订阅后由 `report::on_place_result/on_cancel_result` 生成回报；`ReportIndex` 计数器在网关订单记录内维护；
> 5. **撤单路径**：撤单走 `CoreBridge::submit`（不占 pending 表），结果经广播回传；仅允许撤销网关登记的订单；
> 6. 新增拒绝原因码：**1007** 报文字段非法（适配失败）、**1008** 未登录（登录前收到业务报文，回拒并断开）。

**目标**：使 `gt-auto`（OMS 模拟端）按 `testcase/sse` 场景与本模拟器完成 Logon→新订单→确认→成交报告全流程。

#### 4.7.1 帧编解码（codec.rs）

帧结构（大端序，已对照 `sse_binary.rs` 核实）：

```
┌─────────────┬──────────────┬──────────────┬───────────────┬───────────┐
│ msg_type u32│ msg_seq u64  │ body_len u32 │ body (变长)    │ checksum u32│
└─────────────┴──────────────┴──────────────┴───────────────┴───────────┘
  16 字节固定头（前三字段）        body_len 字节     校验和由 CHECKSUM_SERVICE_CONTEXT("SSE_BIN") 计算，
                                                  未注册服务时编码器回退填结构体自带值
```

- 解码循环：读满 16B 头 → 读 body_len 体 → 读 4B 校验和 → `SseBinary::decode`（内部按 msg_type 分发，未知类型返回 None → 记日志并**跳过该帧继续**，不断连，容错优先）；
- 粘包/半包：以 `body_len` 为准的长度前置解析（`BytesMut` 缓冲，复用 tokio `AsyncReadExt::read_buf`）；
- 编码：构造 `SseBinary{ msg_type, msg_seq_num, body, checksum: 0 }` 调 `encode`，校验和若无法计算则保持 0（测试工具侧不校验，待确认）。

#### 4.7.2 会话状态机（session.rs）

```
                 TCP established
                       │
                       ▼
                ┌─────────────┐   Logon(40) 校验通过        ┌────────────┐
                │ AwaitingLogon│ ─────────────────────────▶ │   Active   │
                └─────────────┘   回 Logon + PlatformState  └─────┬──────┘
                       │ 收到非 Logon 报文                         │ Logout(41)/断线/心跳超时
                       ▼ 回 OrderReject 并断开                    ▼
                    (断开)                                    ┌─────────────┐
                                                              │   Closed    │
                                                              └─────────────┘
Active 态行为：
• 收 Heartbeat(33) → 回 Heartbeat；连续 heart_bt_int 秒无任何入站报文 → 超时断开
• msg_seq_num：宽容模式（默认）——记录、跳变仅告警；严格模式（可配）——乱序回 CancelReject 并断开
• 每连接独立状态，同一账户允许多会话（模拟器语义）
```

- 每连接一个 `spawn_local` 读任务 + 一个写任务（出站队列 `mpsc::channel<SseBinary>(256)`，满则丢弃 Confirm 保留 Report 并告警）；
- Logon 校验（模拟器语义，宽松）：`prtcl_version` 记录不校验；`sender_comp_id/target_comp_id` 记录到会话供回报回填 `pbu` 等字段。

#### 4.7.3 入站报文适配（adapter.rs）

**NewOrderSingle(58) → OrderCommand::PlaceOrder**

| NewOrderSingle 字段 | 类型 | → OrderCommand | 映射规则 |
|---------------------|------|----------------|----------|
| `cl_ord_id` | String | `order_id` u64 | 去除非数字前缀取数值（`c10001`→10001）；解析失败回 OrderReject |
| `account` | String | `uid` u64 | 直接 parse；不存在由核心风控拒绝（AuthInvalidUser） |
| `security_id` | String | `symbol` i32 | 直接 parse |
| `side` | String | `action` | `1`→Bid，`2`→Ask，其他 → OrderReject（**协议标准方向，注意与现状 HTTP 相反，见 §4.2**） |
| `price` | i64 | `price` | 直传 |
| `order_qty` | i64 | `size` | 直传，≤0 → OrderReject |
| `ord_type`+`time_in_force` | String | `order_type` | 按 §3.3 映射，组合非法 → OrderReject |
| （无对应字段） | — | `reserve_price` | 网关策略：`= price`（§4.2 决策 2） |
| `biz_id/biz_pbu/owner_type/credit_tag/clearing_firm/branch_id/user_info/transact_time` | — | — | 存入 OrderRegistry，回报时原样回填 |

**OrderCancel(61) → OrderCommand::CancelOrder**：`orig_cl_ord_id`→`order_id`，`account`→`uid`，`security_id`→`symbol`；`cl_ord_id`（撤单请求自身的 ID）仅登记。

#### 4.7.4 出站回报生成（report.rs）

回报触发点为 `core_bridge` 结果路由（§4.6.2），网关在 `route_result` 处订阅：

| 触发 | 回报 | 关键字段填充 |
|------|------|--------------|
| PlaceOrder 终态 = Success 且无成交 | **Confirm(32)** | `exec_type="0"`、`ord_status="0"`、`price/order_qty` 回填、`leaves_qty=order_qty`、`cxl_qty=0`、`ord_cnfm_id=自增ID` |
| 每个成交事件 Trade | **Report(103) ×2（买卖双方各一条）** | `exec_type="F"`、`last_px=event.price`、`last_qty=event.size`、`gross_trade_amt=size×price×quote_scale_k`、双方各自 `leaves_qty` 递减、`trd_cnfm_id=自增ID`；字段从 OrderRegistry 双侧记录回填（本方+对手方 `matched_order_uid/order_id`） |
| PlaceOrder 终态 = 拒绝码 | **OrderReject(204)** | `ord_rej_reason` 按 §6.4 映射 |
| CancelOrder 终态 = Success | Confirm(32) | `exec_type="4"`（已撤，待确认）、`cxl_qty=本次撤量`、`leaves_qty=0` |
| CancelOrder 终态 = 拒绝码 | CancelReject(59) | 原因映射同 §6.4 |

- `report_index`：会话级单调递增 u64，从 1 开始；`set_id=0`；
- 对手方回报：taker 的 `matcher_events` 含 maker 的 `matched_order_uid/order_id`，双方 `leaves_qty` 均由 OrderRegistry 维护（maker 的成交使 registry 中其记录递减）；
- 顺序保证：同一订单的 Confirm 先于其 Trade Reports（同一 result 分发路径内按序出队）。

#### 4.7.5 OrderRegistry（网关订单跟踪表）

```rust
pub struct OrderRecord {
    pub order_id: OrderId, pub uid: UserId, pub symbol: SymbolId,
    pub side: OrderAction, pub order_qty: i64, pub leaves_qty: i64,
    pub status: OrderStatus,                // New / PartiallyFilled / Filled / Cancelled / Rejected
    pub meta: NewOrderSingleMeta,           // §4.7.3 回填字段
}
```

- 写入方：适配层（New）、结果路由（成交递减/终态）；读取方：回报生成、`GET /orders/:id`；
- 内存表 + LRU 上限；不做持久化（重启丢失即回到纯内存模拟器语义）。

---

## 5. 接口规格

### 5.1 HTTP API

> 【现状】每个市场实例独立监听（SSE `server_port`、SZSE 由配置决定），路由一致。

#### POST /place

请求体见 §4.2；成功响应（含受理与拒绝两种业务结果，HTTP 均为 200）：

```json
{
  "order_id": 10001,
  "result_code": "Success",            // CommandResultCode 字符串
  "accepted": true,
  "leaves_qty": 100,
  "trades": [ {"size": 100, "price": 1000, "counterparty_order_id": 10002} ]
}
```

#### POST /cancel

请求体只需目标订单与账户；标的代码由订单注册表回查（订单已被淘汰则撤不到）：

```json
{ "order_id": 10001, "user_id": 10001 }
```

响应为撤单终态 + 撤单后的订单快照（`status`/`leaves_qty` 在订单已被注册表淘汰时为 `null`）：

```json
{ "order_id": 10001, "result_code": "Success", "accepted": true,
  "status": "filled", "leaves_qty": 0 }
```

> 撤单同步等待核心结果（与 `/place` 共用 `EXECUTE_TIMEOUT`）；解冻口径见 §4.6 的撤单镜像说明。对已进入终态的订单重复撤单返回 200 + `accepted=false` + `result_code=MatchingUnknownOrderId`，与交易所「撤拒」语义一致。

#### 错误响应（统一 JSON 错误体）

所有失败响应（含 JSON/路径/查询参数解析失败）都走同一形状 `{"error": <码>, "detail": <可选说明>}`，便于测试工具统一解析：

| HTTP | error | 场景 |
|------|-------|------|
| 400 | `invalid_request` | action/order_type 非法、买单 reserve_price < 委托价；请求体不是合法 JSON；路径/查询参数类型不符 |
| 404 | `order_not_found` | `GET /orders/:order_id` 无记录；`POST /cancel` 目标订单不存在 |
| 409 | `duplicate_order_id` | order_id 已存在于注册表或仍在途 |
| 409 | `cancel_in_flight` | 同一订单已有撤单请求在途 |
| 415 | `invalid_request` | `POST` 缺少 `Content-Type: application/json` |
| 422 | `validation` | 字段校验失败（`{"error":"validation","fields":{"size":"range"}}`）；JSON 字段类型不符（如 `reserve_price` 传负数） |
| 503 | `core_unavailable` | 命令通道关闭（核心停机中）；`GET /health` 亦用此码 |
| 504 | `core_timeout` | 等待核心结果超时（`EXECUTE_TIMEOUT`，当前 5s） |

> **受理结果说明**：`result_code=Success` 仅表示订单被交易所接受（可能已成交或挂在簿上）；风控/撮合拒绝时 `accepted=false` 且 `result_code` 为拒绝码（§3.4）。**语义修正通告**：v1 中 action `"1"` 曾映射为卖出，v2 起按 `1=买 / 2=卖` 执行，联调方需同步调整。

#### 查询接口

| 方法/路径 | 响应 |
|-----------|------|
| `GET /orders/:order_id` | `OrderRecord`（§4.6）：状态、成交明细（含对手方回填）、冻结参数；无记录 404 |
| `GET /accounts/:uid/balances` | 扁平映射 `{"840": 999500}`，键为币种（`currency`），值为余额镜像 |
| `GET /trades?symbol=&limit=` | `TradeView` 数组（内存环形缓冲，按时间正序）；`limit` 缺省 100、上限 1000 |
| `GET /admin/state` | `{"orders_total","trades_total","registry_size","balance_sums"}`，对账用 |
| `GET /health` | `{"status":"ok","exchange":"sse"}`；核心命令通道已关闭时返回 503 `core_unavailable` |


### 5.2 SSE 二进制协议（网关，阶段二）

- 传输：TCP，监听 `gateway_port`（配置，规划 9010）；字节序大端；帧格式见 §4.7.1。
- 消息字段定义以 `sse-binary` crate 结构体为准（Logon/NewOrderSingle/OrderCancel/Confirm/CancelReject/Report/OrderReject/PlatformState/ExecRpt*），关键 inbound 报文字段表见 §4.7.3，outbound 填充规则见 §4.7.4。
- 测试驱动：`testcase/sse/gw-auto-sse.toml` 定义 OMS 模拟端（`server_address=localhost:9010`，`protocol="binary-sse"`）；场景步骤格式见 ARCHITECTURE §9.2。

---

## 6. 错误处理设计

### 6.1 错误分类

| 层 | 类型 | 处理策略 |
|----|------|----------|
| 接入层 | 参数/校验错误 | 同步拒绝（400/422），不产生核心命令 |
| 接入层 | 通道/超时错误 | 503/504，命令可能已入队（幂等层防重放） |
| 核心层 | 业务拒绝 | 权威结果码（§3.4），原样透传给调用方并映射协议错误码 |
| 基础数据 | 文件/格式错误 | **启动失败退出**（fail-fast），错误含文件/行号/列 |
| 系统 | 配置/绑定失败 | 启动失败退出，`expect` 带上下文说明 |

### 6.2 核心结果码 → HTTP 映射（接入层实现）

| CommandResultCode | HTTP 状态 | accepted | 备注 |
|-------------------|-----------|----------|------|
| `Success` / `Accepted` | 200 | true | — |
| `AuthInvalidUser` | 200 | false | 业务拒绝统一 200（受理成功但订单被拒），与交易所语义一致 |
| `RiskNsf` 等 Risk* | 200 | false | — |
| `Matching*` / `InvalidSymbol` / `UserMgmt*` | 200 | false | — |

### 6.3 内部错误类型

【现状（2026-10-07）】`src/errors.rs` 实现如下（`Data` 类错误在引导阶段以 `anyhow::Result` + `Context` fail-fast 处理，不进入本枚举）：

```rust
pub enum SimError {
    BadRequest { status: StatusCode, detail: String },  // → 400；提取器拒绝沿用原状态码（415/422）
    OrderNotFound,                                      // → 404（订单查询/撤单目标不存在）
    Validation { fields: BTreeMap<String, String> },    // → 422（字段级错误明细）
    DuplicateOrderId,                                   // → 409
    CancelInFlight,                                     // → 409（同一订单撤单在途）
    CoreUnavailable,                                    // → 503
    CoreTimeout,                                        // → 504
}
```

`JsonRejection`/`PathRejection`/`QueryRejection` 经 `From` 归一为 `BadRequest`，因此接入层任何失败都是 `{error, detail?}` 的 JSON 形状（§5.1）。

### 6.4 核心结果码 → SSE `ord_rej_reason` 映射（设计值，模拟器自定义，集中常量定义）

| CommandResultCode | ord_rej_reason |
|-------------------|----------------|
| `AuthInvalidUser` | 1001 |
| `RiskNsf` | 1002 |
| `InvalidSymbol` / `MatchingInvalidOrderBookId` | 1003 |
| `MatchingInvalidOrderSize` | 1004 |
| `MatchingUnknownOrderId`（撤单） | 1005 |
| 重复 order_id（网关幂等层） | 1006 |
| 报文字段非法（适配失败） | 1007 |
| 未登录收到业务报文 | 1008 |
| 其他/未知 | 1999 |

---

## 7. 日志与可观测性设计

### 7.1 初始化（logging.rs）

- `tracing_subscriber::fmt` + `EnvFilter`：`[logging].level`（默认 `info`），环境变量 `RUST_LOG` 优先；
- 格式：默认单行文本（时间 级别 目标 事件 字段）；不引入 JSON 依赖（本地工具定位）。

### 7.2 埋点规范

| 事件 | 级别 | 目标（target） | 关键字段 |
|------|------|----------------|----------|
| 配置加载完成 | info | `startup` | config 路径、启用的交易所 |
| 标的/账户加载 | info | `bootstrap` | 各自条数、文件路径（逐条明细 debug） |
| 引导命令结果非 Success | **error** | `bootstrap` | uid、command、result_code（启动失败） |
| HTTP 请求 | span | `http` | method、path、status、耗时 ms |
| 下单受理/拒绝 | info / warn | `order` | order_id、uid、symbol、result_code |
| 成交 | debug | `match` | 买卖 order_id、price、size |
| 会话建立/断开（阶段二） | info | `session` | peer、comp_id、原因 |
| 报文解码失败（阶段二） | warn | `sse_codec` | msg_type、seq、偏移 |
| 结果通道丢弃 | warn（限频） | `core_bridge` | dropped 计数 |

### 7.3 指标（阶段三，预留）

进程内 `AtomicU64` 计数器集（orders_total / orders_rejected / trades_total / last_latency_us），经 `GET /metrics`（文本格式）暴露；不引入 prometheus 依赖。

---

## 8. 测试设计

### 8.1 分层策略

| 层 | 范围 | 工具 | 位置 |
|----|------|------|------|
| L1 单元 | DTO 校验、action/order_type 映射、CSV 解析、错误映射、帧编解码 | `cargo test` + `#[test]` | 各模块 `#[cfg(test)]` |
| L2 集成 | 装配 + 下单→结果回传 + 余额变更断言 | `#[tokio::test]` + LocalSet | `tests/` |
| L3 场景 | gt-auto 驱动 SSE 协议全流程（阶段二后有效） | `./autotest.sh` | `testcase/sse/` |

### 8.2 L1 用例矩阵（必须覆盖）

| 模块 | 用例 |
|------|------|
| model | action=1/2/其他；order_type 每个合法串与非法串；size/price=0 负数；reserve_price < price |
| base_info_service | 正常加载；表头在最前/注释在最前；CRLF；列数不足；字段非数字（断言 Err 与行号）；symbol_id 重复；uid 重复；空文件 |
| errors | 结果码→HTTP 映射、结果码→ord_rej_reason 映射完整性（枚举穷举） |
| codec（阶段二） | 单帧编解码往返；两帧粘包；半包分片；未知 msg_type 跳过；body_len 越界防护 |
| adapter（阶段二） | NOS→命令全字段映射；side 非法；cl_ord_id 非数字 |

### 8.3 L2 集成用例

1. **下单挂单**：uid=10001 买 600000@1000×100（无对手盘）→ `Success`，`leaves_qty=100`，余额减少 `100×1000×1+100×3=100300`；
2. **成交结算**：10002 卖同标的@1000×100 → 双方 `Success`、各 1 笔 trade；断言：10001 余额返还差价 0、得 base `100×100`；10002 收 `100×1000×1−100×3`；
3. **资金不足**：uid=10009（余额 200000）买 600000@1000×1000 → `RiskNsf`，余额不变；
4. **重复 order_id** → 409；
5. **超时/不可用**：停掉核心任务后下单 → 503（构造：drop command_tx）。

> L2 前提：`main.rs` 装配逻辑抽为可调用函数（§4.1 `bootstrap_exchange`），测试用临时 CSV fixture（`#[cfg(test)]` 内嵌字符串写 tempdir，不依赖仓库相对路径）。

### 8.4 L3 场景用例规划（testcase/sse）

| case | 步骤 | 验证点 |
|------|------|--------|
| sse_001（已有） | Logon→新订单×2→Confirm×2→Report×2 | 登录回执、受理确认、成交回报字段 |
| sse_002（新增） | 登录→下单→撤单→Confirm(撤) | 撤单链路 |
| sse_003（新增） | 登录→下单(超额)→OrderReject(1002) | 拒单与错误码 |
| sse_004（新增） | 登录→心跳×N→下单 | 心跳保活 |

### 8.5 CI（GitHub Actions，阶段一收尾）

`cargo fmt --check` → `cargo clippy --all-targets -- -D warnings` → `cargo test` → `cargo build --release`；git 依赖拉取使用 `CARGO_NET_GIT_FETCH_WITH_CLI=true`。

---

## 9. 开发规范与工程约定

1. **格式与静态检查**：rustfmt 默认配置；clippy 告警零容忍（CI 拦截）。
2. **依赖管理**：
   - git 依赖一律 tag 固定（`{ git = "...", tag = "vX.Y.Z" }`），**Cargo.lock 必须提交**；
   - 升级流程：改 Cargo.toml tag → `cargo update -p <crate>` → 跑全量测试 → 在 PR 中附依赖 changelog 要点；
   - 不引入未使用依赖（当前 `bincode`/`bigdecimal` 待清理，见 ARCHITECTURE P2-1）。
3. **错误处理**：库代码 `SimResult<T>`/`anyhow`，禁止 `unwrap_or(0)` 吞错；入口（main）允许 `expect` 但必须带上下文文案。
4. **常量集中**：协议枚举值（side/ord_type/ord_rej_reason/exec_type）统一放 `errors.rs` / `sse_gateway::consts`，禁止魔法值散落。
5. **文档同步**：接口/字段/错误码变更必须同次 PR 更新 `docs/ARCHITECTURE.md` 与本文档对应表格。
6. **提交信息**：`feat|fix|refactor|test|docs|chore: 摘要`；接口语义变更（如 action 映射修正）需在提交与两份文档中显式标注 BREAKING。
7. **路径与平台**：一律相对 crate 根的 POSIX 风格路径；测试内使用 `env!("CARGO_MANIFEST_DIR")` 拼接（修复 P2-5）。

---

## 10. 任务拆解（WBS）

对应 ARCHITECTURE.md §14 阶段一、二（估算：1 名开发）：

### 阶段一：HTTP 链路夯实（约 5.5 人日）✅ 已完成（2026-10-06，T1.1~T1.7 全部落地）

| ID | 任务 | 产出 | 估算 | 依赖 |
|----|------|------|------|------|
| T1.1 | `model.rs` 校验 + action/order_type/reserve_price 映射修正 | §4.2 全量 + L1 用例 | 0.5d | — |
| T1.2 | `logging.rs` 初始化 + §7.2 埋点 | 日志可见 | 0.5d | — |
| T1.3 | `config.rs` 接入 main + CLI + config.toml 更新 | 端口/路径可配 | 1d | T1.2 |
| T1.4 | `base_info_service.rs` fail-fast 重构 | §4.4 + L1 用例 | 0.5d | — |
| T1.5 | `core_bridge.rs`（结果路由/幂等/快照）+ `/place` v2 + 查询接口 | §4.6、§5.1 + L2 用例 | 1.5d | T1.1 |
| T1.6 | 优雅停机 + 启动自检汇总日志 | §4.1 | 0.5d | T1.3 |
| T1.7 | L1/L2 用例补齐 + CI 流水线 + 依赖清理 | §8、§9 | 1d | 全部 |

### 阶段二：SSE 协议网关（约 9 人日）✅ 已完成（2026-10-06，T2.1~T2.5 全部落地，gt-auto 4 场景 14 步校验全绿）

| ID | 任务 | 产出 | 估算 | 依赖 |
|----|------|------|------|------|
| T2.1 | `sse_gateway/codec.rs` 帧编解码 + 防护用例 | §4.7.1 | 1d | T1.7 |
| T2.2 | `session.rs` 会话状态机/心跳/序列号 | §4.7.2 | 2d | T2.1 |
| T2.3 | `adapter.rs` 入站报文→命令 | §4.7.3 | 1.5d | T2.2 |
| T2.4 | `report.rs` 回报生成 + OrderRegistry | §4.7.4/4.7.5 | 2.5d | T2.3 |
| T2.5 | gt-auto 联调：sse_001 打通，补 sse_002~004 | §8.4 | 2d | T2.4 |

> 阶段三（多市场实例化、WAL/快照恢复）在完成上述两阶段后另行细化设计。

### 阶段三：多市场与状态持久化（约 9 人日，已拆解待实施）

**前置调研结论（2026-10-06）**：

1. 多实例装配循环已就绪（`main.rs` 按 `enabled` 遍历市场），SZSE 数据（标的 000xxx / 账户 20001+）与 SSE（600xxx / 10001+）无 ID 冲突，可并行实例化；
2. **matching-core 的 WAL 无轮转/截断 API**（`Journaler` 恒为追加写，`read_commands` 读全量）：恢复不能"快照+清 WAL"，必须实现**快照 seq 过滤重放**——快照以命令序号为文件名（`snapshot_{seq}.bin`，`get_latest_seq_id()` 可查），重启时读全量 WAL 并跳过前 `seq` 条命令只重放增量，这是崩溃安全的关键；
3. `load_latest_snapshot` 内部走 `from_state`，产出的新核心 **producer 为空且 journaler/snapshot_store 被清空**：恢复后必须重新 enable journaling/snapshotting 并再次 `startup()`，且恢复路径要**跳过 CSV 引导**（否则标的/资金双重登记）；
4. gt-auto 支持 `binary-szse` 协议（`SzseBinFramer`：8B 头 + 4B 校验和，与 SSE 帧格式不同），SZSE 报文体系独立（`fin-proto-szse-bin-rs`：NewOrder/ExecutionConfirm/ExecutionReport/CancelReject/BusinessReject/extend_* 扩展族）。

| ID | 任务 | 内容/产出 | 估算 | 依赖 |
|----|------|-----------|------|------|
| T3.1 | 持久化配置与接入骨架 ✅ 已完成（2026-10-07） | `[sse.matcher.persistence]` 新配置段（`wal_path`，缺省关闭）；`bootstrap_exchange` 中 `enable_journaling` 接线；~~命令序号计数器与周期快照~~（见下方设计调整） | 0.5d | — |
| T3.2 | 恢复语义与启动编排（核心难点）✅ 已完成（2026-10-07，纯 WAL 重放方案） | **设计调整**：上游 `serialize_state` 在 `startup()` 后 panic（pipeline 被 move 进 Disruptor 闭包），**运行期/停机时均无法打快照**——恢复方案从「快照+增量重放」调整为**纯 WAL 全量重放**：启动时 WAL 存在且非空 → `replay_journal` 全量重放（结果消费者自然重建余额镜像/订单注册表）并**跳过 CSV 账户引导**（标的仍从 CSV 注册，因其不进 WAL）；全新启动 → CSV 引导（命令照常入 WAL）。Journaler 每条命令写后即 flush，`kill -9` 安全（截断尾帧自动忽略）；WAL 重放失败 fail-fast。L2 恢复测试 case7：冻结跨重启恢复 + 恢复后订单簿继续撮合；真机 kill -9 冒烟通过 | 2.5d | T3.1 |
| T3.3 | 订单/成交流水持久化 + 对账查询 ✅ 已完成（2026-10-07） | `[matcher.persistence].orders_log_path`：订单/撤单终态 JSONL 追加审计（写失败仅告警）；成交环形缓冲（1 万条）+ 计数器；`GET /trades?symbol=&limit=`、`GET /admin/state`（orders/trades 计数、注册表规模、各币种余额合计）。测试 case8：审计文件内容、环形缓冲过滤、汇总断言 | 1.5d | T3.2 |
| T3.4 | SZSE 协议网关（可并行）✅ 已完成（2026-10-06，gt-auto binary-szse 4 场景 10 步校验全绿） | 引入 `szse-binary` v0.1.0；`src/szse_gateway/`（帧 8B 头适配、宽松登录、adapter/report 码值差异）；`[szse]` 启用（HTTP 8081 / 网关 9011，`gateway_protocol="szse"` 分派）；SZSE 场景 `testcase/szse/*`；**附带修复**：core_bridge 余额镜像补上撤单全额解冻（OrderRecord 新增 reserve_price；上游 matching-core 撤单 Reject 事件 bidder_hold_price=0 导致 R2 只退手续费不退本金，镜像按业务正确语义处理并已记录差异） | 3.5d | —（与 T3.1~T3.3 并行） |

> **T3.4 实施备注**：
> 1. SZSE 会话为**宽松登录**（业务报文不强制先登录，收到 Logon 幂等应答）——gt-auto 参考场景无 Logon 步骤；
> 2. gt-auto 的 Go 侧对 ExecutionReport 不构建 ApplExtend 期望（恒为 nil），而我们发出的报文解码后扩展非 nil，深比较必不等——因此场景中 ExecutionReport 步骤为 `verify_required=N`（只消费不校验），成交回报字段断言由 Rust 端到端测试覆盖；
> 3. SZSE 拒单使用 BusinessReject(4)（business_reject_reason 复用 §6.4 码值），撤单失败用 CancelReject(290008)；
> 4. SZSE 撤单报文无账户字段，uid 从网关订单注册表回查。
| T3.5 | 多实例并存验证 + 文档收尾 ✅ 已完成（2026-10-07） | 双市场（SSE 14✅ + SZSE 10✅）单命令回归（`autotest.sh` 纳入 SZSE 并前置清理 WAL/审计文件）；恢复演练脚本 `scripts/recovery_drill.sh`（before/after 两阶段：kill -9 前后余额一致性 + 订单簿恢复验证）；文档更新 | 1d | T3.2、T3.3、T3.4 |

**实现前需确认的决策**：

| # | 决策点 | 推荐 |
|---|--------|------|
| 1 | 恢复一致性策略 | ~~快照 seq 过滤重放~~ **已调整（2026-10-07）**：上游快照仅在 startup 前可用（serialize_state 于 startup 后 panic），故采用**纯 WAL 全量重放**；代价是 WAL 随重启累积、恢复时间线性增长，待上游支持运行期快照后再引入周期快照+增量重放 |
| 2 | WAL 中命令范围 | 全量命令（含撤单/改单）——重放即恢复，无需区分 ✅ |
| 3 | 流水持久化格式 | JSONL 追加写（零新依赖）✅；如需 SQL 对账再引入 SQLite |
| 4 | SZSE 适配范围 | 最小集：NewOrder / OrderCancelReq / ExecutionConfirm / ExecutionReport / CancelReject / BusinessReject，extend_* 族按场景需要逐个补 ✅ |
| 5 | SZSE 码值校验 | Confirm 字段回填沿用 SSE 校准结论（ReportIndex 从 0 计、流水 ID 留空）；ExecutionReport 因 gt-auto ApplExtend 期望为 nil 仅消费不校验（字段断言由 Rust E2E 覆盖）✅ |
