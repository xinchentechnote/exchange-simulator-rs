#!/usr/bin/env bash
# 模拟撮合服务管理脚本：启动 / 停止 / 重启 / 故障恢复 / 状态查看
#
# 用法:
#   scripts/sim.sh [--config <path>] <command> [--clean]
# 命令:
#   start     启动服务（--clean 先清理 WAL 与审计流水，全新启动）
#   stop      优雅停止（SIGINT 触发三阶段停机，超时后 SIGKILL 兜底）
#   restart   重启（stop + start；默认保留 WAL，重启即重放恢复）
#   recover   故障恢复：服务停止/崩溃后的拉起（不主动杀进程）。
#             WAL 存在即自动全量重放（应用内建行为），输出重放证据与对账汇总
#   drill     崩溃演练：kill -9 模拟崩溃 → 拉起 → WAL 重放 →
#             对比崩溃前后 /admin/state 对账
#   status    查看运行状态、监听端口与健康探针
# 说明:
#   * PID 记录于 data/sim.pid，日志追加写入 data/sim.log；
#   * stop 对非本脚本启动的实例同样生效（按进程名兜底查找）；
#   * 默认配置 conf/config.toml，可用 --config 或环境变量 SIM_CONFIG 覆盖。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${SIM_CONFIG:-$ROOT/conf/config.toml}"
BIN="$ROOT/target/debug/exchange-simulator-rs"
PID_FILE="$ROOT/data/sim.pid"
LOG_FILE="$ROOT/data/sim.log"
STOP_TIMEOUT=20          # 三阶段优雅停机最长约 15s，超时后再强杀
READY_TIMEOUT=15

CLEAN=0
COMMAND=""

# ───────────────────────── 参数解析 ─────────────────────────
while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    --clean)  CLEAN=1; shift ;;
    start|stop|restart|recover|drill|status) COMMAND="$1"; shift ;;
    *) echo "未知参数: $1"; usage; exit 2 ;;
  esac
done
[ -n "$COMMAND" ] || { usage; exit 2; }

usage() {
  sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

log()  { echo "[sim] $*"; }
fail() { echo "[sim] ❌ $*" >&2; exit 1; }

# ───────────────────────── 配置解析 ─────────────────────────
# 输出启用市场的行：market http_port gateway_port protocol（未配网关端口时为 0）
market_rows() {
  awk '
    { sub(/#.*/, "") }
    /^\[(sse|szse)\][ \t]*$/ { sec = $0; gsub(/[\[\] \t]/, "", sec) }
    sec && $1 == "enabled"          { en[sec] = $3 }
    sec && $1 == "server_port"      { sp[sec] = $3 }
    sec && $1 == "gateway_port"     { gp[sec] = $3 + 0 }
    sec && $1 == "gateway_protocol" { gsub(/"/, "", $3); pr[sec] = $3 }
    END {
      split("sse szse", order, " ")
      for (i = 1; i <= 2; i++) {
        m = order[i]
        if (m in en && en[m] == "true")
          printf "%s %s %s %s\n", m, sp[m], gp[m]+0, (pr[m] == "" ? "-" : pr[m])
      }
    }
  ' "$CONFIG"
}

# ───────────────────────── 进程管理 ─────────────────────────
# 查找服务进程：优先 pidfile（校验进程身份防 PID 复用误判），否则按进程名兜底
find_pids() {
  local pids=""
  if [ -f "$PID_FILE" ]; then
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null \
       && ps -p "$pid" -o command= 2>/dev/null | grep -q "exchange-simulator-rs"; then
      pids="$pid"
    fi
  fi
  if [ -z "$pids" ]; then
    pids=$(pgrep -f "exchange-simulator-rs --config" 2>/dev/null || true)
  fi
  echo "$pids"
}

is_running() { [ -n "$(find_pids)" ]; }

wait_gone() {
  local pids="$1" deadline=$((SECONDS + STOP_TIMEOUT))
  while [ $SECONDS -lt $deadline ]; do
    local alive=""
    for pid in $pids; do kill -0 "$pid" 2>/dev/null && alive="$alive $pid"; done
    [ -z "$alive" ] && return 0
    sleep 0.5
  done
  return 1
}

listeners_of() { lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null || true; }

ensure_bin() {
  if [ ! -x "$BIN" ]; then
    log "二进制不存在，执行 cargo build ..."
    (cd "$ROOT" && cargo build) || fail "构建失败"
  fi
}

# 启动前检查配置端口未被占用（列出占用者 PID）
check_ports_free() {
  local busy=""
  while read -r m http gw proto; do
    for p in $http $gw; do
      [ "$p" = "0" ] && continue
      local pids; pids=$(listeners_of "$p")
      [ -n "$pids" ] && busy="$busy $m/$p(PID:${pids// /,})"
    done
  done <<< "$(market_rows)"
  if [ -n "$busy" ]; then
    fail "端口已被占用:$busy —— 先 stop 或 sim.sh restart"
  fi
}

wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT)) ok
  while [ $SECONDS -lt $deadline ]; do
    ok=1
    while read -r m http gw proto; do
      curl -sf -m 2 "http://127.0.0.1:$http/health" > /dev/null 2>&1 || ok=0
    done <<< "$(market_rows)"
    [ "$ok" = 1 ] && return 0
    sleep 0.5
  done
  return 1
}

start_one() {
  ensure_bin
  check_ports_free
  if [ "$CLEAN" = 1 ]; then
    rm -f "$ROOT"/data/{sse,szse}/wal.log "$ROOT"/data/{sse,szse}/orders.jsonl
    log "已清理 WAL 与审计流水（全新启动）"
  fi
  local wal_bytes=0
  for w in "$ROOT"/data/{sse,szse}/wal.log; do
    [ -f "$w" ] && wal_bytes=$((wal_bytes + $(wc -c < "$w" | tr -d ' ')))
  done
  [ "$wal_bytes" -gt 0 ] && log "检测到 WAL（$wal_bytes 字节），启动后将全量重放恢复余额冻结与订单簿"

  cd "$ROOT"
  echo "===== $(date '+%F %T') start (config=$CONFIG) =====" >> "$LOG_FILE"
  nohup "$BIN" --config "$CONFIG" >> "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  if wait_ready; then
    log "服务已就绪（PID $(cat "$PID_FILE")，日志 ${LOG_FILE}）"
  else
    local pid; pid=$(cat "$PID_FILE")
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      tail -n 5 "$LOG_FILE" | sed 's/^/      /'
      fail "进程启动后立即退出，见上方日志"
    fi
    fail "健康探针 ${READY_TIMEOUT}s 内未就绪，检查 $LOG_FILE"
  fi
}

# ───────────────────────── 命令实现 ─────────────────────────
cmd_start() {
  if is_running; then
    log "服务已在运行（PID $(find_pids)），无需启动"; return 0
  fi
  [ -f "$CONFIG" ] || fail "配置不存在: $CONFIG"
  start_one
}

cmd_stop() {
  local pids; pids=$(find_pids)
  if [ -z "$pids" ]; then
    rm -f "$PID_FILE"
    log "服务未运行"; return 0
  fi
  # 应用以 tokio::signal::ctrl_c 处理停机，发 SIGINT 走优雅停机三阶段
  for pid in $pids; do kill -INT "$pid" 2>/dev/null || true; done
  if wait_gone "$pids"; then
    log "已优雅停止（PID:${pids// /,}）"
  else
    log "优雅停机超时，强制 SIGKILL"
    for pid in $pids; do kill -9 "$pid" 2>/dev/null || true; done
    sleep 1
  fi
  rm -f "$PID_FILE"
}

cmd_restart() {
  cmd_stop
  echo
  cmd_start
}

cmd_recover() {
  # 故障恢复：服务停止/崩溃后的拉起。是否重放 WAL 由应用决定（WAL 存在且非空
  # 即全量重放并跳过 CSV 账户引导），本命令不主动杀进程、不清理状态。
  CLEAN=0
  if is_running; then
    log "服务已在运行（PID $(find_pids)），无需恢复；如需主动验证恢复链路，用 drill 命令"
    return 0
  fi
  [ -f "$CONFIG" ] || fail "配置不存在: $CONFIG"
  local wal_bytes=0
  for w in "$ROOT"/data/{sse,szse}/wal.log; do
    [ -f "$w" ] && wal_bytes=$((wal_bytes + $(wc -c < "$w" | tr -d ' ')))
  done
  if [ "$wal_bytes" -eq 0 ]; then
    log "无 WAL（被 --clean 清理过或从未运行），将全新启动"
  fi
  local log_mark=0
  [ -f "$LOG_FILE" ] && log_mark=$(wc -l < "$LOG_FILE" | tr -d ' ')
  start_one
  if [ "$wal_bytes" -gt 0 ] \
     && tail -n +$((log_mark + 1)) "$LOG_FILE" | grep -q "WAL 重放完成"; then
    log "WAL 重放确认 ✅（本次启动日志出现「WAL 重放完成」）"
  fi
  local http
  http=$(market_rows | head -1 | awk '{print $2}')
  log "恢复后对账汇总: $(curl -sf -m 2 "http://127.0.0.1:$http/admin/state" 2>/dev/null || echo 不可达)"
}

cmd_drill() {
  # 崩溃演练：kill -9 模拟崩溃 → 拉起（WAL 自动重放）→ 前后对账。
  CLEAN=0
  local pids state_before=""
  pids=$(find_pids)
  if [ -n "$pids" ]; then
    state_before=$(curl -sf -m 2 "http://127.0.0.1:$(market_rows | head -1 | awk '{print $2}')/admin/state" 2>/dev/null || true)
    log "kill -9 模拟进程崩溃（PID:${pids// /,}）"
    for pid in $pids; do kill -9 "$pid" 2>/dev/null || true; done
    rm -f "$PID_FILE"
    sleep 1
  else
    log "服务未运行，直接拉起（无崩溃前对账快照）"
  fi
  start_one
  local http state_after
  http=$(market_rows | head -1 | awk '{print $2}')
  state_after=$(curl -sf -m 2 "http://127.0.0.1:$http/admin/state" 2>/dev/null || true)
  if [ -n "$state_before" ]; then
    if [ "$state_before" = "$state_after" ]; then
      log "崩溃前后对账一致 ✅ $state_after"
    else
      log "崩溃前后对账不一致 ❌"
      echo "  before: $state_before"
      echo "  after : $state_after"
      exit 1
    fi
  else
    log "崩溃前无对账快照（崩溃前服务不可达），恢复后状态: $state_after"
  fi
}

cmd_status() {
  local pids; pids=$(find_pids)
  if [ -z "$pids" ]; then
    echo "状态: stopped"
  else
    local pid; pid=$(echo "$pids" | awk '{print $1}')
    echo "状态: running  PID: $(echo "$pids" | tr '\n' ' ' | sed 's/ $//')  运行时长: $(ps -p "$pid" -o etime= 2>/dev/null | tr -d ' ')"
  fi
  echo "配置: ${CONFIG#$ROOT/}  日志: ${LOG_FILE#$ROOT/}"
  echo
  printf "%-6s %-10s %-10s %-6s %-8s %s\n" "市场" "HTTP端口" "协议网关" "协议" "监听" "健康探针"
  while read -r m http gw proto; do
    local listen="✗" health="✗"
    if [ -n "$(listeners_of "$http")" ] && [ -n "$(listeners_of "$gw")" ]; then
      listen="✓"
    elif [ -n "$(listeners_of "$http")$(listeners_of "$gw")" ]; then
      listen="部分"
    fi
    if [ "$listen" != "✗" ]; then
      local body; body=$(curl -sf -m 2 "http://127.0.0.1:$http/health" 2>/dev/null || true)
      [ -n "$body" ] && health="$body"
    fi
    [ "$gw" = "0" ] && gw="-"
    printf "%-6s %-10s %-10s %-6s %-8s %s\n" "$m" "$http" "$gw" "$proto" "$listen" "$health"
  done <<< "$(market_rows)"
}

case "$COMMAND" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_restart ;;
  recover) cmd_recover ;;
  drill)   cmd_drill ;;
  status)  cmd_status ;;
esac
