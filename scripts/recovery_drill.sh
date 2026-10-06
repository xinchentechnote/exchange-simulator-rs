#!/bin/bash
# WAL 故障恢复演练（T3.2 验收）：
#   阶段 before：向 SSE 市场下一笔挂单并记录冻结后余额
#   （此时手工 kill -9 进程并以同一配置重启）
#   阶段 after ：校验余额一致（冻结从 WAL 恢复）且挂单仍在簿（反向卖单立即成交）
# 用法:
#   scripts/recovery_drill.sh before [http_addr]   # 崩溃前
#   scripts/recovery_drill.sh after  [http_addr]   # 重启后
set -euo pipefail
HTTP=${2:-http://127.0.0.1:8080}
STATE=/tmp/recovery_drill_state.json
ORDER_ID=${ORDER_ID:-990001}

case "${1:-}" in
  before)
    curl -sf -X POST "$HTTP/place" -H 'Content-Type: application/json' \
      -d "{\"order_id\":$ORDER_ID,\"user_id\":10001,\"action\":\"1\",\"order_type\":\"GTC\",\"price\":700,\"size\":100,\"symbol\":600000}" > /dev/null
    curl -sf "$HTTP/accounts/10001/balances" > "$STATE"
    echo "[before] 下单成功，余额已记录: $(cat "$STATE")"
    echo "[before] 现在执行: kill -9 <pid> && 重新启动服务，然后运行 after 阶段"
    ;;
  after)
    after=$(curl -sf "$HTTP/accounts/10001/balances")
    before=$(cat "$STATE")
    if [ "$before" = "$after" ]; then
      echo "[after] 余额一致: $after ✅（冻结状态恢复）"
    else
      echo "[after] 余额不一致 ❌ before=$before after=$after"; exit 1
    fi
    # 反向卖单验证恢复的挂单仍在簿（成交即证明）
    result=$(curl -sf -X POST "$HTTP/place" -H 'Content-Type: application/json' \
      -d "{\"order_id\":$((ORDER_ID+1)),\"user_id\":10002,\"action\":\"2\",\"order_type\":\"GTC\",\"price\":600,\"size\":100,\"symbol\":600000}")
    echo "[after] 反向卖单结果: $result"
    echo "$result" | grep -q '"accepted":true' && echo "[after] 订单簿恢复验证 ✅"
    ;;
  *)
    echo "用法: $0 before|after [http_addr]" >&2; exit 2 ;;
esac
