#!/bin/bash
# 双市场场景回归（假定服务已在 9010/9011 双网关启动；WAL 会在运行前清理以保证全新状态）
set -e
rm -f data/sse/wal.log data/szse/wal.log data/sse/orders.jsonl data/szse/orders.jsonl

echo "=== SSE 场景 ==="
gt-auto --config testcase/sse/gw-auto-sse.toml --casePath testcase/sse/sse_test_case.csv

echo "=== SZSE 场景 ==="
gt-auto --config testcase/szse/gw-auto-szse.toml --casePath testcase/szse/szse_test_case.csv

echo "SSE + SZSE 场景全部通过"
