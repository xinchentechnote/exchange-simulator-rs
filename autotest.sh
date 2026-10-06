#!/bin/bash
# 场景测试假定全新状态：先清理 WAL（否则跨重启重放会导致订单号重复被拒）
rm -f data/sse/wal.log data/szse/wal.log
gt-auto --config testcase/sse/gw-auto-sse.toml --casePath testcase/sse/sse_test_case.csv
