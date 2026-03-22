#!/bin/bash
cd /root/fredmonitor

# 1. 同步 FRED 数据
python3 scripts/sync_fred_http.py --before-report >> /root/fredmonitor/logs/sync.log 2>&1

# 2. 重算 YoY 派生指标
python3 scripts/calculate_yoy_indicators.py >> /root/fredmonitor/logs/yoy.log 2>&1

# 3. 生成报告（跳过 AI 叙事，跳过 Yahoo 图表失败不中断）
CRISIS_MONITOR_SKIP_AI_NARRATOR=1 python3 crisis_monitor_v2.py >> /root/fredmonitor/logs/report.log 2>&1

echo "完成: $(date)" >> /root/fredmonitor/logs/report.log
