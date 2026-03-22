#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速测试「深度宏观推演」AI 接口（仅用模拟数据，不跑数据管道）。"""
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
from dotenv import load_dotenv
load_dotenv(BASE)

# 仅导入 LLM 函数，避免加载整份 crisis_monitor 的数据与评分
from crisis_monitor import generate_narrative_with_llm

# 模拟数据，仅用于验证 API 调用
MOCK_INDICATORS = [
    {"series_id": "GDP", "name": "Real GDP", "current_value": 28000, "risk_score": 25},
    {"series_id": "PAYEMS", "name": "Nonfarm Payrolls", "current_value": 158000, "risk_score": 30},
    {"series_id": "VIXCLS", "name": "VIX", "current_value": 14.5, "risk_score": 20},
    {"series_id": "UMCSENT", "name": "Consumer Sentiment", "current_value": 65, "risk_score": 75},
    {"series_id": "BAMLH0A0HYM2", "name": "HY OAS", "current_value": 3.8, "risk_score": 55},
]
MOCK_WARSH = {"rrp_level_bn": 45, "sofr_iorb_spread": 0.02, "term_premium": 0.5}

def main():
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        print("⚠️ 未设置 DASHSCOPE_API_KEY（在项目根目录 .env 中配置）")
    print("🤖 使用模拟数据调用大模型...\n")
    result = generate_narrative_with_llm(MOCK_INDICATORS, MOCK_WARSH, api_key=api_key)
    print("=" * 60)
    print(result)
    print("=" * 60)
    print("\n✅ 若上面为 AI 研报内容则接口正常。完整报告: py crisis_monitor.py")

if __name__ == "__main__":
    main()
