#!/usr/bin/env python3
"""Task 1: 验证 DFII5/T5YIE 新鲜度与 ^FVX 可拉取，不修改任何文件。"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

print("=== FRED DFII5 / T5YIE 最新 3 条 ===")
from scripts.fred_http import series_observations
for sid in ["DFII5", "T5YIE"]:
    try:
        resp = series_observations(sid, observation_start="2026-01-01")
        obs = resp.get("observations", [])
        print(f"\n{sid} 最新 3 条：")
        for o in (obs[-3:] if len(obs) >= 3 else obs):
            print(f"  {o.get('date')}: {o.get('value')}")
        if not obs:
            print("  (无数据)")
    except Exception as e:
        print(f"\n{sid} error: {e}")

print("\n=== Yahoo ^FVX 可拉取性 ===")
import yfinance as yf
try:
    df = yf.download("^FVX", period="5d", progress=False)
    if df is not None and len(df) > 0:
        print(f"^FVX 最新日期: {df.index[-1].date()}")
        print(df.tail(3)[["Close"]])
    else:
        print("^FVX 无数据")
except Exception as e:
    print(f"^FVX error: {e}")
