#!/usr/bin/env python3
"""Task 2: 查询 BAA10YM、CPN3M 在 FRED 上的最新 3 条观测，不修改任何文件。"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.fred_http import series_observations

for sid in ["BAA10YM", "CPN3M"]:
    resp = series_observations(sid, observation_start="2026-01-01")
    obs = resp.get("observations", [])
    print(f"\n{sid} 最新观测（从 2026-01-01 起，取最后 3 条）：")
    for o in obs[-3:]:
        print(f"  {o.get('date')}: {o.get('value')}")
    if not obs:
        print("  (无数据)")
