#!/usr/bin/env python3
"""快速检查 4 个 LOCAL_SYNC_MISSING 的 FRED 上游是否有更新（只查 4 个，几秒完成）。"""
import os
import sys
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
os.chdir(BASE)

def local_last(series_id: str) -> str | None:
    p = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        lines = [l for l in f.readlines() if l.strip()][1:]
    if not lines:
        return None
    return lines[-1].split(",")[0].strip()

def fred_latest(series_id: str) -> tuple[str | None, str]:
    try:
        from scripts.fred_http import series_observations
    except Exception as e:
        return None, str(e)
    try:
        start = (datetime.now().replace(year=datetime.now().year - 1)).strftime("%Y-%m-%d")
        r = series_observations(series_id, observation_start=start)
        obs = r.get("observations") or []
        dates = [o.get("date") for o in obs if o.get("date")]
        return (max(dates), "") if dates else (None, "no observations")
    except Exception as e:
        return None, str(e)

def main():
    # 4 个 LOCAL_SYNC_MISSING
    ids = ["CPN3M", "BAA10YM", "DTWEXBGS", "THREEFYTP10"]
    print("Checking FRED upstream for 4 LOCAL_SYNC_MISSING series...")
    for sid in ids:
        local = local_last(sid)
        up, err = fred_latest(sid)
        if err and not up:
            print(f"  {sid}: upstream error — {err}")
            continue
        can = "能" if (up and local and up > local) else "不能"
        print(f"  {sid}: local_last={local}  upstream_last={up}  => {can}下载到更新")

if __name__ == "__main__":
    main()
