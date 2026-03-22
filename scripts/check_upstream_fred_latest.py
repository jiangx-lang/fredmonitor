#!/usr/bin/env python3
"""
检查滞后指标在 FRED 上游是否有更新数据可拉取。
只读：调用 FRED API 取各序列最新观测日期，与本地 raw 对比，不修改任何数据。
"""
import os
import sys
import csv
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

# 需要 FRED_API_KEY（macrolab.env）
os.chdir(BASE)

def get_local_last_date(series_id: str, from_fred_dir: bool = True) -> str | None:
    """本地 raw 最后日期。from_fred_dir: 读 data/fred/series/{id}/raw.csv"""
    if from_fred_dir:
        raw = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    else:
        raw = BASE / "data" / "series" / f"{series_id}.csv"
    if not raw.exists():
        return None
    with open(raw, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) < 2:
        return None
    # 最后一行（可能是空行）
    for line in reversed(lines[1:]):
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if parts:
            return parts[0].strip()
    return None

def get_fred_upstream_last_date(series_id: str) -> tuple[str | None, str]:
    """调用 FRED API 取该序列最新观测日期。返回 (YYYY-MM-DD, error_msg)。"""
    try:
        from scripts.fred_http import series_observations
    except Exception as e:
        return None, str(e)
    try:
        # 只拉最近一年，减少 payload
        from datetime import timedelta
        start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        resp = series_observations(series_id, observation_start=start)
        obs = resp.get("observations") or []
        if not obs:
            return None, "no observations"
        dates = [o.get("date") for o in obs if o.get("date")]
        if not dates:
            return None, "no date in obs"
        return max(dates), ""
    except Exception as e:
        return None, str(e)

def main():
    # 从审计 CSV 读待查指标；只查 FRED 相关（含 alias 与 derived 的底层腿）
    audit_path = BASE / "data" / "stale_indicator_root_cause_audit.csv"
    if not audit_path.exists():
        print("未找到 data/stale_indicator_root_cause_audit.csv，请先运行 stale_indicator_root_cause_audit.py")
        return
    alias_map = {"CREDIT_CARD_DELINQUENCY": "DRCCLACBS"}
    # 派生指标底层 FRED 腿（只取首组）
    derived_fred_legs = {
        "RESERVES_DEPOSITS_PCT": ["TOTRESNS", "TOTALSA"],
        "CORPDEBT_GDP_PCT": ["NCBDBIQ027S", "GDP"],
        "HY_IG_RATIO": ["BAMLHYH0A0HYM2TRIV", "BAMLCC0A0CMTRIV"],
        "VIX_TERM_STRUCTURE": ["VIXCLS", "VIX3M"],
        "HY_OAS_MOMENTUM_RATIO": ["BAMLH0A0HYM2"],
    }
    rows = []
    with open(audit_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    # 收集要查的 FRED series_id
    to_check = {}  # indicator_id -> (list of fred_ids, local_last_date from audit)
    for row in rows:
        iid = row["indicator_id"]
        stype = row.get("source_type", "")
        local = (row.get("local_last_date") or "").strip()
        if "FRED" in stype or "derived" in stype:
            if iid in alias_map:
                to_check[iid] = ([alias_map[iid]], local)
            elif iid in derived_fred_legs:
                to_check[iid] = (derived_fred_legs[iid], local)
            else:
                to_check[iid] = ([iid], local)
    # 查询上游
    print("Querying FRED for latest observation dates (read-only)...")
    results = []
    for indicator_id, (fred_ids, local_audit) in to_check.items():
        upstream_dates = []
        errs = []
        for fred_id in fred_ids:
            up, err = get_fred_upstream_last_date(fred_id)
            if err and not up:
                errs.append(f"{fred_id}:{err}")
            elif up:
                upstream_dates.append(up)
        up_max = max(upstream_dates) if upstream_dates else None
        can_fetch = False
        if up_max and local_audit:
            can_fetch = up_max > local_audit
        results.append({
            "indicator_id": indicator_id,
            "local_last_date": local_audit,
            "upstream_last_date": up_max or "",
            "can_fetch_newer": can_fetch,
            "errors": "; ".join(errs) if errs else "",
        })
    # 输出
    out_path = BASE / "data" / "upstream_fred_latest_check.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["indicator_id", "local_last_date", "upstream_last_date", "can_fetch_newer", "errors"])
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {out_path}")
    # 汇总
    can = [r for r in results if r["can_fetch_newer"]]
    cannot = [r for r in results if not r["can_fetch_newer"] and r["upstream_last_date"]]
    no_upstream = [r for r in results if not r["upstream_last_date"]]
    print("\n--- 能否下载到更新（上游最新 > 本地最新）---")
    print(f"能下载到更新: {len(can)} 个")
    for r in can:
        print(f"  {r['indicator_id']}: local={r['local_last_date']} -> upstream={r['upstream_last_date']}")
    print(f"\n上游已与本地一致或更旧: {len(cannot)} 个")
    for r in cannot[:15]:
        print(f"  {r['indicator_id']}: local={r['local_last_date']} upstream={r['upstream_last_date']}")
    if len(cannot) > 15:
        print(f"  ... 共 {len(cannot)} 个")
    if no_upstream:
        print(f"\n上游无数据或请求失败: {len(no_upstream)} 个")
        for r in no_upstream[:10]:
            print(f"  {r['indicator_id']}: {r['errors']}")

if __name__ == "__main__":
    main()
