#!/usr/bin/env python3
"""
FRED分类目录自动发现器（简化版）

从FRED分类目录自动发现并生成catalog_fred.yaml配置。
只抓顶层分类，避免复杂递归，稳定可靠。
"""

import os
import re
import yaml
import pathlib
from typing import Dict, Any, List
import pandas as pd
from dotenv import load_dotenv

from scripts.fred_http import category, category_children, category_series, polite_sleep

BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))
OUT = pathlib.Path(BASE) / "config" / "catalog_fred.yaml"

# ---- 筛选规则：放宽条件获取更多内容 ----
TOP_NAME_REGEX = re.compile(
    r"(Money|Banking|Finance|Prices|Population|Employment|Labor|National|Accounts|Production|Business|Activity|International|Trade|Economic|Indicators)",
    re.I
)
INCLUDE_FREQ = {"M", "Q", "W", "D"}  # 包含所有频率
POPULARITY_MIN = 5              # 降低人气门槛
MAX_PER_CATEGORY = 200          # 增加每类数量
MIN_YEARS = 3                   # 降低历史要求
TAG_FILTER = None               # 比如 "United States;Seasonally Adjusted"

def freq_short(freq: str) -> str:
    f = (freq or "").lower()
    if f.startswith("month"): return "M"
    if f.startswith("quarter"): return "Q"
    if f.startswith("week"): return "W"
    if f.startswith("day"): return "D"
    return "O"

def ok_history(s: str, e: str) -> bool:
    try:
        return (pd.to_datetime(e) - pd.to_datetime(s)).days >= 365 * MIN_YEARS
    except Exception:
        return True

def discover_top_ids(root_id=0) -> List[int]:
    """只抓顶层命中名称的类目 id（稳定，不深递归）。"""
    top = category_children(root_id).get("categories", []) or []
    print(f"找到 {len(top)} 个顶层分类")
    
    hits = []
    for c in top:
        name = c.get("name", "")
        if TOP_NAME_REGEX.search(name):
            hits.append(c["id"])
            print(f"  匹配分类: {c['id']} - {name}")
    
    print(f"匹配的分类数量: {len(hits)}")
    return hits

def collect_series(cat_id: int) -> List[Dict[str, Any]]:
    out, page = [], 0
    total_found = 0
    while True:
        resp = category_series(
            cat_id, order_by="popularity", sort_order="desc",
            limit=1000, offset=page*1000, tag_names=TAG_FILTER
        )
        arr = resp.get("seriess", []) or []
        if not arr: break
        
        page_found = 0
        for s in arr:
            total_found += 1
            fr = freq_short(s.get("frequency",""))
            if fr not in INCLUDE_FREQ: continue
            if (s.get("popularity",0) or 0) < POPULARITY_MIN: continue
            if not ok_history(s.get("observation_start",""), s.get("observation_end","")): continue
            out.append(s)
            page_found += 1
            if len(out) >= MAX_PER_CATEGORY: break
        if len(out) >= MAX_PER_CATEGORY: break
        page += 1
        polite_sleep()
    
    print(f"    分类 {cat_id}: 检查了 {total_found} 个系列，选中 {len(out)} 个")
    return out

def calc_block(fr: str) -> Dict[str, Any] | None:
    if fr == "M": return {"yoy": {"op":"pct_change","shift":12,"scale":100},
                          "mom": {"op":"pct_change","shift":1,"scale":100}}
    if fr == "Q": return {"yoy": {"op":"pct_change","shift":4,"scale":100},
                          "qoq": {"op":"pct_change","shift":1,"scale":100}}
    return None

def build_catalog(series_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    seen, items = set(), []
    for s in series_list:
        sid = s["id"]
        if sid in seen: continue
        seen.add(sid)
        fr = freq_short(s.get("frequency",""))
        item = {"id": sid, "alias": re.sub(r"[^A-Za-z0-9]+","_", s.get("title",sid))[:40]}
        cb = calc_block(fr)
        if cb: item["calc"] = cb
        item["freshness_days"] = 60 if fr=="M" else (120 if fr=="Q" else 30)
        items.append(item)
    return {"series": items}

def main():
    """主函数"""
    print("FRED分类目录自动发现器（简化版）")
    print("=" * 50)
    
    OUT.parent.mkdir(parents=True, exist_ok=True)

    print("正在获取顶层分类...")
    top_ids = discover_top_ids(0)
    print(f"[INFO] top categories matched: {top_ids}")

    if not top_ids:
        print("警告：没有找到匹配的分类，使用默认分类")
        # 使用一些已知的重要分类ID
        top_ids = [10, 13, 14, 15, 16, 17]  # 这些是FRED的主要分类ID

    all_series = []
    for cid in top_ids:
        print(f"正在收集分类 {cid} 的系列...")
        ser = collect_series(cid)
        print(f"  - category {cid}: selected {len(ser)}")
        all_series.extend(ser)

    catalog = build_catalog(all_series)
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    print(f"[OK] wrote {OUT} with {len(catalog['series'])} series.")
    
    # 显示频率分布
    freq_stats = {}
    for item in catalog["series"]:
        for s in all_series:
            if s["id"] == item["id"]:
                freq = freq_short(s.get("frequency", ""))
                freq_stats[freq] = freq_stats.get(freq, 0) + 1
                break
    
    print("\n频率分布:")
    for freq, count in sorted(freq_stats.items()):
        print(f"  {freq}: {count} 个系列")

if __name__ == "__main__":
    main()
