#!/usr/bin/env python3
"""
测试FRED分类发现器
"""

import os
import re
import yaml
import pathlib
from dotenv import load_dotenv
import pandas as pd

# 基础路径配置
BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))
OUT_PATH = pathlib.Path(BASE) / "config" / "catalog_fred.yaml"

# 规则配置
DEFAULT_FREQS = {
    "M": {"yoy": (12, 100), "mom": (1, 100)},
    "Q": {"yoy": (4, 100), "qoq": (1, 100)},
}

def sanitize_alias(title: str) -> str:
    """清理标题生成alias"""
    alias = re.sub(r"[^A-Za-z0-9]+", "_", title).strip("_")
    return alias[:40] or "series"

def freq_short(freq: str) -> str:
    """将FRED频率转换为短代码"""
    f = (freq or "").strip().lower()
    if f.startswith("month"): return "M"
    if f.startswith("quarter"): return "Q"
    return "O"

def calc_block_for_freq(fr_short: str) -> dict:
    """给系列生成 calc 规则"""
    rules = DEFAULT_FREQS.get(fr_short, {})
    if not rules:
        return None
    out = {}
    for name, (shift, scale) in rules.items():
        out[name] = {"op": "pct_change", "shift": shift, "scale": scale}
    return out

def main():
    """主函数"""
    print("FRED分类目录自动发现器 (测试版)")
    print("=" * 50)
    
    # 示例系列数据
    sample_series = [
        {
            "id": "CPIAUCSL",
            "title": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
            "frequency": "Monthly",
            "seasonal_adjustment": "Seasonally Adjusted",
            "popularity": 100,
            "observation_start": "1947-01-01",
            "observation_end": "2025-07-01"
        },
        {
            "id": "UNRATE",
            "title": "Unemployment Rate",
            "frequency": "Monthly", 
            "seasonal_adjustment": "Seasonally Adjusted",
            "popularity": 95,
            "observation_start": "1948-01-01",
            "observation_end": "2025-07-01"
        },
        {
            "id": "GDP",
            "title": "Gross Domestic Product",
            "frequency": "Quarterly",
            "seasonal_adjustment": "Seasonally Adjusted Annual Rate",
            "popularity": 98,
            "observation_start": "1947-01-01",
            "observation_end": "2025-07-01"
        }
    ]
    
    # 生成catalog
    items = []
    for s in sample_series:
        fr = freq_short(s.get("frequency",""))
        calc = calc_block_for_freq(fr)
        alias = sanitize_alias(s.get("title", s["id"]))
        
        item = {"id": s["id"], "alias": alias}
        if calc: 
            item["calc"] = calc
        
        # freshness_days
        if fr == "M": 
            item["freshness_days"] = 60
        elif fr == "Q": 
            item["freshness_days"] = 120
            
        items.append(item)
    
    catalog = {"series": items}
    
    # 保存到文件
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(catalog, f, allow_unicode=True, sort_keys=False)
    
    print(f"[OK] 已写入 {OUT_PATH}，包含 {len(catalog['series'])} 个系列")
    
    # 显示结果
    print("\n生成的catalog:")
    for item in catalog["series"]:
        print(f"  - {item['id']}: {item['alias']}")

if __name__ == "__main__":
    main()
