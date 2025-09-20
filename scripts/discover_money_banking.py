#!/usr/bin/env python3
"""
专门发现Money, Banking, & Finance分类的序列
"""

import os
import re
import yaml
import pathlib
from typing import Dict, Any, List
import pandas as pd
from dotenv import load_dotenv

from scripts.fred_http import category, category_children, category_series, polite_sleep

BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")
OUT = pathlib.Path(BASE) / "config" / "money_banking_catalog.yaml"

# Money, Banking, & Finance 相关分类ID
MONEY_BANKING_CATEGORIES = {
    10: {"name": "Money, Banking, & Finance", "priority": 1, "max_series": 200},
    15: {"name": "Interest Rates", "parent": 10, "max_series": 100},
    16: {"name": "Exchange Rates", "parent": 10, "max_series": 50},
    17: {"name": "Monetary Data", "parent": 10, "max_series": 100},
    18: {"name": "Financial Indicators", "parent": 10, "max_series": 150},
    19: {"name": "Banking", "parent": 10, "max_series": 100},
}

# 筛选规则
INCLUDE_FREQ = {"M", "Q", "W", "D"}
POPULARITY_MIN = 3  # 降低人气要求
MIN_YEARS = 2       # 降低历史要求

def freq_short(freq: str) -> str:
    f = (freq or "").lower()
    if f.startswith("month"): return "M"
    if f.startswith("quarter"): return "Q"
    if f.startswith("week"): return "W"
    if f.startswith("day"): return "D"
    return "O"

def ok_history(s: str, e: str) -> bool:
    try:
        if not s or not e:
            return False
        start = pd.to_datetime(s)
        end = pd.to_datetime(e)
        years = (end - start).days / 365.25
        return years >= MIN_YEARS
    except:
        return False

def calculate_priority_score(s: Dict[str, Any]) -> float:
    """计算优先级分数"""
    score = 0.0
    
    # 人气分数 (0-50)
    popularity = s.get("popularity", 0) or 0
    score += min(50, popularity * 2)
    
    # 频率分数 (0-20)
    freq = freq_short(s.get("frequency", ""))
    freq_scores = {"D": 20, "W": 15, "M": 10, "Q": 5, "O": 0}
    score += freq_scores.get(freq, 0)
    
    # 历史长度分数 (0-20)
    try:
        start = pd.to_datetime(s.get("observation_start", ""))
        end = pd.to_datetime(s.get("observation_end", ""))
        years = (end - start).days / 365.25
        score += min(20, years * 2)
    except:
        pass
    
    # 关键词加分 (0-10)
    title = (s.get("title", "") or "").upper()
    keywords = ["RATE", "INTEREST", "EXCHANGE", "MONETARY", "BANKING", 
                "FINANCIAL", "VIX", "TREASURY", "FED", "DOLLAR", "CURRENCY"]
    for keyword in keywords:
        if keyword in title:
            score += 1
    
    return score

def collect_series_from_category(cat_id: int, max_series: int) -> List[Dict[str, Any]]:
    """从指定分类收集系列"""
    print(f"正在扫描分类 {cat_id}...")
    
    all_series = []
    page = 0
    
    while len(all_series) < max_series:
        try:
            resp = category_series(
                cat_id, 
                order_by="popularity", 
                sort_order="desc",
                limit=1000, 
                offset=page * 1000
            )
            
            series_list = resp.get("seriess", []) or []
            if not series_list:
                break
            
            for s in series_list:
                # 基础筛选
                fr = freq_short(s.get("frequency", ""))
                if fr not in INCLUDE_FREQ:
                    continue
                    
                if (s.get("popularity", 0) or 0) < POPULARITY_MIN:
                    continue
                    
                if not ok_history(s.get("observation_start", ""), s.get("observation_end", "")):
                    continue
                
                # 计算优先级分数
                s["priority_score"] = calculate_priority_score(s)
                s["category_id"] = cat_id
                all_series.append(s)
            
            page += 1
            polite_sleep()
            
        except Exception as e:
            print(f"  分类 {cat_id} 扫描出错: {e}")
            break
    
    # 按优先级分数排序
    all_series.sort(key=lambda x: x["priority_score"], reverse=True)
    
    # 返回前max_series个
    selected = all_series[:max_series]
    print(f"  分类 {cat_id}: 扫描了 {len(all_series)} 个系列，选中 {len(selected)} 个")
    
    return selected

def calc_block_for_freq(fr: str) -> Dict[str, Any] | None:
    """根据频率生成计算规则"""
    if fr == "M":
        return {
            "yoy": {"op": "pct_change", "shift": 12, "scale": 100},
            "mom": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    elif fr == "Q":
        return {
            "yoy": {"op": "pct_change", "shift": 4, "scale": 100},
            "qoq": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    elif fr == "W":
        return {
            "yoy": {"op": "pct_change", "shift": 52, "scale": 100},
            "wow": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    elif fr == "D":
        return {
            "yoy": {"op": "pct_change", "shift": 252, "scale": 100},
            "mom": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    return None

def build_money_banking_catalog() -> Dict[str, Any]:
    """构建Money, Banking, & Finance catalog"""
    print("🏗️ 构建Money, Banking, & Finance数据库...")
    
    all_series = []
    
    # 按优先级顺序扫描分类
    sorted_categories = sorted(MONEY_BANKING_CATEGORIES.items(), key=lambda x: x[1].get("priority", 2))
    
    for cat_id, cat_info in sorted_categories:
        series = collect_series_from_category(cat_id, cat_info["max_series"])
        all_series.extend(series)
    
    # 去重并按优先级排序
    seen_ids = set()
    unique_series = []
    
    for s in sorted(all_series, key=lambda x: x["priority_score"], reverse=True):
        if s["id"] not in seen_ids:
            seen_ids.add(s["id"])
            unique_series.append(s)
    
    # 构建catalog
    catalog_items = []
    for s in unique_series:
        fr = freq_short(s.get("frequency", ""))
        
        # 生成alias
        title = s.get("title", s["id"])
        alias = re.sub(r"[^A-Za-z0-9]+", "_", title).strip("_")[:50]
        
        item = {
            "id": s["id"],
            "alias": alias,
            "freshness_days": 60 if fr == "M" else (120 if fr == "Q" else 30),
            "category_id": s.get("category_id"),
            "priority_score": s.get("priority_score", 0)
        }
        
        # 添加计算规则
        calc_rules = calc_block_for_freq(fr)
        if calc_rules:
            item["calc"] = calc_rules
        
        catalog_items.append(item)
    
    return {"series": catalog_items}

def main():
    """主函数"""
    print("🚀 Money, Banking, & Finance 专门发现器")
    print("=" * 60)
    
    OUT.parent.mkdir(parents=True, exist_ok=True)
    
    # 构建catalog
    catalog = build_money_banking_catalog()
    
    # 保存catalog
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    
    print(f"\n✅ Money, Banking, & Finance数据库构建完成!")
    print(f"📊 总系列数: {len(catalog['series'])}")
    print(f"💾 已保存到: {OUT}")
    
    # 显示分类统计
    print(f"\n📈 分类统计:")
    category_stats = {}
    for item in catalog["series"]:
        cat_id = item.get("category_id", "unknown")
        if cat_id not in category_stats:
            category_stats[cat_id] = 0
        category_stats[cat_id] += 1
    
    for cat_id, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
        cat_name = MONEY_BANKING_CATEGORIES.get(cat_id, {}).get("name", f"Category_{cat_id}")
        print(f"  {cat_name} ({cat_id}): {count} 个系列")
    
    print(f"\n🎯 下一步:")
    print(f"python -m scripts.sync_fred_http  # 同步数据")

if __name__ == "__main__":
    main()
