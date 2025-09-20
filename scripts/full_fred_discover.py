#!/usr/bin/env python3
"""
完整FRED数据库发现器

自动扫描所有FRED分类，建立完整的本地数据库
包含主目录和所有子目录的数据系列
"""

import os
import re
import yaml
import pathlib
from typing import Dict, Any, List, Set
import pandas as pd
from dotenv import load_dotenv

from scripts.fred_http import category, category_children, category_series, polite_sleep

BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))
OUT = pathlib.Path(BASE) / "config" / "full_fred_catalog.yaml"

# FRED主要分类ID (根据官网结构)
MAIN_CATEGORIES = {
    10: {"name": "Money, Banking, & Finance", "priority": 1, "max_series": 100},
    13: {"name": "Population, Employment, & Labor Markets", "priority": 1, "max_series": 80},
    14: {"name": "National Accounts", "priority": 1, "max_series": 60},
    15: {"name": "Production & Business Activity", "priority": 2, "max_series": 120},
    16: {"name": "Prices", "priority": 1, "max_series": 80},
    17: {"name": "International Data", "priority": 3, "max_series": 60},
}

# 重要子分类ID (手动收集的重要子分类)
IMPORTANT_SUBCATEGORIES = {
    # Money, Banking, & Finance 子分类
    15: {"name": "Interest Rates", "parent": 10, "max_series": 30},
    16: {"name": "Exchange Rates", "parent": 10, "max_series": 20},
    17: {"name": "Monetary Data", "parent": 10, "max_series": 25},
    18: {"name": "Financial Indicators", "parent": 10, "max_series": 40},
    19: {"name": "Banking", "parent": 10, "max_series": 30},
    
    # Population, Employment, & Labor Markets 子分类
    20: {"name": "Current Population Survey", "parent": 13, "max_series": 25},
    21: {"name": "Current Employment Statistics", "parent": 13, "max_series": 20},
    22: {"name": "Education", "parent": 13, "max_series": 15},
    23: {"name": "Income Distribution", "parent": 13, "max_series": 10},
    24: {"name": "Job Openings and Labor Turnover", "parent": 13, "max_series": 30},
    25: {"name": "Population", "parent": 13, "max_series": 20},
    26: {"name": "Productivity & Costs", "parent": 13, "max_series": 25},
    
    # National Accounts 子分类
    27: {"name": "National Income & Product Accounts", "parent": 14, "max_series": 30},
    28: {"name": "Federal Government Debt", "parent": 14, "max_series": 10},
    29: {"name": "U.S. Trade & International Transactions", "parent": 14, "max_series": 15},
    30: {"name": "Flow of Funds", "parent": 14, "max_series": 25},
    
    # Production & Business Activity 子分类
    31: {"name": "Business Cycle Expansions & Contractions", "parent": 15, "max_series": 10},
    32: {"name": "Construction", "parent": 15, "max_series": 15},
    33: {"name": "Expenditures", "parent": 15, "max_series": 30},
    34: {"name": "Housing", "parent": 15, "max_series": 40},
    35: {"name": "Industrial Production & Capacity Utilization", "parent": 15, "max_series": 20},
    36: {"name": "Manufacturing", "parent": 15, "max_series": 15},
    37: {"name": "Retail Trade", "parent": 15, "max_series": 10},
    38: {"name": "Services", "parent": 15, "max_series": 20},
    
    # Prices 子分类
    39: {"name": "Commodities", "parent": 16, "max_series": 20},
    40: {"name": "Consumer Price Indexes", "parent": 16, "max_series": 25},
    41: {"name": "Employment Cost Index", "parent": 16, "max_series": 15},
    42: {"name": "House Price Indexes", "parent": 16, "max_series": 20},
    43: {"name": "Producer Price Indexes", "parent": 16, "max_series": 30},
    44: {"name": "Trade Indexes", "parent": 16, "max_series": 15},
    
    # International Data 子分类
    45: {"name": "Countries", "parent": 17, "max_series": 30},
    46: {"name": "Geography", "parent": 17, "max_series": 10},
    47: {"name": "Indicators", "parent": 17, "max_series": 25},
    48: {"name": "Institutions", "parent": 17, "max_series": 15},
}

# 筛选规则
INCLUDE_FREQ = {"M", "Q", "W", "D"}
POPULARITY_MIN = 3
MIN_YEARS = 2
MAX_TOTAL_SERIES = 1000

# 重要关键词
PRIORITY_KEYWORDS = [
    "CPI", "PCE", "PPI", "GDP", "GNP", "Unemployment", "Employment", "Payroll",
    "Interest", "Rate", "Yield", "Bond", "Treasury", "Federal", "Funds",
    "VIX", "Volatility", "Index", "Stock", "Market", "Price", "Inflation",
    "Consumer", "Confidence", "Sentiment", "Housing", "Home", "Real Estate",
    "Industrial", "Production", "Manufacturing", "Retail", "Sales", "Trade",
    "Exchange", "Dollar", "Currency", "Monetary", "Banking", "Credit",
    "Financial", "Liquidity", "Spread", "Risk", "Default", "Corporate",
    "Job", "Opening", "Hire", "Quit", "Layoff", "Separation", "Turnover",
    "Labor", "Force", "Participation", "Productivity", "Wage", "Income"
]

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

def calculate_priority_score(series: Dict[str, Any]) -> float:
    """计算系列优先级分数"""
    score = 0.0
    
    # 基础分数
    popularity = series.get("popularity", 0) or 0
    score += popularity * 0.1
    
    # 关键词匹配
    title = (series.get("title", "") or "").upper()
    for keyword in PRIORITY_KEYWORDS:
        if keyword.upper() in title:
            score += 10.0
    
    # 频率偏好
    freq = freq_short(series.get("frequency", ""))
    if freq == "M": score += 5.0
    elif freq == "Q": score += 3.0
    elif freq == "W": score += 2.0
    elif freq == "D": score += 1.0
    
    # 季节性调整偏好
    sa = str(series.get("seasonal_adjustment", "")).lower()
    if "seasonally adjusted" in sa:
        score += 3.0
    
    return score

def discover_all_categories() -> Dict[int, Dict[str, Any]]:
    """发现所有分类"""
    print("🔍 发现所有FRED分类...")
    
    all_categories = {}
    
    # 添加主分类
    for cat_id, cat_info in MAIN_CATEGORIES.items():
        all_categories[cat_id] = cat_info
        print(f"  主分类: {cat_id} - {cat_info['name']}")
    
    # 添加重要子分类
    for cat_id, cat_info in IMPORTANT_SUBCATEGORIES.items():
        # 为子分类添加priority字段（继承父分类的priority）
        parent_id = cat_info.get('parent')
        parent_priority = MAIN_CATEGORIES.get(parent_id, {}).get('priority', 2)
        cat_info['priority'] = parent_priority
        all_categories[cat_id] = cat_info
        print(f"  子分类: {cat_id} - {cat_info['name']} (父分类: {cat_info['parent']})")
    
    print(f"总共发现 {len(all_categories)} 个分类")
    return all_categories

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
            "wow": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    elif fr == "D":
        return {
            "dod": {"op": "pct_change", "shift": 1, "scale": 100}
        }
    return None

def build_full_catalog() -> Dict[str, Any]:
    """构建完整catalog"""
    print("🏗️ 构建完整FRED数据库...")
    
    # 发现所有分类
    all_categories = discover_all_categories()
    
    # 按优先级顺序扫描分类
    sorted_categories = sorted(all_categories.items(), key=lambda x: x[1]["priority"])
    
    all_series = []
    
    for cat_id, cat_info in sorted_categories:
        series = collect_series_from_category(cat_id, cat_info["max_series"])
        all_series.extend(series)
        
        if len(all_series) >= MAX_TOTAL_SERIES:
            print(f"达到总系列数上限 {MAX_TOTAL_SERIES}，停止扫描")
            break
    
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
    print("🚀 完整FRED数据库发现器")
    print("=" * 60)
    
    OUT.parent.mkdir(parents=True, exist_ok=True)
    
    # 构建完整catalog
    catalog = build_full_catalog()
    
    # 保存catalog
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    
    print(f"\n✅ 完整FRED数据库构建完成!")
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
        print(f"  分类 {cat_id}: {count} 个系列")
    
    print(f"\n🎯 下一步:")
    print(f"python scripts\\sync_fred_http.py  # 同步数据")
    print(f"python scripts\\render_fact_sheets_http.py  # 生成事实表")

if __name__ == "__main__":
    main()
