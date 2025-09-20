#!/usr/bin/env python3
"""
增强版FRED因子发现器

自动扫描FRED分类，智能筛选重要指标，批量建立因子库
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

# 重要分类ID映射 (根据FRED官网分类)
IMPORTANT_CATEGORIES = {
    # Money, Banking, & Finance
    10: {"name": "Money, Banking, & Finance", "priority": 1, "max_series": 50},
    
    # Population, Employment, & Labor Markets  
    13: {"name": "Population, Employment, & Labor Markets", "priority": 1, "max_series": 30},
    
    # National Accounts
    14: {"name": "National Accounts", "priority": 1, "max_series": 20},
    
    # Production & Business Activity
    15: {"name": "Production & Business Activity", "priority": 2, "max_series": 40},
    
    # Prices
    16: {"name": "Prices", "priority": 1, "max_series": 30},
    
    # International Data
    17: {"name": "International Data", "priority": 3, "max_series": 20},
}

# 筛选规则
INCLUDE_FREQ = {"M", "Q", "W", "D"}  # 包含所有频率
POPULARITY_MIN = 5                    # 最低人气要求
MIN_YEARS = 3                        # 最少历史年数
MAX_TOTAL_SERIES = 200               # 总系列数上限

# 重要关键词 (优先选择包含这些词的系列)
PRIORITY_KEYWORDS = [
    "CPI", "PCE", "PPI", "GDP", "GNP", "Unemployment", "Employment", "Payroll",
    "Interest", "Rate", "Yield", "Bond", "Treasury", "Federal", "Funds",
    "VIX", "Volatility", "Index", "Stock", "Market", "Price", "Inflation",
    "Consumer", "Confidence", "Sentiment", "Housing", "Home", "Real Estate",
    "Industrial", "Production", "Manufacturing", "Retail", "Sales", "Trade",
    "Exchange", "Dollar", "Currency", "Monetary", "Banking", "Credit",
    "Financial", "Liquidity", "Spread", "Risk", "Default", "Corporate"
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

def collect_series_from_category(cat_id: int, max_series: int) -> List[Dict[str, Any]]:
    """从指定分类收集系列"""
    print(f"正在扫描分类 {cat_id} ({IMPORTANT_CATEGORIES[cat_id]['name']})...")
    
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

def build_enhanced_catalog() -> Dict[str, Any]:
    """构建增强版catalog"""
    print("开始构建增强版因子库...")
    
    all_series = []
    
    # 按优先级顺序扫描分类
    sorted_categories = sorted(IMPORTANT_CATEGORIES.items(), key=lambda x: x[1]["priority"])
    
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
        alias = re.sub(r"[^A-Za-z0-9]+", "_", title).strip("_")[:40]
        
        item = {
            "id": s["id"],
            "alias": alias,
            "freshness_days": 60 if fr == "M" else (120 if fr == "Q" else 30)
        }
        
        # 添加计算规则
        calc_rules = calc_block_for_freq(fr)
        if calc_rules:
            item["calc"] = calc_rules
        
        catalog_items.append(item)
    
    return {"series": catalog_items}

def main():
    """主函数"""
    print("🚀 FRED增强版因子发现器")
    print("=" * 60)
    
    OUT.parent.mkdir(parents=True, exist_ok=True)
    
    # 构建增强版catalog
    catalog = build_enhanced_catalog()
    
    # 保存catalog
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    
    print(f"\n✅ 增强版因子库构建完成!")
    print(f"📊 总系列数: {len(catalog['series'])}")
    print(f"💾 已保存到: {OUT}")
    
    # 显示分类统计
    print(f"\n📈 分类统计:")
    for cat_id, cat_info in IMPORTANT_CATEGORIES.items():
        count = len([s for s in catalog["series"] if s.get("category_id") == cat_id])
        print(f"  {cat_info['name']}: {count} 个系列")
    
    print(f"\n🎯 下一步:")
    print(f"python scripts\\sync_fred_http.py  # 同步数据")
    print(f"python scripts\\render_fact_sheets_http.py  # 生成事实表")

if __name__ == "__main__":
    main()
