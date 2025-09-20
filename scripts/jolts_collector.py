#!/usr/bin/env python3
"""
JOLTS数据收集器

专门收集Job Openings and Labor Turnover Survey (JOLTS)数据
包含职位空缺、招聘、离职等详细劳动力市场指标
"""

import os
import re
import yaml
import pathlib
from typing import Dict, Any, List
import pandas as pd
from dotenv import load_dotenv

from scripts.fred_http import category_series, polite_sleep

BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))
OUT = pathlib.Path(BASE) / "config" / "jolts_catalog.yaml"

# JOLTS分类ID (根据FRED网站)
JOLTS_CATEGORY_ID = 32241

# JOLTS重要系列ID (手动精选的重要指标)
IMPORTANT_JOLTS_SERIES = [
    # Job Openings (职位空缺)
    "JTSJOL",      # Job Openings: Total Nonfarm
    "JTS1000JOL",  # Job Openings: Total Private
    "JTS9000JOL",  # Job Openings: Government
    
    # Job Openings Rates (职位空缺率)
    "JTSJOR",      # Job Openings: Total Nonfarm
    "JTS1000JOR",  # Job Openings: Total Private
    "JTS9000JOR",  # Job Openings: Government
    
    # Hires (招聘)
    "JTSHIL",      # Hires: Total Nonfarm
    "JTS1000HIL",  # Hires: Total Private
    "JTS9000HIL",  # Hires: Government
    
    # Hires Rates (招聘率)
    "JTSHIR",      # Hires: Total Nonfarm
    "JTS1000HIR",  # Hires: Total Private
    "JTS9000HIR",  # Hires: Government
    
    # Total Separations (总离职)
    "JTSTSL",      # Total Separations: Total Nonfarm
    "JTS1000TSL",  # Total Separations: Total Private
    "JTS9000TSL",  # Total Separations: Government
    
    # Total Separations Rates (总离职率)
    "JTSTSR",      # Total Separations: Total Nonfarm
    "JTS1000TSR",  # Total Separations: Total Private
    "JTS9000TSR",  # Total Separations: Government
    
    # Quits (主动离职)
    "JTSQUL",      # Quits: Total Nonfarm
    "JTS1000QUL",  # Quits: Total Private
    "JTS9000QUL",  # Quits: Government
    
    # Quits Rates (主动离职率)
    "JTSQUR",      # Quits: Total Nonfarm
    "JTS1000QUR",  # Quits: Total Private
    "JTS9000QUR",  # Quits: Government
    
    # Layoffs and Discharges (裁员和解雇)
    "JTSLDL",      # Layoffs and Discharges: Total Nonfarm
    "JTS1000LDL",  # Layoffs and Discharges: Total Private
    "JTS9000LDL",  # Layoffs and Discharges: Government
    
    # Layoffs and Discharges Rates (裁员和解雇率)
    "JTSLDR",      # Layoffs and Discharges: Total Nonfarm
    "JTS1000LDR",  # Layoffs and Discharges: Total Private
    "JTS9000LDR",  # Layoffs and Discharges: Government
    
    # Other Separations (其他离职)
    "JTSOSL",      # Other Separations: Total Nonfarm
    "JTS1000OSL",  # Other Separations: Total Private
    "JTS9000OSL",  # Other Separations: Government
    
    # Other Separations Rates (其他离职率)
    "JTSOSR",      # Other Separations: Total Nonfarm
    "JTS1000OSR",  # Other Separations: Total Private
    "JTS9000OSR",  # Other Separations: Government
]

def freq_short(freq: str) -> str:
    f = (freq or "").lower()
    if f.startswith("month"): return "M"
    if f.startswith("quarter"): return "Q"
    if f.startswith("week"): return "W"
    if f.startswith("day"): return "D"
    return "O"

def get_jolts_series_info() -> List[Dict[str, Any]]:
    """获取JOLTS系列信息"""
    print("正在获取JOLTS系列信息...")
    
    all_series = []
    page = 0
    
    while True:
        try:
            resp = category_series(
                JOLTS_CATEGORY_ID,
                order_by="popularity",
                sort_order="desc",
                limit=1000,
                offset=page * 1000
            )
            
            series_list = resp.get("seriess", []) or []
            if not series_list:
                break
            
            all_series.extend(series_list)
            page += 1
            polite_sleep()
            
        except Exception as e:
            print(f"获取JOLTS系列信息出错: {e}")
            break
    
    print(f"找到 {len(all_series)} 个JOLTS系列")
    return all_series

def build_jolts_catalog() -> Dict[str, Any]:
    """构建JOLTS catalog"""
    print("构建JOLTS数据目录...")
    
    # 获取所有JOLTS系列
    all_series = get_jolts_series_info()
    
    # 创建系列ID到信息的映射
    series_map = {s["id"]: s for s in all_series}
    
    catalog_items = []
    
    # 处理重要系列
    for series_id in IMPORTANT_JOLTS_SERIES:
        if series_id in series_map:
            s = series_map[series_id]
            fr = freq_short(s.get("frequency", ""))
            
            # 生成alias
            title = s.get("title", series_id)
            alias = re.sub(r"[^A-Za-z0-9]+", "_", title).strip("_")[:50]
            
            item = {
                "id": series_id,
                "alias": alias,
                "freshness_days": 45,  # JOLTS数据通常每月更新
                "description": title
            }
            
            # 添加计算规则 (JOLTS数据通常是月度数据)
            if fr == "M":
                item["calc"] = {
                    "mom": {"op": "pct_change", "shift": 1, "scale": 100},
                    "yoy": {"op": "pct_change", "shift": 12, "scale": 100}
                }
            
            catalog_items.append(item)
            print(f"  添加: {series_id} - {alias}")
    
    # 按重要性排序
    catalog_items.sort(key=lambda x: IMPORTANT_JOLTS_SERIES.index(x["id"]) if x["id"] in IMPORTANT_JOLTS_SERIES else 999)
    
    return {"series": catalog_items}

def main():
    """主函数"""
    print("🔍 JOLTS数据收集器")
    print("=" * 50)
    
    OUT.parent.mkdir(parents=True, exist_ok=True)
    
    # 构建JOLTS catalog
    catalog = build_jolts_catalog()
    
    # 保存catalog
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    
    print(f"\n✅ JOLTS数据目录构建完成!")
    print(f"📊 总系列数: {len(catalog['series'])}")
    print(f"💾 已保存到: {OUT}")
    
    # 显示分类统计
    print(f"\n📈 JOLTS数据分类:")
    categories = {
        "Job Openings": len([s for s in catalog["series"] if "JOL" in s["id"]]),
        "Hires": len([s for s in catalog["series"] if "HIL" in s["id"]]),
        "Total Separations": len([s for s in catalog["series"] if "TSL" in s["id"]]),
        "Quits": len([s for s in catalog["series"] if "QUL" in s["id"]]),
        "Layoffs": len([s for s in catalog["series"] if "LDL" in s["id"]]),
        "Other Separations": len([s for s in catalog["series"] if "OSL" in s["id"]])
    }
    
    for category, count in categories.items():
        print(f"  {category}: {count} 个系列")
    
    print(f"\n🎯 下一步:")
    print(f"python scripts\\sync_fred_http.py  # 同步JOLTS数据")

if __name__ == "__main__":
    main()
