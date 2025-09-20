#!/usr/bin/env python3
"""
简化的FRED发现器 - 直接使用已知分类ID
"""

import os
import yaml
import pathlib
from dotenv import load_dotenv

BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))
OUT = pathlib.Path(BASE) / "config" / "catalog_fred.yaml"

def main():
    print("FRED分类目录自动发现器（简化版）")
    print("=" * 50)
    
    # 根据您提供的FRED分类结构图，使用已知的重要分类ID
    # 这些是FRED的主要分类ID
    important_categories = [
        10,   # Money, Banking, & Finance
        13,   # Population, Employment, & Labor Markets  
        14,   # National Accounts
        15,   # Production & Business Activity
        16,   # Prices
        17,   # International Data
    ]
    
    print(f"使用 {len(important_categories)} 个重要分类: {important_categories}")
    
    # 创建示例catalog，包含一些重要的经济指标
    important_series = [
        {"id": "CPIAUCSL", "alias": "CPI_Headline", "calc": {"yoy": {"op": "pct_change", "shift": 12, "scale": 100}, "mom": {"op": "pct_change", "shift": 1, "scale": 100}}, "freshness_days": 60},
        {"id": "CPILFESL", "alias": "CPI_Core", "calc": {"yoy": {"op": "pct_change", "shift": 12, "scale": 100}}, "freshness_days": 60},
        {"id": "UNRATE", "alias": "Unemployment_Rate", "freshness_days": 30},
        {"id": "GDP", "alias": "GDP", "calc": {"yoy": {"op": "pct_change", "shift": 4, "scale": 100}}, "freshness_days": 120},
        {"id": "VIXCLS", "alias": "VIX", "freshness_days": 7},
        {"id": "DGS10", "alias": "UST10Y", "freshness_days": 7},
        {"id": "DGS2", "alias": "UST2Y", "freshness_days": 7},
        {"id": "NFCI", "alias": "Chicago_NFCI", "freshness_days": 14},
        {"id": "SOFR", "alias": "SOFR", "freshness_days": 7},
        {"id": "DTB3", "alias": "UST3M", "freshness_days": 7},
        {"id": "BAMLH0A0HYM2", "alias": "HY_Spread", "freshness_days": 7},
        {"id": "UMCSENT", "alias": "Consumer_Confidence", "freshness_days": 40},
        {"id": "CSUSHPINSA", "alias": "House_Price_Index", "calc": {"yoy": {"op": "pct_change", "shift": 12, "scale": 100}}, "freshness_days": 40},
        {"id": "BAMLEMCBPIOAS", "alias": "EM_Spread", "freshness_days": 40},
        {"id": "SP500", "alias": "SP500_Index", "freshness_days": 7},
        {"id": "DTWEXBGS", "alias": "DXY", "freshness_days": 7},
        {"id": "FEDFUNDS", "alias": "Fed_Funds_Rate", "freshness_days": 7},
        {"id": "PAYEMS", "alias": "Nonfarm_Payrolls", "freshness_days": 30},
        {"id": "INDPRO", "alias": "Industrial_Production", "calc": {"yoy": {"op": "pct_change", "shift": 12, "scale": 100}}, "freshness_days": 30},
        {"id": "RETAILSALES", "alias": "Retail_Sales", "calc": {"yoy": {"op": "pct_change", "shift": 12, "scale": 100}}, "freshness_days": 30},
    ]
    
    catalog = {"series": important_series}
    
    OUT.parent.mkdir(parents=True, exist_ok=True)
    yaml.safe_dump(catalog, open(OUT, "w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    
    print(f"[OK] 已写入 {OUT}，包含 {len(catalog['series'])} 个系列")
    
    print("\n生成的catalog:")
    for item in catalog["series"]:
        print(f"  - {item['id']}: {item['alias']}")
    
    print(f"\n现在可以运行同步脚本:")
    print(f"python scripts\\sync_fred_http.py")

if __name__ == "__main__":
    main()
