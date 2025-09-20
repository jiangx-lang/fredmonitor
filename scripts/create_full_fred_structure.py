#!/usr/bin/env python3
"""
创建完整FRED目录结构并下载数据
基于full_fred_catalog.yaml创建所有分类的目录结构
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# 分类ID到名称的映射
CATEGORY_NAMES = {
    10: "Money_Banking_Finance",
    13: "Population_Employment_Labor_Markets", 
    14: "National_Accounts",
    15: "Production_Business_Activity",
    16: "Prices",
    17: "International_Data",
    18: "Financial_Indicators",
    19: "Banking",
    20: "Current_Population_Survey",
    21: "Current_Employment_Statistics",
    22: "Education",
    23: "Income_Distribution",
    24: "Job_Openings_Labor_Turnover",
    25: "Population",
    26: "Productivity_Costs",
    27: "National_Income_Product_Accounts",
    28: "Federal_Government_Debt",
    29: "US_Trade_International_Transactions",
    30: "Flow_of_Funds",
    31: "Business_Cycle_Expansions_Contractions",
    32: "Construction",
    33: "Expenditures",
    34: "Housing",
    35: "Industrial_Production_Capacity_Utilization",
    36: "Manufacturing",
    37: "Retail_Trade",
    38: "Services",
    39: "Commodities",
    40: "Consumer_Price_Indexes",
    41: "Employment_Cost_Index",
    42: "House_Price_Indexes",
    43: "Producer_Price_Indexes",
    44: "Trade_Indexes",
    45: "Countries",
    46: "Geography",
    47: "Indicators",
    48: "Institutions"
}

def create_category_directories():
    """创建所有分类的目录结构"""
    print("🏗️ 创建FRED分类目录结构...")
    
    catalog_file = pathlib.Path(BASE) / "config" / "full_fred_catalog.yaml"
    if not catalog_file.exists():
        print("❌ 未找到full_fred_catalog.yaml，请先运行full_fred_discover.py")
        return
    
    # 读取catalog
    with open(catalog_file, 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series_list = catalog.get('series', [])
    
    # 统计每个分类的序列数量
    category_stats = {}
    for item in series_list:
        cat_id = item.get('category_id')
        if cat_id:
            if cat_id not in category_stats:
                category_stats[cat_id] = []
            category_stats[cat_id].append(item)
    
    print(f"📊 发现 {len(category_stats)} 个活跃分类")
    
    # 创建主分类目录
    categories_root = pathlib.Path(BASE) / "data" / "fred" / "categories"
    categories_root.mkdir(parents=True, exist_ok=True)
    
    created_dirs = []
    
    for cat_id, series_items in category_stats.items():
        category_name = CATEGORY_NAMES.get(cat_id, f"Category_{cat_id}")
        
        # 创建分类目录
        category_dir = categories_root / category_name
        category_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建子目录
        subdirs = ["series", "metadata", "reports", "analysis"]
        for subdir in subdirs:
            (category_dir / subdir).mkdir(exist_ok=True)
        
        # 创建分类信息文件
        category_info = {
            "category_id": cat_id,
            "category_name": category_name,
            "series_count": len(series_items),
            "series_list": [item['id'] for item in series_items],
            "description": f"FRED Category {cat_id}: {category_name}",
            "created_at": pathlib.Path().cwd().stat().st_mtime
        }
        
        info_file = category_dir / "metadata" / "category_info.yaml"
        with open(info_file, 'w', encoding='utf-8') as f:
            yaml.safe_dump(category_info, f, allow_unicode=True, sort_keys=False)
        
        created_dirs.append({
            "id": cat_id,
            "name": category_name,
            "path": str(category_dir),
            "series_count": len(series_items)
        })
        
        print(f"✅ {category_name}: {len(series_items)} 个序列")
    
    # 创建总览文件
    overview = {
        "total_categories": len(created_dirs),
        "total_series": len(series_list),
        "categories": created_dirs,
        "created_at": pathlib.Path().cwd().stat().st_mtime
    }
    
    overview_file = categories_root / "overview.yaml"
    with open(overview_file, 'w', encoding='utf-8') as f:
        yaml.safe_dump(overview, f, allow_unicode=True, sort_keys=False)
    
    print(f"\n🎯 目录结构创建完成!")
    print(f"📁 总分类数: {len(created_dirs)}")
    print(f"📊 总序列数: {len(series_list)}")
    print(f"💾 总览文件: {overview_file}")
    
    return created_dirs

def create_series_symlinks():
    """为每个序列创建到分类目录的符号链接"""
    print("\n🔗 创建序列符号链接...")
    
    catalog_file = pathlib.Path(BASE) / "config" / "full_fred_catalog.yaml"
    with open(catalog_file, 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series_list = catalog.get('series', [])
    categories_root = pathlib.Path(BASE) / "data" / "fred" / "categories"
    series_root = pathlib.Path(BASE) / "data" / "fred" / "series"
    
    linked_count = 0
    
    for item in series_list:
        series_id = item['id']
        cat_id = item.get('category_id')
        
        if not cat_id:
            continue
            
        category_name = CATEGORY_NAMES.get(cat_id, f"Category_{cat_id}")
        category_dir = categories_root / category_name / "series"
        
        # 检查原始序列目录是否存在
        original_dir = series_root / series_id
        if not original_dir.exists():
            continue
        
        # 创建符号链接
        link_path = category_dir / series_id
        if not link_path.exists():
            try:
                link_path.symlink_to(original_dir)
                linked_count += 1
            except Exception as e:
                print(f"⚠️ 创建链接失败 {series_id}: {e}")
    
    print(f"✅ 创建了 {linked_count} 个符号链接")

def main():
    """主函数"""
    print("🚀 FRED完整目录结构创建器")
    print("=" * 50)
    
    # 创建分类目录
    created_dirs = create_category_directories()
    
    # 创建符号链接
    create_series_symlinks()
    
    print(f"\n🎉 完成! 下一步:")
    print(f"python -m scripts.sync_fred_http  # 同步数据到分类目录")
    print(f"python -m scripts.render_fact_sheets_http  # 生成分类报告")

if __name__ == "__main__":
    main()
