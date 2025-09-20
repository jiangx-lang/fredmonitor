#!/usr/bin/env python3
"""
检查所有FRED分类的目录结构和数据完整性
"""

import os
import sys
sys.path.append('.')

from scripts.fred_http import category, category_children, category_series
import pathlib

def check_all_categories():
    """检查所有FRED分类"""
    
    print("🔍 检查所有FRED分类...")
    print("=" * 60)
    
    # 获取所有一级分类 - 从已知的主要分类开始
    main_categories = [
        {"id": 10, "name": "Population, Employment, & Labor Markets"},
        {"id": 13, "name": "International Data"},
        {"id": 15, "name": "Prices"},
        {"id": 20, "name": "National Accounts"},
        {"id": 21, "name": "Regional Data"},
        {"id": 22, "name": "Academic Data"},
        {"id": 23, "name": "U.S. Regional Data"},
        {"id": 24, "name": "Alternative Measures"},
        {"id": 25, "name": "Business Cycle Indicators"},
        {"id": 26, "name": "Consumer"},
        {"id": 27, "name": "Financial"},
        {"id": 28, "name": "Health"},
        {"id": 29, "name": "Housing"},
        {"id": 30, "name": "Industry"},
        {"id": 31, "name": "International"},
        {"id": 32, "name": "Money, Banking, & Finance"},
        {"id": 33, "name": "National Accounts"},
        {"id": 34, "name": "Productivity"},
        {"id": 35, "name": "Regional"},
        {"id": 36, "name": "Research"},
        {"id": 37, "name": "Trade"},
        {"id": 38, "name": "Transportation"},
        {"id": 39, "name": "Utilities"},
        {"id": 40, "name": "Weather"},
        {"id": 41, "name": "Other"}
    ]
    
    print(f"📊 找到 {len(main_categories)} 个一级分类:")
    for cat in main_categories:
        print(f"  {cat['id']}: {cat['name']}")
    
    print(f"\n🏗️ 检查本地目录结构...")
    
    categories_root = pathlib.Path("data/fred/categories")
    local_categories = []
    
    if categories_root.exists():
        for item in categories_root.iterdir():
            if item.is_dir():
                local_categories.append(item.name)
                print(f"  ✅ {item.name}")
    
    print(f"\n📈 本地已建立 {len(local_categories)} 个分类目录")
    
    # 检查每个一级分类的子分类
    print(f"\n🔍 检查子分类结构...")
    
    for cat in main_categories:
        cat_id = cat['id']
        cat_name = cat['name']
        
        print(f"\n📁 {cat_id}: {cat_name}")
        
        try:
            # 获取子分类
            children_result = category_children(cat_id)
            children = children_result.get('categories', [])
            
            if children:
                print(f"  📂 子分类数: {len(children)}")
                for child in children[:5]:  # 只显示前5个
                    print(f"    - {child['id']}: {child['name']}")
                if len(children) > 5:
                    print(f"    ... 还有 {len(children) - 5} 个子分类")
            else:
                print(f"  📂 无子分类")
                
        except Exception as e:
            print(f"  ❌ 获取子分类失败: {e}")
    
    return main_categories, local_categories

if __name__ == "__main__":
    main_categories, local_categories = check_all_categories()
