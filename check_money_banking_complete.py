#!/usr/bin/env python3
"""
完整检查Money, Banking, & Finance分类
对比FRED官方分类和本地目录结构
"""

import yaml
import pathlib
from typing import Dict, List, Any

def check_money_banking_complete():
    """完整检查Money, Banking, & Finance分类"""
    
    print("🔍 Money, Banking, & Finance 完整分类检查")
    print("=" * 60)
    
    # FRED官方Money, Banking, & Finance子分类（根据图片）
    official_subcategories = {
        "Interest_Rates": 1057,
        "Exchange_Rates": 160, 
        "Monetary_Data": 1182,
        "Financial_Indicators": 12570,
        "Banking": 6541,
        "Business_Lending": 2603,
        "Foreign_Exchange_Intervention": 21
    }
    
    # 检查本地目录结构
    money_banking_dir = pathlib.Path("data/fred/categories/Money_Banking_Finance")
    
    print("📁 本地目录结构检查:")
    local_subcategories = {}
    
    for subcat_name in official_subcategories.keys():
        subcat_dir = money_banking_dir / subcat_name
        if subcat_dir.exists():
            # 检查是否有序列文件
            series_files = list(subcat_dir.glob("*"))
            local_subcategories[subcat_name] = len(series_files)
            print(f"  ✅ {subcat_name}: 目录存在 ({len(series_files)} 个文件)")
        else:
            local_subcategories[subcat_name] = 0
            print(f"  ❌ {subcat_name}: 目录不存在")
    
    # 检查catalog中的序列
    print(f"\n📊 当前catalog中的Money, Banking, & Finance序列:")
    
    try:
        with open('config/catalog_fred.yaml', 'r', encoding='utf-8') as f:
            catalog = yaml.safe_load(f)
        
        series = catalog.get('series', [])
        money_banking_series = []
        
        # Money, Banking, & Finance相关关键词
        keywords = {
            'Interest_Rates': ['DGS', 'DTB', 'SOFR', 'FEDFUNDS', 'DFF', 'TREASURY', 'RATE'],
            'Exchange_Rates': ['DTWEX', 'DEX', 'EXUSEU', 'EXUSUK', 'EXCHANGE'],
            'Monetary_Data': ['M1', 'M2', 'M3', 'BASE', 'RESBALNS', 'MONETARY'],
            'Financial_Indicators': ['VIX', 'NFCI', 'TEDRATE', 'LIBOR', 'FINANCIAL'],
            'Banking': ['TOTRESNS', 'TOTLL', 'TOTBKCR', 'TOTALSA', 'BANK'],
            'Business_Lending': ['BAML', 'BAMLEM', 'LENDING', 'COMMERCIAL'],
            'Foreign_Exchange_Intervention': ['FXI', 'INTGSB', 'INTGSN', 'INTERVENTION']
        }
        
        categorized = {subcat: [] for subcat in keywords.keys()}
        uncategorized = []
        
        for item in series:
            series_id = item.get('id', '')
            alias = item.get('alias', '')
            
            # 检查属于哪个子分类
            found_category = None
            for category, category_keywords in keywords.items():
                if any(keyword in series_id.upper() for keyword in category_keywords):
                    found_category = category
                    break
            
            if found_category:
                categorized[found_category].append({
                    'id': series_id,
                    'alias': alias
                })
            else:
                uncategorized.append({
                    'id': series_id,
                    'alias': alias
                })
        
        # 显示分类结果
        total_categorized = 0
        for category, series_list in categorized.items():
            if series_list:
                print(f"\n{category} ({len(series_list)} 个序列):")
                for item in series_list:
                    print(f"  {item['id']}: {item['alias']}")
                total_categorized += len(series_list)
        
        print(f"\n📈 对比分析:")
        print(f"FRED官方 vs 本地目录 vs 当前catalog:")
        for subcat, official_count in official_subcategories.items():
            local_count = local_subcategories.get(subcat, 0)
            catalog_count = len(categorized.get(subcat, []))
            print(f"  {subcat}:")
            print(f"    官方: {official_count:,} 个序列")
            print(f"    本地目录: {local_count} 个文件")
            print(f"    当前catalog: {catalog_count} 个序列")
        
        print(f"\n📊 总结:")
        print(f"  总官方序列数: {sum(official_subcategories.values()):,}")
        print(f"  当前catalog序列数: {total_categorized}")
        print(f"  覆盖率: {total_categorized/sum(official_subcategories.values())*100:.4f}%")
        
        # 建议
        print(f"\n💡 建议:")
        if total_categorized < 50:
            print("  - 当前catalog中Money, Banking, & Finance序列较少")
            print("  - 建议运行full_fred_discover.py获取更多序列")
            print("  - 或者手动添加重要的金融指标序列")
        
        # 检查是否需要下载更多数据
        missing_categories = []
        for subcat, catalog_count in categorized.items():
            if catalog_count == 0 and official_subcategories[subcat] > 100:
                missing_categories.append(subcat)
        
        if missing_categories:
            print(f"  - 以下子分类缺少序列: {', '.join(missing_categories)}")
            print("  - 建议重点补充这些分类的数据")
        
    except FileNotFoundError:
        print("❌ 未找到catalog_fred.yaml文件")
    except Exception as e:
        print(f"❌ 检查过程中出错: {e}")

if __name__ == "__main__":
    check_money_banking_complete()
