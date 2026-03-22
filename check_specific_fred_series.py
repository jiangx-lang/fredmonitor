#!/usr/bin/env python3
"""
检查用户指定的具体FRED系列ID覆盖情况
"""

import os
import pathlib
import pandas as pd
from typing import Dict, List, Set

def check_series_exists(series_id: str) -> bool:
    """检查系列是否存在"""
    series_path = pathlib.Path("data/fred/categories")
    
    # 在所有分类中搜索该系列
    for category_dir in series_path.iterdir():
        if category_dir.is_dir():
            series_dir = category_dir / "series" / series_id
            if series_dir.exists() and (series_dir / "raw.csv").exists():
                return True
    return False

def get_series_info(series_id: str) -> Dict:
    """获取系列信息"""
    series_path = pathlib.Path("data/fred/categories")
    
    for category_dir in series_path.iterdir():
        if category_dir.is_dir():
            series_dir = category_dir / "series" / series_id
            if series_dir.exists():
                meta_file = series_dir / "meta.json"
                if meta_file.exists():
                    import json
                    return json.loads(meta_file.read_text(encoding='utf-8'))
    return {}

def check_specific_fred_series():
    """检查用户指定的具体FRED系列ID覆盖情况"""
    
    print("🔍 检查用户指定的具体FRED系列ID覆盖情况...")
    print("=" * 80)
    
    # 用户指定的具体系列ID
    specific_series = {
        "利率与收益率": {
            "FEDFUNDS": "联邦基金利率",
            "T10Y3M": "10年期-3个月国债收益率曲线",
            "T10Y2Y": "10年期-2年期国债收益率曲线", 
            "MORTGAGE30US": "30年期抵押贷款利率"
        },
        "信贷与违约率": {
            "TDSP": "家庭债务偿付比率",
            "NCBDBIQ027S": "企业债/GDP",
            "DRSFRMACBS": "房贷违约率",
            "DRBLACBS": "房贷违约率(备用)"
        },
        "流动性与资金市场": {
            "TEDRATE": "TED利差",
            "CPN3M": "3个月商业票据利率",
            "SOFRON": "SOFR隔夜利率"
        },
        "信用利差与风险溢价": {
            "BAMLH0A0HYM2": "高收益债OAS",
            "BAA10YM": "Baa级企业债收益率",
            "AAA10YM": "Aaa级企业债收益率"
        },
        "宏观与信心指标": {
            "NAPM": "ISM制造业PMI",
            "UMCSENT": "密歇根消费者信心",
            "HOUST": "新屋开工",
            "INDPRO": "工业产出"
        },
        "综合金融压力指数": {
            "STLFSI4": "圣路易斯金融压力指数",
            "NFCI": "芝加哥金融状况指数"
        }
    }
    
    # 检查数据覆盖情况
    total_series = 0
    available_series = 0
    coverage_by_category = {}
    missing_series = []
    
    print("📊 具体系列ID覆盖情况检查结果:")
    print("-" * 80)
    
    for category, series_dict in specific_series.items():
        category_total = len(series_dict)
        category_available = 0
        category_missing = []
        
        print(f"\n🔍 {category}:")
        
        for series_id, description in series_dict.items():
            total_series += 1
            if check_series_exists(series_id):
                category_available += 1
                available_series += 1
                
                # 获取详细信息
                info = get_series_info(series_id)
                title = info.get('title', 'N/A')
                last_updated = info.get('last_updated', 'N/A')
                
                print(f"  ✅ {series_id}: {description}")
                print(f"     标题: {title}")
                print(f"     最后更新: {last_updated}")
            else:
                category_missing.append(series_id)
                missing_series.append(series_id)
                print(f"  ❌ {series_id}: {description}")
        
        coverage_by_category[category] = {
            'total': category_total,
            'available': category_available,
            'coverage_rate': category_available / category_total if category_total > 0 else 0,
            'missing': category_missing
        }
        
        print(f"  📈 覆盖率: {category_available}/{category_total} ({category_available/category_total*100:.1f}%)")
    
    # 总体统计
    overall_coverage = available_series / total_series if total_series > 0 else 0
    
    print(f"\n🎯 总体数据覆盖情况:")
    print("=" * 80)
    print(f"📊 总系列数: {total_series}")
    print(f"✅ 可用系列数: {available_series}")
    print(f"❌ 缺失系列数: {total_series - available_series}")
    print(f"📈 总体覆盖率: {overall_coverage*100:.1f}%")
    
    # 按类别统计
    print(f"\n📋 按类别覆盖率:")
    print("-" * 80)
    for category, stats in coverage_by_category.items():
        print(f"{category}: {stats['available']}/{stats['total']} ({stats['coverage_rate']*100:.1f}%)")
    
    # 缺失数据汇总
    if missing_series:
        print(f"\n❌ 缺失的关键数据系列:")
        print("-" * 80)
        for series_id in missing_series:
            print(f"  {series_id}")
    
    # 检查是否有替代系列ID
    print(f"\n🔍 检查可能的替代系列ID:")
    print("-" * 80)
    
    # 一些常见的替代系列ID
    alternative_checks = {
        "T10Y3M": ["T10Y3M", "T10Y3MM"],
        "T10Y2Y": ["T10Y2Y", "T10Y2YM"], 
        "SOFRON": ["SOFR", "SOFRON"],
        "CPN3M": ["CPF3M", "CPN3M"],
        "BAMLH0A0HYM2": ["BAMLH0A0HYM2", "BAMLH0A0HYM"],
        "BAA10YM": ["BAA", "BAA10YM"],
        "AAA10YM": ["AAA", "AAA10YM"],
        "NAPM": ["NAPM", "PMI"],
        "STLFSI4": ["STLFSI4", "STLFSI3"],
        "INDPRO": ["INDPRO", "INDPROD"]
    }
    
    for original_id, alternatives in alternative_checks.items():
        if original_id in missing_series:
            print(f"\n🔍 寻找 {original_id} 的替代系列:")
            for alt_id in alternatives:
                if check_series_exists(alt_id):
                    info = get_series_info(alt_id)
                    title = info.get('title', 'N/A')
                    print(f"  ✅ 找到替代: {alt_id} - {title}")
                else:
                    print(f"  ❌ 无替代: {alt_id}")
    
    # 建议
    print(f"\n💡 数据补充建议:")
    print("-" * 80)
    if overall_coverage >= 0.9:
        print("✅ 数据覆盖优秀，可以开始构建危机预警系统")
    elif overall_coverage >= 0.7:
        print("⚠️ 数据覆盖良好，建议补充关键缺失数据")
    else:
        print("❌ 数据覆盖不足，需要大量补充数据")
    
    return {
        'total_series': total_series,
        'available_series': available_series,
        'coverage_rate': overall_coverage,
        'coverage_by_category': coverage_by_category,
        'missing_series': missing_series
    }

if __name__ == "__main__":
    result = check_specific_fred_series()
