#!/usr/bin/env python3
"""
检查危机预警检测框架的数据覆盖情况
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

def check_crisis_detection_coverage():
    """检查危机预警检测框架的数据覆盖情况"""
    
    print("🔍 检查危机预警检测框架的数据覆盖情况...")
    print("=" * 80)
    
    # 定义20个危机预警检测项目及其FRED数据源
    crisis_detection_series = {
        # 一、信贷与资产市场类
        "信贷快速扩张": {
            "TOTLL": "Total Loans and Leases at All Commercial Banks",
            "TOTCI": "Total Consumer Credit Outstanding", 
            "TOTALSA": "Total Consumer Credit Outstanding (Seasonally Adjusted)"
        },
        "贷款标准放松": {
            "DEMAUTO": "Net Percentage of Domestic Banks Reporting Stronger Demand for Auto Loans",
            "DEMCC": "Net Percentage of Domestic Banks Reporting Stronger Demand for Credit Card Loans",
            "DRISCFLM": "Net Percentage of Domestic Banks Increasing Spreads of Prime Loans"
        },
        "贷款违约率上升": {
            "DALLACBEP": "Asset Quality Measures, Delinquencies on All Loans",
            "DALLCACBEP": "Asset Quality Measures, Delinquencies on All Loans",
            "DALLCCACBEP": "Asset Quality Measures, Delinquencies on All Loans"
        },
        "资产价格泡沫": {
            "SP500": "S&P 500",
            "CSUSHPINSA": "S&P/Case-Shiller U.S. National Home Price Index",
            "MEDLISPRI": "Median Sales Price of Houses Sold for the United States",
            "MEHOINUSA646N": "Real Median Household Income in the United States"
        },
        "借款成本/利率上升": {
            "FEDFUNDS": "Federal Funds Effective Rate",
            "DGS10": "10-Year Treasury Constant Maturity Rate",
            "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average in the United States",
            "DPRIME": "Bank Prime Loan Rate"
        },
        
        # 二、流动性与资金市场类
        "隔夜/回购利率飙升": {
            "SOFR": "Secured Overnight Financing Rate",
            "EFFR": "Effective Federal Funds Rate",
            "CPF3M": "3-Month Commercial Paper Rate",
            "DTB3": "3-Month Treasury Bill Secondary Market Rate"
        },
        "TED利差": {
            "TEDRATE": "TED Spread",
            "CPF3M": "3-Month Commercial Paper Rate",
            "DTB3": "3-Month Treasury Bill Secondary Market Rate"
        },
        "飞向安全现象": {
            "DGS10": "10-Year Treasury Constant Maturity Rate",
            "DGS2": "2-Year Treasury Constant Maturity Rate",
            "DTB3": "3-Month Treasury Bill Secondary Market Rate",
            "SP500": "S&P 500"
        },
        
        # 三、信用与杠杆类
        "信用利差扩大": {
            "BAA": "Moody's Seasoned Baa Corporate Bond Yield",
            "AAA": "Moody's Seasoned Aaa Corporate Bond Yield",
            "BAMLC0A0CM": "ICE BofA US Corporate Index Option-Adjusted Spread"
        },
        "非金融部门杠杆上升": {
            "TOTALSA": "Total Consumer Credit Outstanding",
            "GDP": "Gross Domestic Product",
            "TDSP": "Household Debt Service Payments as a Percent of Disposable Personal Income",
            "NFCI": "Chicago Fed National Financial Conditions Index"
        },
        "银行贷款/存款错配": {
            "TOTLL": "Total Loans and Leases at All Commercial Banks",
            "TOTRESNS": "Reserves of Depository Institutions: Total",
            "TOTDD": "Total Deposits at All Commercial Banks"
        },
        
        # 四、收益率曲线与宏观基本面
        "收益率曲线倒挂": {
            "DGS10": "10-Year Treasury Constant Maturity Rate",
            "DGS2": "2-Year Treasury Constant Maturity Rate",
            "DTB3": "3-Month Treasury Bill Secondary Market Rate"
        },
        "信用增长放缓": {
            "TOTLL": "Total Loans and Leases at All Commercial Banks",
            "TOTALSA": "Total Consumer Credit Outstanding"
        },
        "宏观衰退信号": {
            "UMCSENT": "University of Michigan: Consumer Sentiment",
            "HOUST": "Housing Starts: Total: New Privately Owned Housing Units Started",
            "PAYEMS": "All Employees, Total Nonfarm"
        },
        
        # 五、外部失衡与政策
        "经常账户赤字恶化": {
            "BOPGSTB": "Balance on Current Account",
            "GDP": "Gross Domestic Product"
        },
        "美元流动性紧缩": {
            "DTWEXBGS": "Nominal Broad U.S. Dollar Index",
            "DTWEXM": "Nominal Major Currencies U.S. Dollar Index"
        },
        "货币政策急转弯": {
            "FEDFUNDS": "Federal Funds Effective Rate",
            "WALCL": "Assets: Total Assets: Total Assets (Less Eliminations from Consolidation)"
        },
        
        # 六、综合指数与市场情绪
        "金融压力指数": {
            "STLFSI4": "St. Louis Fed Financial Stress Index",
            "NFCI": "Chicago Fed National Financial Conditions Index"
        },
        "市场波动率": {
            "VIXCLS": "CBOE Volatility Index: VIX",
            "MOVE": "ICE BofA MOVE Index"
        },
        "投资者信心恶化": {
            "UMCSENT": "University of Michigan: Consumer Sentiment",
            "UMCSENT1": "University of Michigan: Consumer Sentiment"
        }
    }
    
    # 检查数据覆盖情况
    total_series = 0
    available_series = 0
    coverage_by_category = {}
    
    print("📊 数据覆盖情况检查结果:")
    print("-" * 80)
    
    for category, series_dict in crisis_detection_series.items():
        category_total = len(series_dict)
        category_available = 0
        missing_series = []
        
        print(f"\n🔍 {category}:")
        
        for series_id, description in series_dict.items():
            total_series += 1
            if check_series_exists(series_id):
                category_available += 1
                available_series += 1
                print(f"  ✅ {series_id}: {description}")
            else:
                missing_series.append(series_id)
                print(f"  ❌ {series_id}: {description}")
        
        coverage_by_category[category] = {
            'total': category_total,
            'available': category_available,
            'coverage_rate': category_available / category_total if category_total > 0 else 0,
            'missing': missing_series
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
    print(f"\n❌ 缺失的关键数据系列:")
    print("-" * 80)
    missing_critical = []
    for category, stats in coverage_by_category.items():
        if stats['missing']:
            print(f"{category}: {', '.join(stats['missing'])}")
            missing_critical.extend(stats['missing'])
    
    # 建议
    print(f"\n💡 数据补充建议:")
    print("-" * 80)
    if overall_coverage >= 0.8:
        print("✅ 数据覆盖良好，可以开始构建危机预警系统")
    elif overall_coverage >= 0.6:
        print("⚠️ 数据覆盖中等，建议补充关键缺失数据")
    else:
        print("❌ 数据覆盖不足，需要大量补充数据")
    
    # 检查是否有重复的系列ID
    all_series_ids = []
    for series_dict in crisis_detection_series.values():
        all_series_ids.extend(series_dict.keys())
    
    unique_series = set(all_series_ids)
    duplicate_count = len(all_series_ids) - len(unique_series)
    
    print(f"\n📝 数据系列统计:")
    print(f"总引用次数: {len(all_series_ids)}")
    print(f"唯一系列数: {len(unique_series)}")
    print(f"重复引用: {duplicate_count}")
    
    return {
        'total_series': total_series,
        'available_series': available_series,
        'coverage_rate': overall_coverage,
        'coverage_by_category': coverage_by_category,
        'missing_critical': missing_critical
    }

if __name__ == "__main__":
    result = check_crisis_detection_coverage()
