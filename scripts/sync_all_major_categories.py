#!/usr/bin/env python3
"""
同步所有主要分类的数据
"""

import os
import yaml
import pathlib
import pandas as pd
from typing import Dict, Any, List
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import series_observations, series_info, polite_sleep

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

def ensure_series_dir(series_id: str, subcategory_name: str) -> pathlib.Path:
    """确保系列目录存在"""
    p = pathlib.Path(BASE) / "data" / "fred" / "categories" / subcategory_name / "series" / series_id
    (p / "notes" / "attachments").mkdir(parents=True, exist_ok=True)
    custom_notes_file = p / "notes" / "custom_notes.md"
    if not custom_notes_file.exists():
        custom_notes_file.write_text("", encoding="utf-8")
    return p

def sync_series(series_id: str, subcategory_name: str) -> bool:
    """同步单个系列数据"""
    try:
        # 确保目录存在
        series_dir = ensure_series_dir(series_id, subcategory_name)
        
        # 获取系列信息
        info = series_info(series_id)
        if not info:
            print(f"  ❌ {series_id}: 无法获取系列信息")
            return False
        
        # 获取观测数据
        obs = series_observations(series_id)
        if not obs or not obs.get('observations'):
            print(f"  ❌ {series_id}: 无观测数据")
            return False
        
        # 保存元数据
        meta_file = series_dir / "meta.json"
        with open(meta_file, 'w', encoding='utf-8') as f:
            import json
            json.dump(info, f, indent=2, ensure_ascii=False)
        
        # 保存原始数据
        raw_file = series_dir / "raw.csv"
        observations = obs['observations']
        df = pd.DataFrame(observations)
        df.to_csv(raw_file, index=False, encoding='utf-8')
        
        # 创建特征数据
        features_file = series_dir / "features.parquet"
        if len(df) > 0:
            # 转换日期列
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 创建特征
            features_df = pd.DataFrame({
                'date': df['date'],
                'value': pd.to_numeric(df['value'], errors='coerce'),
                'series_id': series_id
            })
            
            # 计算基本统计特征
            features_df['value_ma7'] = features_df['value'].rolling(window=7, min_periods=1).mean()
            features_df['value_ma30'] = features_df['value'].rolling(window=30, min_periods=1).mean()
            features_df['value_change'] = features_df['value'].pct_change(fill_method=None)
            features_df['value_change_ma7'] = features_df['value_change'].rolling(window=7, min_periods=1).mean()
            
            features_df.to_parquet(features_file, index=False)
        
        # 创建事实表
        fact_sheet_file = series_dir / "fact_sheet.md"
        fact_sheet_content = f"""# {info.get('title', series_id)}

## 基本信息
- **系列ID**: {series_id}
- **标题**: {info.get('title', 'N/A')}
- **频率**: {info.get('frequency', 'N/A')}
- **单位**: {info.get('units', 'N/A')}
- **季节性调整**: {info.get('seasonal_adjustment', 'N/A')}
- **最后更新**: {info.get('last_updated', 'N/A')}

## 数据概览
- **观测数量**: {len(observations)}
- **开始日期**: {observations[0]['date'] if observations else 'N/A'}
- **结束日期**: {observations[-1]['date'] if observations else 'N/A'}

## 描述
{info.get('notes', 'N/A')}

## 数据来源
- **来源**: {info.get('source', 'N/A')}
- **来源链接**: {info.get('source_link', 'N/A')}
"""
        fact_sheet_file.write_text(fact_sheet_content, encoding='utf-8')
        
        print(f"  ✅ {series_id}: 同步完成")
        return True
        
    except Exception as e:
        print(f"  ❌ {series_id}: 同步失败 - {e}")
        return False

def load_catalog(catalog_file: pathlib.Path) -> Dict[str, Any]:
    """加载目录文件"""
    try:
        with open(catalog_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 无法加载目录文件 {catalog_file}: {e}")
        return {}

def get_subcategory_name_from_catalog(catalog_file: pathlib.Path) -> str:
    """从目录文件名获取子分类名称"""
    catalog_stem = catalog_file.stem.lower()
    
    # 移除_catalog后缀
    if catalog_stem.endswith('_catalog'):
        catalog_stem = catalog_stem[:-8]
    
    # 转换为目录名格式
    subcategory_name = catalog_stem.replace('_', ' ').title().replace(' ', '_')
    
    # 特殊处理
    special_cases = {
        'u.s._international_finance': 'U.S._International_Finance',
        'frb_rates_-_discount_fed_funds_primary_credit': 'FRB_Rates_-_discount_fed_funds_primary_credit',
        'treasury_inflation-indexed_securities': 'Treasury_Inflation-Indexed_Securities',
        'net_charge-offs_and_charge-off_rates': 'Net_Charge-Offs_and_Charge-Off_Rates',
        'securities_loans_and_other_assets_and_liabilities_held_by_fed': 'Securities_Loans_and_Other_Assets_and_Liabilities_Held_by_Fed',
        'senior_credit_officer_opinion_survey': 'Senior_Credit_Officer_Opinion_Survey',
        'senior_loan_officer_survey': 'Senior_Loan_Officer_Survey',
        'job_openings_and_labor_turnover_jolts': 'Job_Openings_and_Labor_Turnover_JOLTS',
        'productivity_and_costs': 'Productivity_and_Costs',
        'trade-weighted_indexes': 'Trade-Weighted_Indexes',
        '8th_district_banking_performance': '8th_District_Banking_Performance',
        'm2_minus_small_time_deposits': 'M2_Minus_Small_Time_Deposits',
        'factors_affecting_reserve_balances': 'Factors_Affecting_Reserve_Balances',
        'securities_loans_assets_liabilities': 'Securities_Loans_Assets_Liabilities',
        'current_population_survey_household_survey': 'Current_Population_Survey_Household_Survey',
        'current_employment_statistics_establishment_survey': 'Current_Employment_Statistics_Establishment_Survey',
        'job_openings_labor_turnover_jolts': 'Job_Openings_Labor_Turnover_JOLTS',
        'labor_market_conditions': 'Labor_Market_Conditions',
        'weekly_initial_claims': 'Weekly_Initial_Claims',
        'tax_data': 'Tax_Data',
        'income_distribution': 'Income_Distribution',
        'minimum_wage': 'Minimum_Wage',
        'population': 'Population',
        'education': 'Education',
        'adp_employment': 'ADP_Employment',
        'exports': 'Exports',
        'imports': 'Imports',
        'income_payments_and_receipts': 'Income_Payments_and_Receipts',
        'international_investment_position': 'International_Investment_Position',
        'trade_balance': 'Trade_Balance',
        'daily_rates': 'Daily_Rates',
        'monthly_rates': 'Monthly_Rates',
        'annual_rates': 'Annual_Rates',
        'by_country': 'By_Country',
        'ameribor_benchmark_rates': 'AMERIBOR_Benchmark_Rates',
        'automobile_loan_rates': 'Automobile_Loan_Rates',
        'bankers_acceptance_rate': 'Bankers_Acceptance_Rate',
        'certificates_of_deposit': 'Certificates_of_Deposit',
        'commercial_paper': 'Commercial_Paper',
        'corporate_bonds': 'Corporate_Bonds',
        'credit_card_loan_rates': 'Credit_Card_Loan_Rates',
        'eonia_rates': 'EONIA_Rates',
        'euro_short-term_rate': 'Euro_Short-Term_Rate',
        'eurodollar_deposits': 'Eurodollar_Deposits',
        'interest_checking_accounts': 'Interest_Checking_Accounts',
        'interest_rate_spreads': 'Interest_Rate_Spreads',
        'interest_rate_swaps': 'Interest_Rate_Swaps',
        'long-term_securities': 'Long-Term_Securities',
        'monetary_policy': 'Monetary_Policy',
        'money_market_accounts': 'Money_Market_Accounts',
        'mortgage_rates': 'Mortgage_Rates',
        'personal_loan_rates': 'Personal_Loan_Rates',
        'prime_bank_loan_rate': 'Prime_Bank_Loan_Rate',
        'saving_accounts': 'Saving_Accounts',
        'sonia_rates': 'SONIA_Rates',
        'treasury_bills': 'Treasury_Bills',
        'treasury_constant_maturity': 'Treasury_Constant_Maturity',
        'banking_indexes': 'Banking_Indexes',
        'commercial_banking': 'Commercial_Banking',
        'consumer_credit': 'Consumer_Credit',
        'delinquencies_and_delinquency_rates': 'Delinquencies_and_Delinquency_Rates',
        'failures_and_assistance_transactions': 'Failures_and_Assistance_Transactions',
        'mortgage_debt_outstanding': 'Mortgage_Debt_Outstanding',
        'securities_and_investments': 'Securities_and_Investments',
        'monetary_base': 'Monetary_Base',
        'reserves': 'Reserves',
        'm1_and_components': 'M1_and_Components',
        'm2_and_components': 'M2_and_Components',
        'm3_and_components': 'M3_and_Components',
        'mzm': 'MZM',
        'memorandum_items': 'Memorandum_Items',
        'money_velocity': 'Money_Velocity',
        'borrowings': 'Borrowings',
        'commodity_based': 'Commodity_Based',
        'industry_based': 'Industry_Based'
    }
    
    return special_cases.get(catalog_stem, subcategory_name)

def sync_subcategory(subcategory_name: str, catalog_file: pathlib.Path) -> int:
    """同步单个子分类的所有数据"""
    
    print(f"\n🔄 同步 {subcategory_name} 数据...")
    print("=" * 60)
    
    # 加载目录
    catalog = load_catalog(catalog_file)
    if not catalog:
        return 0
    
    series_list = catalog.get('series', [])
    if not series_list:
        print(f"  ❌ {subcategory_name} 没有系列数据")
        return 0
    
    print(f"  📊 总系列数: {len(series_list)}")
    
    # 同步所有系列
    success_count = 0
    failed_series = []
    
    for i, series_config in enumerate(series_list, 1):
        series_id = series_config.get('id', '')
        if not series_id:
            continue
            
        print(f"  [{i}/{len(series_list)}] 同步 {series_id}...")
        
        if sync_series(series_id, subcategory_name):
            success_count += 1
        else:
            failed_series.append(series_id)
        
        # 礼貌性延迟
        polite_sleep()
    
    print(f"\n✅ {subcategory_name} 同步完成!")
    print(f"  📈 成功: {success_count}/{len(series_list)}")
    
    if failed_series:
        print(f"  ❌ 失败系列: {', '.join(failed_series)}")
    
    return success_count

def main():
    """主函数"""
    
    print("🔄 同步所有主要分类数据...")
    print("=" * 80)
    
    # 查找所有目录文件
    config_dir = pathlib.Path(BASE) / "config"
    catalog_files = list(config_dir.glob("*_catalog.yaml"))
    
    # 过滤出主要分类相关的目录文件（排除已处理的）
    excluded_files = {
        'money_banking_catalog.yaml',
        'real_m1_catalog.yaml',
        'm2_components_catalog.yaml',
        'm2_minus_small_time_deposits_catalog.yaml',
        'm3_components_catalog.yaml',
        'monetary_base_catalog.yaml',
        'mzm_catalog.yaml',
        'reserves_catalog.yaml',
        'securities_loans_assets_liabilities_catalog.yaml'
    }
    
    target_catalogs = [f for f in catalog_files if f.name not in excluded_files]
    
    print(f"📁 找到 {len(target_catalogs)} 个目录文件:")
    for catalog_file in target_catalogs:
        print(f"  {catalog_file.name}")
    
    # 同步所有子分类
    total_success = 0
    total_series = 0
    
    for catalog_file in target_catalogs:
        subcategory_name = get_subcategory_name_from_catalog(catalog_file)
        
        success_count = sync_subcategory(subcategory_name, catalog_file)
        total_success += success_count
        
        # 计算总系列数
        catalog = load_catalog(catalog_file)
        if catalog:
            total_series += len(catalog.get('series', []))
    
    print(f"\n🎉 所有主要分类同步完成!")
    print(f"📊 总计: {total_success}/{total_series} 个系列同步成功")
    
    if total_success == total_series:
        print("✅ 所有数据同步成功!")
    else:
        print(f"⚠️ 有 {total_series - total_success} 个系列同步失败")

if __name__ == "__main__":
    main()
