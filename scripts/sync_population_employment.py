#!/usr/bin/env python3
"""
同步Population, Employment, & Labor Markets所有子分类数据
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

# Population, Employment, & Labor Markets的子分类映射
POPULATION_EMPLOYMENT_SUBCATEGORIES = {
    12: "Current_Population_Survey_Household_Survey",
    11: "Current_Employment_Statistics_Establishment_Survey", 
    32250: "ADP_Employment",
    33500: "Education",
    33001: "Income_Distribution",
    32241: "Job_Openings_Labor_Turnover_JOLTS",
    33509: "Labor_Market_Conditions",
    104: "Population",
    2: "Productivity_Costs",
    33831: "Minimum_Wage",
    32240: "Weekly_Initial_Claims",
    33731: "Tax_Data"
}

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
            features_df['value_change'] = features_df['value'].pct_change()
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
    
    print("🔄 同步Population, Employment, & Labor Markets所有子分类数据...")
    print("=" * 80)
    
    # 查找所有目录文件
    config_dir = pathlib.Path(BASE) / "config"
    catalog_files = list(config_dir.glob("*_catalog.yaml"))
    
    # 过滤出Population, Employment, & Labor Markets相关的目录文件
    target_catalogs = []
    for catalog_file in catalog_files:
        filename = catalog_file.stem.lower()
        if any(keyword in filename for keyword in [
            'current_population_survey', 'adp_employment', 'education', 
            'income_distribution', 'job_openings', 'labor_market', 
            'population', 'minimum_wage', 'weekly_initial', 'tax_data'
        ]):
            target_catalogs.append(catalog_file)
    
    print(f"📁 找到 {len(target_catalogs)} 个目录文件:")
    for catalog_file in target_catalogs:
        print(f"  {catalog_file.name}")
    
    # 同步所有子分类
    total_success = 0
    total_series = 0
    
    for catalog_file in target_catalogs:
        subcategory_name = catalog_file.stem.replace('_catalog', '').replace('_', ' ').title()
        
        # 获取子分类的目录名
        subcategory_dir_name = None
        catalog_stem = catalog_file.stem.lower()
        
        # 直接映射
        catalog_mapping = {
            'current_population_survey_household_survey_catalog': 'Current_Population_Survey_Household_Survey',
            'adp_employment_catalog': 'ADP_Employment',
            'education_catalog': 'Education',
            'income_distribution_catalog': 'Income_Distribution',
            'job_openings_labor_turnover_jolts_catalog': 'Job_Openings_Labor_Turnover_JOLTS',
            'labor_market_conditions_catalog': 'Labor_Market_Conditions',
            'population_catalog': 'Population',
            'minimum_wage_catalog': 'Minimum_Wage',
            'weekly_initial_claims_catalog': 'Weekly_Initial_Claims',
            'tax_data_catalog': 'Tax_Data'
        }
        
        subcategory_dir_name = catalog_mapping.get(catalog_stem)
        
        if not subcategory_dir_name:
            print(f"❌ 无法确定 {catalog_file.name} 对应的子分类目录")
            continue
        
        success_count = sync_subcategory(subcategory_dir_name, catalog_file)
        total_success += success_count
        
        # 计算总系列数
        catalog = load_catalog(catalog_file)
        if catalog:
            total_series += len(catalog.get('series', []))
    
    print(f"\n🎉 Population, Employment, & Labor Markets同步完成!")
    print(f"📊 总计: {total_success}/{total_series} 个系列同步成功")
    
    if total_success == total_series:
        print("✅ 所有数据同步成功!")
    else:
        print(f"⚠️ 有 {total_series - total_success} 个系列同步失败")

if __name__ == "__main__":
    main()
