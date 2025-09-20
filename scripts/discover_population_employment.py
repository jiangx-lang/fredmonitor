#!/usr/bin/env python3
"""
发现Population, Employment, & Labor Markets子分类数据
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import category_children, category_series, polite_sleep

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

def discover_subcategory_series(category_id: int, subcategory_name: str) -> List[Dict[str, Any]]:
    """发现特定子分类的数据系列"""
    
    print(f"\n🔍 发现 {subcategory_name} 数据...")
    
    try:
        # 获取子分类的所有系列
        result = category_series(category_id, limit=100)
        series_list = result.get('seriess', [])
        
        print(f"  {subcategory_name} 总系列数: {len(series_list)}")
        
        # 显示前10个系列
        for i, s in enumerate(series_list[:10]):
            series_id = s.get('id', '')
            title = s.get('title', '')
            frequency = s.get('frequency', '')
            print(f"    {i+1}. {series_id}: {title} ({frequency})")
        
        if len(series_list) > 10:
            print(f"    ... 还有 {len(series_list) - 10} 个系列")
        
        return series_list
        
    except Exception as e:
        print(f"  ❌ 获取 {subcategory_name} 数据失败: {e}")
        return []

def create_subcategory_catalog(category_id: int, subcategory_name: str, series_list: List[Dict[str, Any]]):
    """创建子分类目录文件"""
    
    if not series_list:
        print(f"  ❌ {subcategory_name} 没有数据系列")
        return None
    
    print(f"\n📝 创建 {subcategory_name} 目录...")
    
    # 构建目录结构
    catalog = {
        'metadata': {
            'name': subcategory_name.replace('_', ' '),
            'description': f'{subcategory_name.replace("_", " ")} 数据',
            'category_id': category_id,
            'parent_category': 'Population_Employment_Labor_Markets',
            'total_series': len(series_list),
            'created_at': '2025-09-13'
        },
        'series': []
    }
    
    for s in series_list:
        series_config = {
            'id': s.get('id', ''),
            'alias': s.get('title', ''),
            'category_id': category_id,
            'subcategory': subcategory_name,
            'frequency': s.get('frequency', ''),
            'units': s.get('units', ''),
            'seasonal_adjustment': s.get('seasonal_adjustment', ''),
            'freshness_days': 7
        }
        catalog['series'].append(series_config)
    
    # 保存目录文件
    catalog_file = pathlib.Path(BASE) / "config" / f"{subcategory_name.lower()}_catalog.yaml"
    with open(catalog_file, 'w', encoding='utf-8') as f:
        yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True)
    
    print(f"  💾 {subcategory_name} 目录已保存到: {catalog_file}")
    
    return catalog_file

def create_subcategory_directories():
    """创建子分类目录结构"""
    
    print("🏗️ 创建Population, Employment, & Labor Markets子分类目录结构...")
    print("=" * 70)
    
    categories_root = pathlib.Path(BASE) / "data" / "fred" / "categories"
    
    for category_id, subcategory_name in POPULATION_EMPLOYMENT_SUBCATEGORIES.items():
        # 创建子分类目录
        subcategory_path = categories_root / subcategory_name
        subcategory_path.mkdir(parents=True, exist_ok=True)
        
        # 创建series子目录
        series_path = subcategory_path / "series"
        series_path.mkdir(parents=True, exist_ok=True)
        
        # 创建metadata子目录
        metadata_path = subcategory_path / "metadata"
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        # 创建子分类信息文件
        info_file = metadata_path / "subcategory_info.yaml"
        if not info_file.exists():
            info_content = f"""# {subcategory_name.replace('_', ' ')}
subcategory:
  name: "{subcategory_name}"
  description: "{subcategory_name.replace('_', ' ')}"
  category_id: {category_id}
  parent_category: "Population_Employment_Labor_Markets"
  created_at: "{pathlib.Path().cwd()}"
"""
            info_file.write_text(info_content, encoding="utf-8")
        
        print(f"✓ {subcategory_name}: 目录结构已创建")
    
    print(f"\n✅ 子分类目录结构创建完成!")
    print(f"📁 共创建 {len(POPULATION_EMPLOYMENT_SUBCATEGORIES)} 个子分类目录")

def main():
    """主函数"""
    
    print("🔍 发现Population, Employment, & Labor Markets子分类数据...")
    print("=" * 70)
    
    # 首先创建目录结构
    create_subcategory_directories()
    
    # 然后发现数据
    all_catalogs = []
    
    for category_id, subcategory_name in POPULATION_EMPLOYMENT_SUBCATEGORIES.items():
        # 发现系列
        series_list = discover_subcategory_series(category_id, subcategory_name)
        
        # 创建目录
        catalog_file = create_subcategory_catalog(category_id, subcategory_name, series_list)
        
        if catalog_file:
            all_catalogs.append(catalog_file)
        
        # 礼貌性延迟
        polite_sleep()
    
    print(f"\n✅ Population, Employment, & Labor Markets子分类发现完成!")
    print(f"📊 创建了 {len(all_catalogs)} 个目录文件")
    
    if all_catalogs:
        print(f"\n📁 创建的目录文件:")
        for catalog_file in all_catalogs:
            print(f"  {catalog_file}")
        
        print(f"\n🎯 下一步:")
        print(f"python -m scripts.sync_population_employment  # 下载所有子分类数据")

if __name__ == "__main__":
    main()
