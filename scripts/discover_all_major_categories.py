#!/usr/bin/env python3
"""
发现所有主要分类的子分类并建立目录结构
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv
import time

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import category_children, category_series, polite_sleep

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# 主要分类及其子分类映射
MAJOR_CATEGORIES = {
    10: "Population_Employment_Labor_Markets",
    13: "International_Data", 
    15: "Prices",
    22: "Academic_Data",
    23: "US_Regional_Data",
    24: "Alternative_Measures",
    31: "International",
}

def create_category_directories(category_id: int, category_name: str, subcategories: List[Dict]):
    """创建分类目录结构"""
    
    print(f"\n🏗️ 创建 {category_name} 目录结构...")
    
    categories_root = pathlib.Path(BASE) / "data" / "fred" / "categories"
    
    for subcat in subcategories:
        subcat_id = subcat['id']
        subcat_name = subcat['name'].replace(' ', '_').replace('&', 'and').replace(',', '').replace('(', '').replace(')', '')
        
        # 创建子分类目录
        subcategory_path = categories_root / subcat_name
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
            info_content = f"""# {subcat_name.replace('_', ' ')}
subcategory:
  name: "{subcat_name}"
  description: "{subcat['name']}"
  category_id: {subcat_id}
  parent_category: "{category_name}"
  created_at: "{pathlib.Path().cwd()}"
"""
            info_file.write_text(info_content, encoding="utf-8")
        
        print(f"  ✅ {subcat_name}: 目录结构已创建")

def discover_subcategory_data(category_id: int, subcategory_name: str) -> List[Dict[str, Any]]:
    """发现子分类数据"""
    
    print(f"\n🔍 发现 {subcategory_name} 数据...")
    
    try:
        # 获取子分类的所有系列
        result = category_series(category_id, limit=100)
        series_list = result.get('seriess', [])
        
        print(f"  📊 {subcategory_name} 总系列数: {len(series_list)}")
        
        # 显示前5个系列
        for i, s in enumerate(series_list[:5]):
            series_id = s.get('id', '')
            title = s.get('title', '')
            frequency = s.get('frequency', '')
            print(f"    {i+1}. {series_id}: {title[:50]}... ({frequency})")
        
        if len(series_list) > 5:
            print(f"    ... 还有 {len(series_list) - 5} 个系列")
        
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
            'parent_category': 'Major_Category',
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

def process_major_category(category_id: int, category_name: str):
    """处理主要分类"""
    
    print(f"\n🔄 处理 {category_id}: {category_name}")
    print("=" * 60)
    
    try:
        # 获取子分类
        children_result = category_children(category_id)
        children = children_result.get('categories', [])
        
        if not children:
            print(f"  ❌ {category_name} 没有子分类")
            return
        
        print(f"  📂 找到 {len(children)} 个子分类")
        
        # 创建目录结构
        create_category_directories(category_id, category_name, children)
        
        # 发现和创建目录文件
        all_catalogs = []
        
        for child in children:
            child_id = child['id']
            child_name = child['name'].replace(' ', '_').replace('&', 'and').replace(',', '').replace('(', '').replace(')', '')
            
            # 发现数据
            series_list = discover_subcategory_data(child_id, child_name)
            
            # 创建目录
            catalog_file = create_subcategory_catalog(child_id, child_name, series_list)
            
            if catalog_file:
                all_catalogs.append(catalog_file)
            
            # 礼貌性延迟
            polite_sleep()
        
        print(f"\n✅ {category_name} 处理完成!")
        print(f"📊 创建了 {len(all_catalogs)} 个目录文件")
        
        return all_catalogs
        
    except Exception as e:
        print(f"❌ 处理 {category_name} 失败: {e}")
        return []

def main():
    """主函数"""
    
    print("🔍 发现所有主要分类的子分类并建立目录结构...")
    print("=" * 80)
    
    all_catalogs = []
    
    for category_id, category_name in MAJOR_CATEGORIES.items():
        catalogs = process_major_category(category_id, category_name)
        if catalogs:
            all_catalogs.extend(catalogs)
        
        # 分类间延迟
        time.sleep(1)
    
    print(f"\n🎉 所有主要分类处理完成!")
    print(f"📊 总计创建了 {len(all_catalogs)} 个目录文件")
    
    if all_catalogs:
        print(f"\n📁 创建的目录文件:")
        for catalog_file in all_catalogs:
            print(f"  {catalog_file}")
        
        print(f"\n🎯 下一步:")
        print(f"python -m scripts.sync_all_major_categories  # 下载所有数据")

if __name__ == "__main__":
    main()
