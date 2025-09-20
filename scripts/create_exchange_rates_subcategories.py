#!/usr/bin/env python3
"""
为Exchange Rates创建子分类目录结构
基于FRED官网的Exchange Rates子分类
"""

import os
import pathlib
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# Exchange Rates的子分类（基于FRED官网）
EXCHANGE_RATES_SUBCATEGORIES = {
    "Daily_Rates": {
        "description": "Daily Rates",
        "keywords": ["DEX", "DAILY"]
    },
    "Monthly_Rates": {
        "description": "Monthly Rates", 
        "keywords": ["MEX", "MONTHLY"]
    },
    "Annual_Rates": {
        "description": "Annual Rates",
        "keywords": ["AEX", "ANNUAL"]
    },
    "Trade_Weighted_Indexes": {
        "description": "Trade-Weighted Indexes",
        "keywords": ["DTWEX", "TRADE", "WEIGHTED"]
    },
    "By_Country": {
        "description": "By Country",
        "keywords": ["EXUSEU", "EXUSUK", "EXUSJP", "EXUSCA"]
    }
}

def create_exchange_rates_subcategory_directories():
    """创建Exchange Rates的子分类目录结构"""
    
    exchange_rates_path = pathlib.Path(BASE) / "data" / "fred" / "categories" / "Exchange_Rates"
    
    print("🏗️ 创建Exchange Rates子分类目录结构...")
    print("=" * 60)
    
    for subcategory_name, subcategory_info in EXCHANGE_RATES_SUBCATEGORIES.items():
        # 创建子分类目录
        subcategory_path = exchange_rates_path / subcategory_name
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
            info_content = f"""# {subcategory_info['description']}
subcategory:
  name: "{subcategory_name}"
  description: "{subcategory_info['description']}"
  keywords: {subcategory_info['keywords']}
  parent_category: "Exchange_Rates"
  created_at: "{pathlib.Path().cwd()}"
"""
            info_file.write_text(info_content, encoding="utf-8")
        
        print(f"✓ {subcategory_name}: {subcategory_info['description']}")
    
    print(f"\n✅ Exchange Rates子分类目录创建完成!")
    print(f"📁 共创建 {len(EXCHANGE_RATES_SUBCATEGORIES)} 个子分类目录")
    
    # 显示目录结构
    print(f"\n📂 目录结构:")
    for subcategory_name in EXCHANGE_RATES_SUBCATEGORIES.keys():
        subcategory_path = exchange_rates_path / subcategory_name
        print(f"  Exchange_Rates/{subcategory_name}/")
        print(f"    ├── series/")
        print(f"    └── metadata/")

def main():
    """主函数"""
    create_exchange_rates_subcategory_directories()

if __name__ == "__main__":
    main()
