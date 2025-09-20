#!/usr/bin/env python3
"""
为Monetary Data创建子分类目录结构
基于FRED官网的Monetary Data子分类
"""

import os
import pathlib
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# Monetary Data的子分类（基于FRED官网）
MONETARY_SUBCATEGORIES = {
    "Monetary_Base": {
        "description": "Monetary Base (27 series)",
        "keywords": ["BASE", "MBASE", "MONETARYBASE"]
    },
    "Reserves": {
        "description": "Reserves (61 series)", 
        "keywords": ["RESBALNS", "RESERVES", "TOTRESNS"]
    },
    "M1_Components": {
        "description": "M1 and Components (72 series)",
        "keywords": ["M1", "M1SL", "M1NS"]
    },
    "M2_Components": {
        "description": "M2 and Components (50 series)",
        "keywords": ["M2", "M2SL", "M2NS"]
    },
    "M2_Minus_Small_Time_Deposits": {
        "description": "M2 Minus Small Time Deposits (7 series)",
        "keywords": ["M2MSL", "M2MNS"]
    },
    "M3_Components": {
        "description": "M3 and Components (60 series)",
        "keywords": ["M3", "M3SL", "M3NS"]
    },
    "MZM": {
        "description": "MZM (10 series)",
        "keywords": ["MZM", "MZMSL", "MZMNS"]
    },
    "Memorandum_Items": {
        "description": "Memorandum Items (23 series)",
        "keywords": ["MEMO", "MEMORANDUM"]
    },
    "Money_Velocity": {
        "description": "Money Velocity (3 series)",
        "keywords": ["VELOCITY", "VELOCITYM1", "VELOCITYM2"]
    },
    "Borrowings": {
        "description": "Borrowings (18 series)",
        "keywords": ["BORROW", "BORROWINGS"]
    },
    "Factors_Affecting_Reserve_Balances": {
        "description": "Factors Affecting Reserve Balances (650 series)",
        "keywords": ["FARB", "FACTORS", "RESERVE"]
    },
    "Securities_Loans_Assets_Liabilities": {
        "description": "Securities, Loans, & Other Assets & Liabilities Held by Fed (203 series)",
        "keywords": ["SECURITIES", "LOANS", "ASSETS", "LIABILITIES"]
    }
}

def create_monetary_subcategory_directories():
    """创建Monetary Data的子分类目录结构"""
    
    monetary_data_path = pathlib.Path(BASE) / "data" / "fred" / "categories" / "Monetary_Data"
    
    print("🏗️ 创建Monetary Data子分类目录结构...")
    print("=" * 60)
    
    for subcategory_name, subcategory_info in MONETARY_SUBCATEGORIES.items():
        # 创建子分类目录
        subcategory_path = monetary_data_path / subcategory_name
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
  parent_category: "Monetary_Data"
  created_at: "{pathlib.Path().cwd()}"
"""
            info_file.write_text(info_content, encoding="utf-8")
        
        print(f"✓ {subcategory_name}: {subcategory_info['description']}")
    
    print(f"\n✅ Monetary Data子分类目录创建完成!")
    print(f"📁 共创建 {len(MONETARY_SUBCATEGORIES)} 个子分类目录")
    
    # 显示目录结构
    print(f"\n📂 目录结构:")
    for subcategory_name in MONETARY_SUBCATEGORIES.keys():
        subcategory_path = monetary_data_path / subcategory_name
        print(f"  Monetary_Data/{subcategory_name}/")
        print(f"    ├── series/")
        print(f"    └── metadata/")

def main():
    """主函数"""
    create_monetary_subcategory_directories()

if __name__ == "__main__":
    main()
