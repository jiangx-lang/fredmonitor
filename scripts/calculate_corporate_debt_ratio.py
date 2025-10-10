#!/usr/bin/env python3
"""
计算企业债/GDP比率脚本
从企业债总额和GDP数据计算比率，并保存为新的CSV文件
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_corporate_debt_ratio():
    """
    计算企业债/GDP比率
    """
    logger.info("开始计算企业债/GDP比率...")
    
    # 数据文件路径
    corp_debt_file = Path("data/series/NCBDBIQ027S.csv")
    gdp_file = Path("data/series/GDP.csv")
    output_file = Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
    
    # 检查输入文件是否存在
    if not corp_debt_file.exists():
        logger.error(f"企业债数据文件不存在: {corp_debt_file}")
        return False
    
    if not gdp_file.exists():
        logger.error(f"GDP数据文件不存在: {gdp_file}")
        return False
    
    try:
        # 读取企业债数据
        logger.info("读取企业债数据...")
        corp_debt_df = pd.read_csv(corp_debt_file)
        corp_debt_df['date'] = pd.to_datetime(corp_debt_df['date'])
        corp_debt_df['value'] = pd.to_numeric(corp_debt_df['value'], errors='coerce')
        corp_debt_df = corp_debt_df.dropna()
        logger.info(f"企业债数据: {len(corp_debt_df)} 条记录")
        
        # 读取GDP数据
        logger.info("读取GDP数据...")
        gdp_df = pd.read_csv(gdp_file)
        gdp_df['date'] = pd.to_datetime(gdp_df['date'])
        gdp_df['value'] = pd.to_numeric(gdp_df['value'], errors='coerce')
        gdp_df = gdp_df.dropna()
        logger.info(f"GDP数据: {len(gdp_df)} 条记录")
        
        # 合并数据（按日期对齐）
        logger.info("合并数据并计算比率...")
        merged_df = pd.merge(corp_debt_df, gdp_df, on='date', suffixes=('_corp_debt', '_gdp'))
        
        # 计算企业债/GDP比率 (百分比)
        # 注意单位转换：企业债是Millions of Dollars，GDP是Billions of Dollars
        # 需要将企业债转换为Billions：corp_debt / 1000
        merged_df['value'] = (merged_df['value_corp_debt'] / 1000) / merged_df['value_gdp'] * 100
        
        # 选择需要的列
        result_df = merged_df[['date', 'value']].copy()
        result_df = result_df.sort_values('date')
        
        # 保存结果
        logger.info(f"保存结果到: {output_file}")
        result_df.to_csv(output_file, index=False)
        
        # 显示统计信息
        logger.info("计算完成！")
        logger.info(f"总记录数: {len(result_df)}")
        logger.info(f"最新比率: {result_df['value'].iloc[-1]:.2f}%")
        logger.info(f"最新日期: {result_df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        logger.info(f"平均比率: {result_df['value'].mean():.2f}%")
        logger.info(f"比率范围: {result_df['value'].min():.2f}% - {result_df['value'].max():.2f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"计算失败: {e}")
        return False

def verify_calculation():
    """
    验证计算结果
    """
    logger.info("验证计算结果...")
    
    # 读取计算结果
    result_file = Path("data/series/CORPORATE_DEBT_RATIO.csv")
    if not result_file.exists():
        logger.error("计算结果文件不存在")
        return False
    
    result_df = pd.read_csv(result_file)
    result_df['date'] = pd.to_datetime(result_df['date'])
    
    # 显示最新几条记录
    logger.info("最新5条记录:")
    latest_5 = result_df.tail(5)
    for _, row in latest_5.iterrows():
        logger.info(f"  {row['date'].strftime('%Y-%m-%d')}: {row['value']:.2f}%")
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("企业债/GDP比率计算工具")
    print("=" * 60)
    
    # 计算比率
    success = calculate_corporate_debt_ratio()
    
    if success:
        print("\n" + "=" * 60)
        print("验证计算结果")
        print("=" * 60)
        verify_calculation()
        print("\n✅ 企业债/GDP比率计算完成！")
    else:
        print("\n❌ 计算失败！")
