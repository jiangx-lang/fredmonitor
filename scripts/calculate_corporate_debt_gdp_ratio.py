#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业债/GDP比率计算程序
- 从FRED获取企业债(NCBDBIQ027S)和GDP数据
- 计算企业债/GDP比率（百分比）
- 保存到专门的CSV文件供crisis_monitor使用
"""

import os
import sys
import pandas as pd
import pathlib
from datetime import datetime

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 添加项目根目录到路径
sys.path.append('.')

from scripts.fred_http import series_observations
from scripts.clean_utils import parse_numeric_series

def calculate_corporate_debt_gdp_ratio():
    """计算企业债/GDP比率"""
    print("开始计算企业债/GDP比率...")
    
    try:
        # 1. 获取企业债数据 (NCBDBIQ027S)
        print("获取企业债数据 (NCBDBIQ027S)...")
        corp_debt_response = series_observations('NCBDBIQ027S')
        if not corp_debt_response or 'observations' not in corp_debt_response:
            print("无法获取企业债数据")
            return False
        
        corp_debt_obs = corp_debt_response.get('observations', [])
        if not corp_debt_obs:
            print("❌ 企业债数据为空")
            return False
        
        # 转换为DataFrame
        corp_debt_df = pd.DataFrame(corp_debt_obs)
        corp_debt_df['date'] = pd.to_datetime(corp_debt_df['date'])
        corp_debt_df = corp_debt_df.set_index('date')
        corp_debt_ts = parse_numeric_series(corp_debt_df['value']).dropna()
        
        print(f"[成功] 企业债数据获取成功: {len(corp_debt_ts)} 个观测值")
        print(f"   最新值: {corp_debt_ts.iloc[-1]:,.0f} 百万美元")
        
        # 2. 获取GDP数据
        print("[获取] 获取GDP数据...")
        gdp_response = series_observations('GDP')
        if not gdp_response or 'observations' not in gdp_response:
            print("[失败] 无法获取GDP数据")
            return False
        
        gdp_obs = gdp_response.get('observations', [])
        if not gdp_obs:
            print("[失败] GDP数据为空")
            return False
        
        # 转换为DataFrame
        gdp_df = pd.DataFrame(gdp_obs)
        gdp_df['date'] = pd.to_datetime(gdp_df['date'])
        gdp_df = gdp_df.set_index('date')
        gdp_ts = parse_numeric_series(gdp_df['value']).dropna()
        
        print(f"[成功] GDP数据获取成功: {len(gdp_ts)} 个观测值")
        print(f"   最新值: {gdp_ts.iloc[-1]:,.0f} 十亿美元")
        
        # 3. 计算企业债/GDP比率
        print("[计算] 计算企业债/GDP比率...")
        
        # 对齐时间序列（取交集）
        common_dates = corp_debt_ts.index.intersection(gdp_ts.index)
        if len(common_dates) == 0:
            print("[失败] 企业债和GDP数据没有共同日期")
            return False
        
        # 重新索引并前向填充
        corp_debt_aligned = corp_debt_ts.reindex(common_dates).ffill()
        gdp_aligned = gdp_ts.reindex(common_dates).ffill()
        
        # 计算比率：企业债(百万美元) / GDP(十亿美元) * 100
        # 注意单位转换：企业债是百万美元，GDP是十亿美元
        # 需要将企业债转换为十亿美元：corp_debt / 1000
        ratio_ts = (corp_debt_aligned / 1000) / gdp_aligned * 100
        
        # 移除无效值
        ratio_ts = ratio_ts.dropna()
        
        print(f"[成功] 比率计算完成: {len(ratio_ts)} 个有效值")
        print(f"   最新比率: {ratio_ts.iloc[-1]:.2f}%")
        print(f"   历史范围: {ratio_ts.min():.2f}% - {ratio_ts.max():.2f}%")
        
        # 4. 保存到CSV文件
        output_file = pathlib.Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建输出DataFrame
        output_df = pd.DataFrame({
            'date': ratio_ts.index,
            'value': ratio_ts.values,
            'corp_debt_millions': corp_debt_aligned.values,
            'gdp_billions': gdp_aligned.values
        })
        
        output_df.to_csv(output_file, index=False)
        print(f"[保存] 数据已保存到: {output_file}")
        
        # 5. 显示统计信息
        print("\n[统计] 企业债/GDP比率统计:")
        print(f"   数据期间: {ratio_ts.index[0].strftime('%Y-%m-%d')} 至 {ratio_ts.index[-1].strftime('%Y-%m-%d')}")
        print(f"   最新值: {ratio_ts.iloc[-1]:.2f}%")
        print(f"   历史均值: {ratio_ts.mean():.2f}%")
        print(f"   历史中位数: {ratio_ts.median():.2f}%")
        print(f"   历史标准差: {ratio_ts.std():.2f}%")
        print(f"   历史最小值: {ratio_ts.min():.2f}% ({ratio_ts.idxmin().strftime('%Y-%m-%d')})")
        print(f"   历史最大值: {ratio_ts.max():.2f}% ({ratio_ts.idxmax().strftime('%Y-%m-%d')})")
        
        return True
        
    except Exception as e:
        print(f"[失败] 计算企业债/GDP比率失败: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("企业债/GDP比率计算程序")
    print("=" * 60)
    
    success = calculate_corporate_debt_gdp_ratio()
    
    if success:
        print("\n[完成] 企业债/GDP比率计算完成！")
        print("📁 数据文件: data/series/CORPORATE_DEBT_GDP_RATIO.csv")
        print("📊 现在可以在crisis_monitor中直接使用这个比率数据")
    else:
        print("\n[失败] 企业债/GDP比率计算失败！")
        sys.exit(1)
