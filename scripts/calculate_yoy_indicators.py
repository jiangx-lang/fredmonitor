#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YoY指标计算程序
- 从FRED获取原始数据
- 计算YoY百分比变化
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

def infer_shift_for_yoy(idx: pd.DatetimeIndex) -> int:
    """根据索引推断同比所需的期数（M=12, Q=4, W=52, D≈252）。"""
    if len(idx) < 3: return 12
    freq = pd.infer_freq(idx) or ""
    f = freq.upper() if freq else ""
    if f.startswith("Q"): return 4
    if f.startswith("M"): return 12
    if f.startswith("W"): return 52
    # 日频：用交易日近似
    return 252

def calculate_yoy_for_indicator(series_id: str, name: str) -> bool:
    """计算单个指标的YoY数据"""
    print(f"📊 计算 {series_id} ({name}) 的YoY数据...")
    
    try:
        # 1. 获取原始数据
        response = series_observations(series_id)
        if not response or 'observations' not in response:
            print(f"❌ 无法获取 {series_id} 数据")
            return False
        
        observations = response.get('observations', [])
        if not observations:
            print(f"❌ {series_id} 数据为空")
            return False
        
        # 转换为DataFrame
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        ts = parse_numeric_series(df['value']).dropna()
        
        if ts.empty:
            print(f"❌ {series_id} 无有效数据")
            return False
        
        print(f"✅ {series_id} 原始数据获取成功: {len(ts)} 个观测值")
        print(f"   最新值: {ts.iloc[-1]:,.2f}")
        
        # 2. 计算YoY百分比
        # 推断期数
        shift_val = infer_shift_for_yoy(ts.index)
        print(f"   推断期数: {shift_val} (用于YoY计算)")
        
        # 计算YoY
        yoy_ts = (ts / ts.shift(shift_val) - 1.0) * 100.0
        yoy_ts = yoy_ts.dropna()
        
        if yoy_ts.empty:
            print(f"❌ {series_id} YoY计算后无有效数据")
            return False
        
        print(f"✅ {series_id} YoY计算完成: {len(yoy_ts)} 个有效值")
        print(f"   最新YoY: {yoy_ts.iloc[-1]:.2f}%")
        
        # 3. 保存到CSV文件
        output_file = pathlib.Path(f"data/series/{series_id}_YOY.csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建输出DataFrame
        output_df = pd.DataFrame({
            'date': yoy_ts.index,
            'yoy_pct': yoy_ts.values,
            'original_value': ts.reindex(yoy_ts.index).values
        })
        
        output_df.to_csv(output_file, index=False)
        print(f"💾 {series_id} YoY数据已保存到: {output_file}")
        
        # 4. 显示统计信息
        print(f"\n📈 {series_id} YoY统计:")
        print(f"   数据期间: {yoy_ts.index[0].strftime('%Y-%m-%d')} 至 {yoy_ts.index[-1].strftime('%Y-%m-%d')}")
        print(f"   最新YoY: {yoy_ts.iloc[-1]:.2f}%")
        print(f"   历史均值: {yoy_ts.mean():.2f}%")
        print(f"   历史中位数: {yoy_ts.median():.2f}%")
        print(f"   历史标准差: {yoy_ts.std():.2f}%")
        print(f"   历史最小值: {yoy_ts.min():.2f}% ({yoy_ts.idxmin().strftime('%Y-%m-%d')})")
        print(f"   历史最大值: {yoy_ts.max():.2f}% ({yoy_ts.idxmax().strftime('%Y-%m-%d')})")
        
        return True
        
    except Exception as e:
        print(f"[失败] 计算 {series_id} YoY失败: {e}")
        return False

def calculate_all_yoy_indicators():
    """计算所有需要YoY的指标"""
    print("[开始] 开始计算所有YoY指标...")
    
    # 需要计算YoY的指标列表
    yoy_indicators = [
        ('PAYEMS', '非农就业人数'),
        ('INDPRO', '工业生产指数'),
        ('GDP', '国内生产总值'),
        ('NEWORDER', '制造业新订单'),
        ('CSUSHPINSA', 'Case-Shiller房价指数'),
        ('TOTALSA', '消费者信贷'),
        ('TOTLL', '银行总贷款和租赁'),
        ('MANEMP', '制造业就业'),
        ('WALCL', '美联储总资产'),
        ('DTWEXBGS', '贸易加权美元指数'),
        ('PERMIT', '建筑许可'),
        ('TOTRESNS', '银行准备金')
    ]
    
    success_count = 0
    total_count = len(yoy_indicators)
    
    for series_id, name in yoy_indicators:
        print(f"\n{'='*60}")
        if calculate_yoy_for_indicator(series_id, name):
            success_count += 1
        print(f"{'='*60}")
    
    print(f"\n📊 YoY计算完成: {success_count}/{total_count} 个指标成功")
    return success_count == total_count

if __name__ == "__main__":
    print("=" * 60)
    print("YoY指标计算程序")
    print("=" * 60)
    
    success = calculate_all_yoy_indicators()
    
    if success:
        print("\n✅ 所有YoY指标计算完成！")
        print("📁 数据文件保存在: data/series/")
        print("📊 现在可以在crisis_monitor中直接使用这些YoY数据")
    else:
        print("\n⚠️ 部分YoY指标计算失败，请检查网络连接和FRED API")
        sys.exit(1)