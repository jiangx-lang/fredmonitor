#!/usr/bin/env python3
"""
简化的日度风险面板数据检查
"""

import os
import pandas as pd
from datetime import datetime

def check_series(series_id, data_dir):
    """检查单个序列数据"""
    series_path = os.path.join(data_dir, "fred", "series", series_id, "raw.csv")
    
    if not os.path.exists(series_path):
        return False, "文件不存在"
    
    try:
        df = pd.read_csv(series_path, index_col=0, parse_dates=True)
        if df.empty:
            return False, "数据为空"
        
        latest_date = df.index.max()
        days_old = (datetime.now() - latest_date).days
        data_points = len(df.dropna())
        
        return True, f"最新: {latest_date.strftime('%Y-%m-%d')}, {days_old}天前, {data_points}点"
    except Exception as e:
        return False, f"读取失败: {e}"

def main():
    print("日度风险面板数据检查")
    print("=" * 50)
    
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    # 需要检查的指标
    indicators = [
        # VolTerm组
        ("VIXCLS", "VIX波动率指数"),
        ("SKEW", "SKEW指数"),
        ("DGS10", "10年期国债收益率"),
        ("DGS3MO", "3个月期国债收益率"),
        ("DGS2", "2年期国债收益率"),
        
        # Credit组
        ("BAMLH0A0HYM2", "高收益债券OAS"),
        ("BAMLC0A0CM", "投资级债券OAS"),
        ("TEDRATE", "TED利差"),
        ("BAA", "BAA级公司债收益率"),
        
        # Liquidity组
        ("RRPONTSYD", "隔夜逆回购余额"),
        ("WSHOMCB", "美联储总资产"),
        ("WTREGEN", "财政部TGA账户"),
        ("IORB", "IORB利率"),
        ("EFFR", "EFFR利率"),
        
        # RiskOnOff组
        ("DTWEXBGS", "贸易加权美元指数"),
        ("DCOILWTICO", "WTI原油价格"),
        ("GOLDAMGBD228NLBM", "黄金价格"),
        ("SPX", "标普500指数"),
        ("UTIL", "公用事业指数"),
        
        # StressComposite组
        ("STLFSI2", "圣路易斯金融压力指数"),
        ("NFCI", "芝加哥金融状况指数"),
    ]
    
    available_count = 0
    total_count = len(indicators)
    
    for series_id, description in indicators:
        is_available, message = check_series(series_id, data_dir)
        status = "✅" if is_available else "❌"
        print(f"{status} {series_id:15} - {description:20} - {message}")
        
        if is_available:
            available_count += 1
    
    print("=" * 50)
    print(f"检查结果: {available_count}/{total_count} 个指标可用 ({available_count/total_count:.1%})")
    
    if available_count == total_count:
        print("🎉 所有指标数据都可用！可以正常运行日度风险面板")
    else:
        print("⚠️ 有指标数据缺失，建议运行数据下载")
        print("缺失的指标需要从FRED API下载")

if __name__ == "__main__":
    main()