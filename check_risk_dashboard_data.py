#!/usr/bin/env python3
"""
检查日度风险面板所有指标的数据可用性
"""

import os
import yaml
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

def check_series_data(series_id, data_dir):
    """检查单个序列的数据可用性"""
    series_path = os.path.join(data_dir, "fred", "series", series_id)
    
    if not os.path.exists(series_path):
        return False, f"目录不存在: {series_path}"
    
    # 检查原始数据文件
    raw_file = os.path.join(series_path, "raw.csv")
    if not os.path.exists(raw_file):
        return False, f"原始数据文件不存在: {raw_file}"
    
    try:
        # 读取数据
        df = pd.read_csv(raw_file, index_col=0, parse_dates=True)
        if df.empty:
            return False, "数据文件为空"
        
        # 检查最新数据日期
        latest_date = df.index.max()
        days_old = (datetime.now() - latest_date).days
        
        # 检查数据点数量
        data_points = len(df.dropna())
        
        return True, f"最新数据: {latest_date.strftime('%Y-%m-%d')}, {days_old}天前, {data_points}个数据点"
        
    except Exception as e:
        return False, f"读取数据失败: {e}"

def check_derived_series(series_id, data_dir):
    """检查衍生序列的数据可用性"""
    if series_id == "T10Y3M":
        # 检查10年期和3个月期国债收益率
        dgs10_ok, dgs10_msg = check_series_data("DGS10", data_dir)
        dgs3m_ok, dgs3m_msg = check_series_data("DGS3MO", data_dir)
        return dgs10_ok and dgs3m_ok, f"DGS10: {dgs10_msg}, DGS3MO: {dgs3m_msg}"
    
    elif series_id == "T10Y2Y":
        # 检查10年期和2年期国债收益率
        dgs10_ok, dgs10_msg = check_series_data("DGS10", data_dir)
        dgs2y_ok, dgs2y_msg = check_series_data("DGS2", data_dir)
        return dgs10_ok and dgs2y_ok, f"DGS10: {dgs10_msg}, DGS2: {dgs2y_msg}"
    
    elif series_id == "MOVE_PROXY":
        # 检查10年期和2年期国债收益率（用于计算MOVE代理）
        dgs10_ok, dgs10_msg = check_series_data("DGS10", data_dir)
        dgs2y_ok, dgs2y_msg = check_series_data("DGS2", data_dir)
        return dgs10_ok and dgs2y_ok, f"DGS10: {dgs10_msg}, DGS2: {dgs2y_msg}"
    
    elif series_id == "BAA10Y":
        # 检查BAA级公司债收益率和10年期国债收益率
        baa_ok, baa_msg = check_series_data("BAA", data_dir)
        dgs10_ok, dgs10_msg = check_series_data("DGS10", data_dir)
        return baa_ok and dgs10_ok, f"BAA: {baa_msg}, DGS10: {dgs10_msg}"
    
    elif series_id == "IORB_EFFR_SPRD":
        # 检查IORB和EFFR
        iorb_ok, iorb_msg = check_series_data("IORB", data_dir)
        effr_ok, effr_msg = check_series_data("EFFR", data_dir)
        return iorb_ok and effr_ok, f"IORB: {iorb_msg}, EFFR: {effr_msg}"
    
    elif series_id == "SPX_UTIL_RATIO":
        # 检查SPX和UTIL
        spx_ok, spx_msg = check_series_data("SPX", data_dir)
        util_ok, util_msg = check_series_data("UTIL", data_dir)
        return spx_ok and util_ok, f"SPX: {spx_msg}, UTIL: {util_msg}"
    
    else:
        # 直接检查原始序列
        return check_series_data(series_id, data_dir)

def main():
    """主函数"""
    print("=" * 60)
    print("日度风险面板数据可用性检查")
    print("=" * 60)
    
    # 加载配置
    config_path = os.path.join(os.path.dirname(__file__), "daily_risk_dashboard", "config", "risk_dashboard.yaml")
    if not os.path.exists(config_path):
        print(f"❌ 配置文件不存在: {config_path}")
        return
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 数据目录
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    print(f"数据目录: {data_dir}")
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 统计信息
    total_indicators = 0
    available_indicators = 0
    missing_indicators = 0
    
    # 检查每个bucket
    for bucket in config['risk_dashboard']['buckets']:
        print(f"📊 {bucket['name']} 组 (权重: {bucket['weight']:.1%})")
        print("-" * 40)
        
        for indicator in bucket['indicators']:
            series_id = indicator['id']
            label = indicator['label']
            total_indicators += 1
            
            print(f"  {series_id} ({label}): ", end="")
            
            # 检查数据可用性
            is_available, message = check_derived_series(series_id, data_dir)
            
            if is_available:
                print(f"✅ {message}")
                available_indicators += 1
            else:
                print(f"❌ {message}")
                missing_indicators += 1
        
        print()
    
    # 总结
    print("=" * 60)
    print("检查结果总结")
    print("=" * 60)
    print(f"总指标数: {total_indicators}")
    print(f"可用指标: {available_indicators} ({available_indicators/total_indicators:.1%})")
    print(f"缺失指标: {missing_indicators} ({missing_indicators/total_indicators:.1%})")
    
    if missing_indicators == 0:
        print("🎉 所有指标数据都可用！")
    else:
        print("⚠️ 有指标数据缺失，建议运行数据下载")
    
    print()
    print("建议操作:")
    if missing_indicators > 0:
        print("1. 运行 '下载FRED数据' 功能")
        print("2. 检查网络连接和API密钥")
        print("3. 重新运行此检查")
    else:
        print("1. 可以正常运行日度风险面板")
        print("2. 建议定期检查数据更新")

if __name__ == "__main__":
    main()
