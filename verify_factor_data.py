#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证每个因子的数据使用是否正确
"""

import os
import sys
import pandas as pd
import pathlib
import yaml
from datetime import datetime

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 添加项目根目录到路径
sys.path.append('.')

def load_yaml_config(file_path):
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR 加载配置文件失败: {e}")
        return {}

def verify_factor_data():
    """验证每个因子的数据使用"""
    print("开始验证因子数据使用...")
    print("=" * 80)
    
    # 1. 加载配置
    config_path = pathlib.Path("config/crisis_indicators.yaml")
    if not config_path.exists():
        print(f"ERROR 配置文件不存在: {config_path}")
        return False
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    print(f"加载了 {len(indicators)} 个指标")
    
    # 2. 检查数据文件
    data_dir = pathlib.Path("data/series")
    if not data_dir.exists():
        print(f"ERROR 数据目录不存在: {data_dir}")
        return False
    
    # 获取所有数据文件
    data_files = list(data_dir.glob("*.csv"))
    print(f"发现 {len(data_files)} 个数据文件")
    
    # 3. 验证每个指标
    issues = []
    
    for indicator in indicators:
        series_id = indicator.get('id')
        name = indicator.get('name')
        transform = indicator.get('transform', 'none')
        
        print(f"\n检查指标: {series_id} ({name})")
        print(f"  变换类型: {transform}")
        
        # 检查数据文件
        if transform == 'yoy_pct':
            # YoY指标应该使用_YOY.csv文件
            yoy_file = data_dir / f"{series_id}_YOY.csv"
            if yoy_file.exists():
                try:
                    df = pd.read_csv(yoy_file)
                    if 'yoy_pct' in df.columns:
                        latest_value = df['yoy_pct'].iloc[-1]
                        print(f"  ✅ YoY数据: {latest_value:.2f}%")
                    elif 'value' in df.columns:
                        latest_value = df['value'].iloc[-1]
                        print(f"  ✅ YoY数据: {latest_value:.2f}%")
                    else:
                        print(f"  ⚠️ YoY文件缺少yoy_pct或value列")
                        issues.append(f"{series_id}: YoY文件缺少yoy_pct或value列")
                except Exception as e:
                    print(f"  ❌ YoY文件读取失败: {e}")
                    issues.append(f"{series_id}: YoY文件读取失败")
            else:
                print(f"  ❌ YoY文件不存在: {yoy_file}")
                issues.append(f"{series_id}: YoY文件不存在")
        
        elif series_id == 'NCBDBIQ027S':
            # 企业债/GDP比率应该使用预计算数据
            ratio_file = data_dir / "CORPORATE_DEBT_GDP_RATIO.csv"
            if ratio_file.exists():
                try:
                    df = pd.read_csv(ratio_file)
                    if 'value' in df.columns:
                        latest_value = df['value'].iloc[-1]
                        print(f"  ✅ 企业债/GDP比率: {latest_value:.2f}%")
                    else:
                        print(f"  ⚠️ 比率文件缺少value列")
                        issues.append(f"{series_id}: 比率文件缺少value列")
                except Exception as e:
                    print(f"  ❌ 比率文件读取失败: {e}")
                    issues.append(f"{series_id}: 比率文件读取失败")
            else:
                print(f"  ❌ 比率文件不存在: {ratio_file}")
                issues.append(f"{series_id}: 比率文件不存在")
        
        elif series_id in ['CP_MINUS_DTB3', 'SOFR20DMA_MINUS_DTB3', 'CORPDEBT_GDP_PCT', 'RESERVES_ASSETS_PCT', 'RESERVES_DEPOSITS_PCT']:
            # 合成指标应该使用预计算数据
            synthetic_file = data_dir / f"{series_id}.csv"
            if synthetic_file.exists():
                try:
                    df = pd.read_csv(synthetic_file, index_col=0, parse_dates=True)
                    if len(df.columns) == 1:
                        latest_value = df.iloc[-1, 0]
                        print(f"  ✅ 合成指标: {latest_value:.4f}")
                    else:
                        print(f"  ⚠️ 合成指标文件格式异常")
                        issues.append(f"{series_id}: 合成指标文件格式异常")
                except Exception as e:
                    print(f"  ❌ 合成指标文件读取失败: {e}")
                    issues.append(f"{series_id}: 合成指标文件读取失败")
            else:
                print(f"  ❌ 合成指标文件不存在: {synthetic_file}")
                issues.append(f"{series_id}: 合成指标文件不存在")
        
        else:
            # 其他指标使用原始数据
            raw_file = data_dir / f"{series_id}.csv"
            if raw_file.exists():
                try:
                    df = pd.read_csv(raw_file)
                    if 'value' in df.columns:
                        latest_value = df['value'].iloc[-1]
                        print(f"  ✅ 原始数据: {latest_value:.4f}")
                    else:
                        print(f"  ⚠️ 原始文件缺少value列")
                        issues.append(f"{series_id}: 原始文件缺少value列")
                except Exception as e:
                    print(f"  ❌ 原始文件读取失败: {e}")
                    issues.append(f"{series_id}: 原始文件读取失败")
            else:
                print(f"  ❌ 原始文件不存在: {raw_file}")
                issues.append(f"{series_id}: 原始文件不存在")
    
    # 4. 总结
    print("\n" + "=" * 80)
    print("验证结果总结:")
    
    if issues:
        print(f"发现 {len(issues)} 个问题:")
        for issue in issues:
            print(f"  ❌ {issue}")
    else:
        print("✅ 所有指标数据使用正确!")
    
    return len(issues) == 0

def main():
    """主函数"""
    print("因子数据验证程序")
    print("=" * 80)
    
    try:
        success = verify_factor_data()
        if success:
            print("\n✅ 验证完成，所有指标数据使用正确!")
        else:
            print("\n❌ 验证完成，发现数据使用问题!")
    except Exception as e:
        print(f"\n❌ 验证过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
