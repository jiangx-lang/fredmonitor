#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查FRED数据有效性和下载功能
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import yaml

# 添加父目录到路径
BASE = Path(__file__).parent
PARENT_BASE = BASE.parent
sys.path.insert(0, str(PARENT_BASE))

def load_config():
    """加载配置文件"""
    config_path = BASE / "config" / "risk_dashboard.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def test_fred_api():
    """测试FRED API连接"""
    print("🌐 测试FRED API连接...")
    
    try:
        from scripts.fred_http import series_observations
        
        # 测试一个简单的指标
        test_series = "VIXCLS"
        print(f"   测试指标: {test_series}")
        
        data = series_observations(test_series)
        if data and 'observations' in data:
            observations = data.get('observations', [])
            if observations:
                print(f"   ✅ API连接成功，获取到 {len(observations)} 条数据")
                return True
            else:
                print("   ⚠️ API返回空数据")
        else:
            print("   ❌ API返回格式错误")
            
    except Exception as e:
        print(f"   ❌ API连接失败: {e}")
    
    return False

def check_local_data():
    """检查本地缓存数据"""
    print("\n📁 检查本地缓存数据...")
    
    data_dir = PARENT_BASE / "data" / "fred" / "series"
    if not data_dir.exists():
        print("   ⚠️ 数据目录不存在，将需要从API下载")
        return False
    
    # 统计缓存文件
    cached_series = []
    for series_dir in data_dir.iterdir():
        if series_dir.is_dir():
            raw_file = series_dir / "raw.csv"
            if raw_file.exists():
                cached_series.append(series_dir.name)
    
    print(f"   📊 已缓存 {len(cached_series)} 个指标")
    if cached_series:
        print(f"   📋 缓存指标: {', '.join(cached_series[:10])}{'...' if len(cached_series) > 10 else ''}")
    
    return len(cached_series) > 0

def validate_indicators():
    """验证所有指标的数据有效性"""
    print("\n🔍 验证指标数据有效性...")
    
    config = load_config()
    risk_config = config.get('risk_dashboard', {})
    buckets = risk_config.get('buckets', [])
    
    # 收集所有指标
    all_indicators = []
    for bucket in buckets:
        for indicator in bucket.get('indicators', []):
            all_indicators.append(indicator)
    
    print(f"📊 总共需要验证 {len(all_indicators)} 个指标")
    
    # 检查每个指标
    valid_indicators = []
    invalid_indicators = []
    
    for indicator in all_indicators:
        series_id = indicator['id']
        label = indicator.get('label', series_id)
        
        print(f"   🔍 检查 {series_id} ({label})...")
        
        # 检查本地缓存
        cache_path = PARENT_BASE / "data" / "fred" / "series" / series_id / "raw.csv"
        
        if cache_path.exists():
            try:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if not df.empty and len(df) > 100:  # 至少100个数据点
                    latest_date = df.index[-1]
                    days_old = (datetime.now() - latest_date.to_pydatetime()).days
                    
                    print(f"      ✅ 本地缓存有效 ({len(df)} 条数据, 最新: {latest_date.date()}, {days_old}天前)")
                    valid_indicators.append(series_id)
                else:
                    print(f"      ⚠️ 本地缓存数据不足 ({len(df)} 条数据)")
                    invalid_indicators.append(series_id)
            except Exception as e:
                print(f"      ❌ 本地缓存读取失败: {e}")
                invalid_indicators.append(series_id)
        else:
            print(f"      ⚠️ 无本地缓存，需要从API下载")
            invalid_indicators.append(series_id)
    
    print(f"\n📊 验证结果:")
    print(f"   ✅ 有效指标: {len(valid_indicators)}")
    print(f"   ⚠️ 需要下载: {len(invalid_indicators)}")
    
    if invalid_indicators:
        print(f"   📋 需要下载的指标: {', '.join(invalid_indicators)}")
    
    return valid_indicators, invalid_indicators

def test_data_processing():
    """测试数据处理功能"""
    print("\n🧮 测试数据处理功能...")
    
    try:
        # 测试分位数计算
        test_data = pd.Series(np.random.randn(1000))
        test_value = test_data.iloc[-1]
        
        percentile = (test_data <= test_value).mean()
        print(f"   ✅ 分位数计算测试: {percentile:.3f}")
        
        # 测试动量计算
        if len(test_data) > 5:
            change_1d = abs(test_data.iloc[-1] - test_data.iloc[-2])
            change_5d = abs(test_data.iloc[-1] - test_data.iloc[-6])
            print(f"   ✅ 动量计算测试: 1日变化={change_1d:.3f}, 5日变化={change_5d:.3f}")
        
        # 测试评分计算
        base_score = percentile * 100
        momentum_score = min(5, percentile * 5)
        final_score = min(100, base_score + momentum_score)
        print(f"   ✅ 评分计算测试: 基础={base_score:.1f}, 动量={momentum_score:.1f}, 最终={final_score:.1f}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 数据处理测试失败: {e}")
        return False

def test_visualization():
    """测试图表生成功能"""
    print("\n🎨 测试图表生成功能...")
    
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        
        # 测试基础图表
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # 模拟数据
        dates = pd.date_range('2020-01-01', periods=100, freq='D')
        values = np.random.randn(100).cumsum()
        
        ax.plot(dates, values)
        ax.set_title('测试图表')
        ax.set_xlabel('日期')
        ax.set_ylabel('数值')
        
        # 测试中文字体
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 保存测试图片
        test_image_path = BASE / "outputs" / "test_chart.png"
        test_image_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(test_image_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        if test_image_path.exists():
            file_size = test_image_path.stat().st_size / 1024  # KB
            print(f"   ✅ 图表生成成功: {test_image_path} ({file_size:.1f} KB)")
            
            # 清理测试文件
            test_image_path.unlink()
            return True
        else:
            print("   ❌ 图表文件未生成")
            return False
            
    except Exception as e:
        print(f"   ❌ 图表生成测试失败: {e}")
        return False

def main():
    """主函数"""
    print("🔍 FRED数据有效性全面检查")
    print("=" * 60)
    
    # 1. 验证配置文件
    print("1️⃣ 配置文件验证")
    config_valid = validate_config()
    
    # 2. 测试API连接
    print("\n2️⃣ API连接测试")
    api_valid = test_fred_api()
    
    # 3. 检查本地数据
    print("\n3️⃣ 本地数据检查")
    local_data_exists = check_local_data()
    
    # 4. 验证指标数据
    print("\n4️⃣ 指标数据验证")
    valid_indicators, invalid_indicators = validate_indicators()
    
    # 5. 测试数据处理
    print("\n5️⃣ 数据处理测试")
    processing_valid = test_data_processing()
    
    # 6. 测试图表生成
    print("\n6️⃣ 图表生成测试")
    visualization_valid = test_visualization()
    
    # 总结
    print("\n" + "=" * 60)
    print("📊 检查结果总结")
    print("=" * 60)
    
    checks = [
        ("配置文件", config_valid),
        ("API连接", api_valid),
        ("本地数据", local_data_exists),
        ("数据处理", processing_valid),
        ("图表生成", visualization_valid)
    ]
    
    passed = sum(1 for _, valid in checks if valid)
    total = len(checks)
    
    for name, valid in checks:
        status = "✅" if valid else "❌"
        print(f"{status} {name}")
    
    print(f"\n📊 总体评估: {passed}/{total} 项检查通过")
    
    if passed == total:
        print("🎉 系统完全就绪，可以正常运行!")
    elif passed >= total * 0.8:
        print("⚠️ 系统基本就绪，部分功能可能需要网络连接")
    else:
        print("❌ 系统存在问题，需要修复后再运行")
    
    # 数据完整性评估
    data_coverage = len(valid_indicators) / (len(valid_indicators) + len(invalid_indicators)) if (len(valid_indicators) + len(invalid_indicators)) > 0 else 0
    print(f"📈 数据覆盖率: {data_coverage:.1%} ({len(valid_indicators)}/{len(valid_indicators) + len(invalid_indicators)})")
    
    if data_coverage >= 0.8:
        print("✅ 数据覆盖率良好")
    elif data_coverage >= 0.5:
        print("⚠️ 数据覆盖率一般，首次运行可能需要较长时间下载数据")
    else:
        print("❌ 数据覆盖率不足，建议检查网络连接和API权限")
    
    print("=" * 60)

if __name__ == "__main__":
    main()











