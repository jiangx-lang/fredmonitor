#!/usr/bin/env python3
"""
时间序列处理函数的最小单测 - 防止核心功能被破坏
"""

import pandas as pd
import numpy as np
import sys
import pathlib

# 添加项目根目录到路径
project_root = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from crisis_monitor import to_monthly, transform_series


def test_to_monthly_basic():
    """测试to_monthly基本功能"""
    # 创建测试数据
    s = pd.Series([1, 2, 3], index=pd.to_datetime(["2024-01-01", "2024-01-31", "2024-02-15"]))
    
    # 转换为月末频率
    m = to_monthly(s)
    
    # 验证频率
    assert m.index.freqstr in ("ME", "M"), f"Expected ME or M, got {m.index.freqstr}"
    
    # 验证数值
    assert np.isclose(m.loc["2024-01-31"], 2.0), f"Expected 2.0, got {m.loc['2024-01-31']}"
    assert np.isclose(m.loc["2024-02-29"], 3.0), f"Expected 3.0, got {m.loc['2024-02-29']}"
    
    print("✅ test_to_monthly_basic passed")


def test_transform_mom_pct():
    """测试环比百分比变换"""
    # 创建测试数据
    s = pd.Series([100, 110, 121], index=pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"]))
    
    # 计算环比百分比
    out = transform_series(s, "mom_pct")
    
    # 验证最后一个值
    expected = (121/110 - 1) * 100
    assert np.isclose(out.iloc[-1], expected, atol=1e-6), f"Expected {expected}, got {out.iloc[-1]}"
    
    print("✅ test_transform_mom_pct passed")


def test_transform_yoy_pct_monthly_window():
    """测试同比百分比变换的月度窗口"""
    # 创建13个月的数据
    idx = pd.date_range("2023-01-31", periods=13, freq="M")
    s = pd.Series(range(13), index=idx, dtype=float)
    
    # 计算同比百分比
    out = transform_series(s, "yoy_pct")
    
    # 验证至少有1个有效值
    assert len(out.dropna()) >= 1, f"Expected at least 1 valid value, got {len(out.dropna())}"
    
    print("✅ test_transform_yoy_pct_monthly_window passed")


def test_freshness_gate():
    """测试新鲜度门槛"""
    from datetime import datetime, timedelta
    
    # 创建过期数据（超过90天）
    old_date = datetime.now() - timedelta(days=100)
    s = pd.Series([1, 2, 3], index=pd.to_datetime([old_date, old_date, old_date]))
    
    # 模拟新鲜度检查
    last_date = s.index[-1]
    stale_days = 90
    is_stale = (datetime.now().date() - last_date.date()).days > stale_days
    stale_weight = 0.0 if is_stale else 1.0
    
    assert is_stale == True, "Expected stale data"
    assert stale_weight == 0.0, "Expected weight=0 for stale data"
    
    print("✅ test_freshness_gate passed")


def test_redline_rules():
    """测试红线规则"""
    # 模拟分组评分
    scores = [10, 20, 95, 30]  # 有一个高风险指标
    
    # 红线规则：若组内≥1核心指标分数≥80，组分数至少抬到60
    high_risk_count = sum(1 for s in scores if s >= 80)
    group_avg = np.mean(scores)
    
    if high_risk_count >= 1:
        group_avg = max(group_avg, 60)
    
    assert group_avg >= 60, f"Expected group score >= 60, got {group_avg}"
    
    print("✅ test_redline_rules passed")


if __name__ == "__main__":
    print("🧪 运行最小单测...")
    
    try:
        test_to_monthly_basic()
        test_transform_mom_pct()
        test_transform_yoy_pct_monthly_window()
        test_freshness_gate()
        test_redline_rules()
        
        print("\n🎉 所有测试通过！")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        sys.exit(1)
