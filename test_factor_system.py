#!/usr/bin/env python3
"""
因子系统测试脚本
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_factor_registry():
    """测试因子注册表"""
    print("=" * 60)
    print("测试因子注册表")
    print("=" * 60)
    
    try:
        from core.registry import FactorRegistry
        
        # 创建注册表
        registry = FactorRegistry("factors", "config/factor_registry.yaml")
        
        # 测试列出因子
        factors = registry.list_factors()
        print(f"✅ 找到 {len(factors)} 个因子配置")
        
        for factor_config in factors:
            print(f"  - {factor_config['id']}: {factor_config['name']}")
        
        # 测试获取因子实例
        print("\n测试因子实例:")
        for factor_config in factors[:3]:  # 只测试前3个
            factor_id = factor_config['id']
            factor = registry.get_factor(factor_id)
            if factor:
                print(f"  ✅ {factor_id}: {factor.name}")
            else:
                print(f"  ❌ {factor_id}: 创建失败")
        
        # 测试验证
        errors = registry.validate_factors()
        print(f"\n验证结果:")
        print(f"  缺失类: {len(errors['missing_classes'])}")
        print(f"  缺失配置: {len(errors['missing_configs'])}")
        print(f"  无效配置: {len(errors['invalid_configs'])}")
        
        return registry
        
    except Exception as e:
        print(f"❌ 因子注册表测试失败: {e}")
        return None

def test_factor_analysis(registry):
    """测试因子分析"""
    print("\n" + "=" * 60)
    print("测试因子分析")
    print("=" * 60)
    
    if not registry:
        print("❌ 注册表不可用，跳过测试")
        return
    
    try:
        # 获取第一个因子进行测试
        factors = registry.list_factors()
        if not factors:
            print("❌ 没有可用的因子")
            return
        
        factor_config = factors[0]
        factor_id = factor_config['id']
        factor = registry.get_factor(factor_id)
        
        if not factor:
            print(f"❌ 无法创建因子: {factor_id}")
            return
        
        print(f"测试因子: {factor.name}")
        
        # 生成模拟数据
        dates = pd.date_range('2020-01-01', periods=100, freq='D')
        values = np.random.normal(20, 5, 100)  # VIX模拟数据
        values = np.clip(values, 10, 50)  # 限制范围
        
        df = pd.DataFrame({
            'date': dates,
            'value': values
        })
        
        print(f"  模拟数据: {len(df)} 条记录")
        print(f"  数据范围: {df['value'].min():.2f} - {df['value'].max():.2f}")
        
        # 测试数据获取
        print("\n测试数据获取:")
        fetched_df = factor.fetch()
        if not fetched_df.empty:
            print(f"  ✅ 获取到 {len(fetched_df)} 条真实数据")
        else:
            print(f"  ⚠️ 无真实数据，使用模拟数据")
            fetched_df = df
        
        # 测试指标计算
        print("\n测试指标计算:")
        metrics = factor.compute(fetched_df)
        print(f"  计算指标数量: {len(metrics)}")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"    {key}: {value:.4f}")
            else:
                print(f"    {key}: {value}")
        
        # 测试评分计算
        print("\n测试评分计算:")
        score = factor.score(metrics, {})
        print(f"  风险评分: {score:.2f}")
        
        # 测试风险等级
        from core.scoring import get_risk_level
        risk_level = get_risk_level(score, {'low': 30, 'medium': 60, 'high': 80})
        print(f"  风险等级: {risk_level}")
        
        print(f"✅ {factor.name} 分析测试完成")
        
    except Exception as e:
        print(f"❌ 因子分析测试失败: {e}")
        import traceback
        traceback.print_exc()

def test_data_aggregator(registry):
    """测试数据聚合器"""
    print("\n" + "=" * 60)
    print("测试数据聚合器")
    print("=" * 60)
    
    if not registry:
        print("❌ 注册表不可用，跳过测试")
        return
    
    try:
        from core.aggregator import DataAggregator
        
        # 创建模拟的FRED客户端和缓存管理器
        class MockFredClient:
            def get_series_cached(self, series_id):
                # 返回模拟数据
                dates = pd.date_range('2020-01-01', periods=100, freq='D')
                values = np.random.normal(20, 5, 100)
                return pd.Series(values, index=dates)
        
        class MockCacheManager:
            pass
        
        # 创建聚合器
        fred_client = MockFredClient()
        cache_manager = MockCacheManager()
        settings = {
            'risk_thresholds': {'low': 30, 'medium': 60, 'high': 80}
        }
        
        aggregator = DataAggregator(fred_client, cache_manager, registry, settings)
        
        print(f"✅ 聚合器创建成功")
        print(f"  因子数量: {len(aggregator.factors)}")
        
        # 测试每日分析
        print("\n测试每日分析:")
        result = aggregator.run_daily_analysis()
        
        print(f"  分析日期: {result['date'].strftime('%Y-%m-%d')}")
        print(f"  综合评分: {result['total_score']:.2f}")
        print(f"  风险等级: {result['risk_level']}")
        print(f"  因子评分数量: {len(result['factor_scores'])}")
        
        # 显示各因子评分
        print("\n各因子评分:")
        for factor_id, score in result['factor_scores'].items():
            print(f"    {factor_id}: {score:.2f}")
        
        print("✅ 数据聚合器测试完成")
        
    except Exception as e:
        print(f"❌ 数据聚合器测试失败: {e}")
        import traceback
        traceback.print_exc()

def test_report_generator():
    """测试报告生成器"""
    print("\n" + "=" * 60)
    print("测试报告生成器")
    print("=" * 60)
    
    try:
        from core.report import ReportGenerator
        
        # 创建报告生成器
        generator = ReportGenerator(".")
        
        # 创建模拟分析结果
        result = {
            'date': datetime.now(),
            'total_score': 65.5,
            'risk_level': '高风险',
            'factor_scores': {
                'VIX_RISK': 75.0,
                'YIELD_CURVE': 60.0,
                'CREDIT_SPREAD': 55.0
            },
            'factor_values': {
                'VIX_RISK': 25.5,
                'YIELD_CURVE': -0.3,
                'CREDIT_SPREAD': 450.0
            },
            'factor_details': {
                'VIX_RISK': {
                    'status': 'success',
                    'metrics': {'current_value': 25.5, 'mean_20d': 22.1},
                    'data_points': 100
                },
                'YIELD_CURVE': {
                    'status': 'success',
                    'metrics': {'current_value': -0.3, 'mean_20d': 0.1},
                    'data_points': 100
                },
                'CREDIT_SPREAD': {
                    'status': 'success',
                    'metrics': {'current_value': 450.0, 'mean_20d': 420.0},
                    'data_points': 100
                }
            },
            'analysis_summary': {
                'risk_level': '高风险',
                'high_risk_factors': [
                    {'name': 'VIX波动率风险', 'score': 75.0, 'value': 25.5}
                ],
                'total_factors': 3,
                'active_factors': 3
            }
        }
        
        recent_scores = [
            {'date': datetime.now() - timedelta(days=1), 'total_score': 60.0},
            {'date': datetime.now() - timedelta(days=2), 'total_score': 55.0}
        ]
        
        # 生成报告
        report_path = generator.generate_daily_report(result, recent_scores)
        print(f"✅ 报告生成成功: {report_path}")
        
        # 测试Excel报告
        excel_path = "test_report.xlsx"
        generator.generate_excel_summary(result, excel_path)
        print(f"✅ Excel报告生成成功: {excel_path}")
        
        # 清理测试文件
        if os.path.exists(excel_path):
            os.remove(excel_path)
            print("✅ 测试文件已清理")
        
    except Exception as e:
        print(f"❌ 报告生成器测试失败: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主测试函数"""
    print("开始因子系统测试")
    print("=" * 60)
    
    # 测试因子注册表
    registry = test_factor_registry()
    
    # 测试因子分析
    test_factor_analysis(registry)
    
    # 测试数据聚合器
    test_data_aggregator(registry)
    
    # 测试报告生成器
    test_report_generator()
    
    print("\n" + "=" * 60)
    print("因子系统测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
