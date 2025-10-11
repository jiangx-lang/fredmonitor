#!/usr/bin/env python3
"""
分析每日分析打分逻辑
"""

from core.database_integration import DatabaseIntegration

def analyze_scoring():
    """分析打分逻辑"""
    db = DatabaseIntegration()
    result = db.run_daily_analysis_with_database()
    
    print("=== 每日分析打分结果 ===")
    print(f"综合风险评分: {result['total_score']:.2f}")
    print(f"风险等级: {result['risk_level']}")
    print("\n=== 各因子详细评分 ===")
    
    for factor_id, score in result['factor_scores'].items():
        value = result['factor_values'].get(factor_id, "N/A")
        print(f"{factor_id}: {score:.1f}分 (当前值: {value})")
    
    print("\n=== 因子权重分析 ===")
    total_weight = 0
    for factor_id, score in result['factor_scores'].items():
        # 获取因子权重
        series_id = db.factor_mapping.get(factor_id, factor_id)
        indicator_config = db._get_indicator_config(series_id)
        if indicator_config:
            weight = indicator_config.get('weight', 0)
            total_weight += weight
            print(f"{factor_id}: 权重 {weight:.3f}, 评分 {score:.1f}, 加权贡献 {weight * score:.2f}")
    
    print(f"\n总权重: {total_weight:.3f}")

if __name__ == "__main__":
    analyze_scoring()
