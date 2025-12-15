#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证风险面板配置文件的有效性
"""

import yaml
from pathlib import Path

def validate_config():
    """验证配置文件"""
    print("🔍 验证风险面板配置文件...")
    print("=" * 50)
    
    config_path = Path(__file__).parent / "config" / "risk_dashboard.yaml"
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        risk_config = config.get('risk_dashboard', {})
        buckets = risk_config.get('buckets', [])
        
        print(f"✅ 配置文件加载成功")
        print(f"📊 风险分组数: {len(buckets)}")
        
        # 检查权重归一化
        total_weight = 0
        total_indicators = 0
        
        for bucket in buckets:
            bucket_name = bucket['name']
            bucket_weight = bucket['weight']
            indicators = bucket.get('indicators', [])
            
            total_weight += bucket_weight
            total_indicators += len(indicators)
            
            print(f"\n📈 分组: {bucket_name}")
            print(f"   权重: {bucket_weight:.1%}")
            print(f"   指标数: {len(indicators)}")
            
            for indicator in indicators:
                series_id = indicator['id']
                label = indicator.get('label', series_id)
                direction = indicator.get('direction', 'up_is_risk')
                freq = indicator.get('freq', 'D')
                
                print(f"     - {series_id} ({label}) - {direction} - {freq}")
        
        print(f"\n📊 权重统计:")
        print(f"   总权重: {total_weight:.1%}")
        print(f"   总指标数: {total_indicators}")
        
        if abs(total_weight - 1.0) < 0.001:
            print("✅ 权重已正确归一化")
        else:
            print(f"⚠️ 权重未归一化，当前总计: {total_weight:.1%}")
        
        # 检查评分配置
        scoring = risk_config.get('scoring', {})
        print(f"\n🎯 评分配置:")
        print(f"   使用分位数: {scoring.get('use_percentile', True)}")
        print(f"   回看年数: {scoring.get('percentile_window_years', 5)}")
        print(f"   动量天数: {scoring.get('momentum_days', [1, 5])}")
        print(f"   动量加分上限: {scoring.get('momentum_bonus_max', 5)}")
        print(f"   共振加分: {scoring.get('co_move_bonus_per_bucket', 5)}")
        print(f"   共振阈值: {scoring.get('co_move_threshold_pct', 0.9):.1%}")
        
        # 检查风险阈值
        thresholds = risk_config.get('heatmap_thresholds', {})
        print(f"\n🚨 风险阈值:")
        print(f"   低风险: 0-{thresholds.get('low', 35)}")
        print(f"   中风险: {thresholds.get('low', 35)}-{thresholds.get('medium', 65)}")
        print(f"   高风险: {thresholds.get('medium', 65)}-{thresholds.get('high', 80)}")
        print(f"   极高风险: {thresholds.get('high', 80)}-100")
        
        # 检查指标说明
        descriptions = config.get('indicator_descriptions', {})
        print(f"\n📝 指标说明:")
        print(f"   已配置说明的指标数: {len(descriptions)}")
        
        missing_descriptions = []
        for bucket in buckets:
            for indicator in bucket.get('indicators', []):
                series_id = indicator['id']
                if series_id not in descriptions:
                    missing_descriptions.append(series_id)
        
        if missing_descriptions:
            print(f"   ⚠️ 缺少说明的指标: {missing_descriptions}")
        else:
            print("   ✅ 所有指标都有说明")
        
        print("\n" + "=" * 50)
        print("✅ 配置文件验证完成")
        
        return True
        
    except Exception as e:
        print(f"❌ 配置文件验证失败: {e}")
        return False

if __name__ == "__main__":
    validate_config()











