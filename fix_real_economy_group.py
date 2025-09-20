#!/usr/bin/env python3
"""
重组实体经济分组 - 并入INDPRO、PAYEMS、MANEMP、HOUST
"""

import yaml
import pathlib

def fix_real_economy_group():
    config_path = pathlib.Path("config/crisis_indicators.yaml")
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 实体经济分组调整映射
    real_economy_indicators = {
        'INDPRO': 'real_economy',  # 工业生产
        'PAYEMS': 'real_economy',  # 非农就业人数
        'MANEMP': 'real_economy',  # 制造业就业指数
        'HOUST': 'real_economy',   # 新屋开工
        'UMCSENT': 'real_economy', # 密歇根消费者信心（已在）
    }
    
    # 为每个指标调整分组
    updated_count = 0
    for indicator in config['indicators']:
        series_id = indicator.get('series_id', '')
        
        if series_id in real_economy_indicators:
            old_group = indicator.get('group', 'uncategorized')
            indicator['group'] = real_economy_indicators[series_id]
            print(f"✅ {indicator['name']} ({series_id}): {old_group} -> {real_economy_indicators[series_id]}")
            updated_count += 1
    
    # 保存配置
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    
    print(f"\n🎉 完成！共更新了 {updated_count} 个指标到实体经济分组")

if __name__ == "__main__":
    fix_real_economy_group()
