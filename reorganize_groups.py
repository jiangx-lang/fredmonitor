#!/usr/bin/env python3
"""
重新组织分组 - 将相关指标移到新分组
"""

import yaml
import pathlib

def reorganize_groups():
    config_path = pathlib.Path("config/crisis_indicators.yaml")
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 分组调整映射
    group_changes = {
        'CORPDEBT_GDP_PCT': 'leverage_liquidity',  # 企业债/GDP -> 杠杆与表内流动性
        'RESERVES_DEPOSITS_PCT': 'leverage_liquidity',  # 银行准备金/存款 -> 杠杆与表内流动性
        'RESERVES_ASSETS_PCT': 'leverage_liquidity',  # 银行准备金/总资产 -> 杠杆与表内流动性
        'TOTALSA': 'leverage_liquidity',  # 消费者信贷 -> 杠杆与表内流动性
        'TOTLL': 'leverage_liquidity',  # 总贷款和租赁 -> 杠杆与表内流动性
    }
    
    # 权重调整 - 将某些指标权重设为0（仅展示不计分）
    weight_zero_indicators = {
        'T10Y2Y': 0,  # 保留T10Y3M，T10Y2Y权重=0
        'SOFR': 0,  # 保留SOFR20DMA_MINUS_DTB3，SOFR水平权重=0
    }
    
    # 为每个指标调整分组和权重
    updated_count = 0
    for indicator in config['indicators']:
        series_id = indicator.get('series_id', '')
        
        # 调整分组
        if series_id in group_changes:
            old_group = indicator.get('group', 'uncategorized')
            indicator['group'] = group_changes[series_id]
            print(f"✅ {indicator['name']} ({series_id}): {old_group} -> {group_changes[series_id]}")
            updated_count += 1
        
        # 调整权重
        if series_id in weight_zero_indicators:
            indicator['weight'] = weight_zero_indicators[series_id]
            print(f"⚖️  {indicator['name']} ({series_id}): 权重设为 {weight_zero_indicators[series_id]}")
    
    # 保存配置
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    
    print(f"\n🎉 完成！共更新了 {updated_count} 个指标的分组配置")

if __name__ == "__main__":
    reorganize_groups()
