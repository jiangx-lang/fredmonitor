#!/usr/bin/env python3
"""
为所有未分组的指标添加分组配置
"""

import yaml
import pathlib

def fix_groups():
    config_path = pathlib.Path("config/crisis_indicators.yaml")
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 分组映射规则
    group_mapping = {
        # 利率与收益率曲线
        'T10Y3M': 'rates_curve',
        'T10Y2Y': 'rates_curve', 
        'SOFR': 'rates_curve',
        'CPN3M': 'rates_curve',
        'FEDFUNDS': 'rates_curve',
        'DTB3': 'rates_curve',
        'DGS10': 'rates_curve',
        
        # 信用与流动性
        'BAMLH0A0HYM2': 'credit_liquidity',
        'BAA10YM': 'credit_liquidity',
        'TEDRATE': 'credit_liquidity',
        'NFCI': 'credit_liquidity',
        'CP_MINUS_DTB3': 'credit_liquidity',
        'SOFR20DMA_MINUS_DTB3': 'credit_liquidity',
        'VIXCLS': 'credit_liquidity',
        
        # 房地产
        'TDSP': 'housing',
        'MORTGAGE30US': 'housing',
        'CSUSHPISA': 'housing',
        'HOUST': 'housing',
        
        # 实体经济
        'UMCSENT': 'real_economy',
        'INDPRO': 'real_economy',
        'PAYEMS': 'real_economy',
        'MANEMP': 'real_economy',
        'GDP': 'real_economy',
        
        # 美元与政策
        'DTWEXBGS': 'usd_policy',
        'TOTRESNS': 'usd_policy',
        'RESERVES_DEPOSITS_PCT': 'usd_policy',
        'RESERVES_ASSETS_PCT': 'usd_policy',
        'TOTALSA': 'usd_policy',
        'TLAACBW027SBOG': 'usd_policy',
        'CORPDEBT_GDP_PCT': 'usd_policy',
        'FEDTOTALASSETS': 'usd_policy',
    }
    
    # 为每个指标添加分组
    updated_count = 0
    for indicator in config['indicators']:
        series_id = indicator.get('series_id', '')
        if series_id in group_mapping and 'group' not in indicator:
            indicator['group'] = group_mapping[series_id]
            updated_count += 1
            print(f"✅ 为 {indicator['name']} ({series_id}) 添加分组: {group_mapping[series_id]}")
        elif 'group' not in indicator:
            print(f"⚠️  未找到分组规则: {indicator['name']} ({series_id})")
    
    # 保存配置
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    
    print(f"\n🎉 完成！共更新了 {updated_count} 个指标的分组配置")

if __name__ == "__main__":
    fix_groups()
