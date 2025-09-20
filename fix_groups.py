#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速修复缺失的group设置
"""

import yaml

def fix_groups():
    with open('config/crisis_indicators.yaml', 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    # 为缺失group的指标添加group
    group_mapping = {
        'CP_MINUS_DTB3': 'credit_liquidity',
        'SOFR20DMA_MINUS_DTB3': 'credit_liquidity', 
        'VIXCLS': 'credit_liquidity',
        'TDSP': 'housing',
        'MORTGAGE30US': 'housing',
        'CSUSHPINSA': 'housing',
        'MANEMP': 'real_economy',
        'INDPRO': 'real_economy',
        'HOUST': 'housing',
        'PAYEMS': 'real_economy',
        'DTWEXBGS': 'usd_policy',
        'FEDFUNDS': 'rates_curve',
        'DTB3': 'rates_curve',
        'DGS10': 'rates_curve',
        'GDP': 'real_economy',
        'CORPDEBT_GDP_PCT': 'usd_policy',
        'DRSFRMACBS': 'housing',
        'TOTLL': 'credit_liquidity',
        'TOTALSA': 'credit_liquidity',
        'RESERVES_DEPOSITS_PCT': 'credit_liquidity',
        'RESERVES_ASSETS_PCT': 'credit_liquidity',
        'WALCL': 'usd_policy'
    }

    fixed_count = 0
    for indicator in data['indicators']:
        if 'group' not in indicator and indicator['series_id'] in group_mapping:
            indicator['group'] = group_mapping[indicator['series_id']]
            print(f'✅ Added group {group_mapping[indicator["series_id"]]} to {indicator["series_id"]}')
            fixed_count += 1

    with open('config/crisis_indicators.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
    
    print(f'\n🎉 修复完成！共添加了 {fixed_count} 个group设置')

if __name__ == "__main__":
    fix_groups()
