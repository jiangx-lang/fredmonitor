#!/usr/bin/env python3
"""
检查Money, Banking, & Finance分类的序列
"""

import yaml

def check_category_10():
    """检查分类10的序列"""
    
    with open('config/full_fred_catalog.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series = catalog.get('series', [])
    money_banking_series = []
    
    for item in series:
        cat_id = item.get('category_id')
        if cat_id == 10:  # Money, Banking, & Finance
            money_banking_series.append(item)
    
    print('Money, Banking, & Finance 分类 (ID: 10) 的序列:')
    for item in money_banking_series:
        print(f'  {item["id"]}: {item["alias"]}')
    print(f'\n总计: {len(money_banking_series)} 个序列')
    
    # 检查其他分类
    print('\n其他分类统计:')
    category_stats = {}
    for item in series:
        cat_id = item.get('category_id')
        if cat_id:
            if cat_id not in category_stats:
                category_stats[cat_id] = []
            category_stats[cat_id].append(item)
    
    for cat_id, items in sorted(category_stats.items()):
        print(f'  分类 {cat_id}: {len(items)} 个序列')

if __name__ == "__main__":
    check_category_10()
