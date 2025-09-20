#!/usr/bin/env python3
"""
检查新的Money, Banking, & Finance序列
"""

import yaml

def check_new_catalog():
    """检查新的catalog"""
    
    with open('config/money_banking_catalog.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series = catalog.get('series', [])
    print(f'Money, Banking, & Finance 序列总数: {len(series)}')
    
    # 按分类统计
    category_stats = {}
    for item in series:
        cat_id = item.get('category_id')
        if cat_id:
            if cat_id not in category_stats:
                category_stats[cat_id] = []
            category_stats[cat_id].append(item)
    
    for cat_id, items in sorted(category_stats.items()):
        print(f'\n分类 {cat_id} ({len(items)} 个序列):')
        for item in items[:5]:  # 只显示前5个
            print(f'  {item["id"]}: {item["alias"]}')
        if len(items) > 5:
            print(f'  ... 还有 {len(items)-5} 个序列')

if __name__ == "__main__":
    check_new_catalog()
