#!/usr/bin/env python3
"""
测试catalog文件
"""

import yaml

def test_catalog():
    with open('config/catalog_fred.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    print(f"catalog中的序列数量: {len(catalog.get('series', []))}")
    
    print("\n前10个序列:")
    for i, s in enumerate(catalog.get('series', [])[:10]):
        print(f"  {i+1}. {s['id']}: {s['alias']}")

if __name__ == "__main__":
    test_catalog()
