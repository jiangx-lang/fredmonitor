#!/usr/bin/env python3
"""
测试FRED分类API
"""

from scripts.fred_http import category_children, category_series

def test_categories():
    print("测试FRED分类API...")
    
    # 获取顶层分类
    cats = category_children(0)
    print(f"\n顶层分类数量: {len(cats.get('categories', []))}")
    
    print("\n前10个顶层分类:")
    for i, cat in enumerate(cats.get('categories', [])[:10]):
        print(f"  {cat['id']}: {cat['name']}")
    
    # 测试第一个分类的系列
    if cats.get('categories'):
        first_cat = cats['categories'][0]
        print(f"\n测试分类 {first_cat['id']} ({first_cat['name']}) 的系列:")
        
        series = category_series(first_cat['id'], limit=5)
        print(f"  系列数量: {len(series.get('seriess', []))}")
        
        for s in series.get('seriess', [])[:3]:
            print(f"    {s['id']}: {s['title'][:50]}...")

if __name__ == "__main__":
    test_categories()
