#!/usr/bin/env python3
"""
测试FRED分类API
"""

import sys
import os
sys.path.append('.')

def test_category_api():
    print("测试FRED分类API...")
    
    try:
        # 直接导入并测试
        from scripts.fred_http import category_children, category_series
        print("✓ 分类API导入成功")
        
        # 测试获取顶层分类
        print("正在获取顶层分类...")
        cats = category_children(0)
        print(f"API响应类型: {type(cats)}")
        print(f"响应内容: {cats}")
        
        if 'categories' in cats:
            categories = cats['categories']
            print(f"找到 {len(categories)} 个顶层分类")
            
            # 显示前5个分类
            for i, cat in enumerate(categories[:5]):
                print(f"  {i+1}. ID: {cat.get('id')}, 名称: {cat.get('name')}")
            
            # 测试第一个分类的系列
            if categories:
                first_cat = categories[0]
                print(f"\n测试分类 {first_cat['id']} ({first_cat['name']}) 的系列:")
                
                series = category_series(first_cat['id'], limit=5)
                if 'seriess' in series:
                    series_list = series['seriess']
                    print(f"  找到 {len(series_list)} 个系列")
                    
                    for s in series_list[:3]:
                        print(f"    {s['id']}: {s.get('title', 'N/A')[:50]}...")
                else:
                    print(f"  系列响应格式异常: {series}")
        else:
            print("分类响应格式异常")
            
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_category_api()
