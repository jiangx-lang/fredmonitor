#!/usr/bin/env python3
"""
直接测试FRED分类API
"""

import os
import requests
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", r"D:\Macro")
load_dotenv(os.path.join(BASE, "macrolab.env"))

API_KEY = os.getenv("FRED_API_KEY")
API_ROOT = "https://api.stlouisfed.org/fred"

def test_direct_api():
    print("直接测试FRED分类API...")
    
    # 测试获取顶层分类
    url = f"{API_ROOT}/category/children"
    params = {
        "api_key": API_KEY,
        "file_type": "json",
        "category_id": 0
    }
    
    print(f"请求URL: {url}")
    print(f"参数: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"响应状态: {response.status_code}")
        print(f"响应内容: {data}")
        
        if 'categories' in data:
            categories = data['categories']
            print(f"找到 {len(categories)} 个顶层分类")
            
            # 显示前5个分类
            for i, cat in enumerate(categories[:5]):
                print(f"  {i+1}. ID: {cat.get('id')}, 名称: {cat.get('name')}")
        else:
            print("响应格式异常")
            
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    test_direct_api()
