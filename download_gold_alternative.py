#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
黄金价格数据下载 - 替代方案
由于 GOLDAMGBD228NLBM 已在2022年停用，使用替代数据源
"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import os
import pandas as pd
import pathlib
from datetime import datetime, timedelta

BASE = pathlib.Path(__file__).parent
SERIES_ROOT = BASE / "data" / "fred" / "series"
OUTPUT_DIR = BASE / "data" / "series"

def download_gold_from_yahoo():
    """从Yahoo Finance下载黄金价格（GLD ETF作为代理）"""
    try:
        import yfinance as yf
        
        print("📥 从Yahoo Finance下载GLD ETF价格（黄金代理）...")
        
        # GLD是SPDR Gold Shares ETF，紧密跟踪黄金价格
        ticker = yf.Ticker("GLD")
        
        # 获取历史数据（从2004年开始，GLD成立时间）
        hist = ticker.history(start="2004-11-18", end=None)
        
        if hist.empty:
            print("❌ 无法从Yahoo Finance获取数据")
            return False
        
        # 使用收盘价
        gold_prices = hist['Close']
        
        # 转换为DataFrame
        df = gold_prices.reset_index()
        df.columns = ['date', 'value']
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # 保存到FRED格式的目录结构
        gold_dir = SERIES_ROOT / "GOLDAMGBD228NLBM"
        gold_dir.mkdir(parents=True, exist_ok=True)
        
        raw_file = gold_dir / "raw.csv"
        df.to_csv(raw_file, index=False, encoding='utf-8')
        
        # 同时保存到data/series目录（用于合成指标计算）
        output_file = OUTPUT_DIR / "GOLDAMGBD228NLBM.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.set_index('date').to_csv(output_file)
        
        print(f"✅ 成功下载 {len(df)} 条数据")
        print(f"   最新日期: {df['date'].max()}")
        print(f"   最新价格: {df.iloc[-1]['value']:.2f} USD")
        print(f"   文件保存至: {raw_file}")
        
        return True
        
    except ImportError:
        print("❌ yfinance库未安装，尝试安装: pip install yfinance")
        return False
    except Exception as e:
        print(f"❌ 下载失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def download_gold_from_alpha_vantage():
    """从Alpha Vantage下载黄金价格（需要API密钥）"""
    try:
        import requests
        from dotenv import load_dotenv
        
        load_dotenv(BASE / "macrolab.env")
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        
        if not api_key:
            print("⚠️ Alpha Vantage API密钥未设置，跳过")
            return False
        
        print("📥 从Alpha Vantage下载黄金价格...")
        
        # Alpha Vantage的黄金价格API
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": "GLD",
            "apikey": api_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        # 这里只获取实时数据，历史数据需要付费API
        print("⚠️ Alpha Vantage免费版只提供实时数据，历史数据需要付费")
        return False
        
    except Exception as e:
        print(f"❌ Alpha Vantage下载失败: {e}")
        return False

def download_gold_from_csv():
    """从CSV文件导入黄金价格（如果用户有本地文件）"""
    csv_path = BASE / "data" / "gold_price.csv"
    
    if not csv_path.exists():
        print(f"⚠️ 未找到本地CSV文件: {csv_path}")
        print("   请将黄金价格数据保存为CSV格式：")
        print("   格式: date,value")
        print("   示例: 2024-01-01,2000.50")
        return False
    
    try:
        print(f"📥 从CSV文件导入: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # 确保有date和value列
        if 'date' not in df.columns or 'value' not in df.columns:
            print("❌ CSV文件格式错误，需要包含'date'和'value'列")
            return False
        
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # 保存到FRED格式的目录结构
        gold_dir = SERIES_ROOT / "GOLDAMGBD228NLBM"
        gold_dir.mkdir(parents=True, exist_ok=True)
        
        raw_file = gold_dir / "raw.csv"
        df[['date', 'value']].to_csv(raw_file, index=False, encoding='utf-8')
        
        # 同时保存到data/series目录
        output_file = OUTPUT_DIR / "GOLDAMGBD228NLBM.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.set_index('date')[['value']].to_csv(output_file)
        
        print(f"✅ 成功导入 {len(df)} 条数据")
        print(f"   最新日期: {df['date'].max()}")
        print(f"   最新价格: {df.iloc[-1]['value']:.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV导入失败: {e}")
        return False

def main():
    print("=" * 60)
    print("黄金价格数据下载 - 替代方案")
    print("=" * 60)
    print("\n注意: GOLDAMGBD228NLBM 序列已在2022年1月31日停用")
    print("=" * 60)
    
    # 方法1: 尝试从Yahoo Finance下载（GLD ETF）
    print("\n方法1: 从Yahoo Finance下载GLD ETF价格...")
    if download_gold_from_yahoo():
        print("✅ 方法1成功！")
        return
    
    # 方法2: 尝试从CSV文件导入
    print("\n方法2: 从本地CSV文件导入...")
    if download_gold_from_csv():
        print("✅ 方法2成功！")
        return
    
    # 方法3: Alpha Vantage（需要API密钥）
    print("\n方法3: 从Alpha Vantage下载...")
    if download_gold_from_alpha_vantage():
        print("✅ 方法3成功！")
        return
    
    print("\n" + "=" * 60)
    print("❌ 所有方法都失败")
    print("=" * 60)
    print("\n建议:")
    print("1. 安装yfinance库: pip install yfinance")
    print("2. 或准备CSV文件: data/gold_price.csv (格式: date,value)")
    print("3. 或使用其他数据源手动导入")
    print("=" * 60)

if __name__ == "__main__":
    main()
