#!/usr/bin/env python3
"""
FRED数据同步脚本 - 支持命令行参数

用法:
python sync_fred_data.py --series SERIES_ID --start-date YYYY-MM-DD
python sync_fred_data.py --series T10Y3M --start-date 2000-01-01
"""

import os
import sys
import argparse
import pathlib
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import series_info, series_observations
from scripts.clean_utils import parse_numeric_series

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 加载环境变量
BASE = pathlib.Path(__file__).parent.parent
env_files = [BASE / "macrolab.env", BASE / ".env"]
loaded = False
for env_file in env_files:
    if env_file.exists():
        try:
            load_dotenv(env_file, encoding='utf-8')
            loaded = True
            break
        except:
            continue

if not loaded:
    logger.warning("未找到环境变量文件，将使用系统环境变量")

def sync_single_series(series_id: str, start_date: str = "2000-01-01") -> bool:
    """同步单个序列数据"""
    try:
        logger.info(f"开始同步序列: {series_id}")
        
        # 获取序列信息
        info_response = series_info(series_id)
        if not info_response:
            logger.error(f"无法获取序列信息: {series_id}")
            return False
        
        # 获取观测数据
        data_response = series_observations(
            series_id, 
            observation_start=start_date
        )
        
        if not data_response or 'observations' not in data_response:
            logger.error(f"无法获取观测数据: {series_id}")
            return False
        
        observations = data_response.get('observations', [])
        if not observations:
            logger.warning(f"序列 {series_id} 无观测数据")
            return False
        
        # 转换为DataFrame
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        
        # 解析数值
        ts = parse_numeric_series(df['value'])
        ts = ts.dropna()
        
        if ts.empty:
            logger.warning(f"序列 {series_id} 解析后无有效数据")
            return False
        
        # 创建目录结构
        series_dir = BASE / "data" / "fred" / "series" / series_id
        series_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存原始数据
        raw_file = series_dir / "raw.csv"
        ts.to_csv(raw_file, encoding='utf-8')
        
        # 保存元数据
        metadata = {
            "series_id": series_id,
            "title": info_response.get('title', ''),
            "units": info_response.get('units', ''),
            "frequency": info_response.get('frequency', ''),
            "seasonal_adjustment": info_response.get('seasonal_adjustment', ''),
            "last_updated": datetime.now().isoformat(),
            "data_points": len(ts),
            "start_date": str(ts.index.min()),
            "end_date": str(ts.index.max())
        }
        
        import json
        metadata_file = series_dir / "metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ 序列 {series_id} 同步完成: {len(ts)} 个数据点")
        logger.info(f"  数据范围: {ts.index.min()} 至 {ts.index.max()}")
        logger.info(f"  保存位置: {raw_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 同步序列失败 {series_id}: {e}")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='FRED数据同步脚本')
    parser.add_argument('--series', required=True, help='FRED序列ID')
    parser.add_argument('--start-date', default='2000-01-01', help='开始日期 (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    logger.info(f"开始同步序列: {args.series}")
    logger.info(f"开始日期: {args.start_date}")
    
    success = sync_single_series(args.series, args.start_date)
    
    if success:
        logger.info("✓ 同步完成")
        sys.exit(0)
    else:
        logger.error("✗ 同步失败")
        sys.exit(1)

if __name__ == "__main__":
    main()

