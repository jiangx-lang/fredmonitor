#!/usr/bin/env python3
"""
下载剩余Monetary Data子分类的数据
"""

import os
import json
import pathlib
import yaml
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import (
    series_info, 
    series_observations, 
    get_next_release_date, 
    polite_sleep
)
from scripts.duckdb_io import upsert_parquet

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data/fred/sync.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

CATEGORIES_ROOT = pathlib.Path(BASE) / "data" / "fred" / "categories"

# 要同步的目录文件
CATALOG_FILES = [
    "m2_minus_small_time_deposits_catalog.yaml",
    "securities_loans_assets_liabilities_catalog.yaml"
]

def get_subcategory_path(series_id: str, subcategory: str) -> pathlib.Path:
    """获取子分类数据的保存路径"""
    return CATEGORIES_ROOT / "Monetary_Data" / subcategory / "series" / series_id

def ensure_subcategory_series_dir(series_id: str, subcategory: str) -> pathlib.Path:
    """创建子分类序列目录结构"""
    p = get_subcategory_path(series_id, subcategory)
    (p / "notes" / "attachments").mkdir(parents=True, exist_ok=True)
    
    # 创建空的custom_notes.md文件（如果不存在）
    custom_notes_file = p / "notes" / "custom_notes.md"
    if not custom_notes_file.exists():
        custom_notes_file.write_text("", encoding="utf-8")
    
    return p

def safe_write_json(path: pathlib.Path, obj: dict):
    """原子写入JSON文件"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2), 
        encoding="utf-8"
    )
    tmp.replace(path)

def fetch_metadata(series_id: str) -> Dict[str, Any]:
    """获取序列元数据"""
    logger.info(f"获取序列元数据: {series_id}")
    
    meta_response = series_info(series_id)
    series_list = meta_response.get("seriess", [])
    
    if not series_list:
        raise ValueError(f"未找到序列 {series_id}")
    
    meta = series_list[0]
    meta["sync_timestamp"] = datetime.now().isoformat()
    
    return meta

def fetch_observations(series_id: str) -> pd.DataFrame:
    """获取序列观测数据"""
    logger.info(f"获取序列观测数据: {series_id}")
    
    obs_response = series_observations(series_id)
    observations = obs_response.get("observations", [])
    
    if not observations:
        logger.warning(f"序列 {series_id} 无观测数据")
        return pd.DataFrame(columns=["date", "value"])
    
    df = pd.DataFrame(observations)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")
    df = df[["date", "value"]].dropna()
    
    logger.info(f"序列 {series_id} 获取到 {len(df)} 条有效数据")
    
    return df

def calculate_features(df: pd.DataFrame, calc_config: Optional[Dict[str, Any]]) -> pd.DataFrame:
    """计算衍生特征"""
    if not calc_config:
        return df
    
    out = df.sort_values("date").copy()
    
    for name, rule in calc_config.items():
        try:
            if rule.get("op") == "pct_change":
                shift = int(rule.get("shift", 1))
                scale = float(rule.get("scale", 1.0))
                out[name] = out["value"].pct_change(shift) * scale
                logger.debug(f"计算特征 {name}: shift={shift}, scale={scale}")
            else:
                logger.warning(f"不支持的特征计算操作: {rule.get('op')}")
                
        except Exception as e:
            logger.error(f"计算特征失败 {name}: {e}")
    
    return out

def sync_subcategory_series(series_config: Dict[str, Any]) -> None:
    """同步单个子分类序列"""
    series_id = series_config["id"]
    alias = series_config.get("alias", series_id)
    subcategory = series_config.get("subcategory", "Unknown")
    calc_config = series_config.get("calc")
    
    logger.info(f"开始同步 {subcategory} 序列: {series_id} ({alias})")
    
    try:
        # 创建目录
        folder = ensure_subcategory_series_dir(series_id, subcategory)
        
        # 获取元数据
        metadata = fetch_metadata(series_id)
        metadata["alias"] = alias
        metadata["category_id"] = 17
        metadata["subcategory"] = subcategory
        metadata["next_release"] = "N/A"
        
        # 尝试获取下次发布日期
        try:
            next_release = get_next_release_date(series_id)
            if next_release != "N/A":
                metadata["next_release"] = next_release
                logger.debug(f"序列 {series_id} 下次发布日期: {next_release}")
        except Exception as e:
            logger.debug(f"序列 {series_id} 下次发布日期查询失败: {e}")
        
        # 保存元数据
        safe_write_json(folder / "meta.json", metadata)
        
        # 获取观测数据
        df = fetch_observations(series_id)
        
        if df.empty:
            logger.warning(f"序列 {series_id} 无有效数据，创建空文件")
            empty_df = pd.DataFrame(columns=["date", "value"])
            empty_df.to_csv(folder / "raw.csv", index=False, encoding="utf-8")
            empty_df.to_parquet(folder / "features.parquet", index=False)
            logger.info(f"✓ {series_id}: 0 条数据（空序列）")
            return
        
        # 保存原始数据
        raw_file = folder / "raw.csv"
        df.to_csv(raw_file, index=False, encoding="utf-8")
        
        # 计算特征
        features_df = calculate_features(df, calc_config)
        
        # 保存特征数据
        features_file = folder / "features.parquet"
        features_df.to_parquet(features_file, index=False)
        
        # 导入DuckDB
        try:
            upsert_parquet(series_id, str(features_file))
            logger.debug(f"✓ {series_id} 已导入DuckDB")
        except Exception as e:
            logger.warning(f"✗ {series_id} DuckDB导入失败: {e}")
        
        # 礼貌性延迟
        polite_sleep()
        
        logger.info(f"✓ {series_id}: {len(df)} 条数据, 最新日期: {df['date'].max().date()}")
        
    except Exception as e:
        logger.error(f"✗ 同步 {subcategory} 序列失败 {series_id}: {e}")

def sync_catalog_file(catalog_file: str):
    """同步单个目录文件"""
    
    catalog_path = pathlib.Path(BASE) / "config" / catalog_file
    
    if not catalog_path.exists():
        logger.warning(f"目录文件不存在: {catalog_path}")
        return
    
    logger.info(f"开始同步目录文件: {catalog_file}")
    
    # 加载配置
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            catalog = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败 {catalog_file}: {e}")
        return
    
    series_list = catalog.get("series", [])
    if not series_list:
        logger.warning(f"目录文件 {catalog_file} 中没有系列")
        return
    
    subcategory = catalog.get("metadata", {}).get("subcategory", "Unknown")
    logger.info(f"找到 {len(series_list)} 个 {subcategory} 序列需要同步")
    
    # 并行同步序列（最多2个并发，避免API限制）
    success_count = 0
    with ThreadPoolExecutor(max_workers=2) as executor:
        # 提交所有任务
        future_to_series = {
            executor.submit(sync_subcategory_series, series_config): series_config 
            for series_config in series_list
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_series):
            series_config = future_to_series[future]
            series_id = series_config.get("id", "unknown")
            
            try:
                future.result()
                success_count += 1
                logger.info(f"✓ {subcategory} 序列 {series_id} 同步完成")
            except Exception as e:
                logger.error(f"✗ {subcategory} 序列 {series_id} 同步失败: {e}")
    
    logger.info(f"{subcategory} 数据同步完成: {success_count}/{len(series_list)} 成功")

def main():
    """主函数"""
    logger.info("开始剩余Monetary Data子分类数据同步")
    
    # 确保目录存在
    CATEGORIES_ROOT.mkdir(parents=True, exist_ok=True)
    
    for catalog_file in CATALOG_FILES:
        logger.info(f"\n{'='*60}")
        sync_catalog_file(catalog_file)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"剩余Monetary Data子分类数据同步完成!")
    logger.info(f"总共处理了 {len(CATALOG_FILES)} 个目录文件")

if __name__ == "__main__":
    main()
