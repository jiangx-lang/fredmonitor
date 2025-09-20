#!/usr/bin/env python3
"""
基于FRED HTTP API的数据同步脚本

直接使用FRED官方REST API，避免第三方库的不稳定性。
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
# 确保日志目录存在
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fred")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "sync.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
# 确保从项目根目录加载环境变量
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "macrolab.env")
load_dotenv(env_path)

CATALOG_FILE = os.path.join(BASE, "config", "catalog_fred.yaml")
SERIES_ROOT = pathlib.Path(BASE) / "data" / "fred" / "series"


def ensure_series_dir(series_id: str) -> pathlib.Path:
    """创建序列目录结构"""
    p = SERIES_ROOT / series_id
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
    """
    获取序列元数据，包括下一次发布日期
    
    Args:
        series_id: FRED序列ID
        
    Returns:
        包含元数据的字典
    """
    logger.info(f"获取序列元数据: {series_id}")
    
    # 获取基本元数据
    meta_response = series_info(series_id)
    series_list = meta_response.get("seriess", [])
    
    if not series_list:
        raise ValueError(f"未找到序列 {series_id}")
    
    meta = series_list[0]
    
    # 添加同步时间戳
    meta["sync_timestamp"] = datetime.now().isoformat()
    
    return meta


def fetch_observations(series_id: str) -> pd.DataFrame:
    """
    获取序列观测数据
    
    Args:
        series_id: FRED序列ID
        
    Returns:
        包含观测数据的DataFrame
    """
    logger.info(f"获取序列观测数据: {series_id}")
    
    # 获取全历史数据
    obs_response = series_observations(series_id)
    observations = obs_response.get("observations", [])
    
    if not observations:
        logger.warning(f"序列 {series_id} 无观测数据")
        return pd.DataFrame(columns=["date", "value"])
    
    # 转换为DataFrame
    df = pd.DataFrame(observations)
    
    # 处理缺失值（FRED用"."表示缺失）
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # 转换日期格式
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")
    
    # 只保留有效数据
    df = df[["date", "value"]].dropna()
    
    logger.info(f"序列 {series_id} 获取到 {len(df)} 条有效数据")
    
    return df


def calculate_features(df: pd.DataFrame, calc_config: Optional[Dict[str, Any]]) -> pd.DataFrame:
    """
    计算衍生特征
    
    Args:
        df: 原始数据DataFrame
        calc_config: 计算配置
        
    Returns:
        包含特征的DataFrame
    """
    if not calc_config:
        return df
    
    out = df.sort_values("date").copy()
    
    for name, rule in calc_config.items():
        try:
            if rule.get("op") == "pct_change":
                shift = int(rule.get("shift", 1))
                scale = float(rule.get("scale", 1.0))
                
                # 计算百分比变化
                out[name] = out["value"].pct_change(shift) * scale
                
                logger.debug(f"计算特征 {name}: shift={shift}, scale={scale}")
            else:
                logger.warning(f"不支持的特征计算操作: {rule.get('op')}")
                
        except Exception as e:
            logger.error(f"计算特征失败 {name}: {e}")
    
    return out


def sync_series(series_config: Dict[str, Any]) -> None:
    """
    同步单个序列
    
    Args:
        series_config: 序列配置
    """
    series_id = series_config["id"]
    alias = series_config.get("alias", series_id)
    calc_config = series_config.get("calc")
    freshness_days = series_config.get("freshness_days", 45)
    
    logger.info(f"开始同步序列: {series_id} ({alias})")
    
    try:
        # 创建目录
        folder = ensure_series_dir(series_id)
        
        # 1) 获取基本元数据并立即落盘（避免因下次发布日期失败丢失）
        metadata = fetch_metadata(series_id)
        metadata["alias"] = alias
        metadata["freshness_days"] = freshness_days
        metadata["next_release"] = "N/A"  # 默认值
        
        # 原子写入基本元数据
        safe_write_json(folder / "meta.json", metadata)
        
        # 2) 尝试获取下次发布日期（不阻塞主流程）
        try:
            next_release = get_next_release_date(series_id)
            if next_release != "N/A":
                metadata["next_release"] = next_release
                safe_write_json(folder / "meta.json", metadata)
                logger.debug(f"序列 {series_id} 下次发布日期: {next_release}")
        except Exception as e:
            logger.debug(f"序列 {series_id} 下次发布日期查询失败: {e}")
        
        # 3) 获取观测数据
        df = fetch_observations(series_id)
        
        if df.empty:
            logger.warning(f"序列 {series_id} 无有效数据，创建空文件")
            # 创建空的CSV和Parquet文件
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
        logger.error(f"✗ 同步序列失败 {series_id}: {e}")


def main():
    """主函数"""
    logger.info("开始FRED数据同步 (HTTP API)")
    
    # 确保目录存在
    SERIES_ROOT.mkdir(parents=True, exist_ok=True)
    
    # 加载配置
    try:
        with open(CATALOG_FILE, "r", encoding="utf-8") as f:
            catalog = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return
    
    series_list = catalog.get("series", [])
    if not series_list:
        logger.warning("未找到需要同步的序列")
        return
    
    logger.info(f"找到 {len(series_list)} 个序列需要同步")
    
    # 并行同步序列（最多4个并发）
    success_count = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交所有任务
        future_to_series = {
            executor.submit(sync_series, series_config): series_config 
            for series_config in series_list
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_series):
            series_config = future_to_series[future]
            series_id = series_config.get("id", "unknown")
            
            try:
                future.result()  # 获取结果，如果有异常会抛出
                success_count += 1
                logger.info(f"✓ 序列 {series_id} 同步完成")
            except Exception as e:
                logger.error(f"✗ 序列 {series_id} 同步失败: {e}")
    
    logger.info(f"FRED数据同步完成: {success_count}/{len(series_list)} 成功")


if __name__ == "__main__":
    main()
