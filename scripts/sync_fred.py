#!/usr/bin/env python3
"""
FRED数据同步脚本

从FRED API同步数据到本地，计算特征，存储到Parquet和DuckDB。
"""

import os
import json
import time
import yaml
import duckdb
import pathlib
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
from fredapi import Fred
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv

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

# 初始化FRED客户端
FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    logger.error("未设置FRED_API_KEY环境变量")
    exit(1)

fred = Fred(api_key=FRED_API_KEY)


def setup_series_directory(series_id: str) -> pathlib.Path:
    """创建序列目录结构"""
    p = pathlib.Path(BASE) / "data" / "fred" / "series" / series_id
    (p / "notes" / "attachments").mkdir(parents=True, exist_ok=True)
    
    # 创建空的custom_notes.md文件（如果不存在）
    custom_notes_file = p / "notes" / "custom_notes.md"
    if not custom_notes_file.exists():
        custom_notes_file.write_text("", encoding="utf-8")
    
    return p


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def get_series_info(series_id: str) -> Dict[str, Any]:
    """获取序列元数据"""
    try:
        logger.info(f"获取序列元数据: {series_id}")
        info = fred.get_series_info(series_id)
        return info or {}
    except Exception as e:
        logger.error(f"获取序列元数据失败 {series_id}: {e}")
        raise


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def get_series_history(series_id: str) -> pd.Series:
    """获取序列历史数据"""
    try:
        logger.info(f"获取序列历史数据: {series_id}")
        series = fred.get_series(series_id)
        return series
    except Exception as e:
        logger.error(f"获取序列历史数据失败 {series_id}: {e}")
        raise


def compute_features(df: pd.DataFrame, calc_spec: Dict[str, Any]) -> pd.DataFrame:
    """计算特征指标"""
    out = df.sort_values("date").copy()
    
    if not calc_spec:
        return out
    
    for name, rule in calc_spec.items():
        try:
            if rule["op"] == "pct_change":
                shift = rule.get("shift", 1)
                scale = rule.get("scale", 1.0)
                out[name] = out["value"].pct_change(shift) * scale
                logger.debug(f"计算特征 {name}: shift={shift}, scale={scale}")
            else:
                logger.warning(f"不支持的特征计算操作: {rule['op']}")
        except Exception as e:
            logger.error(f"计算特征失败 {name}: {e}")
    
    return out


def upsert_duckdb(series_id: str, parquet_path: str) -> None:
    """将数据插入DuckDB"""
    try:
        db_path = pathlib.Path(BASE) / "data" / "lake" / "fred.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        con = duckdb.connect(str(db_path))
        
        # 创建schema
        con.execute("CREATE SCHEMA IF NOT EXISTS fred;")
        
        # 创建或替换表
        parquet_path_normalized = str(parquet_path).replace('\\', '/')
        con.execute(f"""
            CREATE OR REPLACE TABLE fred.{series_id} AS 
            SELECT * FROM read_parquet('{parquet_path_normalized}')
        """)
        
        # 创建索引
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{series_id}_date ON fred.{series_id}(date);")
        
        con.close()
        logger.info(f"数据已插入DuckDB: fred.{series_id}")
        
    except Exception as e:
        logger.error(f"插入DuckDB失败 {series_id}: {e}")


def process_series(series_config: Dict[str, Any]) -> None:
    """处理单个序列"""
    series_id = series_config["id"]
    alias = series_config.get("alias", series_id)
    calc_spec = series_config.get("calc", {})
    freshness_days = series_config.get("freshness_days", 45)
    
    logger.info(f"开始处理序列: {series_id} ({alias})")
    
    try:
        # 创建目录
        folder = setup_series_directory(series_id)
        
        # 获取元数据
        info = get_series_info(series_id)
        
        # 保存元数据
        meta_fields = [
            "id", "title", "frequency", "units", "seasonal_adjustment",
            "last_updated", "observation_start", "observation_end", "notes"
        ]
        meta_data = {k: info.get(k) for k in meta_fields}
        meta_data["alias"] = alias
        meta_data["freshness_days"] = freshness_days
        
        meta_file = folder / "meta.json"
        meta_file.write_text(
            json.dumps(meta_data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        
        # 获取历史数据
        series = get_series_history(series_id)
        
        # 转换为DataFrame
        df = series.rename("value").to_frame().reset_index()
        df = df.rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")
        
        # 保存原始数据
        raw_file = folder / "raw.csv"
        df.to_csv(raw_file, index=False, encoding="utf-8")
        
        # 计算特征
        features_df = compute_features(df, calc_spec)
        
        # 保存特征数据
        features_file = folder / "features.parquet"
        features_df.to_parquet(features_file, index=False)
        
        # 插入DuckDB
        upsert_duckdb(series_id, str(features_file))
        
        logger.info(f"序列处理完成: {series_id}, 数据点数: {len(df)}")
        
        # 礼貌性延迟
        time.sleep(0.2)
        
    except Exception as e:
        logger.error(f"处理序列失败 {series_id}: {e}")


def main():
    """主函数"""
    logger.info("开始FRED数据同步")
    
    # 加载配置
    catalog_file = os.path.join(BASE, "config", "catalog_fred.yaml")
    try:
        with open(catalog_file, "r", encoding="utf-8") as f:
            catalog = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return
    
    series_list = catalog.get("series", [])
    if not series_list:
        logger.warning("未找到需要同步的序列")
        return
    
    logger.info(f"找到 {len(series_list)} 个序列需要同步")
    
    # 处理每个序列
    for i, series_config in enumerate(series_list, 1):
        logger.info(f"处理进度: {i}/{len(series_list)}")
        process_series(series_config)
    
    logger.info("FRED数据同步完成")


if __name__ == "__main__":
    main()
