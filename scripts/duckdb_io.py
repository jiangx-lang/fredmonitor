#!/usr/bin/env python3
"""
DuckDB数据湖接入模块

提供DuckDB的读写接口，支持Parquet文件导入和SQL查询。
"""

import os
import pathlib
import duckdb
from typing import Optional
import pandas as pd
import threading
import time

# 基础路径配置
BASE = os.getenv("BASE_DIR", os.getcwd())
DB_DIR = pathlib.Path(BASE) / "data" / "lake"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "fred.duckdb"

# DuckDB连接锁，防止并发访问冲突
_db_lock = threading.Lock()


def upsert_parquet(series_id: str, parquet_path: str) -> None:
    """
    将Parquet文件导入DuckDB
    
    Args:
        series_id: 序列ID，用作表名
        parquet_path: Parquet文件路径
    """
    # 使用锁确保DuckDB操作的线程安全
    with _db_lock:
        max_retries = 3
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                con = duckdb.connect(str(DB_PATH))
                
                # Windows路径转斜杠
                p = parquet_path.replace("\\", "/")
                
                # 直接创建表，不使用schema
                con.execute(f"""
                    CREATE OR REPLACE TABLE {series_id} AS 
                    SELECT * FROM read_parquet('{p}')
                """)
                
                con.close()
                return  # 成功则直接返回
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # 递增延迟
                    continue
                else:
                    raise Exception(f"DuckDB upsert failed for {series_id}: {e}")


def query(sql: str) -> pd.DataFrame:
    """
    执行SQL查询
    
    Args:
        sql: SQL查询语句
        
    Returns:
        查询结果DataFrame
    """
    with _db_lock:
        try:
            con = duckdb.connect(str(DB_PATH))
            df = con.execute(sql).df()
            con.close()
            return df
        except Exception as e:
            raise Exception(f"DuckDB query failed: {e}")


def list_tables() -> list:
    """
    列出所有表
    
    Returns:
        表名列表
    """
    with _db_lock:
        try:
            con = duckdb.connect(str(DB_PATH))
            tables_df = con.execute("SHOW TABLES;").df()
            con.close()
            return tables_df["name"].tolist() if not tables_df.empty else []
        except Exception as e:
            raise Exception(f"Failed to list tables: {e}")


def test_connection() -> bool:
    """
    测试DuckDB连接
    
    Returns:
        连接是否成功
    """
    with _db_lock:
        try:
            con = duckdb.connect(str(DB_PATH))
            con.execute("SELECT 1 as test;")
            con.close()
            return True
        except Exception as e:
            print(f"DuckDB连接测试失败: {e}")
            return False


if __name__ == "__main__":
    # 测试连接
    if test_connection():
        print("✓ DuckDB连接成功")
        
        # 列出表
        tables = list_tables()
        print(f"✓ 找到 {len(tables)} 个表: {tables}")
    else:
        print("✗ DuckDB连接失败")