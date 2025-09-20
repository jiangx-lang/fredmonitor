"""
数据缓存管理

提供数据缓存和新鲜度检查功能。
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class CacheManager:
    """缓存管理器"""
    
    def __init__(self, base_dir: str):
        """
        初始化缓存管理器
        
        Args:
            base_dir: 基础目录路径
        """
        self.base_dir = base_dir
        self.raw_dir = os.path.join(base_dir, "data", "raw")
        self.processed_dir = os.path.join(base_dir, "data", "processed")
        
        # 创建目录
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(os.path.join(self.processed_dir, "daily"), exist_ok=True)
        os.makedirs(os.path.join(self.processed_dir, "history"), exist_ok=True)
    
    def get_cache_path(self, relative_path: str) -> str:
        """
        获取缓存文件完整路径
        
        Args:
            relative_path: 相对路径
            
        Returns:
            完整路径
        """
        return os.path.join(self.raw_dir, relative_path)
    
    def is_fresh(self, relative_path: str, max_days: int) -> bool:
        """
        检查缓存文件是否新鲜
        
        Args:
            relative_path: 相对路径
            max_days: 最大天数
            
        Returns:
            是否新鲜
        """
        file_path = self.get_cache_path(relative_path)
        
        if not os.path.exists(file_path):
            return False
            
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        cutoff_time = datetime.now() - timedelta(days=max_days)
        
        return file_time >= cutoff_time
    
    def load_csv(self, relative_path: str) -> pd.Series:
        """
        加载CSV文件为Series
        
        Args:
            relative_path: 相对路径
            
        Returns:
            pandas Series
        """
        file_path = self.get_cache_path(relative_path)
        
        try:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            if len(df.columns) == 1:
                return df.iloc[:, 0]
            else:
                return df
        except Exception as e:
            logger.error(f"加载CSV失败 {file_path}: {e}")
            return pd.Series(dtype=float)
    
    def save_csv(self, data: pd.Series, relative_path: str) -> None:
        """
        保存Series为CSV文件
        
        Args:
            data: pandas Series
            relative_path: 相对路径
        """
        file_path = self.get_cache_path(relative_path)
        
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # 保存数据
            data.to_csv(file_path)
            logger.info(f"保存缓存文件: {file_path}")
        except Exception as e:
            logger.error(f"保存CSV失败 {file_path}: {e}")
    
    def load_parquet(self, relative_path: str) -> pd.DataFrame:
        """
        加载Parquet文件
        
        Args:
            relative_path: 相对路径
            
        Returns:
            pandas DataFrame
        """
        file_path = os.path.join(self.processed_dir, relative_path)
        
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logger.error(f"加载Parquet失败 {file_path}: {e}")
            return pd.DataFrame()
    
    def save_parquet(self, data: pd.DataFrame, relative_path: str) -> None:
        """
        保存DataFrame为Parquet文件
        
        Args:
            data: pandas DataFrame
            relative_path: 相对路径
        """
        file_path = os.path.join(self.processed_dir, relative_path)
        
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # 保存数据
            data.to_parquet(file_path, index=False)
            logger.info(f"保存Parquet文件: {file_path}")
        except Exception as e:
            logger.error(f"保存Parquet失败 {file_path}: {e}")
    
    def append_parquet(self, data: pd.DataFrame, relative_path: str) -> None:
        """
        追加数据到Parquet文件
        
        Args:
            data: pandas DataFrame
            relative_path: 相对路径
        """
        file_path = os.path.join(self.processed_dir, relative_path)
        
        try:
            if os.path.exists(file_path):
                # 读取现有数据
                existing = pd.read_parquet(file_path)
                # 合并数据
                combined = pd.concat([existing, data], ignore_index=True)
                # 去重（基于日期和因子ID）
                combined = combined.drop_duplicates(subset=['date', 'factor_id'], keep='last')
                # 排序
                combined = combined.sort_values(['date', 'factor_id'])
            else:
                combined = data
            
            # 保存合并后的数据
            self.save_parquet(combined, relative_path)
            logger.info(f"追加Parquet文件: {file_path}")
        except Exception as e:
            logger.error(f"追加Parquet失败 {file_path}: {e}")
