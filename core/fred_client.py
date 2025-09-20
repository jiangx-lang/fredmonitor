"""
FRED API客户端

封装FRED API调用，提供数据获取和缓存功能。
"""

import os
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from fredapi import Fred
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from .cache import CacheManager

logger = logging.getLogger(__name__)


class FredClient:
    """FRED API客户端"""
    
    def __init__(self, api_key: str, cache_manager: CacheManager):
        """
        初始化FRED客户端
        
        Args:
            api_key: FRED API密钥
            cache_manager: 缓存管理器
        """
        self.api_key = api_key
        self.cache_manager = cache_manager
        self.fred = Fred(api_key=api_key)
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_series(self, series_id: str, start_date: Optional[str] = None, 
                   end_date: Optional[str] = None) -> pd.Series:
        """
        获取FRED数据序列
        
        Args:
            series_id: FRED序列ID
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）
            
        Returns:
            数据序列
        """
        try:
            logger.info(f"获取FRED数据: {series_id}")
            series = self.fred.get_series(
                series_id, 
                start=start_date, 
                end=end_date
            )
            return series
        except Exception as e:
            logger.error(f"获取FRED数据失败 {series_id}: {e}")
            raise
    
    def get_series_cached(self, series_id: str, max_days: int = 7) -> pd.Series:
        """
        获取缓存的FRED数据序列
        
        Args:
            series_id: FRED序列ID
            max_days: 最大缓存天数
            
        Returns:
            数据序列
        """
        cache_file = f"fred/{series_id}.csv"
        
        # 检查缓存是否有效
        if self.cache_manager.is_fresh(cache_file, max_days):
            logger.info(f"使用缓存数据: {series_id}")
            return self.cache_manager.load_csv(cache_file)
        
        # 获取新数据
        logger.info(f"获取新数据: {series_id}")
        series = self.get_series(series_id)
        
        # 保存到缓存
        self.cache_manager.save_csv(series, cache_file)
        
        return series
    
    def get_latest_within_days(self, series: pd.Series, max_days: int) -> Optional[float]:
        """
        获取指定天数内的最新值
        
        Args:
            series: 数据序列
            max_days: 最大天数
            
        Returns:
            最新值或None（如果数据过期）
        """
        if series.empty:
            return None
            
        latest_date = series.index[-1]
        cutoff_date = datetime.now() - timedelta(days=max_days)
        
        if latest_date.timestamp() < cutoff_date.timestamp():
            logger.warning(f"数据过期: 最新日期 {latest_date.date()}, 要求 {cutoff_date.date()}")
            return None
            
        return float(series.iloc[-1])
    
    def get_latest(self, series_id: str, max_days: int) -> Optional[float]:
        """
        获取序列的最新值（在指定天数内）
        
        Args:
            series_id: FRED序列ID
            max_days: 最大天数
            
        Returns:
            最新值或None
        """
        try:
            series = self.get_series_cached(series_id, max_days)
            return self.get_latest_within_days(series, max_days)
        except Exception as e:
            logger.error(f"获取最新值失败 {series_id}: {e}")
            return None
