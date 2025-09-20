"""
数据聚合器

聚合所有因子的数据，计算加权总分。
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
import logging

from .fred_client import FredClient
from .cache import CacheManager
from .scoring import calculate_factor_score, get_risk_level
from .registry import FactorRegistry

logger = logging.getLogger(__name__)


class DataAggregator:
    """数据聚合器"""
    
    def __init__(self, fred_client: FredClient, cache_manager: CacheManager, 
                 registry: FactorRegistry, global_config: Dict[str, Any]):
        """
        初始化数据聚合器
        
        Args:
            fred_client: FRED客户端
            cache_manager: 缓存管理器
            registry: 因子注册表
            global_config: 全局配置
        """
        self.fred_client = fred_client
        self.cache_manager = cache_manager
        self.registry = registry
        self.global_config = global_config
        self.weights = global_config.get("weights", {})
        self.freshness_days = global_config.get("freshness_days", {})
    
    def run_daily_analysis(self, target_date: datetime = None) -> Dict[str, Any]:
        """
        运行每日分析
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            分析结果字典
        """
        if target_date is None:
            target_date = datetime.now()
        
        logger.info(f"开始每日分析: {target_date.date()}")
        
        # 存储所有因子结果
        all_factor_data = []
        factor_scores = {}
        factor_values = {}
        
        # 处理每个因子
        for factor_id, factor in self.registry.get_all_factors().items():
            try:
                logger.info(f"处理因子: {factor_id}")
                
                # 获取数据
                df = factor.fetch()
                if df.empty:
                    logger.warning(f"因子 {factor_id} 无数据")
                    continue
                
                # 计算指标
                metrics = factor.compute(df)
                
                # 计算评分
                score = calculate_factor_score(factor_id, metrics.get("original_value"), self.global_config)
                
                # 存储结果
                factor_scores[factor_id] = score
                factor_values[factor_id] = metrics.get("original_value")
                
                # 转换为DataFrame
                factor_df = factor.to_frame(target_date, metrics, score)
                all_factor_data.append(factor_df)
                
                logger.info(f"因子 {factor_id}: 值={metrics.get('original_value'):.4f}, 评分={score:.2f}")
                
            except Exception as e:
                logger.error(f"处理因子失败 {factor_id}: {e}")
                # 记录失败但继续处理其他因子
                factor_scores[factor_id] = 0.0
                factor_values[factor_id] = None
        
        # 合并所有因子数据
        if all_factor_data:
            daily_df = pd.concat(all_factor_data, ignore_index=True)
        else:
            daily_df = pd.DataFrame()
        
        # 计算加权总分
        total_score = self._calculate_weighted_total(factor_scores)
        
        # 获取风险等级
        risk_level = get_risk_level(total_score, self.global_config.get("risk_thresholds", {}))
        
        # 创建汇总结果
        summary = {
            "date": target_date,
            "factor_scores": factor_scores,
            "factor_values": factor_values,
            "total_score": total_score,
            "risk_level": risk_level,
            "daily_data": daily_df
        }
        
        # 保存数据
        self._save_daily_data(daily_df, target_date)
        
        logger.info(f"每日分析完成: 总分={total_score:.2f}, 风险等级={risk_level}")
        
        return summary
    
    def _calculate_weighted_total(self, factor_scores: Dict[str, float]) -> float:
        """
        计算加权总分
        
        Args:
            factor_scores: 因子评分字典
            
        Returns:
            加权总分
        """
        total_score = 0.0
        total_weight = 0.0
        
        for factor_id, score in factor_scores.items():
            weight = self.weights.get(factor_id, 0.0)
            total_score += score * weight
            total_weight += weight
        
        if total_weight > 0:
            return total_score / total_weight
        else:
            return 0.0
    
    def _save_daily_data(self, daily_df: pd.DataFrame, target_date: datetime) -> None:
        """
        保存每日数据
        
        Args:
            daily_df: 每日数据DataFrame
            target_date: 目标日期
        """
        if daily_df.empty:
            return
        
        # 保存当日数据
        date_str = target_date.strftime("%Y-%m-%d")
        daily_file = f"daily/{date_str}.parquet"
        self.cache_manager.save_parquet(daily_df, daily_file)
        
        # 追加到历史数据
        history_file = "history/factors.parquet"
        self.cache_manager.append_parquet(daily_df, history_file)
        
        # 同时保存CSV版本
        if self.global_config.get("outputs", {}).get("write_csv", True):
            csv_file = f"daily/{date_str}.csv"
            csv_path = os.path.join(self.cache_manager.processed_dir, csv_file)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            daily_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            history_csv = "history/factors.csv"
            history_csv_path = os.path.join(self.cache_manager.processed_dir, history_csv)
            if os.path.exists(history_csv_path):
                # 追加到现有CSV
                existing = pd.read_csv(history_csv_path)
                combined = pd.concat([existing, daily_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['date', 'factor_id'], keep='last')
                combined = combined.sort_values(['date', 'factor_id'])
            else:
                combined = daily_df
            combined.to_csv(history_csv_path, index=False, encoding='utf-8-sig')
    
    def get_recent_scores(self, days: int = 5) -> pd.DataFrame:
        """
        获取最近几天的评分
        
        Args:
            days: 天数
            
        Returns:
            最近评分DataFrame
        """
        try:
            history_file = "history/factors.parquet"
            df = self.cache_manager.load_parquet(history_file)
            
            if df.empty:
                return pd.DataFrame()
            
            # 按日期分组计算每日总分
            daily_totals = []
            for date, group in df.groupby('date'):
                factor_scores = dict(zip(group['factor_id'], group['score']))
                total_score = self._calculate_weighted_total(factor_scores)
                daily_totals.append({
                    'date': date,
                    'total_score': total_score
                })
            
            recent_df = pd.DataFrame(daily_totals)
            recent_df = recent_df.sort_values('date').tail(days)
            
            return recent_df
            
        except Exception as e:
            logger.error(f"获取最近评分失败: {e}")
            return pd.DataFrame()
