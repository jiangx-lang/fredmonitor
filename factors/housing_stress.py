"""
住房压力指数因子

基于CSUSHPINSA序列计算房价年化变化率。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class HOUSING_STRESS(Factor):
    """住房压力指数因子"""
    
    id = "Housing_Stress"
    name = "住房压力指数"
    units = "百分比"
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "CSUSHPINSA")
        self.yoy_periods = cfg.get("yoy_periods", 13)
    
    def fetch(self) -> pd.DataFrame:
        """获取房价数据"""
        try:
            logger.info(f"获取房价数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取房价数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算住房压力指标"""
        if df.empty or len(df) < self.yoy_periods:
            return {"original_value": None}
        
        # 计算年化变化率（使用最近13个数据点）
        recent_data = df.tail(self.yoy_periods)
        if len(recent_data) < 2:
            return {"original_value": None}
        
        # 计算年化变化率
        yoy_change = (recent_data['value'].iloc[-1] / recent_data['value'].iloc[0] - 1) * 100
        
        return {
            "original_value": float(yoy_change) if not pd.isna(yoy_change) else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算住房压力风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
