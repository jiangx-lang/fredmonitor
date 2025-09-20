"""
高收益信用利差因子

基于BAMLH0A0HYM2序列计算高收益债券信用利差。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class HY_SPREAD(Factor):
    """高收益信用利差因子"""
    
    id = "HY_Spread"
    name = "高收益信用利差"
    units = "百分比"
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "BAMLH0A0HYM2")
    
    def fetch(self) -> pd.DataFrame:
        """获取高收益利差数据"""
        try:
            logger.info(f"获取高收益利差数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取高收益利差数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算高收益利差指标"""
        if df.empty:
            return {"original_value": None}
        
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        
        return {
            "original_value": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算高收益利差风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
