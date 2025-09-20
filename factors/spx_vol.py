"""
标普500波动率因子

基于SP500序列计算股市波动率。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class SPX_VOL(Factor):
    """标普500波动率因子"""
    
    id = "SP500_Vol"
    name = "标普500波动率"
    units = None
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "SP500")
        self.lookback_days = cfg.get("lookback_days", 2)
    
    def fetch(self) -> pd.DataFrame:
        """获取标普500数据"""
        try:
            logger.info(f"获取标普500数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取标普500数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算标普500波动率指标"""
        if df.empty or len(df) < self.lookback_days:
            return {"original_value": None}
        
        # 计算最近几天的绝对收益率
        recent_data = df.tail(self.lookback_days)
        returns = recent_data['value'].pct_change().dropna()
        volatility = returns.abs().mean()
        
        return {
            "original_value": float(volatility) if not pd.isna(volatility) else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算标普500波动率风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
