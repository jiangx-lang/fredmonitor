"""
美元指数波动率因子

基于DTWEXBGS序列计算美元指数波动率。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class DXY_VOL(Factor):
    """美元指数波动率因子"""
    
    id = "DXY_Vol"
    name = "美元指数波动率"
    units = None
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "DTWEXBGS")
        self.vol_window = cfg.get("vol_window", 5)
    
    def fetch(self) -> pd.DataFrame:
        """获取美元指数数据"""
        try:
            logger.info(f"获取美元指数数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取美元指数数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算美元指数波动率指标"""
        if df.empty or len(df) < self.vol_window:
            return {"original_value": None}
        
        # 计算最近几天的收益率标准差
        recent_data = df.tail(self.vol_window)
        returns = recent_data['value'].pct_change().dropna()
        volatility = returns.std()
        
        return {
            "original_value": float(volatility) if not pd.isna(volatility) else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算美元指数波动率风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
