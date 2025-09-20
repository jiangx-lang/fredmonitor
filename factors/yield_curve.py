"""
收益率曲线因子

基于10年期和2年期国债收益率计算收益率曲线斜率。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class YIELD_CURVE(Factor):
    """收益率曲线因子"""
    
    id = "Yield_Spread"
    name = "收益率曲线斜率"
    units = "百分比"
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.long_series = cfg.get("long_series", "DGS10")
        self.short_series = cfg.get("short_series", "DGS2")
    
    def fetch(self) -> pd.DataFrame:
        """获取收益率曲线数据"""
        try:
            logger.info(f"获取收益率曲线数据: {self.long_series}和{self.short_series}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取收益率曲线数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算收益率曲线指标"""
        if df.empty:
            return {
                "original_value": None,
                "long_rate": None,
                "short_rate": None,
                "spread": None
            }
        
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        
        return {
            "original_value": float(latest_value) if latest_value is not None else None,
            "long_rate": None,   # 10年期收益率
            "short_rate": None,  # 2年期收益率
            "spread": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算收益率曲线风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("spread"), global_cfg)
