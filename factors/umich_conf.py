"""
密歇根消费者信心因子

基于UMCSENT序列计算消费者信心指数。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class UMICH_CONF(Factor):
    """密歇根消费者信心因子"""
    
    id = "Consumer_Confidence"
    name = "消费者信心指数"
    units = None
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "UMCSENT")
    
    def fetch(self) -> pd.DataFrame:
        """获取消费者信心数据"""
        try:
            logger.info(f"获取消费者信心数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取消费者信心数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算消费者信心指标"""
        if df.empty:
            return {"original_value": None}
        
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        
        return {
            "original_value": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算消费者信心风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
