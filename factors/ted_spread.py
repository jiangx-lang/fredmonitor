"""
TED利差因子

基于SOFR和3个月国债收益率计算TED利差。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class TED_SPREAD(Factor):
    """TED利差因子"""
    
    id = "TED"
    name = "TED利差"
    units = "百分比"
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.sofr_series = cfg.get("sofr_series", "SOFR")
        self.tbill_series = cfg.get("tbill_series", "DTB3")
        self.moving_avg_days = cfg.get("moving_avg_days", 20)
    
    def fetch(self) -> pd.DataFrame:
        """获取TED利差相关数据"""
        try:
            logger.info(f"获取TED利差数据: SOFR和{self.tbill_series}")
            # 返回空DataFrame，实际数据获取在聚合器中处理
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取TED利差数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算TED利差指标"""
        if df.empty:
            return {
                "original_value": None,
                "sofr_avg": None,
                "tbill": None,
                "ted": None
            }
        
        # 这里需要处理SOFR和国债数据
        # 实际实现中，df应该包含两个序列的数据
        # 为了简化，我们假设df已经包含了计算好的TED利差
        
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        
        return {
            "original_value": float(latest_value) if latest_value is not None else None,
            "sofr_avg": None,  # SOFR 20日平均值
            "tbill": None,     # 3个月国债收益率
            "ted": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算TED利差风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("ted"), global_cfg)
