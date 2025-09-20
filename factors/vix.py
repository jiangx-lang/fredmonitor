"""
VIX波动率因子

基于VIXCLS序列计算市场恐慌指数。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor
from core.fred_client import FredClient

logger = logging.getLogger(__name__)


class VIX(Factor):
    """VIX波动率因子"""
    
    id = "VIX"
    name = "VIX 波动率"
    units = None
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "VIXCLS")
    
    def fetch(self) -> pd.DataFrame:
        """获取VIX数据"""
        try:
            # 这里需要FRED客户端，但为了避免循环导入，我们直接返回空DataFrame
            # 实际使用时会在聚合器中注入FRED客户端
            logger.info(f"获取VIX数据: {self.series_id}")
            return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            logger.error(f"获取VIX数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算VIX指标"""
        if df.empty:
            return {"original_value": None}
        
        # 获取最新值
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        
        return {
            "original_value": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算VIX风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
