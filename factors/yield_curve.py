"""
收益率曲线倒挂因子
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, Any
from factors.base_factor import BaseFactor


class YieldCurveFactor(BaseFactor):
    """收益率曲线倒挂因子"""
    
    def __init__(self, factor_config: Dict[str, Any]):
        super().__init__(factor_config)
        self.id = "YIELD_CURVE"
        self.name = "收益率曲线倒挂"
        self.series_id = "T10Y2Y"
        self.description = "10年期-2年期国债利差，预测经济衰退"
        self.group = "宏观经济"
        self.weight = 0.20
        self.units = "基点"
        self.higher_is_risk = False  # 反向因子：利差越低风险越高
    
    def fetch(self) -> pd.DataFrame:
        """获取收益率曲线数据"""
        try:
            # 尝试从本地缓存读取
            cache_path = os.path.join("data", "fred", "series", self.series_id, "raw.csv")
            if os.path.exists(cache_path):
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                df = df.reset_index()
                df.columns = ['date', 'value']
                return df.dropna()
            else:
                # 返回空DataFrame，让GUI通过FRED API获取
                return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            print(f"获取收益率曲线数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算收益率曲线相关指标"""
        if df.empty:
            return {}
        
        values = df['value'].astype(float).dropna()
        if len(values) == 0:
            return {}
        
        metrics = {
            'current_value': float(values.iloc[-1]),
            'mean_20d': self.calculate_moving_average(df, 20),
            'mean_50d': self.calculate_moving_average(df, 50),
            'std_20d': self.calculate_volatility(df, 20),
            'percentile_rank': self.get_percentile_rank(df, 252),
            'trend_5d': self.calculate_trend(df, 5),
            'trend_20d': self.calculate_trend(df, 20),
            'max_20d': float(values.tail(20).max()) if len(values) >= 20 else float(values.max()),
            'min_20d': float(values.tail(20).min()) if len(values) >= 20 else float(values.min()),
            'range_20d': 0,
            'inversion_status': self._get_inversion_status(values.iloc[-1]),
        }
        
        # 计算20日范围
        if metrics['max_20d'] > 0 and metrics['min_20d'] > 0:
            metrics['range_20d'] = metrics['max_20d'] - metrics['min_20d']
        
        return metrics
    
    def _get_inversion_status(self, current_value: float) -> str:
        """获取倒挂状态"""
        if current_value < -0.5:
            return "严重倒挂"
        elif current_value < 0:
            return "轻微倒挂"
        elif current_value < 0.5:
            return "接近倒挂"
        else:
            return "正常"
    
    def score(self, metrics: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """计算收益率曲线风险评分"""
        if not metrics:
            return 0
        
        current_value = metrics.get('current_value', 0)
        inversion_status = metrics.get('inversion_status', '正常')
        
        # 收益率曲线特殊评分逻辑（反向评分）
        if current_value <= -1.0:
            return 95  # 严重倒挂，极高风险
        elif current_value <= -0.5:
            return 85  # 倒挂，高风险
        elif current_value <= 0:
            return 70  # 轻微倒挂，中高风险
        elif current_value <= 0.5:
            return 50  # 接近倒挂，中等风险
        elif current_value <= 1.0:
            return 30  # 正常偏低，低风险
        elif current_value <= 2.0:
            return 15  # 正常，极低风险
        else:
            return 5   # 正常偏高，极低风险