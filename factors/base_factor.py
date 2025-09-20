"""
宏观因子基类

定义了所有宏观因子必须实现的接口。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd


class Factor(ABC):
    """宏观因子基类"""
    
    id: str = ""           # 因子ID（如 "vix"）
    name: str = ""         # 因子名称（如 "VIX 波动率"）
    units: Optional[str] = None  # 单位

    def __init__(self, cfg: Dict[str, Any]):
        """
        初始化因子
        
        Args:
            cfg: 因子配置参数
        """
        self.cfg = cfg or {}

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """
        获取原始数据
        
        Returns:
            包含 'date' 和 'value' 列的DataFrame，date为pandas datetime类型
        """
        pass

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算派生指标
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            包含以下键的字典：
            - original_value: 原始值（float或None）
            - metrics: 其他派生指标字典
        """
        pass

    @abstractmethod
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """
        计算风险评分
        
        Args:
            metrics: 计算得到的指标
            global_cfg: 全局配置
            
        Returns:
            0-100的风险评分
        """
        pass

    def to_frame(self, date: pd.Timestamp, metrics: Dict[str, Any], score: float) -> pd.DataFrame:
        """
        将因子结果转换为标准DataFrame格式
        
        Args:
            date: 日期
            metrics: 指标字典
            score: 风险评分
            
        Returns:
            标准格式的DataFrame
        """
        data = {
            "date": pd.to_datetime(date),
            "factor_id": self.id,
            "factor_name": self.name,
            "original_value": metrics.get("original_value"),
            "score": score,
        }
        
        # 添加其他指标（排除original_value）
        for key, value in metrics.items():
            if key != "original_value":
                data[f"m_{key}"] = value
                
        return pd.DataFrame([data])
