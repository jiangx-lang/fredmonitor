"""
因子模板

复制此文件并修改以创建新的宏观因子。
"""

import pandas as pd
from typing import Dict, Any
import logging

from .base_factor import Factor

logger = logging.getLogger(__name__)


class TEMPLATE_FACTOR(Factor):
    """模板因子 - 请修改类名和实现"""
    
    # 修改这些属性
    id = "TEMPLATE"           # 因子ID（用于配置和识别）
    name = "模板因子"         # 因子显示名称
    units = "单位"            # 因子单位（可选）
    
    def __init__(self, cfg: Dict[str, Any]):
        """
        初始化因子
        
        Args:
            cfg: 因子配置参数，从config/factor_registry.yaml中获取
        """
        super().__init__(cfg)
        
        # 从配置中获取参数
        self.series_id = cfg.get("series_id", "DEFAULT_SERIES")
        self.param1 = cfg.get("param1", "default_value")
        self.param2 = cfg.get("param2", 10)
        
        logger.info(f"初始化因子 {self.id}: {self.name}")
    
    def fetch(self) -> pd.DataFrame:
        """
        获取原始数据
        
        实现数据获取逻辑，通常从FRED API获取数据。
        在聚合器中，FRED客户端会被注入，这里只需要返回空DataFrame。
        
        Returns:
            包含 'date' 和 'value' 列的DataFrame
        """
        try:
            logger.info(f"获取{self.name}数据: {self.series_id}")
            
            # 注意：实际的数据获取在聚合器中通过FRED客户端完成
            # 这里只返回空DataFrame作为占位符
            return pd.DataFrame(columns=['date', 'value'])
            
        except Exception as e:
            logger.error(f"获取{self.name}数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算派生指标
        
        实现指标计算逻辑，如移动平均、年化变化率等。
        
        Args:
            df: 原始数据DataFrame，包含'date'和'value'列
            
        Returns:
            包含以下键的字典：
            - original_value: 原始值（必需）
            - 其他派生指标（可选）
        """
        if df.empty:
            logger.warning(f"{self.name}数据为空")
            return {"original_value": None}
        
        try:
            # 获取最新值
            latest_value = df['value'].iloc[-1]
            
            # 计算派生指标（示例）
            if len(df) >= self.param2:
                # 计算移动平均
                moving_avg = df['value'].tail(self.param2).mean()
                
                # 计算年化变化率
                if len(df) >= 12:
                    yoy_change = (df['value'].iloc[-1] / df['value'].iloc[-12] - 1) * 100
                else:
                    yoy_change = None
            else:
                moving_avg = None
                yoy_change = None
            
            return {
                "original_value": float(latest_value) if latest_value is not None else None,
                "moving_avg": float(moving_avg) if moving_avg is not None else None,
                "yoy_change": float(yoy_change) if yoy_change is not None else None
            }
            
        except Exception as e:
            logger.error(f"计算{self.name}指标失败: {e}")
            return {"original_value": None}
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """
        计算风险评分
        
        实现风险评分逻辑，基于指标值和配置的评分区间。
        
        Args:
            metrics: 计算得到的指标
            global_cfg: 全局配置，包含评分区间等
            
        Returns:
            0-100的风险评分
        """
        from core.scoring import calculate_factor_score
        
        # 使用原始值进行评分
        value = metrics.get("original_value")
        
        # 调用通用评分函数
        score = calculate_factor_score(self.id, value, global_cfg)
        
        logger.debug(f"{self.name}评分: {score:.2f} (值: {value})")
        
        return score


# 使用说明：
# 1. 复制此文件并重命名（如 my_factor.py）
# 2. 修改类名（如 MyFactor）
# 3. 修改 id, name, units 属性
# 4. 在 __init__ 中添加特定参数
# 5. 实现 fetch, compute, score 方法
# 6. 在 config/factor_registry.yaml 中注册新因子
# 7. 在 config/settings.yaml 中添加评分区间和权重配置
