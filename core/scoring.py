"""
评分算法

实现风险评分计算逻辑。
"""

from typing import List, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)


def risk_score(value: Union[float, None], low: float, high: float, 
               reverse: bool = False) -> float:
    """
    计算风险评分
    
    Args:
        value: 输入值
        low: 低风险阈值
        high: 高风险阈值
        reverse: 是否反向评分（值越低风险越高）
        
    Returns:
        0-100的风险评分
    """
    if value is None:
        logger.warning("输入值为None，返回评分0")
        return 0.0
    
    if reverse:
        # 反向评分：值越低风险越高
        if value >= high:
            return 0.0
        elif value <= low:
            return 100.0
        else:
            # 线性插值
            return 100.0 * (high - value) / (high - low)
    else:
        # 正向评分：值越高风险越高
        if value <= low:
            return 0.0
        elif value >= high:
            return 100.0
        else:
            # 线性插值
            return 100.0 * (value - low) / (high - low)


def get_factor_bands(factor_id: str, global_cfg: Dict[str, Any]) -> List[Union[float, str]]:
    """
    获取因子评分区间配置
    
    Args:
        factor_id: 因子ID
        global_cfg: 全局配置
        
    Returns:
        评分区间配置 [low, high] 或 [low, high, "reverse"]
    """
    bands = global_cfg.get("bands", {})
    factor_bands = bands.get(factor_id, [0, 100])
    
    if len(factor_bands) < 2:
        logger.warning(f"因子 {factor_id} 评分区间配置不完整，使用默认值")
        return [0, 100]
    
    return factor_bands


def calculate_factor_score(factor_id: str, value: Union[float, None], 
                          global_cfg: Dict[str, Any]) -> float:
    """
    计算因子风险评分
    
    Args:
        factor_id: 因子ID
        value: 因子值
        global_cfg: 全局配置
        
    Returns:
        风险评分
    """
    bands = get_factor_bands(factor_id, global_cfg)
    
    if len(bands) == 2:
        low, high = bands
        reverse = False
    elif len(bands) == 3:
        low, high, reverse_flag = bands
        reverse = (reverse_flag == "reverse")
    else:
        logger.warning(f"因子 {factor_id} 评分区间配置格式错误，使用默认值")
        low, high = 0, 100
        reverse = False
    
    return risk_score(value, low, high, reverse)


def get_risk_level(score: float, thresholds: Dict[str, float]) -> str:
    """
    根据评分获取风险等级
    
    Args:
        score: 风险评分
        thresholds: 风险等级阈值配置
        
    Returns:
        风险等级描述
    """
    if score < thresholds.get("low", 30):
        return "低风险"
    elif score < thresholds.get("medium", 50):
        return "中等风险"
    elif score < thresholds.get("high", 70):
        return "偏高风险"
    else:
        return "极高风险"
