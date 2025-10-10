"""
评分工具模块
"""

def risk_score(current_value: float, benchmark_value: float, 
               threshold_value: float, reverse: bool = False) -> float:
    """
    计算风险评分
    
    Args:
        current_value: 当前值
        benchmark_value: 基准值
        threshold_value: 阈值
        reverse: 是否反向评分
        
    Returns:
        风险评分 (0-100)
    """
    if reverse:
        # 反向评分：值越低风险越高
        if current_value <= threshold_value:
            return 90
        elif current_value <= benchmark_value:
            return 70
        else:
            return 30
    else:
        # 正向评分：值越高风险越高
        if current_value >= threshold_value:
            return 90
        elif current_value >= benchmark_value:
            return 70
        else:
            return 30


def get_risk_level(score: float, thresholds: dict) -> str:
    """
    根据评分获取风险等级
    
    Args:
        score: 风险评分
        thresholds: 阈值配置
        
    Returns:
        风险等级
    """
    low_threshold = thresholds.get('low', 30)
    medium_threshold = thresholds.get('medium', 60)
    high_threshold = thresholds.get('high', 80)
    
    if score >= high_threshold:
        return "极高风险"
    elif score >= medium_threshold:
        return "高风险"
    elif score >= low_threshold:
        return "中等风险"
    else:
        return "低风险"


def calculate_percentile_rank(values: list, current_value: float) -> float:
    """
    计算百分位排名
    
    Args:
        values: 历史值列表
        current_value: 当前值
        
    Returns:
        百分位排名 (0-100)
    """
    if not values:
        return 50
    
    sorted_values = sorted(values)
    rank = sum(1 for v in sorted_values if v <= current_value)
    return (rank / len(sorted_values)) * 100


def calculate_momentum_score(values: list, days: int = 5) -> float:
    """
    计算动量评分
    
    Args:
        values: 历史值列表
        days: 计算天数
        
    Returns:
        动量评分 (-100 到 100)
    """
    if len(values) < days + 1:
        return 0
    
    current_value = values[-1]
    past_value = values[-(days + 1)]
    
    if past_value == 0:
        return 0
    
    return ((current_value - past_value) / past_value) * 100