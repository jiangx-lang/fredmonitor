# src/scoring.py - 评分计算模块
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import yaml
from datetime import datetime, timedelta

def load_scoring_config(scoring_yaml_path: str) -> Dict[str, Any]:
    """加载评分配置"""
    with open(scoring_yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def calculate_quantile_score(value: float, benchmark: float, higher_is_risk: bool, 
                           anchors: List[List[float]]) -> float:
    """
    计算分位数评分
    
    Args:
        value: 当前值
        benchmark: 基准值
        higher_is_risk: 是否越高越危险
        anchors: 评分锚点 [[分位数, 分数], ...]
    
    Returns:
        评分 (0-100)
    """
    if np.isnan(value) or np.isnan(benchmark):
        return 50.0  # 默认中性分数
    
    # 计算偏离程度
    if higher_is_risk:
        deviation = value - benchmark
    else:
        deviation = benchmark - value  # 取反
    
    # 根据锚点计算分数
    score = 50.0  # 基准分数
    
    for quantile, anchor_score in anchors:
        if deviation >= quantile:
            score = anchor_score
            break
    
    # 确保分数在[0, 100]范围内
    return max(0.0, min(100.0, score))

def apply_staleness_penalty(score: float, last_update: str, 
                          staleness_config: Dict[str, Any]) -> float:
    """
    应用过期惩罚
    
    Args:
        score: 原始分数
        last_update: 最后更新时间
        staleness_config: 过期配置
    
    Returns:
        调整后的分数
    """
    if not last_update:
        return score * staleness_config.get('factor', 0.9)
    
    try:
        last_dt = pd.to_datetime(last_update)
        days_old = (datetime.now() - last_dt).days
        
        monthly_threshold = staleness_config.get('monthly_days', 60)
        quarterly_threshold = staleness_config.get('quarterly_days', 120)
        penalty_factor = staleness_config.get('factor', 0.9)
        
        # 根据数据频率应用不同的过期阈值
        if days_old > quarterly_threshold:
            return score * penalty_factor
        elif days_old > monthly_threshold:
            return score * (1.0 - (days_old - monthly_threshold) / (quarterly_threshold - monthly_threshold) * (1.0 - penalty_factor))
        
        return score
    except:
        return score * penalty_factor

def normalize_weights(indicators: List[Dict[str, Any]], 
                    group_weights: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    归一化权重
    
    Args:
        indicators: 指标列表
        group_weights: 分组权重
    
    Returns:
        归一化后的指标列表
    """
    # 计算总权重
    total_weight = sum(item.get('weight', 0) for item in indicators)
    
    if total_weight == 0:
        # 如果没有权重，按分组权重分配
        for item in indicators:
            group = item.get('group', 'other')
            item['weight'] = group_weights.get(group, 0.1) / len([i for i in indicators if i.get('group') == group])
    else:
        # 归一化到1.0
        for item in indicators:
            item['weight'] = item.get('weight', 0) / total_weight
    
    return indicators

def calculate_group_scores(indicators: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    计算分组评分
    
    Args:
        indicators: 指标列表
    
    Returns:
        分组评分字典
    """
    group_scores = {}
    
    for item in indicators:
        group = item.get('group', 'other')
        score = item.get('score', 50.0)
        weight = item.get('weight', 0.0)
        
        if group not in group_scores:
            group_scores[group] = {
                'total_score': 0.0,
                'total_weight': 0.0,
                'indicator_count': 0,
                'indicators': []
            }
        
        group_scores[group]['total_score'] += score * weight
        group_scores[group]['total_weight'] += weight
        group_scores[group]['indicator_count'] += 1
        group_scores[group]['indicators'].append({
            'id': item.get('id'),
            'name': item.get('name'),
            'score': score,
            'weight': weight
        })
    
    # 计算加权平均分数
    for group, data in group_scores.items():
        if data['total_weight'] > 0:
            data['weighted_score'] = data['total_score'] / data['total_weight']
        else:
            data['weighted_score'] = 50.0
    
    return group_scores

def calculate_total_score(group_scores: Dict[str, Dict[str, float]], 
                        group_weights: Dict[str, float]) -> float:
    """
    计算总分
    
    Args:
        group_scores: 分组评分
        group_weights: 分组权重
    
    Returns:
        总分
    """
    total_score = 0.0
    total_weight = 0.0
    
    for group, data in group_scores.items():
        group_weight = group_weights.get(group, 0.0)
        weighted_score = data.get('weighted_score', 50.0)
        
        total_score += weighted_score * group_weight
        total_weight += group_weight
    
    if total_weight > 0:
        return total_score / total_weight
    else:
        return 50.0

def get_risk_level(score: float, thresholds: Dict[str, float]) -> str:
    """
    获取风险等级
    
    Args:
        score: 总分
        thresholds: 阈值配置
    
    Returns:
        风险等级
    """
    if score <= thresholds.get('low', 40):
        return 'low'
    elif score <= thresholds.get('mid', 60):
        return 'medium'
    elif score <= thresholds.get('high', 80):
        return 'high'
    else:
        return 'critical'

def recompute_scores(config_yaml: str, scoring_yaml: str, 
                    crisis_yaml: str, figures_dir: str, 
                    raw_json_path: str) -> Dict[str, Any]:
    """
    重新计算所有评分
    
    Args:
        config_yaml: 指标配置文件
        scoring_yaml: 评分配置文件
        crisis_yaml: 危机期间配置文件
        figures_dir: 图表目录
        raw_json_path: 原始JSON文件路径
    
    Returns:
        重新计算后的JSON数据
    """
    # 加载配置
    with open(config_yaml, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    scoring_config = load_scoring_config(scoring_yaml)
    
    # 加载原始数据
    import json
    with open(raw_json_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    # 归一化权重
    indicators = normalize_weights(config['indicators'], scoring_config.get('group_weights', {}))
    
    # 计算评分
    for item in indicators:
        series_id = item['id']
        
        # 查找对应的数据
        series_data = None
        for raw_item in raw_data.get('indicators', []):
            if raw_item.get('series_id') == series_id:
                series_data = raw_item
                break
        
        if series_data:
            # 计算评分
            current_value = series_data.get('current_value', 0)
            benchmark = series_data.get('benchmark', 0)
            higher_is_risk = item.get('higher_is_risk', True)
            
            anchors = scoring_config.get('quantile_tail', {}).get('anchors', [])
            score = calculate_quantile_score(current_value, benchmark, higher_is_risk, anchors)
            
            # 应用过期惩罚
            last_update = series_data.get('last_update')
            staleness_config = scoring_config.get('staleness_penalty', {})
            score = apply_staleness_penalty(score, last_update, staleness_config)
            
            item['score'] = score
            item['current_value'] = current_value
            item['benchmark'] = benchmark
        else:
            item['score'] = 50.0  # 默认分数
            item['current_value'] = 0
            item['benchmark'] = 0
    
    # 计算分组评分
    group_scores = calculate_group_scores(indicators)
    
    # 计算总分
    total_score = calculate_total_score(group_scores, scoring_config.get('group_weights', {}))
    
    # 获取风险等级
    risk_level = get_risk_level(total_score, scoring_config.get('thresholds', {}))
    
    # 构建输出数据
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_indicators': len(indicators),
        'total_score': total_score,
        'risk_level': risk_level,
        'group_scores': group_scores,
        'indicators': indicators,
        'config': {
            'scoring_method': scoring_config.get('method', 'quantile_tail'),
            'group_weights': scoring_config.get('group_weights', {}),
            'thresholds': scoring_config.get('thresholds', {})
        }
    }
    
    return output_data
