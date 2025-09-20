"""
工具函数

提供通用的工具函数。
"""

import os
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """
    加载YAML配置文件
    
    Args:
        file_path: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"加载配置文件成功: {file_path}")
        return config
    except Exception as e:
        logger.error(f"加载配置文件失败 {file_path}: {e}")
        return {}


def ensure_dir_exists(dir_path: str) -> None:
    """
    确保目录存在
    
    Args:
        dir_path: 目录路径
    """
    try:
        os.makedirs(dir_path, exist_ok=True)
    except Exception as e:
        logger.error(f"创建目录失败 {dir_path}: {e}")


def get_env_var(key: str, default: str = None) -> Optional[str]:
    """
    获取环境变量
    
    Args:
        key: 环境变量名
        default: 默认值
        
    Returns:
        环境变量值
    """
    return os.getenv(key, default)


def format_number(value: float, decimals: int = 4) -> str:
    """
    格式化数字
    
    Args:
        value: 数值
        decimals: 小数位数
        
    Returns:
        格式化后的字符串
    """
    if value is None:
        return "N/A"
    
    try:
        return f"{value:.{decimals}f}"
    except (ValueError, TypeError):
        return str(value)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    安全除法
    
    Args:
        numerator: 分子
        denominator: 分母
        default: 默认值
        
    Returns:
        除法结果
    """
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ValueError):
        return default
