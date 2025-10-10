"""
工具模块
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    加载YAML配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            print(f"⚠️ 配置文件不存在: {config_path}")
            return {}
    except Exception as e:
        print(f"❌ 加载配置文件失败 {config_path}: {e}")
        return {}


def save_yaml_config(config: Dict[str, Any], config_path: str):
    """
    保存YAML配置文件
    
    Args:
        config: 配置字典
        config_path: 配置文件路径
    """
    try:
        config_file = Path(config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        
        print(f"✅ 配置文件已保存: {config_path}")
    except Exception as e:
        print(f"❌ 保存配置文件失败 {config_path}: {e}")


def ensure_directory(path: str):
    """
    确保目录存在
    
    Args:
        path: 目录路径
    """
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"❌ 创建目录失败 {path}: {e}")


def get_project_root() -> Path:
    """
    获取项目根目录
    
    Returns:
        项目根目录路径
    """
    return Path(__file__).parent.parent


def format_number(value: float, decimals: int = 2) -> str:
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
    except:
        return str(value)


def format_percentage(value: float, decimals: int = 1) -> str:
    """
    格式化百分比
    
    Args:
        value: 数值
        decimals: 小数位数
        
    Returns:
        格式化后的百分比字符串
    """
    if value is None:
        return "N/A"
    
    try:
        return f"{value:.{decimals}f}%"
    except:
        return str(value)