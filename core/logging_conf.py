"""
日志配置

配置系统日志。
"""

import os
import logging
from datetime import datetime
from typing import Optional


def setup_logging(base_dir: str, debug: bool = False) -> None:
    """
    设置日志配置
    
    Args:
        base_dir: 基础目录
        debug: 是否开启调试模式
    """
    # 创建日志目录
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # 设置日志级别
    level = logging.DEBUG if debug else logging.INFO
    
    # 配置根日志器
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # 控制台处理器
            logging.StreamHandler(),
            # 文件处理器
            logging.FileHandler(
                os.path.join(log_dir, f"macro_{datetime.now().strftime('%Y%m%d')}.log"),
                encoding='utf-8'
            )
        ]
    )
    
    # 设置第三方库日志级别
    logging.getLogger('fredapi').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('openpyxl').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"日志系统初始化完成，级别: {level}")


def get_logger(name: str) -> logging.Logger:
    """
    获取日志器
    
    Args:
        name: 日志器名称
        
    Returns:
        日志器实例
    """
    return logging.getLogger(name)
