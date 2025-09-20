"""
因子注册表

自动发现和加载宏观因子模块。
"""

import os
import importlib
import yaml
from typing import Dict, List, Any, Type
import logging

from factors.base_factor import Factor

logger = logging.getLogger(__name__)


class FactorRegistry:
    """因子注册表"""
    
    def __init__(self, factors_dir: str, config_path: str):
        """
        初始化因子注册表
        
        Args:
            factors_dir: 因子模块目录
            config_path: 因子注册表配置文件路径
        """
        self.factors_dir = factors_dir
        self.config_path = config_path
        self.factors: Dict[str, Factor] = {}
        self._load_config()
        self._discover_factors()
    
    def _load_config(self) -> None:
        """加载因子注册表配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            logger.info(f"加载因子注册表配置: {self.config_path}")
        except Exception as e:
            logger.error(f"加载因子注册表配置失败: {e}")
            self.config = {"enabled": []}
    
    def _discover_factors(self) -> None:
        """发现并加载因子模块"""
        enabled_factors = self.config.get("enabled", [])
        factor_configs = self.config.get("factors", {})
        
        # 获取因子目录下的所有Python文件
        factor_files = []
        for file in os.listdir(self.factors_dir):
            if file.endswith('.py') and file not in ['__init__.py', 'base_factor.py']:
                factor_name = file[:-3]  # 去掉.py后缀
                if factor_name in enabled_factors:
                    factor_files.append(factor_name)
        
        logger.info(f"发现因子文件: {factor_files}")
        
        # 动态导入因子模块
        for factor_name in factor_files:
            try:
                # 构建模块路径
                module_path = f"factors.{factor_name}"
                
                # 导入模块
                module = importlib.import_module(module_path)
                
                # 查找因子类（通常是模块名的大写形式）
                factor_class_name = factor_name.upper()
                if hasattr(module, factor_class_name):
                    factor_class = getattr(module, factor_class_name)
                else:
                    # 尝试其他可能的类名
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if (isinstance(attr, type) and 
                            issubclass(attr, Factor) and 
                            attr != Factor):
                            factor_class = attr
                            break
                    else:
                        logger.warning(f"未找到因子类: {factor_name}")
                        continue
                
                # 获取因子配置
                factor_cfg = factor_configs.get(factor_name, {})
                
                # 实例化因子
                factor_instance = factor_class(factor_cfg)
                
                # 注册因子
                self.factors[factor_instance.id] = factor_instance
                logger.info(f"注册因子: {factor_instance.id} - {factor_instance.name}")
                
            except Exception as e:
                logger.error(f"加载因子失败 {factor_name}: {e}")
    
    def get_factor(self, factor_id: str) -> Factor:
        """
        获取指定因子
        
        Args:
            factor_id: 因子ID
            
        Returns:
            因子实例
        """
        return self.factors.get(factor_id)
    
    def get_all_factors(self) -> Dict[str, Factor]:
        """
        获取所有因子
        
        Returns:
            因子字典
        """
        return self.factors.copy()
    
    def list_factors(self) -> List[Dict[str, str]]:
        """
        列出所有因子信息
        
        Returns:
            因子信息列表
        """
        return [
            {
                "id": factor.id,
                "name": factor.name,
                "units": factor.units or "N/A"
            }
            for factor in self.factors.values()
        ]
