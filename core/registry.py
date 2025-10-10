"""
因子注册表管理
"""

import os
import yaml
import importlib
import inspect
from typing import List, Dict, Any, Optional
from pathlib import Path


class FactorRegistry:
    """因子注册表管理类"""
    
    def __init__(self, factors_dir: str, config_path: str):
        """
        初始化因子注册表
        
        Args:
            factors_dir: 因子定义文件目录
            config_path: 因子注册表配置文件路径
        """
        self.factors_dir = Path(factors_dir)
        self.config_path = Path(config_path)
        self.factors_config = {}
        self.factor_classes = {}
        
        # 加载配置
        self._load_config()
        
        # 加载因子类
        self._load_factor_classes()
    
    def _load_config(self):
        """加载因子配置文件"""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.factors_config = yaml.safe_load(f)
            else:
                print(f"⚠️ 因子配置文件不存在: {self.config_path}")
                self.factors_config = {'factors': []}
        except Exception as e:
            print(f"❌ 加载因子配置失败: {e}")
            self.factors_config = {'factors': []}
    
    def _load_factor_classes(self):
        """动态加载因子类"""
        if not self.factors_dir.exists():
            print(f"⚠️ 因子目录不存在: {self.factors_dir}")
            return
        
        # 遍历因子目录，加载所有Python文件
        for py_file in self.factors_dir.glob("*.py"):
            if py_file.name.startswith("__"):
                continue
            
            try:
                # 动态导入模块
                module_name = f"factors.{py_file.stem}"
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 查找因子类
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        (hasattr(obj, 'id') or name.endswith('Factor'))):
                        
                        # 注册因子类
                        factor_id = getattr(obj, 'id', name)
                        self.factor_classes[factor_id] = obj
                        print(f"加载因子类: {factor_id} -> {name}")
                
            except Exception as e:
                print(f"加载因子文件失败 {py_file.name}: {e}")
    
    def list_factors(self) -> List[Dict[str, Any]]:
        """列出所有因子配置"""
        return self.factors_config.get('factors', [])
    
    def get_factor(self, factor_id: str) -> Optional[Any]:
        """获取因子实例"""
        # 查找因子配置
        factor_config = None
        for config in self.factors_config.get('factors', []):
            if config.get('id') == factor_id:
                factor_config = config
                break
        
        if not factor_config:
            print(f"❌ 未找到因子配置: {factor_id}")
            return None
        
        # 查找因子类
        factor_class = self.factor_classes.get(factor_id)
        
        # 如果直接匹配失败，尝试通过配置中的class字段匹配
        if not factor_class and 'class' in factor_config:
            class_name = factor_config['class']
            factor_class = self.factor_classes.get(class_name)
        
        if not factor_class:
            print(f"未找到因子类: {factor_id}")
            return None
        
        try:
            # 创建因子实例
            factor_instance = factor_class(factor_config)
            return factor_instance
        except Exception as e:
            print(f"❌ 创建因子实例失败 {factor_id}: {e}")
            return None
    
    def get_all_factors(self) -> List[Any]:
        """获取所有因子实例"""
        factors = []
        for config in self.factors_config.get('factors', []):
            factor_id = config.get('id')
            if factor_id:
                factor = self.get_factor(factor_id)
                if factor:
                    factors.append(factor)
        return factors
    
    def get_factor_by_group(self, group: str) -> List[Any]:
        """按组获取因子"""
        factors = []
        for config in self.factors_config.get('factors', []):
            if config.get('group') == group:
                factor_id = config.get('id')
                if factor_id:
                    factor = self.get_factor(factor_id)
                    if factor:
                        factors.append(factor)
        return factors
    
    def get_groups(self) -> List[str]:
        """获取所有因子组"""
        groups = set()
        for config in self.factors_config.get('factors', []):
            group = config.get('group')
            if group:
                groups.add(group)
        return list(groups)
    
    def reload(self):
        """重新加载配置和因子类"""
        self._load_config()
        self._load_factor_classes()
        print("✅ 因子注册表重新加载完成")
    
    def get_factor_info(self, factor_id: str) -> Optional[Dict[str, Any]]:
        """获取因子信息"""
        for config in self.factors_config.get('factors', []):
            if config.get('id') == factor_id:
                return config
        return None
    
    def validate_factors(self) -> Dict[str, List[str]]:
        """验证因子配置"""
        errors = {
            'missing_classes': [],
            'missing_configs': [],
            'invalid_configs': []
        }
        
        # 检查配置中的因子是否有对应的类
        for config in self.factors_config.get('factors', []):
            factor_id = config.get('id')
            if factor_id and factor_id not in self.factor_classes:
                errors['missing_classes'].append(factor_id)
        
        # 检查类是否有对应的配置
        for factor_id in self.factor_classes.keys():
            found = False
            for config in self.factors_config.get('factors', []):
                if config.get('id') == factor_id:
                    found = True
                    break
            if not found:
                errors['missing_configs'].append(factor_id)
        
        # 检查配置有效性
        for config in self.factors_config.get('factors', []):
            if not config.get('id'):
                errors['invalid_configs'].append("缺少id字段")
            if not config.get('name'):
                errors['invalid_configs'].append(f"{config.get('id', 'unknown')}: 缺少name字段")
            if not config.get('series_id'):
                errors['invalid_configs'].append(f"{config.get('id', 'unknown')}: 缺少series_id字段")
        
        return errors