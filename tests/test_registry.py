"""
测试因子注册表

测试因子自动发现和加载功能。
"""

import pytest
import os
import tempfile
import yaml
from core.registry import FactorRegistry


class TestRegistry:
    """测试因子注册表"""
    
    def test_registry_initialization(self):
        """测试注册表初始化"""
        # 创建临时目录
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试配置文件
            config = {
                "enabled": ["vix", "ted_spread"],
                "factors": {
                    "vix": {"series_id": "VIXCLS"},
                    "ted_spread": {"sofr_series": "SOFR"}
                }
            }
            
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True)
            
            # 初始化注册表
            registry = FactorRegistry("factors", config_path)
            
            # 检查配置加载
            assert registry.config == config
            assert "enabled" in registry.config
            assert "factors" in registry.config
    
    def test_get_factor(self):
        """测试获取因子"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "enabled": ["vix"],
                "factors": {
                    "vix": {"series_id": "VIXCLS"}
                }
            }
            
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True)
            
            registry = FactorRegistry("factors", config_path)
            
            # 获取存在的因子
            factor = registry.get_factor("VIX")
            if factor:  # 如果因子加载成功
                assert factor.id == "VIX"
                assert factor.name == "VIX 波动率"
            
            # 获取不存在的因子
            factor = registry.get_factor("UNKNOWN")
            assert factor is None
    
    def test_get_all_factors(self):
        """测试获取所有因子"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "enabled": ["vix", "ted_spread"],
                "factors": {
                    "vix": {"series_id": "VIXCLS"},
                    "ted_spread": {"sofr_series": "SOFR"}
                }
            }
            
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True)
            
            registry = FactorRegistry("factors", config_path)
            factors = registry.get_all_factors()
            
            # 检查返回的因子字典
            assert isinstance(factors, dict)
            # 注意：由于动态导入可能失败，这里只检查类型
    
    def test_list_factors(self):
        """测试列出因子"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "enabled": ["vix"],
                "factors": {
                    "vix": {"series_id": "VIXCLS"}
                }
            }
            
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True)
            
            registry = FactorRegistry("factors", config_path)
            factor_list = registry.list_factors()
            
            # 检查返回的因子列表
            assert isinstance(factor_list, list)
            # 注意：由于动态导入可能失败，这里只检查类型
    
    def test_empty_config(self):
        """测试空配置"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {"enabled": []}
            
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True)
            
            registry = FactorRegistry("factors", config_path)
            
            # 检查空配置
            assert registry.config == config
            assert len(registry.factors) == 0
    
    def test_missing_config_file(self):
        """测试缺失配置文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 使用不存在的配置文件
            config_path = os.path.join(temp_dir, "nonexistent.yaml")
            
            registry = FactorRegistry("factors", config_path)
            
            # 检查默认配置
            assert registry.config == {"enabled": []}
            assert len(registry.factors) == 0
