#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的因子系统测试脚本（无emoji字符）
"""

import os
import sys
import traceback
from pathlib import Path

# 添加项目根目录到Python路径
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

def test_factor_registry():
    """测试因子注册表"""
    try:
        from core.registry import FactorRegistry
        
        factors_dir = BASE_DIR / "factors"
        config_path = BASE_DIR / "config" / "factor_registry.yaml"
        registry = FactorRegistry(str(factors_dir), str(config_path))
        factors = registry.list_factors()
        
        print(f"找到 {len(factors)} 个因子配置")
        
        # 测试加载特定因子
        if factors:
            first_factor = factors[0]
            print(f"测试加载因子: {first_factor['id']}")
            
            factor_instance = registry.get_factor(first_factor['id'])
            if factor_instance:
                print(f"成功加载因子: {factor_instance.name}")
                return registry
            else:
                print("因子加载失败")
                return None
        else:
            print("没有找到任何因子配置")
            return None
            
    except Exception as e:
        print(f"因子注册表加载失败: {e}")
        traceback.print_exc()
        return None

def test_basic_functionality():
    """测试基本功能"""
    try:
        # 测试配置文件加载
        from core.utils import load_yaml_config
        
        config_path = BASE_DIR / "config" / "factor_registry.yaml"
        if config_path.exists():
            config = load_yaml_config(str(config_path))
            print(f"成功加载配置文件: {len(config.get('factors', []))} 个因子")
        else:
            print("配置文件不存在")
            return False
            
        # 测试因子基类
        from factors.base_factor import BaseFactor, Factor
        print("因子基类导入成功")
        
        return True
        
    except Exception as e:
        print(f"基本功能测试失败: {e}")
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    print("开始因子系统测试")
    print("=" * 60)
    
    # 测试基本功能
    if not test_basic_functionality():
        print("基本功能测试失败，退出")
        return
    
    # 测试因子注册表
    registry = test_factor_registry()
    
    if registry:
        print("因子系统测试通过")
    else:
        print("因子系统测试失败")
    
    print("\n" + "=" * 60)
    print("因子系统测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
