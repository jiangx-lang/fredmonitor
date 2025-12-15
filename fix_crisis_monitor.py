#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复crisis_monitor.py中的所有问题
"""

import os
import sys
import re
from pathlib import Path

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def fix_crisis_monitor():
    """修复crisis_monitor.py中的所有问题"""
    print("🔧 开始修复crisis_monitor.py中的所有问题")
    print("=" * 80)
    
    crisis_monitor_file = Path("crisis_monitor.py")
    if not crisis_monitor_file.exists():
        print("❌ crisis_monitor.py 文件不存在")
        return
    
    # 读取文件内容
    with open(crisis_monitor_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    fixes_applied = []
    
    # 1. 修复除零风险
    print("🔧 1. 修复除零风险...")
    content, fixes = fix_zero_division_risks(content)
    fixes_applied.extend(fixes)
    
    # 2. 完善异常处理
    print("🔧 2. 完善异常处理...")
    content, fixes = fix_exception_handling(content)
    fixes_applied.extend(fixes)
    
    # 3. 修复数据类型转换
    print("🔧 3. 修复数据类型转换...")
    content, fixes = fix_data_type_conversion(content)
    fixes_applied.extend(fixes)
    
    # 4. 修复硬编码路径
    print("🔧 4. 修复硬编码路径...")
    content, fixes = fix_hardcoded_paths(content)
    fixes_applied.extend(fixes)
    
    # 5. 修复字符串比较
    print("🔧 5. 修复字符串比较...")
    content, fixes = fix_string_comparison(content)
    fixes_applied.extend(fixes)
    
    # 6. 修复函数命名
    print("🔧 6. 修复函数命名...")
    content, fixes = fix_function_naming(content)
    fixes_applied.extend(fixes)
    
    # 7. 添加性能优化
    print("🔧 7. 添加性能优化...")
    content, fixes = add_performance_optimizations(content)
    fixes_applied.extend(fixes)
    
    # 8. 添加配置管理
    print("🔧 8. 添加配置管理...")
    content, fixes = add_configuration_management(content)
    fixes_applied.extend(fixes)
    
    # 保存修复后的文件
    if content != original_content:
        backup_file = crisis_monitor_file.with_suffix('.py.backup')
        with open(backup_file, 'w', encoding='utf-8') as f:
            f.write(original_content)
        print(f"📁 备份原文件: {backup_file}")
        
        with open(crisis_monitor_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ 修复完成，保存到: {crisis_monitor_file}")
        
        print(f"\n📋 修复总结:")
        print(f"总共应用了 {len(fixes_applied)} 个修复:")
        for i, fix in enumerate(fixes_applied, 1):
            print(f"{i:2d}. {fix}")
    else:
        print("✅ 未发现需要修复的问题")

def fix_zero_division_risks(content):
    """修复除零风险"""
    fixes = []
    
    # 修复除法运算
    patterns = [
        # 简单的除法运算
        (r'(\w+)\s*/\s*(\w+)', r'safe_divide(\1, \2)'),
        # 带括号的除法
        (r'\(([^)]+)\)\s*/\s*(\w+)', r'safe_divide(\1, \2)'),
        # 复杂的除法表达式
        (r'(\w+)\s*/\s*(\w+)\s*\*\s*100', r'safe_divide(\1, \2) * 100'),
    ]
    
    for pattern, replacement in patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复除零风险: {pattern}")
    
    # 添加安全除法函数
    safe_divide_func = '''
def safe_divide(numerator, denominator, default_value=0.0):
    """安全的除法运算，避免除零错误"""
    try:
        if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
            return default_value
        return numerator / denominator
    except (TypeError, ValueError, ZeroDivisionError):
        return default_value

'''
    
    # 在导入部分后添加安全除法函数
    import_end_pattern = r'(from dotenv import load_dotenv\n)'
    if re.search(import_end_pattern, content):
        content = re.sub(import_end_pattern, r'\1' + safe_divide_func, content)
        fixes.append("添加安全除法函数")
    
    return content, fixes

def fix_exception_handling(content):
    """完善异常处理"""
    fixes = []
    
    # 修复裸露的except语句
    bare_except_pattern = r'except:\s*\n'
    if re.search(bare_except_pattern, content):
        content = re.sub(bare_except_pattern, 'except Exception as e:\n', content)
        fixes.append("修复裸露的except语句")
    
    # 为数据类型转换添加异常处理
    float_pattern = r'float\(([^)]+)\)'
    if re.search(float_pattern, content):
        content = re.sub(float_pattern, r'safe_float(\1)', content)
        fixes.append("修复float()转换")
    
    int_pattern = r'int\(([^)]+)\)'
    if re.search(int_pattern, content):
        content = re.sub(int_pattern, r'safe_int(\1)', content)
        fixes.append("修复int()转换")
    
    # 添加安全转换函数
    safe_conversion_funcs = '''
def safe_float(value, default_value=0.0):
    """安全的浮点数转换"""
    try:
        if pd.isna(value) or value is None:
            return default_value
        return float(value)
    except (TypeError, ValueError):
        return default_value

def safe_int(value, default_value=0):
    """安全的整数转换"""
    try:
        if pd.isna(value) or value is None:
            return default_value
        return int(float(value))  # 先转float再转int
    except (TypeError, ValueError):
        return default_value

'''
    
    # 在安全除法函数后添加转换函数
    if 'def safe_divide(' in content:
        content = content.replace('def safe_divide(', safe_conversion_funcs + 'def safe_divide(')
        fixes.append("添加安全转换函数")
    
    return content, fixes

def fix_data_type_conversion(content):
    """修复数据类型转换"""
    fixes = []
    
    # 修复print语句中的类型转换
    print_pattern = r'print\(f"([^"]*\{[^}]*\}[^"]*)"\)'
    if re.search(print_pattern, content):
        # 为print语句添加异常处理
        content = re.sub(
            print_pattern,
            lambda m: f'try:\n    print(f"{m.group(1)}")\nexcept Exception as e:\n    print(f"输出错误: {{e}}")',
            content
        )
        fixes.append("修复print语句类型转换")
    
    return content, fixes

def fix_hardcoded_paths(content):
    """修复硬编码路径"""
    fixes = []
    
    # 添加路径配置
    path_config = '''
# 路径配置
DATA_PATHS = {
    'series': 'data/series',
    'fred': 'data/fred/series',
    'outputs': 'outputs/crisis_monitor',
    'config': 'config',
    'scripts': 'scripts'
}

def get_data_path(path_type, filename=None):
    """获取数据路径"""
    base_path = DATA_PATHS.get(path_type, 'data')
    if filename:
        return Path(base_path) / filename
    return Path(base_path)

'''
    
    # 在BASE定义后添加路径配置
    base_pattern = r'(BASE = pathlib\.Path\(__file__\)\.parent\n)'
    if re.search(base_pattern, content):
        content = re.sub(base_pattern, r'\1' + path_config, content)
        fixes.append("添加路径配置")
    
    # 替换硬编码路径
    hardcoded_patterns = [
        (r'"data/series/([^"]+)"', r'str(get_data_path("series", r"\1"))'),
        (r'"data/fred/series/([^"]+)"', r'str(get_data_path("fred", r"\1"))'),
        (r'"outputs/crisis_monitor"', r'str(get_data_path("outputs"))'),
        (r'"config/([^"]+)"', r'str(get_data_path("config", r"\1"))'),
        (r'"scripts/([^"]+)"', r'str(get_data_path("scripts", r"\1"))'),
    ]
    
    for pattern, replacement in hardcoded_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"替换硬编码路径: {pattern}")
    
    return content, fixes

def fix_string_comparison(content):
    """修复字符串比较"""
    fixes = []
    
    # 修复字符串比较，添加大小写处理
    string_comparison_patterns = [
        (r'if\s+(\w+)\s*==\s*"([^"]+)"', r'if \1.lower() == "\2".lower()'),
        (r'if\s+(\w+)\s*==\s*\'([^\']+)\'', r'if \1.lower() == "\2".lower()'),
        (r'(\w+)\s*==\s*"([^"]+)"', r'\1.lower() == "\2".lower()'),
        (r'(\w+)\s*==\s*\'([^\']+)\'', r'\1.lower() == "\2".lower()'),
    ]
    
    for pattern, replacement in string_comparison_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复字符串比较: {pattern}")
    
    return content, fixes

def fix_function_naming(content):
    """修复函数命名"""
    fixes = []
    
    # 修复私有函数命名
    private_function_patterns = [
        (r'def _md2html\(', 'def md2html('),
        (r'def _month_end_code\(', 'def month_end_code('),
        (r'def _as_float_series\(', 'def as_float_series('),
        (r'def _read_png_as_base64\(', 'def read_png_as_base64('),
        (r'def _mask_by_crisis\(', 'def mask_by_crisis('),
        (r'def _parse_compare_to_to_pct\(', 'def parse_compare_to_to_pct('),
    ]
    
    for pattern, replacement in private_function_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复私有函数命名: {pattern}")
    
    # 修复函数调用
    function_call_patterns = [
        (r'_md2html\(', 'md2html('),
        (r'_month_end_code\(', 'month_end_code('),
        (r'_as_float_series\(', 'as_float_series('),
        (r'_read_png_as_base64\(', 'read_png_as_base64('),
        (r'_mask_by_crisis\(', 'mask_by_crisis('),
        (r'_parse_compare_to_to_pct\(', 'parse_compare_to_to_pct('),
    ]
    
    for pattern, replacement in function_call_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复函数调用: {pattern}")
    
    return content, fixes

def add_performance_optimizations(content):
    """添加性能优化"""
    fixes = []
    
    # 添加缓存装饰器
    cache_decorator = '''
from functools import lru_cache
import hashlib

@lru_cache(maxsize=128)
def cached_read_csv(file_path, **kwargs):
    """缓存的CSV读取"""
    return pd.read_csv(file_path, **kwargs)

def get_file_hash(file_path):
    """获取文件哈希值，用于缓存失效检测"""
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None

'''
    
    # 在导入部分添加缓存
    if 'from functools import' not in content:
        import_pattern = r'(import pandas as pd\n)'
        if re.search(import_pattern, content):
            content = re.sub(import_pattern, r'\1' + cache_decorator, content)
            fixes.append("添加缓存装饰器")
    
    # 替换pd.read_csv调用
    read_csv_pattern = r'pd\.read_csv\(([^)]+)\)'
    if re.search(read_csv_pattern, content):
        content = re.sub(read_csv_pattern, r'cached_read_csv(\1)', content)
        fixes.append("添加CSV读取缓存")
    
    return content, fixes

def add_configuration_management(content):
    """添加配置管理"""
    fixes = []
    
    # 添加配置管理类
    config_manager = '''
class ConfigManager:
    """配置管理器"""
    
    def __init__(self):
        self.config = {}
        self.load_config()
    
    def load_config(self):
        """加载配置"""
        try:
            config_file = get_data_path("config", "app_config.yaml")
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"配置加载失败: {e}")
            self.config = {}
    
    def get(self, key, default=None):
        """获取配置值"""
        return self.config.get(key, default)
    
    def get_path(self, path_type, filename=None):
        """获取路径配置"""
        paths = self.config.get('paths', {})
        base_path = paths.get(path_type, DATA_PATHS.get(path_type, 'data'))
        if filename:
            return Path(base_path) / filename
        return Path(base_path)

# 全局配置管理器
config_manager = ConfigManager()

'''
    
    # 在路径配置后添加配置管理器
    if 'DATA_PATHS = {' in content:
        content = content.replace('DATA_PATHS = {', config_manager + 'DATA_PATHS = {')
        fixes.append("添加配置管理器")
    
    return content, fixes

def main():
    """主函数"""
    print("crisis_monitor.py 修复工具")
    print("=" * 80)
    
    try:
        fix_crisis_monitor()
        print("\n✅ 修复完成!")
        
    except Exception as e:
        print(f"\n❌ 修复过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
