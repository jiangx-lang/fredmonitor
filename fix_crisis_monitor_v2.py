#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复crisis_monitor.py中的所有问题 - 版本2
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

def fix_crisis_monitor_v2():
    """修复crisis_monitor.py中的所有问题 - 版本2"""
    print("🔧 开始修复crisis_monitor.py中的所有问题 (版本2)")
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
    
    # 1. 添加安全函数
    print("🔧 1. 添加安全函数...")
    content, fixes = add_safe_functions(content)
    fixes_applied.extend(fixes)
    
    # 2. 修复除零风险
    print("🔧 2. 修复除零风险...")
    content, fixes = fix_zero_division_v2(content)
    fixes_applied.extend(fixes)
    
    # 3. 修复异常处理
    print("🔧 3. 修复异常处理...")
    content, fixes = fix_exception_handling_v2(content)
    fixes_applied.extend(fixes)
    
    # 4. 修复数据类型转换
    print("🔧 4. 修复数据类型转换...")
    content, fixes = fix_data_type_conversion_v2(content)
    fixes_applied.extend(fixes)
    
    # 5. 修复硬编码问题
    print("🔧 5. 修复硬编码问题...")
    content, fixes = fix_hardcoded_issues_v2(content)
    fixes_applied.extend(fixes)
    
    # 6. 修复字符串比较
    print("🔧 6. 修复字符串比较...")
    content, fixes = fix_string_comparison_v2(content)
    fixes_applied.extend(fixes)
    
    # 7. 修复函数命名
    print("🔧 7. 修复函数命名...")
    content, fixes = fix_function_naming_v2(content)
    fixes_applied.extend(fixes)
    
    # 8. 添加性能优化
    print("🔧 8. 添加性能优化...")
    content, fixes = add_performance_optimizations_v2(content)
    fixes_applied.extend(fixes)
    
    # 保存修复后的文件
    if content != original_content:
        backup_file = crisis_monitor_file.with_suffix('.py.backup_v2')
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

def add_safe_functions(content):
    """添加安全函数"""
    fixes = []
    
    # 在导入部分后添加安全函数
    safe_functions = '''
# 安全函数定义
def safe_divide(numerator, denominator, default_value=0.0):
    """安全的除法运算，避免除零错误"""
    try:
        if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
            return default_value
        return numerator / denominator
    except (TypeError, ValueError, ZeroDivisionError):
        return default_value

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

def safe_print(message, *args, **kwargs):
    """安全的打印函数"""
    try:
        print(message, *args, **kwargs)
    except Exception as e:
        print(f"输出错误: {e}")

'''
    
    # 在导入部分后添加安全函数
    import_end_pattern = r'(from dotenv import load_dotenv\n)'
    if re.search(import_end_pattern, content):
        content = re.sub(import_end_pattern, r'\1' + safe_functions, content)
        fixes.append("添加安全函数")
    
    return content, fixes

def fix_zero_division_v2(content):
    """修复除零风险 - 版本2"""
    fixes = []
    
    # 修复明显的除零风险
    zero_division_patterns = [
        # 修复除法运算
        (r'(\w+)\s*/\s*(\w+)\s*\*\s*100', r'safe_divide(\1, \2) * 100'),
        (r'(\w+)\s*/\s*(\w+)', r'safe_divide(\1, \2)'),
        # 修复文件大小计算
        (r'os\.path\.getsize\(([^)]+)\)\s*/\s*\(1024\*1024\)', r'safe_divide(os.path.getsize(\1), 1024*1024)'),
        # 修复百分比计算
        (r'(\w+)\s*/\s*100', r'safe_divide(\1, 100)'),
    ]
    
    for pattern, replacement in zero_division_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复除零风险: {pattern}")
    
    return content, fixes

def fix_exception_handling_v2(content):
    """修复异常处理 - 版本2"""
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
    
    return content, fixes

def fix_data_type_conversion_v2(content):
    """修复数据类型转换 - 版本2"""
    fixes = []
    
    # 修复print语句中的类型转换
    print_pattern = r'print\(f"([^"]*\{[^}]*\}[^"]*)"\)'
    if re.search(print_pattern, content):
        # 为print语句添加异常处理
        content = re.sub(
            print_pattern,
            lambda m: f'safe_print(f"{m.group(1)}")',
            content
        )
        fixes.append("修复print语句类型转换")
    
    return content, fixes

def fix_hardcoded_issues_v2(content):
    """修复硬编码问题 - 版本2"""
    fixes = []
    
    # 添加配置常量
    config_constants = '''
# 配置常量
DEFAULT_TIMEOUT = 300
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CACHE_SIZE = 128
DEFAULT_WINDOW_SIZE = [1200, 800]
DEFAULT_VIEWPORT_SIZE = [1200, 800]
DEFAULT_FIGURE_SIZE = [10, 6]
DEFAULT_DPI = 100
DEFAULT_MB_DIVISOR = 1024 * 1024

'''
    
    # 在安全函数后添加配置常量
    if 'def safe_print(' in content:
        content = content.replace('def safe_print(', config_constants + 'def safe_print(')
        fixes.append("添加配置常量")
    
    # 替换硬编码数值
    hardcoded_patterns = [
        (r'1200', 'DEFAULT_WINDOW_SIZE[0]'),
        (r'800', 'DEFAULT_WINDOW_SIZE[1]'),
        (r'1024\*1024', 'DEFAULT_MB_DIVISOR'),
        (r'300', 'DEFAULT_TIMEOUT'),
        (r'1000', 'DEFAULT_CHUNK_SIZE'),
        (r'128', 'DEFAULT_CACHE_SIZE'),
    ]
    
    for pattern, replacement in hardcoded_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"替换硬编码数值: {pattern}")
    
    return content, fixes

def fix_string_comparison_v2(content):
    """修复字符串比较 - 版本2"""
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

def fix_function_naming_v2(content):
    """修复函数命名 - 版本2"""
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

def add_performance_optimizations_v2(content):
    """添加性能优化 - 版本2"""
    fixes = []
    
    # 添加缓存装饰器
    cache_decorator = '''
from functools import lru_cache
import hashlib

@lru_cache(maxsize=DEFAULT_CACHE_SIZE)
def cached_read_csv(file_path, **kwargs):
    """缓存的CSV读取"""
    try:
        return pd.read_csv(file_path, **kwargs)
    except Exception as e:
        safe_print(f"CSV读取失败: {e}")
        return pd.DataFrame()

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

def main():
    """主函数"""
    print("crisis_monitor.py 修复工具 (版本2)")
    print("=" * 80)
    
    try:
        fix_crisis_monitor_v2()
        print("\n✅ 修复完成!")
        
    except Exception as e:
        print(f"\n❌ 修复过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









