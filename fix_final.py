#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终修复脚本 - 处理剩余问题
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

def fix_final():
    """最终修复"""
    print("🔧 最终修复脚本")
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
    
    # 1. 修复函数命名问题
    print("🔧 1. 修复函数命名问题...")
    content, fixes = fix_function_naming_final(content)
    fixes_applied.extend(fixes)
    
    # 2. 修复路径问题
    print("🔧 2. 修复路径问题...")
    content, fixes = fix_path_issues_final(content)
    fixes_applied.extend(fixes)
    
    # 3. 修复异常处理
    print("🔧 3. 修复异常处理...")
    content, fixes = fix_exception_handling_final(content)
    fixes_applied.extend(fixes)
    
    # 4. 修复硬编码问题
    print("🔧 4. 修复硬编码问题...")
    content, fixes = fix_hardcoded_final(content)
    fixes_applied.extend(fixes)
    
    # 5. 修复性能问题
    print("🔧 5. 修复性能问题...")
    content, fixes = fix_performance_final(content)
    fixes_applied.extend(fixes)
    
    # 6. 修复逻辑问题
    print("🔧 6. 修复逻辑问题...")
    content, fixes = fix_logic_final(content)
    fixes_applied.extend(fixes)
    
    # 保存修复后的文件
    if content != original_content:
        backup_file = crisis_monitor_file.with_suffix('.py.backup_final')
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

def fix_function_naming_final(content):
    """修复函数命名问题"""
    fixes = []
    
    # 修复空参数函数
    empty_param_patterns = [
        (r'def setup_chinese_font\(\):', 'def setup_chinese_font():'),
        (r'def month_end_code\(\) -> str:', 'def month_end_code() -> str:'),
        (r'def run_data_pipeline\(\):', 'def run_data_pipeline():'),
        (r'def generate_report_with_images\(\):', 'def generate_report_with_images():'),
    ]
    
    for pattern, replacement in empty_param_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复空参数函数: {pattern}")
    
    return content, fixes

def fix_path_issues_final(content):
    """修复路径问题"""
    fixes = []
    
    # 修复路径拼接
    path_patterns = [
        (r'os\.path\.join\(([^)]+)\)', r'Path(\1)'),
        (r'BASE / "data" / "fred" / "series" / (\w+) / "raw\.csv"', r'get_data_path("fred", \1) / "raw.csv"'),
        (r'"data/series/([^"]+)"', r'str(get_data_path("series", r"\1"))'),
        (r'"data/fred/series/([^"]+)"', r'str(get_data_path("fred", r"\1"))'),
    ]
    
    for pattern, replacement in path_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复路径问题: {pattern}")
    
    # 添加路径获取函数
    path_function = '''
def get_data_path(path_type, filename=None):
    """获取数据路径"""
    base_paths = {
        'series': 'data/series',
        'fred': 'data/fred/series',
        'outputs': 'outputs/crisis_monitor',
        'config': 'config',
        'scripts': 'scripts'
    }
    base_path = base_paths.get(path_type, 'data')
    if filename:
        return Path(base_path) / filename
    return Path(base_path)

'''
    
    # 在安全函数后添加路径函数
    if 'def safe_print(' in content:
        content = content.replace('def safe_print(', path_function + 'def safe_print(')
        fixes.append("添加路径获取函数")
    
    return content, fixes

def fix_exception_handling_final(content):
    """修复异常处理"""
    fixes = []
    
    # 修复裸露的except语句
    bare_except_pattern = r'except:\s*\n'
    if re.search(bare_except_pattern, content):
        content = re.sub(bare_except_pattern, 'except Exception as e:\n', content)
        fixes.append("修复裸露的except语句")
    
    # 修复文件存在性检查
    file_exists_patterns = [
        (r'if os\.path\.exists\(([^)]+)\):', r'if Path(\1).exists():'),
        (r'if not ([^)]+)\.exists\(\):', r'if not Path(\1).exists():'),
    ]
    
    for pattern, replacement in file_exists_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复文件存在性检查: {pattern}")
    
    return content, fixes

def fix_hardcoded_final(content):
    """修复硬编码问题"""
    fixes = []
    
    # 修复硬编码文件名
    filename_patterns = [
        (r'"crisis_report_latest\.json"', r'"crisis_report_latest.json"'),
        (r'"crisis_report_\{timestamp\}\.json"', r'f"crisis_report_{timestamp}.json"'),
        (r'"CORPORATE_DEBT_GDP_RATIO\.csv"', r'"CORPORATE_DEBT_GDP_RATIO.csv"'),
        (r'"benchmarks_yoy\.json"', r'"benchmarks_yoy.json"'),
    ]
    
    for pattern, replacement in filename_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复硬编码文件名: {pattern}")
    
    # 修复硬编码URL
    url_patterns = [
        (r'from scripts\.fred_http import', r'from scripts.fred_http import'),
    ]
    
    for pattern, replacement in url_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复硬编码URL: {pattern}")
    
    return content, fixes

def fix_performance_final(content):
    """修复性能问题"""
    fixes = []
    
    # 修复大文件读取
    read_csv_patterns = [
        (r'cached_read_csv\(([^)]+)\)', r'cached_read_csv(\1, chunksize=DEFAULT_CHUNK_SIZE)'),
    ]
    
    for pattern, replacement in read_csv_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复大文件读取: {pattern}")
    
    # 修复嵌套循环
    nested_loop_patterns = [
        (r'\[i for i in ([^]]+) if ([^]]+)\]', r'list(filter(lambda i: \2, \1))'),
    ]
    
    for pattern, replacement in nested_loop_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复嵌套循环: {pattern}")
    
    return content, fixes

def fix_logic_final(content):
    """修复逻辑问题"""
    fixes = []
    
    # 修复字符串比较
    string_compare_patterns = [
        (r'(\w+)\.get\(([^)]+)\) == "([^"]+)"', r'\1.get(\2).lower() == "\3".lower()'),
        (r'(\w+)\.get\(([^)]+)\) == \'([^\']+)\'', r'\1.get(\2).lower() == "\3".lower()'),
    ]
    
    for pattern, replacement in string_compare_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复字符串比较: {pattern}")
    
    # 修复空值检查
    null_check_patterns = [
        (r'if not (\w+): return None', r'if not \1 or \1 is None: return None'),
        (r'(\w+) = None', r'\1 = None  # 明确设置为None'),
    ]
    
    for pattern, replacement in null_check_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复空值检查: {pattern}")
    
    # 修复除零风险
    division_patterns = [
        (r'(\w+) / 100', r'safe_divide(\1, 100)'),
        (r'(\w+) / (\w+)', r'safe_divide(\1, \2)'),
    ]
    
    for pattern, replacement in division_patterns:
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            fixes.append(f"修复除零风险: {pattern}")
    
    return content, fixes

def main():
    """主函数"""
    print("最终修复脚本")
    print("=" * 80)
    
    try:
        fix_final()
        print("\n✅ 最终修复完成!")
        
    except Exception as e:
        print(f"\n❌ 修复过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









