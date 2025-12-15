#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主程序逐行检查工具
检查crisis_monitor.py中的潜在问题
"""

import os
import sys
import ast
import re
from pathlib import Path

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def check_main_program():
    """检查主程序中的问题"""
    print("🔍 主程序逐行检查")
    print("=" * 80)
    
    crisis_monitor_file = Path("crisis_monitor.py")
    if not crisis_monitor_file.exists():
        print("❌ crisis_monitor.py 文件不存在")
        return
    
    issues = []
    
    with open(crisis_monitor_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"📄 文件总行数: {len(lines)}")
    
    # 1. 检查导入问题
    print("\n📦 1. 检查导入问题:")
    import_issues = check_imports(lines)
    issues.extend(import_issues)
    
    # 2. 检查函数定义问题
    print("\n🔧 2. 检查函数定义问题:")
    function_issues = check_functions(lines)
    issues.extend(function_issues)
    
    # 3. 检查数据路径问题
    print("\n📁 3. 检查数据路径问题:")
    path_issues = check_data_paths(lines)
    issues.extend(path_issues)
    
    # 4. 检查异常处理问题
    print("\n⚠️ 4. 检查异常处理问题:")
    exception_issues = check_exception_handling(lines)
    issues.extend(exception_issues)
    
    # 5. 检查硬编码问题
    print("\n🔒 5. 检查硬编码问题:")
    hardcode_issues = check_hardcoded_values(lines)
    issues.extend(hardcode_issues)
    
    # 6. 检查性能问题
    print("\n⚡ 6. 检查性能问题:")
    performance_issues = check_performance_issues(lines)
    issues.extend(performance_issues)
    
    # 7. 检查逻辑问题
    print("\n🧠 7. 检查逻辑问题:")
    logic_issues = check_logic_issues(lines)
    issues.extend(logic_issues)
    
    # 总结
    print("\n" + "=" * 80)
    print("📋 检查结果总结:")
    print(f"发现 {len(issues)} 个问题:")
    
    for i, issue in enumerate(issues, 1):
        print(f"{i:2d}. {issue}")
    
    return issues

def check_imports(lines):
    """检查导入问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查重复导入
        if line.startswith('import ') or line.startswith('from '):
            # 检查是否有重复的导入
            if 'import' in line and line.count('import') > 1:
                issues.append(f"第{i}行: 重复导入 - {line}")
        
        # 检查未使用的导入
        if line.startswith('import ') and 'unused' in line.lower():
            issues.append(f"第{i}行: 可能未使用的导入 - {line}")
    
    return issues

def check_functions(lines):
    """检查函数定义问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查函数定义
        if line.startswith('def '):
            # 检查函数名是否符合规范
            func_match = re.match(r'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', line)
            if func_match:
                func_name = func_match.group(1)
                if func_name.startswith('_') and not func_name.startswith('__'):
                    issues.append(f"第{i}行: 私有函数命名不规范 - {func_name}")
            
            # 检查函数参数
            if '()' in line and not line.endswith('()'):
                issues.append(f"第{i}行: 空参数函数应使用() - {line}")
        
        # 检查函数调用
        if '(' in line and ')' in line and not line.startswith('#'):
            # 检查可能的函数调用问题
            if 'subprocess.run' in line and 'timeout' not in line:
                issues.append(f"第{i}行: subprocess.run缺少timeout参数 - {line}")
    
    return issues

def check_data_paths(lines):
    """检查数据路径问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查硬编码路径
        if 'data/' in line or 'data\\' in line:
            if not line.startswith('#') and 'pathlib' not in line:
                issues.append(f"第{i}行: 硬编码数据路径 - {line}")
        
        # 检查路径拼接
        if 'os.path.join' in line or '+' in line and ('/' in line or '\\' in line):
            issues.append(f"第{i}行: 使用字符串拼接路径，建议使用pathlib - {line}")
        
        # 检查文件存在性检查
        if 'os.path.exists' in line or 'path.exists' in line:
            # 检查是否有适当的错误处理
            next_lines = lines[i:i+3] if i < len(lines) - 3 else []
            if not any('except' in l or 'if' in l for l in next_lines):
                issues.append(f"第{i}行: 文件存在性检查缺少错误处理 - {line}")
    
    return issues

def check_exception_handling(lines):
    """检查异常处理问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查裸露的except
        if line.startswith('except:') or line.startswith('except :'):
            issues.append(f"第{i}行: 裸露的except语句 - {line}")
        
        # 检查空的except块
        if line.startswith('except') and 'pass' in lines[i:i+3]:
            issues.append(f"第{i}行: 空的except块 - {line}")
        
        # 检查可能的异常泄露
        if 'raise' in line and not line.startswith('#'):
            # 检查是否有适当的异常处理
            prev_lines = lines[max(0, i-5):i]
            if not any('try:' in l for l in prev_lines):
                issues.append(f"第{i}行: 可能的异常泄露 - {line}")
    
    return issues

def check_hardcoded_values(lines):
    """检查硬编码问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查硬编码的数值
        if re.search(r'\b\d{4,}\b', line) and not line.startswith('#'):
            # 排除日期、时间等合理的硬编码
            if not any(keyword in line.lower() for keyword in ['date', 'time', 'year', 'month', 'day']):
                issues.append(f"第{i}行: 可能的硬编码数值 - {line}")
        
        # 检查硬编码的字符串
        if 'http' in line or 'https' in line:
            if not line.startswith('#') and 'url' not in line.lower():
                issues.append(f"第{i}行: 硬编码URL - {line}")
        
        # 检查硬编码的文件名
        if '.csv' in line or '.json' in line or '.yaml' in line:
            if not line.startswith('#') and 'config' not in line.lower():
                issues.append(f"第{i}行: 硬编码文件名 - {line}")
    
    return issues

def check_performance_issues(lines):
    """检查性能问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查可能的性能问题
        if 'for ' in line and ' in ' in line:
            # 检查嵌套循环
            next_lines = lines[i:i+10] if i < len(lines) - 10 else []
            if any('for ' in l for l in next_lines):
                issues.append(f"第{i}行: 可能的嵌套循环性能问题 - {line}")
        
        # 检查大文件读取
        if 'read_csv' in line or 'read_json' in line:
            if 'chunksize' not in line and 'nrows' not in line:
                issues.append(f"第{i}行: 大文件读取缺少分块处理 - {line}")
        
        # 检查重复计算
        if 'pd.read_csv' in line and 'cache' not in line.lower():
            # 检查是否有缓存机制
            prev_lines = lines[max(0, i-10):i]
            if not any('cache' in l.lower() for l in prev_lines):
                issues.append(f"第{i}行: 重复文件读取缺少缓存 - {line}")
    
    return issues

def check_logic_issues(lines):
    """检查逻辑问题"""
    issues = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        
        # 检查可能的逻辑错误
        if 'if ' in line and '==' in line:
            # 检查字符串比较
            if '"' in line or "'" in line:
                if 'lower()' not in line and 'upper()' not in line:
                    issues.append(f"第{i}行: 字符串比较可能缺少大小写处理 - {line}")
        
        # 检查除零风险
        if '/' in line and not line.startswith('#'):
            if '0' in line and 'if' not in line:
                issues.append(f"第{i}行: 可能的除零风险 - {line}")
        
        # 检查空值处理
        if 'None' in line or 'null' in line:
            if 'is None' not in line and 'is not None' not in line:
                issues.append(f"第{i}行: 空值检查可能不准确 - {line}")
        
        # 检查数据类型转换
        if 'float(' in line or 'int(' in line:
            if 'try' not in lines[max(0, i-3):i]:
                issues.append(f"第{i}行: 数据类型转换缺少异常处理 - {line}")
    
    return issues

def main():
    """主函数"""
    print("主程序逐行检查工具")
    print("=" * 80)
    
    try:
        issues = check_main_program()
        
        if not issues:
            print("\n✅ 未发现明显问题!")
        else:
            print(f"\n⚠️ 发现 {len(issues)} 个潜在问题，建议修复")
        
    except Exception as e:
        print(f"\n❌ 检查过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









