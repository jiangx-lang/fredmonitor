#!/usr/bin/env python3
"""
MacroLab 每日运行脚本 (Python版本)

一键执行：数据同步 -> 事实表渲染 -> AI分析
"""

import os
import sys
import subprocess
import datetime
from pathlib import Path

def run_command(cmd, description):
    """运行命令并检查结果"""
    print(f"\n{'='*50}")
    print(f"正在执行: {description}")
    print(f"命令: {cmd}")
    print('='*50)
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print("✓ 执行成功")
        if result.stdout:
            print("输出:", result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ 执行失败: {e}")
        if e.stderr:
            print("错误:", e.stderr)
        return False

def main():
    """主函数"""
    print("MacroLab 每日宏观分析流程")
    print("=" * 50)
    
    # 检查基础目录
    base_dir = os.getenv("BASE_DIR", os.getcwd())
    print(f"工作目录: {base_dir}")
    
    # 1. 数据同步
    if not run_command("python scripts/sync_fred_http.py", "FRED数据同步"):
        print("❌ 数据同步失败，停止执行")
        return False
    
    # 2. 事实表渲染
    if not run_command("python scripts/render_fact_sheets_http.py", "事实表渲染"):
        print("❌ 事实表渲染失败，停止执行")
        return False
    
    # 3. AI分析
    if not run_command("python scripts/ai_assess.py", "AI宏观分析"):
        print("❌ AI分析失败，停止执行")
        return False
    
    # 4. 显示结果
    print("\n" + "="*50)
    print("🎉 每日分析流程完成！")
    print("="*50)
    
    # 检查生成的文件
    today = datetime.date.today().strftime("%Y-%m-%d")
    
    files_to_check = [
        "data/lake/fred.duckdb",
        f"outputs/macro_status/{today}.md"
    ]
    
    print("\n生成的文件:")
    for file_path in files_to_check:
        full_path = Path(base_dir) / file_path
        if full_path.exists():
            print(f"✓ {file_path}")
        else:
            print(f"✗ {file_path} (未找到)")
    
    # 检查事实表
    fact_sheets_dir = Path(base_dir) / "data" / "fred" / "series"
    if fact_sheets_dir.exists():
        fact_sheets = list(fact_sheets_dir.glob("*/fact_sheet.md"))
        print(f"✓ 事实表: {len(fact_sheets)} 个")
    
    print(f"\n📊 数据湖位置: {Path(base_dir) / 'data' / 'lake' / 'fred.duckdb'}")
    print(f"📝 AI报告位置: {Path(base_dir) / 'outputs' / 'macro_status' / f'{today}.md'}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
