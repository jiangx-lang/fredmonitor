#!/usr/bin/env python3
"""
将所有项目文件从C盘迁移到D盘
"""

import os
import shutil
import pathlib
from datetime import datetime

def migrate_project_to_d_drive():
    """将整个项目迁移到D盘"""
    
    # 源路径和目标路径
    source_path = pathlib.Path("C:/Users/admin/上网")
    target_path = pathlib.Path("D:/fred_crisis_monitor")
    
    print("🚚 开始迁移项目到D盘...")
    print("=" * 80)
    print(f"📂 源路径: {source_path}")
    print(f"📂 目标路径: {target_path}")
    
    # 检查D盘是否存在
    if not pathlib.Path("D:/").exists():
        print("❌ D盘不存在，无法迁移")
        return False
    
    # 创建目标目录
    target_path.mkdir(parents=True, exist_ok=True)
    print(f"✅ 创建目标目录: {target_path}")
    
    # 获取所有文件和目录
    total_files = 0
    total_dirs = 0
    copied_files = 0
    copied_dirs = 0
    errors = []
    
    print(f"\n📊 开始复制文件...")
    
    # 遍历源目录
    for root, dirs, files in os.walk(source_path):
        # 计算相对路径
        rel_path = pathlib.Path(root).relative_to(source_path)
        target_dir = target_path / rel_path
        
        # 创建目标目录
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            copied_dirs += 1
        except Exception as e:
            errors.append(f"创建目录失败 {target_dir}: {e}")
            continue
        
        # 复制文件
        for file in files:
            source_file = pathlib.Path(root) / file
            target_file = target_dir / file
            
            try:
                # 跳过一些不需要的文件
                if file.endswith('.pyc') or file.startswith('.'):
                    continue
                
                shutil.copy2(source_file, target_file)
                copied_files += 1
                total_files += 1
                
                if copied_files % 100 == 0:
                    print(f"  📄 已复制 {copied_files} 个文件...")
                    
            except Exception as e:
                errors.append(f"复制文件失败 {source_file}: {e}")
                total_files += 1
    
    # 统计目录数量
    for root, dirs, files in os.walk(source_path):
        total_dirs += len(dirs)
    
    print(f"\n🎯 迁移完成统计:")
    print("=" * 80)
    print(f"📁 总目录数: {total_dirs}")
    print(f"📄 总文件数: {total_files}")
    print(f"✅ 成功复制目录: {copied_dirs}")
    print(f"✅ 成功复制文件: {copied_files}")
    print(f"❌ 错误数量: {len(errors)}")
    
    if errors:
        print(f"\n❌ 错误详情:")
        for error in errors[:10]:  # 只显示前10个错误
            print(f"  {error}")
        if len(errors) > 10:
            print(f"  ... 还有 {len(errors) - 10} 个错误")
    
    # 创建迁移记录
    migration_log = target_path / "migration_log.txt"
    with open(migration_log, 'w', encoding='utf-8') as f:
        f.write(f"项目迁移记录\n")
        f.write(f"迁移时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"源路径: {source_path}\n")
        f.write(f"目标路径: {target_path}\n")
        f.write(f"总目录数: {total_dirs}\n")
        f.write(f"总文件数: {total_files}\n")
        f.write(f"成功复制目录: {copied_dirs}\n")
        f.write(f"成功复制文件: {copied_files}\n")
        f.write(f"错误数量: {len(errors)}\n")
        f.write(f"\n错误详情:\n")
        for error in errors:
            f.write(f"{error}\n")
    
    print(f"\n📝 迁移记录已保存: {migration_log}")
    
    # 创建新的工作目录脚本
    create_d_drive_setup_script(target_path)
    
    return True

def create_d_drive_setup_script(target_path):
    """创建D盘工作环境设置脚本"""
    
    setup_script = target_path / "setup_d_drive_environment.py"
    
    script_content = f'''#!/usr/bin/env python3
"""
D盘工作环境设置脚本
"""

import os
import sys
import pathlib

def setup_d_drive_environment():
    """设置D盘工作环境"""
    
    print("🚀 设置D盘工作环境...")
    print("=" * 80)
    
    # 设置工作目录
    work_dir = pathlib.Path("{target_path}")
    os.chdir(work_dir)
    
    print(f"📂 工作目录: {{work_dir}}")
    
    # 添加项目路径到Python路径
    if str(work_dir) not in sys.path:
        sys.path.insert(0, str(work_dir))
    
    print(f"🐍 Python路径已更新")
    
    # 检查关键文件
    key_files = [
        "scripts/crisis_monitor.py",
        "config/crisis_indicators.yaml", 
        "config/crisis_periods.yaml",
        "data/fred/categories"
    ]
    
    print(f"\\n🔍 检查关键文件:")
    for file_path in key_files:
        full_path = work_dir / file_path
        if full_path.exists():
            print(f"  ✅ {{file_path}}")
        else:
            print(f"  ❌ {{file_path}}")
    
    print(f"\\n🎉 D盘环境设置完成！")
    print(f"📂 当前工作目录: {{os.getcwd()}}")
    
    return True

if __name__ == "__main__":
    setup_d_drive_environment()
'''
    
    with open(setup_script, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    print(f"📝 创建D盘环境设置脚本: {setup_script}")
    
    # 创建批处理文件
    batch_file = target_path / "start_d_drive.bat"
    batch_content = f'''@echo off
echo 🚀 启动D盘FRED危机监控系统...
cd /d "{target_path}"
python setup_d_drive_environment.py
python -m scripts.crisis_monitor
pause
'''
    
    with open(batch_file, 'w', encoding='utf-8') as f:
        f.write(batch_content)
    
    print(f"📝 创建启动批处理文件: {batch_file}")

if __name__ == "__main__":
    success = migrate_project_to_d_drive()
    if success:
        print(f"\n🎉 项目迁移到D盘完成！")
        print(f"📂 新工作目录: D:/fred_crisis_monitor")
        print(f"🚀 运行 'start_d_drive.bat' 启动系统")
    else:
        print(f"\n❌ 项目迁移失败")
