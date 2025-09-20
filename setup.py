"""
MacroLab 安装脚本

用于安装和配置MacroLab项目。
"""

import os
import sys
import subprocess
from pathlib import Path


def run_command(command, description):
    """运行命令并显示结果"""
    print(f"正在{description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description}成功")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description}失败: {e}")
        if e.stderr:
            print(f"错误信息: {e.stderr}")
        return False


def create_directories():
    """创建必要的目录"""
    directories = [
        "data/raw/fred",
        "data/processed/daily", 
        "data/processed/history",
        "outputs/reports",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ 创建目录: {directory}")


def setup_environment():
    """设置环境文件"""
    env_example = Path("env.example")
    env_file = Path(".env")
    
    if not env_file.exists() and env_example.exists():
        env_file.write_text(env_example.read_text())
        print("✓ 创建环境配置文件: .env")
        print("请编辑 .env 文件并填入您的API密钥")
    else:
        print("✓ 环境配置文件已存在")


def install_dependencies():
    """安装依赖"""
    commands = [
        ("pip install -e .", "安装项目依赖"),
        ("pip install pytest", "安装测试依赖"),
        ("pip install ruff", "安装代码检查工具")
    ]
    
    for command, description in commands:
        if not run_command(command, description):
            print(f"警告: {description}失败，请手动安装")


def run_tests():
    """运行测试"""
    if run_command("python -m pytest tests/ -v", "运行测试"):
        print("✓ 所有测试通过")
    else:
        print("⚠ 部分测试失败，请检查配置")


def main():
    """主函数"""
    print("MacroLab 安装脚本")
    print("=" * 50)
    
    # 检查Python版本
    if sys.version_info < (3, 10):
        print("错误: 需要Python 3.10或更高版本")
        sys.exit(1)
    
    print(f"Python版本: {sys.version}")
    
    # 创建目录
    print("\n1. 创建目录结构...")
    create_directories()
    
    # 设置环境
    print("\n2. 设置环境配置...")
    setup_environment()
    
    # 安装依赖
    print("\n3. 安装依赖...")
    install_dependencies()
    
    # 运行测试
    print("\n4. 运行测试...")
    run_tests()
    
    print("\n" + "=" * 50)
    print("安装完成！")
    print("\n下一步:")
    print("1. 编辑 .env 文件，填入您的FRED API密钥")
    print("2. 运行示例: python examples/example_usage.py")
    print("3. 运行每日分析: python macro.py run-daily")
    print("4. 查看帮助: python macro.py --help")


if __name__ == "__main__":
    main()
