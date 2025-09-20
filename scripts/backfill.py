"""
历史数据回填脚本

用于批量处理历史数据。
"""

import os
import sys
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from macro import MacroLabCLI


def main():
    """主函数"""
    if len(sys.argv) != 3:
        print("用法: python backfill.py <开始日期> <结束日期>")
        print("日期格式: YYYY-MM-DD")
        print("示例: python backfill.py 2020-01-01 2024-12-31")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    
    try:
        # 验证日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        
        # 运行回填
        cli = MacroLabCLI()
        cli.backfill(start_date, end_date)
        
        print("历史数据回填完成！")
        
    except ValueError as e:
        print(f"日期格式错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"回填失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
