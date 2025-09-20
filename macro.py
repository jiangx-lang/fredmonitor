"""
MacroLab 命令行接口

提供主要的命令行功能。
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional
import logging
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 加载环境变量
load_dotenv('macrolab.env')

from core.fred_client import FredClient
from core.cache import CacheManager
from core.registry import FactorRegistry
from core.aggregator import DataAggregator
from core.report import ReportGenerator
from core.ai_explainer import AIExplainer
from core.utils import load_yaml_config, get_env_var
from core.logging_conf import setup_logging

logger = logging.getLogger(__name__)


class MacroLabCLI:
    """MacroLab命令行接口"""
    
    def __init__(self):
        """初始化CLI"""
        self.base_dir = get_env_var("MACROLAB_BASE_DIR", "D:\\MacroLab")
        self.excel_out = get_env_var("MACROLAB_EXCEL_OUT", "D:\\标普\\backtest_results\\宏观金融危机风险打分系统.xlsx")
        self.debug = get_env_var("MACROLAB_DEBUG", "0") == "1"
        
        # 设置日志
        setup_logging(self.base_dir, self.debug)
        
        # 加载配置
        self.settings = load_yaml_config(os.path.join("config", "settings.yaml"))
        self.factor_registry = FactorRegistry("factors", "config/factor_registry.yaml")
        
        # 初始化组件
        self.cache_manager = CacheManager(self.base_dir)
        self.fred_client = FredClient(get_env_var("FRED_API_KEY", ""), self.cache_manager)
        self.aggregator = DataAggregator(self.fred_client, self.cache_manager, 
                                        self.factor_registry, self.settings)
        self.report_generator = ReportGenerator(self.base_dir)
        self.ai_explainer = AIExplainer(self.settings.get("ai", {}))
    
    def run_daily(self) -> None:
        """运行每日分析"""
        try:
            logger.info("开始每日分析")
            
            # 运行分析
            result = self.aggregator.run_daily_analysis()
            
            # 生成报告
            recent_scores = self.aggregator.get_recent_scores(5)
            report_path = self.report_generator.generate_daily_report(result, recent_scores)
            
            # 生成Excel汇总
            if self.settings.get("outputs", {}).get("write_excel", True):
                self.report_generator.generate_excel_summary(result, self.excel_out)
            
            # AI解读（如果启用）
            if self.settings.get("ai", {}).get("enable_ai_commentary", False):
                ai_commentary = self.ai_explainer.generate_overall_commentary(result)
                if ai_commentary:
                    logger.info("AI解读生成成功")
                    # 可以将AI解读添加到报告中
            
            logger.info(f"每日分析完成，报告保存至: {report_path}")
            
        except Exception as e:
            logger.error(f"每日分析失败: {e}")
            raise
    
    def backfill(self, start_date: str, end_date: str) -> None:
        """历史数据回填"""
        try:
            logger.info(f"开始历史数据回填: {start_date} 到 {end_date}")
            
            # 解析日期
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            # 遍历每一天
            current_date = start_dt
            while current_date <= end_dt:
                try:
                    logger.info(f"处理日期: {current_date.date()}")
                    result = self.aggregator.run_daily_analysis(current_date)
                    
                    # 生成报告
                    recent_scores = self.aggregator.get_recent_scores(5)
                    self.report_generator.generate_daily_report(result, recent_scores)
                    
                    # 生成Excel汇总
                    if self.settings.get("outputs", {}).get("write_excel", True):
                        self.report_generator.generate_excel_summary(result, self.excel_out)
                    
                except Exception as e:
                    logger.error(f"处理日期失败 {current_date.date()}: {e}")
                
                current_date += timedelta(days=1)
            
            logger.info("历史数据回填完成")
            
        except Exception as e:
            logger.error(f"历史数据回填失败: {e}")
            raise
    
    def list_factors(self) -> None:
        """列出所有因子"""
        try:
            factors = self.factor_registry.list_factors()
            
            print("\n启用的宏观因子:")
            print("-" * 50)
            for factor in factors:
                print(f"ID: {factor['id']}")
                print(f"名称: {factor['name']}")
                print(f"单位: {factor['units']}")
                print("-" * 50)
            
        except Exception as e:
            logger.error(f"列出因子失败: {e}")
            raise
    
    def explain(self, date: str, use_ai: bool = False) -> None:
        """生成解读报告"""
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d")
            
            # 运行分析
            result = self.aggregator.run_daily_analysis(target_date)
            
            # 生成报告
            recent_scores = self.aggregator.get_recent_scores(5)
            report_path = self.report_generator.generate_daily_report(result, recent_scores)
            
            print(f"解读报告已生成: {report_path}")
            
            if use_ai and self.settings.get("ai", {}).get("enable_ai_commentary", False):
                ai_commentary = self.ai_explainer.generate_overall_commentary(result)
                if ai_commentary:
                    print("\nAI解读:")
                    print("-" * 50)
                    print(ai_commentary)
                    print("-" * 50)
            
        except Exception as e:
            logger.error(f"生成解读报告失败: {e}")
            raise


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="MacroLab 宏观分析系统")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # run-daily 命令
    subparsers.add_parser("run-daily", help="运行每日分析")
    
    # backfill 命令
    backfill_parser = subparsers.add_parser("backfill", help="历史数据回填")
    backfill_parser.add_argument("--start", required=True, help="开始日期 (YYYY-MM-DD)")
    backfill_parser.add_argument("--end", required=True, help="结束日期 (YYYY-MM-DD)")
    
    # list-factors 命令
    subparsers.add_parser("list-factors", help="列出所有因子")
    
    # explain 命令
    explain_parser = subparsers.add_parser("explain", help="生成解读报告")
    explain_parser.add_argument("--date", required=True, help="分析日期 (YYYY-MM-DD)")
    explain_parser.add_argument("--ai", action="store_true", help="使用AI解读")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        cli = MacroLabCLI()
        
        if args.command == "run-daily":
            cli.run_daily()
        elif args.command == "backfill":
            cli.backfill(args.start, args.end)
        elif args.command == "list-factors":
            cli.list_factors()
        elif args.command == "explain":
            cli.explain(args.date, args.ai)
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"命令执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
