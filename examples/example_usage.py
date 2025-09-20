"""
MacroLab 使用示例

展示如何使用MacroLab进行宏观分析。
"""

import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.fred_client import FredClient
from core.cache import CacheManager
from core.registry import FactorRegistry
from core.aggregator import DataAggregator
from core.report import ReportGenerator
from core.utils import load_yaml_config


def example_daily_analysis():
    """示例：运行每日分析"""
    print("=== MacroLab 每日分析示例 ===")
    
    # 1. 初始化组件
    base_dir = "D:\\MacroLab"
    cache_manager = CacheManager(base_dir)
    
    # 注意：需要设置FRED_API_KEY环境变量
    fred_api_key = os.getenv("FRED_API_KEY", "")
    if not fred_api_key:
        print("警告：未设置FRED_API_KEY，将使用模拟数据")
        fred_client = None
    else:
        fred_client = FredClient(fred_api_key, cache_manager)
    
    # 2. 加载配置
    settings = load_yaml_config("config/settings.yaml")
    registry = FactorRegistry("factors", "config/factor_registry.yaml")
    
    # 3. 创建聚合器
    aggregator = DataAggregator(fred_client, cache_manager, registry, settings)
    
    # 4. 运行分析
    print("开始运行每日分析...")
    result = aggregator.run_daily_analysis()
    
    # 5. 显示结果
    print(f"分析日期: {result['date'].strftime('%Y-%m-%d')}")
    print(f"综合风险评分: {result['total_score']:.2f}")
    print(f"风险等级: {result['risk_level']}")
    print("\n各因子评分:")
    for factor_id, score in result['factor_scores'].items():
        value = result['factor_values'].get(factor_id, "N/A")
        print(f"  {factor_id}: {score:.2f} (值: {value})")
    
    # 6. 生成报告
    report_generator = ReportGenerator(base_dir)
    recent_scores = aggregator.get_recent_scores(5)
    report_path = report_generator.generate_daily_report(result, recent_scores)
    print(f"\n报告已生成: {report_path}")
    
    return result


def example_factor_analysis():
    """示例：分析单个因子"""
    print("\n=== 单个因子分析示例 ===")
    
    # 导入VIX因子
    from factors.vix import VIX
    
    # 创建因子实例
    vix_factor = VIX({"series_id": "VIXCLS"})
    
    print(f"因子ID: {vix_factor.id}")
    print(f"因子名称: {vix_factor.name}")
    print(f"单位: {vix_factor.units}")
    
    # 模拟数据
    import pandas as pd
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    values = [15 + i for i in range(10)]  # 模拟VIX数据
    sample_data = pd.DataFrame({'date': dates, 'value': values})
    
    # 计算指标
    metrics = vix_factor.compute(sample_data)
    print(f"计算指标: {metrics}")
    
    # 计算评分
    global_config = {
        "bands": {
            "VIX": [12, 30]
        }
    }
    score = vix_factor.score(metrics, global_config)
    print(f"风险评分: {score:.2f}")
    
    # 转换为DataFrame
    result_df = vix_factor.to_frame(datetime.now(), metrics, score)
    print(f"结果DataFrame:\n{result_df}")


def example_custom_factor():
    """示例：创建自定义因子"""
    print("\n=== 自定义因子示例 ===")
    
    from factors.base_factor import Factor
    from core.scoring import calculate_factor_score
    
    class CustomFactor(Factor):
        """自定义因子示例"""
        
        id = "CUSTOM"
        name = "自定义因子"
        units = "百分比"
        
        def __init__(self, cfg):
            super().__init__(cfg)
            self.threshold = cfg.get("threshold", 50)
        
        def fetch(self):
            """获取数据"""
            import pandas as pd
            # 模拟数据
            dates = pd.date_range('2024-01-01', periods=5, freq='D')
            values = [45, 47, 52, 48, 51]
            return pd.DataFrame({'date': dates, 'value': values})
        
        def compute(self, df):
            """计算指标"""
            if df.empty:
                return {"original_value": None}
            
            latest_value = df['value'].iloc[-1]
            return {"original_value": float(latest_value)}
        
        def score(self, metrics, global_cfg):
            """计算评分"""
            return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
    
    # 使用自定义因子
    custom_factor = CustomFactor({"threshold": 50})
    
    # 获取数据
    data = custom_factor.fetch()
    print(f"获取数据:\n{data}")
    
    # 计算指标
    metrics = custom_factor.compute(data)
    print(f"计算指标: {metrics}")
    
    # 计算评分
    global_config = {
        "bands": {
            "CUSTOM": [40, 60]
        }
    }
    score = custom_factor.score(metrics, global_config)
    print(f"风险评分: {score:.2f}")


def main():
    """主函数"""
    print("MacroLab 使用示例")
    print("=" * 50)
    
    try:
        # 运行示例
        example_factor_analysis()
        example_custom_factor()
        
        # 注意：每日分析需要FRED API密钥
        print("\n注意：每日分析示例需要设置FRED_API_KEY环境变量")
        print("可以通过以下方式设置：")
        print("1. 在.env文件中设置")
        print("2. 设置环境变量：set FRED_API_KEY=your_key_here")
        print("3. 在代码中直接设置")
        
    except Exception as e:
        print(f"示例运行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
