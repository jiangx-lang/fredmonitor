"""
MacroLab 系统测试脚本

测试系统是否正常工作。
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('macrolab.env')

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """测试模块导入"""
    print("测试模块导入...")
    try:
        from core.fred_client import FredClient
        from core.cache import CacheManager
        from core.registry import FactorRegistry
        from core.aggregator import DataAggregator
        from core.report import ReportGenerator
        from core.scoring import risk_score
        print("✓ 核心模块导入成功")
        
        from factors.vix import VIX
        from factors.ted_spread import TED_SPREAD
        from factors.hy_spread import HY_SPREAD
        print("✓ 因子模块导入成功")
        
        return True
    except Exception as e:
        print(f"✗ 模块导入失败: {e}")
        return False

def test_fred_connection():
    """测试FRED连接"""
    print("\n测试FRED API连接...")
    try:
        from core.fred_client import FredClient
        from core.cache import CacheManager
        
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            print("✗ 未设置FRED_API_KEY")
            return False
        
        cache_manager = CacheManager("D:\\MacroLab")
        fred_client = FredClient(api_key, cache_manager)
        
        # 测试获取VIX数据
        print("正在获取VIX数据...")
        vix_data = fred_client.get_series("VIXCLS")
        if not vix_data.empty:
            print(f"✓ FRED连接成功，获取到{len(vix_data)}条VIX数据")
            print(f"最新VIX值: {vix_data.iloc[-1]:.2f}")
            return True
        else:
            print("✗ 未获取到VIX数据")
            return False
            
    except Exception as e:
        print(f"✗ FRED连接失败: {e}")
        return False

def test_factor_registry():
    """测试因子注册表"""
    print("\n测试因子注册表...")
    try:
        from core.registry import FactorRegistry
        
        registry = FactorRegistry("factors", "config/factor_registry.yaml")
        factors = registry.get_all_factors()
        
        print(f"✓ 注册表加载成功，发现{len(factors)}个因子")
        
        for factor_id, factor in factors.items():
            print(f"  - {factor_id}: {factor.name}")
        
        return True
    except Exception as e:
        print(f"✗ 因子注册表测试失败: {e}")
        return False

def test_scoring():
    """测试评分算法"""
    print("\n测试评分算法...")
    try:
        from core.scoring import risk_score, calculate_factor_score
        
        # 测试基本评分
        score1 = risk_score(20, 10, 30, reverse=False)
        score2 = risk_score(20, 10, 30, reverse=True)
        
        print(f"✓ 基本评分测试: 正向={score1:.2f}, 反向={score2:.2f}")
        
        # 测试因子评分
        global_cfg = {
            "bands": {
                "VIX": [12, 30]
            }
        }
        score3 = calculate_factor_score("VIX", 20, global_cfg)
        print(f"✓ 因子评分测试: VIX(20)={score3:.2f}")
        
        return True
    except Exception as e:
        print(f"✗ 评分算法测试失败: {e}")
        return False

def test_daily_analysis():
    """测试每日分析"""
    print("\n测试每日分析...")
    try:
        from core.fred_client import FredClient
        from core.cache import CacheManager
        from core.registry import FactorRegistry
        from core.aggregator import DataAggregator
        from core.utils import load_yaml_config
        
        # 初始化组件
        cache_manager = CacheManager("D:\\MacroLab")
        fred_client = FredClient(os.getenv("FRED_API_KEY"), cache_manager)
        settings = load_yaml_config("config/settings.yaml")
        registry = FactorRegistry("factors", "config/factor_registry.yaml")
        aggregator = DataAggregator(fred_client, cache_manager, registry, settings)
        
        # 运行分析
        print("正在运行每日分析...")
        result = aggregator.run_daily_analysis()
        
        print(f"✓ 每日分析完成")
        print(f"  分析日期: {result['date'].strftime('%Y-%m-%d')}")
        print(f"  综合评分: {result['total_score']:.2f}")
        print(f"  风险等级: {result['risk_level']}")
        
        print("  各因子评分:")
        for factor_id, score in result['factor_scores'].items():
            value = result['factor_values'].get(factor_id, "N/A")
            print(f"    {factor_id}: {score:.2f} (值: {value})")
        
        return True
    except Exception as e:
        print(f"✗ 每日分析测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("MacroLab 系统测试")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_fred_connection,
        test_factor_registry,
        test_scoring,
        test_daily_analysis
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("✓ 所有测试通过！系统运行正常。")
        print("\n可以运行以下命令:")
        print("  python macro.py run-daily     # 运行每日分析")
        print("  python macro.py list-factors  # 列出所有因子")
        print("  python macro.py --help        # 查看帮助")
    else:
        print("✗ 部分测试失败，请检查配置和依赖。")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
