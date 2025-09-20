#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试更新后的配置文件
"""

import sys
import pathlib
import yaml
from datetime import datetime

# 添加项目路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 导入原有模块
from crisis_monitor import get_series_data, _is_bad

def test_updated_config():
    """测试更新后的配置"""
    print("🧪 测试更新后的危机预警配置...")
    print("=" * 60)
    
    # 加载新配置
    config_path = BASE / "config" / "crisis_indicators_updated.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    indicators = config["indicators"]
    print(f"📊 新配置包含 {len(indicators)} 个指标")
    
    # 测试每个指标的数据可用性
    results = []
    for i, ind in enumerate(indicators, 1):
        name = ind["name"]
        series_id = ind["series_id"]
        
        print(f"\n[{i}/{len(indicators)}] 测试 {name} ({series_id})...")
        
        try:
            # 获取数据
            s = get_series_data(series_id)
            if _is_bad(s):
                print(f"  ❌ 无法获取数据")
                results.append({
                    "indicator": name,
                    "series_id": series_id,
                    "status": "error",
                    "latest_date": "N/A"
                })
            else:
                # 获取最新数据日期
                latest_date = s.index[-1].strftime("%Y-%m-%d")
                data_points = len(s)
                
                print(f"  ✅ 数据可用，最新: {latest_date}, 数据点: {data_points}")
                results.append({
                    "indicator": name,
                    "series_id": series_id,
                    "status": "success",
                    "latest_date": latest_date,
                    "data_points": data_points
                })
        except Exception as e:
            print(f"  ❌ 测试失败: {e}")
            results.append({
                "indicator": name,
                "series_id": series_id,
                "status": "error",
                "latest_date": "N/A"
            })
    
    # 统计结果
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "error"]
    
    print(f"\n📈 测试结果统计:")
    print(f"  ✅ 成功: {len(successful)} 个指标")
    print(f"  ❌ 失败: {len(failed)} 个指标")
    print(f"  📊 成功率: {len(successful)/len(results)*100:.1f}%")
    
    # 显示失败的指标
    if failed:
        print(f"\n❌ 失败的指标:")
        for r in failed:
            print(f"  - {r['indicator']} ({r['series_id']})")
    
    # 显示数据较新的指标
    print(f"\n🆕 数据较新的指标 (2024年后):")
    recent_indicators = []
    for r in successful:
        if r["latest_date"] >= "2024-01-01":
            recent_indicators.append(r)
    
    if recent_indicators:
        for r in recent_indicators:
            print(f"  ✅ {r['indicator']} ({r['series_id']}) - 最新: {r['latest_date']}")
    else:
        print("  📅 暂无2024年后的数据")
    
    return results

if __name__ == "__main__":
    results = test_updated_config()
    
    # 保存测试结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = BASE / "outputs" / "crisis_monitor" / f"config_test_{timestamp}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    import json
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n💾 测试结果已保存到: {output_file}")
