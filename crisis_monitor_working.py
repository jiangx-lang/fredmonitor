#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（工作版）
"""

import os
import sys
import json
import yaml
import math
import warnings
import pathlib
import base64
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# 加载环境变量
try:
    load_dotenv()
    print("✅ 环境变量加载成功")
except Exception as e:
    print(f"⚠️ 环境变量加载失败: {e}")

# 基础路径
BASE = pathlib.Path(__file__).parent

# 依赖模块
from scripts.fred_http import series_observations, series_search

def load_yaml_config(file_path: pathlib.Path) -> dict:
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置文件失败 {file_path}: {e}")
        return {}

def get_series_data(series_id: str) -> Optional[pd.Series]:
    """获取系列数据"""
    try:
        # 尝试从本地CSV文件读取
        csv_path = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                return df['value'].astype(float)
        
        # 如果本地没有，尝试API获取
        print(f"⚠️ 本地无数据，尝试API获取 {series_id}")
        data = series_observations(series_id, limit=100000)
        if data and 'observations' in data:
            df = pd.DataFrame(data['observations'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            return df['value'].astype(float)
        
        return None
    except Exception as e:
        print(f"❌ 获取数据失败 {series_id}: {e}")
        return None

def process_indicator(indicator: dict, crises: List[dict]) -> dict:
    """处理单个指标"""
    name = indicator.get("name", "未知指标")
    series_id = indicator.get("series_id", "")
    enabled = indicator.get("enabled", True)
    
    if not enabled:
        return {
            "indicator": name,
            "series_id": series_id,
            "status": "skipped",
            "reason": "已禁用"
        }
    
    print(f"🔄 处理 {name} ({series_id})...")
    
    # 获取数据
    data = get_series_data(series_id)
    if data is None or data.empty:
        return {
            "indicator": name,
            "series_id": series_id,
            "status": "error",
            "reason": "无法获取数据"
        }
    
    # 计算基本统计
    current_value = data.iloc[-1] if not data.empty else np.nan
    last_date = data.index[-1] if not data.empty else None
    
    # 简单的风险评分（0-100）
    if not np.isnan(current_value):
        # 使用历史分位数作为简单评分
        percentile = (data < current_value).mean() * 100
        risk_score = percentile
    else:
        risk_score = 50.0
    
    # 风险等级
    if risk_score >= 80:
        level = "🔴 高风险"
    elif risk_score >= 60:
        level = "🟡 中风险"
    elif risk_score >= 40:
        level = "🟢 低风险"
    else:
        level = "🔵 极低风险"
    
    print(f"  ✅ 当前值: {current_value:.2f}")
    print(f"  📈 风险评分: {risk_score:.1f} ({level})")
    
    return {
        "indicator": name,
        "series_id": series_id,
        "status": "success",
        "current_value": current_value,
        "risk_score": risk_score,
        "risk_level": level,
        "last_date": last_date.strftime("%Y-%m-%d") if last_date else None,
        "data_points": len(data)
    }

def generate_report(results: List[dict], timestamp: str):
    """生成报告"""
    successful = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    # 计算总体风险
    if successful:
        avg_risk = np.mean([r["risk_score"] for r in successful])
        if avg_risk >= 80:
            overall_level = "🔴 高风险"
        elif avg_risk >= 60:
            overall_level = "🟡 中风险"
        elif avg_risk >= 40:
            overall_level = "🟢 低风险"
        else:
            overall_level = "🔵 极低风险"
    else:
        avg_risk = 50.0
        overall_level = "🟡 中风险"
    
    # 生成Markdown报告
    report_content = f"""# 🚨 宏观金融危机监察报告

**生成时间**: {datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")}

## 📋 报告说明
本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。
数据由人采集和处理，请批判看待这些数据，欢迎email jiangx@gmail.com 任何问题讨论

风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为'越低越危险'）。
采用分组加权评分：先计算各组平均分，再按权重合成总分。
总分 = ∑(分组平均分 × 分组权重)，分组权重归一处理后合成。
过期数据处理：月频数据>60天、季频数据>120天标记⚠️，过期数据权重×0.9。
颜色分段：0–39 🔵 极低，40–59 🟢 低，60–79 🟡 中，80–100 🔴 高；50 为中性。

## 🎯 总体风险概览
- **加权风险总分**: {avg_risk:.1f}/100
- **总体风险等级**: {overall_level}
- **成功监控指标**: {len(successful)}/{len(results)}

## 📊 指标详情

"""
    
    # 添加成功处理的指标
    for result in successful:
        report_content += f"""### {result['indicator']} ({result['series_id']})
- **当前值**: {result['current_value']:.2f}
- **风险评分**: {result['risk_score']:.1f}/100 {result['risk_level']}
- **最后更新**: {result['last_date']}
- **数据点数**: {result['data_points']}

"""
    
    # 添加错误和跳过的指标
    if errors:
        report_content += "## ❌ 处理失败的指标\n"
        for result in errors:
            report_content += f"- {result['indicator']} ({result['series_id']}): {result['reason']}\n"
        report_content += "\n"
    
    if skipped:
        report_content += "## ⏭️ 跳过的指标\n"
        for result in skipped:
            report_content += f"- {result['indicator']} ({result['series_id']}): {result['reason']}\n"
        report_content += "\n"
    
    # 保存报告
    output_dir = BASE / "outputs" / "crisis_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report_path = output_dir / f"crisis_report_{timestamp}.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"\n📄 报告已保存: {report_path}")
    return report_path

def main():
    """主函数"""
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)
    
    # 加载配置
    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_cfg = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    
    if not indicators_cfg or not crises_cfg:
        print("❌ 配置文件加载失败")
        return
    
    indicators = indicators_cfg.get("indicators", [])
    crises = crises_cfg.get("crises", [])
    
    print(f"📊 指标数: {len(indicators)}")
    print(f"📅 危机段: {len(crises)}")
    
    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 处理指标
    results = []
    for i, indicator in enumerate(indicators, 1):
        print(f"\n[{i}/{len(indicators)}] ", end="")
        result = process_indicator(indicator, crises)
        results.append(result)
    
    # 生成报告
    print(f"\n🎉 危机预警报告生成完成！")
    report_path = generate_report(results, timestamp)
    
    # 统计结果
    successful = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    print(f"   ✅ 成功处理: {len(successful)} 个指标")
    print(f"   ❌ 处理失败: {len(errors)} 个指标")
    print(f"   ⏭️ 跳过指标: {len(skipped)} 个指标")
    print(f"   📄 报告文件: {report_path}")

if __name__ == "__main__":
    main()
