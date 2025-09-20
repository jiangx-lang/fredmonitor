#!/usr/bin/env python3
"""
FRED 危机预警监控系统
基于用户提供的框架和我们已有的FRED数据

功能：
1. 读取指标配置和危机期间配置
2. 从本地FRED数据或API获取数据
3. 计算变换和危机基准
4. 比较当前值与历史危机期基准
5. 生成CSV和中文Markdown报告
"""

import os
import sys
import math
import yaml
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import pathlib

# 添加项目根目录到路径
BASE = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

from scripts.fred_http import series_info, series_observations

def load_yaml_config(path: str) -> dict:
    """加载YAML配置文件"""
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_local_series_data(series_id: str) -> Optional[pd.Series]:
    """从本地FRED数据加载系列数据"""
    series_path = BASE / "data" / "fred" / "categories"
    
    # 在所有分类中搜索该系列
    for category_dir in series_path.iterdir():
        if category_dir.is_dir():
            series_dir = category_dir / "series" / series_id
            raw_file = series_dir / "raw.csv"
            if raw_file.exists():
                try:
                    df = pd.read_csv(raw_file, index_col=0, parse_dates=True)
                    if 'value' in df.columns:
                        s = df['value'].dropna()
                        s.index = pd.to_datetime(s.index)
                        s = s.sort_index()
                        return s
                except Exception as e:
                    print(f"❌ 读取本地数据失败 {series_id}: {e}")
                    return None
    return None

def fetch_series_from_api(series_id: str) -> Optional[pd.Series]:
    """从FRED API获取系列数据"""
    try:
        obs_data = series_observations(series_id, limit=10000)
        if not obs_data or 'observations' not in obs_data:
            return None
        
        # 转换为DataFrame
        observations_list = obs_data['observations']
        if not observations_list:
            return None
        
        # 创建DataFrame
        df_data = []
        for obs in observations_list:
            if obs['value'] != '.':
                df_data.append({
                    'date': obs['date'],
                    'value': float(obs['value'])
                })
        
        if not df_data:
            return None
        
        df = pd.DataFrame(df_data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        s = df['value'].dropna()
        s.index = pd.to_datetime(s.index)
        s = s.sort_index()
        return s
        
    except Exception as e:
        print(f"❌ API获取失败 {series_id}: {e}")
        return None

def get_series_data(series_id: str) -> Optional[pd.Series]:
    """获取系列数据，优先使用本地数据"""
    # 先尝试本地数据
    local_data = load_local_series_data(series_id)
    if local_data is not None and not local_data.empty:
        return local_data
    
    # 如果本地没有，尝试API
    print(f"⚠️ 本地无数据，尝试API获取 {series_id}")
    return fetch_series_from_api(series_id)


def slice_crisis_window(s: pd.Series, start: str, end: str) -> pd.Series:
    """切片危机期间数据"""
    try:
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        return s.loc[start_date:end_date].dropna()
    except Exception:
        return pd.Series(dtype=float)

def calculate_crisis_stats(s: pd.Series, crises: List[dict]) -> Dict[str, float]:
    """计算危机期间统计量"""
    crisis_values = []
    
    for crisis in crises:
        window_data = slice_crisis_window(s, crisis['start'], crisis['end'])
        if not window_data.empty:
            crisis_values.extend(window_data.values)
    
    if not crisis_values:
        return {
            'crisis_median': np.nan,
            'crisis_p25': np.nan,
            'crisis_p75': np.nan,
            'crisis_mean': np.nan,
            'crisis_std': np.nan
        }
    
    crisis_array = np.array(crisis_values)
    crisis_array = crisis_array[~np.isnan(crisis_array)]
    
    if len(crisis_array) == 0:
        return {
            'crisis_median': np.nan,
            'crisis_p25': np.nan,
            'crisis_p75': np.nan,
            'crisis_mean': np.nan,
            'crisis_std': np.nan
        }
    
    return {
        'crisis_median': float(np.nanmedian(crisis_array)),
        'crisis_p25': float(np.nanpercentile(crisis_array, 25)),
        'crisis_p75': float(np.nanpercentile(crisis_array, 75)),
        'crisis_mean': float(np.nanmean(crisis_array)),
        'crisis_std': float(np.nanstd(crisis_array, ddof=1))
    }

def calculate_zscore(value: float, mean: float, std: float) -> float:
    """计算Z分数"""
    if std is None or std == 0 or np.isnan(std) or np.isnan(mean):
        return np.nan
    return (value - mean) / std

def calculate_risk_score(deviation: float, higher_is_risk: bool, benchmark_type: str) -> float:
    """计算风险评分 (0-100)"""
    if np.isnan(deviation):
        return 50.0  # 中性分数
    
    # 根据基准类型调整权重
    weight_multiplier = {
        'crisis_median': 1.0,
        'crisis_p25': 1.2,
        'crisis_p75': 1.2,
        'crisis_mean': 1.0
    }.get(benchmark_type, 1.0)
    
    # 计算风险分数
    if higher_is_risk:
        # 正值表示更危险
        risk_score = 50 + (deviation * weight_multiplier * 10)
    else:
        # 负值表示更危险
        risk_score = 50 - (deviation * weight_multiplier * 10)
    
    # 限制在0-100范围内
    return max(0, min(100, risk_score))

def generate_crisis_report():
    """生成危机预警报告"""
    
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)
    
    # 加载配置
    indicators_config = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_config = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    
    indicators = indicators_config["indicators"]
    crises = crises_config["crises"]
    
    print(f"📊 加载了 {len(indicators)} 个指标")
    print(f"📅 加载了 {len(crises)} 个历史危机期间")
    
    # 处理每个指标
    results = []
    series_cache = {}
    
    for i, indicator in enumerate(indicators, 1):
        name = indicator["name"]
        series_id = indicator["series_id"]
        transform = indicator.get("transform", "level")
        higher_is_risk = bool(indicator.get("higher_is_risk", True))
        compare_to = indicator.get("compare_to", "crisis_median")
        unit_hint = indicator.get("unit_hint", "")
        explainer = indicator.get("plain_explainer", "")
        
        print(f"\n[{i}/{len(indicators)}] 处理 {name} ({series_id})...")
        
        try:
            # 获取数据
            if series_id not in series_cache:
                s = get_series_data(series_id)
                if s is None or s.empty:
                    results.append({
                        'indicator': name,
                        'series_id': series_id,
                        'status': 'error',
                        'error_message': '无法获取数据',
                        'current_value': np.nan,
                        'benchmark_value': np.nan,
                        'deviation': np.nan,
                        'risk_score': 50.0,
                        'zscore': np.nan,
                        'unit': unit_hint,
                        'explanation': explainer
                    })
                    continue
                
                # 转换为月度数据
                s = to_monthly(s)
                series_cache[series_id] = s
            else:
                s = series_cache[series_id]
            
            # 应用变换
            ts = transform_series(s, transform).dropna()
            
            if ts.empty:
                results.append({
                    'indicator': name,
                    'series_id': series_id,
                    'status': 'error',
                    'error_message': '变换后无数据',
                    'current_value': np.nan,
                    'benchmark_value': np.nan,
                    'deviation': np.nan,
                    'risk_score': 50.0,
                    'zscore': np.nan,
                    'unit': unit_hint,
                    'explanation': explainer
                })
                continue
            
            # 当前值
            current_date = ts.index[-1]
            current_value = float(ts.iloc[-1])
            
            # 计算危机基准
            crisis_stats = calculate_crisis_stats(ts, crises)
            benchmark_value = crisis_stats.get(compare_to, np.nan)
            
            # 计算历史统计
            hist_mean = np.nanmean(ts.values)
            hist_std = np.nanstd(ts.values, ddof=1)
            current_zscore = calculate_zscore(current_value, hist_mean, hist_std)
            
            # 计算偏离度
            if np.isnan(benchmark_value):
                deviation = np.nan
            else:
                deviation = current_value - benchmark_value
                if not higher_is_risk:
                    deviation = -deviation  # 反向处理
            
            # 计算风险评分
            risk_score = calculate_risk_score(deviation, higher_is_risk, compare_to)
            
            # 确定风险等级
            if risk_score >= 80:
                risk_level = "🔴 高风险"
            elif risk_score >= 60:
                risk_level = "🟡 中风险"
            elif risk_score >= 40:
                risk_level = "🟢 低风险"
            else:
                risk_level = "🔵 极低风险"
            
            results.append({
                'indicator': name,
                'series_id': series_id,
                'status': 'success',
                'last_observation': current_date.strftime('%Y-%m-%d'),
                'current_value': round(current_value, 4),
                'benchmark_type': compare_to,
                'benchmark_value': round(benchmark_value, 4) if not np.isnan(benchmark_value) else None,
                'deviation': round(deviation, 4) if not np.isnan(deviation) else None,
                'risk_score': round(risk_score, 1),
                'risk_level': risk_level,
                'zscore': round(current_zscore, 2) if not np.isnan(current_zscore) else None,
                'unit': unit_hint,
                'higher_is_risk': higher_is_risk,
                'explanation': explainer,
                'data_points': len(ts),
                'crisis_periods_used': len([c for c in crises if not slice_crisis_window(ts, c['start'], c['end']).empty])
            })
            
            print(f"  ✅ 当前值: {current_value:.4f} {unit_hint}")
            print(f"  📊 基准值: {benchmark_value:.4f} ({compare_to})")
            print(f"  📈 风险评分: {risk_score:.1f} ({risk_level})")
            
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            results.append({
                'indicator': name,
                'series_id': series_id,
                'status': 'error',
                'error_message': str(e),
                'current_value': np.nan,
                'benchmark_value': np.nan,
                'deviation': np.nan,
                'risk_score': 50.0,
                'zscore': np.nan,
                'unit': unit_hint,
                'explanation': explainer
            })
    
    # 生成报告
    generate_outputs(results, crises)
    
    return results

def generate_outputs(results: List[dict], crises: List[dict]):
    """生成输出文件"""
    
    # 创建输出目录
    output_dir = BASE / "outputs" / "crisis_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 生成CSV报告
    csv_file = output_dir / f"crisis_report_{timestamp}.csv"
    df_results = pd.DataFrame(results)
    df_results.to_csv(csv_file, index=False, encoding='utf-8-sig')
    
    # 生成Markdown报告
    md_file = output_dir / f"crisis_report_{timestamp}.md"
    generate_markdown_report(results, crises, md_file)
    
    # 生成JSON报告
    json_file = output_dir / f"crisis_report_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n📄 报告已生成:")
    print(f"  📊 CSV: {csv_file}")
    print(f"  📝 Markdown: {md_file}")
    print(f"  📋 JSON: {json_file}")

def generate_markdown_report(results: List[dict], crises: List[dict], output_file: pathlib.Path):
    """生成Markdown格式的危机预警报告"""
    
    lines = []
    
    # 报告头部
    lines.append("# 🚨 宏观金融危机监察报告")
    lines.append("")
    lines.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    lines.append("")
    lines.append("## 📋 报告说明")
    lines.append("")
    lines.append("本报告基于FRED数据库的宏观经济指标，通过比较当前数值与历史危机期间的基准值，")
    lines.append("评估当前金融市场的风险水平。每个指标的风险评分范围为0-100分：")
    lines.append("")
    lines.append("- 🔴 **高风险** (80-100分): 当前值接近或超过历史危机水平")
    lines.append("- 🟡 **中风险** (60-79分): 当前值高于历史正常水平")
    lines.append("- 🟢 **低风险** (40-59分): 当前值处于历史正常范围")
    lines.append("- 🔵 **极低风险** (0-39分): 当前值低于历史正常水平")
    lines.append("")
    
    # 总体风险概览
    successful_results = [r for r in results if r['status'] == 'success']
    if successful_results:
        avg_risk_score = np.mean([r['risk_score'] for r in successful_results])
        high_risk_count = len([r for r in successful_results if r['risk_score'] >= 80])
        medium_risk_count = len([r for r in successful_results if 60 <= r['risk_score'] < 80])
        
        lines.append("## 🎯 总体风险概览")
        lines.append("")
        lines.append(f"- **平均风险评分**: {avg_risk_score:.1f}/100")
        lines.append(f"- **高风险指标数量**: {high_risk_count}")
        lines.append(f"- **中风险指标数量**: {medium_risk_count}")
        lines.append(f"- **成功监控指标**: {len(successful_results)}/{len(results)}")
        lines.append("")
        
        # 风险等级分布
        if avg_risk_score >= 80:
            overall_risk = "🔴 高风险"
        elif avg_risk_score >= 60:
            overall_risk = "🟡 中风险"
        elif avg_risk_score >= 40:
            overall_risk = "🟢 低风险"
        else:
            overall_risk = "🔵 极低风险"
        
        lines.append(f"**总体风险等级**: {overall_risk}")
        lines.append("")
    
    # 高风险指标
    high_risk_indicators = [r for r in successful_results if r['risk_score'] >= 80]
    if high_risk_indicators:
        lines.append("## 🔴 高风险指标")
        lines.append("")
        for result in sorted(high_risk_indicators, key=lambda x: x['risk_score'], reverse=True):
            lines.append(f"### {result['indicator']}")
            lines.append("")
            lines.append(f"- **当前值**: {result['current_value']} {result['unit']}")
            lines.append(f"- **基准值**: {result['benchmark_value']} ({result['benchmark_type']})")
            lines.append(f"- **风险评分**: {result['risk_score']:.1f}/100")
            lines.append(f"- **偏离度**: {result['deviation']}")
            lines.append(f"- **历史Z分数**: {result['zscore']}")
            lines.append(f"- **解释**: {result['explanation']}")
            lines.append("")
    
    # 中风险指标
    medium_risk_indicators = [r for r in successful_results if 60 <= r['risk_score'] < 80]
    if medium_risk_indicators:
        lines.append("## 🟡 中风险指标")
        lines.append("")
        for result in sorted(medium_risk_indicators, key=lambda x: x['risk_score'], reverse=True):
            lines.append(f"### {result['indicator']}")
            lines.append("")
            lines.append(f"- **当前值**: {result['current_value']} {result['unit']}")
            lines.append(f"- **基准值**: {result['benchmark_value']} ({result['benchmark_type']})")
            lines.append(f"- **风险评分**: {result['risk_score']:.1f}/100")
            lines.append(f"- **偏离度**: {result['deviation']}")
            lines.append(f"- **历史Z分数**: {result['zscore']}")
            lines.append(f"- **解释**: {result['explanation']}")
            lines.append("")
    
    # 低风险指标
    low_risk_indicators = [r for r in successful_results if r['risk_score'] < 60]
    if low_risk_indicators:
        lines.append("## 🟢 低风险指标")
        lines.append("")
        for result in sorted(low_risk_indicators, key=lambda x: x['risk_score']):
            lines.append(f"### {result['indicator']}")
            lines.append("")
            lines.append(f"- **当前值**: {result['current_value']} {result['unit']}")
            lines.append(f"- **基准值**: {result['benchmark_value']} ({result['benchmark_type']})")
            lines.append(f"- **风险评分**: {result['risk_score']:.1f}/100")
            lines.append(f"- **偏离度**: {result['deviation']}")
            lines.append(f"- **历史Z分数**: {result['zscore']}")
            lines.append(f"- **解释**: {result['explanation']}")
            lines.append("")
    
    # 错误指标
    error_results = [r for r in results if r['status'] == 'error']
    if error_results:
        lines.append("## ❌ 数据获取失败")
        lines.append("")
        for result in error_results:
            lines.append(f"- **{result['indicator']}** ({result['series_id']}): {result['error_message']}")
        lines.append("")
    
    # 历史危机期间参考
    lines.append("## 📅 历史危机期间参考")
    lines.append("")
    lines.append("本报告基于以下历史危机期间计算基准值：")
    lines.append("")
    for crisis in crises:
        lines.append(f"- **{crisis['name']}** ({crisis['code']}): {crisis['start']} 至 {crisis['end']}")
        if 'description' in crisis:
            lines.append(f"  - {crisis['description']}")
    lines.append("")
    
    # 免责声明
    lines.append("## ⚠️ 免责声明")
    lines.append("")
    lines.append("本报告仅供参考，不构成投资建议。金融市场存在不确定性，")
    lines.append("历史数据不能保证未来表现。请结合其他信息进行综合判断。")
    lines.append("")
    
    # 写入文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

if __name__ == "__main__":
    results = generate_crisis_report()
    print(f"\n🎉 危机预警报告生成完成！")
    print(f"📊 成功处理 {len([r for r in results if r['status'] == 'success'])} 个指标")
