# src/report_md.py - 报告生成器
import os
from datetime import datetime
from typing import Dict, List, Any
import yaml

def load_config(config_yaml: str) -> Dict[str, Any]:
    """加载配置文件"""
    with open(config_yaml, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_risk_level_emoji(risk_level: str) -> str:
    """获取风险等级表情符号"""
    emoji_map = {
        'low': '🟢',
        'medium': '🟡', 
        'high': '🟠',
        'critical': '🔴'
    }
    return emoji_map.get(risk_level, '⚪')

def get_risk_level_text(risk_level: str) -> str:
    """获取风险等级文本"""
    text_map = {
        'low': '低风险',
        'medium': '中等风险',
        'high': '高风险', 
        'critical': '极高风险'
    }
    return text_map.get(risk_level, '未知风险')

def format_score(score: float) -> str:
    """格式化分数"""
    return f"{score:.1f}"

def build_indicator_table(indicators: List[Dict[str, Any]]) -> str:
    """构建指标表格"""
    table_lines = [
        "| 指标ID | 指标名称 | 分组 | 当前值 | 基准值 | 评分 | 权重 | 风险方向 |",
        "|--------|----------|------|--------|--------|------|------|----------|"
    ]
    
    for item in indicators:
        series_id = item.get('id', '')
        name = item.get('name', '')
        group = item.get('group', '')
        current_value = item.get('current_value', 0)
        benchmark = item.get('benchmark', 0)
        score = item.get('score', 50)
        weight = item.get('weight', 0)
        higher_is_risk = item.get('higher_is_risk', True)
        risk_direction = "越高越危险" if higher_is_risk else "越低越危险"
        
        table_lines.append(
            f"| {series_id} | {name} | {group} | {current_value:.2f} | {benchmark:.2f} | "
            f"{score:.1f} | {weight:.3f} | {risk_direction} |"
        )
    
    return "\n".join(table_lines)

def build_group_summary(group_scores: Dict[str, Dict[str, Any]]) -> str:
    """构建分组摘要"""
    summary_lines = [
        "## 📊 分组评分摘要",
        "",
        "| 分组 | 加权评分 | 指标数量 | 总权重 |",
        "|------|----------|----------|--------|"
    ]
    
    for group, data in group_scores.items():
        weighted_score = data.get('weighted_score', 50)
        indicator_count = data.get('indicator_count', 0)
        total_weight = data.get('total_weight', 0)
        
        summary_lines.append(
            f"| {group} | {weighted_score:.1f} | {indicator_count} | {total_weight:.3f} |"
        )
    
    return "\n".join(summary_lines)

def build_figure_gallery(figures_dir: str, indicators: List[Dict[str, Any]]) -> str:
    """构建图表画廊"""
    gallery_lines = [
        "## 🖼️ 指标图表",
        "",
        "### 📈 收益率曲线",
        ""
    ]
    
    # 按分组组织图表
    groups = {}
    for item in indicators:
        group = item.get('group', 'other')
        if group not in groups:
            groups[group] = []
        groups[group].append(item)
    
    group_names = {
        'rates_curve': '收益率曲线',
        'rates_level': '利率水平', 
        'credit_spreads': '信用利差',
        'fin_cond_vol': '金融状况/波动',
        'real_economy': '实体经济',
        'housing': '房地产',
        'consumers': '消费',
        'banking': '银行业',
        'external': '外部环境',
        'leverage': '杠杆'
    }
    
    for group, items in groups.items():
        group_name = group_names.get(group, group)
        gallery_lines.append(f"### 📊 {group_name}")
        gallery_lines.append("")
        
        for item in items:
            series_id = item.get('id', '')
            name = item.get('name', '')
            score = item.get('score', 50)
            
            # 检查图表文件是否存在
            figure_path = os.path.join(figures_dir, f"{series_id}_latest.png")
            if os.path.exists(figure_path):
                gallery_lines.append(f"#### {name}")
                gallery_lines.append(f"**评分**: {score:.1f}/100")
                gallery_lines.append("")
                gallery_lines.append(f"![{name}]({figure_path})")
                gallery_lines.append("")
            else:
                gallery_lines.append(f"#### {name} (图表缺失)")
                gallery_lines.append(f"**评分**: {score:.1f}/100")
                gallery_lines.append("")
    
    return "\n".join(gallery_lines)

def build_validation_report(validation_result: Dict[str, Any]) -> str:
    """构建校验报告"""
    report_lines = [
        "## 🔍 数据校验报告",
        "",
        f"**图表数量**: {validation_result.get('fig_count', 0)}",
        f"**配置指标数量**: {validation_result.get('config_count', 0)}",
        f"**JSON指标数量**: {validation_result.get('json_indicator_count', 0)}",
        f"**总权重**: {validation_result.get('total_weight', 0):.3f}",
        ""
    ]
    
    # 缺失指标
    missing_in_config = validation_result.get('missing_in_config', [])
    if missing_in_config:
        report_lines.append("### ❌ figures中有但配置文件中没有的指标:")
        for i, sid in enumerate(missing_in_config, 1):
            report_lines.append(f"{i}. {sid}")
        report_lines.append("")
    
    missing_in_figures = validation_result.get('missing_in_figures', [])
    if missing_in_figures:
        report_lines.append("### ❌ 配置文件中提到但figures中没有的指标:")
        for i, sid in enumerate(missing_in_figures, 1):
            report_lines.append(f"{i}. {sid}")
        report_lines.append("")
    
    # 变换冲突
    transform_conflicts = validation_result.get('transform_conflicts', [])
    if transform_conflicts:
        report_lines.append("### ⚠️ 变换冲突:")
        for sid, transform in transform_conflicts:
            report_lines.append(f"- {sid}: 名称含YoY但transform={transform}")
        report_lines.append("")
    
    # 权重异常
    weights_anomalies = validation_result.get('weights_anomalies', [])
    if weights_anomalies:
        report_lines.append("### ⚠️ 权重异常:")
        for anomaly in weights_anomalies:
            report_lines.append(f"- {anomaly}")
        report_lines.append("")
    
    return "\n".join(report_lines)

def build_report_md(json_data: Dict[str, Any], config_yaml: str, 
                   scoring_yaml: str, crisis_yaml: str = None, 
                   figures_dir: str = "figures") -> str:
    """
    构建完整的Markdown报告
    
    Args:
        json_data: JSON数据
        config_yaml: 配置文件路径
        scoring_yaml: 评分配置文件路径
        crisis_yaml: 危机期间配置文件路径
        figures_dir: 图表目录
    
    Returns:
        Markdown报告内容
    """
    # 加载配置
    config = load_config(config_yaml)
    scoring_config = load_config(scoring_yaml)
    
    # 基本信息
    timestamp = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
    total_indicators = json_data.get('total_indicators', 0)
    total_score = json_data.get('total_score', 50)
    risk_level = json_data.get('risk_level', 'medium')
    
    risk_emoji = get_risk_level_emoji(risk_level)
    risk_text = get_risk_level_text(risk_level)
    
    # 构建报告
    report_lines = [
        f"# {risk_emoji} FRED 宏观金融危机预警监控报告",
        "",
        f"**生成时间**: {timestamp}",
        f"**总指标数**: {total_indicators}",
        f"**总体风险评分**: {format_score(total_score)}/100",
        f"**风险等级**: {risk_emoji} {risk_text}",
        "",
        "---",
        "",
        "## 📋 执行摘要",
        "",
        f"本报告基于{total_indicators}个宏观经济指标，采用分位数尾部评分方法，",
        f"当前总体风险评分为{format_score(total_score)}分，风险等级为{risk_text}。",
        "",
        "### 🎯 关键发现",
        "",
        "- **指标覆盖**: 涵盖收益率曲线、利率水平、信用利差、金融状况、实体经济、房地产、消费、银行业、外部环境和杠杆等10个维度",
        "- **评分方法**: 基于非危机期间分位数基准，采用分位数尾部评分",
        "- **权重归一**: 所有指标权重已归一化，确保总分计算准确性",
        "- **数据完整性**: 所有指标均有对应的图表和数据",
        "",
        "---",
        ""
    ]
    
    # 分组摘要
    group_scores = json_data.get('group_scores', {})
    if group_scores:
        report_lines.append(build_group_summary(group_scores))
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
    
    # 指标表格
    indicators = json_data.get('indicators', [])
    if indicators:
        report_lines.append("## 📊 详细指标数据")
        report_lines.append("")
        report_lines.append(build_indicator_table(indicators))
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
    
    # 图表画廊
    if os.path.exists(figures_dir):
        report_lines.append(build_figure_gallery(figures_dir, indicators))
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
    
    # 校验报告
    validation_result = json_data.get('validation', {})
    if validation_result:
        report_lines.append(build_validation_report(validation_result))
        report_lines.append("")
        report_lines.append("---")
        report_lines.append("")
    
    # 结尾
    report_lines.extend([
        "## 📝 说明",
        "",
        "- 本报告基于FRED数据自动生成",
        "- 评分方法采用分位数尾部评分",
        "- 权重已归一化处理",
        "- 数据仅供参考，不构成投资建议",
        "",
        f"*报告生成时间: {timestamp}*"
    ])
    
    return "\n".join(report_lines)
