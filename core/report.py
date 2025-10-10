"""
报告生成器
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path


class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, base_dir: str):
        """
        初始化报告生成器
        
        Args:
            base_dir: 基础目录
        """
        self.base_dir = Path(base_dir)
        self.output_dir = self.base_dir / "outputs" / "daily_analysis"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_daily_report(self, result: Dict[str, Any], recent_scores: List[Dict[str, Any]]) -> str:
        """
        生成每日分析报告
        
        Args:
            result: 分析结果
            recent_scores: 最近评分历史
            
        Returns:
            报告文件路径
        """
        date_str = result['date'].strftime('%Y%m%d')
        
        # 生成HTML报告
        html_path = self.output_dir / f"daily_analysis_{date_str}.html"
        self._generate_html_report(result, recent_scores, html_path)
        
        # 生成JSON报告
        json_path = self.output_dir / f"daily_analysis_{date_str}.json"
        self._generate_json_report(result, recent_scores, json_path)
        
        return str(html_path)
    
    def _generate_html_report(self, result: Dict[str, Any], recent_scores: List[Dict[str, Any]], output_path: Path):
        """生成HTML报告"""
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>每日风险分析报告 - {result['date'].strftime('%Y-%m-%d')}</title>
    <style>
        body {{
            font-family: 'Microsoft YaHei', 'SimHei', Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #d32f2f;
            text-align: center;
            border-bottom: 3px solid #d32f2f;
            padding-bottom: 10px;
        }}
        .summary {{
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }}
        .risk-level {{
            font-size: 24px;
            font-weight: bold;
            text-align: center;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }}
        .risk-low {{ background-color: #d4edda; color: #155724; }}
        .risk-medium {{ background-color: #fff3cd; color: #856404; }}
        .risk-high {{ background-color: #f8d7da; color: #721c24; }}
        .risk-extreme {{ background-color: #d1ecf1; color: #0c5460; }}
        .factor-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .factor-card {{
            border: 1px solid #dee2e6;
            border-radius: 8px;
            padding: 15px;
            background-color: #f8f9fa;
        }}
        .factor-header {{
            font-weight: bold;
            font-size: 16px;
            margin-bottom: 10px;
            color: #495057;
        }}
        .factor-score {{
            font-size: 20px;
            font-weight: bold;
            text-align: center;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }}
        .score-low {{ background-color: #d4edda; color: #155724; }}
        .score-medium {{ background-color: #fff3cd; color: #856404; }}
        .score-high {{ background-color: #f8d7da; color: #721c24; }}
        .score-extreme {{ background-color: #d1ecf1; color: #0c5460; }}
        .trend-chart {{
            margin: 20px 0;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 8px;
        }}
        .footer {{
            text-align: center;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #dee2e6;
            color: #6c757d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>每日风险分析报告</h1>
        
        <div class="summary">
            <h2>分析概览</h2>
            <p><strong>分析日期:</strong> {result['date'].strftime('%Y-%m-%d')}</p>
            <p><strong>综合风险评分:</strong> {result['total_score']:.2f}/100</p>
            <p><strong>分析因子数量:</strong> {result['analysis_summary']['total_factors']}</p>
            <p><strong>活跃因子数量:</strong> {result['analysis_summary']['active_factors']}</p>
        </div>
        
        <div class="risk-level risk-{self._get_risk_class(result['risk_level'])}">
            风险等级: {result['risk_level']}
        </div>
        
        <h2>因子分析结果</h2>
        <div class="factor-grid">
"""
        
        # 添加因子卡片
        for factor_id, score in result['factor_scores'].items():
            factor_details = result['factor_details'].get(factor_id, {})
            if factor_details.get('status') == 'success':
                metrics = factor_details.get('metrics', {})
                current_value = metrics.get('current_value', 0)
                
                html_content += f"""
            <div class="factor-card">
                <div class="factor-header">{factor_id}</div>
                <div class="factor-score score-{self._get_score_class(score)}">{score:.1f}</div>
                <p><strong>当前值:</strong> {current_value:.4f}</p>
                <p><strong>数据点数:</strong> {factor_details.get('data_points', 0)}</p>
                <p><strong>最新日期:</strong> {factor_details.get('latest_date', 'N/A')}</p>
            </div>
"""
        
        html_content += """
        </div>
        
        <div class="trend-chart">
            <h2>最近趋势</h2>
            <p>最近5天的评分变化趋势将在这里显示</p>
        </div>
        
        <div class="footer">
            <p>报告生成时间: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
            <p>MacroLab 风险分析系统</p>
        </div>
    </div>
</body>
</html>
"""
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_json_report(self, result: Dict[str, Any], recent_scores: List[Dict[str, Any]], output_path: Path):
        """生成JSON报告"""
        # 准备JSON数据
        json_data = {
            'report_info': {
                'date': result['date'].strftime('%Y-%m-%d'),
                'generated_at': datetime.now().isoformat(),
                'total_score': result['total_score'],
                'risk_level': result['risk_level']
            },
            'factor_scores': result['factor_scores'],
            'factor_values': result['factor_values'],
            'factor_details': result['factor_details'],
            'analysis_summary': result['analysis_summary'],
            'recent_scores': recent_scores
        }
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
    
    def _get_risk_class(self, risk_level: str) -> str:
        """获取风险等级CSS类"""
        if '极低' in risk_level or '低' in risk_level:
            return 'low'
        elif '中' in risk_level:
            return 'medium'
        elif '高' in risk_level:
            return 'high'
        else:
            return 'extreme'
    
    def _get_score_class(self, score: float) -> str:
        """获取评分CSS类"""
        if score >= 80:
            return 'extreme'
        elif score >= 60:
            return 'high'
        elif score >= 40:
            return 'medium'
        else:
            return 'low'
    
    def generate_excel_summary(self, result: Dict[str, Any], excel_path: str):
        """生成Excel汇总报告"""
        try:
            # 准备数据
            data = []
            for factor_id, score in result['factor_scores'].items():
                factor_details = result['factor_details'].get(factor_id, {})
                metrics = factor_details.get('metrics', {})
                
                data.append({
                    '因子ID': factor_id,
                    '风险评分': score,
                    '当前值': metrics.get('current_value', 0),
                    '20日均值': metrics.get('mean_20d', 0),
                    '20日波动率': metrics.get('std_20d', 0),
                    '百分位排名': metrics.get('percentile_rank', 0),
                    '5日趋势': metrics.get('trend_5d', 0),
                    '数据点数': factor_details.get('data_points', 0),
                    '状态': factor_details.get('status', 'unknown')
                })
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 写入Excel
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # 主要数据
                df.to_excel(writer, sheet_name='因子分析', index=False)
                
                # 汇总信息
                summary_data = {
                    '项目': ['分析日期', '综合风险评分', '风险等级', '总因子数', '活跃因子数'],
                    '值': [
                        result['date'].strftime('%Y-%m-%d'),
                        f"{result['total_score']:.2f}",
                        result['risk_level'],
                        result['analysis_summary']['total_factors'],
                        result['analysis_summary']['active_factors']
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='汇总信息', index=False)
            
            print(f"✅ Excel报告已生成: {excel_path}")
            
        except Exception as e:
            print(f"❌ Excel报告生成失败: {e}")
            raise