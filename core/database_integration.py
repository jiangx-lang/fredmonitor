#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库集成模块
将日度风险面板与现有crisis_monitor数据库集成
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pathlib
import yaml

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())


class DatabaseIntegration:
    """数据库集成器"""
    
    def __init__(self, data_dir: str = "data/series"):
        """
        初始化数据库集成器
        
        Args:
            data_dir: 数据目录路径
        """
        self.data_dir = pathlib.Path(data_dir)
        with open("config/indicators.yaml", "r", encoding="utf-8") as f:
            self.indicators_config = yaml.safe_load(f)
        with open("config/crisis_periods.yaml", "r", encoding="utf-8") as f:
            self.crisis_periods = yaml.safe_load(f)
        
        # 因子到指标的映射（优化后，移除重复因子）
        self.factor_mapping = {
            'VIX_RISK': 'VIXCLS',           # 市场波动率
            'YIELD_CURVE': 'T10Y2Y',        # 收益率曲线
            'CREDIT_SPREAD': 'BAMLH0A0HYM2', # 信用利差
            'DXY_VOL': 'DTWEXBGS',          # 美元指数
            'HOUSING_STRESS': 'HOUST',      # 房地产压力
            'NFCI': 'NFCI',                 # 金融状况指数
            'TED_SPREAD': 'TEDRATE',         # TED利差
            'UMICH_CONF': 'UMCSENT',        # 消费者信心
            'PAYEMS': 'PAYEMS',             # 非农就业
            'INDPRO': 'INDPRO',             # 工业生产
            'GDP': 'GDP',                   # GDP
            'FEDFUNDS': 'FEDFUNDS',         # 联邦基金利率
            'DGS10': 'DGS10',               # 10年期国债
            'MORTGAGE30US': 'MORTGAGE30US',  # 30年期按揭利率
            'SOFR': 'SOFR',                 # SOFR利率
            'DTB3': 'DTB3',                 # 3个月国债
            'CPN3M': 'CPN3M',               # 3个月商业票据
            'WALCL': 'WALCL',               # 美联储总资产
            'TOTLL': 'TOTLL',               # 总贷款
            'TOTALSA': 'TOTALSA',           # 消费者信贷
            'TDSP': 'TDSP',                 # 家庭债务偿付比率
            'TOTRESNS': 'TOTRESNS',         # 银行准备金
            'NCBDBIQ027S': 'NCBDBIQ027S',   # 企业债/GDP
            'CSUSHPINSA': 'CSUSHPINSA',     # 房价指数
            'NEWORDER': 'NEWORDER',         # 新订单
            'AWHMAN': 'AWHMAN',             # 制造业工时
            'PERMIT': 'PERMIT',             # 建筑许可
            'IC4WSA': 'IC4WSA',             # 初请失业金
            'MANEMP': 'MANEMP',             # 制造业就业
            'HOUST': 'HOUST',               # 新屋开工
            'STLFSI3': 'STLFSI3',           # 圣路易斯金融压力
            'DRSFRMACBS': 'DRSFRMACBS'      # 房贷违约率
        }
        
        # YoY指标列表
        self.yoy_indicators = [
            'PAYEMS', 'INDPRO', 'GDP', 'NEWORDER', 'CSUSHPINSA', 
            'TOTALSA', 'TOTLL', 'MANEMP', 'WALCL', 'DTWEXBGS', 
            'PERMIT', 'TOTRESNS'
        ]
    
    def get_factor_data(self, factor_id: str, target_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        获取因子数据（优先使用sync_fred_http.py下载的数据）
        
        Args:
            factor_id: 因子ID
            target_date: 目标日期
            
        Returns:
            数据DataFrame或None
        """
        # 1. 映射到指标ID
        series_id = self.factor_mapping.get(factor_id, factor_id)
        
        # 2. 优先使用sync_fred_http.py下载的数据
        fred_data_file = pathlib.Path("data/fred/series") / series_id / "raw.csv"
        if fred_data_file.exists():
            try:
                df = pd.read_csv(fred_data_file)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                
                # 处理不同的列名格式
                if 'value' in df.columns:
                    value_col = 'value'
                elif series_id in df.columns:
                    value_col = series_id
                else:
                    # 使用第一个非日期列作为值列
                    value_col = df.columns[0]
                
                print(f"📊 {series_id}: 使用sync_fred_http数据 ({len(df)} 条记录, 列: {value_col})")
                return df[[value_col]].rename(columns={value_col: 'value'})
            except Exception as e:
                print(f"⚠️ sync_fred_http数据读取失败 {series_id}: {e}")
        
        # 3. 回退到YoY指标
        if series_id in self.yoy_indicators:
            yoy_file = self.data_dir / f"{series_id}_YOY.csv"
            if yoy_file.exists():
                try:
                    df = pd.read_csv(yoy_file)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    print(f"📊 {series_id}: 使用YoY预计算数据")
                    return df[['yoy_pct']].rename(columns={'yoy_pct': 'value'})
                except Exception as e:
                    print(f"⚠️ YoY数据读取失败 {series_id}: {e}")
        
        # 4. 回退到特殊预计算数据
        if series_id == 'NCBDBIQ027S':
            ratio_file = self.data_dir / "CORPORATE_DEBT_GDP_RATIO.csv"
            if ratio_file.exists():
                try:
                    df = pd.read_csv(ratio_file)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    print(f"📊 {series_id}: 使用企业债/GDP预计算数据")
                    return df[['value']]
                except Exception as e:
                    print(f"⚠️ 企业债/GDP数据读取失败: {e}")
        
        # 5. 最后回退到原始数据
        data_file = self.data_dir / f"{series_id}.csv"
        if data_file.exists():
            try:
                df = pd.read_csv(data_file)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                print(f"📊 {series_id}: 使用原始数据")
                return df[['value']]
            except Exception as e:
                print(f"⚠️ 原始数据读取失败 {series_id}: {e}")
        
        return None
    
    def calculate_factor_score(self, factor_id: str, df: pd.DataFrame, 
                             target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        计算因子评分（使用crisis_monitor的评分逻辑）
        
        Args:
            factor_id: 因子ID
            df: 数据DataFrame
            target_date: 目标日期
            
        Returns:
            评分结果字典
        """
        if df.empty:
            return {
                'score': 0,
                'current_value': 0,
                'benchmark_value': 0,
                'status': 'no_data'
            }
        
        # 获取指标配置
        series_id = self.factor_mapping.get(factor_id, factor_id)
        indicator_config = self._get_indicator_config(series_id)
        
        if not indicator_config:
            return {
                'score': 0,
                'current_value': 0,
                'benchmark_value': 0,
                'status': 'no_config'
            }
        
        # 计算当前值
        current_value = float(df['value'].iloc[-1])
        current_date = df.index[-1]
        
        # 计算基准值（使用crisis_monitor的逻辑）
        benchmark_value = self._calculate_benchmark(df, indicator_config)
        
        # 计算风险评分
        risk_score = self._calculate_risk_score(
            current_value, benchmark_value, indicator_config, df
        )
        
        return {
            'score': risk_score,
            'current_value': current_value,
            'benchmark_value': benchmark_value,
            'current_date': current_date,
            'status': 'success',
            'data_points': len(df),
            'indicator_config': indicator_config
        }
    
    def _get_indicator_config(self, series_id: str) -> Optional[Dict[str, Any]]:
        """获取指标配置"""
        for indicator in self.indicators_config.get('indicators', []):
            if indicator.get('id') == series_id:
                return indicator
        return None
    
    def _calculate_benchmark(self, df: pd.DataFrame, indicator_config: Dict[str, Any]) -> float:
        """计算基准值"""
        compare_to = indicator_config.get('compare_to', 'noncrisis_p75')
        
        # 这里需要实现crisis_monitor的基准值计算逻辑
        # 简化版本：使用分位数
        if 'noncrisis' in compare_to:
            # 非危机期间的分位数
            return float(df['value'].quantile(0.75))
        elif 'crisis' in compare_to:
            # 危机期间的分位数
            return float(df['value'].quantile(0.25))
        else:
            return float(df['value'].median())
    
    def _calculate_risk_score(self, current_value: float, benchmark_value: float,
                            indicator_config: Dict[str, Any], df: pd.DataFrame) -> float:
        """计算风险评分（优化版本）"""
        higher_is_risk = indicator_config.get('higher_is_risk', True)
        series_id = indicator_config.get('id', '')
        
        # 特殊处理收益率曲线（调整阈值）
        if series_id == 'T10Y2Y':
            # 收益率曲线：倒挂风险阈值调整
            if current_value <= 0.1:  # 接近倒挂
                return min(100, (0.1 - current_value) * 1000)  # 更温和的评分
            elif current_value <= 0.5:  # 利差收窄
                return min(50, (0.5 - current_value) * 100)
            else:
                return 0
        
        # 特殊处理VIX（调整阈值）
        elif series_id == 'VIXCLS':
            if current_value >= 30:  # 恐慌水平
                return min(100, (current_value - 30) * 2)
            elif current_value >= 20:  # 紧张水平
                return min(50, (current_value - 20) * 5)
            else:
                return 0
        
        # 特殊处理消费者信心（调整阈值）
        elif series_id == 'UMCSENT':
            if current_value <= 50:  # 极低信心
                return min(100, (50 - current_value) * 2)
            elif current_value <= 70:  # 低信心
                return min(50, (70 - current_value) * 1.5)
            else:
                return 0
        
        # 通用评分逻辑
        if higher_is_risk:
            # 值越高风险越大
            if current_value >= benchmark_value:
                risk_score = min(100, (current_value / benchmark_value - 1) * 100)
            else:
                risk_score = max(0, (current_value / benchmark_value - 1) * 50)
        else:
            # 值越低风险越大
            if current_value <= benchmark_value:
                risk_score = min(100, (benchmark_value / current_value - 1) * 100)
            else:
                risk_score = max(0, (benchmark_value / current_value - 1) * 50)
        
        return max(0, min(100, risk_score))
    
    def run_daily_analysis_with_database(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        使用现有数据库运行每日分析
        
        Args:
            target_date: 目标日期
            
        Returns:
            分析结果
        """
        if target_date is None:
            target_date = datetime.now()
        
        print(f"📊 开始每日分析（使用现有数据库）: {target_date.strftime('%Y-%m-%d')}")
        
        factor_scores = {}
        factor_values = {}
        factor_details = {}
        
        # 遍历所有因子
        for factor_id in self.factor_mapping.keys():
            try:
                print(f"  处理因子: {factor_id}")
                
                # 获取数据
                df = self.get_factor_data(factor_id, target_date)
                
                if df is None or df.empty:
                    print(f"    ⚠️ {factor_id} 无可用数据")
                    factor_scores[factor_id] = 0
                    factor_values[factor_id] = 0
                    factor_details[factor_id] = {
                        'status': 'no_data',
                        'message': '无可用数据'
                    }
                    continue
                
                # 计算评分
                result = self.calculate_factor_score(factor_id, df, target_date)
                
                factor_scores[factor_id] = result['score']
                factor_values[factor_id] = result['current_value']
                factor_details[factor_id] = result
                
                print(f"    ✅ {factor_id}: {result['current_value']:.2f} (评分: {result['score']:.1f})")
                
            except Exception as e:
                print(f"    ❌ {factor_id} 处理失败: {e}")
                factor_scores[factor_id] = 0
                factor_values[factor_id] = 0
                factor_details[factor_id] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        # 计算综合评分
        total_score = self._calculate_total_score(factor_scores)
        risk_level = self._get_risk_level(total_score)
        
        return {
            'date': target_date.strftime('%Y-%m-%d'),
            'total_score': total_score,
            'risk_level': risk_level,
            'factor_scores': factor_scores,
            'factor_values': factor_values,
            'factor_details': factor_details,
            'summary': self._generate_summary(factor_scores, factor_values, risk_level)
        }
    
    def _calculate_total_score(self, factor_scores: Dict[str, float]) -> float:
        """计算综合评分（优化权重配置）"""
        # 优化后的因子权重（总权重=1.0）
        weights = {
            # 金融早期信号 (40%)
            'VIX_RISK': 0.08,           # 市场波动率
            'YIELD_CURVE': 0.12,        # 收益率曲线（降低权重）
            'CREDIT_SPREAD': 0.10,      # 信用利差
            'TED_SPREAD': 0.06,         # TED利差
            'NFCI': 0.04,               # 金融状况指数
            
            # 实体经济指标 (25%)
            'PAYEMS': 0.05,             # 非农就业
            'INDPRO': 0.05,             # 工业生产
            'GDP': 0.04,               # GDP
            'NEWORDER': 0.03,          # 新订单
            'MANEMP': 0.02,            # 制造业就业
            'IC4WSA': 0.02,            # 初请失业金
            'AWHMAN': 0.02,            # 制造业工时
            'PERMIT': 0.02,            # 建筑许可
            
            # 利率与货币政策 (15%)
            'FEDFUNDS': 0.04,          # 联邦基金利率
            'DGS10': 0.03,             # 10年期国债
            'MORTGAGE30US': 0.03,      # 30年期按揭利率
            'SOFR': 0.02,              # SOFR利率
            'DTB3': 0.02,              # 3个月国债
            'CPN3M': 0.01,             # 3个月商业票据
            
            # 银行与信贷 (10%)
            'TOTLL': 0.03,             # 总贷款
            'TOTALSA': 0.03,           # 消费者信贷
            'TDSP': 0.02,              # 家庭债务偿付比率
            'TOTRESNS': 0.02,          # 银行准备金
            'WALCL': 0.02,             # 美联储总资产
            
            # 房地产与消费 (5%)
            'HOUSING_STRESS': 0.02,    # 房地产压力
            'CSUSHPINSA': 0.02,        # 房价指数
            'UMICH_CONF': 0.01,        # 消费者信心
            
            # 外部与杠杆 (3%)
            'DXY_VOL': 0.01,           # 美元指数
            'NCBDBIQ027S': 0.01,       # 企业债/GDP
            'STLFSI3': 0.01,           # 圣路易斯金融压力
            
            # 监测项 (2%)
            'DRSFRMACBS': 0.01,        # 房贷违约率
            'HOUST': 0.01              # 新屋开工
        }
        
        total_weighted_score = 0
        total_weight = 0
        
        for factor_id, score in factor_scores.items():
            weight = weights.get(factor_id, 0.01)  # 默认权重
            total_weighted_score += score * weight
            total_weight += weight
        
        # 确保权重归一化
        if total_weight > 0:
            return total_weighted_score / total_weight
        else:
            return 0
    
    def _get_risk_level(self, total_score: float) -> str:
        """获取风险等级"""
        if total_score >= 80:
            return "极高风险"
        elif total_score >= 60:
            return "高风险"
        elif total_score >= 30:
            return "中等风险"
        else:
            return "低风险"
    
    def _generate_summary(self, factor_scores: Dict[str, float], 
                         factor_values: Dict[str, float], risk_level: str) -> Dict[str, Any]:
        """生成分析摘要"""
        high_risk_factors = []
        for factor_id, score in factor_scores.items():
            if score >= 70:
                factor_name = self.factor_mapping.get(factor_id, factor_id)
                high_risk_factors.append({
                    'name': factor_name,
                    'score': score,
                    'value': factor_values.get(factor_id, 0)
                })
        
        high_risk_factors.sort(key=lambda x: x['score'], reverse=True)
        
        return {
            'risk_level': risk_level,
            'high_risk_factors': high_risk_factors,
            'total_factors': len(factor_scores),
            'active_factors': len([s for s in factor_scores.values() if s > 0])
        }


def main():
    """测试数据库集成"""
    integration = DatabaseIntegration()
    result = integration.run_daily_analysis_with_database()
    
    print("\n" + "="*50)
    print("📊 每日分析结果")
    print("="*50)
    print(f"日期: {result['date']}")
    print(f"综合评分: {result['total_score']:.1f}")
    print(f"风险等级: {result['risk_level']}")
    print(f"活跃因子: {result['summary']['active_factors']}/{result['summary']['total_factors']}")
    
    if result['summary']['high_risk_factors']:
        print("\n🔴 高风险因子:")
        for factor in result['summary']['high_risk_factors']:
            print(f"  {factor['name']}: {factor['score']:.1f} ({factor['value']:.2f})")


if __name__ == "__main__":
    main()
