"""
数据聚合器
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from core.registry import FactorRegistry


class DataAggregator:
    """数据聚合器"""
    
    def __init__(self, fred_client, cache_manager, registry: FactorRegistry, settings: Dict[str, Any]):
        """
        初始化数据聚合器
        
        Args:
            fred_client: FRED客户端
            cache_manager: 缓存管理器
            registry: 因子注册表
            settings: 系统设置
        """
        self.fred_client = fred_client
        self.cache_manager = cache_manager
        self.registry = registry
        self.settings = settings
        
        # 获取所有因子
        self.factors = self.registry.get_all_factors()
        
        # 历史评分缓存
        self.score_history = []
    
    def run_daily_analysis(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        运行每日分析
        
        Args:
            target_date: 目标分析日期，默认为今天
            
        Returns:
            分析结果字典
        """
        if target_date is None:
            target_date = datetime.now()
        
        print(f"📊 开始每日分析: {target_date.strftime('%Y-%m-%d')}")
        
        # 分析每个因子
        factor_scores = {}
        factor_values = {}
        factor_details = {}
        
        for factor in self.factors:
            try:
                print(f"  处理因子: {factor.name}")
                
                # 获取数据
                df = factor.fetch()
                
                # 如果数据为空，尝试通过FRED客户端获取
                if df.empty and self.fred_client and hasattr(factor, 'series_id'):
                    series_id = getattr(factor, 'series_id', None)
                    if series_id:
                        print(f"    通过FRED API获取数据: {series_id}")
                        try:
                            fred_data = self.fred_client.get_series_cached(series_id)
                            if not fred_data.empty:
                                df = fred_data.rename("value").to_frame().reset_index(names="date").dropna()
                        except Exception as e:
                            print(f"    FRED API获取失败: {e}")
                
                if df.empty:
                    print(f"    ⚠️ {factor.name} 无可用数据")
                    factor_scores[factor.id] = 0
                    factor_values[factor.id] = 0
                    factor_details[factor.id] = {
                        'status': 'no_data',
                        'message': '无可用数据'
                    }
                    continue
                
                # 计算指标
                metrics = factor.compute(df)
                
                # 计算评分
                score = factor.score(metrics, self.settings)
                
                # 存储结果
                factor_scores[factor.id] = score
                factor_values[factor.id] = metrics.get('current_value', 0)
                factor_details[factor.id] = {
                    'status': 'success',
                    'metrics': metrics,
                    'score': score,
                    'data_points': len(df),
                    'latest_date': df['date'].max() if not df.empty else None
                }
                
                print(f"    ✅ {factor.name}: {score:.1f}分")
                
            except Exception as e:
                print(f"    ❌ {factor.name} 处理失败: {e}")
                factor_scores[factor.id] = 0
                factor_values[factor.id] = 0
                factor_details[factor.id] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        # 计算综合评分
        total_score = self._calculate_total_score(factor_scores)
        
        # 确定风险等级
        risk_level = self._get_risk_level(total_score)
        
        # 生成分析结果
        result = {
            'date': target_date,
            'total_score': total_score,
            'risk_level': risk_level,
            'factor_scores': factor_scores,
            'factor_values': factor_values,
            'factor_details': factor_details,
            'analysis_summary': self._generate_summary(factor_scores, factor_values, risk_level)
        }
        
        # 保存到历史记录
        self._save_score_history(result)
        
        print(f"📊 每日分析完成: 总分 {total_score:.1f}, 风险等级 {risk_level}")
        
        return result
    
    def _calculate_total_score(self, factor_scores: Dict[str, float]) -> float:
        """计算综合评分"""
        if not factor_scores:
            return 0
        
        # 获取因子权重
        total_weight = 0
        weighted_score = 0
        
        for factor in self.factors:
            factor_id = factor.id
            if factor_id in factor_scores:
                weight = factor.weight
                score = factor_scores[factor_id]
                
                total_weight += weight
                weighted_score += score * weight
        
        if total_weight == 0:
            return 0
        
        return weighted_score / total_weight
    
    def _get_risk_level(self, total_score: float) -> str:
        """获取风险等级"""
        thresholds = self.settings.get('risk_thresholds', {})
        low_threshold = thresholds.get('low', 30)
        medium_threshold = thresholds.get('medium', 60)
        high_threshold = thresholds.get('high', 80)
        
        if total_score >= high_threshold:
            return "极高风险"
        elif total_score >= medium_threshold:
            return "高风险"
        elif total_score >= low_threshold:
            return "中等风险"
        else:
            return "低风险"
    
    def _generate_summary(self, factor_scores: Dict[str, float], 
                         factor_values: Dict[str, float], risk_level: str) -> Dict[str, Any]:
        """生成分析摘要"""
        # 找出最高风险的因子
        high_risk_factors = []
        for factor_id, score in factor_scores.items():
            if score >= 70:
                factor = self.registry.get_factor(factor_id)
                if factor:
                    high_risk_factors.append({
                        'name': factor.name,
                        'score': score,
                        'value': factor_values.get(factor_id, 0)
                    })
        
        # 按评分排序
        high_risk_factors.sort(key=lambda x: x['score'], reverse=True)
        
        return {
            'risk_level': risk_level,
            'high_risk_factors': high_risk_factors,
            'total_factors': len(factor_scores),
            'active_factors': len([s for s in factor_scores.values() if s > 0])
        }
    
    def _save_score_history(self, result: Dict[str, Any]):
        """保存评分历史"""
        history_entry = {
            'date': result['date'],
            'total_score': result['total_score'],
            'risk_level': result['risk_level'],
            'factor_scores': result['factor_scores'].copy()
        }
        
        self.score_history.append(history_entry)
        
        # 只保留最近100天的历史
        if len(self.score_history) > 100:
            self.score_history = self.score_history[-100:]
    
    def get_recent_scores(self, days: int = 5) -> List[Dict[str, Any]]:
        """获取最近几天的评分"""
        return self.score_history[-days:] if self.score_history else []
    
    def get_factor_trend(self, factor_id: str, days: int = 30) -> List[float]:
        """获取因子趋势"""
        trend = []
        for entry in self.score_history[-days:]:
            if factor_id in entry['factor_scores']:
                trend.append(entry['factor_scores'][factor_id])
        return trend
    
    def backfill(self, start_date: str, end_date: str):
        """历史数据回填"""
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            print(f"📊 开始历史数据回填: {start_date} 到 {end_date}")
            
            current_date = start_dt
            while current_date <= end_dt:
                print(f"  处理日期: {current_date.strftime('%Y-%m-%d')}")
                
                # 运行该日期的分析
                result = self.run_daily_analysis(current_date)
                
                # 移动到下一天
                current_date += timedelta(days=1)
            
            print(f"✅ 历史数据回填完成")
            
        except Exception as e:
            print(f"❌ 历史数据回填失败: {e}")
            raise
    
    def get_factor_summary(self, factor_id: str) -> Dict[str, Any]:
        """获取因子摘要"""
        factor = self.registry.get_factor(factor_id)
        if not factor:
            return {}
        
        # 获取最近评分趋势
        trend = self.get_factor_trend(factor_id, 30)
        
        # 计算统计信息
        if trend:
            avg_score = np.mean(trend)
            max_score = np.max(trend)
            min_score = np.min(trend)
            volatility = np.std(trend)
        else:
            avg_score = max_score = min_score = volatility = 0
        
        return {
            'factor_id': factor_id,
            'name': factor.name,
            'description': factor.description,
            'group': factor.group,
            'weight': factor.weight,
            'recent_trend': trend,
            'avg_score': avg_score,
            'max_score': max_score,
            'min_score': min_score,
            'volatility': volatility
        }