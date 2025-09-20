"""
测试评分算法

测试风险评分计算逻辑。
"""

import pytest
from core.scoring import risk_score, calculate_factor_score, get_risk_level


class TestScoring:
    """测试评分算法"""
    
    def test_risk_score_normal(self):
        """测试正常评分"""
        # 正常情况：值在区间内
        score = risk_score(20, 10, 30, reverse=False)
        assert score == 50.0  # (20-10)/(30-10) * 100 = 50
        
        # 边界情况：值等于低阈值
        score = risk_score(10, 10, 30, reverse=False)
        assert score == 0.0
        
        # 边界情况：值等于高阈值
        score = risk_score(30, 10, 30, reverse=False)
        assert score == 100.0
    
    def test_risk_score_reverse(self):
        """测试反向评分"""
        # 反向情况：值在区间内
        score = risk_score(20, 10, 30, reverse=True)
        assert score == 50.0  # (30-20)/(30-10) * 100 = 50
        
        # 边界情况：值等于高阈值
        score = risk_score(30, 10, 30, reverse=True)
        assert score == 0.0
        
        # 边界情况：值等于低阈值
        score = risk_score(10, 10, 30, reverse=True)
        assert score == 100.0
    
    def test_risk_score_out_of_range(self):
        """测试超出范围的值"""
        # 值低于低阈值
        score = risk_score(5, 10, 30, reverse=False)
        assert score == 0.0
        
        # 值高于高阈值
        score = risk_score(35, 10, 30, reverse=False)
        assert score == 100.0
        
        # 反向情况：值低于低阈值
        score = risk_score(5, 10, 30, reverse=True)
        assert score == 100.0
        
        # 反向情况：值高于高阈值
        score = risk_score(35, 10, 30, reverse=True)
        assert score == 0.0
    
    def test_risk_score_none_value(self):
        """测试None值"""
        score = risk_score(None, 10, 30, reverse=False)
        assert score == 0.0
    
    def test_calculate_factor_score(self):
        """测试因子评分计算"""
        global_cfg = {
            "bands": {
                "TEST": [10, 30]
            }
        }
        
        # 正常情况
        score = calculate_factor_score("TEST", 20, global_cfg)
        assert score == 50.0
        
        # 不存在的因子
        score = calculate_factor_score("UNKNOWN", 20, global_cfg)
        assert score == 0.0
        
        # None值
        score = calculate_factor_score("TEST", None, global_cfg)
        assert score == 0.0
    
    def test_calculate_factor_score_reverse(self):
        """测试反向因子评分"""
        global_cfg = {
            "bands": {
                "TEST": [10, 30, "reverse"]
            }
        }
        
        score = calculate_factor_score("TEST", 20, global_cfg)
        assert score == 50.0
    
    def test_get_risk_level(self):
        """测试风险等级获取"""
        thresholds = {
            "low": 30,
            "medium": 50,
            "high": 70
        }
        
        # 低风险
        level = get_risk_level(25, thresholds)
        assert level == "低风险"
        
        # 中等风险
        level = get_risk_level(40, thresholds)
        assert level == "中等风险"
        
        # 偏高风险
        level = get_risk_level(60, thresholds)
        assert level == "偏高风险"
        
        # 极高风险
        level = get_risk_level(80, thresholds)
        assert level == "极高风险"
    
    def test_get_risk_level_default_thresholds(self):
        """测试默认风险等级阈值"""
        # 使用默认阈值
        level = get_risk_level(25, {})
        assert level == "低风险"
        
        level = get_risk_level(40, {})
        assert level == "中等风险"
        
        level = get_risk_level(60, {})
        assert level == "偏高风险"
        
        level = get_risk_level(80, {})
        assert level == "极高风险"
