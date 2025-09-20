"""
测试因子接口合约

确保所有因子都正确实现了基类接口。
"""

import pytest
import pandas as pd
from datetime import datetime

# 导入所有因子
from factors.vix import VIX
from factors.ted_spread import TED_SPREAD
from factors.hy_spread import HY_SPREAD
from factors.yield_curve import YIELD_CURVE
from factors.nfci import NFCI
from factors.spx_vol import SPX_VOL
from factors.dxy_vol import DXY_VOL
from factors.umich_conf import UMICH_CONF
from factors.housing_stress import HOUSING_STRESS
from factors.em_spread import EM_SPREAD


class TestFactorContract:
    """测试因子接口合约"""
    
    @pytest.fixture
    def sample_data(self):
        """创建示例数据"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        values = [100 + i for i in range(10)]
        return pd.DataFrame({'date': dates, 'value': values})
    
    @pytest.fixture
    def global_config(self):
        """创建全局配置"""
        return {
            "bands": {
                "VIX": [12, 30],
                "TED": [0.1, 1.2],
                "HY_Spread": [3, 7],
                "Yield_Spread": [1.0, 0.0, "reverse"],
                "FCI": [-2, 1],
                "SP500_Vol": [0.01, 0.02],
                "DXY_Vol": [0.005, 0.01],
                "Consumer_Confidence": [90, 70, "reverse"],
                "Housing_Stress": [0, -0.02, "reverse"],
                "EM_Risk": [2, 6]
            }
        }
    
    def test_vix_contract(self, sample_data, global_config):
        """测试VIX因子接口"""
        factor = VIX({})
        
        # 测试属性
        assert factor.id == "VIX"
        assert factor.name == "VIX 波动率"
        
        # 测试fetch方法
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        assert 'date' in df.columns
        assert 'value' in df.columns
        
        # 测试compute方法
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        assert "original_value" in metrics
        
        # 测试score方法
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
        
        # 测试to_frame方法
        result_df = factor.to_frame(datetime.now(), metrics, score)
        assert isinstance(result_df, pd.DataFrame)
        assert 'date' in result_df.columns
        assert 'factor_id' in result_df.columns
        assert 'score' in result_df.columns
    
    def test_ted_spread_contract(self, sample_data, global_config):
        """测试TED利差因子接口"""
        factor = TED_SPREAD({})
        
        assert factor.id == "TED"
        assert factor.name == "TED利差"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_hy_spread_contract(self, sample_data, global_config):
        """测试高收益利差因子接口"""
        factor = HY_SPREAD({})
        
        assert factor.id == "HY_Spread"
        assert factor.name == "高收益信用利差"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_yield_curve_contract(self, sample_data, global_config):
        """测试收益率曲线因子接口"""
        factor = YIELD_CURVE({})
        
        assert factor.id == "Yield_Spread"
        assert factor.name == "收益率曲线斜率"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_nfci_contract(self, sample_data, global_config):
        """测试NFCI因子接口"""
        factor = NFCI({})
        
        assert factor.id == "FCI"
        assert factor.name == "国家金融状况指数"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_spx_vol_contract(self, sample_data, global_config):
        """测试标普500波动率因子接口"""
        factor = SPX_VOL({})
        
        assert factor.id == "SP500_Vol"
        assert factor.name == "标普500波动率"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_dxy_vol_contract(self, sample_data, global_config):
        """测试美元指数波动率因子接口"""
        factor = DXY_VOL({})
        
        assert factor.id == "DXY_Vol"
        assert factor.name == "美元指数波动率"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_umich_conf_contract(self, sample_data, global_config):
        """测试消费者信心因子接口"""
        factor = UMICH_CONF({})
        
        assert factor.id == "Consumer_Confidence"
        assert factor.name == "消费者信心指数"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_housing_stress_contract(self, sample_data, global_config):
        """测试住房压力因子接口"""
        factor = HOUSING_STRESS({})
        
        assert factor.id == "Housing_Stress"
        assert factor.name == "住房压力指数"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_em_spread_contract(self, sample_data, global_config):
        """测试新兴市场利差因子接口"""
        factor = EM_SPREAD({})
        
        assert factor.id == "EM_Risk"
        assert factor.name == "新兴市场信用利差"
        
        df = factor.fetch()
        assert isinstance(df, pd.DataFrame)
        
        metrics = factor.compute(sample_data)
        assert isinstance(metrics, dict)
        
        score = factor.score(metrics, global_config)
        assert isinstance(score, float)
        assert 0 <= score <= 100
