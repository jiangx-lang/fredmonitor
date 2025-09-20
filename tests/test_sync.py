"""
测试FRED数据同步功能
"""

import pytest
import os
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.sync_fred import (
    setup_series_directory,
    compute_features,
    upsert_duckdb,
    process_series
)


class TestSyncFred:
    """测试FRED同步功能"""
    
    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.original_base = os.environ.get("BASE_DIR")
        os.environ["BASE_DIR"] = self.temp_dir
        
        # 创建必要的目录
        os.makedirs(os.path.join(self.temp_dir, "data", "fred", "series"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "data", "lake"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "config"), exist_ok=True)
    
    def teardown_method(self):
        """测试后清理"""
        if self.original_base:
            os.environ["BASE_DIR"] = self.original_base
        else:
            os.environ.pop("BASE_DIR", None)
        
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_setup_series_directory(self):
        """测试创建序列目录"""
        series_id = "TEST_SERIES"
        series_dir = setup_series_directory(series_id)
        
        # 检查目录结构
        assert series_dir.exists()
        assert (series_dir / "notes" / "attachments").exists()
        assert (series_dir / "notes" / "custom_notes.md").exists()
        
        # 检查custom_notes.md是否为空
        custom_notes = (series_dir / "notes" / "custom_notes.md").read_text()
        assert custom_notes == ""
    
    def test_compute_features(self):
        """测试特征计算"""
        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=24, freq='M')
        values = [100 + i for i in range(24)]
        
        df = pd.DataFrame({
            'date': dates,
            'value': values
        })
        
        # 测试YoY计算
        calc_spec = {
            'yoy': {'op': 'pct_change', 'shift': 12, 'scale': 100}
        }
        
        result = compute_features(df, calc_spec)
        
        # 检查结果
        assert 'yoy' in result.columns
        assert len(result) == len(df)
        
        # 检查YoY计算（第13个值应该是第一个有效的YoY值）
        yoy_values = result['yoy'].dropna()
        assert len(yoy_values) > 0
        
        # 第一个YoY值应该是0（因为值相同）
        first_yoy = yoy_values.iloc[0]
        assert abs(first_yoy) < 1e-10  # 接近0
    
    def test_compute_features_empty_spec(self):
        """测试空特征规格"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'value': [1, 2, 3, 4, 5]
        })
        
        result = compute_features(df, {})
        
        # 应该返回原始数据
        assert len(result) == len(df)
        assert 'date' in result.columns
        assert 'value' in result.columns
    
    def test_upsert_duckdb(self):
        """测试DuckDB插入"""
        # 创建测试数据
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=5, freq='D'),
            'value': [1, 2, 3, 4, 5]
        })
        
        # 保存为Parquet
        parquet_file = os.path.join(self.temp_dir, "test.parquet")
        df.to_parquet(parquet_file, index=False)
        
        # 插入DuckDB
        upsert_duckdb("TEST_SERIES", parquet_file)
        
        # 检查DuckDB文件是否存在
        db_file = os.path.join(self.temp_dir, "data", "lake", "fred.duckdb")
        assert os.path.exists(db_file)
        
        # 验证数据
        import duckdb
        con = duckdb.connect(db_file)
        result = con.execute("SELECT * FROM fred.TEST_SERIES").df()
        con.close()
        
        assert len(result) == 5
        assert 'date' in result.columns
        assert 'value' in result.columns
    
    @patch('scripts.sync_fred.get_series_info')
    @patch('scripts.sync_fred.get_series_history')
    def test_process_series(self, mock_get_history, mock_get_info):
        """测试处理单个序列"""
        # 模拟FRED API响应
        mock_info = {
            'id': 'TEST_SERIES',
            'title': 'Test Series',
            'frequency': 'Monthly',
            'units': 'Index',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'last_updated': '2024-01-01',
            'observation_start': '2020-01-01',
            'observation_end': '2024-01-01',
            'notes': 'Test notes'
        }
        mock_get_info.return_value = mock_info
        
        # 模拟历史数据
        dates = pd.date_range('2024-01-01', periods=12, freq='M')
        values = [100 + i for i in range(12)]
        mock_series = pd.Series(values, index=dates)
        mock_get_history.return_value = mock_series
        
        # 测试配置
        series_config = {
            'id': 'TEST_SERIES',
            'alias': 'test_series',
            'calc': {
                'yoy': {'op': 'pct_change', 'shift': 12, 'scale': 100}
            },
            'freshness_days': 30
        }
        
        # 处理序列
        process_series(series_config)
        
        # 检查文件是否创建
        series_dir = os.path.join(self.temp_dir, "data", "fred", "series", "TEST_SERIES")
        assert os.path.exists(series_dir)
        assert os.path.exists(os.path.join(series_dir, "meta.json"))
        assert os.path.exists(os.path.join(series_dir, "raw.csv"))
        assert os.path.exists(os.path.join(series_dir, "features.parquet"))
        assert os.path.exists(os.path.join(series_dir, "notes", "custom_notes.md"))
        
        # 检查元数据
        with open(os.path.join(series_dir, "meta.json"), 'r') as f:
            meta = json.load(f)
        
        assert meta['id'] == 'TEST_SERIES'
        assert meta['alias'] == 'test_series'
        assert meta['freshness_days'] == 30
        
        # 检查原始数据
        raw_df = pd.read_csv(os.path.join(series_dir, "raw.csv"))
        assert len(raw_df) == 12
        assert 'date' in raw_df.columns
        assert 'value' in raw_df.columns
        
        # 检查特征数据
        features_df = pd.read_parquet(os.path.join(series_dir, "features.parquet"))
        assert len(features_df) == 12
        assert 'yoy' in features_df.columns
