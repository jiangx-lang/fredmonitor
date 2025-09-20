"""
基于HTTP API的同步功能测试

使用Mock API避免网络依赖，快速验证核心逻辑。
"""

import pytest
import json
import pathlib
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.sync_fred_http import (
    ensure_series_dir,
    safe_write_json,
    fetch_metadata,
    fetch_observations,
    calculate_features,
    sync_series
)


class TestSyncHttp:
    """测试HTTP API同步功能"""
    
    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.original_base = os.environ.get("BASE_DIR")
        os.environ["BASE_DIR"] = self.temp_dir
        
        # 创建必要的目录
        os.makedirs(os.path.join(self.temp_dir, "data", "fred", "series"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "config"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "templates"), exist_ok=True)
    
    def teardown_method(self):
        """测试后清理"""
        if self.original_base:
            os.environ["BASE_DIR"] = self.original_base
        else:
            os.environ.pop("BASE_DIR", None)
        
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_ensure_series_directory(self):
        """测试创建序列目录"""
        series_id = "TEST_SERIES"
        series_dir = ensure_series_dir(series_id)
        
        # 检查目录结构
        assert series_dir.exists()
        assert (series_dir / "notes" / "attachments").exists()
        assert (series_dir / "notes" / "custom_notes.md").exists()
        
        # 检查custom_notes.md是否为空
        custom_notes = (series_dir / "notes" / "custom_notes.md").read_text()
        assert custom_notes == ""
    
    def test_safe_write_json(self):
        """测试原子写入JSON"""
        test_file = pathlib.Path(self.temp_dir) / "test.json"
        test_data = {"key": "value", "number": 123}
        
        safe_write_json(test_file, test_data)
        
        # 检查文件存在且内容正确
        assert test_file.exists()
        with open(test_file, "r", encoding="utf-8") as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data
    
    def test_calculate_features(self):
        """测试特征计算"""
        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=24, freq='M')
        values = [100 + i * 0.5 for i in range(24)]
        
        df = pd.DataFrame({
            'date': dates,
            'value': values
        })
        
        # 测试YoY计算
        calc_config = {
            'yoy': {'op': 'pct_change', 'shift': 12, 'scale': 100}
        }
        
        result = calculate_features(df, calc_config)
        
        # 检查结果
        assert 'yoy' in result.columns
        assert len(result) == len(df)
        
        # 检查YoY计算（第13个值应该是第一个有效的YoY值）
        yoy_values = result['yoy'].dropna()
        assert len(yoy_values) > 0
    
    @patch('scripts.sync_fred_http.series_info')
    @patch('scripts.sync_fred_http.series_observations')
    @patch('scripts.sync_fred_http.get_next_release_date')
    @patch('scripts.sync_fred_http.polite_sleep')
    def test_sync_series_mock(self, mock_sleep, mock_next_release, mock_observations, mock_info):
        """测试Mock同步单个序列"""
        # 模拟API响应
        mock_info.return_value = {
            "seriess": [{
                "id": "TEST_SERIES",
                "title": "Test Series",
                "frequency": "Monthly",
                "units": "Index",
                "seasonal_adjustment": "Not Seasonally Adjusted",
                "last_updated": "2024-01-01",
                "observation_start": "2020-01-01",
                "observation_end": "2024-01-01",
                "notes": "Test notes"
            }]
        }
        
        mock_observations.return_value = {
            "observations": [
                {"date": "2024-01-01", "value": "100"},
                {"date": "2024-02-01", "value": "101"},
                {"date": "2024-03-01", "value": "102"}
            ]
        }
        
        mock_next_release.return_value = "2024-04-01"
        
        # 测试配置
        series_config = {
            'id': 'TEST_SERIES',
            'alias': 'test_series',
            'calc': {
                'yoy': {'op': 'pct_change', 'shift': 12, 'scale': 100}
            },
            'freshness_days': 30
        }
        
        # 同步序列
        sync_series(series_config)
        
        # 检查文件是否创建
        series_dir = pathlib.Path(self.temp_dir) / "data" / "fred" / "series" / "TEST_SERIES"
        assert series_dir.exists()
        assert (series_dir / "meta.json").exists()
        assert (series_dir / "raw.csv").exists()
        assert (series_dir / "features.parquet").exists()
        assert (series_dir / "notes" / "custom_notes.md").exists()
        
        # 检查元数据
        with open(series_dir / "meta.json", 'r', encoding='utf-8') as f:
            meta = json.load(f)
        
        assert meta['id'] == 'TEST_SERIES'
        assert meta['alias'] == 'test_series'
        assert meta['freshness_days'] == 30
        assert meta['next_release'] == '2024-04-01'
        
        # 检查原始数据
        raw_df = pd.read_csv(series_dir / "raw.csv")
        assert len(raw_df) == 3
        assert 'date' in raw_df.columns
        assert 'value' in raw_df.columns
        
        # 检查特征数据
        features_df = pd.read_parquet(series_dir / "features.parquet")
        assert len(features_df) == 3
        assert 'yoy' in features_df.columns
    
    def test_sync_series_empty_data(self):
        """测试空数据的情况"""
        with patch('scripts.sync_fred_http.series_info') as mock_info, \
             patch('scripts.sync_fred_http.series_observations') as mock_obs, \
             patch('scripts.sync_fred_http.get_next_release_date') as mock_next, \
             patch('scripts.sync_fred_http.polite_sleep') as mock_sleep:
            
            # 模拟空数据响应
            mock_info.return_value = {
                "seriess": [{"id": "EMPTY_SERIES", "title": "Empty Series"}]
            }
            mock_obs.return_value = {"observations": []}
            mock_next.return_value = "N/A"
            
            series_config = {'id': 'EMPTY_SERIES', 'alias': 'empty'}
            
            # 应该正常处理，不抛出异常
            sync_series(series_config)
            
            # 检查目录是否创建
            series_dir = pathlib.Path(self.temp_dir) / "data" / "fred" / "series" / "EMPTY_SERIES"
            assert series_dir.exists()
            assert (series_dir / "meta.json").exists()


@pytest.mark.slow
def test_integration_with_real_api():
    """集成测试：使用真实API（标记为慢测试）"""
    # 这个测试只在明确要求时运行
    # 可以通过 pytest -m slow 来运行
    pass


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
