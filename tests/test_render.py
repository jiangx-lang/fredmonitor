"""
测试事实表渲染功能
"""

import pytest
import os
import json
import tempfile
import shutil
from unittest.mock import patch
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.render_fact_sheets import (
    load_template,
    compute_trend_6m,
    get_attachments_list,
    render_fact_sheet
)


class TestRenderFactSheets:
    """测试事实表渲染功能"""
    
    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.original_base = os.environ.get("BASE_DIR")
        os.environ["BASE_DIR"] = self.temp_dir
        
        # 创建必要的目录
        os.makedirs(os.path.join(self.temp_dir, "templates"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "data", "fred", "series", "TEST_SERIES"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "data", "fred", "series", "TEST_SERIES", "notes", "attachments"), exist_ok=True)
    
    def teardown_method(self):
        """测试后清理"""
        if self.original_base:
            os.environ["BASE_DIR"] = self.original_base
        else:
            os.environ.pop("BASE_DIR", None)
        
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_template(self):
        """创建测试模板"""
        template_content = """
# {{ title }} ({{ series_id }})

**Observations**: {{ latest_value }} ({{ latest_period }})
**Units**: {{ units }}
**Frequency**: {{ frequency }}

## Latest Readings
- Latest: **{{ latest_value }}** ({{ latest_period }})
{% if yoy %} - YoY: {{ yoy }}{% endif %}
{% if mom %} - MoM: {{ mom }}{% endif %}

## Official Notes (from FRED)
{{ official_notes }}

## Research Notes (Manual)
{{ custom_notes_block }}

## Attachments
{{ attachments_list }}
"""
        
        template_file = os.path.join(self.temp_dir, "templates", "fact_sheet.md.j2")
        with open(template_file, "w", encoding="utf-8") as f:
            f.write(template_content)
    
    def create_test_data(self, series_id="TEST_SERIES"):
        """创建测试数据"""
        series_dir = os.path.join(self.temp_dir, "data", "fred", "series", series_id)
        
        # 创建元数据
        meta_data = {
            "id": series_id,
            "title": "Test Series",
            "frequency": "Monthly",
            "units": "Index",
            "seasonal_adjustment": "Not Seasonally Adjusted",
            "last_updated": "2024-01-01",
            "observation_start": "2020-01-01",
            "observation_end": "2024-01-01",
            "notes": "This is a test series for unit testing.",
            "alias": "test_series"
        }
        
        with open(os.path.join(series_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        
        # 创建特征数据
        dates = pd.date_range('2024-01-01', periods=12, freq='M')
        values = [100 + i * 0.5 for i in range(12)]
        yoy_values = [2.0 + i * 0.1 for i in range(12)]
        mom_values = [0.1 + i * 0.01 for i in range(12)]
        
        features_df = pd.DataFrame({
            'date': dates,
            'value': values,
            'yoy': yoy_values,
            'mom': mom_values
        })
        
        features_df.to_parquet(os.path.join(series_dir, "features.parquet"), index=False)
        
        # 创建自定义笔记
        custom_notes = "这是测试用的自定义笔记。\n\n包含多行内容。"
        with open(os.path.join(series_dir, "notes", "custom_notes.md"), "w", encoding="utf-8") as f:
            f.write(custom_notes)
        
        # 创建测试附件
        attachment_file = os.path.join(series_dir, "notes", "attachments", "test_chart.png")
        with open(attachment_file, "w") as f:
            f.write("fake image data")
    
    def test_load_template(self):
        """测试加载模板"""
        self.create_test_template()
        template = load_template()
        assert template is not None
    
    def test_compute_trend_6m(self):
        """测试6个月趋势计算"""
        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=6, freq='M')
        values = [100 + i for i in range(6)]  # 线性增长
        
        df = pd.DataFrame({
            'date': dates,
            'value': values
        })
        
        trend = compute_trend_6m(df)
        
        # 线性增长的趋势应该接近1
        assert trend is not None
        assert abs(trend - 1.0) < 0.1
    
    def test_compute_trend_6m_insufficient_data(self):
        """测试数据不足的情况"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=1, freq='M'),
            'value': [100]
        })
        
        trend = compute_trend_6m(df)
        assert trend is None
    
    def test_get_attachments_list(self):
        """测试获取附件列表"""
        series_dir = os.path.join(self.temp_dir, "data", "fred", "series", "TEST_SERIES")
        
        # 创建测试附件
        attachments_dir = os.path.join(series_dir, "notes", "attachments")
        os.makedirs(attachments_dir, exist_ok=True)
        
        # 创建几个测试文件
        test_files = ["chart1.png", "chart2.jpg", "data.csv"]
        for filename in test_files:
            with open(os.path.join(attachments_dir, filename), "w") as f:
                f.write("test content")
        
        attachments = get_attachments_list(series_dir)
        
        assert len(attachments) == 3
        assert "chart1.png" in attachments
        assert "chart2.jpg" in attachments
        assert "data.csv" in attachments
        # 应该按字母顺序排序
        assert attachments == sorted(attachments)
    
    def test_get_attachments_list_empty(self):
        """测试空附件目录"""
        series_dir = os.path.join(self.temp_dir, "data", "fred", "series", "TEST_SERIES")
        os.makedirs(series_dir, exist_ok=True)
        
        attachments = get_attachments_list(series_dir)
        assert attachments == []
    
    def test_render_fact_sheet(self):
        """测试渲染事实表"""
        self.create_test_template()
        self.create_test_data()
        
        template = load_template()
        render_fact_sheet("TEST_SERIES", template)
        
        # 检查事实表是否创建
        fact_sheet_file = os.path.join(
            self.temp_dir, "data", "fred", "series", "TEST_SERIES", "fact_sheet.md"
        )
        assert os.path.exists(fact_sheet_file)
        
        # 检查内容
        with open(fact_sheet_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 检查关键内容
        assert "Test Series (TEST_SERIES)" in content
        assert "**Observations**:" in content
        assert "**Units**: Index" in content
        assert "**Frequency**: Monthly" in content
        assert "This is a test series for unit testing." in content
        assert "这是测试用的自定义笔记" in content
        assert "- test_chart.png" in content
        assert "YoY:" in content
        assert "MoM:" in content
    
    def test_render_fact_sheet_missing_files(self):
        """测试缺少文件的情况"""
        self.create_test_template()
        
        # 不创建数据文件
        template = load_template()
        
        # 应该跳过渲染
        render_fact_sheet("NONEXISTENT_SERIES", template)
        
        # 检查是否没有创建事实表
        fact_sheet_file = os.path.join(
            self.temp_dir, "data", "fred", "series", "NONEXISTENT_SERIES", "fact_sheet.md"
        )
        assert not os.path.exists(fact_sheet_file)
    
    def test_render_fact_sheet_empty_data(self):
        """测试空数据的情况"""
        self.create_test_template()
        
        # 创建空数据
        series_dir = os.path.join(self.temp_dir, "data", "fred", "series", "EMPTY_SERIES")
        os.makedirs(series_dir, exist_ok=True)
        
        # 创建元数据
        meta_data = {"id": "EMPTY_SERIES", "title": "Empty Series"}
        with open(os.path.join(series_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta_data, f)
        
        # 创建空特征数据
        empty_df = pd.DataFrame(columns=['date', 'value'])
        empty_df.to_parquet(os.path.join(series_dir, "features.parquet"), index=False)
        
        template = load_template()
        
        # 应该跳过渲染
        render_fact_sheet("EMPTY_SERIES", template)
        
        # 检查是否没有创建事实表
        fact_sheet_file = os.path.join(series_dir, "fact_sheet.md")
        assert not os.path.exists(fact_sheet_file)
