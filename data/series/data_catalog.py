#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据目录查询工具
为crisis_monitor主程序提供统一的数据访问接口
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

class DataCatalog:
    """数据目录查询工具"""
    
    def __init__(self, data_dir: str = None):
        """
        初始化数据目录
        
        Args:
            data_dir: 数据目录路径，默认为当前目录
        """
        if data_dir is None:
            self.data_dir = Path(__file__).parent
        else:
            self.data_dir = Path(data_dir)
        
        # 加载数据摘要
        self.summary_file = self.data_dir / "data_summary.json"
        self.summary = self.load_summary()
        
        # 加载README信息
        self.readme_file = self.data_dir / "README.md"
        
        print(f"📁 数据目录初始化: {self.data_dir}")
        print(f"📊 总文件数: {self.summary.get('total_files', 0)}")
        print(f"📈 序列数: {self.summary.get('series_count', 0)}")
    
    def load_summary(self) -> Dict[str, Any]:
        """加载数据摘要"""
        if self.summary_file.exists():
            try:
                with open(self.summary_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载数据摘要失败: {e}")
                return {}
        return {}
    
    def get_series_info(self, series_id: str) -> Dict[str, Any]:
        """
        获取序列信息
        
        Args:
            series_id: 序列ID
            
        Returns:
            包含序列信息的字典
        """
        info = {
            'id': series_id,
            'exists': False,
            'file_path': None,
            'data_type': None,
            'unit': None,
            'frequency': None,
            'description': None,
            'latest_date': None,
            'latest_value': None,
            'data_count': 0
        }
        
        # 检查原始数据文件
        raw_file = self.data_dir / f"{series_id}.csv"
        if raw_file.exists():
            info['exists'] = True
            info['file_path'] = str(raw_file)
            info['data_type'] = 'raw'
            
            try:
                df = pd.read_csv(raw_file, parse_dates=['date'])
                if not df.empty:
                    info['latest_date'] = df['date'].max()
                    info['latest_value'] = df['value'].iloc[-1]
                    info['data_count'] = len(df)
            except Exception as e:
                print(f"⚠️ 读取数据文件失败 {series_id}: {e}")
        
        # 检查YoY数据文件
        yoy_file = self.data_dir / f"{series_id}_YOY.csv"
        if yoy_file.exists():
            info['yoy_exists'] = True
            info['yoy_file_path'] = str(yoy_file)
            info['data_type'] = 'yoy'
            
            try:
                df = pd.read_csv(yoy_file, parse_dates=['date'])
                if not df.empty:
                    info['latest_yoy_date'] = df['date'].max()
                    info['latest_yoy_value'] = df['yoy_pct'].iloc[-1]
                    info['yoy_data_count'] = len(df)
            except Exception as e:
                print(f"⚠️ 读取YoY数据文件失败 {series_id}: {e}")
        
        return info
    
    def get_all_series(self) -> List[str]:
        """
        获取所有可用的序列ID
        
        Returns:
            序列ID列表
        """
        series_list = []
        
        # 从CSV文件中提取序列ID
        for csv_file in self.data_dir.glob("*.csv"):
            if csv_file.name != "data_summary.json":
                series_id = csv_file.stem
                # 移除_YOY后缀
                if series_id.endswith("_YOY"):
                    series_id = series_id[:-4]
                if series_id not in series_list:
                    series_list.append(series_id)
        
        return sorted(series_list)
    
    def get_data(self, series_id: str, data_type: str = 'raw') -> Optional[pd.DataFrame]:
        """
        获取序列数据
        
        Args:
            series_id: 序列ID
            data_type: 数据类型 ('raw', 'yoy')
            
        Returns:
            数据DataFrame或None
        """
        if data_type == 'raw':
            file_path = self.data_dir / f"{series_id}.csv"
        elif data_type == 'yoy':
            file_path = self.data_dir / f"{series_id}_YOY.csv"
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")
        
        if not file_path.exists():
            return None
        
        try:
            df = pd.read_csv(file_path, parse_dates=['date'])
            return df
        except Exception as e:
            print(f"❌ 读取数据失败 {series_id}: {e}")
            return None
    
    def get_latest_value(self, series_id: str, data_type: str = 'raw') -> Optional[float]:
        """
        获取序列最新值
        
        Args:
            series_id: 序列ID
            data_type: 数据类型 ('raw', 'yoy')
            
        Returns:
            最新值或None
        """
        df = self.get_data(series_id, data_type)
        if df is None or df.empty:
            return None
        
        if data_type == 'yoy' and 'yoy_pct' in df.columns:
            return df['yoy_pct'].iloc[-1]
        elif 'value' in df.columns:
            return df['value'].iloc[-1]
        
        return None
    
    def get_latest_date(self, series_id: str, data_type: str = 'raw') -> Optional[datetime]:
        """
        获取序列最新日期
        
        Args:
            series_id: 序列ID
            data_type: 数据类型 ('raw', 'yoy')
            
        Returns:
            最新日期或None
        """
        df = self.get_data(series_id, data_type)
        if df is None or df.empty:
            return None
        
        return df['date'].max()
    
    def validate_data(self, series_id: str) -> Dict[str, Any]:
        """
        验证数据完整性
        
        Args:
            series_id: 序列ID
            
        Returns:
            验证结果字典
        """
        result = {
            'series_id': series_id,
            'valid': False,
            'issues': [],
            'warnings': []
        }
        
        # 检查原始数据
        raw_df = self.get_data(series_id, 'raw')
        if raw_df is None:
            result['issues'].append("原始数据文件不存在")
            return result
        
        # 检查数据格式
        if 'date' not in raw_df.columns:
            result['issues'].append("缺少date列")
        if 'value' not in raw_df.columns:
            result['issues'].append("缺少value列")
        
        # 检查数据完整性
        if raw_df.empty:
            result['issues'].append("数据为空")
        else:
            # 检查缺失值
            missing_values = raw_df['value'].isna().sum()
            if missing_values > 0:
                result['warnings'].append(f"有{missing_values}个缺失值")
            
            # 检查日期连续性
            date_diff = raw_df['date'].diff().dropna()
            if len(date_diff) > 1:
                min_diff = date_diff.min()
                max_diff = date_diff.max()
                if max_diff > min_diff * 2:
                    result['warnings'].append("日期间隔不均匀")
        
        # 检查YoY数据（如果存在）
        yoy_df = self.get_data(series_id, 'yoy')
        if yoy_df is not None:
            if 'yoy_pct' not in yoy_df.columns:
                result['issues'].append("YoY数据缺少yoy_pct列")
            if 'original_value' not in yoy_df.columns:
                result['warnings'].append("YoY数据缺少original_value列")
        
        result['valid'] = len(result['issues']) == 0
        return result
    
    def get_category_series(self, category: str) -> List[str]:
        """
        获取指定类别的序列
        
        Args:
            category: 类别名称
            
        Returns:
            序列ID列表
        """
        categories = self.summary.get('categories', {})
        if category in categories:
            return categories[category].get('series', [])
        return []
    
    def get_series_by_unit(self, unit: str) -> List[str]:
        """
        获取指定单位的序列
        
        Args:
            unit: 单位名称
            
        Returns:
            序列ID列表
        """
        # 这里需要根据README中的信息来映射
        unit_mapping = {
            'percentage': ['T10Y3M', 'T10Y2Y', 'BAMLH0A0HYM2', 'BAA10YM', 'TEDRATE', 
                          'FEDFUNDS', 'DGS10', 'MORTGAGE30US', 'SOFR', 'DTB3', 'CPN3M',
                          'UNRATE', 'CIVPART', 'EMRATIO', 'UMCSENT', 'DRTSCILM', 'TDSP',
                          'VIXCLS', 'DRSFRMACBS', 'CORPORATE_DEBT_GDP_RATIO'],
            'index': ['CSUSHPINSA', 'INDPRO', 'NFCI', 'THREEFYTP10', 'STLFSI3', 'EPUINDX', 'SP500'],
            'currency': ['NCBDBIQ027S', 'GDP', 'WALCL', 'TOTLL', 'TOTALSA', 'TOTRESNS'],
            'count': ['PAYEMS', 'MANEMP', 'HOUST', 'PERMIT', 'IC4WSA']
        }
        
        return unit_mapping.get(unit, [])
    
    def get_series_by_frequency(self, frequency: str) -> List[str]:
        """
        获取指定频率的序列
        
        Args:
            frequency: 频率名称
            
        Returns:
            序列ID列表
        """
        frequency_mapping = {
            'daily': ['T10Y3M', 'T10Y2Y', 'BAMLH0A0HYM2', 'BAA10YM', 'TEDRATE', 
                     'DGS10', 'MORTGAGE30US', 'SOFR', 'DTB3', 'VIXCLS', 'SP500'],
            'weekly': ['IC4WSA', 'NFCI', 'STLFSI3'],
            'monthly': ['PAYEMS', 'MANEMP', 'AWHMAN', 'INDPRO', 'CSUSHPINSA', 'HOUST', 
                       'PERMIT', 'FEDFUNDS', 'CPN3M', 'WALCL', 'TOTLL', 'TOTALSA', 
                       'TOTRESNS', 'UNRATE', 'CIVPART', 'EMRATIO', 'UMCSENT', 'RETAILSMNSA',
                       'DTWEXBGS', 'DRTSCILM', 'TDSP', 'EPUINDX'],
            'quarterly': ['GDP', 'NCBDBIQ027S', 'DRSFRMACBS', 'CORPORATE_DEBT_GDP_RATIO']
        }
        
        return frequency_mapping.get(frequency, [])
    
    def search_series(self, keyword: str) -> List[str]:
        """
        搜索序列
        
        Args:
            keyword: 关键词
            
        Returns:
            匹配的序列ID列表
        """
        all_series = self.get_all_series()
        keyword_lower = keyword.lower()
        
        matches = []
        for series_id in all_series:
            if keyword_lower in series_id.lower():
                matches.append(series_id)
        
        return matches
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        获取数据摘要
        
        Returns:
            数据摘要字典
        """
        return self.summary.copy()
    
    def print_catalog(self):
        """打印数据目录"""
        print("📊 FRED数据目录")
        print("=" * 80)
        
        categories = self.summary.get('categories', {})
        for category, info in categories.items():
            print(f"\n📁 {info['description']} ({info['count']}个)")
            for series_id in info['series']:
                series_info = self.get_series_info(series_id)
                status = "✅" if series_info['exists'] else "❌"
                print(f"  {status} {series_id}")
        
        print(f"\n📈 总计: {self.summary.get('total_files', 0)} 个文件")
        print(f"🕒 最后更新: {self.summary.get('last_updated', {}).get('pipeline_run', 'Unknown')}")

def main():
    """主函数"""
    print("FRED数据目录查询工具")
    print("=" * 80)
    
    try:
        catalog = DataCatalog()
        
        # 打印目录
        catalog.print_catalog()
        
        # 示例查询
        print("\n🔍 示例查询:")
        
        # 查询特定序列
        series_info = catalog.get_series_info('T10Y3M')
        print(f"T10Y3M信息: {series_info}")
        
        # 查询最新值
        latest_value = catalog.get_latest_value('T10Y3M')
        print(f"T10Y3M最新值: {latest_value}")
        
        # 查询类别
        yield_series = catalog.get_category_series('yield_curve')
        print(f"收益率曲线序列: {yield_series}")
        
        # 搜索
        search_results = catalog.search_series('T10Y')
        print(f"搜索T10Y结果: {search_results}")
        
    except Exception as e:
        print(f"❌ 程序运行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
