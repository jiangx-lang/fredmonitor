#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据管道
整合数据下载、处理、存储和使用
"""

import os
import sys
import pathlib
import yaml
import pandas as pd
import shutil
from datetime import datetime
from typing import Dict, List, Any

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 添加项目根目录到路径
sys.path.append('.')

from scripts.fred_http import series_observations, polite_sleep
from scripts.clean_utils import parse_numeric_series

class UnifiedDataPipeline:
    """统一数据管道"""
    
    def __init__(self):
        self.base_dir = pathlib.Path(__file__).parent
        self.data_dir = self.base_dir / "data" / "series"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载配置
        self.crisis_config = self.load_crisis_config()
        self.catalog_config = self.load_catalog_config()
        
        # 获取所有需要的序列ID
        self.required_series = self.get_required_series()
        
        print(f"🔧 统一数据管道初始化完成")
        print(f"📁 数据目录: {self.data_dir}")
        print(f"📊 需要处理的序列数: {len(self.required_series)}")
    
    def load_crisis_config(self) -> Dict[str, Any]:
        """加载crisis_monitor配置"""
        config_file = self.base_dir / "config" / "crisis_indicators.yaml"
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def load_catalog_config(self) -> Dict[str, Any]:
        """加载catalog配置"""
        config_file = self.base_dir / "config" / "catalog_fred.yaml"
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def get_required_series(self) -> List[str]:
        """获取所有需要的序列ID"""
        series_set = set()
        
        # 从crisis_indicators.yaml获取
        crisis_indicators = self.crisis_config.get('indicators', [])
        for indicator in crisis_indicators:
            series_id = indicator.get('id')
            if series_id:
                series_set.add(series_id)
        
        # 从catalog_fred.yaml获取
        catalog_series = self.catalog_config.get('series', [])
        for series in catalog_series:
            series_id = series.get('id')
            if series_id:
                series_set.add(series_id)
        
        # 添加合成指标依赖的序列
        synthetic_dependencies = [
            'CPN3M', 'DTB3', 'SOFR', 'NCBDBIQ027S', 'GDP',
            'TOTRESNS', 'WALCL', 'DPSACBW027SBOG', 'TOTALSA', 'TOTALSL'
        ]
        series_set.update(synthetic_dependencies)
        
        return sorted(list(series_set))
    
    def download_series_data(self, series_id: str) -> bool:
        """下载单个序列数据"""
        try:
            print(f"📥 下载序列: {series_id}")
            
            # 获取数据
            response = series_observations(series_id)
            observations = response.get('observations', [])
            
            if not observations:
                print(f"⚠️ 序列 {series_id} 无数据")
                return False
            
            # 转换为DataFrame
            df = pd.DataFrame(observations)
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[['date', 'value']].dropna()
            
            if df.empty:
                print(f"⚠️ 序列 {series_id} 无有效数据")
                return False
            
            # 保存到统一路径
            output_file = self.data_dir / f"{series_id}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ {series_id}: {len(df)} 条数据, 最新日期: {df['date'].max().date()}")
            
            # 礼貌性延迟
            polite_sleep()
            return True
            
        except Exception as e:
            print(f"❌ 下载序列 {series_id} 失败: {e}")
            return False
    
    def calculate_yoy_data(self, series_id: str) -> bool:
        """计算YoY数据"""
        try:
            # 检查是否需要YoY计算
            crisis_indicators = self.crisis_config.get('indicators', [])
            needs_yoy = False
            for indicator in crisis_indicators:
                if indicator.get('id') == series_id and indicator.get('transform') == 'yoy_pct':
                    needs_yoy = True
                    break
            
            if not needs_yoy:
                return True
            
            print(f"📊 计算YoY数据: {series_id}")
            
            # 读取原始数据
            input_file = self.data_dir / f"{series_id}.csv"
            if not input_file.exists():
                print(f"⚠️ 原始数据文件不存在: {input_file}")
                return False
            
            df = pd.read_csv(input_file, parse_dates=['date'])
            df = df.set_index('date')
            
            # 计算YoY
            values = df['value'].dropna()
            if len(values) < 13:  # 至少需要13个月的数据
                print(f"⚠️ 数据不足，无法计算YoY: {series_id}")
                return False
            
            # 推断频率
            freq = pd.infer_freq(values.index)
            if freq and freq.startswith('M'):
                shift = 12  # 月度数据
            elif freq and freq.startswith('Q'):
                shift = 4   # 季度数据
            else:
                shift = 12  # 默认月度
            
            # 计算YoY百分比
            yoy_pct = ((values / values.shift(shift)) - 1) * 100
            
            # 创建输出DataFrame
            output_df = pd.DataFrame({
                'date': yoy_pct.index,
                'yoy_pct': yoy_pct.values,
                'original_value': values.values
            }).dropna()
            
            # 保存YoY数据
            output_file = self.data_dir / f"{series_id}_YOY.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ {series_id}_YOY: {len(output_df)} 条数据, 最新YoY: {output_df['yoy_pct'].iloc[-1]:.2f}%")
            return True
            
        except Exception as e:
            print(f"❌ 计算YoY数据失败 {series_id}: {e}")
            return False
    
    def calculate_synthetic_indicators(self):
        """计算合成指标"""
        print("🔧 计算合成指标...")
        
        # 1. 企业债/GDP比率
        self.calculate_corp_debt_gdp_ratio()
        
        # 2. 商业票据-3个月国债利差
        self.calculate_cp_minus_dtb3()
        
        # 3. SOFR 20日均值-3个月国债利差
        self.calculate_sofr_minus_dtb3()
        
        # 4. 准备金/资产比率
        self.calculate_reserves_assets_ratio()
        
        # 5. 准备金/存款比率
        self.calculate_reserves_deposits_ratio()
    
    def calculate_corp_debt_gdp_ratio(self):
        """计算企业债/GDP比率"""
        try:
            print("📊 计算企业债/GDP比率...")
            
            # 读取企业债数据
            corp_debt_file = self.data_dir / "NCBDBIQ027S.csv"
            gdp_file = self.data_dir / "GDP.csv"
            
            if not corp_debt_file.exists() or not gdp_file.exists():
                print("⚠️ 企业债或GDP数据文件不存在")
                return
            
            corp_debt_df = pd.read_csv(corp_debt_file, parse_dates=['date'])
            gdp_df = pd.read_csv(gdp_file, parse_dates=['date'])
            
            # 设置日期索引
            corp_debt_df = corp_debt_df.set_index('date')
            gdp_df = gdp_df.set_index('date')
            
            # 单位转换：企业债从百万美元转为十亿美元
            corp_debt_billions = corp_debt_df['value'] / 1000
            
            # 对齐数据并计算比率
            gdp_values = gdp_df['value']
            corp_debt_aligned = corp_debt_billions.reindex(gdp_values.index).fillna(method='ffill')
            ratio = (corp_debt_aligned / gdp_values) * 100
            
            # 保存结果
            output_df = pd.DataFrame({
                'date': ratio.index,
                'value': ratio.values
            }).dropna()
            
            output_file = self.data_dir / "CORPORATE_DEBT_GDP_RATIO.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ 企业债/GDP比率: {len(output_df)} 条数据, 最新比率: {output_df['value'].iloc[-1]:.2f}%")
            
        except Exception as e:
            print(f"❌ 计算企业债/GDP比率失败: {e}")
    
    def calculate_cp_minus_dtb3(self):
        """计算商业票据-3个月国债利差"""
        try:
            print("📊 计算CP_MINUS_DTB3...")
            
            cp_file = self.data_dir / "CPN3M.csv"
            tb_file = self.data_dir / "DTB3.csv"
            
            if not cp_file.exists() or not tb_file.exists():
                print("⚠️ CPN3M或DTB3数据文件不存在")
                return
            
            cp_df = pd.read_csv(cp_file, parse_dates=['date'])
            tb_df = pd.read_csv(tb_file, parse_dates=['date'])
            
            cp_df = cp_df.set_index('date')
            tb_df = tb_df.set_index('date')
            
            # 计算利差
            cp_values = cp_df['value']
            tb_values = tb_df['value']
            
            # 对齐数据
            cp_aligned = cp_values.reindex(tb_values.index).fillna(method='ffill')
            spread = cp_aligned - tb_values
            
            # 保存结果
            output_df = pd.DataFrame({
                'date': spread.index,
                'value': spread.values
            }).dropna()
            
            output_file = self.data_dir / "CP_MINUS_DTB3.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ CP_MINUS_DTB3: {len(output_df)} 条数据, 最新利差: {output_df['value'].iloc[-1]:.4f}")
            
        except Exception as e:
            print(f"❌ 计算CP_MINUS_DTB3失败: {e}")
    
    def calculate_sofr_minus_dtb3(self):
        """计算SOFR 20日均值-3个月国债利差"""
        try:
            print("📊 计算SOFR20DMA_MINUS_DTB3...")
            
            sofr_file = self.data_dir / "SOFR.csv"
            tb_file = self.data_dir / "DTB3.csv"
            
            if not sofr_file.exists() or not tb_file.exists():
                print("⚠️ SOFR或DTB3数据文件不存在")
                return
            
            sofr_df = pd.read_csv(sofr_file, parse_dates=['date'])
            tb_df = pd.read_csv(tb_file, parse_dates=['date'])
            
            sofr_df = sofr_df.set_index('date')
            tb_df = tb_df.set_index('date')
            
            # 计算SOFR 20日均值
            sofr_values = sofr_df['value']
            sofr_20dma = sofr_values.rolling(window=20, min_periods=1).mean()
            
            # 计算利差
            tb_values = tb_df['value']
            tb_aligned = tb_values.reindex(sofr_20dma.index).fillna(method='ffill')
            spread = sofr_20dma - tb_aligned
            
            # 保存结果
            output_df = pd.DataFrame({
                'date': spread.index,
                'value': spread.values
            }).dropna()
            
            output_file = self.data_dir / "SOFR20DMA_MINUS_DTB3.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ SOFR20DMA_MINUS_DTB3: {len(output_df)} 条数据, 最新利差: {output_df['value'].iloc[-1]:.4f}")
            
        except Exception as e:
            print(f"❌ 计算SOFR20DMA_MINUS_DTB3失败: {e}")
    
    def calculate_reserves_assets_ratio(self):
        """计算准备金/资产比率"""
        try:
            print("📊 计算RESERVES_ASSETS_PCT...")
            
            reserves_file = self.data_dir / "TOTRESNS.csv"
            assets_file = self.data_dir / "WALCL.csv"
            
            if not reserves_file.exists() or not assets_file.exists():
                print("⚠️ TOTRESNS或WALCL数据文件不存在")
                return
            
            reserves_df = pd.read_csv(reserves_file, parse_dates=['date'])
            assets_df = pd.read_csv(assets_file, parse_dates=['date'])
            
            reserves_df = reserves_df.set_index('date')
            assets_df = assets_df.set_index('date')
            
            # 计算比率
            reserves_values = reserves_df['value']
            assets_values = assets_df['value']
            
            reserves_aligned = reserves_values.reindex(assets_values.index).fillna(method='ffill')
            ratio = (reserves_aligned / assets_values) * 100
            
            # 保存结果
            output_df = pd.DataFrame({
                'date': ratio.index,
                'value': ratio.values
            }).dropna()
            
            output_file = self.data_dir / "RESERVES_ASSETS_PCT.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ RESERVES_ASSETS_PCT: {len(output_df)} 条数据, 最新比率: {output_df['value'].iloc[-1]:.4f}%")
            
        except Exception as e:
            print(f"❌ 计算RESERVES_ASSETS_PCT失败: {e}")
    
    def calculate_reserves_deposits_ratio(self):
        """计算准备金/存款比率"""
        try:
            print("📊 计算RESERVES_DEPOSITS_PCT...")
            
            reserves_file = self.data_dir / "TOTRESNS.csv"
            
            if not reserves_file.exists():
                print("⚠️ TOTRESNS数据文件不存在")
                return
            
            reserves_df = pd.read_csv(reserves_file, parse_dates=['date'])
            reserves_df = reserves_df.set_index('date')
            
            # 尝试不同的存款指标
            deposits_series = ["DPSACBW027SBOG", "TOTALSA", "TOTALSL"]
            deposits_data = None
            
            for dep_series in deposits_series:
                dep_file = self.data_dir / f"{dep_series}.csv"
                if dep_file.exists():
                    deposits_data = pd.read_csv(dep_file, parse_dates=['date'])
                    deposits_data = deposits_data.set_index('date')
                    print(f"使用存款指标: {dep_series}")
                    break
            
            if deposits_data is None:
                print("⚠️ 未找到合适的存款指标")
                return
            
            # 计算比率
            reserves_values = reserves_df['value']
            deposits_values = deposits_data['value']
            
            reserves_aligned = reserves_values.reindex(deposits_values.index).fillna(method='ffill')
            ratio = (reserves_aligned / deposits_values) * 100
            
            # 保存结果
            output_df = pd.DataFrame({
                'date': ratio.index,
                'value': ratio.values
            }).dropna()
            
            output_file = self.data_dir / "RESERVES_DEPOSITS_PCT.csv"
            output_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ RESERVES_DEPOSITS_PCT: {len(output_df)} 条数据, 最新比率: {output_df['value'].iloc[-1]:.4f}%")
            
        except Exception as e:
            print(f"❌ 计算RESERVES_DEPOSITS_PCT失败: {e}")
    
    def run_pipeline(self):
        """运行完整的数据管道"""
        print("🚀 启动统一数据管道")
        print("=" * 80)
        
        success_count = 0
        total_count = len(self.required_series)
        
        # 1. 下载所有序列数据
        print(f"\n📥 步骤1: 下载 {total_count} 个序列数据")
        for i, series_id in enumerate(self.required_series, 1):
            print(f"[{i}/{total_count}] 处理序列: {series_id}")
            if self.download_series_data(series_id):
                success_count += 1
        
        print(f"\n✅ 数据下载完成: {success_count}/{total_count} 成功")
        
        # 2. 计算YoY数据
        print(f"\n📊 步骤2: 计算YoY数据")
        yoy_count = 0
        for series_id in self.required_series:
            if self.calculate_yoy_data(series_id):
                yoy_count += 1
        
        print(f"✅ YoY计算完成: {yoy_count} 个序列")
        
        # 3. 计算合成指标
        print(f"\n🔧 步骤3: 计算合成指标")
        self.calculate_synthetic_indicators()
        
        # 4. 生成数据摘要
        self.generate_data_summary()
        
        print("\n🎉 统一数据管道运行完成!")
        print(f"📁 数据文件保存在: {self.data_dir}")
    
    def generate_data_summary(self):
        """生成数据摘要"""
        print("\n📋 生成数据摘要...")
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_files": len(list(self.data_dir.glob("*.csv"))),
            "series_count": len(self.required_series),
            "data_dir": str(self.data_dir)
        }
        
        # 保存摘要
        summary_file = self.data_dir / "data_summary.json"
        import json
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 数据摘要已保存: {summary_file}")

def main():
    """主函数"""
    print("统一数据管道程序")
    print("=" * 80)
    
    try:
        pipeline = UnifiedDataPipeline()
        pipeline.run_pipeline()
        
    except Exception as e:
        print(f"\n❌ 数据管道运行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









