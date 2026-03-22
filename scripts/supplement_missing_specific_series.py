#!/usr/bin/env python3
"""
补充用户指定的缺失具体FRED系列ID
"""

import os
import sys
import pathlib
import time
import pandas as pd
from typing import Dict, List, Optional

# 添加项目根目录到路径
BASE = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

from scripts.fred_http import series_info, series_observations

def ensure_series_dir(series_id: str, category_name: str = "Crisis_Detection") -> pathlib.Path:
    """确保系列目录存在"""
    series_dir = BASE / "data" / "fred" / "categories" / category_name / "series" / series_id
    (series_dir / "notes" / "attachments").mkdir(parents=True, exist_ok=True)
    
    # 创建自定义笔记文件
    custom_notes_file = series_dir / "notes" / "custom_notes.md"
    if not custom_notes_file.exists():
        custom_notes_file.write_text("", encoding="utf-8")
    
    return series_dir

def download_series_data(series_id: str, category_name: str = "Crisis_Detection") -> bool:
    """下载单个系列数据"""
    try:
        print(f"📥 下载 {series_id}...")
        
        # 获取系列信息
        info = series_info(series_id)
        if not info:
            print(f"❌ 无法获取 {series_id} 的信息")
            return False
        
        # 确保目录存在
        series_dir = ensure_series_dir(series_id, category_name)
        
        # 获取观测数据
        obs_data = series_observations(series_id, limit=10000)
        if not obs_data or 'observations' not in obs_data:
            print(f"❌ 无法获取 {series_id} 的观测数据")
            return False
        
        # 转换为DataFrame
        observations_list = obs_data['observations']
        if not observations_list:
            print(f"❌ {series_id} 没有观测数据")
            return False
        
        # 创建DataFrame
        df_data = []
        for obs in observations_list:
            if obs['value'] != '.':
                df_data.append({
                    'date': obs['date'],
                    'value': float(obs['value'])
                })
        
        if not df_data:
            print(f"❌ {series_id} 没有有效数据")
            return False
        
        observations = pd.DataFrame(df_data)
        observations['date'] = pd.to_datetime(observations['date'])
        observations.set_index('date', inplace=True)
        
        # 保存原始数据
        raw_file = series_dir / "raw.csv"
        observations.to_csv(raw_file, index=True)
        
        # 保存元数据
        meta_file = series_dir / "meta.json"
        import json
        meta_file.write_text(json.dumps(info, indent=2, ensure_ascii=False), encoding="utf-8")
        
        # 创建事实表
        fact_sheet_file = series_dir / "fact_sheet.md"
        fact_sheet_content = f"""# {info.get('title', series_id)}

## 基本信息
- **系列ID**: {series_id}
- **标题**: {info.get('title', 'N/A')}
- **单位**: {info.get('units', 'N/A')}
- **频率**: {info.get('frequency', 'N/A')}
- **季节性调整**: {info.get('seasonal_adjustment', 'N/A')}
- **最后更新**: {info.get('last_updated', 'N/A')}

## 数据概览
- **观测数量**: {len(observations)}
- **数据范围**: {observations.index.min()} 到 {observations.index.max()}
- **最新值**: {observations.iloc[-1] if len(observations) > 0 else 'N/A'}

## 用途
此系列用于危机预警检测框架中的相关指标分析。

## 数据来源
FRED (Federal Reserve Economic Data)
"""
        fact_sheet_file.write_text(fact_sheet_content, encoding="utf-8")
        
        print(f"✅ {series_id} 下载完成 ({len(observations)} 个观测值)")
        return True
        
    except Exception as e:
        print(f"❌ 下载 {series_id} 时出错: {e}")
        return False

def supplement_missing_specific_series():
    """补充用户指定的缺失具体FRED系列ID"""
    
    print("🔧 补充用户指定的缺失具体FRED系列ID...")
    print("=" * 80)
    
    # 用户指定的缺失系列ID
    missing_series = [
        "NCBDBIQ027S",  # 企业债/GDP
        "SOFRON",       # SOFR隔夜利率
        "BAMLH0A0HYM2", # 高收益债OAS
        "NAPM",         # ISM制造业PMI
        "INDPRO",       # 工业产出
        "STLFSI4",      # 圣路易斯金融压力指数
    ]
    
    # 创建危机检测分类目录
    crisis_dir = BASE / "data" / "fred" / "categories" / "Crisis_Detection"
    crisis_dir.mkdir(parents=True, exist_ok=True)
    
    total_series = len(missing_series)
    success_count = 0
    
    print(f"\n📂 补充缺失的具体系列:")
    print("-" * 60)
    
    for series_id in missing_series:
        if download_series_data(series_id, "Crisis_Detection"):
            success_count += 1
        
        # 避免API限制
        time.sleep(0.5)
    
    print(f"\n🎯 数据补充完成:")
    print("=" * 80)
    print(f"📊 总系列数: {total_series}")
    print(f"✅ 成功下载: {success_count}")
    print(f"❌ 失败数量: {total_series - success_count}")
    print(f"📈 成功率: {success_count/total_series*100:.1f}%")
    
    # 创建分类概览文件
    overview_file = crisis_dir / "metadata" / "category_info.yaml"
    overview_file.parent.mkdir(parents=True, exist_ok=True)
    
    overview_content = f"""# Crisis Detection Category Overview - Updated

## 分类信息
- **分类名称**: Crisis Detection
- **分类ID**: Crisis_Detection
- **描述**: 危机预警检测框架相关数据系列
- **更新时间**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## 数据系列统计
- **总系列数**: {success_count}
- **成功下载**: {success_count}
- **失败数量**: {total_series - success_count}

## 补充的系列
"""
    
    for series_id in missing_series:
        overview_content += f"- **{series_id}**: 用户指定的关键系列\n"
    
    overview_file.write_text(overview_content, encoding="utf-8")
    
    print(f"\n📁 数据已保存到: {crisis_dir}")
    print(f"📄 概览文件: {overview_file}")
    
    return success_count, total_series

if __name__ == "__main__":
    success_count, total_series = supplement_missing_specific_series()
