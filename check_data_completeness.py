#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED数据完整性检查脚本
优先检查本地数据是否下载成功，如果没有则提醒运行数据下载
"""

import os
import sys
import json
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# 添加项目根目录到路径
BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))

def load_risk_dashboard_config():
    """加载风险面板配置"""
    try:
        config_path = BASE / "daily_risk_dashboard" / "config" / "risk_dashboard.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载风险面板配置失败: {e}")
        return None

def load_crisis_monitor_config():
    """加载危机监测配置"""
    try:
        config_path = BASE / "config" / "crisis_indicators.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载危机监测配置失败: {e}")
        return None

def check_local_data_status(series_id: str) -> Dict[str, any]:
    """检查单个指标的本地数据状态"""
    data_dir = BASE / "data" / "fred" / "series" / series_id
    
    status = {
        'series_id': series_id,
        'has_raw_data': False,
        'has_metadata': False,
        'data_points': 0,
        'latest_date': None,
        'days_old': None,
        'file_size_mb': 0,
        'status': 'missing'
    }
    
    # 检查原始数据文件
    raw_file = data_dir / "raw.csv"
    if raw_file.exists():
        try:
            df = pd.read_csv(raw_file, index_col=0, parse_dates=True)
            if not df.empty:
                status['has_raw_data'] = True
                status['data_points'] = len(df)
                status['latest_date'] = df.index[-1]
                status['days_old'] = (datetime.now() - df.index[-1].to_pydatetime()).days
                status['file_size_mb'] = raw_file.stat().st_size / (1024 * 1024)
                
                # 判断数据新鲜度
                if status['days_old'] <= 7:
                    status['status'] = 'fresh'
                elif status['days_old'] <= 30:
                    status['status'] = 'stale'
                else:
                    status['status'] = 'outdated'
        except Exception as e:
            status['status'] = 'corrupted'
            print(f"⚠️ 数据文件损坏 {series_id}: {e}")
    
    # 检查元数据文件
    meta_file = data_dir / "meta.json"
    if meta_file.exists():
        status['has_metadata'] = True
    
    return status

def check_all_indicators():
    """检查所有指标的数据完整性"""
    print("检查FRED数据完整性...")
    print("=" * 60)
    
    # 加载配置
    risk_config = load_risk_dashboard_config()
    crisis_config = load_crisis_monitor_config()
    
    if not risk_config or not crisis_config:
        print("❌ 配置文件加载失败")
        return None
    
    # 收集所有需要的指标
    all_indicators = set()
    
    # 从风险面板配置收集指标
    risk_buckets = risk_config.get('risk_dashboard', {}).get('buckets', [])
    for bucket in risk_buckets:
        for indicator in bucket.get('indicators', []):
            series_id = indicator.get('id')
            if series_id:
                all_indicators.add(series_id)
    
    # 从危机监测配置收集指标
    crisis_indicators = crisis_config.get('indicators', [])
    for indicator in crisis_indicators:
        series_id = indicator.get('series_id') or indicator.get('id')
        if series_id:
            all_indicators.add(series_id)
    
    print(f"需要检查的指标总数: {len(all_indicators)}")
    
    # 检查每个指标
    results = []
    fresh_count = 0
    stale_count = 0
    outdated_count = 0
    missing_count = 0
    corrupted_count = 0
    
    for series_id in sorted(all_indicators):
        status = check_local_data_status(series_id)
        results.append(status)
        
        if status['status'] == 'fresh':
            fresh_count += 1
        elif status['status'] == 'stale':
            stale_count += 1
        elif status['status'] == 'outdated':
            outdated_count += 1
        elif status['status'] == 'missing':
            missing_count += 1
        elif status['status'] == 'corrupted':
            corrupted_count += 1
    
    # 显示统计结果
    print(f"\n📊 数据完整性统计:")
    print(f"✅ 数据新鲜 (≤7天): {fresh_count}")
    print(f"⚠️ 数据陈旧 (8-30天): {stale_count}")
    print(f"🔄 数据过期 (>30天): {outdated_count}")
    print(f"❌ 数据缺失: {missing_count}")
    print(f"💥 数据损坏: {corrupted_count}")
    
    # 计算完整性百分比
    total_indicators = len(all_indicators)
    available_indicators = fresh_count + stale_count + outdated_count
    completeness = (available_indicators / total_indicators) * 100 if total_indicators > 0 else 0
    
    print(f"\n📈 数据完整性: {completeness:.1f}% ({available_indicators}/{total_indicators})")
    
    # 显示问题指标
    problem_indicators = [r for r in results if r['status'] in ['missing', 'corrupted', 'outdated']]
    
    if problem_indicators:
        print(f"\n⚠️ 需要处理的指标 ({len(problem_indicators)}个):")
        for status in problem_indicators:
            if status['status'] == 'missing':
                print(f"  ❌ {status['series_id']}: 数据缺失")
            elif status['status'] == 'corrupted':
                print(f"  💥 {status['series_id']}: 数据损坏")
            elif status['status'] == 'outdated':
                print(f"  🔄 {status['series_id']}: 数据过期 ({status['days_old']}天前)")
    
    # 显示数据陈旧的指标
    stale_indicators = [r for r in results if r['status'] == 'stale']
    if stale_indicators:
        print(f"\n⚠️ 数据陈旧的指标 ({len(stale_indicators)}个):")
        for status in stale_indicators:
            print(f"  ⚠️ {status['series_id']}: {status['days_old']}天前")
    
    return {
        'total_indicators': total_indicators,
        'completeness': completeness,
        'fresh_count': fresh_count,
        'stale_count': stale_count,
        'outdated_count': outdated_count,
        'missing_count': missing_count,
        'corrupted_count': corrupted_count,
        'problem_indicators': problem_indicators,
        'results': results
    }

def recommend_action(completeness_stats: Dict):
    """根据数据完整性推荐操作"""
    print(f"\n💡 操作建议:")
    print("=" * 60)
    
    completeness = completeness_stats['completeness']
    missing_count = completeness_stats['missing_count']
    outdated_count = completeness_stats['outdated_count']
    corrupted_count = completeness_stats['corrupted_count']
    
    if completeness >= 90:
        print("✅ 数据完整性良好，可以直接运行风险监控系统")
        print("💡 建议: 运行 python macrolab_gui.py 开始监控")
    elif completeness >= 70:
        print("⚠️ 数据完整性一般，建议先更新数据")
        if missing_count > 0:
            print(f"📥 需要下载 {missing_count} 个缺失指标")
        if outdated_count > 0:
            print(f"🔄 需要更新 {outdated_count} 个过期指标")
        print("💡 建议: 运行数据下载脚本后再进行监控")
    else:
        print("❌ 数据完整性不足，必须先下载数据")
        print(f"📥 需要下载 {missing_count} 个缺失指标")
        if corrupted_count > 0:
            print(f"💥 需要修复 {corrupted_count} 个损坏指标")
        print("💡 建议: 立即运行数据下载脚本")
    
    # 提供具体的运行命令
    print(f"\n🚀 推荐运行顺序:")
    print("1️⃣ 数据下载: python scripts/sync_fred_http.py")
    print("2️⃣ 数据检查: python check_data_completeness.py")
    print("3️⃣ 风险监控: python macrolab_gui.py")

def main():
    """主函数"""
    print("FRED数据完整性检查")
    print("=" * 60)
    
    # 检查数据完整性
    completeness_stats = check_all_indicators()
    
    if completeness_stats:
        # 推荐操作
        recommend_action(completeness_stats)
        
        # 保存检查结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = BASE / "outputs" / f"data_completeness_check_{timestamp}.json"
        result_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(completeness_stats, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n📄 检查结果已保存: {result_file}")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
