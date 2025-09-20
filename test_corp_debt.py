#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试企业债/GDP计算
"""

import sys
import pathlib
import pandas as pd
import numpy as np

# 添加项目路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

from crisis_monitor import load_local_series_data, fetch_series_from_api

def test_corp_debt_calculation():
    """测试企业债/GDP计算"""
    print("🧪 测试企业债/GDP计算...")
    
    # 获取企业债数据
    corp = load_local_series_data("NCBDBIQ027S")
    if corp is None:
        corp = fetch_series_from_api("NCBDBIQ027S")
    print(f"企业债数据: {corp is not None}, 长度: {len(corp) if corp is not None else 0}")
    if corp is not None:
        print(f"企业债最新值: {corp.iloc[-1] if not corp.empty else 'N/A'}")
        print(f"企业债最新日期: {corp.index[-1] if not corp.empty else 'N/A'}")
    
    # 获取GDP数据
    gdp = load_local_series_data("GDP")
    if gdp is None:
        gdp = fetch_series_from_api("GDP")
    print(f"GDP数据: {gdp is not None}, 长度: {len(gdp) if gdp is not None else 0}")
    if gdp is not None:
        print(f"GDP最新值: {gdp.iloc[-1] if not gdp.empty else 'N/A'}")
        print(f"GDP最新日期: {gdp.index[-1] if not gdp.empty else 'N/A'}")
    
    if corp is None or gdp is None or corp.empty or gdp.empty:
        print("❌ 数据获取失败")
        return None
    
    try:
        # 统一到季度末
        corp_q = corp.resample("Q").last().astype("float64")
        gdp_q = (gdp * 1000.0).resample("Q").last().astype("float64")
        
        print(f"企业债季度数据长度: {len(corp_q)}")
        print(f"GDP季度数据长度: {len(gdp_q)}")
        
        # 确保两个序列有相同的索引
        common_index = corp_q.index.intersection(gdp_q.index)
        print(f"共同索引长度: {len(common_index)}")
        
        if len(common_index) == 0:
            print("❌ 没有共同索引")
            return None
            
        corp_aligned = corp_q.loc[common_index]
        gdp_aligned = gdp_q.loc[common_index]
        
        print(f"对齐后企业债长度: {len(corp_aligned)}")
        print(f"对齐后GDP长度: {len(gdp_aligned)}")
        
        ratio = (corp_aligned / gdp_aligned) * 100.0
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        
        print(f"比率数据长度: {len(ratio)}")
        print(f"最新比率: {ratio.iloc[-1] if not ratio.empty else 'N/A'}")
        print(f"最新日期: {ratio.index[-1] if not ratio.empty else 'N/A'}")
        
        return ratio
        
    except Exception as e:
        print(f"❌ 计算失败: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = test_corp_debt_calculation()
    if result is not None:
        print("✅ 企业债/GDP计算成功")
    else:
        print("❌ 企业债/GDP计算失败")
