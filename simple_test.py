#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""简单图表测试"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import pathlib

def test_matplotlib():
    """测试matplotlib基本功能"""
    print("测试matplotlib基本功能...")
    
    # 创建测试数据
    dates = pd.date_range('2020-01-01', periods=50, freq='M')
    values = np.random.randn(50).cumsum() + 100
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.plot(dates, values, linewidth=1.5, label='Test Data')
    ax.set_title('Test Chart', fontsize=12)
    ax.set_ylabel('Value', fontsize=10)
    ax.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best", fontsize=8)
    
    # 保存图表
    out_path = pathlib.Path("simple_test.png")
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    
    # 检查文件
    if out_path.exists():
        file_size = out_path.stat().st_size
        print(f"✅ 图表已保存: {out_path}")
        print(f"📊 文件大小: {file_size:,} bytes")
        
        if file_size > 10000:
            print("✅ 图表文件大小正常")
            return True
        else:
            print("❌ 图表文件过小")
            return False
    else:
        print("❌ 图表文件未生成")
        return False

if __name__ == "__main__":
    success = test_matplotlib()
    if success:
        print("\n🎉 matplotlib测试成功！")
    else:
        print("\n❌ matplotlib测试失败！")