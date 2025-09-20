#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试中文字体显示"""
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np

def test_chinese_fonts():
    """测试中文字体"""
    print("🔍 检查系统中的中文字体...")
    
    # 获取所有字体
    all_fonts = [f.name for f in fm.fontManager.ttflist]
    
    # 查找中文字体
    chinese_fonts = []
    for font in all_fonts:
        if any(keyword in font.lower() for keyword in ['simhei', 'microsoft yahei', 'arial unicode ms', 'noto', 'source han']):
            chinese_fonts.append(font)
    
    print(f"📋 找到的中文字体: {chinese_fonts}")
    
    # 测试字体显示
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # 测试文本
    test_text = "测试中文显示：密歇根消费者信心指数"
    
    # 尝试不同的字体设置
    font_configs = [
        ['SimHei'],
        ['Microsoft YaHei'], 
        ['Arial Unicode MS'],
        ['DejaVu Sans'],
        chinese_fonts[:1] if chinese_fonts else ['DejaVu Sans']
    ]
    
    for i, font_list in enumerate(font_configs):
        try:
            plt.rcParams['font.sans-serif'] = font_list
            ax.text(0.1, 0.8 - i*0.15, f"字体 {font_list[0]}: {test_text}", 
                   fontsize=12, transform=ax.transAxes)
            print(f"✅ 字体 {font_list[0]} 测试成功")
        except Exception as e:
            print(f"❌ 字体 {font_list[0]} 测试失败: {e}")
    
    ax.set_title("中文字体测试")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # 保存测试图片
    plt.tight_layout()
    plt.savefig('chinese_font_test.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    print("📸 测试图片已保存为 chinese_font_test.png")

if __name__ == "__main__":
    test_chinese_fonts()


