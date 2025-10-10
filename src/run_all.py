# src/run_all.py - 一键运行脚本
import os
import sys
import json
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from validator import validate, print_validation_report
from report_md import build_report_md
from scoring import recompute_scores

def main():
    """主函数"""
    print("🚀 FRED 危机预警系统 - 一键重构")
    print("=" * 60)
    
    # 配置文件路径
    CFG = "config/indicators.yaml"
    SC = "config/scoring.yaml"
    CP = "config/crisis_periods.yaml"
    FIG = "outputs/crisis_monitor/figures"
    JIN = "outputs/crisis_monitor/crisis_report_20250921_134721.json"  # 原始JSON
    JOUT = "outputs/crisis_monitor/latest.json"  # 输出JSON
    MOUT = "outputs/crisis_monitor/latest.md"    # 输出MD
    
    # 检查文件是否存在
    required_files = [CFG, SC, CP, JIN]
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ 缺少必要文件:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        return
    
    if not os.path.exists(FIG):
        print(f"❌ 图表目录不存在: {FIG}")
        return
    
    print("✅ 所有必要文件存在")
    print()
    
    # 1. 校验
    print("🔍 步骤1: 数据校验")
    print("-" * 30)
    
    try:
        validation_result = validate(FIG, CFG, JIN)
        print_validation_report(validation_result)
        
        # 将校验结果保存到JSON数据中
        with open(JIN, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        json_data['validation'] = validation_result
        
    except Exception as e:
        print(f"❌ 校验失败: {e}")
        return
    
    print()
    
    # 2. 重新计算评分
    print("📊 步骤2: 重新计算评分")
    print("-" * 30)
    
    try:
        output_json = recompute_scores(
            config_yaml=CFG,
            scoring_yaml=SC,
            crisis_yaml=CP,
            figures_dir=FIG,
            raw_json_path=JIN
        )
        
        # 添加校验结果
        output_json['validation'] = validation_result
        
        # 保存JSON
        os.makedirs(os.path.dirname(JOUT), exist_ok=True)
        with open(JOUT, 'w', encoding='utf-8') as f:
            json.dump(output_json, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON输出: {JOUT}")
        print(f"📊 总指标数: {output_json.get('total_indicators', 0)}")
        print(f"📊 总分: {output_json.get('total_score', 0):.1f}")
        print(f"📊 风险等级: {output_json.get('risk_level', 'unknown')}")
        
    except Exception as e:
        print(f"❌ 评分计算失败: {e}")
        return
    
    print()
    
    # 3. 生成报告
    print("📝 步骤3: 生成报告")
    print("-" * 30)
    
    try:
        md_content = build_report_md(
            json_data=output_json,
            config_yaml=CFG,
            scoring_yaml=SC,
            crisis_yaml=CP,
            figures_dir=FIG
        )
        
        # 保存Markdown
        os.makedirs(os.path.dirname(MOUT), exist_ok=True)
        with open(MOUT, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        print(f"✅ Markdown输出: {MOUT}")
        print(f"📝 报告长度: {len(md_content)} 字符")
        
    except Exception as e:
        print(f"❌ 报告生成失败: {e}")
        return
    
    print()
    
    # 4. 总结
    print("🎉 重构完成!")
    print("=" * 60)
    print(f"📄 JSON报告: {JOUT}")
    print(f"📝 Markdown报告: {MOUT}")
    print(f"📊 总指标数: {output_json.get('total_indicators', 0)}")
    print(f"📊 总分: {output_json.get('total_score', 0):.1f}")
    print(f"📊 风险等级: {output_json.get('risk_level', 'unknown')}")
    
    # 显示关键问题
    validation = output_json.get('validation', {})
    if validation.get('missing_in_config'):
        print(f"⚠️ 需要添加到配置的指标: {len(validation['missing_in_config'])}个")
    if validation.get('missing_in_figures'):
        print(f"⚠️ 需要生成图表的指标: {len(validation['missing_in_figures'])}个")
    if validation.get('transform_conflicts'):
        print(f"⚠️ 需要修正的变换冲突: {len(validation['transform_conflicts'])}个")
    
    print()
    print("💡 下一步建议:")
    print("1. 检查生成的报告文件")
    print("2. 根据校验结果修正配置")
    print("3. 重新运行以验证修复")

if __name__ == "__main__":
    main()
