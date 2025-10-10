# src/validator.py - 核心校验脚本
from pathlib import Path
import yaml
import json
import re

def load_yaml(p): 
    with open(p, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def list_figure_ids(fig_dir: Path):
    """从figures目录获取所有指标ID"""
    ids = []
    for p in fig_dir.glob("*_latest.png"):
        # e.g., CSUSHPINSA_latest.png -> CSUSHPINSA
        ids.append(p.name.replace("_latest.png",""))
    return sorted(set(ids))

def build_config_index(cfg):
    """构建配置索引"""
    idx = {}
    for it in cfg["indicators"]:
        idx[it["id"]] = it
    return idx

def validate(fig_dir, config_yaml, json_path, md_path=None):
    """核心校验函数"""
    cfg = load_yaml(config_yaml)
    cfg_idx = build_config_index(cfg)
    fig_ids = list_figure_ids(Path(fig_dir))

    # 读取JSON文件
    with open(json_path, 'r', encoding='utf-8') as f:
        j = json.load(f)
    json_ids = [x["series_id"] for x in j["indicators"]]

    # 交叉校验
    missing_in_config   = [x for x in fig_ids  if x not in cfg_idx]
    missing_in_figures  = [x for x in json_ids if x not in fig_ids]
    missing_in_json     = [x for x in fig_ids  if x not in json_ids]

    # transform/方向自检：名称含YoY但transform≠yoy_pct
    transform_conflicts = []
    for sid, meta in cfg_idx.items():
        if re.search(r"\bYoY\b", meta.get("name",""), re.I):
            if meta.get("transform") != "yoy_pct":
                transform_conflicts.append((sid, meta.get("transform")))

    # 权重检查
    weights_anomalies = []
    total_weight = sum(item.get("weight", 0) for item in cfg["indicators"])
    if abs(total_weight - 1.0) > 0.01:
        weights_anomalies.append(f"总权重不为1.0: {total_weight:.3f}")
    
    zero_weights = [item["id"] for item in cfg["indicators"] if item.get("weight", 0) == 0]
    if zero_weights:
        weights_anomalies.append(f"零权重指标: {zero_weights}")

    result = {
        "fig_count": len(fig_ids),
        "config_count": len(cfg_idx),
        "json_indicator_count": len(json_ids),
        "missing_in_config": missing_in_config,
        "missing_in_figures": missing_in_figures,
        "missing_in_json": missing_in_json,
        "transform_conflicts": transform_conflicts,
        "weights_anomalies": weights_anomalies,
        "total_weight": total_weight
    }
    return result

def print_validation_report(result):
    """打印校验报告"""
    print("=" * 60)
    print("📊 校验报告")
    print("=" * 60)
    print(f"📈 figures目录图表数量: {result['fig_count']}")
    print(f"📋 配置文件指标数量: {result['config_count']}")
    print(f"📄 JSON文件指标数量: {result['json_indicator_count']}")
    print(f"⚖️ 总权重: {result['total_weight']:.3f}")
    
    if result['missing_in_config']:
        print(f"\n❌ figures中有但配置文件中没有的指标 ({len(result['missing_in_config'])}个):")
        for i, sid in enumerate(result['missing_in_config'], 1):
            print(f"  {i:2d}. {sid}")
    
    if result['missing_in_figures']:
        print(f"\n❌ 配置文件中提到但figures中没有的指标 ({len(result['missing_in_figures'])}个):")
        for i, sid in enumerate(result['missing_in_figures'], 1):
            print(f"  {i:2d}. {sid}")
    
    if result['missing_in_json']:
        print(f"\n❌ figures中有但JSON中没有的指标 ({len(result['missing_in_json'])}个):")
        for i, sid in enumerate(result['missing_in_json'], 1):
            print(f"  {i:2d}. {sid}")
    
    if result['transform_conflicts']:
        print(f"\n⚠️ 变换冲突 ({len(result['transform_conflicts'])}个):")
        for sid, transform in result['transform_conflicts']:
            print(f"  - {sid}: 名称含YoY但transform={transform}")
    
    if result['weights_anomalies']:
        print(f"\n⚠️ 权重异常:")
        for anomaly in result['weights_anomalies']:
            print(f"  - {anomaly}")
    
    if not any([result['missing_in_config'], result['missing_in_figures'], 
                result['missing_in_json'], result['transform_conflicts'], 
                result['weights_anomalies']]):
        print("\n✅ 所有校验通过！")

if __name__ == "__main__":
    # 测试校验
    result = validate(
        fig_dir="figures",
        config_yaml="config/indicators.yaml", 
        json_path="outputs/crisis_monitor/crisis_report_20250921_134721.json"
    )
    print_validation_report(result)
