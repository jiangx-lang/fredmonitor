# scripts/plot_one.py
# -*- coding: utf-8 -*-
import pathlib
import sys
import os

# 添加项目根目录到路径
BASE = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

from scripts.crisis_monitor import (
    load_yaml_config, get_series_data, transform_series, calculate_crisis_stats
)
from scripts.viz import save_indicator_plot

if __name__ == "__main__":
    series_id = "HOUST"
    name = "新屋开工"
    unit = "千套"  # 你喜欢的单位提示

    crises = load_yaml_config(BASE / "config" / "crisis_periods.yaml")["crises"]
    s = get_series_data(series_id)
    ts = transform_series(s, "level").dropna()
    cstats = calculate_crisis_stats(ts, crises)

    out = BASE / "outputs" / "crisis_monitor" / "figures" / f"{series_id}_latest.png"
    save_indicator_plot(ts, f"{name} ({series_id})", unit, crises, cstats, out)
    print(f"Saved: {out}")
