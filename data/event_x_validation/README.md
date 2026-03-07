# Event-X 历史验证数据

本目录由 `scripts/build_event_x_validation_dataset.py` 生成；占位列由 `scripts/run_event_x_daily_replay.py` 逐日重放规则填实。

## 文件

| 文件 | 说明 |
|------|------|
| event_x_validation_dataset.csv | 日频原始主腿（date, hy_oas, stlfsi4, bizd_close, brent_spot, t5yie, vix, dgs5, t5yifr, breakeven_effective） |
| event_x_validation_features.csv | 衍生特征（占位列未填，仅作输入） |
| event_x_validation_features_filled.csv | 重放后：private_credit_watch/alert_flag, geopolitics_watch/alert_flag, resonance_level, signal_confidence, geopolitics_completeness 已填实 |
| event_x_validation_scenarios.csv | 四类场景标签（oil_shock, credit_widening, vol_no_resonance, true_resonance） |
| event_x_validation_scenario_results.csv | 按窗口聚合：验证状态(NOT_TESTED/TESTED)、首次触发日、首次升级日、峰值等级、提前/滞后、是否误报过多 |
| event_x_validation_vol_window_daily_legs.csv | 2021-09~11 逐日触发腿小表：日期、PC/Geo/Res 状态、每条腿是否 True、completeness、signal_confidence |
| event_x_validation_denoising_before_after_vol.csv | 2021-09~11 去噪前后逐日对比：Geopolitics/Resonance before vs after，腿_breakeven/VIX/Brent、completeness、signal_confidence |
| event_x_validation_denoising_before_after_vol_summary.csv | Vol 窗口汇总：去噪前后 Geo ALERT/ALARM 天数、Res LEVEL1+ 天数，验证误报是否减少 |
| event_x_validation_denoising_before_after_oil_shock_summary.csv | Oil shock 窗口汇总：去噪前后同上，验证真实场景是否被压没 |

## 数据源

- FRED：BAMLH0A0HYM2, STLFSI4, DCOILBRENTEU, T5YIE, VIXCLS, DGS5（T5YIFR 若本地无则列为空）
- Yahoo Finance：BIZD 收盘价

## 重建与重放

```bash
# 1) 产出原始表 + 特征表 + 场景表
py scripts/build_event_x_validation_dataset.py --start 2021-01-01 --out-dir data/event_x_validation

# 2) 逐日重放 Event-X 规则，填实占位列并产出场景聚合
py scripts/run_event_x_daily_replay.py --features data/event_x_validation/event_x_validation_features.csv --scenarios data/event_x_validation/event_x_validation_scenarios.csv --out-dir data/event_x_validation
```

**说明**：dataset 若从 2021-01-01 起，则 2020 年窗口（credit_widening / true_resonance）内无数据，首次触发日/首次升级日为空，属正常。详细字段见 `docs/EVENT_X_VALIDATION_DATA_TEMPLATE.md`。
