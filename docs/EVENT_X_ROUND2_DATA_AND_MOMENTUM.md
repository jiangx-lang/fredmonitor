# Event-X 第二轮：数据打通与逻辑补强

## 修改方案概述

本轮在保留既有 Event-X 架构与 Base Layer 的前提下，完成：

1. **Brent / T5YIE 数据链打通**：纳入同步列表、雷达 details 与报告可见性
2. **Geopolitics 数据完整性约束**：evaluate_geopolitics_data_completeness，单腿站立时明确提示
3. **Private Credit 动量型 Early Watch**：HY OAS 5D 变化 > +30bp 触发 Watch，details 含 used_inputs / missing_inputs / stlfsi_series_used
4. **STLFSI4 彻底统一**：config 与 radar details 仅 STLFSI4
5. **Machine Summary 单腿/动量表述**：Geopolitics 主要靠 VIX 时、Private Credit 靠 BIZD/动量时的 1–3 句说明

所有新增逻辑 **fail-open**，不因单数据源失败中断报告。

---

## 修改文件清单

| 文件 | 修改内容 |
|------|----------|
| `scripts/sync_fred_http.py` | `get_daily_factors_needed()` 增加 event_x_radar：DCOILBRENTEU, T5YIE, STLFSI4, CPIENGSL, GOLD*, DRTSCILM |
| `structural_risk.py` | Geopolitics: details 增加 brent_last/brent_yoy/t5yie_last/t5yie_sustained_alert, used_inputs, missing_inputs, *_last_valid_date, *_whether_used_in_radar；Private Credit: hy_oas_5d_bp_change, is_hy_momentum_watch, Watch 条件 +30bp，details 增加 hy_oas_last, stlfsi4_last, bizd_drawdown_50dma, used_inputs, missing_inputs, stlfsi_series_used, watch_triggered_by_momentum |
| `event_x_freshness.py` | 新增 `evaluate_geopolitics_data_completeness(details)` → core_inputs_available, completeness (HIGH/MEDIUM/LOW), summary |
| `crisis_monitor_v2.py` | 写入 json_data event_x_geopolitics_completeness、event_x_private_credit_detail；Event-X 区块增加 Geopolitics completeness、Data legs（Brent/T5YIE 当前值或 missing）；_build_event_x_machine_summary 改为单腿/动量 1–3 句，并接收 geopolitics_completeness |
| `config/indicators.yaml` | STLFSI3 → STLFSI4 |
| `config/indicators_cursor_optimized.yaml` | STLFSI3 → STLFSI4 |
| **新建** `tests/test_event_x_round2_data_and_momentum.py` | Smoke：Brent/T5YIE/STLFSI4 在 sync 列表、Geopolitics completeness LOW/HIGH、Private Credit details 含 stlfsi_series_used 与 hy_oas_5d_bp_change、Geopolitics details 含 brent_last/t5yie_last/used_inputs/missing_inputs |

---

## Task 1: Brent / T5YIE 数据链

- **sync_fred_http.py**：`get_daily_factors_needed()` 增加 `event_x_radar`，包含 DCOILBRENTEU、T5YIE、STLFSI4、CPIENGSL、GOLD*、DRTSCILM，确保 sync 会拉取并落盘。
- **structural_risk.check_geopolitics_inflation_radar()**：details 统一并扩展：
  - `brent_last`, `brent_yoy`, `t5yie_last`, `t5yie_sustained_alert`, `vix_last`, `cpi_energy_mom`
  - `used_inputs` / `missing_inputs`
  - 每条腿：`*_last_valid_date`, `*_data_source`, `*_whether_used_in_radar`
- **报告**：Event-X 顶部区块增加 **Data legs** 行：Brent 当前值/YoY、T5YIE 当前值/as-of 日期；若未使用则写 "missing / stale / unavailable"。

---

## Task 2: Geopolitics 数据完整性

- **event_x_freshness.evaluate_geopolitics_data_completeness(details)**：
  - 核心三条腿：Brent、T5YIE、VIX。
  - HIGH：三者均有可用值；MEDIUM：缺 1 或 1 条 stale；LOW：仅 VIX 或 Brent/T5YIE 均缺失。
  - summary 在 LOW 且仅 VIX 时："WATCH driven mainly by VIX; oil and breakeven inflation legs incomplete."
- **接入**：postprocess 中计算并写入 `json_data["event_x_geopolitics_completeness"]`；Event-X 区块显示 **Geopolitics completeness**；Machine Summary 在 WATCH + LOW 时使用上述单腿表述。

---

## Task 3: Private Credit 动量型 Early Watch

- **structural_risk.check_private_credit_liquidity_radar()**：
  - `hy_oas_5d_bp_change` = (last - 5 日前) * 100 bp；`is_hy_momentum_watch` = 5D change > 30bp。
  - 保留原有 Watch（HY>4.5%、STLFSI4>0、BIZD<-5%），**新增**：`is_hy_momentum_watch` 时也设 Watch，reason 含 "HY OAS 5D widening +XXbp"。
  - details：`hy_oas_last`, `hy_oas_5d_bp_change`, `stlfsi4_last`, `bizd_drawdown_50dma`, `used_inputs`, `missing_inputs`, `stlfsi_series_used` = "STLFSI4", `watch_triggered_by_momentum`.
- **报告**：若 WATCH 由动量触发，Machine Summary 中写 "Private credit watch is supported by HY spread widening momentum (5D +XXbp), while absolute credit spreads remain benign."

---

## Task 4: STLFSI4 彻底统一

- **config**：`crisis_indicators.yaml`（首轮已改）、`indicators.yaml`、`indicators_cursor_optimized.yaml` 中 STLFSI3 → STLFSI4。
- **雷达**：Private Credit details 显式返回 `stlfsi_series_used`: "STLFSI4"。
- **报告**：新报告由 crisis_indicators 驱动，指标列表与热力图为 STLFSI4；历史 outputs 未改。

---

## Task 5: Machine Summary 单腿/动量表述

- Geopolitics WATCH 且 completeness = LOW：  
  "Geopolitics watch is currently driven mainly by VIX; oil and breakeven inflation legs remain incomplete."
- Private Credit WATCH 且为动量触发：  
  "Private credit watch is supported by HY spread widening momentum (5D +XXbp), while absolute credit spreads remain benign."
- Private Credit WATCH 且为 BIZD/STLFSI4：  
  "Private credit watch is supported by BIZD weakness and/or HY spread widening momentum, while absolute credit spreads remain benign."
- Resonance = OFF：  
  "No systemic resonance is confirmed."
- 最后补一句 Signal confidence 与可选 Freshness risk；整体 1–3 句。

---

## 集成与 JSON

- `json_data["event_x_geopolitics_completeness"]` = evaluate_geopolitics_data_completeness(geo_details)
- `json_data["event_x_private_credit_detail"]` = 从 pc details 抽取 hy_oas_last, hy_oas_5d_bp_change, stlfsi4_last, bizd_drawdown_50dma, used_inputs, missing_inputs, stlfsi_series_used, watch_triggered_by_momentum
- 异常时写入默认空/安全结构，不中断报告。

---

## Smoke Test

```bash
py tests/test_event_x_round2_data_and_momentum.py
```

覆盖：Brent/T5YIE/STLFSI4 在 sync 需要列表；Geopolitics 仅 VIX → LOW、三腿齐全 → HIGH；Private Credit details 含 stlfsi_series_used=STLFSI4 与 hy_oas_5d_bp_change；Geopolitics details 含 brent_last、t5yie_last、used_inputs、missing_inputs；crisis_indicators 仅 STLFSI4。
