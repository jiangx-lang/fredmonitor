# Event-X Code Review & Acceptance Checklist

**总纲**：Round 3 最大的成功，不是多加了一个 breakeven proxy，而是让系统终于学会区分「数据存在」与「信号有效」；下一步要做的，是把这种判断能力固化成可维护、可解释、可验证的标准。

本文档用于：上线前固化、Code Review、防止回退、维护者自检。

---

## 1) Non-regression / 不允许回退

| 项 | 要求 | 验收方式 |
|----|------|----------|
| STLFSI4 唯一口径 | 报告、freshness、图表、mapping 中不得再出现 STLFSI3 | 全项目 grep STLFSI3 仅允许历史 outputs / 文档说明 / 测试中的断言说明 |
| Private Credit 使用 STLFSI4 | details 中必须有 `stlfsi_series_used`: "STLFSI4" | 检查 structural_risk 与 json event_x_private_credit_detail |
| Signal Confidence 保留 | 顶部区块必须显示 Signal Confidence | 不得删除 _build_event_x_priority_risks_section 中该行 |
| Freshness Risk 保留 | 顶部区块必须显示 Freshness Risk | 同上 |
| Geopolitics completeness 为「有效可用」 | 不得退回「有字段即 HIGH」；必须使用 core_inputs_effective 与 breakeven_is_stale | evaluate_geopolitics_data_completeness 必须接受 details.breakeven_is_stale |
| breakeven_source_used 存在 | Geopolitics details 必须包含 breakeven_source_used | structural_risk 与 json 中可见 |
| event_x_status_quality 存在 | json_data 必须包含 event_x_status_quality | postprocess 中必须写入 |
| Machine Summary 非退化 | 不得退化为只写 WATCH / OFF；必须包含触发腿、未确认腿、stale 说明 | _build_event_x_machine_summary 必须输出多句解释 |
| Brent 接入保留 | Data legs 中 Brent 当前值/YoY 可见；不触发时解释为「Brent is available but does not confirm oil shock」 | 报告与 summary 中可查 |
| 动量逻辑保留 | Private Credit 必须保留 HY 5D 动量 Watch；不得回退为只看绝对水平 | details 含 hy_oas_5d_bp_change、watch_triggered_by_momentum |

---

## 2) Required fields / 必须存在字段

### 2.1 顶层 json_data

- `event_x_resonance`（含 level, detail, summary）
- `event_x_freshness`（含 critical, important, info, event_x_freshness_risk, summary）
- `event_x_signal_confidence`（含 confidence, reasons, summary）
- `event_x_geopolitics_completeness`（含 core_inputs_present, core_inputs_effective, completeness, summary, missing_or_weak_legs）
- `event_x_status_quality`（含 private_credit, geopolitics，各有 fixed_items, remaining_weaknesses）
- `event_x_acceptance_status`（见下文结构化 checklist）
- `event_x_maintainer_summary`（见 Task 5）

### 2.2 Private Credit details（structural_risk / event_x_private_credit_detail）

- `hy_oas_last`
- `hy_oas_5d_bp_change`
- `bizd_drawdown_50dma` 或 `bizd_vs_50dma_pct`
- `stlfsi4_last`
- `stlfsi_series_used` = "STLFSI4"
- `used_inputs`
- `missing_inputs`

### 2.3 Geopolitics details

- `brent_last`
- `brent_yoy` 或 `brent_yoy_pct`
- `breakeven_last` 或 `breakeven_effective_last`
- `breakeven_source_used`
- `breakeven_last_date`
- `breakeven_is_stale`
- `breakeven_quality`
- `vix_last`
- `used_inputs`
- `missing_inputs`

---

## 3) Stale 条件下降级规则

| 条件 | 必须结果 |
|------|----------|
| Breakeven 核心腿 stale | Geopolitics completeness ≠ HIGH |
| ≥2 个 critical inputs stale | event_x_freshness_risk 至少 MEDIUM；Signal Confidence 应为 LOW（或 MEDIUM） |
| Geopolitics WATCH 且主要由 VIX 触发，Brent/breakeven 未确认 | summary 中必须显式出现「VIX-led」或「driven mainly by VIX」 |
| Proxy 失败且 FRED breakeven stale | Freshness risk 不得仍为 LOW（应 MEDIUM 或 HIGH） |

---

## 4) Smoke tests / 必须通过的最小测试

| 测试 | 文件/命令 |
|------|------------|
| STLFSI4 成功替代 STLFSI3 | `py tests/test_event_x_freshness_and_confidence.py`（config 仅 STLFSI4） |
| Brent 有值但不触发 | `tests/test_event_x_round2_data_and_momentum.py` |
| FRED breakeven stale | `tests/test_event_x_round3_breakeven_and_completeness.py`（completeness 非 HIGH） |
| Realtime breakeven proxy 成功 | 有 DGS5/T5YIFR 且 T5YIE 过期时 breakeven_source_used = COMPUTED_DGS5_T5YIFR |
| Proxy 失败后回退 FRED | event_x_breakeven.get_realtime_5y_breakeven_proxy_safe 返回 FRED_T5YIE 或 NONE，不抛错 |
| Geopolitics 仅 VIX 触发 WATCH 时 completeness ≠ HIGH | evaluate_geopolitics_data_completeness 当 breakeven_is_stale 时 PARTIAL/LOW |
| Private Credit 由 BIZD + HY momentum 触发 WATCH，绝对水平仍 benign | Machine Summary 含「absolute spreads and STLFSI4 remain benign」 |
| Patch 失败不影响报告生成 | 任意 Event-X 子模块异常时 postprocess 不中断，写入默认结构 |

**建议测试矩阵**（最小集）：

| # | 测试 | 命令 |
|---|------|------|
| 1 | STLFSI4 / 新鲜度与可信度 | `py tests/test_event_x_freshness_and_confidence.py` |
| 2 | 数据腿与动量（Brent/BIZD/HY） | `py tests/test_event_x_round2_data_and_momentum.py` |
| 3 | Breakeven 与 completeness | `py tests/test_event_x_round3_breakeven_and_completeness.py` |
| 4 | 历史场景验证脚本 | `py scripts/run_event_x_historical_validation.py --dir outputs/crisis_monitor` |

```bash
py tests/test_event_x_freshness_and_confidence.py
py tests/test_event_x_round2_data_and_momentum.py
py tests/test_event_x_round3_breakeven_and_completeness.py
py scripts/run_event_x_historical_validation.py --dir outputs/crisis_monitor
```

---

## 5) 结构化 checklist（机器可读）

`json_data["event_x_acceptance_status"]` 由 `event_x_acceptance.run_acceptance_checks(json_data)` 生成，结构示例：

```json
{
  "non_regression": {
    "signal_confidence_present": true,
    "freshness_risk_present": true,
    "completeness_effective_not_field_only": true,
    "breakeven_source_used_exists": true,
    "event_x_status_quality_exists": true,
    "stlfsi_series_used_is_stlfsi4": true
  },
  "required_fields": {
    "top_level": true,
    "private_credit_details": true,
    "geopolitics_details": true
  },
  "stale_downgrade_rules": {
    "breakeven_stale_then_completeness_not_high": true,
    "two_critical_stale_then_confidence_low_or_freshness_high": true,
    "vix_led_when_partial_must_say": true
  },
  "smoke_tests_ready": true
}
```

---

## 6) 发布标准 vs 必须回滚

- **可以发布**：`event_x_maintainer_summary.non_regression_passed` 与 `required_fields_present` 为 True；`stale_downgrade_rules_passed` 为 True；smoke tests 全部通过；Reading Guide 与 plain-English 层已接入。
- **必须回滚 / 修复后再发布**：任一「不允许回退」项被破坏；必选字段缺失；stale 时仍显示 completeness HIGH 或 Freshness LOW；Machine Summary 退化为仅状态枚举；或 smoke tests 失败。
