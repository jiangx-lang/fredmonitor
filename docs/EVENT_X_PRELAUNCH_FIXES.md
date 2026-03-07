# Event-X 上线前修复与一致性加固

## 修改方案概述

本次仅做**口径统一、新鲜度分层、信号可信度、顶部摘要可解释性**增强，**未修改** Base Layer 评分逻辑/权重、未改动 Event-X 两雷达与 Resonance Trigger 的核心阈值逻辑。所有新增逻辑 **fail-open**。

---

## 涉及文件列表

| 文件 | 修改内容 |
|------|----------|
| `config/crisis_indicators.yaml` | STLFSI3 → STLFSI4（唯一口径） |
| `crisis_monitor.py` | indicator_explanations STLFSI3 → STLFSI4 |
| `core/database_integration.py` | 映射与权重 STLFSI3 → STLFSI4 |
| `data/series/data_catalog.py` | index/weekly 列表 STLFSI3 → STLFSI4 |
| `check_specific_fred_series.py` | 综合金融压力指数 STLFSI3 → STLFSI4；alternative_checks STLFSI4 |
| `check_crisis_detection_data_coverage.py` | 金融压力指数 STLFSI → STLFSI4 |
| `scripts/supplement_crisis_detection_data.py` | STLFSI → STLFSI4 |
| `scripts/supplement_missing_specific_series.py` | STLFSI3 → STLFSI4 |
| **新建** `event_x_freshness.py` | 分层新鲜度 `evaluate_data_freshness_severity`、信号可信度 `evaluate_event_x_signal_confidence` |
| `crisis_monitor_v2.py` | 接入 freshness/confidence、扩展 Event-X 区块（Signal Confidence、Freshness Risk、指挥台风格 Machine Summary） |

---

## Task 1: STLFSI 口径统一

- **报告层 / 逻辑层 / 配置层** 统一为 **STLFSI4**。
- **配置**：`crisis_indicators.yaml` 中指标 `id` 由 `STLFSI3` 改为 `STLFSI4`，名称仍为「圣路易斯金融压力」。
- **Base 报告**：`series_id` 来自配置 `id`，因此报告与图表列表自然显示 STLFSI4；`crisis_monitor.py` 中 `indicator_explanations` 键改为 `STLFSI4`。
- **数据**：`catalog_fred.yaml` 已包含 STLFSI4，同步脚本按 catalog 拉取；`structural_risk.py` 与 `event_x_resonance.py` 已使用 STLFSI4，未改。
- **一致性**：全项目检索并替换脚本/检查脚本中的 STLFSI3 或 STLFSI 为 STLFSI4（或兼容层仅作 alias），**禁止报告同时出现 STLFSI3 与 STLFSI4**。

---

## Task 2: 分层 Freshness 严重度

- **新模块** `event_x_freshness.py`：
  - **CRITICAL**：BAMLH0A0HYM2, STLFSI4, DCOILBRENTEU, T5YIE, VIXCLS, CPIENGSL, DRTSCILM, BIZD（若存在）。
  - **IMPORTANT**：GOLD/SPX 相关等结构性雷达输入。
  - **INFO**：其余 stale 仅提示。
- **滞后规则**（按序列频率）：
  - 日频 D: 0–5 天 FRESH，6–14 天 AGING，>14 天 STALE
  - 周频 W: 0–14 天 FRESH，15–35 天 AGING，>35 天 STALE
  - 月频 M: 0–45 天 FRESH，46–75 天 AGING，>75 天 STALE
  - 季频 Q: 0–120 天 FRESH，121–180 天 AGING，>180 天 STALE
- **event_x_freshness_risk**：任意 CRITICAL STALE → 至少 MEDIUM；2+ CRITICAL STALE → HIGH；若 Resonance 依赖核心腿存在 stale，在 `summary` 中说明。
- **返回结构**：`critical` / `important` / `info`（每项含 series_id, last_date, lag_days, freq, severity）、`event_x_freshness_risk`、`summary`。

---

## Task 3: Event-X Signal Confidence

- **新函数** `evaluate_event_x_signal_confidence(struct_results, resonance_result, freshness_result)`：
  - **HIGH**：Event-X 核心 CRITICAL 均为 FRESH/少量 AGING，无关键输入缺失，BIZD/Gold patch 有或缺失不影响核心闭环。
  - **MEDIUM**：1 个核心 STALE，或某 patch 缺失但 FRED 主腿完整，或使用 fallback/最近有效值。
  - **LOW**：2+ 核心 STALE，或 Resonance 核心腿明显缺口，或多补丁缺失。
- **输出**：`confidence`（HIGH/MEDIUM/LOW）、`reasons`、`summary`。
- **展示**：在 Event-X 置顶区块中增加一行 **Signal Confidence**: …

---

## Task 4: Machine Summary 指挥台风格

- **新函数** `_build_event_x_machine_summary(...)`：根据 Private Credit / Geopolitics 雷达 details、Resonance level、freshness_result、confidence_result 拼接 **1–2 句** 状态+原因+可信度。
- **内容**：为何当前未升级 / 为何有风险苗头（如 HY OAS、BIZD、STLFSI4、VIX、通胀/油价确认缺失等）；Low-frequency 沿用最近已知值说明；No systemic resonance；Signal confidence: HIGH/MEDIUM/LOW；若 Freshness risk HIGH 则补充说明。
- **风格**：简洁、专业、适合投委会晨会；不重复状态枚举。

---

## 主流程接入

- 在 `postprocess_reports()` 中，在得到 `struct_results`、`resonance_result` 后：
  1. 调用 `evaluate_data_freshness_severity(json_data, struct_results, resonance_result, indicators_config)` → 写入 `json_data["event_x_freshness"]`。
  2. 调用 `evaluate_event_x_signal_confidence(struct_results, resonance_result, freshness_result)` → 写入 `json_data["event_x_signal_confidence"]`。
  3. `_build_event_x_priority_risks_section(..., freshness_result, confidence_result)` 生成区块，内含：
     - Private Credit Liquidity Radar / Geopolitics & Inflation Radar / Resonance Trigger
     - **Signal Confidence** / **Freshness Risk**
     - *Machine Summary:* …（指挥台风格）
- 异常时写入默认 `event_x_freshness`、`event_x_signal_confidence`，不中断报告生成。

---

## 报告区块示例（Markdown）

```markdown
## 🔴 Event-X Priority Risks

- **Private Credit Liquidity Radar**: NORMAL
- **Geopolitics & Inflation Radar**: WATCH
- **Resonance Trigger**: OFF
- **Signal Confidence**: MEDIUM
- **Freshness Risk**: LOW

*Machine Summary:*
Private credit 利差温和. Geopolitics/inflation 风险抬升. Low-frequency (沿用最近已知值): T5YIE as of 2025-01-20. No systemic resonance. Signal confidence: MEDIUM.
```

---

## Smoke Test 建议

运行：

```bash
py tests/test_event_x_freshness_and_confidence.py
```

覆盖：

1. **STLFSI 口径**：配置中仅含 STLFSI4，不含 STLFSI3。
2. **Critical stale**：空 indicators 或 2+ CRITICAL 陈旧 → event_x_freshness_risk 至少 MEDIUM / 2+ 为 HIGH。
3. **Patch 缺失**：struct/resonance/freshness 缺省时 confidence 仍返回有效值（fail-open）。
4. **Signal confidence**：全 FRESH → HIGH 或 MEDIUM；1 STALE → MEDIUM/LOW；2+ STALE 或 Resonance+stale → LOW。
5. **输入 None**：`evaluate_data_freshness_severity(None, ...)`、`evaluate_event_x_signal_confidence(None, None, None)` 不抛错，返回默认结构。

---

## 安全与稳健

- 所有新函数允许输入缺失、空 dict、NaN；`.get()` 链式读取安全。
- 不因单序列或 patch 失败中断报告；Structural/Event-X 异常时写入默认 freshness/confidence 并继续。
- 时间滞后基于「最后有效观测日」；低频数据不伪造「今日已更新」，保留真实最新观测日期并在 Summary 中暴露（如 DRTSCILM/CPIENGSL/T5YIE as of …）。
