# Event-X 第三轮：Breakeven 失真修复与 Completeness 有效可用性

## 修改方案概述

在**不修改 Base 总分/权重、不推翻三层架构、不回退已修部分**的前提下，本轮只做：

1. **T5YIE 失真死穴**：新增 5Y breakeven 实时/计算代理，Geopolitics 优先用更新更近的 breakeven；报告区分 FRED vs proxy 并标 stale。
2. **Completeness 逻辑修正**：从「字段存在」改为「有效可用」；枚举改为 HIGH / PARTIAL / LOW；stale 核心腿必须降低 completeness。
3. **Machine Summary 指挥台化**：明确写出触发腿、未确认腿、失真腿；点名 breakeven stale。
4. **Private Credit 解释强化**：保留动量/BIZD，补充「absolute spreads and STLFSI4 remain benign」「early proxy stress, not confirmed systemic credit tightening」。
5. **event_x_status_quality**：json_data 新增 fixed_items / remaining_weaknesses，区分已修好与仍弱项。

所有新增逻辑 **fail-open**。

---

## 修改文件清单

| 文件 | 修改内容 |
|------|----------|
| **新建** `event_x_breakeven.py` | `get_realtime_5y_breakeven_proxy_safe()`：FRED T5YIE 优先，stale 则 DGS5−T5YIFR 计算；返回 breakeven_source_used / breakeven_last / breakeven_last_date / breakeven_is_stale / breakeven_quality |
| `structural_risk.py` | Geopolitics 调用 breakeven 代理；用 breakeven_effective 做 Watch/Alert/Alarm 阈值；details 增加 breakeven_*；used_inputs 用 "breakeven" |
| `event_x_freshness.py` | `evaluate_geopolitics_data_completeness(details, freshness_result)` 重写：core_inputs_present / core_inputs_effective，completeness 为 HIGH/PARTIAL/LOW，missing_or_weak_legs；breakeven_is_stale 时不算有效 |
| `crisis_monitor_v2.py` | data_snapshot 用 breakeven_effective_last；completeness 传入 freshness_result；Data legs 显示 Breakeven 来源与 stale；_build_event_x_machine_summary 重写为触发/未确认/失真腿；json_data 增加 event_x_status_quality |
| **新建** `tests/test_event_x_round3_breakeven_and_completeness.py` | Smoke：STLFSI4、Brent 不触发、proxy fail-open、breakeven stale 时 completeness 非 HIGH、三腿有效时 HIGH、status_quality 结构、details 含 breakeven 字段 |

---

## Task 1: Breakeven 代理

- **event_x_breakeven.get_realtime_5y_breakeven_proxy_safe(base_module=None)**  
  - 先读 FRED T5YIE，若 last_date 距今 ≤7 天则用 FRED_T5YIE，quality HIGH。  
  - 否则尝试 DGS5、T5YIFR，计算 breakeven = DGS5 − T5YIFR，若两者都较新则用 COMPUTED_DGS5_T5YIFR。  
  - 若计算不可用则仍返回 FRED T5YIE 但 breakeven_is_stale=True，quality LOW。  
- **structural_risk**：Geopolitics 先取 proxy，breakeven_effective = proxy["breakeven_last"] 或回退 t5yie_last；阈值判断用 breakeven_effective；details 写入 breakeven_source_used / breakeven_last / breakeven_last_date / breakeven_is_stale / breakeven_quality / breakeven_effective_last。  
- **报告**：Data legs 中 Breakeven 标明来源（FRED / realtime proxy）及 as-of 日期；若 stale 则加 "; stale"。正文可提示："Breakeven input is stale; geopolitics inflation chain is only partially confirmed."

---

## Task 2: Completeness 有效可用性

- **evaluate_geopolitics_data_completeness(details, freshness_result)**  
  - 核心三条腿：Brent、Breakeven（effective_last）、VIX。  
  - core_inputs_present：三条中存在即计数。  
  - core_inputs_effective：存在且 Breakeven 非 breakeven_is_stale。  
  - completeness：HIGH = 3 条有效；PARTIAL = 2 条有效；LOW = 0–1 条有效。  
  - missing_or_weak_legs：如 breakeven_stale, breakeven_missing, brent_missing, vix_missing。  
- **报告**：Geopolitics completeness 显示 HIGH/PARTIAL/LOW；核心腿 stale 时不得为 HIGH。

---

## Task 3: Machine Summary

- 必须包含：Private Credit 为何亮灯、Geopolitics 为何亮灯/不应过度解读、Resonance 为何 OFF；若有 stale 核心腿要点名。  
- 示例：  
  - "Private credit watch is supported by BIZD weakness and HY spread widening momentum (5D +XXbp), while absolute spreads and STLFSI4 remain benign in level terms; reflects early proxy stress, not confirmed systemic credit tightening."  
  - "Geopolitics watch is currently driven mainly by VIX; Brent is available but does not confirm oil shock, and breakeven inflation input remains stale/partial."  
  - "No systemic resonance is confirmed."  
  - 若 breakeven_is_stale：补一句 "Breakeven input is stale; geopolitics inflation chain is only partially confirmed."

---

## Task 4: Private Credit 解释（保留+强化）

- 不改 Alert/Resonance 条件；保留 BIZD、HY 动量、STLFSI4 口径。  
- 顶部/详情中可见：  
  - "HY OAS still low in level terms, but 5D widening = +XXbp"  
  - "STLFSI4 level remains benign despite rising change_score"  
  - "Private credit watch reflects early proxy stress, not confirmed systemic credit tightening"  
- 已体现在 _build_event_x_machine_summary 的 Private Credit 句中。

---

## Task 5: event_x_status_quality

- **json_data["event_x_status_quality"]** = { "private_credit": { "fixed_items": [...], "remaining_weaknesses": [...] }, "geopolitics": { "fixed_items": [...], "remaining_weaknesses": [...] } }  
- private_credit.fixed_items：STLFSI4 unified, BIZD patch active, HY momentum input active。  
- private_credit.remaining_weaknesses：absolute spreads still benign, early watch depends on proxy + momentum。  
- geopolitics.fixed_items：Brent connected, VIX active, confidence/freshness fields active。  
- geopolitics.remaining_weaknesses：breakeven input stale unless realtime proxy succeeds, current watch may still be VIX-led；若 breakeven_is_stale 则改为 "breakeven input stale; geopolitics inflation chain only partially confirmed" 等。

---

## 报告顶部区块示例

```markdown
## 🔴 Event-X Priority Risks

- **Private Credit Liquidity Radar**: WATCH
- **Geopolitics & Inflation Radar**: WATCH
- **Resonance Trigger**: OFF
- **Signal Confidence**: LOW
- **Freshness Risk**: HIGH
- **Geopolitics completeness**: PARTIAL

- **Data legs**: Brent 77.2, YoY 6.8%; Breakeven 2.32% (FRED); stale; VIX 22.0

*Machine Summary:*
Private credit watch is supported by BIZD weakness and HY spread widening momentum (5D +35bp), while absolute spreads and STLFSI4 remain benign in level terms; reflects early proxy stress, not confirmed systemic credit tightening. Geopolitics watch is currently driven mainly by VIX; Brent is available but does not confirm oil shock, and breakeven inflation input remains stale/partial. No systemic resonance is confirmed. Signal confidence: LOW. Breakeven input is stale; geopolitics inflation chain is only partially confirmed.
```

---

## Smoke Test

```bash
py tests/test_event_x_round3_breakeven_and_completeness.py
```

覆盖：STLFSI4 仍用于 Private Credit；Brent 有值但不触发；breakeven proxy fail-open；breakeven stale 时 completeness 为 PARTIAL/LOW；三腿有效时 HIGH；event_x_status_quality 结构；Geopolitics details 含 breakeven_source_used、breakeven_is_stale。
