# Event-X 历史场景验证框架

**目标**：检验 Event-X 模块在历史场景中的行为是否合理——**会不会太迟？会不会太敏感？是否只会事后解释而不会提前预警？**

验证重点不是回测收益，而是**行为验证**。

---

## 验证三问

| 维度 | 含义 |
|------|------|
| **会不会太迟？** | 风险已明显恶化后才亮灯，缺乏提前量。 |
| **会不会太敏感？** | 轻微波动就频繁 WATCH/ALERT，噪音过多。 |
| **是否只会解释过去？** | 新闻/冲击发生后报告才“解释得很好”，但事前无任何前兆。 |

---

## 支持的四类场景

### A. Oil shock / 油价冲击期

- **典型窗口**：油价快速上行、通胀预期抬升。
- **预期行为**：Geopolitics Radar 应在油价与通胀预期抬升过程中或之前进入 WATCH，而非仅事后才 HIGH。
- **评价**：若报告仅在冲击见顶后才亮灯 → 太迟；若油价仅小幅波动即 WATCH → 可结合 completeness 判断是否太敏感。

### B. Credit widening / 信用利差快速走阔期

- **典型窗口**：HY OAS 快速拉大、BIZD 明显下跌。
- **预期行为**：Private Credit Radar 应在绝对阈值触发前，因 HY 动量或 BIZD 弱势先亮 WATCH（早期动量逻辑）。
- **评价**：若仅当 OAS 已到高位才 WATCH → 太迟；若利差略升即 WATCH 且无动量支撑 → 可能太敏感。

### C. Volatility rise without full resonance / 波动上升但未共振期

- **典型窗口**：VIX 上行，但 Brent/breakeven/credit 未同步确认。
- **预期行为**：系统应给 Geopolitics WATCH（或 PARTIAL），且 **不** 升级为 RED ALERT / 共振 ON；completeness 应为 PARTIAL 或 LOW，summary 应显式“VIX-led”。
- **评价**：若误判为共振或 ALERT → 太敏感；若完全无反应 → 可能太迟或漏报。

### D. True resonance / 真实共振压力期

- **典型窗口**：信用利差走阔 + 通胀预期抬升 + 流动性代理恶化 + credit_stress = ON。
- **预期行为**：Resonance 应升级（非 OFF）；两雷达中至少一个 WATCH/ALERT；Signal Confidence 与 Freshness 应反映数据质量。
- **评价**：若明显共振环境下 Resonance 仍 OFF → 太迟或逻辑过严；若仅单腿动即共振 ON → 太敏感。

---

## 实现方式

- **脚本**：`scripts/run_event_x_historical_validation.py`
- **输入**：可选单份报告 JSON、或按日期存放的多份 JSON；场景定义内嵌或从配置读取。
- **输出**：每场景一条结果，结构如下。

---

## 输出结构（单场景）

```json
{
  "scenario_name": "Oil shock 2022",
  "date_range": "2022-02-01 / 2022-06-30",
  "expected_behavior": "Geopolitics WATCH 在油价与通胀预期抬升期间出现，不晚于 2022-03.",
  "observed_behavior": "2022-03-15 报告 Geopolitics WATCH；Brent/breakeven 有效.",
  "too_late": false,
  "too_sensitive": false,
  "only_explains_after": false,
  "summary": "PASS: Geopolitics 在窗口内提前/同步反应，未仅事后解释."
}
```

---

## 通过 / 不通过标准

- **通过**：`too_late`、`too_sensitive`、`only_explains_after` 均为 false，且 `summary` 与预期一致。
- **不通过**：任一为 true，或 observed 与 expected 严重不符（如预期 WATCH 却始终 OFF → 太迟；预期仅 WATCH 却 ALERT → 太敏感）。
- **待补数据**：无该窗口的报告 JSON 时，可标为 `"skipped": true, "reason": "No report for date range"`。

---

## 与维护者摘要的关系

`event_x_maintainer_summary.historical_validation_ready` 仅表示脚本与文档已就绪，**不**表示已对历史数据全部跑通。实际发布前建议对至少 1–2 个代表性窗口跑一次验证并记录结果。
