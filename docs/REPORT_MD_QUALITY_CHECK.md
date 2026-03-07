# 产出 MD 报告质量检查清单

基于对 `outputs/crisis_monitor/crisis_report_latest.md` 的检查结果。

---

## 一、已修复问题

| 问题 | 处理 |
|------|------|
| RRP 在 Executive Summary 中显示为 "4B" | 已改为 `.1f` 显示 "4.5B"（`build_warsh_executive_summary`） |
| 分组风险权重重显示为长小数（如 67.97…%） | 已改为 `data['weight']:.1f%` 显示一位小数（base 报告写入） |

---

## 二、当前仍存在的问题 / 潜在问题

### 1. 内容与结构

- **「🤖 每日宏观简报 (AI)」可能未出现**  
  - 若未配置或未成功调用 AI（如 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1` 或 API 失败），该段不会写入。  
  - 建议：确认 `macrolab.env` 中 `DASHSCOPE_API_KEY` 等已配置，且未设置 skip 环境变量；查看运行日志是否有「✅ AI 叙事已生成并写入报告」。

- **Warsh 运行元数据与报告生成时间相差 1 小时**  
  - 报告头为「生成时间: 2026年02月03日 05:03:25」，Warsh 元数据为「2026-02-03 06:03:25 (JST)」。  
  - 可能原因：生成时间用本地时区，Warsh 用 JST；或写入时机不同。  
  - 影响：仅展示不一致，逻辑无影响。可选统一为同一时区或同一时间源。

- **总体风险概览位于 `<details>` 内**  
  - 当前结构为：`<details><summary>🔵 极低风险指标</summary> … 指标 … ## 📊 总体风险概览 … </details>`。  
  - 即「总体风险概览」在可折叠的极低风险区块内，读者可能误以为没有总览。  
  - 建议：若希望总览始终可见，可将「总体风险概览」移出该 `<details>`，单独成节。

### 2. 数据与口径

- **Warsh RED 依赖 RRP 旧缓存**  
  - RRP 4.5B 触发 RED（<100B），来自本地 RRPONTSYD 缓存（如截止 2025-10-09）。  
  - 若先运行 `py scripts/sync_fred_http.py` 更新 RRP，再生成报告，Warsh 等级可能变为 YELLOW/GREEN。  
  - 建议：报告或运行说明中注明「Warsh 结论依赖 SOFR/IORB/RRP/THREEFYTP10 等数据新鲜度」，或定期同步后再跑报告。

- **Data Freshness 与 Warsh 元数据两处**  
  - 报告中有「Data Freshness Confidence: LOW (core stale: …)」和 Warsh 的「运行元数据 | SOFR−IORB/RRP/THREEFYTP10 数据截止」。  
  - 两处均正确反映偏旧序列，但读者可能不清楚「RRP 4.5B」与「core stale」的对应关系。  
  - 可选：在 Warsh 段落加一句「上述读数基于本地缓存，若日期偏旧请先运行 sync_fred_http」。

### 3. 格式与展示

- **图片路径为相对路径**  
  - 所有图为 `figures/xxx_latest.png`。HTML 渲染时会 base64 内嵌，无问题；单独打开 MD 时需与 `figures/` 同目录才能看到图。  
  - 建议：若需单独分发 MD，说明「需与 figures 目录一起」或提供 HTML 版本。

- **指标配置表与当前 YAML 可能不完全一致**  
  - 报告内「指标配置表」为 base 中写死的 `config_table` 列表，若 `crisis_indicators.yaml` 已改，表可能未同步。  
  - 建议：后续可改为从 YAML 动态生成该表，避免与真实配置不一致。

### 4. 代码与运行

- **v2 后处理「latest.json 解析失败，尝试回退」**  
  - 若出现该日志，说明后处理先读的 JSON 路径或格式与预期不符，已用回退逻辑。  
  - 建议：确认后处理读取的 JSON 路径与 `generate_report_with_images` 写入的路径、文件名一致（含 timestamp 与 latest 副本）。

- **crisis_monitor_v2.py 中 sigmoid 的 overflow**  
  - `RuntimeWarning: overflow encountered in exp` 出现在约 426 行，对报告内容影响有限，但可能影响个别边界分数。  
  - 建议：对输入做 clip 或改用数值稳定的 sigmoid 实现。

---

## 三、检查结论

- 报告结构完整：综合性结论 → Regime → Conflict → Structural → 配置建议 → Data Freshness → Warsh（Executive Summary / 信号灯 / Why Warsh / Interpretation）→ 报告说明 → 执行摘要 → 详细指标分析 → 总体风险概览 → 分组风险评分 → 指标配置表 等。
- 已修复：RRP 显示 4B→4.5B、分组权重重小数格式化。
- 其余为展示/数据新鲜度/可选优化项，不影响主流程；按需同步数据、确认 AI 与 JSON 路径即可保证产出 MD 质量。

---

*检查日期：基于 2026-02-03 产出 MD 与当前代码*
