# 危机监控系统「未更新」指标根因排查报告

本文档基于 `data/stale_indicator_root_cause_audit.csv` 的审计结果，对「看起来没有更新」的 36 个指标做根因归类与优先修复建议。**本轮仅做诊断与归类，不修改 Base Layer、Event-X、评分或报告文案。**

---

## 一、总体结论

这批「未更新」指标主要分布在以下类别：

| 根因类别 | 数量 | 说明 |
|----------|------|------|
| **LOW_FREQUENCY_NORMAL** | 14 | 季频(Q)/月频(M) 序列，当前滞后在预期内，属正常 |
| **SOURCE_TRULY_STALE** | 17 | 上游源本身未更新或本地已在阈值内，无本地过错 |
| **LOCAL_SYNC_MISSING** | 4 | FRED 日/周频序列本地未跟上，需查同步队列与 catalog |
| **ALIAS_OR_MAPPING_PROBLEM** | 1 | 显示 ID 与真实数据源不一致，需文档化映射 |
| **DERIVED_NEEDS_REBUILD** | 0 | 本轮审计中未发现「派生链未重算」的明确案例 |
| **THRESHOLD_TOO_STRICT** | 0 | 未发现明显阈值过严导致误判 |

结论要点：

- **多数「看起来没更新」是正常低频（Q/M）或上游未发布**，不必当作系统故障。
- **真正需要优先修的是 4 个 LOCAL_SYNC_MISSING**（CPN3M、BAA10YM、DTWEXBGS、THREEFYTP10）和 **1 个 ALIAS_OR_MAPPING_PROBLEM**（CREDIT_CARD_DELINQUENCY）。
- 派生指标（CORPDEBT_GDP_PCT、RESERVES_DEPOSITS_PCT 等）本轮均落在 LOW_FREQUENCY_NORMAL，说明派生链在季频下已跟上；若未来出现「腿更新了但派生未重算」，应归为 DERIVED_NEEDS_REBUILD 并优先修派生脚本/流水线。

---

## 二、正常低频，不该惊慌（LOW_FREQUENCY_NORMAL）

以下 14 个指标为**季频或月频**，且 freshness 阈值（60d/120d）与频率匹配，当前滞后在可接受范围内：

- **季频 (Q)**：NCBDBIQ027S、TDSP、RESERVES_DEPOSITS_PCT、CORPDEBT_GDP_PCT、DRSFRMACBS、GDP  
- **月频 (M)**：TOTRESNS、MANEMP、CSUSHPINSA、HOUST、NEWORDER、PERMIT、INDPRO、UMCSENT  

**建议**：无需改代码，可在报告中注明「季/月频，数据发布滞后属正常」。

---

## 三、本地同步断裂（LOCAL_SYNC_MISSING）

以下 4 个指标为 **FRED 原生日频(D)或周频(W)**，本地 `raw.csv` 最后日期明显早于当前，且超过各自 freshness 阈值，应优先排查同步链路：

| 指标 ID | 本地最后日期 | 阈值 | 建议动作 |
|---------|----------------|------|----------|
| CPN3M | 2026-02-01 | 7d | 确认 catalog 与同步队列包含 CPN3M；检查 freshness_days 是否阻止重拉 |
| BAA10YM | 2026-02-01 | 7d | 同上 |
| DTWEXBGS | 2026-02-27 | 7d | 周频序列可考虑将阈值放宽至 14d；并确认同步是否按周拉取 |
| THREEFYTP10 | 2026-02-27 | 7d | 确认在 catalog 中且被 sync 脚本拉取 |

**建议**：检查 `config/catalog_fred.yaml` 是否包含上述 series_id、同步脚本是否按频率拉取、以及 meta/checkpoint 是否错误地阻止了重拉。

---

## 四、派生链条没重算（DERIVED_NEEDS_REBUILD）

本轮审计中**没有**指标被归为 DERIVED_NEEDS_REBUILD。  

- 派生指标如 CORPDEBT_GDP_PCT、RESERVES_DEPOSITS_PCT、HY_IG_RATIO、VIX_TERM_STRUCTURE、HY_OAS_MOMENTUM_RATIO 等，要么底层腿为季/月频（已归为 LOW_FREQUENCY_NORMAL），要么本地/上游日期在阈值内（归为 SOURCE_TRULY_STALE）。  
- 若后续发现「某条腿已更新但派生 CSV 或 compose 结果未更新」，应归入此类，并检查：`scripts/sync_fred_http.py` 的派生计算、`crisis_monitor.py` / `crisis_monitor_v2.py` 的 compose 与 Yahoo 缓存刷新。

---

## 五、命名/映射有问题（ALIAS_OR_MAPPING_PROBLEM）

- **CREDIT_CARD_DELINQUENCY**：展示用 ID，实际数据来自 **DRCCLACBS**（在 `crisis_monitor.py` 的 `compose_series` 中映射）。  

**建议**：在文档中明确写出「CREDIT_CARD_DELINQUENCY → DRCCLACBS」，避免误以为存在独立序列未同步；无需改读取逻辑。

---

## 六、最值得优先修复的前 10 个指标

按「可修复性」与「对监控重要性」排序：

1. **CPN3M** — 本地同步缺失，日频商业票据利率，影响短端定价视图  
2. **BAA10YM** — 本地同步缺失，信用利差核心指标  
3. **DTWEXBGS** — 本地同步/阈值，贸易加权美元  
4. **THREEFYTP10** — 本地同步缺失，期限溢价  
5. **CREDIT_CARD_DELINQUENCY** — 别名/映射需文档化，避免误判  
6. **CORPDEBT_GDP_PCT** — 派生链；确保 NCBDBIQ027S + GDP 更新后 sync 脚本重算并写入 data/series  
7. **RESERVES_DEPOSITS_PCT** — 派生链；确保 TOTRESNS 与存款序列更新后重算  
8. **HY_IG_RATIO** — 依赖 BAMLHYH0A0HYM2TRIV、BAMLCC0A0CMTRIV；确保两腿在 catalog 并同步后再看 compose  
9. **VIX_TERM_STRUCTURE** — 依赖 VIXCLS、VIX3M 或 Yahoo；确保 FRED 腿同步、Yahoo 缓存刷新  
10. **NFCI / STLFSI4** — 当前归为 SOURCE_TRULY_STALE；若业务要求更及时，可评估将周频 freshness 从 14d 略放宽或增加同步频次  

---

## 七、优先修复名单（Priority 1–4）

### Priority 1：应先修本地同步 / 派生链的

1. CPN3M  
2. BAA10YM  
3. DTWEXBGS  
4. THREEFYTP10  

（派生链本轮无明确「未重算」案例；若后续发现 CORPDEBT_GDP_PCT / RESERVES_DEPOSITS_PCT 在腿更新后未更新，加入本类并优先修 sync 脚本中的派生计算与写入。）

---

### Priority 2：应先修 freshness 阈值的

1. **DTWEXBGS** — 周频，阈值 7d 偏紧，建议评估改为 14d  
2. **NFCI** — 周频，若需更及时可评估 14d→21d 或增加同步  
3. **STLFSI4** — 同上  

（仅当业务要求「更少标红」且上游确实周更时调整；避免把真正断更掩盖掉。）

---

### Priority 3：应先修 alias / mapping 的

1. **CREDIT_CARD_DELINQUENCY** — 文档化与 DRCCLACBS 的映射  
2. **CORPDEBT_GDP_PCT** — 文档化底层 NCBDBIQ027S + GDP 及写入路径 data/series/CORPDEBT_GDP_PCT.csv  
3. **RESERVES_DEPOSITS_PCT** — 文档化底层 TOTRESNS + 存款序列及写入路径  

---

### Priority 4：可接受为正常低频的

1. NCBDBIQ027S（企业债/GDP 新）  
2. TDSP（家庭债务偿付比率）  
3. GDP（GDP YoY）  
4. DRSFRMACBS（房贷违约率）  
5. PERMIT（住宅建筑许可 YoY）  
6. HOUST（新屋开工）  
7. INDPRO（工业生产 YoY）  
8. UMCSENT（密歇根消费者信心）  
9. NEWORDER、CSUSHPINSA、MANEMP、TOTRESNS、RESERVES_DEPOSITS_PCT、CORPDEBT_GDP_PCT  

以上均为季频或月频，在 60d/120d 阈值下视为正常滞后，无需按故障处理。

---

## 八、数据来源与复核说明

- **审计脚本**：`scripts/stale_indicator_root_cause_audit.py`  
- **输出 CSV**：`data/stale_indicator_root_cause_audit.csv`（字段含 indicator_id, source_type, underlying_series_ids, frequency_expected, local_last_date, freshness_threshold_days, root_cause_category, immediate_reason, recommended_action, notes）  
- **配置依据**：`config/catalog_fred.yaml`（freshness_days）、`config/crisis_indicators.yaml`（freq、name）  
- **upstream_last_date**：本轮未调用 FRED API，CSV 中为空；若需与上游对齐，可在有 API 时在审计脚本中补查并写回。  

结论均基于当前代码路径、配置与本地 raw/meta、data/series 状态；未修改任何生产逻辑。
