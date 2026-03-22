# 滞后 30 天以上指标详细说明

基于 `data/stale_indicator_root_cause_audit.csv` 与 `data/upstream_fred_latest_check.csv`，对**最后观测日期距今超过 30 天**的指标逐项说明，便于判断是「正常发布滞后」还是「需处理」。

---

## 一、总览（按滞后天数排序）

| 滞后天数 | 指标 ID | 显示名 | 本地最后日期 | 频率 | 根因类别 | 上游是否更新 | 能否拉取更新 |
|----------|---------|--------|----------------|------|----------|--------------|----------------|
| 249 | NCBDBIQ027S | 企业债/GDP（新） | 2025-07-01 | Q | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 249 | TDSP | 家庭债务偿付比率 | 2025-07-01 | Q | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 178 | RESERVES_DEPOSITS_PCT | 准备金/存款% | 2025-09-10 | Q | LOW_FREQUENCY_NORMAL | **上游更新** | **是** |
| 157 | CREDIT_CARD_DELINQUENCY | 信用卡违约率 | 2025-10-01 | Q | ALIAS_OR_MAPPING_PROBLEM | 一致 | 否 |
| 157 | CORPDEBT_GDP_PCT | 企业债/GDP（旧） | 2025-10-01 | Q | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 157 | DRSFRMACBS | 房贷违约率 | 2025-10-01 | Q | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 157 | GDP | GDP YoY | 2025-10-01 | Q | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 96 | CSUSHPINSA | 房价指数 Case-Shiller 20城 YoY | 2025-12-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 96 | HOUST | 新屋开工 年化 | 2025-12-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 96 | NEWORDER | 制造业新订单（非国防资本货不含飞机）YoY | 2025-12-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 96 | PERMIT | 住宅建筑许可 YoY | 2025-12-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 65 | TOTRESNS | 银行准备金 YoY | 2026-01-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 65 | INDPRO | 工业生产 YoY | 2026-01-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 65 | UMCSENT | 密歇根消费者信心 | 2026-01-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 34 | CPN3M | 3个月商业票据利率 | 2026-02-01 | D | LOCAL_SYNC_MISSING | 一致 | 否 |
| 34 | MANEMP | 制造业就业 YoY | 2026-02-01 | M | LOW_FREQUENCY_NORMAL | 一致 | 否 |
| 34 | BAA10YM | 投资级信用利差 Baa-10Y国债 | 2026-02-01 | D | LOCAL_SYNC_MISSING | 一致 | 否 |

**说明**：「上游是否更新」= FRED 该序列最新观测日与本地是否一致；「能否拉取更新」= 上游最新 > 本地最新（来自 `upstream_fred_latest_check.csv`）。

---

## 二、逐项详情

### 1. NCBDBIQ027S（滞后 249 天）

- **显示名**：企业债/GDP（新）
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/NCBDBIQ027S/raw.csv`
- **频率**：季频 (Q)
- **catalog**：`freshness_days: 120`
- **根因**：季频序列，FRED 当前最新也是 2025-07-01，属**发布节奏滞后**，非本地或源故障。
- **建议**：接受为正常滞后；等 Q3 数据发布后跑 sync 即可更新。

---

### 2. TDSP（滞后 249 天）

- **显示名**：家庭债务偿付比率
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/TDSP/raw.csv`
- **频率**：季频 (Q)
- **catalog**：`freshness_days: 120`
- **根因**：同上，季频发布滞后，上游与本地一致。
- **建议**：无需处理，按季频理解即可。

---

### 3. RESERVES_DEPOSITS_PCT（滞后 178 天）★ 可修复

- **显示名**：准备金/存款%
- **来源**：派生（TOTRESNS / 存款序列，如 DPSACBW027SBOG、TOTALSA、TOTALSL）
- **本地路径**：`data/series/RESERVES_DEPOSITS_PCT.csv`（由 `sync_fred_http.py` 中 `calculate_derived_series()` 写入）
- **频率**：按季 (Q) 使用
- **根因**：底层腿 TOTRESNS 等在 FRED 已更新到 2026-02-01，但**派生脚本未重算**或未在 sync 后重跑，导致 CSV 停在 2025-09-10。
- **上游**：`upstream_fred_latest_check` 显示 **can_fetch_newer=True**，上游最新 2026-02-01。
- **建议**：跑一次完整 `sync_fred_http.py`（会拉 TOTRESNS 等并执行 `calculate_derived_series()`），或单独重算 RESERVES_DEPOSITS_PCT 并写回 `data/series/RESERVES_DEPOSITS_PCT.csv`。

---

### 4. CREDIT_CARD_DELINQUENCY（滞后 157 天）

- **显示名**：信用卡违约率
- **来源**：FRED 原生但用**别名**，实际数据来自 **DRCCLACBS**
- **本地路径**：无独立存储，compose 时读 `data/fred/series/DRCCLACBS/raw.csv`
- **频率**：季频 (Q)
- **根因**：ALIAS_OR_MAPPING_PROBLEM；DRCCLACBS 上游最新即 2025-10-01，与本地一致。
- **建议**：文档化「CREDIT_CARD_DELINQUENCY → DRCCLACBS」；数据本身已与源一致，无需改逻辑。

---

### 5. CORPDEBT_GDP_PCT（滞后 157 天）

- **显示名**：企业债/GDP（旧）
- **来源**：派生（NCBDBIQ027S / GDP × 100）
- **本地路径**：`data/series/CORPDEBT_GDP_PCT.csv`
- **频率**：季频 (Q)
- **根因**：两条腿 NCBDBIQ027S、GDP 在 FRED 最新均为 2025-10-01，派生结果与腿一致，属**正常季频滞后**。
- **建议**：无需处理；等 GDP/NCBDBIQ 更新后 sync 会重算。

---

### 6. DRSFRMACBS（滞后 157 天）

- **显示名**：房贷违约率
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/DRSFRMACBS/raw.csv`
- **频率**：季频 (Q)，catalog `freshness_days: 120`
- **根因**：季频发布滞后，上游=本地。
- **建议**：接受为正常滞后。

---

### 7. GDP（滞后 157 天）

- **显示名**：GDP YoY
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/GDP/raw.csv`
- **频率**：季频 (Q)，catalog `freshness_days: 120`
- **根因**：季频，上游=本地。
- **建议**：无需处理。

---

### 8. CSUSHPINSA（滞后 96 天）

- **显示名**：房价指数 Case-Shiller 20城 YoY
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/CSUSHPINSA/raw.csv`
- **频率**：月频 (M)，catalog `freshness_days: 60`
- **根因**：月频发布滞后（Case-Shiller 本身发布较晚），上游=本地。
- **建议**：接受为正常滞后。

---

### 9. HOUST（滞后 96 天）

- **显示名**：新屋开工 年化
- **来源**：FRED 原生，catalog `freshness_days: 60`
- **本地路径**：`data/fred/series/HOUST/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地。
- **建议**：无需处理。

---

### 10. NEWORDER（滞后 96 天）

- **显示名**：制造业新订单-非国防资本货不含飞机 YoY
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/NEWORDER/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地。
- **建议**：无需处理。

---

### 11. PERMIT（滞后 96 天）

- **显示名**：住宅建筑许可 YoY
- **来源**：FRED 原生，catalog `freshness_days: 60`
- **本地路径**：`data/fred/series/PERMIT/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地。
- **建议**：无需处理。

---

### 12. TOTRESNS（滞后 65 天）

- **显示名**：银行准备金 YoY
- **来源**：FRED 原生，catalog `freshness_days: 60`
- **本地路径**：`data/fred/series/TOTRESNS/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地 2026-01-01。
- **建议**：无需处理。

---

### 13. INDPRO（滞后 65 天）

- **显示名**：工业生产 YoY
- **来源**：FRED 原生
- **本地路径**：`data/fred/series/INDPRO/raw.csv`
- **频率**：月频 (M)，catalog `freshness_days: 60`
- **根因**：月频，上游=本地。
- **建议**：无需处理。

---

### 14. UMCSENT（滞后 65 天）

- **显示名**：密歇根消费者信心
- **来源**：FRED 原生，catalog `freshness_days: 60`
- **本地路径**：`data/fred/series/UMCSENT/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地。
- **建议**：无需处理。

---

### 15. CPN3M（滞后 34 天）

- **显示名**：3个月商业票据利率
- **来源**：FRED 原生，catalog `freshness_days: 7`
- **本地路径**：`data/fred/series/CPN3M/raw.csv`
- **频率**：日频 (D)
- **根因**：LOCAL_SYNC_MISSING（审计时标为「本地未跟上」）；但**上游检查显示 FRED 最新也是 2026-02-01**，与本地一致，即**源本身未再更新**。
- **建议**：当前无需修同步；若 FRED 后续恢复日更，sync 会自动拉新。可关注 FRED 该序列说明是否改为周频或停更。

---

### 16. MANEMP（滞后 34 天）

- **显示名**：制造业就业 YoY
- **来源**：FRED 原生，catalog `freshness_days: 60`
- **本地路径**：`data/fred/series/MANEMP/raw.csv`
- **频率**：月频 (M)
- **根因**：月频，上游=本地 2026-02-01，属正常滞后。
- **建议**：无需处理。

---

### 17. BAA10YM（滞后 34 天）

- **显示名**：投资级信用利差 Baa-10Y国债
- **来源**：FRED 原生，catalog `freshness_days: 7`
- **本地路径**：`data/fred/series/BAA10YM/raw.csv`
- **频率**：日频 (D)
- **根因**：审计标 LOCAL_SYNC_MISSING；**上游最新 2026-02-01 = 本地**，源未更新。
- **建议**：同 CPN3M，暂无需改同步；关注 FRED 发布节奏。

---

## 三、小结

| 类型 | 数量 | 指标 |
|------|------|------|
| **可立即修复**（派生未重算，上游已更新） | 1 | RESERVES_DEPOSITS_PCT |
| **正常季/月频滞后**（上游=本地，无需动） | 14 | NCBDBIQ027S, TDSP, CORPDEBT_GDP_PCT, DRSFRMACBS, GDP, CSUSHPINSA, HOUST, NEWORDER, PERMIT, TOTRESNS, INDPRO, UMCSENT, MANEMP |
| **别名/映射**（文档化即可） | 1 | CREDIT_CARD_DELINQUENCY |
| **日频源未更新**（上游=本地，非同步故障） | 2 | CPN3M, BAA10YM |

**建议优先动作**：对 **RESERVES_DEPOSITS_PCT** 跑一次完整 `sync_fred_http.py` 或单独重算并写回 `data/series/RESERVES_DEPOSITS_PCT.csv`，其余 30 天以上滞后指标均为正常频率或源未更新，无需改代码。
