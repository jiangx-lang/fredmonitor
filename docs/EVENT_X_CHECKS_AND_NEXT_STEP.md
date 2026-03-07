# Event-X 五步检查与下一阶段

## 已完成的 5 项检查

### 1. 缺 BIZD 快测（fail-open 闭环）

- **目的**：BIZD 全空、GLD fallback 失败时，仍能正常生成 Event-X 区块且 Resonance 不误触发。
- **实现**：`tests/test_event_x_fail_open_and_red_alert.py` 中 `test_event_x_with_no_bizd_and_no_gold_fallback()` 通过 mock `fetch_bizd_safe` 返回空 Series、`get_gold_spx_rolling_corr_20d` 返回 `(np.nan, "GLD", {"reason": "missing"})`，跑 StructuralRiskMonitor + evaluate_resonance_triggers，断言 Resonance 为 OFF、两个雷达存在且可拼出摘要句。
- **运行**：`py tests/test_event_x_fail_open_and_red_alert.py`

### 2. 低频序列“很旧”时的报告暴露

- **目的**：DRTSCILM / CPIENGSL 等低频数据仅沿用最近已知值时，details 与报告里清楚暴露「原始最后观测日」和「沿用最近已知值」。
- **实现**：
  - `structural_risk.py` 中 Private Credit 雷达 details 增加 `drtscilm_last_obs_date`；Geopolitics 雷达 details 增加 `cpiengsl_last_obs_date`、`t5yie_last_obs_date`（均为最近有效观测日字符串）。
  - `_build_event_x_priority_risks_section` 在 Machine Summary 末尾追加一行：`Low-frequency (沿用最近已知值): DRTSCILM as of YYYY-MM-DD; CPIENGSL as of YYYY-MM-DD; T5YIE as of YYYY-MM-DD.`（仅对存在的日期输出）。

### 3. RED_ALERT 假数据验证

- **目的**：人工构造满足「HY 周升>50bp + T5YIE>2.5 + (BIZD<-10 或 STLFSI4>1) + credit_stress_on=True」的 snapshot，确保一定能打出 RED_ALERT，避免 override 逻辑因类型或布尔写法未触发。
- **实现**：`tests/test_event_x_fail_open_and_red_alert.py` 中 `test_red_alert_mock_snapshot()` 用三组 mock snapshot（含 BIZD 路径、STLFSI4 路径、BIZD 为 NaN 仅 STLFSI4 路径）调用 `evaluate_resonance_triggers`，断言 `level == "RED_ALERT"`。

### 4. struct_results 状态映射

- **目的**：确认雷达状态仅出现 NONE / WATCH / ALERT / ALARM，不混入小写或旧格式。
- **实现**：`_build_event_x_priority_risks_section` 内维护 `_VALID_ALERTS = {"NONE", "WATCH", "ALERT", "ALARM"}`，对 `pc_alert` / `geo_alert` 做校验，非法则回退为 `"NONE"`；展示时 NONE -> NORMAL。

### 5. Machine Summary 方向感与后续升级

- **目的**：当前固定句已够用；增加一点方向感，并为后续「状态 + 解释原因」投委会摘要留口。
- **实现**：
  - 固定句：`Private Credit is {radar_a_status}; Geopolitics/Inflation is {radar_b_status}; Systemic Resonance: {res_status}.`
  - 当 `res_status == "OFF"` 时追加 `" No escalation."`
  - 若存在低频 last_obs_date，追加 `" Low-frequency (沿用最近已知值): ..."`
  - 函数注释中注明：后续可升级为「状态 + 解释原因」双层。

---

## 建议下一阶段：最小回测 / 历史回放

**不要先加新指标**，而是做一次**最小回测或历史场景回放**，验证 Event-X 在以下段落中的反应是否合理：

- **2022 年** 能源冲击与通胀再定价
- **2023 年** 区域银行 / 信用紧张阶段
- **2024–2025 年** HY 利差急扩与波动上升（若有）
- **近期** 伊朗相关冲击窗口

**验证目标**：

- 会不会**太迟**（信号滞后于市场/新闻）
- 会不会**太容易误报**（平静期也亮灯）
- 会不会**只在新闻最热时才亮灯**，而没抓到真正的市场传导

结论：本版设计可进入「历史场景验证 + 实盘日报试跑」阶段。
