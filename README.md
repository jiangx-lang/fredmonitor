# FRED 危机预警监控系统

基于 FRED 与多数据源的宏观金融危机监察与早预警系统，含 Base Layer 评分、Regime/Conflict/Structural 模块、Warsh 流动性约束与日本传染检测，输出 MD/HTML/JSON 报告。

---

## 目录

- [一、系统概述与入口](#一系统概述与入口)
- [二、快速开始](#二快速开始)
- [三、数据流与数据源](#三数据流与数据源)
- [四、指标配置与权重结构](#四指标配置与权重结构)
- [五、序列变换、基准值、水平评分](#五序列变换基准值水平评分)
- [六、变化评分与单指标得分（V2）](#六变化评分与单指标得分v2)
- [七、组内聚合、早预警指数、确认与敏感度](#七组内聚合早预警指数确认与敏感度)
- [八、Regime 与 Structural Risk](#八regime-与-structural-risk)
- [九、报告后处理与输出](#九报告后处理与输出)
- [十、API 与配置](#十api-与配置)
- [十一、每日报告 API 使用量与成本](#十一每日报告-api-使用量与成本)
- [十二、数据目录与子模块](#十二数据目录与子模块)
- [十三、注意事项与可优化方向](#十三注意事项与可优化方向)

---

## 一、系统概述与入口

- **主入口（当前使用）**：`crisis_monitor_v2.py` → 调用 `generate_report_with_images_v2()`，内部会 patch `base.calculate_real_fred_scores = calculate_real_fred_scores_v2` 和 `base.compose_series = compose_series_v2`，然后执行 `base.generate_report_with_images()`，最后执行 **V2 后处理** `postprocess_reports()`（早预警指数、敏感度表、Regime Dashboard、数据新鲜度等）。
- **基础层**：`crisis_monitor.py` 提供数据获取（FRED + Yahoo）、序列变换、基准与水平打分、报告骨架与图表。
- **Regime 层**：`crisis_monitor_regime.py` 提供财政主导/日本传染/黄金/K 型四模块及综合结论，在后处理中注入报告。
- **Conflict 层**：`conflict_monitor.py` 提供政策冲突、黄金残差、日本传染、K 型劳动力、流动性五模块（Conflict & Divergence 面板）。
- **Regime Layer（制度性风险雷达）**：`structural_risk.py` 实现 **StructuralRiskMonitor**，与 Base Layer 并行运行，**不参与基础评分**，仅输出「Regime Alerts」。报告中的「⚠️ Structural & Regime Risks」小节**仅在有警报时**出现。

### 双层架构 (Dual-Layer Architecture)

| 层级 | 职责 | 是否影响 0–100 分 |
|------|------|-------------------|
| **Base Layer** | 信用利差、VIX、经济数据等，监控常规周期性/衰退风险（fast_ew_index、slow_macro、确认矩阵等） | 是 |
| **Regime Layer** | 政策冲突、去美元化、黄金-实际利率背离、K 型复苏、熊市陡峭化等制度性/结构性风险 | 否，仅输出 Regime Alerts |

报告输出：`outputs/crisis_monitor/` 下 `crisis_report_latest.md`、`crisis_report_latest.html`、`crisis_report_latest.json` 及带时间戳副本。

---

## 二、快速开始

### 运行主程序

```bash
# 推荐：先做数据/Regime 预检查
python scripts/check_regime_data.py

# 运行 V2 报告（含数据管道 + 评分 + 图表 + 后处理）
python crisis_monitor_v2.py
```

或直接运行基础层（无 V2 后处理）：`python crisis_monitor.py`。

### 每日定时运行并发邮件

1. 在项目根目录 `.env` 或 `config.env` 中配置 `QQ_EMAIL_USER`、`QQ_EMAIL_PASSWORD`（QQ 邮箱授权码）。
2. 编辑 `config/email_settings.yaml`（发件人、收件人、主题前缀等）。
3. 运行 `scripts/send_daily_report.py`。定时任务（如周六早上）会设 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1`，不调用大模型；若仅发邮件不跑报告，可设 `CRISIS_MONITOR_SEND_ONLY=1`。

Windows 可配合计划任务：例如 `scripts\create_daily_task.bat` 或任务计划程序里每日 9:00 执行 `py send_daily_report.py`。

### 预检查（建议首次或异常时执行）

```bash
python scripts/check_regime_data.py
```

检查 FRED/Yahoo 关键序列是否可拉取、Regime 五模块是否正常，避免主报告跑一半因数据/网络失败大量报错。

### 输出文件

- `outputs/crisis_monitor/crisis_report_latest.md` / `.html` / `.json`
- 带时间戳副本：`crisis_report_YYYYMMDD_HHMMSS.*`
- 图表：`outputs/crisis_monitor/figures/*.png`

---

## 三、数据流与数据源

### 3.1 数据获取优先级

1. **本地缓存**：`data/fred/series/{series_id}/raw.csv`（FRED 序列）。
2. **FRED API**：缓存不存在或需更新时调用，结果写回缓存。
3. **Yahoo Finance**：部分合成指标（如 VIX 期限结构、HYG/LQD、KRE/SPY、BTC/QQQ、DXY 5 日变化、CROSS_ASSET_CORR_STRESS、黄金/SPX 等）通过 `fetch_yahoo_safe()` 拉取，带重试与退避。

`crisis_monitor_v2.py` 中的 `compose_series_v2()` 会先处理合成 ID，再回退到 `base.compose_series()`。

### 3.2 预计算与外部序列

- **NCBDBIQ027S**：若存在 `data/series/CORPORATE_DEBT_GDP_RATIO.csv` 或预计算企业债/GDP 文件，则从该文件读入，否则走合成或 FRED。
- **危机期间**：来自 `config/crisis_periods.yaml` 的 `crises` 列表，用于基准分位数与确认逻辑。

---

## 四、指标配置与权重结构

配置文件：**`config/crisis_indicators.yaml`**。

### 4.1 通用字段（每个 indicator）

| 字段 | 含义 |
|------|------|
| `id` / `series_id` | 指标 ID |
| `name` | 显示名称 |
| `group` | 分组（core_warning / real_economy / monetary_policy / banking / consumers_leverage / monitoring 等） |
| `transform` | 序列变换：`none`/`level`、`yoy_pct`、`zscore` |
| `higher_is_risk` | true=数值越高风险越高 |
| `compare_to` | 基准类型（见「基准值计算」） |
| `tail` | 单尾 `single` 或 双尾 `both` |
| `weight` | 组内权重，按组归一后参与全局 100% |
| `freq` | D/W/M/Q，影响 lookback 与新鲜度惩罚 |
| `role` | `score` 参与打分；`monitor` 仅展示 |
| `use_velocity` | 若 true，变化评分加入速度/加速度项 |

### 4.2 分组与权重（概要）

- **核心预警 (core_warning)**：约 40%。**信用利差簇**：约 18%。**实体经济 (real_economy)**：约 25%。**货币政策 (monetary_policy)**：约 14%。**银行与信贷 (banking)**：约 13%。**消费与杠杆/外部 (consumers_leverage)**：约 5%。**监测项 (monitoring)**：weight=0，仅展示。

`scoring.deprecated_series` 中的 ID 会被跳过。

---

## 五、序列变换、基准值、水平评分

- **transform_series**：`yoy_pct`（同比）、`zscore`（滚动 Z-Score）、`none`/`level`（不变换）。
- **基准值 (calculate_benchmark_simple)**：用 `ts_trans` 与 `crisis_periods.yaml` 建危机/非危机掩码；`compare_to` 决定取 crisis_* / noncrisis_* 分位数（如 noncrisis_p75、crisis_median）。
- **水平评分 (score_with_threshold)**：p_cur = 全样本中「≤ current」比例；结合 compare_to 解析的阈值分位 p_thr，按单尾/双尾公式映射到 0–100，得到 **level_score**。

---

## 六、变化评分与单指标得分（V2）

- **change_score**：在 `crisis_monitor_v2.compute_change_score()` 中，基于动量比率、斜率、加速度、波动偏离（及可选的 velocity）等 z 分量，加权后 sigmoid 映射到 0–100，并乘**新鲜度因子**（日/周/月/季滞后惩罚）。
- **final_score** = `w_level * level_score + w_change * change_score`（默认 0.6/0.4），即报告中的 **risk_score**；并标记 **early_warning_flag**（change_score ≥ 阈值）。

---

## 七、组内聚合、早预警指数、确认与敏感度

- **组得分与 stress_now_index**：组内 Top-K 的 risk_score 平均 × 组权重。
- **早预警指数**：fast_ew_index（快变量 change_score 加权）、slow_macro_deterioration_index（慢变量）、early_warning_index = 0.7×fast + 0.3×slow；**breadth** 为触发 change 阈值的指标占比（含按 pillar 统计）。
- **确认信号**：三项——price_stress（SPX/200DMA 或回撤）、vol_term（VIX/VIX3M）、credit_stress（HYG/LQD 或利差）。最近 N 次运行中至少 M 次达到「至少 2 个信号 on」→ **fast_ew_alert**；**confirmed** = fast_ew_alert 且 credit_breadth ≥ 阈值。
- **状态标签**：Early Warning (confirmed) / Market Stress Watch / Macro Softening Watch (unconfirmed) / All Clear。
- **敏感度档位**：conservative / base / aggressive 三档，每档不同 credit/real/slow_macro 阈值与 persistence 参数；报告给出三档 Verdict 与共识结论。

---

## 八、Regime 与 Structural Risk

### Regime 四大模块 (crisis_monitor_regime)

- **A. Fiscal Dominance**：T5YIFR、10Y–2Y 利差、Bear Steepener 检测。
- **B. Japan Contagion**：USD/JPY、JGB 10Y、回流压力。
- **C. Gold / Anti-Fiat**：SPX/Gold、黄金与 10Y 实际利率相关。
- **D. K-Shaped**：大学及以上失业率。

综合结论优先级：SOVEREIGN LIQUIDITY CRISIS → JAPAN_CONTAGION_CRITICAL → FISCAL_DOMINANCE_ACTIVE → ANTI_FIAT_REGIME → K_SHAPED_RECESSION → NORMAL。

### StructuralRiskMonitor (structural_risk.py)

与 Base 并行，不参与 0–100 分，仅输出 Regime Alerts（NONE/WATCH/ALERT/ALARM）：Policy Conflict、De-Dollarization、Gold-Real Rate Divergence、K-Shaped、Bear Steepening 等模块。报告「⚠️ Structural & Regime Risks」仅在有警报时出现。

---

## 九、报告后处理与输出

`postprocess_reports()`：写入 V2/Regime 字段、数据新鲜度、Executive Verdict、Regime Dashboard、早预警指数、敏感度表、热力图、Top Drivers、低风险折叠与 context_note，并重新渲染 MD/HTML/JSON。

---

## 十、API 与配置

### 10.1 环境变量（.env / macrolab.env）

| 用途 | 变量名 |
|------|--------|
| FRED 数据 | `FRED_API_KEY` |
| QQ 邮箱发邮件 | `QQ_EMAIL_USER`、`QQ_EMAIL_PASSWORD`（.env / config.env） |
| 通义千问叙事 | `DASHSCOPE_API_KEY` 或 `TONGYI_API_KEY` |
| OpenAI / Gemini | `OPENAI_API_KEY`、`GEMINI_API_KEY` |
| 项目/数据根目录 | `BASE_DIR`、`MACROLAB_BASE_DIR` |

**行为开关**：`CRISIS_MONITOR_SKIP_AI_NARRATOR=1` 跳过 AI 叙事；`CRISIS_MONITOR_SEND_ONLY=1` 仅发邮件不跑报告；`CRISIS_MONITOR_FULL_REPORT=1` 强制完整报告并调 AI。`STRICT`、`DEBUG` 等见代码或历史文档。

密钥请仅放在 `.env` / `macrolab.env` 并加入 `.gitignore`。

### 10.2 配置文件摘要

- **config/email_settings.yaml**：sender、recipients、subject_prefix、include_paths（及可选 bcc）。
- **config/app_config.yaml**：paths、timeouts、charts、api.fred（base_url、timeout）、api.rate_limit、logging、security、performance。
- **config/crisis_indicators.yaml**：指标定义与 scoring。**config/crisis_periods.yaml**：危机时间段。**config/catalog_fred.yaml**：FRED 同步序列列表（约 65 个）。

---

## 十一、每日报告 API 使用量与成本

跑一次完整每日报告（数据管道 + 评分 + 报告 + 可选发邮件）的典型外部 API 用量：

| API | 典型用量（次/次） | 说明 |
|-----|-------------------|------|
| **FRED** | **约 200–280** | 数据管道：65 序列×约 3 次（series_info + series_release + series_observations）+ 企业债 2 + YoY 12；报告阶段以本地为主 |
| **大模型** | **0**（定时）/ **1**（手动开 AI） | 定时默认 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1` |
| **QQ SMTP** | **1** | 一封带附件的日报邮件 |
| **Yahoo** | **约几十** | 合成指标用，失败会跳过 |

**单次 AI 叙事成本（手动开 AI 时）**：主用通义 qwen-plus，约 2000 输入 + 600–1000 输出 token，**约 ¥0.003–0.006/次**；OpenAI gpt-4o-mini 约 ¥0.006，Gemini 1.5 Flash 约 ¥0.003。每天手动跑 1 次带 AI 约 **¥0.1–0.2/月**，可忽略。定价与免费额度以各厂商为准。

---

## 十二、数据目录与子模块

- **data/fred/series/{id}/**：FRED 序列 raw.csv、features.parquet、meta.json；由 `scripts/sync_fred_http.py` 维护。
- **data/series/**：预计算 YoY（如 PAYEMS_YOY.csv）、企业债/GDP、合成指标等；详见 `data/series/README.md`。
- **scripts/**：`sync_fred_http.py`（FRED 同步）、`calculate_yoy_indicators.py`、`calculate_corporate_debt_gdp_ratio.py`、`send_daily_report.py`、`check_regime_data.py`、`fred_http.py` 等；预检查说明见 `scripts/README_CHECK.md`。
- **examples/**：MacroLab 使用示例与自定义因子示例，见 `examples/README.md`。
- **daily_risk_dashboard/**：独立的高频风险监控子项目，日度/周度指标与共振检测，见 `daily_risk_dashboard/README.md`。

---

## 十三、注意事项与可优化方向

### 注意事项

- 本系统仅供参考，不构成投资建议；历史数据不能保证未来表现。
- 部分指标可能因数据源/网络无法获取；部分序列存在 1–2 个月延迟或历史修正。
- 依赖 FRED API 稳定性；建议定期运行数据管道并检查 `check_regime_data.py` 输出。

### 可优化方向（供扩展）

1. **阈值与权重**：compare_to、bands、credit/real/slow_macro 阈值随样本或 regime 校准。  
2. **变化评分**：z 分量权重、velocity 窗口、sigmoid 尺度按指标类型或波动率调整。  
3. **确认逻辑**：2-of-3 选型、persistence 窗口、credit_breadth 阈值。  
4. **Regime 模块**：A/B/C/D 阈值可配置或随历史分位数调整。  
5. **数据与稳健性**：缺失/滞后降权、fallback 策略、异常值处理（已有 data_error 降权与 anomaly_notes）。  
6. **分组与 pillar**：组权重、pillar 划分、breadth 按场景区分。  
7. **敏感度档位**：三档参数可配置或从回测学习。

---

**文档说明**：本 README 已整合原根目录 `README.md`、`API_SETTINGS_REFERENCE.md`、`CRISIS_MONITOR_README.md`、`EARLY_WARNING_README.md` 及 `docs/API_USAGE_DAILY_REPORT.md` 的要点。子目录中的 `data/series/README.md`、`examples/README.md`、`scripts/README_CHECK.md`、`daily_risk_dashboard/README.md` 仍可单独查阅。
