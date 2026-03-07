# 每日报告 API 使用量说明

本文档说明**跑一次每日报告**（由 `scripts/send_daily_report.py` 或直接运行 `crisis_monitor_v2.py`）时，各外部 API 的**典型使用量**。实际次数会因缓存、增量更新、网络失败重试等略有浮动。

---

## 一、流程概览

每日报告一次运行大致包含：

1. **数据管道**（`run_data_pipeline()`）：FRED 同步 → 企业债/GDP 计算 → YoY 指标计算  
2. **评分与报告**：读本地数据为主，少量可能再调 FRED / Yahoo  
3. **AI 叙事**：仅当**未**设置 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1` 时调用大模型  
4. **发邮件**：SendGrid 发 1 封邮件  

定时任务（如周六早上）默认会设 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1`，因此**通常不调用大模型**。

---

## 二、FRED API（圣路易斯联储）

**环境变量**：`FRED_API_KEY`  
**限流**：代码内 `scripts/fred_http.py` 使用约 0.2 秒/请求；`config/app_config.yaml` 中为 120 次/分钟、最小间隔 0.5 秒。  
**免费档**：一般 120 次/分钟，具体以 FRED 官网为准。

### 2.1 数据管道阶段（占绝大部分）

| 步骤 | 脚本 | 说明 | 单次运行约请求数 |
|------|------|------|------------------|
| 1 | `scripts/sync_fred_http.py` | 同步 `config/catalog_fred.yaml` 中 65 个序列 | **约 195** |
| 2 | `scripts/calculate_corporate_debt_gdp_ratio.py` | 拉取 NCBDBIQ027S、GDP | **2** |
| 3 | `scripts/calculate_yoy_indicators.py` | 拉取 12 个 YoY 序列 | **12** |

**sync_fred_http 单序列约 3 次请求**：

- `series_info(series_id)`：1 次  
- `series_release(series_id)`：1 次（下次发布日期）  
- `series_observations(series_id)`：1 次（全量或增量，增量时可能再多 1 次）  

合计：**65×3 + 2 + 12 ≈ 209 次 FRED 请求/次**（全量同步时）。

若部分序列因“数据新鲜”被跳过或只做增量，实际会略低于 209，但**一般仍在 150–210 次/次**量级。

### 2.2 报告与图表阶段

- 评分与正文：**优先读本地**（`data/fred/series/<id>/raw.csv` 等），一般不额外打 FRED。  
- 图表：部分逻辑会再调 `series_observations` 取数画图，**最多约与参与绘图的指标数同量级**（约几十次），且很多会命中本地缓存。  

整体上，**一次完整每日报告 FRED 总用量约 200–280 次请求**，以数据管道为主。

---

## 三、大模型 API（AI 叙事）

**环境变量**：`DASHSCOPE_API_KEY` / `TONGYI_API_KEY` 或 `OPENAI_API_KEY` / `GEMINI_API_KEY`（见 `ai_narrator.py`、`crisis_monitor.py`）。

- **每日定时（如周六早上）**：`send_daily_report.py` 会设 `CRISIS_MONITOR_SKIP_AI_NARRATOR=1`，**不调用大模型**，使用量 = **0**。  
- **手动跑完整报告且未设跳过**：会调 `generate_narrative_with_llm`（或 `ai_narrator.generate_narrative_from_data`），**约 1 次请求/次**（一次长上下文，含系统提示 + 指标与 Warsh 数据）。  

即：**默认每日自动报告 = 0 次；手动开 AI 叙事 = 1 次/次**。

### 3.1 单次 AI 叙事成本估算（手动开 AI 时）

本系统主用 **通义千问 qwen-plus**（`crisis_monitor.py` 中 `base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"`, `model="qwen-plus"`）。单次调用大致：**系统提示约 1000 token + 用户数据约 1000 token ≈ 2000 输入 token**，**输出 CRO 解读约 600–1000 token**。

| 厂商 / 模型 | 单次约成本（1 次/天） | 说明 |
|-------------|------------------------|------|
| **阿里云 通义 qwen-plus** | **约 ¥0.003–0.006** | 输入 ≤128k 档：0.0008 元/千 token 入、0.002 元/千 token 出；按 2k 入 + 0.8k 出估算 |
| **OpenAI gpt-4o-mini** | **约 $0.0008–0.001（约 ¥0.006）** | 输入 $0.15/1M、输出 $0.60/1M（若改用 ai_narrator 的 OpenAI 路径） |
| **Google Gemini 1.5 Flash** | **约 $0.0004（约 ¥0.003）** | 输入 $0.075/1M、输出 $0.30/1M |

- **结论**：开 AI 叙事时，**单次报告大模型成本约 0.3–0.6 分人民币**（通义最常用）；即便每天手动跑 1 次带 AI，一个月约 **¥0.1–0.2**，可忽略。  
- 定价以各厂商官网/控制台为准，可能有免费额度（如阿里云百炼新用户送数千万 token）。

---

## 四、SendGrid（邮件）

**环境变量**：`SENDGRID_API_KEY`  
**配置**：`config/email_settings.yaml`（发件人、收件人、主题前缀等）。

- 每次报告发送 **1 封邮件**（含正文 + 可选 MD 附件）。  
- 即 **1 次 SendGrid API 调用/次**。

---

## 五、Yahoo Finance（yfinance）

- 用于部分指标（如 HYG/LQD、DXY、KRE/SPY、VIX3M、BTC/QQQ 等），在 `fetch_series` 或合成指标里通过 yfinance 拉行情。  
- 未在代码中做严格计数；**一次报告大约几十次**请求（视失败重试和缓存而定），且常因网络或限流失败被跳过，不影响报告主流程。

---

## 六、单次每日报告汇总（典型值）

| API | 典型用量（次/次） | 说明 |
|-----|-------------------|------|
| **FRED** | **约 200–280** | 以数据管道 65 序列×3 + 企业债 + YoY 为主 |
| **大模型** | **0**（定时） / **1**（手动开 AI） | 定时默认跳过 AI 叙事 |
| **SendGrid** | **1** | 一封带附件的日报邮件 |
| **Yahoo** | **约几十** | 非关键，失败会跳过 |

若每天跑**一次**完整管道 + 报告 + 发邮件且**不**开 AI：

- **FRED**：约 **200–280 次/天**  
- **SendGrid**：**1 次/天**  
- **大模型**：**0 次/天**

FRED 免费档若为 120 次/分钟，单次管道约 2–3 分钟可打完（在现有 0.2s 间隔下）。

---

## 七、相关配置与文档

- 所有 API/环境变量汇总：根目录 **`API_SETTINGS_REFERENCE.md`**  
- FRED 限流与 base_url：**`config/app_config.yaml`** → `api.fred`、`api.rate_limit`  
- 邮件收件人/发件人：**`config/email_settings.yaml`**  
- 行为开关：`CRISIS_MONITOR_SKIP_AI_NARRATOR`、`CRISIS_MONITOR_SEND_ONLY`、`CRISIS_MONITOR_FULL_REPORT` 见 `API_SETTINGS_REFERENCE.md`。
