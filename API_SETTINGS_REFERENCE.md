# D:\fred_crisis_monitor 所有 API / 配置汇总

本文档仅列出**配置项名称与用途**，不包含密钥取值。密钥请保存在 `.env` 或 `macrolab.env` 或系统环境变量中，勿提交到版本库。

---

## 一、环境变量（.env / macrolab.env / 系统环境变量）

### 数据与路径

| 变量名 | 用途 | 使用位置示例 |
|-------|------|--------------|
| `BASE_DIR` | 项目或数据根目录 | scripts/sync_fred_http.py, sync_fred_http_backup, duckdb_io, run_daily, 多个 discover/sync 脚本 |
| `MACROLAB_BASE_DIR` | GUI 默认数据目录 | macrolab_gui.py |
| `MACROLAB_EXCEL_OUT` | 宏观风险打分 Excel 输出路径 | macrolab_gui.py |

### FRED API

| 变量名 | 用途 | 使用位置示例 |
|-------|------|--------------|
| `FRED_API_KEY` | 圣路易斯联储 FRED 数据 API 密钥 | crisis_monitor_*.py, scripts/fred_http.py, sync_fred.py, test_*.py, macrolab_gui.py, macro.py |

### 邮件（QQ 邮箱 SMTP）

| 变量名 | 用途 | 使用位置示例 |
|-------|------|--------------|
| `QQ_EMAIL_USER` | QQ 发件邮箱 | notify/sendgrid_mail.py（.env / config.env） |
| `QQ_EMAIL_PASSWORD` | QQ 邮箱授权码 | notify/sendgrid_mail.py（.env / config.env） |

### AI / 大模型

| 变量名 | 用途 | 使用位置示例 |
|-------|------|--------------|
| `DASHSCOPE_API_KEY` | 阿里通义千问（DashScope） | ai_narrator.py, crisis_monitor.py, crisis_monitor_v2.py, scripts/test_ai_insights.py |
| `TONGYI_API_KEY` | 同上，与 DASHSCOPE 二选一 | ai_narrator.py, crisis_monitor_v2.py |
| `OPENAI_API_KEY` | OpenAI | ai_narrator.py, crisis_monitor_v2.py, scripts/ai_assess.py |
| `GEMINI_API_KEY` | Google Gemini | ai_narrator.py, crisis_monitor_v2.py |
| `AI_API_KEY` | 通用 AI Key（如 OpenAI） | macrolab.env, scripts/ai_assess.py |
| `AI_PROVIDER` | AI 提供商，如 openai | scripts/ai_assess.py |
| `AI_MODEL` | 模型名，如 gpt-4o-mini | scripts/ai_assess.py, settings.yaml |

### 行为开关

| 变量名 | 用途 | 使用位置示例 |
|-------|------|--------------|
| `STRICT` | 严格模式（1 启用） | crisis_monitor.py, crisis_monitor_v1_backup 等 |
| `DEBUG` | 调试开关 | macrolab.env |
| `CRISIS_MONITOR_SEND_ONLY` | 仅发邮件不跑报告（1/true/yes） | scripts/send_daily_report.py |
| `CRISIS_MONITOR_SKIP_AI_NARRATOR` | 跳过 AI 叙事（定时任务常用 1） | scripts/send_daily_report.py, crisis_monitor_v2.py |
| `CRISIS_MONITOR_FULL_REPORT` | 强制完整报告并调 AI（1 启用） | crisis_monitor_v2.py, macrolab.env 注释 |

---

## 二、配置文件（config/*.yaml）

### config/email_settings.yaml

| 键 | 用途 |
|----|------|
| `sender` | 发件人邮箱（如 jiangx@gmail.com） |
| `recipients` | 收件人列表 |
| `subject_prefix` | 主题前缀，如 "[Crisis Monitor]" |
| `include_paths` | 正文是否包含本地报告路径 |

### config/app_config.yaml

- **paths**: series, fred, outputs, config, scripts, figures  
- **values**: 除法/类型默认值、百分比系数等  
- **timeouts**: subprocess_default, fred_download, corporate_debt_calc, yoy_calc, long_image_gen  
- **charts**: figure_size, dpi, window_size, viewport_size  
- **scoring**: bands (low/med/high), default_weight, max_score  
- **data**: cache_size, chunk_size, max_file_size_mb  
- **fonts**: chinese_fonts, fallback_font  
- **output**: formats, encoding, include_images, generate_long_image  
- **logging**: level, format, file, max_size_mb, backup_count  
- **api.fred**: base_url (`https://api.stlouisfed.org/fred`), timeout, retry_attempts, retry_delay  
- **api.rate_limit**: requests_per_minute, min_interval_seconds  
- **security**: strict_mode, validate_inputs, sanitize_outputs, max_file_size_mb  
- **performance**: enable_caching, cache_ttl_seconds, parallel_processing, max_workers, memory_limit_mb  

### config/settings.yaml

- **user**: id, auto_confirm_all  
- **freshness_default_days**  
- **bands**: 各指标风险区间（如 CPI_yoy, NFCI, VIX 等）  
- **weights**: 因子权重  
- **outputs**: write_parquet, write_duckdb, render_fact_sheet, write_csv  
- **ai**: enable, language, temperature, max_tokens  
- **risk_thresholds**: low, medium, high, extreme  

### 其他 config 文件

- **crisis_indicators.yaml** / crisis_indicators_*.yaml：指标定义与 scoring  
- **crisis_periods.yaml**：危机时间段  
- **scoring.yaml**：评分相关  
- **factor_registry.yaml**：因子注册  
- 各类 `*_catalog.yaml`：FRED 分类/目录  

---

## 三、小结：常用 API 与密钥来源

| 功能 | 配置来源 | 关键变量/键 |
|------|----------|-------------|
| FRED 数据 | 环境变量 | `FRED_API_KEY` |
| QQ 邮箱发邮件 | .env/config.env + config/email_settings.yaml | `QQ_EMAIL_USER`、`QQ_EMAIL_PASSWORD`，`recipients` |
| 通义千问叙事 | 环境变量 | `DASHSCOPE_API_KEY` 或 `TONGYI_API_KEY` |
| OpenAI / Gemini | 环境变量 | `OPENAI_API_KEY`，`GEMINI_API_KEY` |
| FRED API 地址与限流 | config/app_config.yaml | `api.fred.base_url`，`api.rate_limit` |

建议：敏感键（FRED_API_KEY, QQ_EMAIL_PASSWORD, DASHSCOPE_API_KEY 等）只放在 `.env` 或 `config.env`，并将该文件加入 `.gitignore`，不在文档中写出具体取值。
