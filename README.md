# MacroLab - Local FRED + AI 宏观分析系统

MacroLab是一个模块化的宏观经济分析系统，能够从FRED API同步数据到本地，计算衍生特征，生成FRED风格的事实表，并提供AI辅助的宏观分析。

## 功能特性

- 🔄 **FRED数据同步**: 自动从FRED API同步历史数据和元数据
- 📊 **特征计算**: 自动计算YoY、MoM等衍生指标
- 💾 **本地存储**: 数据存储在Parquet和DuckDB中，支持快速查询
- 📋 **事实表生成**: 生成FRED风格的数据说明页面
- 🤖 **AI分析**: 基于最新数据生成宏观状态分析报告
- 🔧 **模块化设计**: 易于扩展和维护

## 项目结构

```
MacroLab/
├── config/                 # 配置文件
│   ├── catalog_fred.yaml  # FRED序列目录
│   └── settings.yaml      # 全局设置
├── data/                  # 数据存储
│   ├── fred/             # FRED数据
│   │   └── series/       # 各序列数据
│   └── lake/             # DuckDB数据湖
├── scripts/              # 核心脚本
│   ├── sync_fred.py      # FRED数据同步
│   ├── render_fact_sheets.py # 事实表渲染
│   ├── ai_assess.py      # AI分析
│   └── run_daily.bat     # 每日运行脚本
├── templates/            # 模板文件
│   ├── fact_sheet.md.j2  # 事实表模板
│   └── ai_prompt_status.md # AI提示词模板
├── outputs/              # 输出文件
│   └── macro_status/     # 宏观状态报告
└── tests/                # 测试文件
```

## 快速开始

### 1. 安装依赖

```bash
pip install -e .
```

### 2. 配置环境

复制环境配置文件：

```bash
copy env.example .env
```

编辑 `.env` 文件，填入您的API密钥：

```
BASE_DIR=D:\MacroLab
FRED_API_KEY=your_fred_api_key_here
AI_PROVIDER=openai
AI_API_KEY=your_openai_api_key_here
AI_MODEL=gpt-4o-mini
```

### 3. 运行分析

#### 手动运行

```bash
# 同步FRED数据
python scripts/sync_fred.py

# 渲染事实表
python scripts/render_fact_sheets.py

# AI宏观分析
python scripts/ai_assess.py
```

#### 一键运行

```bash
# Windows
scripts\run_daily.bat

# 或直接运行Python脚本
python scripts/run_daily.py
```

## 配置说明

### FRED序列配置 (`config/catalog_fred.yaml`)

```yaml
series:
  - id: CPIAUCSL         # FRED序列ID
    alias: CPI_headline  # 别名
    calc:                # 特征计算规则
      yoy: {op: pct_change, shift: 12, scale: 100}
      mom: {op: pct_change, shift: 1,  scale: 100}
    freshness_days: 60   # 数据新鲜度要求（天）
```

### 全局设置 (`config/settings.yaml`)

```yaml
# 评分区间配置
bands:
  CPI_yoy: [2.0, 4.0]    # 风险评分区间
  VIX: [12, 30]

# 因子权重
weights:
  CPI_yoy: 0.15
  VIX: 0.15

# AI配置
ai:
  enable: true
  language: zh-CN
  temperature: 0.4
```

## 添加新序列

要添加新的FRED序列，只需在 `config/catalog_fred.yaml` 中添加配置：

```yaml
series:
  - id: NEW_SERIES_ID
    alias: new_series
    calc:
      yoy: {op: pct_change, shift: 12, scale: 100}
    freshness_days: 30
```

然后重新运行同步脚本：

```bash
python scripts/sync_fred.py
```

## 手动笔记和附件

每个序列都有独立的笔记目录：

```
data/fred/series/SERIES_ID/
├── notes/
│   ├── custom_notes.md      # 手动笔记
│   └── attachments/         # 附件目录
│       ├── chart1.png
│       └── data.csv
```

- 在 `custom_notes.md` 中添加您的研究笔记
- 在 `attachments/` 目录中放置图表和数据文件
- 这些内容会自动包含在生成的事实表中

## DuckDB查询示例

系统使用DuckDB作为数据湖，支持SQL查询：

```sql
-- 查看所有可用的表
SHOW TABLES FROM fred;

-- 查询CPI数据
SELECT * FROM fred.CPIAUCSL 
WHERE date >= '2024-01-01' 
ORDER BY date DESC 
LIMIT 10;

-- 计算期限利差
SELECT 
    y10.date,
    y10.value as yield_10y,
    y2.value as yield_2y,
    (y10.value - y2.value) as term_spread
FROM fred.DGS10 y10
JOIN fred.DGS2 y2 ON y10.date = y2.date
ORDER BY y10.date DESC
LIMIT 10;

-- 查询最新数据快照
WITH latest_data AS (
    SELECT 'CPI' as indicator, date, yoy as value FROM fred.CPIAUCSL ORDER BY date DESC LIMIT 1
    UNION ALL
    SELECT 'VIX' as indicator, date, value FROM fred.VIXCLS ORDER BY date DESC LIMIT 1
    UNION ALL
    SELECT 'NFCI' as indicator, date, value FROM fred.NFCI ORDER BY date DESC LIMIT 1
)
SELECT * FROM latest_data;
```

## 输出文件

### 事实表 (`data/fred/series/SERIES_ID/fact_sheet.md`)

每个序列都会生成一个FRED风格的事实表，包含：
- 序列基本信息
- 最新数据值
- 衍生指标（YoY、MoM等）
- 官方说明
- 您的手动笔记
- 附件列表

### 宏观状态报告 (`outputs/macro_status/YYYY-MM-DD.md`)

每日AI分析报告，包含：
- 执行摘要
- 重点关注指标
- 近期风险提示

## 测试

运行测试套件：

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行特定测试
python -m pytest tests/test_sync.py -v
python -m pytest tests/test_render.py -v
```

## 故障排除

### 常见问题

1. **FRED API错误**: 检查API密钥是否正确设置
2. **数据同步失败**: 检查网络连接和API限制
3. **AI分析失败**: 检查OpenAI API密钥和网络连接
4. **文件权限错误**: 确保对数据目录有写入权限

### 调试模式

设置环境变量启用调试模式：

```bash
set DEBUG=1
python scripts/sync_fred.py
```

## 开发

### 添加新的特征计算

在 `scripts/sync_fred.py` 的 `compute_features` 函数中添加新的计算规则：

```python
def compute_features(df: pd.DataFrame, calc_spec: Dict[str, Any]) -> pd.DataFrame:
    # 添加新的计算规则
    if rule["op"] == "moving_average":
        window = rule.get("window", 5)
        out[name] = out["value"].rolling(window=window).mean()
```

### 自定义AI提示词

编辑 `templates/ai_prompt_status.md` 来自定义AI分析的提示词模板。

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request来改进这个项目！