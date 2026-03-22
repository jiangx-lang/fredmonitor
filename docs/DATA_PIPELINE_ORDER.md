# 数据管道顺序：先下载，后计算，再出报告

为避免生成报告时出现「未拉取数据到本地」或「派生指标未重算」导致的遗漏，管道顺序与行为约定如下。

## 一、原则

1. **先下载**：所有报告依赖的 FRED 原始序列先同步到本地（`data/fred/series/{id}/raw.csv`）。
2. **后计算**：在下载完成后，再执行派生指标、YoY、企业债/GDP 等计算，写入 `data/series/` 等。
3. **再出报告**：报告生成阶段只读取本地已更新数据，不再在报告流程内决定「是否拉取」。

## 二、报告生成时的数据管道（crisis_monitor）

执行 `python crisis_monitor.py` 生成危机监控报告时，会先调用 `run_data_pipeline()`，顺序为：

| 步骤 | 脚本 / 逻辑 | 说明 |
|------|----------------|------|
| 1 | `scripts/sync_fred_http.py --before-report` | **报告前模式**：不按新鲜度跳过，拉取所有 crisis 依赖的 FRED 序列；拉取完成后**始终**执行 `calculate_derived_series()`，重算 CP_MINUS_DTB3、CORPDEBT_GDP_PCT、RESERVES_DEPOSITS_PCT 等。 |
| 2 | `scripts/calculate_corporate_debt_gdp_ratio.py` | 企业债/GDP 等比率计算（若与 sync 内派生重复，以 sync 后结果为准）。 |
| 3 | `scripts/calculate_yoy_indicators.py` | YoY 指标计算。 |
| 4 | 报告生成 | 读取上述步骤产出的本地数据，计算评分并生成报告。 |

## 三、`--before-report` 行为（sync_fred_http.py）

- **不加参数**（如定时任务只做增量）：仅对「未满足新鲜度」的序列拉取，满足新鲜度的跳过；拉取完成后仍会执行 `calculate_derived_series()`。
- **加 `--before-report`**（报告前调用）：  
  - 不按新鲜度过滤，**所有**在 catalog 且被 crisis 依赖的序列都会入队并拉取，确保报告前本地为最新。  
  - 拉取后**始终**执行派生指标计算，即使本次没有拉取任何序列（例如全部已是最新），也会重算派生，避免报告使用陈旧的 RESERVES_DEPOSITS_PCT 等。

## 四、为何会出现「遗漏下载/计算」

此前可能原因包括：

1. **按新鲜度跳过拉取**：未在报告前使用 `--before-report`，部分序列因「在新鲜度内」被跳过，上游若已更新而本地未再拉取，报告仍用旧数据。
2. **派生未在拉取后重算**：若 sync 时 `filtered_series` 为空（全部跳过），旧逻辑会直接 `return`，未执行 `calculate_derived_series()`，导致 RESERVES_DEPOSITS_PCT 等派生指标未更新。
3. **顺序依赖**：若先算派生再拉取，或报告生成路径未先跑数据管道，会出现「用旧数据算派生、再出报告」的遗漏。

当前已做修改：

- 报告生成前固定调用 `sync_fred_http.py --before-report`，先全量拉取再计算。  
- sync 内**无论是否拉取到任何序列**，都会执行 `calculate_derived_series()`。  
- 管道顺序在代码中固定为：下载 → 派生/企业债/YoY 计算 → 报告生成。

## 五、每日自动运行批处理

- **scripts/run_daily_report.bat**：每日报告专用。顺序：① `sync_fred_http.py --before-report`（先全量下载，无 Python 内超时）；② 设置 `CRISIS_MONITOR_SKIP_SYNC=1` 后执行 `send_daily_report.py`（生成报告并发邮件，管道内跳过重复下载）。定时任务建议调度此批处理。
- **scripts/create_daily_task.bat**：创建 Windows 计划任务，每天 09:00 执行 `run_daily_report.bat`。
- **scripts/run_daily.bat**：MacroLab 每日流程（事实表 + AI 分析），同步步骤已改为 `--before-report`，保证数据全量更新。

## 六、其他入口

- **仅做增量同步**：直接执行 `py scripts/sync_fred_http.py`（不加 `--before-report`）即可，派生仍会在同步后重算。
