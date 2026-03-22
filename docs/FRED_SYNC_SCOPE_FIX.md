# FRED 同步范围修复说明

## 一、根因

- **不是** FRED API 未更新，也不是 `observation_end` / `realtime_end` 被硬编码。
- **根本原因**：`scripts/sync_fred_http.py` 原先只同步同时满足以下条件的序列：
  1. 在 `config/catalog_fred.yaml` 中；
  2. 在 **硬编码** 的 `get_daily_factors_needed()` 返回列表中。
- 很多 crisis 监控实际使用的日/周频 FRED 序列（如 DTB3、DGS10、T10Y3M、SOFR、MORTGAGE30US、BAA10YM、CPN3M）不在该硬编码列表中，因此从未进入同步队列，本地 `data/fred/series/{id}/raw.csv` 停留在旧日期。
- 已通过 `scripts/verify_fred_recent.py` 验证：FRED 端上述序列有最新数据，问题在本地同步范围。

## 二、本次修改内容

1. **依赖与覆盖率审计**
   - 新增 `scripts/build_crisis_fred_scope.py`：从 `config/crisis_indicators.yaml` 与派生逻辑推导 crisis 实际依赖的 FRED 原生序列。
   - 产出：
     - `data/crisis_required_fred_series.csv`：各序列来源、是否在 catalog/原 needed 列表、是否应被同步；
     - `data/crisis_fred_sync_coverage.csv`：同步覆盖率审计（required_by_crisis / present_in_catalog / present_in_sync_queue / status）。

2. **补全 catalog**
   - 在 `config/catalog_fred.yaml` 中补充 crisis 依赖但此前缺失的 FRED 序列，包括：
     - 利率与利差：DTB3, T10Y3M, T10Y2Y, BAA10YM, CPN3M, MORTGAGE30US, TEDRATE；
     - 央行/银行：WALCL, TOTLL, TOTALSA, TOTRESNS, MANEMP；
     - 派生依赖：WTREGEN, RRPONTSYD, IORB, DRCCLACBS, VIX3M；以及 BAMLHYH0A0HYM2TRIV, BAMLCC0A0CMTRIV 等。
   - 未删除任何既有条目，格式与现有风格一致。

3. **同步入队范围改为配置驱动（方案 A）**
   - 修改 `scripts/sync_fred_http.py`：
     - 新增 `get_crisis_required_fred_series()`：从 `crisis_indicators.yaml` 与派生依赖表推导「crisis 依赖的 FRED 原生序列」列表。
     - 同步队列 = **catalog 中且在该列表中的序列**，不再依赖狭窄的 `get_daily_factors_needed()`。
     - 保留 `_fallback_needed_series()`：当无法读取 crisis 配置时回退到扩大版硬编码列表（含原列表 + 漏同步的 7 个序列），并在日志中说明为过渡行为。

4. **同步覆盖率审计**
   - 每次执行 `sync_fred_http.py` 后写入 `data/crisis_fred_sync_coverage.csv`，字段包括：
     - `series_id`, `required_by_crisis`, `present_in_catalog`, `present_in_sync_queue`, `status`
     - status：`OK` | `MISSING_IN_CATALOG` | `MISSING_IN_SYNC_QUEUE`

5. **向后兼容与 fail-open**
   - 单序列同步失败仅记录 warning，不中断整体同步。
   - 未改动 raw/meta 存储结构，未删除既有 checkpoint/metadata 机制。

## 三、验收要点

- 以下 7 个序列已纳入同步范围且可审计为 OK：
  - **DTB3, DGS10, T10Y3M, SOFR, MORTGAGE30US, BAA10YM, CPN3M**
- Crisis 依赖的 FRED 序列可明确列出（见 `data/crisis_required_fred_series.csv`）。
- 同步队列覆盖率可审计（见 `data/crisis_fred_sync_coverage.csv`），不再依赖黑箱硬编码。
- 复验：执行一次 `scripts/sync_fred_http.py` 后，用 `scripts/verify_fred_recent.py` 对 DTB3 / DGS10 / IC4WSA 等拉取最近 30 天数据，确认本地 `raw.csv` 最后日期能与 FRED 端一致。

## 四、派生依赖作为一级配置与 Code Review 项

- **`config/derived_fred_deps.yaml`** 为「派生指标 → 依赖的 FRED 原始序列」的一级配置资产；`sync_fred_http.py` 与 `build_crisis_fred_scope.py` 均从此文件加载，缺失或异常时使用代码内 fallback。
- **维护规则**：
  - **新增 derived 指标时，必须同时在此文件补一条映射**，否则该派生用到的 FRED 腿可能被漏同步。
  - **Code review 检查项**：`derived_fred_deps.yaml` 是否与 `crisis_monitor.compose_series` 中依赖 FRED 的派生分支一致。

## 五、freshness_days 与「入队后是否真的发请求」

- 入队条件：序列在 catalog 且被 crisis 依赖（needed_set），**且** `check_series_freshness(series_id, freshness_days)` 返回 **True**（需要更新）才会进入本次 `filtered_series` 并实际发请求。
- `check_series_freshness` 逻辑：基于本地 `raw.csv` 最后一行日期算 `days_old`；**若 `days_old > freshness_days` 则返回 True**。因此：
  - **长期滞后的序列**（如本地卡在 2025-09）：`days_old` 很大，必然大于 `freshness_days`，会返回 True → 入队并拉取，不会被旧状态错误跳过。
- 结论：入队且「需要更新」的序列一定会发请求；仅「数据已新鲜」的序列会被跳过，避免重复拉取。

## 六、过渡方案与后续优化

- **过渡**：若 `crisis_indicators.yaml` 读取失败，脚本使用 `_fallback_needed_series()`（扩大版硬编码列表），并在日志中提示；建议后续保证配置文件可用并完全依赖配置驱动。
- **后续可做**：
  - 定期运行 `scripts/build_crisis_fred_scope.py` 并对比 `crisis_fred_sync_coverage.csv`，发现新增依赖但未入 catalog 的序列时告警或自动补 catalog。
  - 复验流程见 **同步修复后的 Smoke Test Checklist**：`docs/FRED_SYNC_SMOKE_TEST.md`。
