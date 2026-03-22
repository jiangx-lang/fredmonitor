# FRED 同步覆盖率说明

## 产出文件

- **`data/crisis_fred_sync_coverage.csv`**  
  每次运行 `scripts/sync_fred_http.py` 后更新，用于审计「crisis 依赖的 FRED 序列」是否都在 catalog 且进入同步队列。

## 字段含义

| 字段 | 说明 |
|------|------|
| `series_id` | FRED 序列 ID |
| `required_by_crisis` | 是否被 crisis 监控依赖（来自 crisis_indicators + 派生依赖） |
| `present_in_catalog` | 是否在 `config/catalog_fred.yaml` 中 |
| `present_in_sync_queue` | 是否进入本次同步队列（catalog ∩ crisis_required） |
| `status` | `OK` / `MISSING_IN_CATALOG` / `MISSING_IN_SYNC_QUEUE` |

## 使用方式

- 查看当前覆盖率：直接打开 `data/crisis_fred_sync_coverage.csv`。
- 根因与修复说明：见 [FRED_SYNC_SCOPE_FIX.md](FRED_SYNC_SCOPE_FIX.md)。
