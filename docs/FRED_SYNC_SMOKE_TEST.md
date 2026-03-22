# FRED 同步修复后的 Smoke Test Checklist

每次改动 **catalog**（`config/catalog_fred.yaml`）或 **crisis 指标**（`config/crisis_indicators.yaml`）或 **派生依赖**（`config/derived_fred_deps.yaml`）后，建议按本清单做一次快速复验，确保同步链路与覆盖率仍正确。

---

## 1. 先看覆盖率

- 打开 **`data/crisis_fred_sync_coverage.csv`**（若不存在，先运行一次 `py scripts/build_crisis_fred_scope.py` 或 `py scripts/sync_fred_http.py`）。
- 确认以下 7 个序列的 **`status` 均为 `OK`**：
  - **DTB3**
  - **DGS10**
  - **T10Y3M**
  - **SOFR**
  - **MORTGAGE30US**
  - **BAA10YM**
  - **CPN3M**
- 若有 `MISSING_IN_CATALOG` 或 `MISSING_IN_SYNC_QUEUE`，先补 catalog 或检查 `derived_fred_deps.yaml` / crisis 配置，再继续。

---

## 2. 再跑同步

```bash
py scripts/sync_fred_http.py
```

- 观察日志：应有「Crisis 依赖的 FRED 序列: N 个」「需要同步 M 个序列」及最终「FRED数据同步完成: x/M 成功」。
- 同步结束后会重写 `data/crisis_fred_sync_coverage.csv`，可再次打开确认上述 7 个仍为 OK。

---

## 3. 本地末行日期对比（闭环）

- 用 **`scripts/verify_fred_recent.py`** 看 FRED 端最近 30 天、最后 5 行日期（脚本会打印）。
- 直接看本地文件最后一行日期是否与之一致或接近（考虑发布滞后 1～2 个工作日）：
  - **`data/fred/series/DTB3/raw.csv`**
  - **`data/fred/series/DGS10/raw.csv`**
  - **`data/fred/series/SOFR/raw.csv`**
- 若本地末行明显早于 FRED 端（例如仍停在数月前），说明该序列未入队或未实际发请求，需回到步骤 1 查 coverage 与配置。

---

## 4. 可选：派生依赖一致性

- 若新增或修改了 **派生指标**（如 `compose_series` 里新分支）：
  - 必须在 **`config/derived_fred_deps.yaml`** 中补全该派生依赖的 FRED 腿；
  - 运行 **`py scripts/build_crisis_fred_scope.py`**，再查 `data/crisis_required_fred_series.csv` 中对应依赖是否 `in_catalog` / `should_sync` 合理。

---

## 5. 检查顺序小结

| 顺序 | 动作 | 目的 |
|------|------|------|
| 1 | 看 coverage CSV，确认 7 序列 OK | 保证入队范围正确 |
| 2 | 跑 `sync_fred_http.py` | 实际拉取 |
| 3 | 对比本地 raw 末行与 verify_fred_recent 输出 | 确认真的推进到最近发布日期 |

按此顺序做，即可在改 catalog / 指标 / 派生依赖后快速确认「同步修复」仍有效。
