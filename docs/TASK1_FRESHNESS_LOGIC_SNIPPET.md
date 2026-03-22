# Task 1：报告「滞后」标记逻辑（只读输出）

## 1. 阈值来源

报告里「计算滞后天数并标记 ⚠️」的逻辑**不读取** `catalog_fred.yaml` 的 `freshness_days`。  
阈值来自 **`config/crisis_indicators.yaml` 的 `freq` 字段** + **代码内硬编码的「最大可接受滞后天数」**。

---

## 2. 逻辑位置与代码片段

**文件**: `crisis_monitor.py`  
**函数**: `_compute_freshness_classes()`，约第 **3364–3403** 行。

```python
def _compute_freshness_classes(processed_indicators, indicators, scoring_config, reference_date=None):
    """按周末感知的滞后阈值将指标分为「今日/可接受」与「已滞后」，排除已废弃序列。"""
    ref = reference_date or datetime.now(JST).date()
    deprecated_set = set(scoring_config.get("deprecated_series") or [])
    id_to_indicator = {ind.get("id"): ind for ind in (indicators or []) if ind.get("id")}
    fresh, stale = [], []
    for p in processed_indicators or []:
        sid = p.get("series_id")
        if sid in deprecated_set:
            continue
        last_s = p.get("last_date")
        if not last_s:
            continue
        try:
            last_d = datetime.strptime(last_s, "%Y-%m-%d").date()
        except Exception:
            continue
        age_days = (ref - last_d).days
        ind = id_to_indicator.get(sid, {})
        freq = (ind.get("freq") or "D").upper()   # ← 阈值来源 1：crisis_indicators 的 freq
        if freq in ("D", "W"):
            is_weekend_or_monday = ref.weekday() in (5, 6, 0)
            max_lag_days = 3 if is_weekend_or_monday else 2   # ← 阈值来源 2：硬编码
        elif freq == "M":
            max_lag_days = 60
        elif freq == "Q":
            max_lag_days = 120
        else:
            max_lag_days = 7
        row = {
            "name": p.get("name", sid),
            "series_id": sid,
            "last_date": last_s,
            "age_days": age_days,
            "max_lag_days": max_lag_days,
        }
        if age_days <= max_lag_days:
            fresh.append(row)
        else:
            stale.append(row)
    return fresh, stale
```

**格式化输出**（「⚠️ 已滞后」列表）在 **`_format_freshness_section()`**，约第 **3406–3423** 行，使用上面返回的 `stale` 列表。

---

## 3. 结论（Task 1）

| 项目 | 说明 |
|------|------|
| **freq 来源** | `crisis_indicators.yaml` 里每个 indicator 的 `freq` 字段（缺省为 `"D"`） |
| **阈值来源** | 代码内硬编码，未读 `catalog_fred.yaml` 的 `freshness_days` |
| **当前硬编码规则** | D/W → 2 或 3 天（周末/周一放宽到 3）；M → 60 天；Q → 120 天；其他 → 7 天 |

因此：若某指标在 `crisis_indicators.yaml` 中 `freq` 设错（例如季频写成 D），或报告侧希望与 catalog 的 freshness_days 一致，需要要么改 `freq`，要么改这段硬编码规则（或改为从 catalog 读取）。
