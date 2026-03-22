# 滞后阈值修复说明

## _compute_freshness_classes() 输出影响报告的哪个部分

- **函数**: `crisis_monitor.py` 中 `_compute_freshness_classes()`（约 3364–3403 行）。
- **D/W 完整判断逻辑**（修改前）:
  ```python
  if freq in ("D", "W"):
      is_weekend_or_monday = ref.weekday() in (5, 6, 0)
      max_lag_days = 3 if is_weekend_or_monday else 2
  elif freq == "M":
      max_lag_days = 60
  elif freq == "Q":
      max_lag_days = 120
  else:
      max_lag_days = 7
  # ...
  if age_days <= max_lag_days:
      fresh.append(row)
  else:
      stale.append(row)
  ```
- **输出影响**: 返回值 `(fresh, stale)` 传入 `_format_freshness_section()`，写入报告中的 **「## 📅 数据新鲜度与覆盖」** 小节，直接生成 **「✅ 今日已更新 / 可接受滞后」** 与 **「⚠️ 已滞后（建议关注或更新数据源）」** 列表（见 3713–3714 行）。  
- **结论**: 这套阈值**直接控制**报告里的 ⚠️ 已滞后列表，不是仅热力图。因此需要一并把 D 改为 7、W 改为 10。

---

## 修改内容

### 1. crisis_monitor.py — _compute_freshness_classes()

**修改前**:
```python
        if freq in ("D", "W"):
            is_weekend_or_monday = ref.weekday() in (5, 6, 0)
            max_lag_days = 3 if is_weekend_or_monday else 2
        elif freq == "M":
```

**修改后**:
```python
        if freq == "D":
            max_lag_days = 7   # 日频：留周末+发布延迟缓冲
        elif freq == "W":
            max_lag_days = 10  # 周频：最多 10 天属正常发布节奏
        elif freq == "M":
```

- `_format_freshness_section()` 的说明文案已改为「日频 7 天、周频 10 天、月频 60 天、季频 120 天」。

### 2. crisis_monitor_v2.py — postprocess_reports() stale_thresholds

**修改前**:
```python
    stale_thresholds = {"D": 15, "W": 30, "M": 60, "Q": 120}
```

**修改后**:
```python
    stale_thresholds = {"D": 7, "W": 10, "M": 60, "Q": 120}
```

- 日频 (D) 分支：改为仅当 `lag > 7` 时加入 `stale_series`，与 threshold=7 一致；`lag <= 7` 时保留原有 `stale_but_acceptable` 逻辑。
