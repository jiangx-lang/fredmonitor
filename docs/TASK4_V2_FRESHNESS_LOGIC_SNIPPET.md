# Task 4：V2 报告「滞后」/stale 列表逻辑（只读输出）

## 1. 逻辑位置

**文件**: `crisis_monitor_v2.py`  
**函数**: `postprocess_reports()`，约第 **2003–2051** 行（Data freshness & coverage 块）。

---

## 2. 阈值从哪里读取？

- **freq**：从 **`config/crisis_indicators.yaml`** 的每个 indicator 的 **`freq`** 字段读取；缺省为 `"D"`。  
  - 代码：`cfg = indicators_config.get(series_id, {})`，`freq = (cfg.get("freq") or "D").upper()`。
- **阈值**：**不读** `catalog_fred.yaml` 的 `freshness_days`，使用**代码内硬编码字典**。

---

## 3. 硬编码字典（现有）

```python
# 第 2008 行
stale_thresholds = {"D": 15, "W": 30, "M": 60, "Q": 120}
```

- 使用方式：`threshold = stale_thresholds.get(freq[:1], 30)`，即按 freq 首字母取 D/W/M/Q，缺省 30 天。
- 与 prompt 建议对比：
  - 季频 Q：120 ✅ 已一致
  - 月频 M：60 ✅ 已一致
  - 周频 W：30 → prompt 建议 10
  - 日频 D：15 → prompt 建议 3

---

## 4. 相关代码片段（摘录）

```python
    # Data freshness & coverage
    config = base.load_yaml_config(base.BASE / "config" / "crisis_indicators.yaml")
    indicators_config = { (i.get("series_id") or i.get("id")): i for i in config.get("indicators", []) }
    deprecated_series = set(config.get("scoring", {}).get("deprecated_series", []))
    today = pd.Timestamp.now(tz=base.JST).date()
    stale_thresholds = {"D": 15, "W": 30, "M": 60, "Q": 120}  # ← 硬编码
    stale_series = []
    # ...
    for item in json_data.get("indicators", []):
        series_id = item.get("series_id")
        # ...
        cfg = indicators_config.get(series_id, {})
        freq = (cfg.get("freq") or "D").upper()   # ← 从 crisis_indicators 读 freq
        threshold = stale_thresholds.get(freq[:1], 30)   # ← 用硬编码字典
        # ...
        if last_date:
            dt = pd.to_datetime(last_date).date()
            lag = (today - dt).days
            if freq.startswith("D"):
                weekday = today.weekday()
                max_ok = 3 if weekday in {0, 5, 6} else 1
                # ... 日频还有周末放宽逻辑
            elif lag > threshold:
                stale_series.append(series_id)
                stale_weight += weight
```

---

## 5. 结论（Task 4）

| 项目 | 说明 |
|------|------|
| **阈值来源** | 硬编码字典 `stale_thresholds = {"D": 15, "W": 30, "M": 60, "Q": 120}`，缺省 30 |
| **freq 来源** | `crisis_indicators.yaml` 的 `freq` |
| **是否读 catalog_fred.yaml** | 否 |
| **若按 prompt 标准改** | Q=120、M=60 保持；W 改为 10；D 改为 3；缺省可改为 10 或 30 |

**先打印现有逻辑，等你确认后再改。** 需要改时再动 `stale_thresholds` 及（若有）日频周末逻辑。
