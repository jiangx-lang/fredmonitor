## V2 早预警逻辑说明

本文件描述 `crisis_monitor_v2.py` 的早预警机制、确认规则与数据一致性约束。

### 核心评分
- **level_score**：静态偏离程度（历史分位/基准）。
- **change_score**：动态变化（动量、斜率、加速度、波动偏离）。
- **final_score**：`w_level * level_score + w_change * change_score`（默认 0.6/0.4）。
- **freshness_factor**：按最近发布日期衰减 `change_score` 与 `w_change`（日/周不衰减，月/季随滞后线性降权）。

### 早预警指数
- **stress_now_index**：各指标 level_score 的加权均值。
- **fast_ew_index**：仅日/周频指标 change_score 的加权均值。
- **slow_macro_deterioration_index**：仅月/季频指标 change_score 的加权均值。
- **early_warning_index**：`0.7 * fast_ew_index + 0.3 * slow_macro_deterioration_index`。
- **breadth_early_warning**：触发 change_score 阈值的指标占比。
- **breadth_by_pillar**：分支柱（funding/credit/real/other）触发占比。

### 确认矩阵（Watchlist vs Confirmed）
确认需同时满足：
1) **2-of-3 信号持续触发**（最近 5 次运行里至少 3 次）  
   - A 价格压力：SPX < 200DMA  
   - B 波动结构：VIX term structure > 1  
   - C 信用压力：BAA-AAA 或 HY OAS 超阈值  
2) **信用支柱 breadth 超阈值**（credit_breadth_threshold）

若不满足则标记 **Watchlist (unconfirmed)**。

### 门控状态输出
- **Early Warning (confirmed)**：fast_ew_alert=true 且信用广度达标
- **Market Stress Watch**：fast_ew_alert=true 但信用广度未达标
- **Macro Softening Watch (unconfirmed)**：fast_ew_alert=false 且 slow_macro_deterioration_index 上行并伴随 real 广度抬升
- **All Clear**：其余情况

### 多敏感度档位（Conservative / Base / Aggressive）
一次运行同时输出三档结论，底层分数只计算一次：
- **Conservative**：更严格、低误报
- **Base**：默认
- **Aggressive**：更敏感、用于捕捉早期边际变化

输出包含：
- fast_ew_index / slow_macro_deterioration_index / early_warning_index
- fast_ew_alert / breadth（credit/real/funding）
- Verdict（All Clear / Macro Softening Watch / Market Stress Watch / Early Warning）
- stability：days_in_watch、fast_ew_trend_5d、breadth_trend_5d

### 数据一致性（P0）
对关键指标设置：
- `expected_units`
- `expected_value_range`
- `transform_chain`

检测到严重尺度异常时标记 **DATA ERROR**：
1) 报告头部提示  
2) 该指标权重置零，不参与指数  

### 运行方式
```
py crisis_monitor_v2.py
```

### 回测方式
```
py backtest_tactical_ew.py
```

### 输出
保持原有 HTML/PNG/JSON 输出结构不变，并附加：
- `level_score` / `change_score`
- `fast_ew_index` / `slow_macro_deterioration_index`
- `early_warning_index` / `stress_now_index`
- `breadth_by_pillar` / `confirmation_signals`
- `fast_ew_alert` / `status_label`
- `profiles` / `consensus_summary` / `stability`
- `data_freshness` 与 `top_drivers`

