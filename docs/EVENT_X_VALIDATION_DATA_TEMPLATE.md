# Event-X 历史验证数据清单模板

目标：为 Cursor / 维护者提供**按日对齐的历史风险腿数据表**，用于比较「信号首次触发日」与「场景确认日」的关系，而不是报告截图或新闻链接。

---

## 1) 最小必备数据表：event_x_validation_dataset.csv

| 建议 | 说明 |
|------|------|
| 频率 | 日频 |
| 时间范围 | 至少 2021-01-01 至今（覆盖油价冲击、信用利差走阔、波动抬升未共振、近年多种风险窗口） |
| 主键 | `date` |

### 主键与主腿字段

| 字段 | 来源 | 含义 |
|------|------|------|
| **date** | 对齐日历 | 交易日/观测日 |
| **hy_oas** | FRED BAMLH0A0HYM2 | 美国高收益债 OAS，垃圾债风险溢价 |
| **stlfsi4** | FRED STLFSI4 | 圣路易斯联储金融压力指数（18 个周度金融变量） |
| **bizd_close** | Yahoo Finance BIZD | BDC/私募信贷公开市场影子代理 |
| **brent_spot** | FRED DCOILBRENTEU 或 EIA Brent Europe | Brent 现货，油价冲击主腿 |
| **t5yie** | FRED T5YIE | 5 年 breakeven inflation，市场通胀预期 |
| **vix** | FRED VIXCLS | VIX 风险情绪指标 |
| **dgs5** | FRED DGS5 | 5 年期名义国债收益率（breakeven proxy 用） |
| **t5yifr** | FRED T5YIFR | 5Y5Y 远期通胀（breakeven proxy 用） |
| **breakeven_effective** | 规则 | 优先 t5yie；若 stale 则 dgs5 - t5yifr |

---

## 2) 衍生特征表：event_x_validation_features.csv

| 分类 | 字段 | 公式/说明 |
|------|------|-----------|
| Private Credit | hy_oas_5d_bp_change | (hy_oas - hy_oas.shift(5)) * 100；验证 HY 动量 Watch 是否早于绝对阈值 |
| | bizd_50dma | BIZD 50 日均价 |
| | bizd_vs_50dma_pct | (bizd_close / bizd_50dma - 1) * 100 |
| | stlfsi4_4w_change | 金融压力 4 周变化 |
| Geopolitics | brent_yoy_pct | Brent 同比 % |
| | breakeven_source_used | FRED_T5YIE / REALTIME_PROXY / NONE |
| | breakeven_is_stale | 是否过期 |
| | breakeven_quality | HIGH / MEDIUM / LOW |
| | vix_5d_change | VIX 5 日变化 |
| Resonance | private_credit_watch_flag | 由规则引擎按日重放填充 |
| | geopolitics_watch_flag | 同上 |
| | resonance_level | 同上 |
| | credit_stress_on | 同上 |

---

## 3) 场景标签表：event_x_validation_scenarios.csv

| 字段 | 说明 |
|------|------|
| scenario_name | 场景名称 |
| date_start | 窗口起始 |
| date_end | 窗口结束 |
| scenario_type | oil_shock / credit_widening / vol_no_resonance / true_resonance |
| expected_behavior | 预期行为描述 |
| notes | 备注 |

最少四类场景：A. Oil shock；B. Credit widening；C. Volatility rise without full resonance；D. True resonance。

---

## 4) 数据源优先级

1. **FRED**：BAMLH0A0HYM2, STLFSI4, T5YIE, DGS5, T5YIFR, VIXCLS, DCOILBRENTEU  
2. **EIA**：Brent 历史现货（可选，当前脚本用 FRED DCOILBRENTEU）  
3. **Yahoo Finance**：BIZD 历史价格  

---

## 5) 字段解释（给 Cursor / 读者）

- **hy_oas**：高收益债绝对风险溢价，衡量垃圾债融资环境有多紧。  
- **hy_oas_5d_bp_change**：高收益债风险溢价最近 5 天恶化速度；即使绝对水平不高，若快速走阔也可能是早期信用压力。  
- **stlfsi4**：系统性金融压力水平；水平低不代表没有变化，若变化很快也要关注。  
- **bizd_vs_50dma_pct**：私募信贷公开市场代理是否明显走弱。  
- **brent_spot**：油价冲击主腿。  
- **breakeven_effective**：市场对未来通胀的近实时定价主腿。  
- **vix**：市场恐慌或避险情绪主腿。  
- **resonance_level**：多个风险腿是否形成闭环，而不是单点噪音。  

---

## 6) 执行目标（验证层 / 结论层）

- **数据层**：一张按日期对齐的原始表、一张衍生特征表、一张场景标签表。  
- **验证层**：每场景输出首次触发日、首次升级日、场景确认日、提前/滞后天数、是否太敏感、是否只会解释过去。  
- **结论层**：每场景一句 PASS / FAIL / PARTIAL 及原因。  

---

## 7) 生成方式

```bash
py scripts/build_event_x_validation_dataset.py --start 2021-01-01 --out-dir data/event_x_validation
```

产出路径（默认）：`data/event_x_validation/`  
- `event_x_validation_dataset.csv`  
- `event_x_validation_features.csv`  
- `event_x_validation_scenarios.csv`  

所有外部源 fail-open；缺失值不阻断输出。
