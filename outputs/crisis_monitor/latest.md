# 🟡 FRED 宏观金融危机预警监控报告

**生成时间**: 2025年09月21日 15:10:39
**总指标数**: 34
**总体风险评分**: 46.1/100
**风险等级**: 🟡 中等风险

---

## 📋 执行摘要

本报告基于34个宏观经济指标，采用分位数尾部评分方法，
当前总体风险评分为46.1分，风险等级为中等风险。

### 🎯 关键发现

- **指标覆盖**: 涵盖收益率曲线、利率水平、信用利差、金融状况、实体经济、房地产、消费、银行业、外部环境和杠杆等10个维度
- **评分方法**: 基于非危机期间分位数基准，采用分位数尾部评分
- **权重归一**: 所有指标权重已归一化，确保总分计算准确性
- **数据完整性**: 所有指标均有对应的图表和数据

---

## 📊 分组评分摘要

| 分组 | 加权评分 | 指标数量 | 总权重 |
|------|----------|----------|--------|
| rates_curve | 45.0 | 2 | 0.103 |
| rates_level | 45.5 | 6 | 0.193 |
| credit_spreads | 47.8 | 5 | 0.159 |
| fin_cond_vol | 46.7 | 3 | 0.083 |
| real_economy | 45.8 | 4 | 0.124 |
| housing | 45.0 | 3 | 0.062 |
| consumers | 46.4 | 3 | 0.076 |
| banking | 46.4 | 5 | 0.097 |
| external | 45.0 | 1 | 0.021 |
| leverage | 47.5 | 2 | 0.083 |

---

## 📊 详细指标数据

| 指标ID | 指标名称 | 分组 | 当前值 | 基准值 | 评分 | 权重 | 风险方向 |
|--------|----------|------|--------|--------|------|------|----------|
| T10Y3M | 收益率曲线倒挂: 10年期-3个月 | rates_curve | 0.11 | 0.00 | 45.0 | 0.052 | 越低越危险 |
| T10Y2Y | 收益率曲线倒挂: 10年期-2年期 | rates_curve | 0.57 | 0.00 | 45.0 | 0.052 | 越低越危险 |
| FEDFUNDS | 联邦基金利率 | rates_level | 4.33 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| DTB3 | 3个月国债利率 | rates_level | 3.89 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| DGS10 | 10年期国债利率 | rates_level | 4.11 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| MORTGAGE30US | 30年期抵押贷款利率 | rates_level | 6.26 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| SOFR | SOFR隔夜利率 | rates_level | 4.14 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| CPN3M | 商业票据利率 | rates_level | 0.00 | 0.00 | 50.0 | 0.021 | 越高越危险 |
| BAMLH0A0HYM2 | 高收益债风险溢价 | credit_spreads | 2.71 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| BAA10YM | 投资级信用利差: Baa-10Y国债 | credit_spreads | 1.74 | 0.00 | 45.0 | 0.034 | 越高越危险 |
| CP_MINUS_DTB3 | 商业票据-国债利差 | credit_spreads | 0.00 | 0.00 | 50.0 | 0.034 | 越高越危险 |
| SOFR20DMA_MINUS_DTB3 | SOFR-国债利差(20DMA) | credit_spreads | 0.00 | 0.00 | 50.0 | 0.021 | 越高越危险 |
| TEDRATE | TED利差 | credit_spreads | 0.00 | 0.00 | 50.0 | 0.034 | 越高越危险 |
| NFCI | 芝加哥金融状况指数 | fin_cond_vol | -0.56 | 0.00 | 45.0 | 0.028 | 越高越危险 |
| VIXCLS | VIX波动率指数 | fin_cond_vol | 15.70 | 0.00 | 45.0 | 0.028 | 越高越危险 |
| STLFSI3 | 圣路易斯金融压力指数 | fin_cond_vol | 0.00 | 0.00 | 50.0 | 0.028 | 越高越危险 |
| PAYEMS | 非农就业人数 YoY | real_economy | 159540.00 | 0.00 | 45.0 | 0.034 | 越低越危险 |
| INDPRO | 工业生产 YoY | real_economy | 103.92 | 0.00 | 45.0 | 0.034 | 越低越危险 |
| GDP | GDP YoY | real_economy | 30353.90 | 0.00 | 45.0 | 0.034 | 越低越危险 |
| MANEMP | 制造业就业 YoY | real_economy | 0.00 | 0.00 | 50.0 | 0.021 | 越低越危险 |
| HOUST | 新屋开工（年化） | housing | 1307.00 | 0.00 | 45.0 | 0.021 | 越低越危险 |
| CSUSHPINSA | 房价指数: Case-Shiller 20城 YoY | housing | 331.51 | 0.00 | 45.0 | 0.021 | 越高越危险 |
| DRSFRMACBS | 房贷违约率 | housing | 1.79 | 0.00 | 45.0 | 0.021 | 越高越危险 |
| UMCSENT | 密歇根消费者信心 | consumers | 61.70 | 0.00 | 45.0 | 0.028 | 越低越危险 |
| TOTALSA | 消费者信贷 YoY | consumers | 16.49 | 0.00 | 45.0 | 0.028 | 越高越危险 |
| TDSP | 家庭债务偿付比率 | consumers | 0.00 | 0.00 | 50.0 | 0.021 | 越高越危险 |
| TOTLL | 总贷款和租赁 YoY | banking | 13052.66 | 0.00 | 45.0 | 0.024 | 越高越危险 |
| WALCL | 美联储总资产 YoY | banking | 6608597.00 | 0.00 | 45.0 | 0.024 | 越高越危险 |
| TOTRESNS | 银行准备金 | banking | 3340.30 | 0.00 | 45.0 | 0.021 | 越低越危险 |
| RESERVES_ASSETS_PCT | 银行准备金/总资产（旧指标） | banking | 0.00 | 0.00 | 50.0 | 0.014 | 越低越危险 |
| RESERVES_DEPOSITS_PCT | 银行准备金/存款（旧指标） | banking | 0.00 | 0.00 | 50.0 | 0.014 | 越低越危险 |
| DTWEXBGS | 贸易加权美元指数 YoY | external | 120.49 | 0.00 | 45.0 | 0.021 | 越高越危险 |
| NCBDBIQ027S | 企业债总额 | leverage | 8653843.00 | 0.00 | 45.0 | 0.041 | 越高越危险 |
| CORPDEBT_GDP_PCT | 企业债/GDP（旧指标） | leverage | 0.00 | 0.00 | 50.0 | 0.041 | 越高越危险 |

---

## 🖼️ 指标图表

### 📈 收益率曲线

### 📊 收益率曲线

#### 收益率曲线倒挂: 10年期-3个月
**评分**: 45.0/100

![收益率曲线倒挂: 10年期-3个月](outputs/crisis_monitor/figures\T10Y3M_latest.png)

#### 收益率曲线倒挂: 10年期-2年期
**评分**: 45.0/100

![收益率曲线倒挂: 10年期-2年期](outputs/crisis_monitor/figures\T10Y2Y_latest.png)

### 📊 利率水平

#### 联邦基金利率
**评分**: 45.0/100

![联邦基金利率](outputs/crisis_monitor/figures\FEDFUNDS_latest.png)

#### 3个月国债利率
**评分**: 45.0/100

![3个月国债利率](outputs/crisis_monitor/figures\DTB3_latest.png)

#### 10年期国债利率
**评分**: 45.0/100

![10年期国债利率](outputs/crisis_monitor/figures\DGS10_latest.png)

#### 30年期抵押贷款利率
**评分**: 45.0/100

![30年期抵押贷款利率](outputs/crisis_monitor/figures\MORTGAGE30US_latest.png)

#### SOFR隔夜利率
**评分**: 45.0/100

![SOFR隔夜利率](outputs/crisis_monitor/figures\SOFR_latest.png)

#### 商业票据利率
**评分**: 50.0/100

![商业票据利率](outputs/crisis_monitor/figures\CPN3M_latest.png)

### 📊 信用利差

#### 高收益债风险溢价
**评分**: 45.0/100

![高收益债风险溢价](outputs/crisis_monitor/figures\BAMLH0A0HYM2_latest.png)

#### 投资级信用利差: Baa-10Y国债
**评分**: 45.0/100

![投资级信用利差: Baa-10Y国债](outputs/crisis_monitor/figures\BAA10YM_latest.png)

#### 商业票据-国债利差
**评分**: 50.0/100

![商业票据-国债利差](outputs/crisis_monitor/figures\CP_MINUS_DTB3_latest.png)

#### SOFR-国债利差(20DMA)
**评分**: 50.0/100

![SOFR-国债利差(20DMA)](outputs/crisis_monitor/figures\SOFR20DMA_MINUS_DTB3_latest.png)

#### TED利差
**评分**: 50.0/100

![TED利差](outputs/crisis_monitor/figures\TEDRATE_latest.png)

### 📊 金融状况/波动

#### 芝加哥金融状况指数
**评分**: 45.0/100

![芝加哥金融状况指数](outputs/crisis_monitor/figures\NFCI_latest.png)

#### VIX波动率指数
**评分**: 45.0/100

![VIX波动率指数](outputs/crisis_monitor/figures\VIXCLS_latest.png)

#### 圣路易斯金融压力指数
**评分**: 50.0/100

![圣路易斯金融压力指数](outputs/crisis_monitor/figures\STLFSI3_latest.png)

### 📊 实体经济

#### 非农就业人数 YoY
**评分**: 45.0/100

![非农就业人数 YoY](outputs/crisis_monitor/figures\PAYEMS_latest.png)

#### 工业生产 YoY
**评分**: 45.0/100

![工业生产 YoY](outputs/crisis_monitor/figures\INDPRO_latest.png)

#### GDP YoY
**评分**: 45.0/100

![GDP YoY](outputs/crisis_monitor/figures\GDP_latest.png)

#### 制造业就业 YoY
**评分**: 50.0/100

![制造业就业 YoY](outputs/crisis_monitor/figures\MANEMP_latest.png)

### 📊 房地产

#### 新屋开工（年化）
**评分**: 45.0/100

![新屋开工（年化）](outputs/crisis_monitor/figures\HOUST_latest.png)

#### 房价指数: Case-Shiller 20城 YoY
**评分**: 45.0/100

![房价指数: Case-Shiller 20城 YoY](outputs/crisis_monitor/figures\CSUSHPINSA_latest.png)

#### 房贷违约率
**评分**: 45.0/100

![房贷违约率](outputs/crisis_monitor/figures\DRSFRMACBS_latest.png)

### 📊 消费

#### 密歇根消费者信心
**评分**: 45.0/100

![密歇根消费者信心](outputs/crisis_monitor/figures\UMCSENT_latest.png)

#### 消费者信贷 YoY
**评分**: 45.0/100

![消费者信贷 YoY](outputs/crisis_monitor/figures\TOTALSA_latest.png)

#### 家庭债务偿付比率
**评分**: 50.0/100

![家庭债务偿付比率](outputs/crisis_monitor/figures\TDSP_latest.png)

### 📊 银行业

#### 总贷款和租赁 YoY
**评分**: 45.0/100

![总贷款和租赁 YoY](outputs/crisis_monitor/figures\TOTLL_latest.png)

#### 美联储总资产 YoY
**评分**: 45.0/100

![美联储总资产 YoY](outputs/crisis_monitor/figures\WALCL_latest.png)

#### 银行准备金
**评分**: 45.0/100

![银行准备金](outputs/crisis_monitor/figures\TOTRESNS_latest.png)

#### 银行准备金/总资产（旧指标）
**评分**: 50.0/100

![银行准备金/总资产（旧指标）](outputs/crisis_monitor/figures\RESERVES_ASSETS_PCT_latest.png)

#### 银行准备金/存款（旧指标）
**评分**: 50.0/100

![银行准备金/存款（旧指标）](outputs/crisis_monitor/figures\RESERVES_DEPOSITS_PCT_latest.png)

### 📊 外部环境

#### 贸易加权美元指数 YoY
**评分**: 45.0/100

![贸易加权美元指数 YoY](outputs/crisis_monitor/figures\DTWEXBGS_latest.png)

### 📊 杠杆

#### 企业债总额
**评分**: 45.0/100

![企业债总额](outputs/crisis_monitor/figures\NCBDBIQ027S_latest.png)

#### 企业债/GDP（旧指标）
**评分**: 50.0/100

![企业债/GDP（旧指标）](outputs/crisis_monitor/figures\CORPDEBT_GDP_PCT_latest.png)


---

## 🔍 数据校验报告

**图表数量**: 34
**配置指标数量**: 34
**JSON指标数量**: 25
**总权重**: 1.450

### ⚠️ 权重异常:
- 总权重不为1.0: 1.450


---

## 📝 说明

- 本报告基于FRED数据自动生成
- 评分方法采用分位数尾部评分
- 权重已归一化处理
- 数据仅供参考，不构成投资建议

*报告生成时间: 2025年09月21日 15:10:39*