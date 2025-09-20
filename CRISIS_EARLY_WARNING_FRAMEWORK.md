# 📑 美国金融危机预警检测框架（1929–2023 历史总结版）

## 🎯 框架概述

本框架基于1929-2023年历史危机事件总结，设计了一套标准化的金融危机预警检测体系。框架包含20个核心检测项目，覆盖六大风险类别，可直接应用于FRED数据库和内部宏观打分系统。

---

## 📊 一、信贷与资产市场类

### 1. 信贷快速扩张（Credit Boom）
**检测内容**: 银行贷款总量/私人部门信贷增速异常偏高
**FRED数据源**: 
- `TOTLL`: Total Loans and Leases at All Commercial Banks
- `TOTCI`: Total Consumer Credit Outstanding
- `TOTALSA`: Total Consumer Credit Outstanding (Seasonally Adjusted)

**解释**: 历史上大多数危机（1929、2007）都伴随信贷过度扩张，是泡沫形成的燃料。

**预警阈值**: 年化信贷增速 > 15% 或 超过历史均值2个标准差

---

### 2. 贷款标准放松（Lending Standards）
**检测内容**: Senior Loan Officer Survey（SLOOS）显示贷款条件持续宽松
**FRED数据源**:
- `DEMAUTO`: Net Percentage of Domestic Banks Reporting Stronger Demand for Auto Loans
- `DEMCC`: Net Percentage of Domestic Banks Reporting Stronger Demand for Credit Card Loans
- `DRISCFLM`: Net Percentage of Domestic Banks Increasing Spreads of Prime Loans

**解释**: 信贷质量恶化的前兆，说明银行在"放水"。

**预警阈值**: 连续3个月净宽松比例 > 20%

---

### 3. 贷款违约率上升（Delinquency/Default Rates）
**检测内容**: 抵押贷款、商业地产贷款、信用卡等违约率变化
**FRED数据源**:
- `DALLACBEP`: Asset Quality Measures, Delinquencies on All Loans
- `DALLCACBEP`: Asset Quality Measures, Delinquencies on All Loans
- `DALLCCACBEP`: Asset Quality Measures, Delinquencies on All Loans

**解释**: 最直接的危机早期信号，2006次贷危机前已有显著上升。

**预警阈值**: 违约率上升 > 50bps 或 超过历史均值1.5个标准差

---

### 4. 资产价格泡沫（Asset Price Bubble）
**检测内容**: 股票/房地产价格远超基本面（P/E极高，房价收入比偏离）
**FRED数据源**:
- `SP500`: S&P 500
- `CSUSHPINSA`: S&P/Case-Shiller U.S. National Home Price Index
- `MEDLISPRI`: Median Sales Price of Houses Sold for the United States
- `MEHOINUSA646N`: Real Median Household Income in the United States

**解释**: 1929股市、2000科网、2007房市均出现典型泡沫。

**预警阈值**: 
- 房价收入比 > 5.0
- 股市P/E > 25
- 房价年化增速 > 15%

---

### 5. 借款成本/利率上升（Borrowing Costs）
**检测内容**: 短期/长期利率、抵押贷款利率快速走高
**FRED数据源**:
- `FEDFUNDS`: Federal Funds Effective Rate
- `DGS10`: 10-Year Treasury Constant Maturity Rate
- `MORTGAGE30US`: 30-Year Fixed Rate Mortgage Average in the United States
- `DPRIME`: Bank Prime Loan Rate

**解释**: 利率上升会戳破泡沫，加剧偿债压力。

**预警阈值**: 利率上升 > 200bps 或 超过历史均值2个标准差

---

## 💧 二、流动性与资金市场类

### 6. 隔夜/回购利率飙升（Repo/FFR/CP Rates）
**检测内容**: SOFR、回购利率、商业票据利差异常波动
**FRED数据源**:
- `SOFR`: Secured Overnight Financing Rate
- `EFFR`: Effective Federal Funds Rate
- `CPF3M`: 3-Month Commercial Paper Rate
- `DTB3`: 3-Month Treasury Bill Secondary Market Rate

**解释**: 资金市场最敏感的流动性压力指标，1970、2019、2020都有体现。

**预警阈值**: SOFR-EFFR利差 > 50bps 或 商业票据利差 > 100bps

---

### 7. TED利差（TED Spread）
**检测内容**: 3M Libor/CP vs Tbill利差
**FRED数据源**:
- `TEDRATE`: TED Spread
- `CPF3M`: 3-Month Commercial Paper Rate
- `DTB3`: 3-Month Treasury Bill Secondary Market Rate

**解释**: 衡量银行间信用风险，一旦急升说明市场不信任对手方。

**预警阈值**: TED利差 > 100bps 或 超过历史均值2个标准差

---

### 8. "飞向安全"现象（Flight to Quality）
**检测内容**: 高风险资产抛售，美债/Tbill收益率骤降
**FRED数据源**:
- `DGS10`: 10-Year Treasury Constant Maturity Rate
- `DGS2`: 2-Year Treasury Constant Maturity Rate
- `DTB3`: 3-Month Treasury Bill Secondary Market Rate
- `SP500`: S&P 500

**解释**: 危机前夕资金流向安全资产，2020疫情初期尤为明显。

**预警阈值**: 10年期国债收益率下降 > 100bps 且股市下跌 > 10%

---

## 🏦 三、信用与杠杆类

### 9. 信用利差扩大（Credit Spreads）
**检测内容**: 高收益债OAS、BAA–AAA利差
**FRED数据源**:
- `BAA`: Moody's Seasoned Baa Corporate Bond Yield
- `AAA`: Moody's Seasoned Aaa Corporate Bond Yield
- `BAMLC0A0CM`: ICE BofA US Corporate Index Option-Adjusted Spread

**解释**: 市场提前反映信用风险，2007–2009前利差显著走阔。

**预警阈值**: BAA-AAA利差 > 200bps 或 高收益债OAS > 500bps

---

### 10. 非金融部门杠杆上升（Nonfinancial Leverage）
**检测内容**: 家庭/企业债务占GDP、债务偿付比率
**FRED数据源**:
- `TOTALSA`: Total Consumer Credit Outstanding
- `GDP`: Gross Domestic Product
- `TDSP`: Household Debt Service Payments as a Percent of Disposable Personal Income
- `NFCI`: Chicago Fed National Financial Conditions Index

**解释**: 杠杆过高使经济对利率变化极敏感，2007危机前美国家庭DSR居高不下。

**预警阈值**: 
- 家庭债务/GDP > 80%
- 债务偿付比率 > 12%

---

### 11. 银行贷款/存款错配（Loan-to-Deposit Ratio）
**检测内容**: 银行LDR、非保本存款比例
**FRED数据源**:
- `TOTLL`: Total Loans and Leases at All Commercial Banks
- `TOTRESNS`: Reserves of Depository Institutions: Total
- `TOTDD`: Total Deposits at All Commercial Banks

**解释**: 2023 SVB暴露此问题 → 挤兑风险。

**预警阈值**: LDR > 90% 或 非保本存款比例 > 60%

---

## 📈 四、收益率曲线与宏观基本面

### 12. 收益率曲线倒挂（Yield Curve Inversion）
**检测内容**: 10Y–3M、10Y–2Y利差
**FRED数据源**:
- `DGS10`: 10-Year Treasury Constant Maturity Rate
- `DGS2`: 2-Year Treasury Constant Maturity Rate
- `DTB3`: 3-Month Treasury Bill Secondary Market Rate

**解释**: 最经典的衰退/危机前信号，提前12–24个月出现。

**预警阈值**: 10Y-2Y利差 < 0 或 10Y-3M利差 < -50bps

---

### 13. 信用增长放缓（Credit Growth Slowdown）
**检测内容**: 信贷从高增速 → 急速放缓或收缩
**FRED数据源**:
- `TOTLL`: Total Loans and Leases at All Commercial Banks
- `TOTALSA`: Total Consumer Credit Outstanding

**解释**: Reinhart & Rogoff研究证明，信贷扩张后的骤停是危机高概率前兆。

**预警阈值**: 信贷增速从 > 10% 降至 < 2% 或 负增长

---

### 14. 宏观衰退信号（Macro Weakness）
**检测内容**: ISM制造业PMI、新屋开工、消费者信心
**FRED数据源**:
- `UMCSENT`: University of Michigan: Consumer Sentiment
- `HOUST`: Housing Starts: Total: New Privately Owned Housing Units Started
- `PAYEMS`: All Employees, Total Nonfarm

**解释**: 1990信贷紧缩、2000科网泡沫破裂前均有明显下降。

**预警阈值**: 
- PMI < 50
- 新屋开工下降 > 20%
- 消费者信心 < 80

---

## 🌍 五、外部失衡与政策

### 15. 经常账户赤字恶化（Current Account Deficit）
**检测内容**: CA/GDP占比持续扩大
**FRED数据源**:
- `BOPGSTB`: Balance on Current Account
- `GDP`: Gross Domestic Product

**解释**: 外部依赖资金流入，一旦外资撤退，容易触发危机（1982拉美债务）。

**预警阈值**: 经常账户赤字/GDP > 5%

---

### 16. 美元流动性紧缩（Global Dollar Liquidity）
**检测内容**: 美元指数、离岸美元融资成本
**FRED数据源**:
- `DTWEXBGS`: Nominal Broad U.S. Dollar Index
- `DTWEXM`: Nominal Major Currencies U.S. Dollar Index

**解释**: 美元收紧时新兴市场违约风险飙升，反馈到美银行。

**预警阈值**: 美元指数上升 > 10% 或 超过历史均值2个标准差

---

### 17. 货币政策急转弯（Policy Shock）
**检测内容**: 联储快速加息/缩表
**FRED数据源**:
- `FEDFUNDS`: Federal Funds Effective Rate
- `WALCL`: Assets: Total Assets: Total Assets (Less Eliminations from Consolidation)

**解释**: 1980s Volcker暴力加息、1994债市危机、2023银行业动荡均由此引爆。

**预警阈值**: 联邦基金利率上升 > 200bps 或 资产负债表收缩 > 10%

---

## 📊 六、综合指数与市场情绪

### 18. 金融压力指数（STLFSI / NFCI）
**检测内容**: 圣路易斯金融压力指数、芝加哥金融状况指数
**FRED数据源**:
- `STLFSI`: St. Louis Fed Financial Stress Index
- `NFCI`: Chicago Fed National Financial Conditions Index

**解释**: 综合信用、利差、波动等，常在危机前6–12个月走高。

**预警阈值**: STLFSI > 1.0 或 NFCI > 0.5

---

### 19. 市场波动率（VIX、MOVE Index）
**检测内容**: 股市/债市隐含波动率
**FRED数据源**:
- `VIXCLS`: CBOE Volatility Index: VIX
- `MOVE`: ICE BofA MOVE Index

**解释**: 危机爆发前，波动率往往提前上升。

**预警阈值**: VIX > 30 或 MOVE > 150

---

### 20. 投资者信心恶化（Consumer & Investor Sentiment）
**检测内容**: 密歇根消费者信心、AAII投资者情绪
**FRED数据源**:
- `UMCSENT`: University of Michigan: Consumer Sentiment
- `UMCSENT1`: University of Michigan: Consumer Sentiment

**解释**: 情绪面下滑会加速资金撤退，导致危机放大。

**预警阈值**: 消费者信心 < 80 或 投资者情绪 < 30%

---

## 🎯 框架实施建议

### 数据更新频率
- **高频指标** (日度): 市场波动率、利率、汇率
- **中频指标** (周度): TED利差、信用利差
- **低频指标** (月度): 信贷数据、宏观指标

### 预警等级
- **🟢 绿色**: 正常范围
- **🟡 黄色**: 关注级别 (超过1个标准差)
- **🟠 橙色**: 警告级别 (超过2个标准差)
- **🔴 红色**: 危机级别 (超过3个标准差)

### 综合评分
每个指标按权重计算综合风险评分：
- 信贷与资产市场类: 30%
- 流动性与资金市场类: 25%
- 信用与杠杆类: 20%
- 收益率曲线与宏观基本面: 15%
- 外部失衡与政策: 5%
- 综合指数与市场情绪: 5%

---

## 📌 框架总结

**20个检测项目，覆盖六大类**:
1. 信贷与资产市场 (5项)
2. 流动性与资金市场 (3项)
3. 信用与杠杆 (3项)
4. 收益率曲线与宏观基本面 (3项)
5. 外部失衡与政策 (3项)
6. 综合指数与市场情绪 (3项)

每个项目都包含：
- ✅ FRED数据源映射
- ✅ 具体检测内容
- ✅ 历史解释
- ✅ 预警阈值
- ✅ 实施建议

**可直接作为未来风险监控面板的"指标库"**，支持实时监控和历史回测。
