# FRED 风险监控系统架构

## 🏗️ 系统架构图

```
D:\fred_crisis_monitor\
├── crisis_monitor/                    # 宏观危机监测系统
│   ├── crisis_monitor.py             # 主程序 (低频+宏观)
│   ├── config/
│   │   ├── crisis_indicators.yaml    # 26个宏观指标配置
│   │   └── crisis_periods.yaml       # 历史危机期间定义
│   └── outputs/
│       └── crisis_monitor/           # 输出目录
│
├── daily_risk_dashboard/              # 高频风险监控系统 ⭐ 新增
│   ├── risk_dashboard.py             # 主程序 (高频+灵敏)
│   ├── test_risk_dashboard.py        # 测试脚本
│   ├── config/
│   │   └── risk_dashboard.yaml       # 20个高频指标配置
│   ├── outputs/                      # 输出目录
│   ├── run_test.bat                  # 测试批处理
│   ├── run_dashboard.bat             # 运行批处理
│   └── README.md                     # 系统说明
│
├── scripts/                          # 公共函数库
│   ├── fred_http.py                  # FRED API接口
│   ├── clean_utils.py                # 数据清洗工具
│   └── viz.py                        # 图表生成工具
│
└── data/                             # 数据缓存
    └── fred/
        └── series/                   # FRED数据缓存
```

## 📊 两系统对比

| 特性 | crisis_monitor | risk_dashboard |
|------|----------------|----------------|
| **定位** | 宏观危机预警 | 高频风险监控 |
| **数据频率** | 月频/季频 | 日频/周频 |
| **指标数量** | 26个 | 20个 |
| **监控重点** | 长期危机趋势 | 短期风险情绪 |
| **更新频率** | 日/周 | 日 |
| **适用场景** | 危机预警 | 牛市逃顶 |
| **输出格式** | 详细报告+长图 | 风险面板+JSON |

## 🎯 指标分类对比

### crisis_monitor (宏观指标)
- **收益率曲线**: T10Y3M, T10Y2Y
- **利率水平**: FEDFUNDS, DTB3, DGS10, MORTGAGE30US, SOFR
- **信用利差**: BAMLH0A0HYM2, BAA10YM
- **实体经济**: PAYEMS, INDPRO, GDP
- **房地产**: HOUST, CSUSHPINSA
- **消费**: UMCSENT, TOTALSA
- **银行业**: TOTLL, WALCL
- **外部环境**: DTWEXBGS
- **杠杆**: CORPDEBT_GDP_PCT

### risk_dashboard (高频指标)
- **波动/期限**: VIXCLS, SKEW, T10Y3M, T10Y2Y, MOVE_PROXY
- **信用**: BAMLH0A0HYM2, BAMLC0A0CM, TEDRATE, BAA10Y
- **流动性**: RRPONTSYD, WSHOMCB, WTREGEN, IORB_EFFR_SPRD
- **风险偏好**: DTWEXBGS, DCOILWTICO, GOLDAMGBD228NLBM, SPX_UTIL_RATIO
- **压力综合**: STLFSI2, NFCI

## 🔄 运行流程

### 日常运行 (推荐)
```bash
# 06:30 AM - 宏观危机监测
cd D:\fred_crisis_monitor
python crisis_monitor.py

# 06:35 AM - 高频风险监控
cd D:\fred_crisis_monitor\daily_risk_dashboard
python risk_dashboard.py
```

### 测试运行
```bash
# 测试高频系统
cd D:\fred_crisis_monitor\daily_risk_dashboard
python test_risk_dashboard.py
# 或双击 run_test.bat
```

## 📈 输出文件

### crisis_monitor 输出
- `outputs/crisis_monitor/crisis_report_YYYYMMDD_HHMMSS.md`
- `outputs/crisis_monitor/crisis_report_YYYYMMDD_HHMMSS.html`
- `outputs/crisis_monitor/crisis_report_YYYYMMDD_HHMMSS.json`
- `outputs/crisis_monitor/crisis_report_long_YYYYMMDD_HHMMSS.png`

### risk_dashboard 输出
- `outputs/risk_dashboard_YYYYMMDD_HHMMSS.png` (风险面板图片)
- `outputs/risk_dashboard_YYYYMMDD_HHMMSS.json` (结构化数据)

## 🎨 可视化对比

### crisis_monitor 图表
- 详细的指标分析图表
- 历史危机期间阴影标记
- 移动平均线和趋势分析
- 完整的报告格式

### risk_dashboard 图表
- 紧凑的风险面板布局
- 实时风险评分圆盘图
- 风险等级指示器
- 指标详情表格
- 触发统计信息

## 🔧 技术架构

### 共同依赖
- **FRED API**: 数据获取
- **pandas/numpy**: 数据处理
- **matplotlib**: 图表生成
- **yaml**: 配置管理

### 独特功能
- **crisis_monitor**: 长图生成、HTML报告、危机期间分析
- **risk_dashboard**: 分位数评分、动量检测、共振分析

## 🚀 未来扩展

### 计划功能
1. **dashboard_combiner.py**: 整合两个系统的输出
2. **实时推送**: 高风险时自动发送通知
3. **历史回测**: 验证预警准确性
4. **自定义指标**: 支持用户添加新指标

### 集成方案
```python
# 未来可能的整合脚本
def generate_combined_report():
    # 运行两个系统
    crisis_data = run_crisis_monitor()
    risk_data = run_risk_dashboard()
    
    # 生成综合报告
    combined_report = combine_reports(crisis_data, risk_data)
    
    # 输出总览图
    generate_overview_chart(combined_report)
```

## ⚠️ 注意事项

1. **数据依赖**: 两个系统都依赖FRED API，需要稳定的网络连接
2. **缓存共享**: 数据缓存目录共享，避免重复下载
3. **资源占用**: 同时运行两个系统时注意内存和CPU使用
4. **时间安排**: 建议错开运行时间，避免API限制

## 📞 技术支持

如有问题，请检查：
1. Python环境和依赖包
2. FRED API访问权限
3. 配置文件格式
4. 数据缓存完整性











