# FRED 危机预警监控系统

## 系统概述

FRED危机预警监控系统是一个基于美国联邦储备经济数据(FRED)的宏观经济风险监测平台，通过实时分析40+个关键经济指标，提供综合风险评分和预警功能。

## 主要功能

### 1. 每日风险分析
- **实时数据获取**: 自动从FRED API获取最新经济数据
- **智能评分系统**: 基于40+个指标的综合风险评分
- **风险等级分类**: 极低风险、低风险、中等风险、高风险、极高风险
- **报告生成**: 自动生成Markdown、HTML、JSON格式的分析报告

### 2. 数据管道
- **增量更新**: 智能检测数据变化，只更新过期数据
- **数据预处理**: 自动计算YoY指标、合成指标
- **数据验证**: 确保数据完整性和准确性

### 3. 可视化分析
- **图表生成**: 为每个指标生成专业图表
- **长图报告**: 使用Playwright生成完整的长图报告
- **危机期间标注**: 在图表中标注历史危机期间

## 系统架构

```
fred_crisis_monitor/
├── core/                    # 核心模块
│   ├── aggregator.py       # 数据聚合器
│   ├── database_integration.py  # 数据库集成
│   ├── registry.py         # 因子注册表
│   └── utils.py           # 工具函数
├── scripts/                # 脚本目录
│   ├── sync_fred_http.py  # FRED数据同步
│   ├── calculate_corporate_debt_gdp_ratio.py  # 企业债/GDP计算
│   └── calculate_yoy_indicators.py  # YoY指标计算
├── config/                 # 配置文件
│   ├── indicators.yaml    # 指标配置
│   └── crisis_periods.yaml # 危机期间定义
├── data/                  # 数据目录
│   ├── fred/              # FRED原始数据
│   └── series/            # 处理后的数据
├── outputs/               # 输出目录
│   └── crisis_monitor/    # 分析报告
└── macrolab_gui.py        # GUI主程序
```

## 快速开始

### 1. 环境准备
```bash
# 安装Python依赖
pip install -r requirements.txt

# 配置环境变量
cp macrolab.env.example macrolab.env
# 编辑macrolab.env，设置FRED_API_KEY
```

### 2. 运行系统
```bash
# 启动GUI界面
python macrolab_gui.py

# 或直接运行主程序
python crisis_monitor.py
```

### 3. 每日分析
1. 点击"运行每日分析"按钮
2. 系统自动获取最新数据
3. 生成风险评分和报告
4. 查看输出结果

## 指标分类

### 金融早期信号 (40%)
- **VIX_RISK**: 市场波动率 (8%)
- **YIELD_CURVE**: 收益率曲线 (12%)
- **CREDIT_SPREAD**: 信用利差 (10%)
- **TED_SPREAD**: TED利差 (6%)
- **NFCI**: 金融状况指数 (4%)

### 实体经济指标 (25%)
- **PAYEMS**: 非农就业 (5%)
- **INDPRO**: 工业生产 (5%)
- **GDP**: GDP (4%)
- **NEWORDER**: 新订单 (3%)
- **MANEMP**: 制造业就业 (2%)
- **IC4WSA**: 初请失业金 (2%)
- **AWHMAN**: 制造业工时 (2%)
- **PERMIT**: 建筑许可 (2%)

### 利率与货币政策 (15%)
- **FEDFUNDS**: 联邦基金利率 (4%)
- **DGS10**: 10年期国债 (3%)
- **MORTGAGE30US**: 30年期按揭利率 (3%)
- **SOFR**: SOFR利率 (2%)
- **DTB3**: 3个月国债 (2%)
- **CPN3M**: 3个月商业票据 (1%)

### 银行与信贷 (10%)
- **TOTLL**: 总贷款 (3%)
- **TOTALSA**: 消费者信贷 (3%)
- **TDSP**: 家庭债务偿付比率 (2%)
- **TOTRESNS**: 银行准备金 (2%)
- **WALCL**: 美联储总资产 (2%)

### 房地产与消费 (5%)
- **HOUSING_STRESS**: 房地产压力 (2%)
- **CSUSHPINSA**: 房价指数 (2%)
- **UMICH_CONF**: 消费者信心 (1%)

### 外部与杠杆 (3%)
- **DXY_VOL**: 美元指数 (1%)
- **NCBDBIQ027S**: 企业债/GDP (1%)
- **STLFSI3**: 圣路易斯金融压力 (1%)

### 监测项 (2%)
- **DRSFRMACBS**: 房贷违约率 (1%)
- **HOUST**: 新屋开工 (1%)

## 数据流程

### 1. 数据获取
```python
# 使用sync_fred_http.py进行增量更新
python scripts/sync_fred_http.py
```

### 2. 数据处理
```python
# 计算企业债/GDP比率
python scripts/calculate_corporate_debt_gdp_ratio.py

# 计算YoY指标
python scripts/calculate_yoy_indicators.py
```

### 3. 风险分析
```python
# 使用数据库集成器进行分析
from core.database_integration import DatabaseIntegration
db = DatabaseIntegration()
result = db.run_daily_analysis_with_database()
```

## 配置说明

### indicators.yaml
```yaml
indicators:
  - id: VIXCLS
    name: "VIX 波动率"
    group: fin_cond_vol
    transform: level
    higher_is_risk: true
    compare_to: noncrisis_p90
    weight: 0.08
    freq: D
    role: score
```

### crisis_periods.yaml
```yaml
crisis_periods:
  - name: "2008金融危机"
    start: "2007-12-01"
    end: "2009-06-01"
  - name: "2020疫情危机"
    start: "2020-02-01"
    end: "2020-04-01"
```

## 输出报告

### 1. Markdown报告
- 包含所有指标的分析结果
- 风险评分和等级
- 图表嵌入

### 2. HTML报告
- 交互式网页格式
- 响应式设计
- 图表可视化

### 3. JSON报告
- 结构化数据
- 便于程序处理
- 包含原始数据

### 4. 长图报告
- 使用Playwright生成
- 适合打印和分享
- 包含完整分析内容

## 开发指南

### 添加新指标
1. 在`config/indicators.yaml`中添加指标配置
2. 在`core/database_integration.py`中添加因子映射
3. 更新权重配置
4. 测试新指标

### 修改评分逻辑
1. 编辑`core/database_integration.py`中的`_calculate_risk_score`方法
2. 调整阈值和权重
3. 验证评分结果

### 自定义报告
1. 修改`core/report.py`中的报告生成逻辑
2. 添加新的报告格式
3. 自定义图表样式

## 故障排除

### 常见问题

1. **数据获取失败**
   - 检查FRED_API_KEY配置
   - 验证网络连接
   - 查看API限制

2. **评分异常**
   - 检查数据完整性
   - 验证指标配置
   - 查看日志输出

3. **图表生成失败**
   - 检查matplotlib配置
   - 验证中文字体
   - 查看Playwright安装

### 日志查看
```bash
# 查看系统日志
tail -f logs/crisis_monitor.log

# 查看错误日志
grep ERROR logs/crisis_monitor.log
```

## 性能优化

### 1. 数据缓存
- 使用本地CSV文件缓存
- 增量更新机制
- 智能数据验证

### 2. 并行处理
- 多线程数据获取
- 异步图表生成
- 批量数据处理

### 3. 内存管理
- 数据分块处理
- 及时释放内存
- 优化数据结构

## 更新日志

### v2.0.0 (2025-10-12)
- 优化打分系统
- 移除重复因子
- 调整阈值设置
- 完善权重配置
- 新增多个指标

### v1.0.0 (2025-09-10)
- 初始版本发布
- 基础功能实现
- GUI界面完成

## 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

## 许可证

MIT License

## 联系方式

- 项目维护者: MacroLab Team
- 邮箱: macrolab@example.com
- 项目地址: https://github.com/macrolab/fred_crisis_monitor

## 参考文献

1. Federal Reserve Economic Data (FRED) API Documentation
2. 宏观经济风险监测方法研究
3. 金融危机预警指标体系构建
4. 数据可视化最佳实践

---

**注意**: 本系统仅供研究和教育目的，不构成投资建议。使用者应自行承担使用风险。