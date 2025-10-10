# FRED 金融危机预警监控系统

## 项目概述

本项目是一个基于FRED（Federal Reserve Economic Data）数据的美国金融危机预警检测框架。通过监控26个关键宏观经济指标，将当前值与历史危机期间基准值比较，生成风险评分和可视化报告。

## 开发时间线

**2025年9月14日-21日** - 完整开发周期
- 数据下载与同步
- 危机预警框架设计
- 数据处理与清洗
- 风险评分算法
- 可视化报告生成
- HTML/PNG长图输出

## 核心功能

### 1. 数据获取与同步
- **FRED API集成**: 自动获取宏观经济数据
- **本地数据缓存**: 优先使用本地数据，减少API调用
- **断点续传**: 支持网络中断后的数据恢复
- **数据清洗**: 处理缺失值、异常值和格式问题

### 2. 危机预警算法
- **26个关键指标**: 涵盖利率、信用利差、实体经济、房地产、消费、银行业等
- **历史基准对比**: 使用危机期和非危机期分位数作为基准
- **分组加权评分**: 按指标重要性分组，计算综合风险评分
- **实时监控**: 自动更新最新数据并重新计算风险等级

### 3. 可视化报告
- **Markdown报告**: 包含详细指标分析和图表
- **HTML自包含报告**: 嵌入Base64图片，适合移动端查看
- **PNG长图**: HTML转图片，便于分享和存档
- **中文支持**: 完整的中文字体和编码支持

## 目录结构

```
fred_crisis_monitor/
├── crisis_monitor.py              # 主程序 - 危机监控系统
├── config/
│   ├── crisis_indicators.yaml     # 指标配置文件
│   └── crisis_periods.yaml        # 历史危机期间定义
├── scripts/
│   ├── fred_http.py               # FRED API客户端
│   ├── clean_utils.py             # 数据清洗工具
│   ├── sync_fred_data.py          # 单序列数据同步
│   ├── sync_fred_http.py          # HTTP数据同步器
│   ├── html2longpng.py            # HTML转PNG工具
│   ├── plot_one.py                # 单指标图表测试
│   └── viz.py                     # 可视化模块（已合并到主程序）
├── data/
│   └── fred/
│       ├── series/                # 按序列ID存储的原始数据
│       └── categories/             # 按分类存储的数据
├── outputs/
│   └── crisis_monitor/
│       ├── figures/               # 指标图表PNG文件
│       ├── *.md                   # Markdown报告
│       ├── *.html                 # HTML自包含报告
│       ├── *.png                  # HTML转PNG长图
│       ├── *.json                 # JSON数据文件
│       ├── latest.md              # 最新Markdown报告
│       └── latest.html            # 最新HTML报告
├── factors/                       # 因子计算模块
├── examples/                      # 使用示例
├── tests/                         # 测试文件
└── templates/                     # 报告模板
```

## 核心文件详解

### 1. crisis_monitor.py (主程序)
**功能**: 危机监控系统核心程序
**主要特性**:
- 加载26个FRED指标配置
- 计算历史危机基准值
- 生成风险评分和等级
- 创建可视化图表
- 输出多种格式报告
- 支持中文显示和移动端优化

**关键函数**:
- `calculate_real_fred_scores()`: 计算真实FRED数据评分
- `process_single_indicator_real()`: 处理单个指标
- `calculate_crisis_stats()`: 计算危机期统计
- `generate_indicator_chart()`: 生成指标图表
- `render_html_report()`: 渲染HTML报告
- `generate_long_image()`: 生成长图

### 2. scripts/fred_http.py
**功能**: FRED API客户端
**主要特性**:
- HTTP请求封装
- 重试机制和错误处理
- 速率限制
- 数据格式转换

**关键函数**:
- `series_observations()`: 获取序列观测数据
- `series_info()`: 获取序列信息
- `series_search()`: 搜索替代序列

### 3. scripts/clean_utils.py
**功能**: 数据清洗工具
**主要特性**:
- 数值解析和转换
- 异常值处理
- 频率标准化

**关键函数**:
- `parse_numeric_series()`: 解析数值序列
- `clean_value()`: 清洗单个数值

### 4. config/crisis_indicators.yaml
**功能**: 指标配置文件
**内容**:
- 26个关键指标定义
- 分组和权重设置
- 基准分位配置
- 变换方法定义

**指标分组**:
- `rates_curve`: 收益率曲线 (15%)
- `rates_level`: 利率水平 (15%)
- `credit_spreads`: 信用利差 (15%)
- `fin_cond_vol`: 金融状况/波动 (10%)
- `real_economy`: 实体经济 (15%)
- `housing`: 房地产 (10%)
- `consumers`: 消费 (8%)
- `banking`: 银行业 (7%)
- `external`: 外部环境 (5%)

## 技术栈

### 核心库
- **pandas**: 数据处理和分析
- **numpy**: 数值计算
- **matplotlib**: 图表生成
- **yaml**: 配置文件解析
- **requests**: HTTP请求
- **tenacity**: 重试机制

### 渲染库
- **markdown**: Markdown转HTML
- **mistune**: 备选Markdown渲染器
- **wkhtmltoimage**: HTML转PNG

### 工具库
- **pathlib**: 路径处理
- **dotenv**: 环境变量
- **base64**: 图片编码
- **subprocess**: 外部命令调用

## 使用方法

### 1. 环境设置
```bash
# 安装依赖
pip install pandas numpy matplotlib pyyaml requests tenacity markdown

# 设置FRED API密钥
export FRED_API_KEY="your_api_key_here"
```

### 2. 运行监控系统
```bash
python crisis_monitor.py
```

### 3. 输出文件
- `latest.html`: 最新HTML报告（推荐移动端查看）
- `latest.md`: 最新Markdown报告
- `figures/`: 所有指标图表
- `crisis_report_long_*.png`: HTML转PNG长图

## 风险评分系统

### 评分范围
- **0-39**: 🔵 极低风险
- **40-59**: 🟢 低风险  
- **60-79**: 🟡 中风险
- **80-100**: 🔴 高风险

### 基准分位类型
- `crisis_median`: 危机期中位数
- `crisis_p25`: 危机期25%分位数
- `noncrisis_p75`: 非危机期75%分位数
- `noncrisis_p90`: 非危机期90%分位数

### 历史危机期间
- 2008年金融危机
- 欧债危机
- 缩减恐慌
- 中国股市崩盘
- 英国脱欧
- 贸易战
- 疫情初期
- 通胀飙升

## 数据来源

所有数据来源于FRED (Federal Reserve Economic Data):
- **官网**: https://fred.stlouisfed.org/
- **API文档**: https://fred.stlouisfed.org/docs/api/
- **数据更新**: 实时同步最新数据

## 注意事项

### 数据质量
- 部分指标可能存在数据延迟
- 季频数据更新较慢
- 过期数据会被标记并降权

### 免责声明
- 本系统仅供参考，不构成投资建议
- 历史数据不保证未来表现
- 请结合其他信息源进行决策

## 开发团队

**主要开发者**: AI Assistant (Claude)
**项目时间**: 2025年9月14日-21日
**联系方式**: jiangx@gmail.com

## 更新日志

### v1.0 (2025-09-21)
- ✅ 完成26个指标的数据获取和清洗
- ✅ 实现危机预警算法和评分系统
- ✅ 生成多格式可视化报告
- ✅ 支持中文显示和移动端优化
- ✅ 实现HTML转PNG长图功能
- ✅ 优化Markdown表格渲染

### 待优化项目
- [ ] 添加更多国际指标
- [ ] 实现实时数据推送
- [ ] 增加机器学习预测模型
- [ ] 支持多国数据对比

---

**最后更新**: 2025年9月21日
**版本**: v1.0
**状态**: 生产就绪