# MacroLab 使用示例

本目录包含MacroLab的使用示例和教程。

## 文件说明

- `example_usage.py`: 基本使用示例
- `README.md`: 本说明文件

## 快速开始

### 1. 环境设置

首先确保已安装所有依赖：

```bash
pip install -e .
```

### 2. 配置API密钥

复制环境配置文件：

```bash
copy env.example .env
```

编辑 `.env` 文件，填入您的FRED API密钥：

```
FRED_API_KEY=your_fred_api_key_here
```

### 3. 运行示例

```bash
python examples/example_usage.py
```

## 示例内容

### 基本使用

```python
from macro import MacroLabCLI

# 创建CLI实例
cli = MacroLabCLI()

# 运行每日分析
cli.run_daily()

# 列出所有因子
cli.list_factors()

# 生成解读报告
cli.explain("2024-01-01", use_ai=True)
```

### 自定义因子

```python
from factors.base_factor import Factor

class MyFactor(Factor):
    id = "MY_FACTOR"
    name = "我的因子"
    
    def fetch(self):
        # 实现数据获取逻辑
        pass
    
    def compute(self, df):
        # 实现指标计算逻辑
        pass
    
    def score(self, metrics, global_cfg):
        # 实现评分逻辑
        pass
```

### 批量处理

```python
# 历史数据回填
cli.backfill("2020-01-01", "2024-12-31")

# 或使用脚本
python scripts/backfill.py 2020-01-01 2024-12-31
```

## 注意事项

1. **API密钥**: 需要有效的FRED API密钥才能获取真实数据
2. **数据目录**: 确保有足够的磁盘空间存储数据
3. **网络连接**: 需要稳定的网络连接访问FRED API
4. **权限**: 确保对输出目录有写入权限

## 故障排除

### 常见问题

1. **ImportError**: 确保在项目根目录运行示例
2. **API错误**: 检查FRED API密钥是否正确
3. **文件权限**: 确保对数据目录有写入权限
4. **内存不足**: 对于大量历史数据，可能需要更多内存

### 调试模式

设置环境变量启用调试模式：

```bash
set MACROLAB_DEBUG=1
python examples/example_usage.py
```

这将输出详细的调试信息。

## 更多示例

查看 `examples/` 目录中的其他示例文件，了解更高级的用法。
