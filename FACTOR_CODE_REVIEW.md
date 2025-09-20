# MacroLab 因子代码审查

## 代码结构概览

MacroLab采用模块化的因子设计，每个因子都是独立的Python模块，继承自`base_factor.Factor`基类。

## 核心组件

### 1. 基础因子类 (`factors/base_factor.py`)

```python
class Factor(ABC):
    """宏观因子基类"""
    
    id: str = ""           # 因子ID（如 "vix"）
    name: str = ""         # 因子名称（如 "VIX 波动率"）
    units: Optional[str] = None  # 单位

    def __init__(self, cfg: Dict[str, Any]):
        """初始化因子"""
        self.cfg = cfg or {}

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """获取原始数据"""
        pass

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算派生指标"""
        pass

    @abstractmethod
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算风险评分"""
        pass

    def to_frame(self, date: pd.Timestamp, metrics: Dict[str, Any], score: float) -> pd.DataFrame:
        """将因子结果转换为标准DataFrame格式"""
        pass
```

**设计优点：**
- 清晰的抽象接口，强制所有因子实现必要方法
- 统一的配置管理
- 标准化的数据输出格式
- 易于扩展和维护

### 2. 简单因子实现 (`factors/vix.py`)

```python
class VIX(Factor):
    """VIX波动率因子"""
    
    id = "VIX"
    name = "VIX 波动率"
    units = None
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "VIXCLS")
    
    def fetch(self) -> pd.DataFrame:
        """获取VIX数据"""
        # 返回空DataFrame，实际数据获取由聚合器完成
        return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算VIX指标"""
        if df.empty:
            return {"original_value": None}
        
        latest_value = df['value'].iloc[-1] if not df['value'].empty else None
        return {
            "original_value": float(latest_value) if latest_value is not None else None
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算VIX风险评分"""
        from core.scoring import calculate_factor_score
        return calculate_factor_score(self.id, metrics.get("original_value"), global_cfg)
```

**设计优点：**
- 简洁明了，易于理解
- 遵循单一职责原则
- 良好的错误处理
- 配置参数化

### 3. 复杂因子实现 (`factors/vix_complete.py`)

```python
class VIXComplete(Factor):
    """VIX波动率因子 - 完整实现"""
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.series_id = cfg.get("series_id", "VIXCLS")
        self.moving_avg_days = cfg.get("moving_avg_days", 5)
        self.volatility_window = cfg.get("volatility_window", 20)
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算VIX相关指标"""
        # 1. 原始值
        original_value = float(values.iloc[-1])
        
        # 2. 移动平均值
        moving_avg = float(values.tail(self.moving_avg_days).mean())
        
        # 3. 历史波动率
        volatility = float(values.tail(self.volatility_window).std())
        
        # 4. 百分位排名
        percentile_rank = float((values < original_value).sum() / len(values) * 100)
        
        # 5. 趋势分析
        trend = "上升" if recent_3.iloc[-1] > recent_3.iloc[0] else "下降"
        
        # 6. 异常值检测
        is_outlier = abs(original_value - mean_val) > 2 * std_val
        
        return {
            "original_value": original_value,
            "moving_avg": moving_avg,
            "volatility": volatility,
            "percentile_rank": percentile_rank,
            "trend": trend,
            "is_outlier": is_outlier
        }
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """计算VIX风险评分"""
        score = calculate_factor_score(self.id, value, global_cfg)
        
        # 额外的风险调整
        if metrics.get("is_outlier", False):
            score = min(100, score + 10)
        
        if metrics.get("trend") == "上升":
            score = min(100, score + 5)
        
        return score
```

**设计优点：**
- 丰富的指标计算
- 智能的风险调整
- 完整的数据验证
- 详细的日志记录
- 可配置的参数

### 4. 多数据源因子 (`factors/ted_spread_complete.py`)

```python
class TEDSpreadComplete(Factor):
    """TED利差因子 - 完整实现"""
    
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.sofr_series = cfg.get("sofr_series", "SOFR")
        self.tbill_series = cfg.get("tbill_series", "DTB3")
        self.moving_avg_days = cfg.get("moving_avg_days", 20)
    
    def get_fred_series_ids(self) -> list:
        """获取需要的FRED序列ID列表"""
        return [self.sofr_series, self.tbill_series]
    
    def calculate_ted_spread(self, sofr_data: pd.Series, tbill_data: pd.Series) -> pd.DataFrame:
        """计算TED利差"""
        sofr_avg = sofr_data.rolling(window=self.moving_avg_days).mean()
        ted_spread = sofr_avg - tbill_data
        
        return pd.DataFrame({
            'date': ted_spread.index,
            'value': ted_spread.values
        }).dropna()
```

**设计优点：**
- 支持多数据源
- 复杂计算逻辑
- 数据质量验证
- 灵活的配置

## 代码质量评估

### ✅ 优点

1. **模块化设计**
   - 每个因子独立成模块
   - 清晰的接口定义
   - 易于测试和维护

2. **配置驱动**
   - 参数化配置
   - 支持运行时调整
   - 易于扩展

3. **错误处理**
   - 完善的异常处理
   - 数据验证机制
   - 友好的错误信息

4. **日志记录**
   - 详细的日志信息
   - 调试友好的输出
   - 性能监控

5. **数据质量**
   - 数据验证函数
   - 异常值检测
   - 数据范围检查

6. **可扩展性**
   - 抽象基类设计
   - 插件式架构
   - 易于添加新因子

### ⚠️ 需要改进的地方

1. **数据获取逻辑**
   - 当前`fetch()`方法返回空DataFrame
   - 实际数据获取在聚合器中完成
   - 可以考虑注入数据获取器

2. **类型注解**
   - 部分方法缺少完整的类型注解
   - 可以添加更严格的类型检查

3. **单元测试**
   - 需要为每个因子编写单元测试
   - 测试覆盖率有待提高

4. **文档字符串**
   - 部分方法缺少详细的文档字符串
   - 可以添加更多使用示例

## 使用建议

### 1. 创建新因子

```python
# 1. 复制模板文件
cp factors/_TEMPLATE.py factors/my_factor.py

# 2. 修改类名和属性
class MyFactor(Factor):
    id = "MY_FACTOR"
    name = "我的因子"
    units = "单位"

# 3. 实现必要方法
def fetch(self) -> pd.DataFrame:
    # 实现数据获取逻辑
    pass

def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
    # 实现指标计算逻辑
    pass

def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
    # 实现评分逻辑
    pass

# 4. 在配置文件中注册
# config/factor_registry.yaml
enabled:
  - my_factor

factors:
  my_factor:
    series_id: "MY_SERIES"
    param1: "value1"
```

### 2. 测试因子

```python
# 运行单个因子测试
python -c "
from factors.my_factor import MyFactor
factor = MyFactor({'series_id': 'MY_SERIES'})
print(f'因子ID: {factor.id}')
print(f'因子名称: {factor.name}')
"

# 运行完整测试套件
python -m pytest tests/test_factor_contract.py -v
```

### 3. 调试因子

```python
# 启用调试模式
import logging
logging.basicConfig(level=logging.DEBUG)

# 运行因子
factor = MyFactor({})
df = factor.fetch()
metrics = factor.compute(df)
score = factor.score(metrics, global_config)
```

## 总结

MacroLab的因子设计采用了良好的软件工程实践：

- **单一职责原则**：每个因子只负责一个特定的指标
- **开闭原则**：对扩展开放，对修改封闭
- **依赖倒置原则**：依赖抽象而不是具体实现
- **接口隔离原则**：清晰的接口定义

这种设计使得系统具有很好的可维护性、可扩展性和可测试性。每个因子都可以独立开发、测试和部署，同时保持与整个系统的良好集成。
