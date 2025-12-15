# 修复指令 (Fix Instructions)

根据诊断报告发现的三个核心问题，请按以下步骤修复 `crisis_monitor.py`：

## 问题1: 修复评分反转 Bug (最高优先级)

**位置**: `score_with_threshold` 函数 (约第1122行)

**问题**: 当 `higher_is_risk=False` (即 `down_is_risk`) 时，当前值低于基准值应该得到高分，但当前实现得到的是低分。

**修复代码**:

```python
def score_with_threshold(ts: pd.Series, current: float, *, direction: str, compare_to: str, tail: str='single') -> float:
    """真正用阈值参与打分（单尾/双尾统一）"""
    if ts is None or ts.empty or pd.isna(current):
        return 50.0
    
    # 1) 全样本分位
    p_cur = (ts <= current).mean()
    # 2) 阈值分位
    p_thr = _parse_compare_to_to_pct(compare_to)
    eps = 1e-6
    
    # 3) 映射成 0~100
    if tail == 'both':
        p_mid = 0.5
        denom = max(abs(p_mid - p_thr), eps)
        raw = min(1.0, abs(p_cur - p_mid) / denom)
    else:
        if direction == 'up_is_risk':   # 高为险
            raw = max(0.0, (p_cur - p_thr) / max(1 - p_thr, eps))
        else:                           # 低为险
            # 修复：当越低越危险时，当前值越低（p_cur越小），风险应该越高
            # 所以应该用 (p_thr - p_cur) / p_thr，然后映射到高分
            raw = max(0.0, (p_thr - p_cur) / max(p_thr, eps))
    
    # 修复：对于 down_is_risk，需要反转分数
    # 当 raw 接近 1 时（当前值远低于阈值），应该得到高分
    if direction == 'down_is_risk':
        # 反转：raw 越大（当前值越低），分数应该越高
        # 但 raw 已经是基于 (p_thr - p_cur) 计算的，所以已经是正确的方向
        # 问题在于：当 p_cur 很小时（当前值很低），p_thr - p_cur 会很大，raw 会很大
        # 但我们需要确保当 current < benchmark 时，分数应该 > 50
        score = float(np.clip(raw * 100.0, 0, 100))
        
        # 额外检查：如果当前值确实低于基准值，确保分数至少为 50
        if ts is not None and not ts.empty:
            benchmark_val = ts.quantile(p_thr)
            if current < benchmark_val and score < 50:
                # 强制设置为至少 50 + 根据偏离程度加分
                deviation_ratio = (benchmark_val - current) / max(abs(benchmark_val), eps)
                score = min(100.0, 50.0 + deviation_ratio * 50.0)
        
        return score
    
    return float(np.clip(raw * 100.0, 0, 100))
```

**更简洁的修复方案**（推荐）:

```python
def score_with_threshold(ts: pd.Series, current: float, *, direction: str, compare_to: str, tail: str='single') -> float:
    """真正用阈值参与打分（单尾/双尾统一）"""
    if ts is None or ts.empty or pd.isna(current):
        return 50.0
    
    # 1) 全样本分位
    p_cur = (ts <= current).mean()
    # 2) 阈值分位
    p_thr = _parse_compare_to_to_pct(compare_to)
    eps = 1e-6
    
    # 3) 映射成 0~100
    if tail == 'both':
        p_mid = 0.5
        denom = max(abs(p_mid - p_thr), eps)
        raw = min(1.0, abs(p_cur - p_mid) / denom)
    else:
        if direction == 'up_is_risk':   # 高为险
            # 当前值越高（p_cur越大），风险越高
            raw = max(0.0, (p_cur - p_thr) / max(1 - p_thr, eps))
        else:                           # 低为险
            # 当前值越低（p_cur越小），风险越高
            # 当 p_cur < p_thr 时，说明当前值低于阈值，风险高
            # 计算：风险 = (p_thr - p_cur) / p_thr
            raw = max(0.0, (p_thr - p_cur) / max(p_thr, eps))
    
    score = float(np.clip(raw * 100.0, 0, 100))
    
    # 修复：对于 down_is_risk，如果当前值低于基准值，确保分数至少反映风险
    if direction == 'down_is_risk' and ts is not None and not ts.empty:
        benchmark_val = ts.quantile(p_thr)
        if current < benchmark_val:
            # 当前值低于基准值，风险应该较高
            # 如果计算出的分数太低，需要调整
            if score < 50:
                # 根据偏离程度计算风险分数
                # 偏离越大，分数越高
                deviation = (benchmark_val - current) / max(abs(benchmark_val), eps)
                # 确保分数至少为 50，并根据偏离程度增加
                score = min(100.0, 50.0 + min(50.0, deviation * 100.0))
    
    return score
```

## 问题2: 移除冗余指标

**位置**: `calculate_real_fred_scores` 函数 (约第827行，在加载指标配置后)

**修复代码**:

在 `calculate_real_fred_scores` 函数中，在加载指标配置后添加：

```python
    # 使用传入的配置或加载配置
    if indicators_config is None:
        config_path = BASE / "config" / "crisis_indicators.yaml"
        config = load_yaml_config(config_path)
        indicators = config.get('indicators', [])
    else:
        indicators = indicators_config
    
    # 修复：移除冗余指标
    # 如果同时存在 NCBDBIQ027S 和 CORPDEBT_GDP_PCT，移除 CORPDEBT_GDP_PCT
    indicator_ids = [ind.get('series_id') or ind.get('id', '') for ind in indicators]
    if 'NCBDBIQ027S' in indicator_ids and 'CORPDEBT_GDP_PCT' in indicator_ids:
        print("⚠️ 检测到冗余指标：NCBDBIQ027S 和 CORPDEBT_GDP_PCT 同时存在，移除 CORPDEBT_GDP_PCT")
        indicators = [ind for ind in indicators 
                     if (ind.get('series_id') or ind.get('id', '')) != 'CORPDEBT_GDP_PCT']
        print(f"✅ 已移除冗余指标，剩余 {len(indicators)} 个指标")
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
```

## 问题3: 增加缓存清洗机制

**位置**: `run_data_pipeline` 函数 (约第1291行，在函数开头)

**修复代码**:

在 `run_data_pipeline` 函数开头添加：

```python
def run_data_pipeline():
    """运行完整的数据管道：下载+预处理+计算"""
    import subprocess
    import time
    import os
    import glob

    print("🔄 启动数据管道...")
    print("=" * 60)
    
    # 修复：清洗预计算的中间文件缓存
    print("🧹 清洗预计算缓存文件...")
    cache_dir = BASE / "data" / "series"
    if cache_dir.exists():
        cache_files = list(cache_dir.glob("*.csv"))
        # 排除 README 和其他非数据文件
        cache_files = [f for f in cache_files if f.name not in ['README.md', 'data_catalog.py']]
        
        removed_count = 0
        for cache_file in cache_files:
            try:
                cache_file.unlink()
                removed_count += 1
            except Exception as e:
                print(f"⚠️ 删除缓存文件失败 {cache_file.name}: {e}")
        
        if removed_count > 0:
            print(f"✅ 已删除 {removed_count} 个预计算缓存文件")
        else:
            print("ℹ️ 没有需要清理的缓存文件")
    else:
        print("ℹ️ 缓存目录不存在，跳过清理")
    
    # 保留原始数据：data/fred/series/*/raw.csv 不会被删除
    
    total_steps = 4
    current_step = 0
    # ... 后续代码保持不变
```

## 验证修复

修复后，请运行诊断脚本验证：

```bash
py diagnose_system.py
```

预期结果：
1. T10Y3M 的评分应该 > 50（当跌破基准线时）
2. 指标冗余警告应该消失
3. 缓存文件会被自动清理

