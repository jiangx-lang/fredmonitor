#!/usr/bin/env python3
"""
v3.0: 市场信号计算脚本 (Market Signals Calculator) - L2计算层

独立计算层：从共享数据源读取原始市场数据，计算合成指标，保存为与FRED兼容的格式。
"""

import os
import sys
import pathlib
import logging
import time
import functools
from typing import Dict

# 强制设置标准输出为utf-8，解决Windows控制台乱码（必须在所有print之前）
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 性能诊断：启动警告（必须在最前面，检测import卡顿）
print("🚀 启动计算层... (如果此处卡顿，说明可能有隐形联网操作)")

# 性能诊断：记录pandas和numpy导入耗时
import_start = time.time()
import pandas as pd
import numpy as np
import_end = time.time()
print(f"⏱️ 导入基础库 (pandas/numpy) 耗时: {import_end - import_start:.4f} 秒")

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import_start = time.time()
    import pandas_ta as ta
    import_end = time.time()
    print(f"⏱️ 导入 pandas_ta 耗时: {import_end - import_start:.4f} 秒")
except ImportError:
    print("❌ 错误: 未安装 pandas_ta。请运行: pip install pandas-ta")
    sys.exit(1)

# 性能诊断：计时器装饰器
def time_execution(func):
    """计时器装饰器，用于监控函数执行耗时"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        logger.info(f"⏱️ [{func.__name__}] 耗时: {elapsed:.4f} 秒")
        return result
    return wrapper

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 配置：共享数据源（硬编码绝对路径）
MARKET_DATA_DIR = pathlib.Path(r"D:\标普\data_clean")

# 输出目录：当前项目的 data/series 目录
BASE_DIR = pathlib.Path(__file__).parent.parent
SERIES_DATA_DIR = BASE_DIR / "data" / "series"
SERIES_DATA_DIR.mkdir(parents=True, exist_ok=True)


@time_execution
def read_market_csv(ticker: str) -> pd.DataFrame:
    """
    从共享数据源读取市场数据CSV文件
    
    Args:
        ticker: 股票代码（如 'SPY', 'GLD'）
    
    Returns:
        DataFrame with OHLCV data, or empty DataFrame if file not found
    """
    csv_path = MARKET_DATA_DIR / f"{ticker}.csv"
    
    if not csv_path.exists():
        logger.warning(f"⚠️ {ticker}.csv 不存在于 {MARKET_DATA_DIR}")
        return pd.DataFrame()
    
    try:
        read_start = time.time()
        # 先尝试正常读取
        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        except Exception as parse_error:
            # 如果解析失败（可能是时区问题），先读取为字符串再转换
            logger.warning(f"⚠️ {ticker}: 标准解析失败，尝试手动处理时区: {parse_error}")
            df = pd.read_csv(csv_path, index_col=0)
            # 手动转换日期，忽略时区信息
            df.index = pd.to_datetime(df.index, errors='coerce', utc=False)
            df = df.dropna(subset=[df.index.name if df.index.name else df.columns[0]])
        
        read_end = time.time()
        logger.info(f"📄 读取 {ticker}: {len(df)} 行 (耗时: {read_end - read_start:.4f} 秒)")
        
        # 确保索引是日期类型
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors='coerce', utc=False)
            # 删除无法解析的日期行（使用索引的notna()方法）
            df = df[df.index.notna()]
        
        # ✅ 新增：防御性时区处理（解决时区不一致问题）
        # 1. 如果索引有时区信息，先转换为UTC再去除时区
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_convert('UTC').tz_localize(None)
        
        # 2. 标准化日期：去除具体时间，只留日期
        df.index = pd.to_datetime(df.index).normalize()
        
        # 3. 排序：按索引排序
        df = df.sort_index()
        
        return df
    except Exception as e:
        logger.error(f"❌ 读取 {ticker}.csv 失败: {e}")
        return pd.DataFrame()


@time_execution
def calculate_spy_trend_status() -> bool:
    """
    计算 SPY 趋势信号：MKT_SPY_TREND_STATUS
    SPY 价格 > MA200 (1/0)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        spy_df = read_market_csv('SPY')
        if spy_df.empty:
            logger.warning("⚠️ SPY 数据为空，跳过趋势信号计算")
            return False
        
        # 确定收盘价列名（可能是 'Close', 'close', 'Adj Close' 等）
        close_col = None
        for col in ['close', 'Close', 'Adj Close', 'adj_close']:
            if col in spy_df.columns:
                close_col = col
                break
        
        if close_col is None:
            logger.warning("⚠️ SPY 数据缺少收盘价列")
            return False
        
        # 计算200日均线
        calc_start = time.time()
        spy_df['sma200'] = ta.sma(spy_df[close_col], length=200)
        calc_end = time.time()
        logger.info(f"  📊 计算SMA200耗时: {calc_end - calc_start:.4f} 秒")
        
        # 计算趋势状态：1 = 价格 > MA200 (牛市), 0 = 价格 < MA200 (熊市)
        spy_df['trend_status'] = (spy_df[close_col] > spy_df['sma200']).astype(int)
        
        # 保存为FRED兼容格式 (date, value)
        output_df = pd.DataFrame({
            'date': spy_df.index.strftime('%Y-%m-%d'),
            'value': spy_df['trend_status']
        })
        
        save_start = time.time()
        output_file = SERIES_DATA_DIR / "MKT_SPY_TREND_STATUS.csv"
        output_df.to_csv(output_file, index=False, encoding='utf-8')
        save_end = time.time()
        logger.info(f"  💾 保存文件耗时: {save_end - save_start:.4f} 秒")
        logger.info(f"✅ MKT_SPY_TREND_STATUS: 已保存到 {output_file} (最新值: {spy_df['trend_status'].iloc[-1]})")
        return True
        
    except Exception as e:
        logger.error(f"❌ 计算 MKT_SPY_TREND_STATUS 失败: {e}", exc_info=True)
        return False


@time_execution
def calculate_credit_appetite() -> bool:
    """
    计算信用恐慌信号：MKT_CREDIT_APPETITE
    HYG / TLT 的比率
    
    Returns:
        True if successful, False otherwise
    """
    try:
        hyg_df = read_market_csv('HYG')
        tlt_df = read_market_csv('TLT')
        
        if hyg_df.empty or tlt_df.empty:
            logger.warning("⚠️ HYG 或 TLT 数据不存在，跳过信用恐慌信号计算")
            return False
        
        # 确定收盘价列名
        def get_close_col(df):
            for col in ['close', 'Close', 'Adj Close', 'adj_close']:
                if col in df.columns:
                    return col
            return None
        
        hyg_close_col = get_close_col(hyg_df)
        tlt_close_col = get_close_col(tlt_df)
        
        if hyg_close_col is None or tlt_close_col is None:
            logger.warning("⚠️ HYG 或 TLT 数据缺少收盘价列")
            return False
        
        # 对齐日期索引（Inner Join）
        common_dates = hyg_df.index.intersection(tlt_df.index)
        if len(common_dates) == 0:
            logger.warning("⚠️ HYG 和 TLT 没有共同的日期")
            return False
        
        hyg_aligned = hyg_df.loc[common_dates, hyg_close_col]
        tlt_aligned = tlt_df.loc[common_dates, tlt_close_col]
        
        # 计算比率
        calc_start = time.time()
        credit_ratio = hyg_aligned / tlt_aligned
        calc_end = time.time()
        logger.info(f"  📊 计算比率耗时: {calc_end - calc_start:.4f} 秒")
        
        # 保存为FRED兼容格式 (date, value)
        output_df = pd.DataFrame({
            'date': credit_ratio.index.strftime('%Y-%m-%d'),
            'value': credit_ratio.values
        })
        
        save_start = time.time()
        output_file = SERIES_DATA_DIR / "MKT_CREDIT_APPETITE.csv"
        output_df.to_csv(output_file, index=False, encoding='utf-8')
        save_end = time.time()
        logger.info(f"  💾 保存文件耗时: {save_end - save_start:.4f} 秒")
        logger.info(f"✅ MKT_CREDIT_APPETITE: 已保存到 {output_file} (最新值: {credit_ratio.iloc[-1]:.4f})")
        return True
        
    except Exception as e:
        logger.error(f"❌ 计算 MKT_CREDIT_APPETITE 失败: {e}", exc_info=True)
        return False


@time_execution
def calculate_spy_realized_vol() -> bool:
    """
    计算 SPY 波动率信号：MKT_SPY_REALIZED_VOL
    SPY 20日滚动波动率（年化）
    
    Returns:
        True if successful, False otherwise
    """
    try:
        spy_df = read_market_csv('SPY')
        if spy_df.empty:
            logger.warning("⚠️ SPY 数据为空，跳过波动率信号计算")
            return False
        
        # 确定收盘价列名
        close_col = None
        for col in ['close', 'Close', 'Adj Close', 'adj_close']:
            if col in spy_df.columns:
                close_col = col
                break
        
        if close_col is None:
            logger.warning("⚠️ SPY 数据缺少收盘价列")
            return False
        
        # 计算对数收益率
        calc_start = time.time()
        spy_df['log_returns'] = np.log(spy_df[close_col] / spy_df[close_col].shift(1))
        
        # 计算20日滚动波动率（年化，百分比）
        spy_df['realized_vol_20d'] = spy_df['log_returns'].rolling(window=20).std() * np.sqrt(252) * 100
        calc_end = time.time()
        logger.info(f"  📊 计算波动率耗时: {calc_end - calc_start:.4f} 秒")
        
        # 保存为FRED兼容格式 (date, value)
        output_df = pd.DataFrame({
            'date': spy_df.index.strftime('%Y-%m-%d'),
            'value': spy_df['realized_vol_20d']
        }).dropna()
        
        save_start = time.time()
        output_file = SERIES_DATA_DIR / "MKT_SPY_REALIZED_VOL.csv"
        output_df.to_csv(output_file, index=False, encoding='utf-8')
        save_end = time.time()
        logger.info(f"  💾 保存文件耗时: {save_end - save_start:.4f} 秒")
        logger.info(f"✅ MKT_SPY_REALIZED_VOL: 已保存到 {output_file} (最新值: {spy_df['realized_vol_20d'].iloc[-1]:.2f}%)")
        return True
        
    except Exception as e:
        logger.error(f"❌ 计算 MKT_SPY_REALIZED_VOL 失败: {e}", exc_info=True)
        return False


@time_execution
def calculate_gold_momentum() -> bool:
    """
    计算黄金动量信号：MKT_GOLD_MOMENTUM (可选)
    GLD Close / SMA50 - 1 (乖离率)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        gld_df = read_market_csv('GLD')
        if gld_df.empty:
            logger.warning("⚠️ GLD 数据不存在，跳过黄金动量信号计算")
            return False
        
        # 确定收盘价列名
        close_col = None
        for col in ['close', 'Close', 'Adj Close', 'adj_close']:
            if col in gld_df.columns:
                close_col = col
                break
        
        if close_col is None:
            logger.warning("⚠️ GLD 数据缺少收盘价列")
            return False
        
        # 计算50日均线
        calc_start = time.time()
        gld_df['sma50'] = ta.sma(gld_df[close_col], length=50)
        
        # 计算乖离率：Close / SMA50 - 1
        gld_df['momentum'] = (gld_df[close_col] / gld_df['sma50']) - 1
        calc_end = time.time()
        logger.info(f"  📊 计算动量耗时: {calc_end - calc_start:.4f} 秒")
        
        # 保存为FRED兼容格式 (date, value)
        output_df = pd.DataFrame({
            'date': gld_df.index.strftime('%Y-%m-%d'),
            'value': gld_df['momentum']
        }).dropna()
        
        save_start = time.time()
        output_file = SERIES_DATA_DIR / "MKT_GOLD_MOMENTUM.csv"
        output_df.to_csv(output_file, index=False, encoding='utf-8')
        save_end = time.time()
        logger.info(f"  💾 保存文件耗时: {save_end - save_start:.4f} 秒")
        logger.info(f"✅ MKT_GOLD_MOMENTUM: 已保存到 {output_file} (最新值: {gld_df['momentum'].iloc[-1]:.4f})")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ 计算 MKT_GOLD_MOMENTUM 失败（可选指标）: {e}")
        return False


@time_execution
def calculate_all_market_signals() -> Dict[str, bool]:
    """
    计算所有市场体制信号
    
    Returns:
        字典，key为指标ID，value为是否成功
    """
    results = {}
    
    logger.info("=" * 60)
    logger.info("🧮 开始计算市场体制信号 (v3.0 - L2计算层)")
    logger.info(f"📂 数据源: {MARKET_DATA_DIR}")
    logger.info(f"📂 输出目录: {SERIES_DATA_DIR}")
    logger.info("=" * 60)
    
    # 检查数据源目录是否存在
    if not MARKET_DATA_DIR.exists():
        logger.error(f"❌ 数据源目录不存在: {MARKET_DATA_DIR}")
        logger.error("   请确保共享数据源目录存在，或修改 MARKET_DATA_DIR 配置")
        return results
    
    # 计算核心指标
    results['MKT_SPY_TREND_STATUS'] = calculate_spy_trend_status()
    results['MKT_SPY_REALIZED_VOL'] = calculate_spy_realized_vol()
    results['MKT_CREDIT_APPETITE'] = calculate_credit_appetite()
    
    # 计算可选指标（如果数据存在）
    results['MKT_GOLD_MOMENTUM'] = calculate_gold_momentum()
    
    # 统计
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info("=" * 60)
    logger.info(f"✅ 计算完成: {successful}/{total} 个指标成功")
    logger.info("=" * 60)
    
    return results


def main():
    """主函数"""
    results = calculate_all_market_signals()
    
    logger.info("=" * 60)
    logger.info("🎉 市场信号计算完成 (v3.0 - L2计算层)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
