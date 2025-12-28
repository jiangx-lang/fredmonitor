#!/usr/bin/env python3
"""
v3.0: 市场数据同步脚本 (Market Data Sync) - 增量更新版本

智能增量下载关键资产的日度 OHLCV 数据。
只下载缺失的数据，避免重复下载。
"""

import os
import sys
import pathlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import yfinance as yf
except ImportError:
    print("❌ 错误: 未安装 yfinance。请运行: pip install yfinance")
    sys.exit(1)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 配置：共享数据源（硬编码绝对路径，与 calc_market_signals.py 保持一致）
MARKET_DATA_DIR = pathlib.Path(r"D:\标普\data_clean")
MARKET_DATA_DIR.mkdir(parents=True, exist_ok=True)

# 关键资产列表
# 宏观监控核心资产：SPY(美股基准), GLD(黄金), TLT(长债), HYG(高收益债), UUP(美元), ^VIX(恐慌指数)
# 扩展资产：QQQ(纳斯达克), UPRO(3倍做多), TQQQ(3倍做多纳指), SQQQ(3倍做空纳指), BIL(短期国债)
MARKET_TICKERS = [
    # 宏观监控核心资产
    'SPY', 'GLD', 'TLT', 'HYG', 'UUP', '^VIX',
    # 扩展资产
    'QQQ', 'UPRO', 'TQQQ', 'SQQQ', 'BIL'
]


def get_last_date_from_file(ticker: str) -> Optional[datetime]:
    """
    从本地文件获取最后一条数据的日期
    
    Args:
        ticker: 股票代码
    
    Returns:
        最后一条数据的日期，如果文件不存在则返回None
    """
    safe_ticker = ticker.replace('^', '').replace('/', '_')
    file_path = MARKET_DATA_DIR / f"{safe_ticker}.csv"
    
    if not file_path.exists():
        return None
    
    try:
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        if df.empty:
            return None
        # 转换为naive datetime（移除时区信息）
        last_date = df.index[-1]
        if hasattr(last_date, 'to_pydatetime'):
            last_date = last_date.to_pydatetime()
        if last_date.tzinfo is not None:
            last_date = last_date.replace(tzinfo=None)
        return last_date
    except Exception as e:
        logger.warning(f"⚠️ {ticker}: 读取本地文件失败: {e}")
        return None


def download_ticker_data_incremental(ticker: str) -> Optional[pd.DataFrame]:
    """
    增量下载单个 ticker 的数据
    
    Args:
        ticker: 股票代码
    
    Returns:
        DataFrame with OHLCV data, or None if failed
    """
    try:
        last_date = get_last_date_from_file(ticker)
        
        if last_date is None:
            # 首次下载：下载过去10年数据
            logger.info(f"📥 {ticker}: 首次下载，获取过去10年数据...")
            ticker_obj = yf.Ticker(ticker)
            df = ticker_obj.history(period="10y")
        else:
            # 增量下载：从最后日期+1天开始
            start_date = last_date + timedelta(days=1)
            end_date = datetime.now()
            
            # 如果最后日期已经很新（3天内），跳过下载
            days_diff = (end_date - last_date).days
            if days_diff <= 3:
                logger.info(f"⏭️ {ticker}: 数据已是最新（最后日期: {last_date.date()}，距今天数: {days_diff}），跳过下载")
                return None
            
            logger.info(f"📥 {ticker}: 增量下载（从 {start_date.date()} 至 {end_date.date()}，缺失 {days_diff} 天）...")
            ticker_obj = yf.Ticker(ticker)
            # 使用字符串格式的日期，避免时区问题
            df = ticker_obj.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
        
        if df.empty:
            logger.warning(f"⚠️ {ticker}: 未获取到新数据")
            return None
        
        # 确保索引是日期类型
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # 重命名列（确保一致性）
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        logger.info(f"✅ {ticker}: 成功下载 {len(df)} 条新记录")
        return df
        
    except Exception as e:
        logger.error(f"❌ {ticker}: 下载失败 - {e}")
        return None


def merge_and_save_ticker_data(ticker: str, new_df: pd.DataFrame) -> bool:
    """
    合并新数据到现有文件并保存
    
    Args:
        ticker: 股票代码
        new_df: 新下载的数据 DataFrame
    
    Returns:
        True if successful, False otherwise
    """
    try:
        safe_ticker = ticker.replace('^', '').replace('/', '_')
        file_path = MARKET_DATA_DIR / f"{safe_ticker}.csv"
        
        if file_path.exists():
            # 读取现有数据
            existing_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            
            # 合并数据（去重，保留最新）
            combined_df = pd.concat([existing_df, new_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()
        else:
            # 首次保存
            combined_df = new_df
        
        # 保存为 CSV
        combined_df.to_csv(file_path, encoding='utf-8')
        logger.info(f"💾 {ticker}: 已保存到 {file_path} (总计 {len(combined_df)} 条记录)")
        return True
        
    except Exception as e:
        logger.error(f"❌ {ticker}: 保存失败 - {e}")
        return False


def download_all_market_data() -> Dict[str, bool]:
    """
    增量下载所有关键资产的数据
    
    Returns:
        字典，key为ticker，value为是否成功
    """
    results = {}
    
    logger.info("=" * 60)
    logger.info("🚀 开始增量下载市场数据 (v3.0)")
    logger.info("=" * 60)
    
    for ticker in MARKET_TICKERS:
        new_df = download_ticker_data_incremental(ticker)
        if new_df is not None:
            success = merge_and_save_ticker_data(ticker, new_df)
            results[ticker] = success
        else:
            # 即使没有新数据，也认为成功（数据已是最新）
            results[ticker] = True
    
    # 统计
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info("=" * 60)
    logger.info(f"✅ 下载完成: {successful}/{total} 个资产成功")
    logger.info("=" * 60)
    
    return results


def main():
    """主函数"""
    results = download_all_market_data()
    
    logger.info("=" * 60)
    logger.info("🎉 市场数据增量同步完成 (v3.0)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
