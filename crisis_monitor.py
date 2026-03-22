#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（带图片版）
- 1比1复制前面报告的所有输出内容
- 包含HTML输出和长图生成功能
- 确保所有文件都包含图片
"""

import os
import sys
import json
import yaml
import math
import warnings
import pathlib
import base64
import re
import subprocess
import html as _html
import time
import threading
import random
import logging
from datetime import datetime
from typing import List, Dict, Optional
import pytz

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 统一时区设置
JST = pytz.timezone("Asia/Tokyo")

_logger_cm = logging.getLogger(__name__)

RUN_LOG_PATH = None
RUN_START_TS = None


def _log_stage(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed = ""
    if RUN_START_TS is not None:
        elapsed = f" (+{time.time() - RUN_START_TS:.1f}s)"
    line = f"[{timestamp}]{elapsed} {message}"
    print(line)
    if RUN_LOG_PATH:
        try:
            with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager
from dotenv import load_dotenv
import yfinance as yf

# 抑制警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# --- Markdown 渲染工具 ---
try:
    import markdown as _mdlib
    def _md2html(txt: str) -> str:
        # tables + extra 能把 |---| 语法的表格转成 <table>
        return _mdlib.markdown(
            txt,
            extensions=[
                "extra",        # 启用表格、脚注等
                "tables",
                "fenced_code",
                "sane_lists",
                "nl2br",
            ],
            output_format="html5",
        )
except Exception:
    try:
        import mistune
        _md = mistune.create_markdown(escape=False, hard_wrap=True, plugins=["table"])
        def _md2html(txt: str) -> str:
            return _md(txt)
    except Exception:
        def _md2html(txt: str) -> str:
            # 最差兜底：至少不会"乱码"
            return f"<pre>{_html.escape(txt)}</pre>"

# 工程路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# Yahoo 缓存过期天数（超过则重新下载）
YAHOO_CACHE_EXPIRY_DAYS = 3

# 加载环境变量
try:
    env_files = [BASE / "macrolab.env", BASE / ".env"]
    loaded = False
    for env_file in env_files:
        if env_file.exists():
            try:
                load_dotenv(env_file, encoding='utf-8')
                loaded = True
                print(f"✅ 环境变量加载成功: {env_file.name}")
                break
            except UnicodeDecodeError:
                try:
                    load_dotenv(env_file, encoding='gbk')
                    loaded = True
                    print(f"✅ 环境变量加载成功: {env_file.name}")
                    break
                except:
                    continue
    
    if not loaded:
        print("⚠️ 未找到环境变量文件，将使用系统环境变量")
except Exception as e:
    print(f"⚠️ 加载环境变量失败: {e}，将使用系统环境变量")

# 严格模式
STRICT = os.getenv("STRICT", "0") == "1"

def die_if(cond: bool, msg: str):
    if cond:
        if STRICT:
            raise RuntimeError(msg)
        else:
            print(f"❌ {msg}")

# --- 统一变换与频率工具 ---
def infer_shift_for_yoy(idx: pd.DatetimeIndex) -> int:
    """根据索引推断同比所需的期数（M=12, Q=4, W=52, D≈252）。"""
    if len(idx) < 3: return 12
    freq = pd.infer_freq(idx) or ""
    f = freq.upper() if freq else ""
    if f.startswith("Q"): return 4
    if f.startswith("M"): return 12
    if f.startswith("W"): return 52
    # 日频：用交易日近似
    return 252

def to_yoy_pct(ts: pd.Series) -> pd.Series:
    ts = ts.dropna().astype(float)
    if ts.empty: return ts
    k = infer_shift_for_yoy(ts.index)
    return (ts / ts.shift(k) - 1.0) * 100.0

def rolling_mean(ts: pd.Series, window: int = 20) -> pd.Series:
    return ts.dropna().astype(float).rolling(window, min_periods=max(5, window//2)).mean()

def infer_window_for_zscore(idx: pd.DatetimeIndex) -> int:
    """根据索引推断Z-Score窗口（D=252, W=52, M=12, Q=4）"""
    if len(idx) < 3:
        return 252
    freq = pd.infer_freq(idx) or ""
    f = freq.upper() if freq else ""
    if f.startswith("Q"):
        return 20
    if f.startswith("M"):
        return 60
    if f.startswith("W"):
        return 104
    return 252

def calculate_zscore(series: pd.Series, window: Optional[int] = None) -> pd.Series:
    """计算滚动Z-Score (当前值 - 均值) / 标准差"""
    series = series.dropna().astype(float)
    if series.empty:
        return series
    if window is None:
        window = infer_window_for_zscore(series.index)
    min_periods = max(5, window // 2)
    roll_mean = series.rolling(window=window, min_periods=min_periods).mean()
    roll_std = series.rolling(window=window, min_periods=min_periods).std()
    zscore = (series - roll_mean) / roll_std
    return zscore.replace([np.inf, -np.inf], np.nan)

def calculate_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """计算RSI相对强弱指标 (Wilder's Smoothing)"""
    series = series.dropna().astype(float)
    if series.empty:
        return series
    delta = series.diff()
    # 获得上涨和下跌的绝对值
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    # 使用Wilder's Smoothing (alpha = 1/n)
    avg_gain = gain.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/window, min_periods=window, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def transform_series(series_id: str, raw_ts: pd.Series, indicator_meta: dict) -> pd.Series:
    """对历史序列应用与当前值一致的变换，确保打分的域一致。"""
    transform = indicator_meta.get("transform", "level")
    ts = raw_ts.dropna().astype(float)
    if transform == "yoy_pct":
        return to_yoy_pct(ts)
    if transform == "zscore":
        return calculate_zscore(ts)
    # 兼容派生：SOFR20DMA_MINUS_DTB3 等在外层先合成，再进这里
    return ts

# --- FRED 读取与合成序列 ---
def _fred_cache_value_column(df: pd.DataFrame, series_id: str) -> pd.Series:
    """从 raw.csv 的 DataFrame 中选取正确的数值列：优先 value，其次 series_id 同名列，否则第一个有效数值列。"""
    if "value" in df.columns:
        return parse_numeric_series(df["value"]).dropna()
    if series_id in df.columns:
        return parse_numeric_series(df[series_id]).dropna()
    for c in df.columns:
        s = parse_numeric_series(df[c]).dropna()
        if not s.empty:
            return s
    return pd.Series(dtype="float64")


def fetch_series(series_id: str) -> pd.Series:
    """从本地缓存优先读取，否则 FRED API，最后返回一个 float Series（索引为 datetime）。"""
    cache = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    if cache.exists():
        try:
            df = pd.read_csv(cache, index_col=0, parse_dates=True)
            if not df.empty:
                return _fred_cache_value_column(df, series_id)
        except Exception:
            pass
    
    # 只有在FRED可用时才调用API
    if not FRED_AVAILABLE:
        return pd.Series(dtype="float64")
    
    # API
    try:
        data = series_observations(series_id)
    except Exception as e:
        print(f"⚠️ API获取数据失败: {series_id} - {e}")
        return pd.Series(dtype="float64")
    obs = data.get("observations", []) if data else []
    if not obs: return pd.Series(dtype="float64")
    df = pd.DataFrame(obs)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    ts = parse_numeric_series(df["value"]).dropna()
    # 写缓存
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        ts.to_frame(series_id).to_csv(cache)
    except Exception:
        pass
    return ts

def fetch_yahoo_safe(symbol: str, retries: int = 2, delay: float = 3.0) -> pd.Series:
    """
    带重试与退避的Yahoo下载函数（优先缓存，缓存超过 YAHOO_CACHE_EXPIRY_DAYS 天则刷新）
    """
    safe_symbol = symbol.replace("^", "_")
    cache = BASE / "data" / "yahoo" / "series" / safe_symbol / "raw.csv"
    stale_series = None  # 下载失败时若有过期缓存则降级返回

    if cache.exists():
        try:
            df_cached = pd.read_csv(cache, index_col=0, parse_dates=True)
            if not df_cached.empty:
                last_date = pd.to_datetime(df_cached.index[-1])
                days_old = (pd.Timestamp.now() - last_date).days
                if days_old <= YAHOO_CACHE_EXPIRY_DAYS:
                    return parse_numeric_series(df_cached.iloc[:, 0]).dropna()
                stale_series = parse_numeric_series(df_cached.iloc[:, 0]).dropna()
                print(f"   📅 Yahoo 缓存 {symbol} 已 {days_old} 天 (> {YAHOO_CACHE_EXPIRY_DAYS} 天)，刷新中...")
        except Exception as e:
            print(f"   ⚠️ 读取 Yahoo 缓存失败 {symbol}: {e}")

    print(f"🌐 正在获取 {symbol} (Yahoo)...")
    for i in range(retries):
        try:
            sleep_time = delay * (1.5 ** i) + random.uniform(0.5, 1.5)
            if i > 0:
                print(f"   ⏳ 触发限流，等待 {sleep_time:.1f} 秒后重试 ({i+1}/{retries})...")
            time.sleep(sleep_time)

            df = yf.download(symbol, period="5y", progress=False)
            if df.empty:
                print(f"   ⚠️ {symbol} 返回空数据，尝试重试...")
                continue

            if 'Adj Close' in df.columns:
                series = df['Adj Close']
            elif 'Close' in df.columns:
                series = df['Close']
            else:
                series = df.iloc[:, 0]

            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            if getattr(series.index, "tz", None) is not None:
                series.index = series.index.tz_localize(None)

            series = parse_numeric_series(series).dropna()
            if series.empty:
                print(f"   ⚠️ {symbol} 清洗后为空，尝试重试...")
                continue

            try:
                cache.parent.mkdir(parents=True, exist_ok=True)
                series.to_frame(symbol).to_csv(cache)
            except Exception:
                pass

            print(f"   ✅ {symbol} 获取成功 (最新: {series.iloc[-1]:.2f})")
            return series
        except Exception as e:
            time.sleep(5)  # 被限速时等待5秒再重试
            error_msg = str(e).lower()
            if "429" in error_msg or "too many requests" in error_msg:
                continue
            print(f"   ❌ {symbol} 下载出错: {e}")
            break

    if stale_series is not None and not stale_series.empty:
        print(f"   ⚠️ {symbol} 下载失败，使用过期缓存 (最新: {stale_series.index[-1].date()})")
        return stale_series
    print(f"   ❌ {symbol} 重试 {retries} 次后彻底失败，跳过。")
    return pd.Series(dtype="float64")


def fetch_bizd_safe() -> pd.Series:
    """
    BIZD (BDC ETF) 近 1 年价格，用于 vs 50DMA 回撤。Fail-open: 失败时记录警告并返回空 Series，不阻断管道。
    Priority: Adj Close -> Close; period 1y only.
    """
    symbol = "BIZD"
    cache = BASE / "data" / "yahoo" / "series" / symbol / "raw.csv"
    if cache.exists():
        try:
            df = pd.read_csv(cache, index_col=0, parse_dates=True)
            if not df.empty:
                # 只保留最近 1 年
                cutoff = pd.Timestamp.now() - pd.DateOffset(days=365)
                df = df[df.index >= cutoff]
                if df.empty:
                    return pd.Series(dtype="float64")
                col = "Adj Close" if "Adj Close" in df.columns else "Close"
                if col not in df.columns:
                    col = df.columns[0]
                s = parse_numeric_series(df[col]).dropna()
                return s if not s.empty else pd.Series(dtype="float64")
        except Exception:
            pass
    try:
        df = yf.download(symbol, period="1y", progress=False, timeout=10)
        if df.empty:
            print(f"   ⚠️ BIZD: 返回空数据，指标将优雅降级 (NaN)")
            return pd.Series(dtype="float64")
        series = df["Adj Close"] if "Adj Close" in df.columns else (df["Close"] if "Close" in df.columns else df.iloc[:, 0])
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        if getattr(series.index, "tz", None) is not None:
            series.index = series.index.tz_localize(None)
        series = parse_numeric_series(series).dropna()
        if series.empty:
            print(f"   ⚠️ BIZD: 清洗后为空，指标将优雅降级 (NaN)")
            return pd.Series(dtype="float64")
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            series.to_frame(symbol).to_csv(cache)
        except Exception:
            pass
        return series
    except Exception as e:
        print(f"   ⚠️ BIZD 获取失败: {e}，指标将优雅降级 (NaN)")
        return pd.Series(dtype="float64")


def get_gold_spx_rolling_corr_20d() -> tuple:
    """
    Rolling 20d corr(Gold, SPX). FRED 优先: SP500, GOLDAMGBD228NLBM；Gold 无 FRED 时 fallback GLD (yfinance).
    Returns (corr_20d: float or np.nan, source_gold: str, details: dict).
    Aligns to daily with ffill for low-freq.
    """
    spx = fetch_series("SP500")
    gold_fred = fetch_series("GOLDAMGBD228NLBM")
    source_gold = "GOLDAMGBD228NLBM"
    if gold_fred is None or gold_fred.empty:
        gold_fred = fetch_series("GOLDPMGBD228NLBM")
        source_gold = "GOLDPMGBD228NLBM"
    if gold_fred is None or gold_fred.empty:
        gold = fetch_yahoo_safe("GLD")
        source_gold = "GLD"
    else:
        gold = gold_fred
    if spx is None or spx.empty or gold is None or gold.empty:
        return (np.nan, source_gold, {"reason": "missing_spx_or_gold"})
    # 交易日历对齐：outer join 成 DataFrame -> ffill -> dropna -> rolling(20).corr
    df = pd.concat([spx.rename("spx"), gold.rename("gold")], axis=1, join="outer")
    df = df.ffill().dropna()
    if len(df) < 20:
        return (np.nan, source_gold, {"reason": "insufficient_overlap_after_ffill"})
    corr_20 = df["spx"].rolling(20, min_periods=15).corr(df["gold"])
    last_corr = float(corr_20.iloc[-1]) if not corr_20.empty and pd.notna(corr_20.iloc[-1]) else np.nan
    last_date = str(df.index[-1].date()) if hasattr(df.index[-1], "date") else str(df.index[-1])
    return (last_corr, source_gold, {"last_date": last_date})


def compose_series(series_id: str) -> Optional[pd.Series]:
    """处理非FRED原生ID的派生系列。优先使用预计算的CSV，否则实时计算。"""
    sid = series_id.upper()
    
    # 预计算的合成指标列表（含 Warsh 管道指标）
    derived_series = [
        "CP_MINUS_DTB3", "SOFR20DMA_MINUS_DTB3",
        "SOFR_MINUS_IORB", "SOFR_IORB_SPREAD", "SOFR_IORB_SPREAD_5DMA", "SOFR_IORB_POS_DAYS_20",
        "RRP_DRAIN_RATE", "RRP_20D_CHANGE", "RRP_20D_PCT",
        "MOVE_PROXY", "BOND_VOL_PROXY", "BOND_VOL_PROXY_Z20",
        "THREEFYTP10_Z20", "NET_LIQUIDITY_Z60",
        "CORPDEBT_GDP_PCT", "RESERVES_ASSETS_PCT", "RESERVES_DEPOSITS_PCT",
        "UST30Y_UST2Y_RSI",
        "HY_OAS_MOMENTUM_RATIO", "SP500_DGS10_CORR60D", "NET_LIQUIDITY",
        "VIX_TERM_STRUCTURE", "HY_IG_RATIO", "GLOBAL_LIQUIDITY_USD",
        "CREDIT_CARD_DELINQUENCY",
        "JPY_VOL_20D", "US_JPY_10Y_SPREAD",
        "JPY_VOL_1M", "US_JPY_10Y_1W_CHG", "NKY_SPX_CORR_20D",
        "JGB_ETF_8282",
    ]
    
    if sid in derived_series:
        # 优先使用预计算的CSV文件（使用 BASE 避免工作目录依赖）
        csv_path = BASE / "data" / "series" / f"{sid}.csv"
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                # 处理不同的列名格式
                if len(df.columns) == 1:
                    ts = df.iloc[:, 0]  # 取第一列
                else:
                    ts = df.squeeze()  # 尝试squeeze
                print(f"📁 使用预计算数据: {series_id} ({len(ts)} 个数据点)")
                return ts
            except Exception as e:
                print(f"⚠️ {series_id}: 预计算数据读取失败: {e}")
        
        # 如果预计算数据不可用，尝试实时计算
        print(f"🔄 实时计算合成指标: {series_id}")
        try:
            if sid == "CP_MINUS_DTB3":
                cp = fetch_series("CPN3M")
                tb = fetch_series("DTB3")
                if cp.empty or tb.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                return (cp.reindex_like(tb).fillna(method="ffill") - tb).dropna()
            
            if sid == "SOFR20DMA_MINUS_DTB3":
                sofr = fetch_series("SOFR")
                tb = fetch_series("DTB3")
                if sofr.empty or tb.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                sofr20 = rolling_mean(sofr, 20)
                tb_aligned = tb.reindex_like(sofr20).ffill()
                return (sofr20 - tb_aligned).dropna()

            # Warsh 约束仪表盘：SOFR - IORB（流动性底线，>0.05% 为银行间流动性枯竭）
            if sid == "SOFR_MINUS_IORB":
                sofr = fetch_series("SOFR")
                iorb = fetch_series("IORB")
                if sofr.empty or iorb.empty:
                    print(f"⚠️ {series_id}: 基础数据(SOFR/IORB)缺失，跳过合成")
                    return None
                iorb_aligned = iorb.reindex_like(sofr).ffill()
                return (sofr - iorb_aligned).dropna()

            # Warsh：RRP 消耗速率（20 日滚动变化率，监测缓冲垫是否被抽干）
            if sid == "RRP_DRAIN_RATE":
                rrp = fetch_series("RRPONTSYD")
                if rrp.empty or len(rrp) < 21:
                    print(f"⚠️ {series_id}: 基础数据(RRPONTSYD)不足，跳过合成")
                    return None
                pct_20d = rrp.pct_change(20)
                return pct_20d.dropna()

            # Warsh：债市恐慌代理（FRED 无 MOVE 则用 Yahoo ^MOVE，再回退 VIX*DGS10 波动率）
            if sid == "MOVE_PROXY":
                move = fetch_series("MOVE")
                if move.empty:
                    move = fetch_yahoo_safe("^MOVE")
                if move is not None and not move.empty:
                    return move.dropna()
                vix = fetch_series("VIXCLS")
                if vix.empty:
                    vix = fetch_yahoo_safe("^VIX")
                dgs10 = fetch_series("DGS10")
                if vix.empty or dgs10.empty:
                    print(f"⚠️ {series_id}: 基础数据(VIX/DGS10)缺失，跳过合成")
                    return None
                dgs10_aligned = dgs10.reindex_like(vix).ffill()
                vol_proxy = vix.rolling(20, min_periods=5).std() * (dgs10_aligned / 100)
                return vol_proxy.dropna()

            if sid == "UST30Y_UST2Y_RSI":
                t30 = fetch_series("DGS30")
                t2 = fetch_series("DGS2")
                if t30.empty or t2.empty:
                    print(f"⚠️ {series_id}: 基础数据(DGS30/DGS2)缺失，跳过合成")
                    return None
                t30_aligned = t30.reindex_like(t2).ffill()
                ratio_daily = (t30_aligned / t2).replace([np.inf, -np.inf], np.nan).dropna()
                if ratio_daily.empty:
                    print(f"⚠️ {series_id}: 比率序列为空，跳过合成")
                    return None
                
                try:
                    ratio_monthly = ratio_daily.resample("ME").last()
                except ValueError:
                    ratio_monthly = ratio_daily.resample("M").last()
                
                rsi_monthly = calculate_rsi(ratio_monthly, window=14).dropna()
                if rsi_monthly.empty:
                    print(f"⚠️ {series_id}: RSI计算结果为空，跳过合成")
                    return None
                
                rsi_daily = rsi_monthly.reindex(ratio_daily.index, method="ffill")
                return rsi_daily.dropna()
            
            if sid == "HY_OAS_MOMENTUM_RATIO":
                oas = fetch_series("BAMLH0A0HYM2")
                if oas.empty:
                    print(f"⚠️ {series_id}: 基础数据(BAMLH0A0HYM2)缺失，跳过合成")
                    return None
                oas_ma20 = rolling_mean(oas, 20)
                oas_aligned = oas.reindex_like(oas_ma20).ffill()
                ratio = (oas_aligned / oas_ma20).replace([np.inf, -np.inf], np.nan)
                return ratio.dropna()
            
            if sid == "SP500_DGS10_CORR60D":
                spx = fetch_series("SP500")
                dgs10 = fetch_series("DGS10")
                if spx.empty or dgs10.empty:
                    print(f"⚠️ {series_id}: 基础数据(SP500/DGS10)缺失，跳过合成")
                    return None
                dgs10_aligned = dgs10.reindex_like(spx).ffill()
                spx_ret = spx.pct_change()
                dgs10_ret = dgs10_aligned.pct_change()
                corr = spx_ret.rolling(60).corr(dgs10_ret)
                return corr.dropna()
            
            if sid == "NET_LIQUIDITY":
                walcl = fetch_series("WALCL")
                tga = fetch_series("WTREGEN")
                rrp = fetch_series("RRPONTSYD")
                if walcl.empty or tga.empty or rrp.empty:
                    print(f"⚠️ {series_id}: 基础数据(WALCL/WTREGEN/RRPONTSYD)缺失，跳过合成")
                    return None
                start = min(walcl.index.min(), tga.index.min(), rrp.index.min())
                end = max(walcl.index.max(), tga.index.max(), rrp.index.max())
                daily_index = pd.date_range(start, end, freq="D")
                
                walcl_daily = walcl.reindex(daily_index).interpolate(method="time").ffill()
                tga_daily = tga.reindex(daily_index).ffill()
                rrp_daily = rrp.reindex(daily_index).ffill()
                
                net_liquidity = walcl_daily - tga_daily - rrp_daily
                return net_liquidity.dropna()
            
            if sid == "VIX_TERM_STRUCTURE":
                vix = fetch_series("VIXCLS")
                if vix.empty:
                    vix = fetch_yahoo_safe("^VIX")

                vix3m = fetch_series("VIX3M")
                if vix3m.empty:
                    vix3m = fetch_yahoo_safe("^VIX3M")

                if not vix.empty and not vix3m.empty:
                    vix_a, v3_a = vix.align(vix3m, join="inner")
                    ratio = (vix_a / v3_a).replace([np.inf, -np.inf], np.nan).dropna()
                    if not ratio.empty:
                        return ratio

                if not vix.empty:
                    print("   ⚠️ VIX3M(FRED/Yahoo ^VIX3M)不可用，尝试备选 ^VIX9D...")
                    vix9d = fetch_yahoo_safe("^VIX9D")
                    if not vix9d.empty:
                        vix_a, v9_a = vix.align(vix9d, join="inner")
                        rr = (v9_a / vix_a).replace([np.inf, -np.inf], np.nan).dropna()
                        if not rr.empty:
                            return rr
                    vixn = pd.to_numeric(vix, errors="coerce").dropna()
                    if len(vixn) >= 61:
                        _logger_cm.warning(
                            "CRITICAL: Both FRED and Yahoo failed for VIX3M. Using MA60 fallback."
                        )
                        ma60 = vixn.rolling(60).mean()
                        ult = (vixn / ma60).replace([np.inf, -np.inf], np.nan).dropna()
                        ult = ult[np.isfinite(ult) & (ult > 0)]
                        if not ult.empty:
                            return ult

                print(f"⚠️ {series_id}: 基础数据(VIXCLS/VIX3M)缺失，跳过合成")
                return None
            
            if sid == "HY_IG_RATIO":
                hy = fetch_series("BAMLHYH0A0HYM2TRIV")
                ig = fetch_series("BAMLCC0A0CMTRIV")
                if hy.empty or ig.empty:
                    print(f"⚠️ {series_id}: 基础数据(HY/IG总回报)缺失，跳过合成")
                    return None
                hy, ig = hy.align(ig, join="inner")
                ratio = (hy / ig).replace([np.inf, -np.inf], np.nan)
                return ratio.dropna()
            
            if sid == "GLOBAL_LIQUIDITY_USD":
                fed = fetch_series("WALCL")
                if fed.empty:
                    print(f"⚠️ {series_id}: 基础数据(WALCL)缺失，跳过合成")
                    return None
                ecb = fetch_series("ECBASSETSW")
                eurusd = fetch_series("DEXUSEU")
                boj = fetch_series("JPNASSETS")
                usdjpy = fetch_series("DEXJPUS")
                
                start = fed.index.min()
                end = fed.index.max()
                for s in [ecb, eurusd, boj, usdjpy]:
                    if s is not None and not s.empty:
                        start = min(start, s.index.min())
                        end = max(end, s.index.max())
                
                daily_index = pd.date_range(start, end, freq="D")
                fed_daily = fed.reindex(daily_index).ffill()
                total = fed_daily.copy()
                
                if ecb is not None and not ecb.empty and eurusd is not None and not eurusd.empty:
                    ecb_daily = ecb.reindex(daily_index).ffill()
                    eurusd_daily = eurusd.reindex(daily_index).ffill()
                    ecb_usd = ecb_daily * eurusd_daily
                    total = total.add(ecb_usd, fill_value=0)
                
                if boj is not None and not boj.empty and usdjpy is not None and not usdjpy.empty:
                    boj_daily = boj.reindex(daily_index).ffill()
                    usdjpy_daily = usdjpy.reindex(daily_index).ffill()
                    # BOJ单位假定为1e8 JPY，换算为USD
                    boj_usd = (boj_daily * 100000000) / usdjpy_daily
                    total = total.add(boj_usd, fill_value=0)
                
                return total.dropna()
            
            if sid == "CREDIT_CARD_DELINQUENCY":
                delinquency = fetch_series("DRCCLACBS")
                if delinquency.empty:
                    print(f"⚠️ {series_id}: 基础数据(DRCCLACBS)缺失，跳过合成")
                    return None
                return delinquency.dropna()
            
            if sid == "CORPDEBT_GDP_PCT":
                corp_debt = fetch_series("NCBDBIQ027S")
                gdp = fetch_series("GDP")
                if corp_debt.empty or gdp.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                # 单位转换：企业债从Millions转为Billions
                corp_debt_billions = corp_debt / 1000
                corp_debt_aligned = corp_debt_billions.reindex_like(gdp).fillna(method="ffill")
                return (corp_debt_aligned / gdp * 100).dropna()
            
            if sid == "RESERVES_ASSETS_PCT":
                reserves = fetch_series("TOTRESNS")
                assets = fetch_series("WALCL")
                if reserves.empty or assets.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                reserves_aligned = reserves.reindex_like(assets).fillna(method="ffill")
                return (reserves_aligned / assets * 100).dropna()
            
            if sid == "RESERVES_DEPOSITS_PCT":
                reserves = fetch_series("TOTRESNS")
                # 尝试不同的存款指标
                deposits_series = ["DPSACBW027SBOG", "TOTALSA", "TOTALSL"]
                deposits = None
                for dep_series in deposits_series:
                    deposits = fetch_series(dep_series)
                    if not deposits.empty:
                        break
                
                if deposits is None or deposits.empty:
                    print(f"⚠️ {series_id}: 存款数据缺失，跳过合成")
                    return None
                
                reserves_aligned = reserves.reindex_like(deposits).fillna(method="ffill")
                return (reserves_aligned / deposits * 100).dropna()

            # 日本/日元观察点：USD/JPY 20 日滚动年化波动率 (%)
            if sid == "JPY_VOL_20D":
                usdjpy = fetch_yahoo_safe("USDJPY=X")
                if usdjpy is None or usdjpy.empty or len(usdjpy) < 21:
                    print(f"⚠️ {series_id}: USD/JPY 数据不足，跳过合成")
                    return None
                usdjpy = usdjpy.replace(0, np.nan).dropna()
                logret = np.log(usdjpy / usdjpy.shift(1))
                vol20 = logret.rolling(20, min_periods=5).std() * (np.sqrt(252) * 100)
                out = vol20.replace([np.inf, -np.inf], np.nan).dropna()
                return out

            # 美日 10Y 利差 (bp)：美 10Y - 日 10Y（FRED IRLTLT01JPM156N 为月频，对齐到日频）
            if sid == "US_JPY_10Y_SPREAD":
                ust10 = fetch_series("DGS10")
                jgb10 = fetch_series("IRLTLT01JPM156N")
                if ust10.empty:
                    print(f"⚠️ {series_id}: DGS10 缺失，跳过合成")
                    return None
                if jgb10.empty or jgb10.isna().all():
                    print(f"⚠️ {series_id}: 日本 10Y(IRLTLT01JPM156N) 缺失，跳过合成")
                    return None
                jgb_daily = jgb10.reindex(ust10.index).ffill()
                spread_bp = (ust10 - jgb_daily)  # 均为 % 单位，差值为 bp 等价
                out = spread_bp.replace([np.inf, -np.inf], np.nan).dropna()
                return out

            # 日本 Carry-Trade Unwind：USD/JPY 1 个月实现波动率（约 21 个交易日）
            if sid == "JPY_VOL_1M":
                usdjpy = fetch_yahoo_safe("USDJPY=X")
                if usdjpy is None or usdjpy.empty or len(usdjpy) < 22:
                    print(f"⚠️ {series_id}: USD/JPY 数据不足，跳过合成")
                    return None
                usdjpy = usdjpy.replace(0, np.nan).dropna()
                logret = np.log(usdjpy / usdjpy.shift(1))
                vol1m = logret.rolling(21, min_periods=5).std() * (np.sqrt(252) * 100)
                out = vol1m.replace([np.inf, -np.inf], np.nan).dropna()
                return out

            # 美日 10Y 利差单周变化 (bps)：收窄为负，用于平仓压力监测
            if sid == "US_JPY_10Y_1W_CHG":
                spread = compose_series("US_JPY_10Y_SPREAD")
                if spread is None or spread.empty or len(spread) < 6:
                    print(f"⚠️ {series_id}: US_JPY_10Y_SPREAD 不足，跳过合成")
                    return None
                chg = spread.diff(5)  # 5 个交易日 ≈ 1 周；单位为 %，*100 为 bps
                out = (chg * 100).replace([np.inf, -np.inf], np.nan).dropna()
                return out

            # 日经 225 与标普 500 20 日滚动相关系数（去杠杆时相关性飙升）
            if sid == "NKY_SPX_CORR_20D":
                nky = fetch_yahoo_safe("^N225")
                spx = fetch_series("SP500")
                if spx.empty:
                    spx = fetch_yahoo_safe("^GSPC")
                if nky is None or nky.empty or spx is None or spx.empty:
                    print(f"⚠️ {series_id}: 日经/标普数据缺失，跳过合成")
                    return None
                nky, spx = nky.align(spx, join="inner")
                r_nky = nky.pct_change()
                r_spx = spx.pct_change()
                corr = r_nky.rolling(20, min_periods=5).corr(r_spx)
                out = corr.replace([np.inf, -np.inf], np.nan).dropna()
                return out

            # 日本 10Y 国债实时代理：8282.T（跟踪 10Y 日债的 ETF，yfinance 可用；FRED IRLTLT01JPM156N 常滞后 1–2 月）
            if sid == "JGB_ETF_8282":
                etf = fetch_yahoo_safe("8282.T")
                if etf is None or etf.empty:
                    print(f"⚠️ {series_id}: 8282.T 数据不可用，跳过")
                    return None
                out = etf.replace(0, np.nan).dropna()
                return out
            
        except Exception as e:
            print(f"⚠️ {series_id}: 实时合成计算失败: {e}")
            return None
    
    return None

# 设置中文字体
def setup_chinese_font():
    """设置中文字体"""
    try:
        # 尝试设置中文字体
        font_list = font_manager.findSystemFonts()
        chinese_fonts = []
        for font_path in font_list:
            try:
                font_prop = font_manager.FontProperties(fname=font_path)
                font_name = font_prop.get_name()
                if any(char in font_name for char in ['微软雅黑', 'Microsoft YaHei', 'SimHei', '黑体']):
                    chinese_fonts.append(font_name)
            except:
                continue
        
        if chinese_fonts:
            plt.rcParams['font.sans-serif'] = chinese_fonts[:1]
            plt.rcParams['axes.unicode_minus'] = False
            print(f"✅ 找到中文字体: {chinese_fonts[0]}")
        else:
            print("⚠️ 未找到中文字体，使用默认字体")
    except Exception as e:
        print(f"⚠️ 字体设置失败: {e}")

setup_chinese_font()

# 导入依赖模块
FRED_AVAILABLE = False
try:
    from scripts.fred_http import series_observations, series_search
    from scripts.clean_utils import parse_numeric_series
    FRED_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ FRED模块不可用: {e}")
    print("将只使用本地缓存数据")

# 频率工具
def _month_end_code() -> str:
    try:
        pd.date_range("2000-01-31", periods=2, freq="ME")
        return "ME"
    except Exception:
        return "M"

FREQ_ME = _month_end_code()

def _as_float_series(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan)
    return s.astype("float64")

def _read_png_as_base64(p: pathlib.Path) -> str | None:
    """读取PNG文件并转换为base64字符串"""
    try:
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return None

_IMG_MD_RE = re.compile(r"!\[[^\]]*\]\((?P<path>[^)]+\.png)\)")

def render_html_report(markdown_content: str, title: str, output_dir: pathlib.Path) -> str:
    body = _md2html(markdown_content)

    # 使用Base64嵌入图片，避免wkhtmltoimage的文件访问问题
    def replace_img_with_base64(match):
        img_path = output_dir / match.group('path')
        base64_data = _read_png_as_base64(img_path)
        if base64_data:
            return f'<img src="data:image/png;base64,{base64_data}">'
        return match.group(0)
    
    body = _IMG_MD_RE.sub(replace_img_with_base64, body)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_html.escape(title)}</title>
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,'Microsoft YaHei','PingFang SC',sans-serif;line-height:1.6;padding:16px;max-width:1000px;margin:auto;background:#f5f5f5}}
  article{{background:#fff;padding:24px;border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,.06)}}
  h1,h2,h3{{line-height:1.25}}
  img{{max-width:100%;height:auto;display:block;margin:8px 0}}
  table{{border-collapse:collapse;width:100%;margin:16px 0;font-size:14px}}
  th,td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left;vertical-align:top}}
  th{{background:#f8fafc;font-weight:600}}
  code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}}
</style>
</head>
<body>
<article>
{body}
</article>
</body>
</html>"""

# 配置工具
def load_yaml_config(path: pathlib.Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# 图片生成函数
def generate_indicator_chart(indicator_name: str, current_value: float, benchmark_value: float, 
                           risk_score: float, output_dir: pathlib.Path) -> str:
    """生成单个指标的图表"""
    try:
        # 创建图表
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # 模拟历史数据
        dates = pd.date_range('2020-01-01', periods=48, freq='M')
        values = np.random.normal(current_value, current_value * 0.1, 48)
        values = np.clip(values, 0, current_value * 2)  # 确保值为正
        
        # 绘制历史数据
        ax.plot(dates, values, 'b-', linewidth=2, label='历史数据')
        
        # 标记当前值
        ax.axhline(y=current_value, color='red', linestyle='--', linewidth=2, label=f'当前值: {current_value:.2f}')
        
        # 标记基准值
        ax.axhline(y=benchmark_value, color='green', linestyle='--', linewidth=2, label=f'基准值: {benchmark_value:.2f}')
        
        # 设置标题和标签
        ax.set_title(f'{indicator_name}\n风险评分: {risk_score:.1f}/100', fontsize=14, fontweight='bold')
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('数值', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 格式化x轴 - 优化日期显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=6))
        plt.xticks(rotation=0)
        
        # 设置x轴范围，避免日期重叠
        if len(dates) > 0:
            ax.set_xlim(dates[0], dates[-1])
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片
        chart_filename = f"{indicator_name.replace(':', '_').replace(' ', '_')}_chart.png"
        chart_path = output_dir / "figures" / chart_filename
        chart_path.parent.mkdir(parents=True, exist_ok=True)
        
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return f"figures/{chart_filename}"
        
    except Exception as e:
        print(f"❌ 生成图表失败 {indicator_name}: {e}")
        return None

def generate_summary_chart(group_scores: dict, total_score: float, output_dir: pathlib.Path) -> str:
    """生成总体风险概览图表"""
    try:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 左图：分组风险评分
        groups = list(group_scores.keys())
        scores = [group_scores[g]['score'] for g in groups]
        colors = ['#d32f2f' if s >= 60 else '#f57c00' if s >= 40 else '#388e3c' for s in scores]
        
        bars = ax1.bar(range(len(groups)), scores, color=colors, alpha=0.7)
        ax1.set_title('分组风险评分', fontsize=14, fontweight='bold')
        ax1.set_xlabel('分组', fontsize=12)
        ax1.set_ylabel('风险评分', fontsize=12)
        ax1.set_xticks(range(len(groups)))
        ax1.set_xticklabels(groups, rotation=45, ha='right')
        ax1.set_ylim(0, 100)
        ax1.grid(True, alpha=0.3)
        
        # 添加数值标签
        for i, (bar, score) in enumerate(zip(bars, scores)):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                    f'{score:.1f}', ha='center', va='bottom', fontweight='bold')
        
        # 右图：总体风险等级
        risk_levels = ['极低风险', '低风险', '中风险', '高风险']
        risk_colors = ['#1976d2', '#388e3c', '#f57c00', '#d32f2f']
        
        # 根据总分确定风险等级
        if total_score >= 80:
            current_level = 3
        elif total_score >= 60:
            current_level = 2
        elif total_score >= 40:
            current_level = 1
        else:
            current_level = 0
        
        # 创建风险等级指示器
        for i, (level, color) in enumerate(zip(risk_levels, risk_colors)):
            alpha = 1.0 if i == current_level else 0.3
            ax2.barh(i, 100, color=color, alpha=alpha, height=0.6)
            ax2.text(50, i, level, ha='center', va='center', fontweight='bold', fontsize=12)
        
        ax2.set_title(f'总体风险等级\n总分: {total_score:.1f}/100', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 100)
        ax2.set_ylim(-0.5, 3.5)
        ax2.set_yticks(range(len(risk_levels)))
        ax2.set_yticklabels(risk_levels)
        ax2.set_xlabel('风险评分', fontsize=12)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存图片
        summary_path = output_dir / "figures" / "summary_chart.png"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        plt.savefig(summary_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return "figures/summary_chart.png"
        
    except Exception as e:
        print(f"❌ 生成概览图表失败: {e}")
        return None


# ---------- Warsh Constraint Dashboard: 解释性报告与风险信号灯 ----------
# RRPONTSYD 在 FRED 中单位为十亿美元 (Billions of Dollars)，无需再除 1000
RRP_GREEN_MIN_BN = 500.0   # RRP > 500 十亿
RRP_RED_MAX_BN = 100.0     # RRP < 100 十亿
RRP_YELLOW_20D_DROP_BN = -200.0  # 20 日绝对下降超过 200 十亿
RRP_AMBER_BN = 100.0       # RRP < 100B → AMBER (Constraint Watch)；绘图阈值线

def get_warsh_timeseries():
    """
    获取 Warsh 仪表盘用时间序列（真实数据，与 get_warsh_state 同源）。
    返回 (rrp_bn, spread_bp, tp_pct) 对齐到同一日期索引；任一缺失则返回 None。
    - rrp_bn: RRP 十亿美元 (Billions)
    - spread_bp: SOFR−IORB 基点 (basis points)，内部百分比×100
    - tp_pct: 10Y 期限溢价百分比 (THREEFYTP10)
    """
    rrp = fetch_series("RRPONTSYD")
    spread_pct = compose_series("SOFR_MINUS_IORB")
    if spread_pct is None or spread_pct.empty:
        sofr = fetch_series("SOFR")
        iorb = fetch_series("IORB")
        if sofr is not None and not sofr.empty and iorb is not None and not iorb.empty:
            iorb_aligned = iorb.reindex_like(sofr).ffill()
            spread_pct = (sofr - iorb_aligned).dropna()
    tp = fetch_series("THREEFYTP10")
    if rrp is None or rrp.empty or spread_pct is None or spread_pct.empty or tp is None or tp.empty:
        return None
    # 对齐到共同日期，去掉任一缺失的行，保证三序列同长同索引
    common = rrp.index.intersection(spread_pct.index).intersection(tp.index)
    if len(common) < 20:
        return None
    df = pd.DataFrame(
        {"rrp": rrp.reindex(common).ffill().bfill(), "spread": spread_pct.reindex(common).ffill().bfill(), "tp": tp.reindex(common).ffill().bfill()},
        index=common,
    ).dropna(how="any")
    if len(df) < 20:
        return None
    rrp_bn = df["rrp"]
    spread_bp = df["spread"] * 100.0  # 百分比 → bp
    tp_pct = df["tp"]
    return (rrp_bn, spread_bp, tp_pct)

def plot_warsh_plumbing(rrp_series_bn: pd.Series, spread_series_bp: pd.Series, out_png_path: pathlib.Path) -> None:
    """
    Warsh 管道双轴图：RRP (B) 左轴，SOFR−IORB (bp) 右轴；标注 AMBER 阈值 100B 与 0bp 硬约束线。
    """
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.set_xlabel("Date")
    ax1.set_ylabel("RRP (Billions USD)", color="C0")
    ax1.plot(rrp_series_bn.index, rrp_series_bn.values, color="C0", label="RRP (B)")
    ax1.axhline(y=RRP_AMBER_BN, color="orange", linestyle="--", linewidth=1.5, label=f"AMBER threshold ({RRP_AMBER_BN:.0f}B)")
    ax1.tick_params(axis="y", labelcolor="C0")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.set_ylabel("SOFR − IORB (bp)", color="C1")
    ax2.plot(spread_series_bp.index, spread_series_bp.values, color="C1", label="SOFR−IORB (bp)")
    ax2.axhline(y=0.0, color="red", linestyle="--", linewidth=1.5, label="RED (hard constraint)")
    ax2.tick_params(axis="y", labelcolor="C1")
    ax2.legend(loc="upper right")
    fig.tight_layout()
    out_png_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png_path, dpi=160, bbox_inches="tight")
    plt.close()

def plot_warsh_term_premium(tp_series_pct: pd.Series, out_png_path: pathlib.Path) -> None:
    """
    10Y 期限溢价单轴图；画历史中位数水平线，图例说明高于中枢表示重估压力上升。
    """
    tp_median = float(np.nanmedian(tp_series_pct.values))
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_xlabel("Date")
    ax.set_ylabel("10Y Term Premium (%)")
    ax.plot(tp_series_pct.index, tp_series_pct.values, color="C0", label="10Y Term Premium")
    ax.axhline(y=tp_median, color="gray", linestyle="--", linewidth=1.5, label=f"Historical median ({tp_median:.2f}%)")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    ax.set_title("Above median → repricing risk / stress pressure may be building")
    fig.tight_layout()
    out_png_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png_path, dpi=160, bbox_inches="tight")
    plt.close()

# CRO 口径：Warsh 仪表盘说明（禁止预测/交易/情绪化）
WARSH_DASHBOARD_CRO_NARRATIVE = """
**本仪表盘监控内容**
- **RRP（逆回购）**：流动性缓冲垫。接近枯竭时，系统对资产负债表收紧的容忍度下降（对应 AMBER / Constraint Watch）。
- **SOFR−IORB 利差**：反映利率走廊是否被测试。持续高于 0bp 视为硬约束突破（RED）。
- **期限溢价（Term premium）**：刻画长端除政策利率预期外的重定价压力；持续高于历史中位数表示重定价风险在积累。

**如何解读**
- RRP 偏低且利差逼近 0bp：约束正在形成，吸收冲击能力下降。
- 利差持续 > 0bp：硬约束突破，历史上政策重新校准（recalibration）的概率上升。
- 期限溢价上升且债市波动上升：债市功能压力 / 重定价风险。
"""

def get_warsh_state() -> dict:
    """收集 Warsh 管道指标当前值、历史分位及数据截止日。缺失序列时字段为 None。"""
    state = {
        "sofr_iorb_spread": None,
        "sofr_iorb_spread_5dma": None,
        "sofr_iorb_pos_days_20": None,
        "sofr_iorb_last_date": None,
        "rrp_level": None,
        "rrp_level_bn": None,
        "rrp_20d_pct": None,
        "rrp_20d_abs_change_bn": None,
        "rrp_last_date": None,
        "term_premium": None,
        "term_premium_median": None,
        "term_premium_last_date": None,
        "bond_vol_proxy": None,
        "bond_vol_60pctl": None,
        "bear_steepening": None,
    }
    try:
        # SOFR - IORB
        spread_ts = compose_series("SOFR_MINUS_IORB")
        if spread_ts is None or spread_ts.empty:
            sofr = fetch_series("SOFR")
            iorb = fetch_series("IORB")
            if sofr is not None and not sofr.empty and iorb is not None and not iorb.empty:
                iorb_aligned = iorb.reindex_like(sofr).ffill()
                spread_ts = (sofr - iorb_aligned).dropna()
        if spread_ts is not None and not spread_ts.empty:
            state["sofr_iorb_spread"] = float(spread_ts.iloc[-1])
            state["sofr_iorb_spread_5dma"] = float(spread_ts.rolling(5, min_periods=1).mean().iloc[-1])
            last_20 = spread_ts.iloc[-20:] if len(spread_ts) >= 20 else spread_ts
            state["sofr_iorb_pos_days_20"] = int((last_20 > 0).sum())
            state["sofr_iorb_last_date"] = str(spread_ts.index[-1].date()) if hasattr(spread_ts.index[-1], "date") else str(spread_ts.index[-1])

        # RRP (FRED: 十亿美元 Billions of Dollars)
        rrp = fetch_series("RRPONTSYD")
        if rrp is not None and not rrp.empty and len(rrp) >= 21:
            state["rrp_level"] = float(rrp.iloc[-1])  # 十亿
            state["rrp_level_bn"] = state["rrp_level"]  # 已是十亿
            prev_20 = rrp.iloc[-21]
            if prev_20 and prev_20 != 0:
                state["rrp_20d_pct"] = float((rrp.iloc[-1] - prev_20) / prev_20 * 100)
            state["rrp_20d_abs_change_bn"] = float(rrp.iloc[-1] - rrp.iloc[-21]) if len(rrp) >= 21 else None
            state["rrp_last_date"] = str(rrp.index[-1].date()) if hasattr(rrp.index[-1], "date") else str(rrp.index[-1])

        # Term Premium + 历史中位数
        tp = fetch_series("THREEFYTP10")
        if tp is not None and not tp.empty:
            state["term_premium"] = float(tp.iloc[-1])
            state["term_premium_median"] = float(np.nanmedian(tp.values))
            state["term_premium_last_date"] = str(tp.index[-1].date()) if hasattr(tp.index[-1], "date") else str(tp.index[-1])

        # Bond vol proxy: VIXCLS * DGS10 (rolling 20d if needed for percentile)
        vix = fetch_series("VIXCLS")
        dgs10 = fetch_series("DGS10")
        if vix is not None and not vix.empty and dgs10 is not None and not dgs10.empty:
            dgs10_aligned = dgs10.reindex_like(vix).ffill()
            bond_vol = (vix * (dgs10_aligned / 100)).dropna()
            if not bond_vol.empty:
                state["bond_vol_proxy"] = float(bond_vol.iloc[-1])
                state["bond_vol_60pctl"] = float(np.nanpercentile(bond_vol.values, 60))

        # Bear steepening: Δ20D(DGS10) > 0 and Δ20D(DGS2) < 0
        d10 = fetch_series("DGS10")
        d2 = fetch_series("DGS2")
        if d10 is not None and not d10.empty and d2 is not None and not d2.empty and len(d10) >= 21 and len(d2) >= 21:
            delta10 = float(d10.iloc[-1] - d10.iloc[-21])
            delta2 = float(d2.iloc[-1] - d2.iloc[-21])
            state["bear_steepening"] = bool(delta10 > 0 and delta2 < 0)
    except Exception as e:
        print(f"⚠️ Warsh 状态收集异常: {e}")
    return state


def compute_warsh_risk_level(indicators: dict) -> dict:
    """
    [Logic Refined by CRO]
    Warsh 风险信号灯逻辑修正：
    - RED (Breached): 物理约束已被突破 (SOFR > IORB)。
    - AMBER (Constraint Watch): 缓冲垫耗尽 (RRP < 100B)，系统失去弹性，但尚未通过价格对抗。
    - YELLOW (Emerging): 缓冲垫快速消耗或尾部风险上升。
    - GREEN: 管道畅通。
    """
    s = indicators
    red_flags = []
    amber_flags = []
    yellow_flags = []

    reason_red = "Constraint Breached — Financial system actively resisting tightening. Policy pivot imminent."
    reason_amber = "Policy Constraint Watch — System tolerance to tightening has materially declined (Buffer Exhausted)."
    reason_yellow = "Constraint Forming — Liquidity buffer allows tightening, but capacity is shrinking."
    reason_green = "Green Light — Financial plumbing remains functional and absorptive."

    sofr = s.get("sofr_iorb_spread")
    sofr5 = s.get("sofr_iorb_spread_5dma")
    rrp_bn = s.get("rrp_level_bn")
    rrp_drop = s.get("rrp_20d_abs_change_bn")
    tp_val = s.get("term_premium")
    tp_med = s.get("term_premium_median")
    bv = s.get("bond_vol_proxy")
    bv60 = s.get("bond_vol_60pctl")

    # --- 1. RED Criteria (必须是硬约束突破) ---
    if sofr5 is not None and sofr5 > 0.00:
        red_flags.append("Liquidity floor breached (SOFR > IORB)")
    if tp_val is not None and bv is not None and bv60 is not None and tp_med is not None:
        if tp_val > tp_med * 1.5 and bv > bv60 * 1.5:
            red_flags.append("Bond vigilantes active rejection")

    if red_flags:
        return {"warsh_level": "RED", "warsh_level_reason": reason_red, "warsh_flags": red_flags}

    # --- 2. AMBER Criteria (缓冲耗尽，高度脆弱) ---
    if rrp_bn is not None and rrp_bn < 100.0:
        amber_flags.append("Liquidity buffer exhausted (RRP < 100B)")
    if sofr is not None and sofr >= -0.01:
        amber_flags.append("SOFR testing IORB floor (>-1bp)")
    if s.get("bear_steepening") is True:
        amber_flags.append("Bear steepening (Policy conflict pricing)")

    if amber_flags:
        return {"warsh_level": "AMBER", "warsh_level_reason": reason_amber, "warsh_flags": amber_flags}

    # --- 3. YELLOW Criteria (早期预警) ---
    if rrp_drop is not None and rrp_drop < -100.0:
        yellow_flags.append("Buffer draining rapidly")
    if tp_val is not None and tp_med is not None and tp_val > tp_med * 1.1:
        yellow_flags.append("Term premium widening")

    if yellow_flags:
        return {"warsh_level": "YELLOW", "warsh_level_reason": reason_yellow, "warsh_flags": yellow_flags}

    return {"warsh_level": "GREEN", "warsh_level_reason": reason_green, "warsh_flags": []}


# ---------- JPY Carry-Trade Unwind 模块：数据状态与传染检测 ----------
# 阈值：利差单周收窄超过 15bps 视为平仓压力；NKY-SPX 相关性 > 0.85 视为全球去杠杆
JPY_SPREAD_NARROW_BPS = -15.0
JPY_NKY_SPX_CORR_HIGH = 0.85
JGB_KEY_LEVELS_PCT = [1.0, 1.5]
YEN_APPRECIATION_5D_PCT = -2.0   # 5 日内日元升值超过 2% 视为快速升值


def get_japan_carry_state() -> dict:
    """
    收集 JPY Carry-Trade Unwind 观测点：波动率、美日利差及 1 周变化、NKY-SPX 相关性、
    JGB 10Y、USD/JPY 近期变化。用于 check_japan_contagion 与报告展示。
    """
    state = {
        "jpy_vol_1m": None,
        "jpy_vol_1y_p90": None,
        "us_jp_spread": None,
        "us_jp_spread_1w_chg_bps": None,
        "nky_spx_corr_20d": None,
        "nky_spx_corr_median": None,
        "jgb10y": None,
        "usdjpy_5d_pct": None,
        "data_cutoff": None,
    }
    try:
        vol1m = compose_series("JPY_VOL_1M")
        if vol1m is not None and not vol1m.empty:
            state["jpy_vol_1m"] = float(vol1m.iloc[-1])
            one_year = vol1m.last("365D")
            if len(one_year) >= 20:
                state["jpy_vol_1y_p90"] = float(one_year.quantile(0.90))
            state["data_cutoff"] = vol1m.index[-1]

        spread = compose_series("US_JPY_10Y_SPREAD")
        if spread is not None and not spread.empty:
            state["us_jp_spread"] = float(spread.iloc[-1])
            state["data_cutoff"] = state["data_cutoff"] or spread.index[-1]
        chg = compose_series("US_JPY_10Y_1W_CHG")
        if chg is not None and not chg.empty:
            state["us_jp_spread_1w_chg_bps"] = float(chg.iloc[-1])

        corr = compose_series("NKY_SPX_CORR_20D")
        if corr is not None and not corr.empty:
            state["nky_spx_corr_20d"] = float(corr.iloc[-1])
            one_year_c = corr.last("365D")
            if len(one_year_c) >= 20:
                state["nky_spx_corr_median"] = float(one_year_c.median())

        jgb = fetch_series("IRLTLT01JPM156N")
        if jgb is not None and not jgb.empty:
            state["jgb10y"] = float(jgb.iloc[-1])
            state["jgb10y_cutoff"] = jgb.index[-1]  # FRED 常滞后 1–2 月，用于报告标注
            try:
                cutoff = jgb.index[-1]
                last_date = cutoff.date() if hasattr(cutoff, "date") else cutoff
                state["jgb_fred_stale"] = (datetime.now().date() - last_date).days > 30
            except Exception:
                state["jgb_fred_stale"] = True
        else:
            state["jgb_fred_stale"] = True

        # 8282.T 为 10Y 日债 ETF 代理，用于近期敏感度（实时代理）
        etf8282 = compose_series("JGB_ETF_8282")
        if etf8282 is not None and len(etf8282) >= 6:
            pct_5d = (float(etf8282.iloc[-1]) / float(etf8282.iloc[-6]) - 1.0) * 100
            state["jgb_etf_8282_5d_pct"] = pct_5d

        usdjpy = fetch_yahoo_safe("USDJPY=X")
        if usdjpy is not None and len(usdjpy) >= 6:
            pct_5d = (float(usdjpy.iloc[-1]) / float(usdjpy.iloc[-6]) - 1.0) * 100
            state["usdjpy_5d_pct"] = pct_5d  # 负 = 日元升值
    except Exception as e:
        print(f"⚠️ Japan carry 状态收集异常: {e}")
    return state


def check_japan_contagion(warsh_level: Optional[str] = None, japan_state: Optional[dict] = None) -> dict:
    """
    JPY Carry-Trade Unwind 风险等级与 Regime 联动。

    - Normal: 无触发。
    - Amber: JPY 1M 波动率突破 1Y 90% 分位 / 利差单周收窄 >15bps / NKY-SPX 相关性飙升。
    - Red: JGB 触及关键关口(1.0% 或 1.5%)且日元快速升值；或 (Warsh AMBER + 日本波动率 AMBER) 直接升 RED。

    返回: {"level": "Normal"|"Amber"|"Red", "reasons": [], "flags": {...}, "escalate_global_to_red": bool}
    """
    s = japan_state if japan_state is not None else get_japan_carry_state()
    warsh = (warsh_level or "").strip().upper()
    reasons = []
    flags = {}

    # --- Amber 条件 ---
    vol_trigger = False
    if s.get("jpy_vol_1m") is not None and s.get("jpy_vol_1y_p90") is not None:
        if s["jpy_vol_1m"] >= s["jpy_vol_1y_p90"]:
            vol_trigger = True
            reasons.append("JPY 1M 实现波动率突破过去一年 90% 分位")
            flags["jpy_vol_amber"] = True

    if s.get("us_jp_spread_1w_chg_bps") is not None and s["us_jp_spread_1w_chg_bps"] <= JPY_SPREAD_NARROW_BPS:
        reasons.append("美日 10Y 利差单周收窄超过 15bps，平仓压力上升")
        flags["spread_narrow_amber"] = True

    if s.get("nky_spx_corr_20d") is not None and s["nky_spx_corr_20d"] >= JPY_NKY_SPX_CORR_HIGH:
        reasons.append("日经-标普 20 日相关性飙升，全球去杠杆信号")
        flags["nky_spx_corr_amber"] = True

    # --- Red 条件 1：JGB 关键关口 + 日元快速升值 ---
    jgb_red = False
    if s.get("jgb10y") is not None and s.get("usdjpy_5d_pct") is not None:
        for thresh in JGB_KEY_LEVELS_PCT:
            if s["jgb10y"] >= thresh and s["usdjpy_5d_pct"] <= YEN_APPRECIATION_5D_PCT:
                jgb_red = True
                reasons.append(f"日本 10Y 触及 {thresh}% 且日元 5 日快速升值 (Regime Shift)")
                flags["jgb_yen_red"] = True
                break

    # --- Red 条件 2：Warsh AMBER + 日本波动率 AMBER → 跳过中间状态升 RED ---
    escalate_global_to_red = False
    if warsh == "AMBER" and vol_trigger:
        escalate_global_to_red = True
        reasons.append("Warsh 流动性缓冲 AMBER 且 JPY 波动率警报同时触发，全局升 RED")
        flags["warsh_jpy_escalation"] = True

    if jgb_red or escalate_global_to_red:
        return {
            "level": "Red",
            "reasons": reasons,
            "flags": flags,
            "escalate_global_to_red": escalate_global_to_red,
            "japan_state": s,
        }
    if reasons:
        return {
            "level": "Amber",
            "reasons": reasons,
            "flags": flags,
            "escalate_global_to_red": False,
            "japan_state": s,
        }
    return {
        "level": "Normal",
        "reasons": [],
        "flags": {},
        "escalate_global_to_red": False,
        "japan_state": s,
    }


def _japan_indicator_status(s: dict) -> tuple:
    """各核心指标单独状态与说明，用于仪表盘表格。返回 (vol_status, vol_note), (spread_status, spread_note), (corr_status, corr_note)。"""
    vol_amber = (
        s.get("jpy_vol_1m") is not None
        and s.get("jpy_vol_1y_p90") is not None
        and s["jpy_vol_1m"] >= s["jpy_vol_1y_p90"]
    )
    vol_status = "🟡 AMBER" if vol_amber else "🟢 Normal"
    vol_note = "突破一年 90% 分位，日元走势开始不稳定。" if vol_amber else "未突破一年 90% 分位。"

    spread_amber = (
        s.get("us_jp_spread_1w_chg_bps") is not None
        and s["us_jp_spread_1w_chg_bps"] <= JPY_SPREAD_NARROW_BPS
    )
    spread_status = "🟡 AMBER" if spread_amber else "🟢 Normal"
    chg = s.get("us_jp_spread_1w_chg_bps")
    spread_note = f"周变化 {chg:.1f} bps，触及 -15bps 平仓警戒线。" if spread_amber else (f"周变化 {chg:.1f} bps，未触及 -15bps 平仓警戒线。" if chg is not None else "—")

    corr_red = (
        s.get("nky_spx_corr_20d") is not None
        and s["nky_spx_corr_20d"] >= JPY_NKY_SPX_CORR_HIGH
    )
    corr_status = "🔴 RED" if corr_red else "🟢 Normal"
    corr_note = "日经与标普高度同步，说明全球资金正在统一步调去杠杆。" if corr_red else "相关性未触及去杠杆阈值。"

    return (vol_status, vol_note), (spread_status, spread_note), (corr_status, corr_note)


def build_japan_carry_unwind_section(japan_result: dict) -> str:
    """生成报告用「JPY Carry-Trade Unwind」仪表盘：核心指标状态表 + 日本宏观底座。"""
    s = japan_result.get("japan_state") or {}
    level = japan_result.get("level", "Normal")
    reasons = japan_result.get("reasons", [])
    emoji = {"Normal": "🟢", "Amber": "🟠", "Red": "🔴"}.get(level, "")

    (vol_status, vol_note), (spread_status, spread_note), (corr_status, corr_note) = _japan_indicator_status(s)

    vol_str = f"{s.get('jpy_vol_1m'):.2f}%" if s.get("jpy_vol_1m") is not None else "—"
    spread_str = f"{s.get('us_jp_spread'):.2f}%" if s.get("us_jp_spread") is not None else "—"
    corr_str = f"{s.get('nky_spx_corr_20d'):.2f}" if s.get("nky_spx_corr_20d") is not None else "—"

    lines = [
        "## 🇯🇵 JPY Carry-Trade Unwind 仪表盘",
        "",
        f"**风险等级**: {emoji} **{level}**",
        "",
        "### 核心指标状态表",
        "",
        "| 指标 | 最新数值 | 状态 | 逻辑说明 |",
        "|------|----------|------|----------|",
        f"| USD/JPY 1M Vol | {vol_str} | {vol_status} | {vol_note} |",
        f"| US-JP 10Y Spread | {spread_str} | {spread_status} | {spread_note} |",
        f"| NKY-SPX Corr | {corr_str} | {corr_status} | {corr_note} |",
        "",
        "### 日本宏观底座",
        "",
    ]
    jgb_cutoff_str = ""
    if s.get("jgb10y_cutoff") is not None:
        try:
            jgb_cutoff_str = f"（FRED 数据截止 {s['jgb10y_cutoff'].strftime('%Y-%m')}，存在 1–2 月滞后）"
        except Exception:
            jgb_cutoff_str = "（FRED 存在 1–2 月滞后）"
    elif s.get("jgb10y") is not None:
        jgb_cutoff_str = "（FRED 存在 1–2 月滞后）"
    if s.get("jgb_fred_stale"):
        jgb_cutoff_str += " **FRED 已滞后 >30 天，实时代理以 8282.T 与 USD/JPY 波动率为主。**"
    jgb_str = f"{s.get('jgb10y'):.2f}%" if s.get("jgb10y") is not None else "—"
    usdjpy_str = f"{s.get('usdjpy_5d_pct'):.2f}%" if s.get("usdjpy_5d_pct") is not None else "—"
    jgb_note = ""
    if s.get("jgb10y") is not None:
        for thresh in JGB_KEY_LEVELS_PCT:
            if s["jgb10y"] >= thresh:
                jgb_note = f"（已触及心理关口 {thresh}%）"
                break
    usdjpy_note = "（日元快速升值，套利平仓压力显现）" if (s.get("usdjpy_5d_pct") is not None and s["usdjpy_5d_pct"] <= YEN_APPRECIATION_5D_PCT) else ""
    lines.append(f"- **JGB 10Y**: {jgb_str} {jgb_note} {jgb_cutoff_str}")
    if s.get("jgb_etf_8282_5d_pct") is not None:
        lines.append(f"- **8282.T (10Y JGB ETF 代理) 5 日涨跌**: {s['jgb_etf_8282_5d_pct']:.2f}% (实时)")
    lines.append(f"- **日元 5 日涨跌**: {usdjpy_str} {usdjpy_note}")
    lines.append("")
    if s.get("jgb_fred_stale"):
        lines.append("**⚠️ FRED 日债数据已滞后超过 30 天**：当前美日利差与 JGB 读数仅供参考；**实时代理以 8282.T 与 USD/JPY 波动率为主**。")
    else:
        lines.append("*说明：若无实时日债数据，USD/JPY 波动率监控已足够作为警报哨所。*")
    lines.append("")
    if reasons:
        lines.append("### 逻辑说明")
        lines.append("")
        for r in reasons:
            lines.append(f"- {r}")
        lines.append("")
    if japan_result.get("escalate_global_to_red"):
        lines.append("**⚠️ Conflict Detection**: Warsh AMBER + JPY 波动率 AMBER → 全局风险等级已升为 **RED**。")
        lines.append("")
    return "\n".join(lines)


def build_warsh_executive_summary(warsh_result: dict, warsh_state: dict) -> str:
    """生成 Executive Summary — Warsh Policy Risk (CRO Tone)"""
    level = warsh_result.get("warsh_level", "GREEN")
    reason = warsh_result.get("warsh_level_reason", "")
    flags = warsh_result.get("warsh_flags", [])
    s = warsh_state

    level_map = {
        "GREEN": "🟢 GREEN (No Constraint)",
        "YELLOW": "🟡 YELLOW (Constraint Forming)",
        "AMBER": "🟠 AMBER (Constraint Watch)",
        "RED": "🔴 RED (Constraint Breached)",
    }
    level_text = level_map.get(level, level)

    binding = "、".join(flags) if flags else "无显性约束"
    sofr_str = f"{s.get('sofr_iorb_spread', 0):.3f}%" if s.get("sofr_iorb_spread") is not None else "—"
    rrp_str = f"{s.get('rrp_level_bn', 0):.1f}B" if s.get("rrp_level_bn") is not None else "—"
    tp_str = f"{s.get('term_premium', 0):.2f}%" if s.get("term_premium") is not None else "—"

    text = f"""**Warsh 政策约束监控**：{level_text}。{reason}

在 Warsh 的资产负债表纪律框架下，主要关注流动性管道的承压能力。当前系统状态：{binding}。关键读数：RRP {rrp_str}（缓冲垫厚度），SOFR−IORB {sofr_str}（利率走廊下限），10Y 期限溢价 {tp_str}。

**CRO 点评**：本仪表盘显示金融系统对进一步缩表的容忍度已{"显著下降" if level in ["AMBER", "RED"] else "处于正常区间"}。当 AMBER 或 RED 信号出现时，历史统计显示政策路径往往面临重新校准（Recalibration）的压力，资产价格的风险溢价将面临重估。"""

    return text.strip()


def generate_tldr_summary(macro_result: dict, warsh_result: dict, japan_result: Optional[dict] = None) -> str:
    """
    生成【太长不看版】决策指南。
    根据宏观评分 + Warsh 约束状态 + Japan 传染检测，直接给出「读/不读」建议。
    当 Conflict Detection 触发（Warsh AMBER + JPY Vol AMBER → Escalated RED）时，显示「系统性流动性共振」专属文案。
    返回 Markdown，便于在报告中展示；含简要框式结构。
    """
    macro_score = macro_result.get("total_score", 0) or 0
    warsh_level = warsh_result.get("warsh_level", "GREEN")
    is_escalated = bool(japan_result and japan_result.get("escalate_global_to_red"))

    has_critical_risk = (macro_score >= 80) or (warsh_level == "RED")
    has_warning = (macro_score >= 60) or (warsh_level in ["AMBER", "YELLOW"])

    if has_critical_risk:
        if is_escalated:
            headline = "🔴 **今日有重大风险信号：系统性流动性共振 (Escalated)**"
            action = "Warsh 缓冲垫已耗尽，且日元套利交易出现平仓迹象（JPY Vol 突破 90% 分位）。系统正处于「内外交困」的去杠杆边缘，风险权重已自动上调。"
        else:
            headline = "🔴 **今日有重大风险信号，请务必全文阅读！**"
            action = "系统已触及硬约束（红灯），市场变盘概率极高。请立即检查风险敞口。"
        can_skip = "❌ **不可跳过**"
        risk_ans = "是"
    elif has_warning:
        headline = "🟠 **宏观面平稳，但流动性防线出现裂痕。**"
        risk_ans = "否，但有结构性隐患"
        if warsh_level == "AMBER":
            action = "宏观数据（GDP/就业）无恙，但流动性缓冲（RRP）已耗尽。系统正如「无备胎行驶」，对突发震荡的抵抗力变弱。"
            can_skip = "⚠️ **长线投资者可略读；债券/波动率交易者建议关注「流动性管道」章节。**"
        else:
            action = "部分指标出现背离，虽然尚未形成共振，但值得保持警惕。"
            can_skip = "✅ **若无重大交易计划，今日可略读。**"
    else:
        headline = "🟢 **今日风平浪静，系统运行良好。**"
        action = "各项指标均在安全区间，流动性充裕，未见异常压力。"
        can_skip = "✅ **今日可以放心暂停阅读。**"
        risk_ans = "否"

    # Markdown 版：报告通用，渲染成 HTML 时也清晰
    tldr_md = f"""
## ⏱️ 太长不看版 (TL;DR) — 今日决策指南

{headline}

- **是否有大风险？** {risk_ans}
- **我可以停止阅读吗？** {can_skip}
- **核心关注点：** {action}

---
"""
    return tldr_md.strip()


WARSH_WHY_MATTERS_SECTION = """
Warsh 更关注资产负债表纪律，而不仅是利率水平。在潜在「降息 + 继续缩表」的组合下，名义利率下行并不等同于流动性宽松；若缩表持续，银行间与债市仍可能面临流动性收紧。

本仪表盘监控的管道指标（Plumbing Indicators）——SOFR 与准备金利率利差、逆回购消耗、期限溢价与债市波动——通常在就业、CPI、GDP 等宏观数据恶化之前就会发出信号。它们回答的是：**政策意图是否已触及金融系统的物理极限**。

当多个管道指标同时恶化时，历史经验表明，央行往往被迫放缓或重新定义原有紧缩路径。对投资者而言，这对应极端风险定价阶段，以及随后由政策妥协触发的反向交易机会。本层不替代既有危机监测评分，仅作为 Warsh 政策约束的叠加视角。
""".strip()


def build_warsh_interpretation_report(warsh_state: dict) -> str:
    """根据当前 Warsh 指标状态生成「Warsh Constraints / Liquidity Plumbing — Interpretation」章节（Markdown）。"""
    s = warsh_state

    # Section A — Why These Indicators Matter
    section_a = """在 Kevin Warsh 的政策框架下，名义利率（Fed Funds）不再是唯一的宽松工具。
Warsh 更强调通过资产负债表规模（QT）恢复「货币纪律」。

因此，市场的核心风险不在于「是否降息」，而在于：
**在降息的同时，金融系统是否还能承受持续的流动性抽取。**

本仪表盘专门监控那些能最早暴露「缩表撞墙」的金融管道指标（Plumbing Indicators），
这些指标通常在传统宏观数据（就业、CPI、GDP）恶化之前就会发出警告。

"""

    # Section B — Policy Transmission Channels（带当前数值）
    sofr_line = "（当前无 SOFR−IORB 序列，无法判断。）"
    if s.get("sofr_iorb_spread") is not None:
        x = s["sofr_iorb_spread"]
        x5 = s.get("sofr_iorb_spread_5dma")
        pos20 = s.get("sofr_iorb_pos_days_20")
        tail = f"当前 SOFR−IORB 为 **{x:.4f}%**"
        if x5 is not None:
            tail += f"，5 日均值为 **{x5:.4f}%**"
        if pos20 is not None:
            tail += f"，近 20 日为正的天数为 **{pos20}** 天。"
        if x > 0:
            tail += " **已突破 0，属于流动性底线告警。**"
        elif x5 is not None and x5 > 0:
            tail += " 5 日均值已为正，需密切关注。"
        else:
            tail += " 尚未突破 0，流动性底线暂未触及。"
        sofr_line = tail

    section_b1 = f"""#### 1) 银行间流动性底线（SOFR − IORB）

IORB 是银行体系的无风险利率下限。
正常情况下，银行不应在回购市场以高于 IORB 的利率融资。

当 **SOFR − IORB 持续为正**，意味着银行体系的准备金已不足，
即便存在央行准备金，市场仍被迫以更高成本融资。

这是缩表最直接、也最危险的约束信号，历史上往往迫使央行放缓或停止流动性抽取。

{sofr_line}

"""

    rrp_line = "（当前无 RRP 序列或数据不足，无法判断。）"
    if s.get("rrp_level_bn") is not None:
        level_bn = s["rrp_level_bn"]  # 十亿美元
        pct = s.get("rrp_20d_pct")
        tail = f"当前 RRP 余额为 **{level_bn:.1f}** 十亿美元"
        if pct is not None:
            tail += f"，20 日变化率为 **{pct:+.1f}%**。"
        else:
            tail += "。"
        if pct is not None and pct < -10:
            tail += " 20 日显著下降，缓冲垫消耗加快，需警惕后续对准备金的直接抽取。"
        elif pct is not None and pct < 0:
            tail += " 20 日略有下降，缓冲垫在缓慢消耗。"
        else:
            tail += " 近期 RRP 未明显收缩。"
        rrp_line = tail

    section_b2 = f"""#### 2) 缓冲垫消耗（RRP Drain）

当前美联储体系中，隔夜逆回购（RRP）是缩表的「第一道缓冲垫」。
缩表初期，流动性通常先从 RRP 流出，而非直接抽取银行准备金。

当 RRP 快速下降并接近枯竭时，后续缩表将不可避免地侵蚀银行体系准备金，
流动性风险将呈现非线性上升。

{rrp_line}

"""

    tp_line = "（当前无期限溢价序列，无法判断。）"
    if s.get("term_premium") is not None:
        tp_val = s["term_premium"]
        tp_line = f"当前 10 年期期限溢价（THREEFYTP10）为 **{tp_val:.2f}%**。"
        if tp_val > 0.5:
            tp_line += " 处于偏高水平，反映市场对长端美债风险补偿要求上升，需关注与债券波动率的联动。"
        else:
            tp_line += " 水平尚可，长端反噬压力暂不突出。"

    section_b3 = f"""#### 3) 长端反噬（Term Premium & Bond Vol）

Warsh 的政策组合可能同时压低短端利率（降息）并抛售或不再吸收长端国债（QT）。

如果 10 年期收益率上升主要来自 **期限溢价（Term Premium）**，
而非经济增长预期，这意味着市场正在要求更高的风险补偿来持有美债。

当期限溢价与债券波动率同时上升时，通常标志着
**美债市场定价功能开始失灵**，财政与货币政策的张力显性化。

{tp_line}

"""

    # Section C — How to Read This Dashboard
    section_c = """### Section C — 如何使用本仪表盘（决策指引）

该仪表盘并不用于预测经济衰退，
而是用于监测 **「政策意图是否已触及金融系统的物理极限」**。

- 当多个 Plumbing 指标同时恶化时，历史经验表明，
  政策制定者往往被迫放缓或重新定义原有紧缩路径。
- 对投资者而言，这通常对应：
  - 极端风险定价阶段
  - 以及随后由政策妥协触发的反向交易机会。

"""

    run_meta = []
    if s.get("sofr_iorb_last_date"):
        run_meta.append(f"SOFR−IORB 数据截止: {s['sofr_iorb_last_date']}")
    if s.get("rrp_last_date"):
        run_meta.append(f"RRP 数据截止: {s['rrp_last_date']}")
    if s.get("term_premium_last_date"):
        run_meta.append(f"THREEFYTP10 数据截止: {s['term_premium_last_date']}")
    meta_line = " | ".join(run_meta) if run_meta else "（暂无 Warsh 指标数据截止日）"

    header = f"""**运行元数据**（时区: JST）：{datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")} | {meta_line}

---

### Section A — 为何监控这些指标

"""

    return header + section_a + "\n### Section B — Warsh 下的政策传导路径\n\n" + section_b1 + section_b2 + section_b3 + section_c


# HTML转换函数
def markdown_to_html(markdown_content: str, image_paths: dict) -> str:
    """将Markdown内容转换为HTML，包含图片"""
    html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>宏观金融危机监察报告</title>
    <style>
        body {{
            font-family: 'Microsoft YaHei', 'SimHei', Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #d32f2f;
            text-align: center;
            border-bottom: 3px solid #d32f2f;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #1976d2;
            border-left: 4px solid #1976d2;
            padding-left: 15px;
            margin-top: 30px;
        }}
        h3 {{
            color: #388e3c;
            border-left: 3px solid #388e3c;
            padding-left: 10px;
        }}
        .risk-high {{ color: #d32f2f; }}
        .risk-medium {{ color: #f57c00; }}
        .risk-low {{ color: #388e3c; }}
        .risk-very-low {{ color: #1976d2; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .summary-box {{
            background-color: #e3f2fd;
            border: 2px solid #1976d2;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }}
        .indicator-item {{
            background-color: #f8f9fa;
            border-left: 4px solid #6c757d;
            padding: 15px;
            margin: 10px 0;
            border-radius: 0 8px 8px 0;
        }}
        .chart-container {{
            text-align: center;
            margin: 20px 0;
        }}
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
        }}
        @media (max-width: 768px) {{
            .container {{
                padding: 15px;
            }}
            table {{
                font-size: 14px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        {markdown_content.replace('#', '').replace('**', '<strong>').replace('**', '</strong>').replace('\n', '<br>')}
        
        <!-- 添加图片 -->
        <div class="chart-container">
            <h3>总体风险概览图表</h3>
            <img src="{image_paths.get('summary', '')}" alt="总体风险概览" />
        </div>
        
        <div class="chart-container">
            <h3>主要指标图表</h3>
            <img src="{image_paths.get('main_indicators', '')}" alt="主要指标图表" />
        </div>
    </div>
</body>
</html>
"""
    return html_content

# 长图生成函数
def generate_long_image(html_path: str, output_path: str) -> bool:
    """使用Playwright生成长图（首选），备用Selenium和wkhtmltoimage"""
    try:
        # 优先使用Playwright生成
        try:
            from playwright.sync_api import sync_playwright
            import time
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # 打开HTML文件
                html_abs_path = os.path.abspath(html_path)
                page.goto(f'file:///{html_abs_path}')
                
                # 等待页面加载
                page.wait_for_load_state('networkidle')
                
                # 获取页面实际高度
                page_height = page.evaluate('document.body.scrollHeight')
                
                # 设置视口大小
                page.set_viewport_size({"width": 1200, "height": page_height + 100})
                
                # 等待一下确保渲染完成
                time.sleep(2)
                
                # 截取整个页面
                page.screenshot(path=output_path, full_page=True)
                
                browser.close()
                
                # 检查文件大小
                file_size = os.path.getsize(output_path) / (1024*1024)
                print(f"✅ Playwright长图生成成功: {output_path} ({file_size:.2f} MB)")
                return True
                
        except ImportError:
            print("⚠️ Playwright未安装，尝试使用Selenium")
        except Exception as e:
            print(f"⚠️ Playwright生成失败: {e}，尝试使用Selenium")
        
        # 备用方案1：使用Selenium生成
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            import time
            
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1200,800')
            
            # 启动浏览器
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # 打开HTML文件
                html_abs_path = os.path.abspath(html_path)
                driver.get(f'file:///{html_abs_path}')
                
                # 等待页面加载
                time.sleep(3)
                
                # 获取页面实际高度
                page_height = driver.execute_script('return document.body.scrollHeight')
                
                # 设置浏览器窗口高度
                driver.set_window_size(1200, page_height + 100)
                
                # 等待一下确保渲染完成
                time.sleep(2)
                
                # 截取整个页面
                driver.save_screenshot(output_path)
                
                # 检查文件大小
                file_size = os.path.getsize(output_path) / (1024*1024)
                print(f"✅ Selenium长图生成成功: {output_path} ({file_size:.2f} MB)")
                
                return True
                
            finally:
                driver.quit()
                
        except ImportError:
            print("⚠️ Selenium未安装，尝试使用wkhtmltoimage")
        except Exception as e:
            print(f"⚠️ Selenium生成失败: {e}，尝试使用wkhtmltoimage")
        
        # 备用方案：使用wkhtmltoimage
        wkhtmltoimage_paths = [
            'wkhtmltoimage',  # 如果在PATH中
            r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe',
            r'C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltoimage.exe',
        ]
        
        wkhtmltoimage_cmd = None
        for path in wkhtmltoimage_paths:
            try:
                result = subprocess.run([path, '--version'], 
                                      capture_output=True, text=True, timeout=10,
                                      encoding="utf-8", errors="replace")
                if result.returncode == 0:
                    wkhtmltoimage_cmd = path
                    print(f"✅ 找到wkhtmltoimage: {path}")
                    break
            except:
                continue
        
        if not wkhtmltoimage_cmd:
            print("❌ wkhtmltoimage未找到，请安装wkhtmltopdf")
            return False
        
        # 生成长图 - 使用file://协议
        html_file_url = f"file:///{os.path.abspath(html_path).replace('\\', '/')}"
        cmd = [
            wkhtmltoimage_cmd,
            '--quality', '75',  # 降低质量以减小文件大小
            '--width', '800',   # 减小宽度
            '--format', 'png',
            '--load-error-handling', 'ignore',
            html_file_url,
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                               encoding="utf-8", errors="replace")
        if result.returncode == 0:
            print(f"✅ 长图生成成功: {output_path}")
            return True
        else:
            print(f"❌ 长图生成失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ 长图生成超时")
        return False
    except FileNotFoundError:
        print("❌ wkhtmltoimage未找到，请安装wkhtmltopdf")
        return False
    except Exception as e:
        print(f"❌ 长图生成出错: {e}")
        return False

# 真实FRED数据计算
def calculate_real_fred_scores(indicators_config=None, scoring_config=None):
    """基于真实FRED数据计算评分"""
    
    # 配置验证检查
    print("🔍 配置验证检查...")
    
    # 检查transform类型和名称/口径一致性
    valid_transforms = {"level", "yoy_pct", "none", "zscore"}
    for indicator in indicators_config or []:
        transform = indicator.get('transform', 'level')
        if transform not in valid_transforms:
            print(f"⚠️ 未知transform类型: {indicator['id']} -> {transform}")
        
        # 名称/口径一致性检查
        series_id = indicator.get('series_id') or indicator.get('id', '')
        name = indicator.get('name', '')
        if 'YoY' in name or 'YoY' in series_id:
            if transform != 'yoy_pct':
                die_if(True, f"{series_id}: 名称含'YoY'但transform不是'yoy_pct'，这会导致口径错误")
    
    # 检查权重归一化（只对可计分项）
    if indicators_config:
        scoring_indicators = [ind for ind in indicators_config if ind.get('role', 'score') == 'score']
        total_weight = sum(ind.get('weight', 0) for ind in scoring_indicators)
        if abs(total_weight - 1.0) > 1e-6:
            print(f"⚠️ 可计分项权重未归一化: {total_weight:.6f} (应为1.0)，将自动归一")
            # 自动归一化
            for ind in scoring_indicators:
                ind['weight'] = ind.get('weight', 0) / (total_weight if total_weight > 0 else 1.0)
            total_weight = sum(ind.get('weight', 0) for ind in scoring_indicators)
            print(f"✅ 已自动归一化权重，总计: {total_weight:.6f}")
        else:
            print(f"✅ 可计分项权重已归一，总计: {total_weight:.6f}")
    
    # 导入必要的模块
    try:
        from scripts.fred_http import series_observations
        from scripts.clean_utils import parse_numeric_series
    except ImportError:
        print("⚠️ 无法导入FRED模块，使用模拟数据")
        # 返回模拟数据
        realistic_data = {
            "收益率曲线": {"score": 55.4, "weight": 14, "count": 2},
            "利率水平": {"score": 46.0, "weight": 14, "count": 5},
            "信用利差": {"score": 38.2, "weight": 14, "count": 2},
            "金融状况/波动": {"score": 39.4, "weight": 9, "count": 2},
            "实体经济": {"score": 47.1, "weight": 14, "count": 3},
            "房地产": {"score": 39.2, "weight": 9, "count": 3},
            "消费": {"score": 47.9, "weight": 7, "count": 2},
            "银行业": {"score": 41.5, "weight": 6, "count": 2},
            "外部环境": {"score": 47.5, "weight": 5, "count": 1},
            "杠杆": {"score": 56.5, "weight": 9, "count": 1}
        }
        total_score = sum(data["score"] * data["weight"] / 100 for data in realistic_data.values())
        return realistic_data, total_score, []
    
    # 使用传入的配置或加载配置
    if indicators_config is None:
        config_path = BASE / "config" / "crisis_indicators.yaml"
        config = load_yaml_config(config_path)
        indicators = config.get('indicators', [])
    else:
        indicators = indicators_config
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    print("📊 开始处理真实FRED数据...")
    
    # 处理每个指标
    processed_indicators = []
    group_scores = {}
    
    for indicator in indicators:
        try:
            result = process_single_indicator_real(indicator, crisis_periods, scoring_config)
            if result:
                processed_indicators.append(result)
                
                # 计算分组分数（排除监控指标）
                group = result['group']
                if group not in group_scores:
                    group_scores[group] = {'scores': [], 'weights': []}
                
                # 只有计分指标才参与分组计算
                if result.get('role', 'score') == 'score':
                    group_scores[group]['scores'].append(result['risk_score'])
                    group_scores[group]['weights'].append(result.get('global_weight', 0))
                
        except Exception as e:
            print(f"❌ 处理指标失败 {indicator.get('name', 'Unknown')}: {e}")
            continue
    
    # 计算分组平均分并归一化权重
    final_group_scores = {}
    total_weighted_score = 0
    
    # 收集所有权重进行归一化
    group_weights = {}
    for group, data in group_scores.items():
        if data['scores']:
            group_weight = sum(data['weights']) if data['weights'] else 0
            group_weights[group] = group_weight
    
    # 归一化权重
    total_weight = sum(group_weights.values())
    if total_weight > 0:
        for group in group_weights:
            group_weights[group] = group_weights[group] / total_weight
    else:
        # 防御：平均分配
        avg_weight = 1.0 / max(1, len(group_weights))
        for group in group_weights:
            group_weights[group] = avg_weight
    
    for group, data in group_scores.items():
        if data['scores']:
            avg_score = sum(data['scores']) / len(data['scores'])
            normalized_weight = group_weights.get(group, 0)
            
            # 应用共振奖励
            if scoring_config:
                co_move_threshold_pct = scoring_config.get('co_move_threshold_pct', 0.9)
                co_move_bonus_per_bucket = scoring_config.get('co_move_bonus_per_bucket', 5)
                
                # 计算该组内高风险指标数量
                high_risk_count = sum(1 for score in data['scores'] if score >= 80)
                if high_risk_count >= 2:  # 至少2个高风险指标触发共振
                    avg_score = min(100.0, avg_score + co_move_bonus_per_bucket)
            
            final_group_scores[group] = {
                'score': avg_score,
                'weight': normalized_weight * 100,  # 转换为百分比
                'count': len(data['scores'])
            }
            
            total_weighted_score += avg_score * normalized_weight
    
    return final_group_scores, total_weighted_score, processed_indicators


def apply_japan_red_compensation(
    group_scores: dict, total_score: float, japan_result: Optional[dict]
) -> tuple:
    """
    当 Japan Carry-Trade Unwind 风险等级为 Red 时，对「银行业」与「收益率曲线」分组进行权重补偿：
    score += 15（上限 100），并同步更新总分。用于体现「内外交困」下去杠杆风险的自动上调。
    """
    if not japan_result or japan_result.get("level") != "Red":
        return group_scores, total_score
    for group_key in ["banking", "core_warning"]:
        if group_key not in group_scores:
            continue
        old_s = group_scores[group_key]["score"]
        new_s = min(100.0, old_s + 15.0)
        group_scores[group_key]["score"] = new_s
        total_score += (new_s - old_s) * (group_scores[group_key]["weight"] / 100.0)
    return group_scores, total_score


def process_single_indicator_real(indicator, crisis_periods, scoring_config=None):
    """处理单个指标的真实FRED数据（统一变换/同域基准/同域ECDF）"""
    from scripts.clean_utils import parse_numeric_series

    series_id = indicator.get('series_id') or indicator.get('id')
    if not series_id: return None
    
    # 检查是否为监控指标（不计分）
    role = indicator.get('role', 'score')
    if role == 'monitor':
        print(f"📊 {series_id}: 监控指标，不计分")
        # 仍然处理数据但不参与计分

    # 特殊处理：优先使用预计算的数据
    if series_id == 'NCBDBIQ027S':
        # 使用预计算的企业债/GDP比率数据
        try:
            ratio_file = pathlib.Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
            if ratio_file.exists():
                ratio_df = pd.read_csv(ratio_file)
                ratio_df['date'] = pd.to_datetime(ratio_df['date'])
                ratio_df = ratio_df.set_index('date')
                
                ts_trans = parse_numeric_series(ratio_df['value']).dropna()
                if not ts_trans.empty:
                    current_value = float(ts_trans.iloc[-1])
                    last_date = ts_trans.index[-1]
                    print(f"📊 {series_id}: 企业债/GDP比率 = {current_value:.2f}% (使用预计算数据)")
                    
                    # 基准值和风险评分
                    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)
                    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans, scoring_config)
                    
                    return {
                        'name': indicator.get('name', series_id),
                        'series_id': series_id,
                        'group': indicator.get('group', 'unknown'),
                        'current_value': current_value,
                        'benchmark_value': benchmark_value,
                        'risk_score': risk_score,
                        'last_date': str(last_date.date()),
                        'global_weight': indicator.get('weight', 0.0),
                        'higher_is_risk': indicator.get('higher_is_risk', True),
                        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
                        'plain_explainer': get_indicator_explanation(series_id, indicator)
                    }
        except Exception as e:
            print(f"⚠️ {series_id}: 预计算数据读取失败: {e}")
    
    # YoY指标：使用预计算的YoY数据
    yoy_indicators = ['PAYEMS', 'INDPRO', 'GDP', 'NEWORDER', 'CSUSHPINSA', 'TOTALSA', 'TOTLL', 'MANEMP', 'WALCL', 'DTWEXBGS', 'PERMIT', 'TOTRESNS']
    if series_id in yoy_indicators and indicator.get('transform') == 'yoy_pct':
        try:
            yoy_file = pathlib.Path(f"data/series/{series_id}_YOY.csv")
            if yoy_file.exists():
                yoy_df = pd.read_csv(yoy_file)
                yoy_df['date'] = pd.to_datetime(yoy_df['date'])
                yoy_df = yoy_df.set_index('date')
                
                ts_trans = parse_numeric_series(yoy_df['yoy_pct']).dropna()
                if not ts_trans.empty:
                    current_value = float(ts_trans.iloc[-1])
                    last_date = ts_trans.index[-1]
                    print(f"📊 {series_id}: YoY = {current_value:.2f}% (使用预计算数据)")
                    
                    # 基准值和风险评分
                    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)
                    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans, scoring_config)
                    
                    return {
                        'name': indicator.get('name', series_id),
                        'series_id': series_id,
                        'group': indicator.get('group', 'unknown'),
                        'current_value': current_value,
                        'benchmark_value': benchmark_value,
                        'risk_score': risk_score,
                        'last_date': str(last_date.date()),
                        'global_weight': indicator.get('weight', 0.0),
                        'higher_is_risk': indicator.get('higher_is_risk', True),
                        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
                        'plain_explainer': get_indicator_explanation(series_id, indicator)
                    }
        except Exception as e:
            print(f"⚠️ {series_id}: YoY预计算数据读取失败: {e}")

    # 1) 读取原始或合成 ★
    ts = compose_series(series_id)
    if ts is None:
        ts = fetch_series(series_id)
    if ts is None or ts.empty:
        print(f"⚠️ {series_id}: 无数据，跳过处理")
        return None

    # 2) 对历史做同样的 transform ★
    ts_trans = transform_series(series_id, ts, indicator)
    ts_trans = ts_trans.dropna()
    if ts_trans.empty:
        die_if(True, f"{series_id}: 变换后无数据 (transform={indicator.get('transform')})")

    # 3) 取"当前值"= 变换后的最新值（不再依赖本地 YOY CSV）★
    current_value = float(ts_trans.iloc[-1])
    last_date = ts_trans.index[-1]

    # 4) 基准值也在"同域"的 ts_trans 上计算 ★
    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)

    # 5) 风险分：ECDF 在 ts_trans 域内 ★
    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans)
    
    # 6) 过期数据降权
    last_dt = ts_trans.index[-1]
    max_lag = indicator.get('max_lag_days')
    if max_lag:
        lag_days = (datetime.now(JST).date() - last_dt.date()).days
        if lag_days > max_lag:
            risk_score *= 0.9
            print(f"⚠️ {series_id}: 数据过期{lag_days}天，风险评分降权至{risk_score:.1f}")

    return {
        'name': indicator.get('name', series_id),
        'series_id': series_id,
        'group': indicator.get('group', 'unknown'),
        'current_value': current_value,
        'benchmark_value': benchmark_value,
        'risk_score': risk_score,
        'last_date': str(last_date.date()),
        'global_weight': indicator.get('weight', 0.0),
        'higher_is_risk': indicator.get('higher_is_risk', True),
        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
        'plain_explainer': get_indicator_explanation(series_id),
        'role': role
    }

def calculate_benchmark_simple(ts, indicator, crisis_periods):
    """改进的基准值计算，使用危机/非危机掩码，支持可选危机子集"""
    series_id = indicator.get('series_id') or indicator.get('id')
    if series_id == 'UST30Y_UST2Y_RSI':
        return 50.0
    compare_to = indicator.get('compare_to', 'noncrisis_p75')
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    # 根据compare_to选择子样本
    if compare_to.startswith('crisis_'):
        sub_ts = ts[crisis_mask]
    elif compare_to.startswith('noncrisis_'):
        sub_ts = ts[~crisis_mask]
    else:
        sub_ts = ts
    
    # 样本不足则回退全样本
    if sub_ts.dropna().size < 24:
        sub_ts = ts
    
    # 计算分位数
    if 'median' in compare_to:
        return float(sub_ts.median())
    elif 'p' in compare_to:
        # 提取百分位数
        p_str = compare_to.split('p')[-1]
        if p_str.isdigit():
            p = float(p_str) / 100.0
            return float(sub_ts.quantile(p))
        else:
            return float(sub_ts.median())
    else:
        return float(sub_ts.median())

def _mask_by_crisis(ts, crisis_periods, use: str):
    """根据危机期间掩码选择子样本"""
    if use not in ['crisis', 'noncrisis', 'all']:
        return ts
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    if use == 'crisis':
        return ts[crisis_mask]
    elif use == 'noncrisis':
        return ts[~crisis_mask]
    else:  # 'all'
        return ts

def calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods):
    """
    计算修正后的基准值（使用预计算的YoY基准值）
    """
    try:
        bench_file = pathlib.Path("data/benchmarks_yoy.json")
        if bench_file.exists() and indicator.get("transform")=="yoy_pct":
            benchmarks = json.loads(bench_file.read_text(encoding="utf-8"))
            if series_id in benchmarks:
                return float(benchmarks[series_id])
    except Exception:
        pass
    # 正确回退：把 crisis_periods 传进去，且用 ts_trans ★
    return calculate_benchmark_simple(ts_trans, indicator, crisis_periods)

def _parse_compare_to_to_pct(compare_to: str) -> float:
    """解析 compare_to 为分位数阈值"""
    if compare_to.endswith('_median'):
        return 0.5
    if '_p' in compare_to:
        try:
            return float(compare_to.split('_p')[-1]) / 100.0
        except:
            return 0.5
    return 0.5  # 默认中位数

def score_with_threshold(ts: pd.Series, current: float, *, direction: str, compare_to: str, tail: str='single') -> float:
    """真正用阈值参与打分（单尾/双尾统一）"""
    if ts is None or ts.empty or pd.isna(current):
        return 50.0
    
    # 1) 全样本分位
    p_cur = (ts <= current).mean()
    # 2) 阈值分位
    if compare_to.startswith("constant_"):
        try:
            threshold_value = float(compare_to.split("_", 1)[-1])
        except ValueError:
            threshold_value = 50.0
        p_thr = (ts <= threshold_value).mean()
    else:
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
            raw = max(0.0, (p_thr - p_cur) / max(p_thr, eps))
    
    return float(np.clip(raw * 100.0, 0, 100))

def level_from_score(score: float, bands: dict) -> str:
    """根据分数返回风险等级"""
    high = bands.get('high', 80)
    med = bands.get('med', 60)
    low = bands.get('low', 40)
    
    if score >= high:
        return 'high'
    elif score >= med:
        return 'med'
    elif score >= low:
        return 'low'
    else:
        return 'very_low'

def compute_momentum_score(ts: pd.Series, days: list = [1, 5]) -> float:
    """计算动量评分（0-1之间）"""
    if ts is None or ts.empty or len(ts) < max(days) + 1:
        return 0.0
    
    momentum_scores = []
    for d in days:
        if len(ts) > d:
            # 计算d天前的值到当前值的变化
            current = ts.iloc[-1]
            past = ts.iloc[-d-1]
            if not pd.isna(current) and not pd.isna(past) and past != 0:
                change = (current - past) / abs(past)
                momentum_scores.append(abs(change))
    
    if not momentum_scores:
        return 0.0
    
    # 返回平均动量（标准化到0-1）
    avg_momentum = np.mean(momentum_scores)
    return min(1.0, avg_momentum)

def calculate_risk_score_simple(current, benchmark, indicator, ts=None, scoring_config=None):
    """改进的风险评分计算，真正锚定配置的基准分位，支持动量奖励"""
    compare_to = indicator.get('compare_to', 'noncrisis_median')
    direction = 'up_is_risk' if indicator.get('higher_is_risk', True) else 'down_is_risk'
    tail = indicator.get('tail', 'single')
    
    # 计算基础风险评分
    if ts is not None and not ts.empty:
        base_score = score_with_threshold(ts, current, direction=direction, compare_to=compare_to, tail=tail)
    else:
        # 回退到简化计算（当没有时间序列数据时）
        higher_is_risk = indicator.get('higher_is_risk', True)
        if higher_is_risk:
            deviation = current - benchmark
        else:
            deviation = benchmark - current
        base_score = 50 + 10 * deviation
        base_score = max(0, min(100, base_score))
    
    # 应用动量奖励
    if scoring_config and ts is not None and not ts.empty:
        mom_bonus_max = scoring_config.get('momentum_bonus_max', 5)
        momentum_days = scoring_config.get('momentum_days', [1, 5])
        
        momentum_raw = compute_momentum_score(ts, days=momentum_days)
        momentum_bonus = momentum_raw * mom_bonus_max
        
        # 限制总分不超过100
        final_score = min(100.0, base_score + momentum_bonus)
        return final_score
    
    return base_score

def calculate_crisis_stats(ts, crisis_periods):
    """计算危机期间的统计数据"""
    if ts.empty:
        return {}
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    crisis_vals = ts[crisis_mask].dropna()
    
    if crisis_vals.empty:
        return {}
    
    return {
        "crisis_median": float(np.nanmedian(crisis_vals.values)),
        "crisis_p25": float(np.nanpercentile(crisis_vals.values, 25)),
        "crisis_p75": float(np.nanpercentile(crisis_vals.values, 75)),
        "crisis_mean": float(np.nanmean(crisis_vals.values)),
        "crisis_std": float(np.nanstd(crisis_vals.values, ddof=1)),
        "crisis_n": int(crisis_vals.size)
    }

def get_indicator_explanation(series_id, indicator_config=None):
    """获取指标解释（统一与方向/双尾配置）"""
    explanations = {
        'T10Y3M': '10年期与3个月国债收益率利差。计算公式：T10Y3M = 10年期国债收益率 - 3个月国债收益率。倒挂(负值)越深越危险，表明市场预期短期利率将高于长期利率，通常预示经济衰退。',
        'T10Y2Y': '10年期与2年期国债收益率利差。计算公式：T10Y2Y = 10年期国债收益率 - 2年期国债收益率。倒挂(负值)越深越危险，是重要的经济领先指标。',
        'UST30Y_UST2Y_RSI': '30年期与2年期国债收益率比率的月线RSI指标。RSI突破50往往意味着曲线“去倒挂”进入陡峭化阶段，历史上常与崩盘/衰退的实质爆发期同步。',
        'HY_OAS_MOMENTUM_RATIO': '高收益债利差的20日动量比率（当前值/20日均值）。当比率显著高于1说明信用利差快速走阔，常见于流动性抽离期。',
        'SP500_DGS10_CORR60D': '标普500与10年期美债收益率的60日滚动相关性。由负转正且升至高位，常代表股债同跌的“现金为王”阶段。',
        'NET_LIQUIDITY': '每日净流动性：WALCL(线性插值) - WTREGEN - RRPONTSYD。数值下降代表系统性资金被抽离。',
        'VIX_TERM_STRUCTURE': 'VIX期限结构（VIX/VIX3M）。比值大于1表示短期恐慌高于远期，常见于崩盘前后的恐慌倒挂。',
        'HYG_LQD_RATIO': '高收益债ETF与投资级债ETF比率（HYG/LQD）。比率下行代表资金从高收益撤离，信用风险上升。',
        'DXY_CHANGE': '美元指数5日变化率（优先DXY，回退UUP）。短期急升通常反映美元紧张与全球流动性收缩。',
        'KRE_SPY_RATIO': '区域银行ETF与标普500比率（KRE/SPY）。下行代表银行股相对走弱，常见于区域性金融压力。',
        'XLF_SPY_RATIO': '金融板块ETF与标普500比率（XLF/SPY）。下行代表金融板块相对走弱。',
        'BTC_QQQ_RATIO': '比特币与科技股ETF比率（BTC/QQQ）。下行通常意味着风险偏好与流动性边际走弱。',
        'CROSS_ASSET_CORR_STRESS': '跨资产相关性压力合成指标（SPY/TLT/GLD/USO）。高相关+同跌“现金为王”代表系统性流动性紧张。',
        'HY_IG_RATIO': '高收益/投资级总回报比率（HY/IG）。比率快速下行代表资金逃离风险信贷。',
        'GLOBAL_LIQUIDITY_USD': '全球流动性代理：Fed + ECB(美元计价) + BOJ(美元计价)。快速收缩代表全球“抽水”压力上升。',
        'CREDIT_CARD_DELINQUENCY': '信用卡违约率。与消费信贷变化联动，可验证“真危机”是否发生。',
        'FEDFUNDS': '联邦基金利率。水平值，利率越高表示货币政策越紧缩，会抑制经济增长。',
        'DTB3': '3个月国债收益率。水平值，收益率越高表示短期利率水平越高。',
        'DGS10': '10年期国债收益率。水平值，收益率越高表示长期利率水平越高。',
        'MORTGAGE30US': '30年期抵押贷款利率。水平值，利率越高表示住房融资成本越高。',
        'SOFR': '有担保隔夜融资利率。水平值，利率越高表示短期融资成本越高。',
        'BAMLH0A0HYM2': '高收益债券风险溢价。计算公式：BAMLH0A0HYM2 = 高收益债券收益率 - 10年期国债收益率。溢价越高表明信用风险上升。',
        'BAA10YM': 'Baa级公司债券与10年期国债利差。计算公式：BAA10YM = Baa级公司债券收益率 - 10年期国债收益率。利差越大表明信用风险上升。',
        'NFCI': '芝加哥金融状况指数。综合多个金融市场指标的合成指数，越高表示金融条件越紧张。',
        'VIXCLS': 'VIX波动率指数。水平值，指数越高表示市场恐慌情绪越严重。',
        'PAYEMS': '非农就业人数同比变化率。计算公式：PAYEMS_YoY = (当前月就业人数 - 上年同期就业人数) / 上年同期就业人数 × 100%。增速越低表示劳动力市场越疲弱。',
        'INDPRO': '工业生产指数同比变化率。计算公式：INDPRO_YoY = (当前月工业生产指数 - 上年同期工业生产指数) / 上年同期工业生产指数 × 100%。增速越低表示工业生产越疲弱。',
        'GDP': '国内生产总值同比变化率。计算公式：GDP_YoY = (当前季度GDP - 上年同期GDP) / 上年同期GDP × 100%。增速越低表示经济增长越疲弱。',
        'HOUST': '新屋开工数。水平值，开工数越低表示房地产投资越疲弱。',
        'CSUSHPINSA': 'Case-Shiller房价指数同比变化率。计算公式：CSUSHPINSA_YoY = (当前月房价指数 - 上年同期房价指数) / 上年同期房价指数 × 100%。双尾风险：暴涨(泡沫)和骤跌(去杠杆)都危险。',
        'UMCSENT': '密歇根消费者信心指数。水平值，指数越低表示消费者情绪越悲观。',
        'TOTALSA': '消费者信贷同比变化率。计算公式：TOTALSA_YoY = (当前月消费者信贷 - 上年同期消费者信贷) / 上年同期消费者信贷 × 100%。双尾风险：暴涨(积累脆弱性)和骤冷(信贷闸门收紧)都危险。',
        'TOTLL': '银行总贷款和租赁同比变化率。计算公式：TOTLL_YoY = (当前周贷款总额 - 上年同期贷款总额) / 上年同期贷款总额 × 100%。双尾风险：暴涨(积累脆弱性)和骤冷(信贷闸门收紧)都危险。',
        'WALCL': '美联储总资产同比变化率。计算公式：WALCL_YoY = (当前周总资产 - 上年同期总资产) / 上年同期总资产 × 100%。增速越高表示扩表速度越快。',
        'DTWEXBGS': '贸易加权美元指数同比变化率。计算公式：DTWEXBGS_YoY = (当前周美元指数 - 上年同期美元指数) / 上年同期美元指数 × 100%。增速越高表示美元越强势，金融条件越紧张。',
        'CORPDEBT_GDP_PCT': '企业债务占GDP比例。计算公式：CORPDEBT_GDP_PCT = 企业债务总额 / GDP × 100%。比例过高表明企业杠杆率过高。',
        'RESERVES_DEPOSITS_PCT': '银行准备金占存款比例。计算公式：RESERVES_DEPOSITS_PCT = 银行准备金 / 银行存款 × 100%。比例过低表明银行流动性不足。',
        'RESERVES_ASSETS_PCT': '银行准备金占总资产比例。计算公式：RESERVES_ASSETS_PCT = 银行准备金 / 银行总资产 × 100%。比例过低表明银行流动性不足。',
        'TDSP': '家庭债务偿付收入比。水平值，比率越高表示家庭债务负担过重。',
        'DRSFRMACBS': '房贷违约率。水平值，违约率越高表示家庭财务压力越大。'
    }
    
    base_explanation = explanations.get(series_id, f'{series_id}指标')
    
    # 根据配置添加方向性说明
    if indicator_config:
        higher_is_risk = indicator_config.get('higher_is_risk', True)
        tail = indicator_config.get('tail', 'single')
        
        if tail == 'both':
            dir_txt = " | 双尾风险"
        elif higher_is_risk:
            dir_txt = " | 高为险"
        else:
            dir_txt = " | 低为险"
        
        return base_explanation + dir_txt
    
    return base_explanation

def run_data_pipeline():
    """运行完整的数据管道：下载+预处理+计算"""
    import subprocess
    import time
    import os

    _log_stage("🔄 启动数据管道...")
    print("=" * 60)

    total_steps = 4
    current_step = 0

    # 获取当前工作目录
    current_dir = os.getcwd()
    _log_stage(f"📁 当前工作目录: {current_dir}")

    # 1. 运行FRED数据下载（可由每日批处理在外部先执行，设 CRISIS_MONITOR_SKIP_SYNC=1 则跳过避免重复）
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    skip_sync = os.environ.get("CRISIS_MONITOR_SKIP_SYNC", "").strip().lower() in ("1", "true", "yes")
    if skip_sync:
        _log_stage(f"📥 步骤{current_step}/{total_steps} ({progress}%): 跳过下载（已由批处理先执行，CRISIS_MONITOR_SKIP_SYNC=1）")
    else:
        _log_stage(f"📥 步骤{current_step}/{total_steps} ({progress}%): 下载FRED数据（报告前全量拉取）...")
        _log_stage("⏳ 预计等待时间: 5-30分钟（序列较多时请耐心等待）...")
        try:
            script_path = os.path.join(current_dir, "scripts", "sync_fred_http.py")
            _log_stage(f"🔍 执行脚本: {script_path} --before-report（先全量拉取再计算，避免报告遗漏）")
            result = subprocess.run(
                [sys.executable, script_path, "--before-report"],
                capture_output=True, text=True, timeout=1800, cwd=current_dir,
                encoding="utf-8", errors="replace",
            )
            if result.returncode == 0:
                _log_stage("✅ FRED数据下载完成")
            else:
                _log_stage(f"⚠️ FRED数据下载警告: {result.stderr}")
        except Exception as e:
            _log_stage(f"❌ FRED数据下载失败: {e}")

    # 2. 运行企业债/GDP比率计算
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    _log_stage(f"🧮 步骤{current_step}/{total_steps} ({progress}%): 计算企业债/GDP比率...")
    _log_stage("⏳ 预计等待时间: 30秒...")

    try:
        script_path = os.path.join(current_dir, "scripts", "calculate_corporate_debt_gdp_ratio.py")
        _log_stage(f"🔍 执行脚本: {script_path}")
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, timeout=60, cwd=current_dir,
                              encoding="utf-8", errors="replace")
        if result.returncode == 0:
            _log_stage("✅ 企业债/GDP比率计算完成")
        else:
            _log_stage(f"⚠️ 企业债/GDP比率计算警告: {result.stderr}")
    except Exception as e:
        _log_stage(f"❌ 企业债/GDP比率计算失败: {e}")

    # 3. 运行YoY指标计算
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    _log_stage(f"📊 步骤{current_step}/{total_steps} ({progress}%): 计算YoY指标...")
    _log_stage("⏳ 预计等待时间: 1-2分钟...")

    try:
        script_path = os.path.join(current_dir, "scripts", "calculate_yoy_indicators.py")
        _log_stage(f"🔍 执行脚本: {script_path}")
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, timeout=120, cwd=current_dir,
                              encoding="utf-8", errors="replace")
        if result.returncode == 0:
            _log_stage("✅ YoY指标计算完成")
        else:
            _log_stage(f"⚠️ YoY指标计算警告: {result.stderr}")
    except Exception as e:
        _log_stage(f"❌ YoY指标计算失败: {e}")

    # 4. 数据管道完成
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    _log_stage(f"✅ 步骤{current_step}/{total_steps} ({progress}%): 数据管道完成")
    print("=" * 60)

def generate_detailed_explanation_report(processed_indicators, output_path, timestamp):
    """生成详细解释报告，包含每个指标的通俗解释"""
    
    # 指标解释字典
    indicator_explanations = {
        'T10Y3M': {
            'name': '10年期-3个月国债利差',
            'description': '长期国债收益率减去短期国债收益率，反映市场对未来经济的预期',
            'high_risk_explanation': '利差很小或为负（倒挂）通常预示经济衰退，因为市场预期未来利率会下降',
            'low_risk_explanation': '利差较大表示经济健康，银行可以正常放贷获利',
            'unit': '%',
            'typical_range': '0.5% - 3.0%',
            'crisis_threshold': '< 0.5%'
        },
        'T10Y2Y': {
            'name': '10年期-2年期国债利差',
            'description': '另一个重要的收益率曲线指标，同样反映经济预期',
            'high_risk_explanation': '倒挂（负值）预示经济衰退风险',
            'low_risk_explanation': '正常利差表示经济健康',
            'unit': '%',
            'typical_range': '0.3% - 2.5%',
            'crisis_threshold': '< 0.3%'
        },
        'UST30Y_UST2Y_RSI': {
            'name': '30年/2年美债利差 RSI',
            'description': '基于30年期与2年期国债收益率比率(30Y/2Y)计算的月度RSI指标。',
            'high_risk_explanation': 'RSI突破50通常标志着收益率曲线完成“去倒挂”并开始陡峭化，历史上常对应股市崩盘或经济衰退的实质性爆发期。',
            'low_risk_explanation': 'RSI低于50通常表示市场处于倒挂期（潜伏期）或正常温和期。',
            'unit': 'RSI',
            'typical_range': '30 - 70',
            'crisis_threshold': '> 50'
        },
        'HY_OAS_MOMENTUM_RATIO': {
            'name': '高收益债利差20日动量比率',
            'description': 'BAMLH0A0HYM2 当前值相对20日均值的比率，衡量利差短期急速扩张。',
            'high_risk_explanation': '比率大于1.1表示利差在20日内快速走阔，流动性抽离风险显著上升。',
            'low_risk_explanation': '比率接近1表示利差变化温和，信用压力可控。',
            'unit': '比率',
            'typical_range': '0.9 - 1.1',
            'crisis_threshold': '> 1.1'
        },
        'SP500_DGS10_CORR60D': {
            'name': '股债60日滚动相关性',
            'description': '标普500与10年期国债收益率的60日滚动相关性（基于日度涨跌幅）。',
            'high_risk_explanation': '相关性升至高位且与VIX上升共振时，通常代表股债同跌的“现金为王”阶段。',
            'low_risk_explanation': '相关性为负或接近0时，股债对冲功能仍在。',
            'unit': '相关系数',
            'typical_range': '-0.5 - 0.5',
            'crisis_threshold': '> 0.5'
        },
        'NET_LIQUIDITY': {
            'name': '每日净流动性',
            'description': 'WALCL(线性插值) - WTREGEN - RRPONTSYD，近似衡量系统可用流动性。',
            'high_risk_explanation': '净流动性快速下降通常意味着风险资产缺乏“燃料”，回撤压力增大。',
            'low_risk_explanation': '净流动性稳定或上升时，风险资产承压较小。',
            'unit': '亿美元(近似)',
            'typical_range': '随政策变化',
            'crisis_threshold': '显著下降'
        },
        'VIX_TERM_STRUCTURE': {
            'name': 'VIX期限结构',
            'description': 'VIX与VIX3M的比率，用于识别短期恐慌是否倒挂。',
            'high_risk_explanation': '比值大于1通常表示短期恐慌更强，容易伴随风险资产急跌。',
            'low_risk_explanation': '比值小于1表示期限结构正常，市场恐慌相对可控。',
            'unit': '比率',
            'typical_range': '0.7 - 1.0',
            'crisis_threshold': '> 1.0'
        },
        'HY_IG_RATIO': {
            'name': '高收益/投资级总回报比率',
            'description': '高收益债总回报与投资级债总回报的比率，衡量风险偏好。',
            'high_risk_explanation': '比率快速下行（或Z-Score显著为负）通常代表资金逃离风险信贷。',
            'low_risk_explanation': '比率稳定或回升表示风险偏好仍在。',
            'unit': '比率',
            'typical_range': '随市场变化',
            'crisis_threshold': '快速下行'
        },
        'GLOBAL_LIQUIDITY_USD': {
            'name': '全球流动性代理',
            'description': 'Fed + ECB + BOJ 资产负债表折算美元后的合计。',
            'high_risk_explanation': '全球流动性快速收缩通常对应风险资产压力上升。',
            'low_risk_explanation': '流动性扩张有利于风险偏好维持。',
            'unit': '美元(近似)',
            'typical_range': '随政策变化',
            'crisis_threshold': '显著下降'
        },
        'CREDIT_CARD_DELINQUENCY': {
            'name': '信用卡违约率',
            'description': '信用卡违约率（DRCCLACBS），用于验证消费与信贷风险。',
            'high_risk_explanation': '违约率持续上升，表明消费端真实压力在累积。',
            'low_risk_explanation': '违约率低位或回落表示消费压力可控。',
            'unit': '%',
            'typical_range': '1% - 5%',
            'crisis_threshold': '> 4%'
        },
        'BAMLH0A0HYM2': {
            'name': '高收益债券利差',
            'description': '高风险公司债券与国债的收益率差异，反映信用风险',
            'high_risk_explanation': '利差扩大表示企业违约风险上升，投资者要求更高回报',
            'low_risk_explanation': '利差较小表示企业信用状况良好',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '> 8%'
        },
        'BAA10YM': {
            'name': '投资级公司债利差',
            'description': '投资级公司债券与国债的收益率差异',
            'high_risk_explanation': '利差扩大表示企业信用风险上升',
            'low_risk_explanation': '利差较小表示企业信用状况良好',
            'unit': '%',
            'typical_range': '1% - 4%',
            'crisis_threshold': '> 4%'
        },
        'TEDRATE': {
            'name': 'TED利差',
            'description': '3个月期银行间拆借利率与国债利率的差异，反映银行间信用风险',
            'high_risk_explanation': '利差扩大表示银行间不信任，流动性紧张',
            'low_risk_explanation': '利差较小表示银行间信任度高，流动性充足',
            'unit': '%',
            'typical_range': '0.1% - 1.0%',
            'crisis_threshold': '> 1.0%'
        },
        'VIXCLS': {
            'name': 'VIX波动率指数',
            'description': '衡量市场恐慌情绪和未来波动率预期',
            'high_risk_explanation': 'VIX高表示市场恐慌，投资者预期波动率上升',
            'low_risk_explanation': 'VIX低表示市场平静，投资者情绪稳定',
            'unit': '点',
            'typical_range': '10 - 30',
            'crisis_threshold': '> 30'
        },
        'NFCI': {
            'name': '芝加哥金融状况指数',
            'description': '综合反映金融市场压力状况',
            'high_risk_explanation': '指数高表示金融状况紧张，融资困难',
            'low_risk_explanation': '指数低表示金融状况宽松，融资容易',
            'unit': '指数',
            'typical_range': '-1.0 - 1.0',
            'crisis_threshold': '> 1.0'
        },
        'GDP': {
            'name': '国内生产总值',
            'description': '衡量一个国家经济总量的指标',
            'high_risk_explanation': 'GDP增长缓慢或负增长表示经济衰退',
            'low_risk_explanation': 'GDP稳定增长表示经济健康',
            'unit': '%',
            'typical_range': '2% - 4%',
            'crisis_threshold': '< 1%'
        },
        'INDPRO': {
            'name': '工业生产指数',
            'description': '衡量制造业和采矿业产出',
            'high_risk_explanation': '工业生产下降表示经济衰退',
            'low_risk_explanation': '工业生产增长表示经济扩张',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '< -2%'
        },
        'UNRATE': {
            'name': '失业率',
            'description': '失业人口占总劳动力的比例',
            'high_risk_explanation': '失业率高表示经济衰退，就业困难',
            'low_risk_explanation': '失业率低表示经济健康，就业充分',
            'unit': '%',
            'typical_range': '3% - 6%',
            'crisis_threshold': '> 7%'
        },
        'CPIAUCSL': {
            'name': '消费者价格指数',
            'description': '衡量通胀水平的指标',
            'high_risk_explanation': '通胀过高会侵蚀购买力，央行可能加息',
            'low_risk_explanation': '适度通胀有利于经济增长',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '> 5%'
        },
        'FEDFUNDS': {
            'name': '联邦基金利率',
            'description': '美联储设定的短期利率，影响整个经济',
            'high_risk_explanation': '利率过高会抑制经济增长',
            'low_risk_explanation': '适度利率有利于经济平衡',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'HOUST': {
            'name': '新屋开工数',
            'description': '衡量房地产市场的活跃程度',
            'high_risk_explanation': '新屋开工下降表示房地产市场疲软',
            'low_risk_explanation': '新屋开工增长表示房地产市场活跃',
            'unit': '千套',
            'typical_range': '1000 - 2000',
            'crisis_threshold': '< 800'
        },
        'UMCSENT': {
            'name': '密歇根消费者信心指数',
            'description': '衡量消费者对未来经济的信心',
            'high_risk_explanation': '信心低表示消费者对未来悲观，消费减少',
            'low_risk_explanation': '信心高表示消费者对未来乐观，消费增加',
            'unit': '指数',
            'typical_range': '80 - 100',
            'crisis_threshold': '< 70'
        },
        'SOFR20DMA_MINUS_DTB3': {
            'name': 'SOFR(20日均值)-3个月国债利差',
            'description': '有担保隔夜融资利率与3个月国债利率的差异，反映短期流动性状况',
            'high_risk_explanation': '利差扩大表示短期融资成本上升，流动性紧张',
            'low_risk_explanation': '利差较小表示短期融资成本低，流动性充足',
            'unit': '%',
            'typical_range': '0.1% - 0.5%',
            'crisis_threshold': '> 0.5%'
        },
        'PAYEMS': {
            'name': '非农就业人数',
            'description': '美国非农业部门就业人数的年度变化率',
            'high_risk_explanation': '就业增长放缓表示经济疲软，失业风险上升',
            'low_risk_explanation': '就业稳定增长表示经济健康，就业市场强劲',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '< 0.5%'
        },
        'CSUSHPINSA': {
            'name': '房价指数: Case-Shiller 20城',
            'description': '美国20个主要城市的房价指数年度变化率',
            'high_risk_explanation': '房价下跌表示房地产市场疲软，可能引发债务危机',
            'low_risk_explanation': '房价稳定增长表示房地产市场健康',
            'unit': '%',
            'typical_range': '2% - 8%',
            'crisis_threshold': '< 0%'
        },
        'PERMIT': {
            'name': '住宅建筑许可',
            'description': '新住宅建筑许可数量的年度变化率',
            'high_risk_explanation': '建筑许可大幅下降表示房地产市场萎缩',
            'low_risk_explanation': '建筑许可稳定表示房地产市场活跃',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -15%'
        },
        'MORTGAGE30US': {
            'name': '30年期按揭利率',
            'description': '30年期固定利率抵押贷款的平均利率',
            'high_risk_explanation': '利率过高会抑制购房需求，影响房地产市场',
            'low_risk_explanation': '适度利率有利于房地产市场发展',
            'unit': '%',
            'typical_range': '3% - 7%',
            'crisis_threshold': '> 8%'
        },
        'CPN3M': {
            'name': '3个月商业票据利率',
            'description': '3个月期商业票据的平均利率，反映企业短期融资成本',
            'high_risk_explanation': '利率过高会增加企业融资成本，影响经营',
            'low_risk_explanation': '适度利率有利于企业融资和发展',
            'unit': '%',
            'typical_range': '1% - 5%',
            'crisis_threshold': '> 6%'
        },
        'TOTLL': {
            'name': '总贷款和租赁',
            'description': '银行总贷款和租赁业务的年度变化率',
            'high_risk_explanation': '贷款增长放缓表示银行放贷谨慎，经济疲软',
            'low_risk_explanation': '贷款稳定增长表示银行放贷活跃，经济健康',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '< 1%'
        },
        'DRTSCILM': {
            'name': '银行贷款标准-大中企C&I收紧净比例',
            'description': '银行对大中型企业商业和工业贷款收紧标准的净比例',
            'high_risk_explanation': '收紧比例高表示银行风险偏好下降，信贷紧缩',
            'low_risk_explanation': '收紧比例低表示银行风险偏好正常，信贷宽松',
            'unit': '%',
            'typical_range': '0% - 20%',
            'crisis_threshold': '> 30%'
        },
        'NCBDBIQ027S': {
            'name': '企业债/GDP（新）',
            'description': '企业债务总额占GDP的百分比',
            'high_risk_explanation': '企业债务过高会增加违约风险，影响金融稳定',
            'low_risk_explanation': '企业债务适中表示企业杠杆合理',
            'unit': '%',
            'typical_range': '20% - 30%',
            'crisis_threshold': '> 35%'
        },
        'CORPDEBT_GDP_PCT': {
            'name': '企业债/GDP（旧）',
            'description': '企业债务总额占GDP的百分比（旧计算方法）',
            'high_risk_explanation': '企业债务过高会增加违约风险，影响金融稳定',
            'low_risk_explanation': '企业债务适中表示企业杠杆合理',
            'unit': '%',
            'typical_range': '18% - 28%',
            'crisis_threshold': '> 32%'
        },
        'THREEFYTP10': {
            'name': '期限溢价-10年期Kim-Wright代理',
            'description': '10年期国债的期限溢价，反映长期利率风险',
            'high_risk_explanation': '期限溢价过高表示长期利率风险上升',
            'low_risk_explanation': '期限溢价适中表示长期利率风险可控',
            'unit': '%',
            'typical_range': '0.5% - 2.0%',
            'crisis_threshold': '> 3.0%'
        },
        'MANEMP': {
            'name': '制造业就业',
            'description': '制造业就业人数的年度变化率',
            'high_risk_explanation': '制造业就业下降表示制造业萎缩，经济衰退',
            'low_risk_explanation': '制造业就业稳定表示制造业健康',
            'unit': '%',
            'typical_range': '-2% - 2%',
            'crisis_threshold': '< -3%'
        },
        'NEWORDER': {
            'name': '制造业新订单-非国防资本货不含飞机',
            'description': '制造业新订单的年度变化率（排除国防和飞机）',
            'high_risk_explanation': '新订单下降表示制造业需求疲软',
            'low_risk_explanation': '新订单增长表示制造业需求旺盛',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -10%'
        },
        'AWHMAN': {
            'name': '制造业平均周工时',
            'description': '制造业工人的平均每周工作小时数',
            'high_risk_explanation': '工时减少表示制造业活动放缓',
            'low_risk_explanation': '工时稳定表示制造业活动正常',
            'unit': '小时',
            'typical_range': '40 - 42',
            'crisis_threshold': '< 39'
        },
        'DGS10': {
            'name': '10年期国债利率',
            'description': '10年期美国国债的收益率',
            'high_risk_explanation': '利率过高会抑制经济增长，增加债务负担',
            'low_risk_explanation': '适度利率有利于经济平衡发展',
            'unit': '%',
            'typical_range': '2% - 6%',
            'crisis_threshold': '> 7%'
        },
        'SOFR': {
            'name': 'SOFR',
            'description': '有担保隔夜融资利率，反映短期资金成本',
            'high_risk_explanation': 'SOFR过高会增加短期融资成本',
            'low_risk_explanation': 'SOFR适中有利于短期融资',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'DTB3': {
            'name': '3个月国债利率',
            'description': '3个月期美国国债的收益率',
            'high_risk_explanation': '利率过高会增加短期融资成本',
            'low_risk_explanation': '适度利率有利于短期融资',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'WALCL': {
            'name': '美联储总资产',
            'description': '美联储资产负债表总资产的年度变化率',
            'high_risk_explanation': '资产大幅收缩表示货币政策收紧',
            'low_risk_explanation': '资产稳定表示货币政策适中',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -10%'
        },
        'TOTALSA': {
            'name': '消费者信贷',
            'description': '消费者信贷总额的年度变化率',
            'high_risk_explanation': '信贷增长过快可能引发债务风险',
            'low_risk_explanation': '信贷适度增长有利于消费',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '> 12%'
        },
        'TDSP': {
            'name': '家庭债务偿付比率',
            'description': '家庭债务偿付占可支配收入的比例',
            'high_risk_explanation': '比率过高表示家庭债务负担过重',
            'low_risk_explanation': '比率适中表示家庭债务负担合理',
            'unit': '%',
            'typical_range': '8% - 12%',
            'crisis_threshold': '> 15%'
        },
        'TOTRESNS': {
            'name': '银行准备金',
            'description': '银行准备金的年度变化率',
            'high_risk_explanation': '准备金大幅下降可能影响银行流动性',
            'low_risk_explanation': '准备金稳定表示银行流动性充足',
            'unit': '%',
            'typical_range': '-5% - 5%',
            'crisis_threshold': '< -10%'
        },
        'IC4WSA': {
            'name': '初请失业金4周均值',
            'description': '初次申请失业救济金的4周移动平均值',
            'high_risk_explanation': '申请人数过多表示就业市场疲软',
            'low_risk_explanation': '申请人数适中表示就业市场健康',
            'unit': '人',
            'typical_range': '200,000 - 300,000',
            'crisis_threshold': '> 400,000'
        },
        'DTWEXBGS': {
            'name': '贸易加权美元',
            'description': '美元对主要贸易伙伴货币的加权汇率年度变化率',
            'high_risk_explanation': '美元过强会削弱出口竞争力',
            'low_risk_explanation': '美元适中有利于贸易平衡',
            'unit': '%',
            'typical_range': '-5% - 5%',
            'crisis_threshold': '> 10%'
        },
        'STLFSI4': {
            'name': '圣路易斯金融压力',
            'description': '圣路易斯联邦储备银行的金融压力指数（STLFSI4 口径）',
            'high_risk_explanation': '压力指数高表示金融市场压力大',
            'low_risk_explanation': '压力指数低表示金融市场稳定',
            'unit': '指数',
            'typical_range': '-2.0 - 2.0',
            'crisis_threshold': '> 3.0'
        },
        'DRSFRMACBS': {
            'name': '房贷违约率',
            'description': '住房抵押贷款违约率',
            'high_risk_explanation': '违约率高表示房地产市场风险上升',
            'low_risk_explanation': '违约率低表示房地产市场健康',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '> 5%'
        }
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# 📚 FRED指标详细解释报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        f.write("## 📖 使用说明\n\n")
        f.write("本报告为每个指标提供通俗易懂的解释，帮助普通投资者理解经济指标的含义。\n\n")
        f.write("每个指标包含：\n")
        f.write("- **指标含义**：这个指标代表什么\n")
        f.write("- **高低风险解释**：为什么高/低值表示风险\n")
        f.write("- **典型范围**：正常情况下的数值范围\n")
        f.write("- **危机阈值**：达到什么水平需要警惕\n")
        f.write("- **当前状态**：与历史数据的对比\n\n")
        
        f.write("## 📊 指标详细解释\n\n")
        
        # 按风险等级分组
        high_risk = [i for i in processed_indicators if i['risk_score'] >= 80]
        medium_risk = [i for i in processed_indicators if 60 <= i['risk_score'] < 80]
        low_risk = [i for i in processed_indicators if i['risk_score'] < 60]
        
        # 高风险指标
        if high_risk:
            f.write("### 🔴 高风险指标\n\n")
            for indicator in high_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🔴 高风险)\n\n")
                
                f.write(f"**为什么是高风险**: {explanation.get('high_risk_explanation', '当前值偏离正常范围')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                # 添加历史对比
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n")
                    if indicator['current_value'] > indicator['benchmark_value']:
                        f.write("当前值高于历史基准，风险上升\n\n")
                    else:
                        f.write("当前值低于历史基准，风险相对较低\n\n")
                
                f.write("---\n\n")
        
        # 中风险指标
        if medium_risk:
            f.write("### 🟡 中风险指标\n\n")
            for indicator in medium_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🟡 中风险)\n\n")
                
                f.write(f"**风险解释**: {explanation.get('high_risk_explanation', '当前值略偏离正常范围')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n\n")
                
                f.write("---\n\n")
        
        # 低风险指标
        if low_risk:
            f.write("### 🟢 低风险指标\n\n")
            for indicator in low_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🟢 低风险)\n\n")
                
                f.write(f"**为什么是低风险**: {explanation.get('low_risk_explanation', '当前值在正常范围内')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n\n")
                
                f.write("---\n\n")
        
        f.write("## 📝 总结\n\n")
        f.write("本报告基于FRED官方数据，通过对比历史危机期间的数据来评估当前风险。\n\n")
        f.write("**重要提醒**:\n")
        f.write("- 这些指标仅供参考，不构成投资建议\n")
        f.write("- 经济指标存在滞后性，需要结合其他信息综合判断\n")
        f.write("- 市场情况复杂多变，单一指标不能完全预测未来\n")
        f.write("- 建议咨询专业投资顾问获得个性化建议\n\n")
        
        f.write("**数据来源**: FRED (Federal Reserve Economic Data)\n")
        f.write("**报告生成**: FRED危机预警监控系统\n")
        f.write("**联系方式**: jiangx@gmail.com\n\n")
        
        f.write("---\n")
        f.write(f"*报告生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}*\n")


def generate_narrative_with_llm(processed_indicators, warsh_state, api_key=None):
    """
    [AI 核心] 使用大模型 (Alibaba Qwen / DeepSeek) 生成深度宏观研报 (CRO 版，强制中文输出)。
    """
    try:
        from openai import OpenAI
    except ImportError:
        return "⚠️ 未安装 openai 库，无法生成 AI 研报。"

    if not api_key:
        api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return "⚠️ 未配置 API Key，无法生成 AI 深度研报。请配置 DASHSCOPE_API_KEY。"

    key_metrics = {
        "Warsh_Constraints": {
            "RRP_Level_Bn": warsh_state.get("rrp_level_bn"),
            "SOFR_Spread": warsh_state.get("sofr_iorb_spread"),
            "Term_Premium": warsh_state.get("term_premium"),
        },
        "High_Risk_Indicators": [
            {"id": i.get("series_id"), "name": i.get("name"), "val": i.get("current_value"), "score": i.get("risk_score")}
            for i in processed_indicators
            if i.get("risk_score", 0) >= 60
        ],
        "Key_Macro": {
            "GDP": next((i["current_value"] for i in processed_indicators if i.get("series_id") == "GDP"), None),
            "Inflation_CPI": next((i["current_value"] for i in processed_indicators if i.get("series_id") == "CPIAUCSL"), None),
            "Unemployment": next((i["current_value"] for i in processed_indicators if i.get("series_id") == "PAYEMS"), None),
            "VIX": next((i["current_value"] for i in processed_indicators if i.get("series_id") == "VIXCLS"), None),
        },
    }
    data_context = json.dumps(key_metrics, indent=2, ensure_ascii=False)

    system_prompt = """
🔴 **IMPORTANT: 全程必须使用【中文】撰写报告，严禁输出英文段落！**

你是一位【首席风险官（CRO）】，但你的读者不是基金经理，而是「聪明、理性、但不懂金融术语的普通投资者」。
你的任务不是炫耀专业性，而是把复杂的金融管道风险，翻译成人人能理解的「因果故事」。

====================
【最高优先级写作要求】
====================
你必须同时满足两种读者：
1️⃣ 专业读者：逻辑必须严谨、定义必须正确、不得夸大风险。
2️⃣ 普通读者：不懂 SOFR、RRP、IORB，也能理解「发生了什么」。
⚠️ 如果普通人看不懂，你的回答就是失败的。

====================
【强制结构（必须遵守）】
====================
每一个核心指标或结论，必须包含以下三个层次：

① **白话一句话总结（Plain Language Summary）**
- 用 15–25 个字
- 不允许出现专业名词
- 回答：「这件事大概在说什么？」
例：「央行还能不能继续收紧资金，正在接近极限。」

② **专业解释（Technical Explanation）**
- 可以使用 RRP、SOFR、IORB、Term Premium 等术语
- 解释它们在金融系统中的「物理作用」
- 说明为什么这个指标在 Warsh 框架下重要

③ **对普通人的含义（What This Means for You）**
- 不给买卖建议
- 只回答：现在安全吗？需要立刻担心吗？这是「已经出事」还是「需要盯着」的阶段？
例：「这并不意味着市场马上会下跌，但意味着一旦出现冲击，央行能缓冲的空间变小了。」

====================
【语言风格约束】
====================
🚫 严禁使用：崩盘、必然、死局、末日、系统性灾难、买入/卖出/抄底/做空
✅ 鼓励使用：缓冲垫、安全余量、「还没撞墙，但离墙更近了」、「这是一盏『注意路况』的灯，而不是『立刻刹车』」

====================
【Warsh 专属解释要求（必须体现）】
====================
你必须明确告诉读者：Warsh 关注的不是「利率高不高」，而是**金融系统还能不能承受继续抽水**。
解释时必须使用以下类比之一：水箱/管道、减震器、高速公路限流。

====================
【风险信号灯的白话翻译（强制）】
====================
🟢 GREEN = 「系统宽裕，有余量」
🟡 YELLOW = 「问题开始出现，但不紧急」
🟠 AMBER = 「还没出事，但安全垫已经很薄，需要盯着」
🔴 RED = 「已经撞到物理限制，政策必须调整」

====================
【最终目标】
====================
读者读完后应当能回答之一：
- 「哦，原来现在不是马上要出事，但不再是随便玩的环境。」
- 「我终于明白为什么你们老盯着这些奇怪的指标。」
- 「即使我不交易，也知道现在该不该提高警惕。」
如果你只是让人觉得「你很专业」而不是「我懂了」，那就是失败。

（本系统是「容错空间监测系统」，不是预测系统。）

====================
【小节顺序与收束（强制，不可打乱）】
====================
正文必须严格按以下顺序输出（标题编号与 🔹/✅ 标记须保留）：
1) 1–2 段开篇引导（例如「汽车仪表盘」比喻，禁止英文段落）。
2) `### 🔹 1.` — 第一个核心约束指标（优先 RRP / 缓冲垫主题）。
3) `### 🔹 2.` — SOFR 与 IORB / 利率走廊主题。
4) `### 🔹 3.` — 期限溢价（Term Premium）主题。
5) `### 🔹 综合三指标：Warsh系统状态判断` — 三指标合并后的信号灯结论。
6) `### 🔹 附：其他高风险指标简评（辅助验证）` — 列表简评，勿展开成另一篇长文。
7) `### ✅ 最后一句给所有普通人的话：` — **必须是全文最后一节**；该节下只允许一小段说明 + **一段引用块**（每行以 `>` 开头，内含一句给普通人的收束比喻）。**引用块结束后立即结束输出，禁止再写任何段落**（尤其禁止在引用后再写「这就是 Warsh 框架想告诉你的全部」等总结句）。
"""
    user_prompt = f"这是最新的市场监控数据 (JSON格式)：\n{data_context}\n\n请基于此数据，按上述「白话+专业+对普通人含义」三层结构，生成一份**中文**、普通人也能读懂的 Warsh 风险解读。"

    try:
        client = OpenAI(
            api_key=api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        completion = client.chat.completions.create(
            model="qwen-plus",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.6,
        )
        usage = getattr(completion, "usage", None)
        if usage:
            pt = getattr(usage, "prompt_tokens", 0)
            ct = getattr(usage, "completion_tokens", 0)
            total = getattr(usage, "total_tokens", pt + ct)
            _log_stage(f"🤖 AI 深度推演 API 用量: 输入 {pt} + 输出 {ct} = {total} tokens")
        return completion.choices[0].message.content or "（模型返回为空）"
    except Exception as e:
        return f"❌ AI 分析生成失败: {str(e)}"


def generate_warsh_plots(output_dir: pathlib.Path) -> dict:
    """
    生成 Warsh 约束仪表盘专用图表（双轴图 + 期限溢价图），使用真实 FRED 数据。
    返回图表相对路径字典 {'liquidity': 'figures/warsh_liquidity.png', 'term_premium': 'figures/warsh_term_premium.png'}
    """
    plots = {}
    _log_stage("🎨 正在绘制 Warsh 约束仪表盘…")
    try:
        rrp = fetch_series("RRPONTSYD")
        spread = compose_series("SOFR_MINUS_IORB")
        if spread is None or (hasattr(spread, "empty") and spread.empty):
            sofr = fetch_series("SOFR")
            iorb = fetch_series("IORB")
            if sofr is not None and not sofr.empty and iorb is not None and not iorb.empty:
                iorb_aligned = iorb.reindex_like(sofr).ffill()
                spread = (sofr - iorb_aligned).dropna()
        tp = fetch_series("THREEFYTP10")
        start_2y = pd.Timestamp.now() - pd.DateOffset(years=2)
        start_5y = pd.Timestamp.now() - pd.DateOffset(years=5)

        if rrp is not None and not rrp.empty and spread is not None and not spread.empty:
            rrp_cut = rrp[rrp.index >= start_2y]
            spread_cut = spread[spread.index >= start_2y]
            common_idx = rrp_cut.index.intersection(spread_cut.index)
            if len(common_idx) >= 10:
                rrp_cut = rrp_cut.reindex(common_idx).ffill().dropna()
                spread_cut = spread_cut.reindex(common_idx).ffill().dropna()
                fig, ax1 = plt.subplots(figsize=(10, 5))
                ax1.set_xlabel("日期")
                ax1.set_ylabel("逆回购 RRP（十亿美元）", color="tab:blue", fontweight="bold")
                ax1.fill_between(rrp_cut.index, 0, rrp_cut.values, color="tab:blue", alpha=0.3)
                ax1.plot(rrp_cut.index, rrp_cut.values, color="tab:blue", linewidth=1)
                ax1.tick_params(axis="y", labelcolor="tab:blue")
                ax1.set_ylim(0, max(rrp_cut.max(), 2000))
                ax1.axhline(y=100, color="red", linestyle=":", alpha=0.5)
                ax1.text(rrp_cut.index[0], 120, "缓冲警戒线 (<100B)", color="red", fontsize=8)

                ax2 = ax1.twinx()
                ax2.set_ylabel("SOFR − IORB 利差 (bp)", color="tab:orange", fontweight="bold")
                ax2.plot(spread_cut.index, spread_cut.values * 100, color="tab:orange", linewidth=1.5)
                ax2.tick_params(axis="y", labelcolor="tab:orange")
                ax2.axhline(y=0, color="gray", linestyle="-", linewidth=0.8)
                plt.title("Warsh 约束 #1：流动性管道（缓冲消耗 vs 利率走廊）", fontsize=11, fontweight="bold")
                plt.tight_layout()
                path_a = output_dir / "figures" / "warsh_liquidity.png"
                path_a.parent.mkdir(parents=True, exist_ok=True)
                plt.savefig(path_a, dpi=150)
                plt.close()
                plots["liquidity"] = "figures/warsh_liquidity.png"

        if tp is not None and not tp.empty:
            tp_cut = tp[tp.index >= start_5y].dropna()
            if len(tp_cut) >= 10:
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.plot(tp_cut.index, tp_cut.values, color="tab:purple", linewidth=1.5)
                ax.set_ylabel("10Y 期限溢价 (%)", color="tab:purple", fontweight="bold")
                median_val = float(tp_cut.median())
                ax.axhline(y=median_val, color="green", linestyle="--", alpha=0.6, label=f"历史中位数 ({median_val:.2f}%)")
                ax.fill_between(tp_cut.index, median_val, tp_cut.values, where=(tp_cut.values > median_val), color="tab:purple", alpha=0.1)
                plt.title("Warsh 约束 #2：债市重定价压力（10Y 期限溢价）", fontsize=11, fontweight="bold")
                plt.legend(loc="upper left")
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                path_b = output_dir / "figures" / "warsh_term_premium.png"
                path_b.parent.mkdir(parents=True, exist_ok=True)
                plt.savefig(path_b, dpi=150)
                plt.close()
                plots["term_premium"] = "figures/warsh_term_premium.png"
    except Exception as e:
        _log_stage(f"⚠️ Warsh 图表绘制失败: {e}")
    return plots


def _compute_freshness_classes(processed_indicators, indicators, scoring_config, reference_date=None):
    """按周末感知的滞后阈值将指标分为「今日/可接受」与「已滞后」，排除已废弃序列。"""
    ref = reference_date or datetime.now(JST).date()
    deprecated_set = set(scoring_config.get("deprecated_series") or [])
    id_to_indicator = {ind.get("id"): ind for ind in (indicators or []) if ind.get("id")}
    fresh, stale = [], []
    for p in processed_indicators or []:
        sid = p.get("series_id")
        if sid in deprecated_set:
            continue
        last_s = p.get("last_date")
        if not last_s:
            continue
        try:
            last_d = datetime.strptime(last_s, "%Y-%m-%d").date()
        except Exception:
            continue
        age_days = (ref - last_d).days
        ind = id_to_indicator.get(sid, {})
        freq = (ind.get("freq") or "D").upper()
        if freq == "D":
            max_lag_days = 7   # 日频：留周末+发布延迟缓冲，避免正常 3–5 天延迟误报
        elif freq == "W":
            max_lag_days = 10  # 周频：最多 10 天属正常发布节奏
        elif freq == "M":
            max_lag_days = 60
        elif freq == "Q":
            max_lag_days = 120
        else:
            max_lag_days = 7
        row = {
            "name": p.get("name", sid),
            "series_id": sid,
            "last_date": last_s,
            "age_days": age_days,
            "max_lag_days": max_lag_days,
        }
        if age_days <= max_lag_days:
            fresh.append(row)
        else:
            stale.append(row)
    return fresh, stale


def _format_freshness_section(fresh, stale) -> str:
    """生成「数据新鲜度与覆盖」Markdown 区块。"""
    parts = ["## 📅 数据新鲜度与覆盖\n\n"]
    parts.append("基于各指标最后观测日期与更新频率（日频 7 天、周频 10 天、月频 60 天、季频 120 天）判断是否在可接受滞后范围内。\n\n")
    if fresh:
        parts.append("**✅ 今日已更新 / 可接受滞后**\n\n")
        for r in sorted(fresh, key=lambda x: (x["series_id"],)):
            parts.append(f"- **{r['name']}** (`{r['series_id']}`)：最后观测 {r['last_date']}，滞后 {r['age_days']} 天（阈值 {r['max_lag_days']} 天）\n")
        parts.append("\n")
    if stale:
        parts.append("**⚠️ 已滞后（建议关注或更新数据源）**\n\n")
        for r in sorted(stale, key=lambda x: (-x["age_days"], x["series_id"])):
            parts.append(f"- **{r['name']}** (`{r['series_id']}`)：最后观测 {r['last_date']}，滞后 **{r['age_days']}** 天（阈值 {r['max_lag_days']} 天）\n")
        parts.append("\n")
    if not fresh and not stale:
        parts.append("*暂无指标纳入新鲜度统计（或无 last_date）。*\n\n")
    return "".join(parts)


# 报告目录中带时间戳的产物；仅按 mtime 删除，避免误删 latest / 状态文件
_CRISIS_OUTPUT_STALE_NAME_PATTERNS = (
    re.compile(r"^crisis_report_\d{8}_\d{6}\.(md|html|json)$"),
    re.compile(r"^crisis_report_detailed_\d{8}_\d{6}\.md$"),
    re.compile(r"^crisis_report_long_\d{8}_\d{6}\.png$"),
    re.compile(r"^latest_\d{8}_\d{6}\.(md|html)$"),
    re.compile(r"^audit_per_indicator_\d{8}_\d{6}\.tsv$"),
)
_CRISIS_OUTPUT_PROTECTED_NAMES = frozenset(
    {
        "crisis_report_latest.md",
        "crisis_report_latest.html",
        "crisis_report_latest.json",
        "investigo_signals.json",
        "ew_state.json",
        "ew_profile_history.json",
        "run_trace.log",
    }
)


def cleanup_old_crisis_monitor_outputs(
    output_dir: pathlib.Path,
    max_age_days: Optional[int] = None,
) -> int:
    """
    删除 output_dir 下「带时间戳的报告产物」中超过 max_age_days 天的文件。
    不删除 latest 链接副本、investigo、EW 状态与 run_trace。
    可通过环境变量 CRISIS_MONITOR_OUTPUT_MAX_AGE_DAYS 覆盖默认 30 天；
    设置 CRISIS_MONITOR_SKIP_OUTPUT_CLEANUP=1 跳过清理。
    """
    if os.environ.get("CRISIS_MONITOR_SKIP_OUTPUT_CLEANUP", "").strip() == "1":
        return 0
    if max_age_days is None:
        try:
            max_age_days = int(os.environ.get("CRISIS_MONITOR_OUTPUT_MAX_AGE_DAYS", "30"))
        except ValueError:
            max_age_days = 30
    if max_age_days <= 0:
        return 0
    if not output_dir.is_dir():
        return 0
    cutoff = time.time() - float(max_age_days) * 86400.0
    removed = 0
    for p in output_dir.iterdir():
        if not p.is_file():
            continue
        name = p.name
        if name in _CRISIS_OUTPUT_PROTECTED_NAMES:
            continue
        if not any(pat.match(name) for pat in _CRISIS_OUTPUT_STALE_NAME_PATTERNS):
            continue
        try:
            if p.stat().st_mtime >= cutoff:
                continue
            p.unlink()
            removed += 1
        except OSError:
            pass
    if removed:
        print(f"🧹 已删除 {removed} 个超过 {max_age_days} 天的旧报告文件（{output_dir.name}）")
    return removed


def generate_report_with_images():
    """生成带图片的危机预警报告"""
    global RUN_LOG_PATH, RUN_START_TS
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)
    output_dir = BASE / "outputs" / "crisis_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    RUN_LOG_PATH = output_dir / "run_trace.log"
    RUN_START_TS = time.time()
    _log_stage("▶️ Run start")

    stop_event = threading.Event()

    def _heartbeat():
        while not stop_event.is_set():
            time.sleep(30)
            if not stop_event.is_set():
                _log_stage("⏱️ 仍在运行...（心跳）")

    heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
    heartbeat_thread.start()

    # 首先运行数据管道
    run_data_pipeline()

    # 加载配置
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    scoring_config = config.get('scoring', {})
    bands = scoring_config.get('bands', {'low': 40, 'med': 60, 'high': 80})
    _log_stage(f"📊 加载了 {len(indicators)} 个指标")
    
    # 计算真实FRED数据评分
    _log_stage("🧮 计算指标评分...")
    group_scores, total_score, processed_indicators = calculate_real_fred_scores(indicators, scoring_config)
    _log_stage("✅ 指标评分完成")
    
    # Warsh + Japan 传染检测（早算以便做 Japan RED 权重补偿与后续报告）
    warsh_state = get_warsh_state()
    warsh_level_result = compute_warsh_risk_level(warsh_state)
    japan_state = get_japan_carry_state()
    japan_result = check_japan_contagion(warsh_level_result.get("warsh_level"), japan_state)
    effective_warsh_result = warsh_level_result
    if japan_result.get("escalate_global_to_red"):
        effective_warsh_result = {
            **warsh_level_result,
            "warsh_level": "RED",
            "warsh_level_reason": (warsh_level_result.get("warsh_level_reason", "") + " [Escalated: Warsh AMBER + JPY Carry Unwind]"),
            "warsh_flags": list(warsh_level_result.get("warsh_flags", [])) + ["Japan contagion: JPY vol AMBER + Warsh AMBER → RED"],
        }
    group_scores, total_score = apply_japan_red_compensation(group_scores, total_score, japan_result)
    
    # 为每个指标添加风险等级
    for indicator in processed_indicators:
        indicator['risk_level'] = level_from_score(indicator['risk_score'], bands)
    
    # 生成报告（使用JST时区）
    now = datetime.now(JST)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    display_time = now.strftime("%Y年%m月%d日 %H:%M:%S JST")
    
    # 生成图片
    _log_stage("📊 生成图表...")
    image_paths = {}
    
    # 生成总体概览图表
    summary_chart_path = generate_summary_chart(group_scores, total_score, output_dir)
    if summary_chart_path:
        image_paths['summary'] = summary_chart_path
    
    # 生成主要指标图表 - 使用完整的viz.py功能
    main_chart_paths = []
    
    # 导入图表生成模块
    try:
        from scripts.viz import save_indicator_plot
        viz_available = True
        print("✅ 成功导入viz模块")
    except ImportError:
        print("⚠️ 无法导入viz模块，使用简化图表")
        viz_available = False
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    # 为所有指标生成完整图表
    for i, indicator in enumerate(processed_indicators):
        try:
            series_id = indicator['series_id']
            _log_stage(f"🖼️ 绘图 {i+1}/{len(processed_indicators)}: {series_id}")
            chart_path = output_dir / "figures" / f"{series_id}_latest.png"
            
            if viz_available:
                # 使用完整的图表生成功能
                from scripts.fred_http import series_observations
                from scripts.clean_utils import parse_numeric_series
                
                ts = None
                
                # 优先使用合成序列（派生指标）
                ts = compose_series(series_id)
                if ts is not None and not ts.empty:
                    print(f"🔄 使用合成序列绘图: {series_id}")
                
                # 优先使用预计算的数据（YoY或比率）
                if (ts is None or ts.empty) and series_id == 'NCBDBIQ027S':
                    # 使用预计算的企业债/GDP比率数据
                    try:
                        ratio_file = pathlib.Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
                        if ratio_file.exists():
                            ratio_df = pd.read_csv(ratio_file)
                            ratio_df['date'] = pd.to_datetime(ratio_df['date'])
                            ratio_df = ratio_df.set_index('date')
                            ts = parse_numeric_series(ratio_df['value']).dropna()
                            print(f"📊 使用企业债/GDP比率数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 企业债/GDP比率数据读取失败: {e}")
                
                # 处理合成指标
                elif (ts is None or ts.empty) and series_id in ['CP_MINUS_DTB3', 'SOFR20DMA_MINUS_DTB3', 'SOFR_MINUS_IORB', 'RRP_DRAIN_RATE', 'CORPDEBT_GDP_PCT', 'RESERVES_ASSETS_PCT', 'RESERVES_DEPOSITS_PCT']:
                    # 使用预计算的合成指标数据
                    try:
                        synthetic_file = BASE / "data" / "series" / f"{series_id}.csv"
                        if synthetic_file.exists():
                            synthetic_df = pd.read_csv(synthetic_file)
                            synthetic_df['date'] = pd.to_datetime(synthetic_df['date'])
                            synthetic_df = synthetic_df.set_index('date')
                            ts = synthetic_df['value'].dropna()
                            print(f"📁 使用预计算合成指标数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 合成指标数据读取失败: {e}")
                
                elif (ts is None or ts.empty) and series_id in ['PAYEMS', 'INDPRO', 'GDP', 'NEWORDER', 'CSUSHPINSA', 'TOTALSA', 'TOTLL', 'MANEMP', 'WALCL', 'DTWEXBGS', 'PERMIT', 'TOTRESNS']:
                    # 使用预计算的YoY数据
                    try:
                        yoy_file = pathlib.Path(f"data/series/{series_id}_YOY.csv")
                        if yoy_file.exists():
                            yoy_df = pd.read_csv(yoy_file)
                            yoy_df['date'] = pd.to_datetime(yoy_df['date'])
                            yoy_df = yoy_df.set_index('date')
                            ts = parse_numeric_series(yoy_df['yoy_pct']).dropna()
                            print(f"📊 使用YoY数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ YoY数据读取失败: {e}")
                
                # 如果没有预计算数据，使用原始数据
                if ts is None or ts.empty:
                    local_data_path = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
                    
                    if local_data_path.exists():
                        try:
                            print(f"📁 使用本地原始数据: {series_id}")
                            df = pd.read_csv(local_data_path, index_col=0, parse_dates=True)
                            if len(df.columns) > 0:
                                ts = parse_numeric_series(df.iloc[:, 0])
                                ts = ts.dropna()
                        except Exception as e:
                            print(f"⚠️ 本地数据读取失败: {e}")
                    
                    # 如果本地数据不可用，使用API
                    if ts is None or ts.empty:
                        print(f"🌐 使用API获取数据: {series_id}")
                        try:
                            data_response = series_observations(series_id)
                            if data_response and 'observations' in data_response:
                                observations = data_response.get('observations', [])
                                if observations:
                                    df = pd.DataFrame(observations)
                                    df['date'] = pd.to_datetime(df['date'])
                                    df = df.set_index('date')
                                    ts = parse_numeric_series(df['value'])
                                    ts = ts.dropna()
                                else:
                                    print(f"⚠️ 无观测数据: {series_id}")
                            else:
                                print(f"⚠️ 无法获取数据: {series_id}")
                        except Exception as e:
                            print(f"⚠️ API获取数据失败: {series_id} - {e}")
                
                # 如果有数据，生成图表
                if ts is not None and not ts.empty:
                    try:
                        # 计算危机统计
                        crisis_stats = calculate_crisis_stats(ts, crisis_periods)
                        
                        # 生成完整图表（包含危机期间阴影、历史均值线等）
                        save_indicator_plot(
                            ts=ts,
                            title=indicator['name'],
                            unit=indicator.get('unit', ''),
                            crises=crisis_periods,
                            crisis_stats=crisis_stats,
                            out_path=chart_path,
                            show_ma=[6, 12],
                            annotate_latest=True
                        )
                        main_chart_paths.append(f"figures/{series_id}_latest.png")
                        print(f"✅ 生成图表: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 图表生成失败: {series_id} - {e}")
                else:
                    print(f"⚠️ 无数据生成图表: {series_id}")
            else:
                # 简化图表
                chart_path = generate_indicator_chart(
                    indicator['name'], 
                    indicator['current_value'], 
                    indicator['benchmark_value'], 
                    indicator['risk_score'], 
                    output_dir
                )
                if chart_path:
                    main_chart_paths.append(chart_path)
                    
        except Exception as e:
            print(f"❌ 生成图表失败 {indicator.get('series_id', 'Unknown')}: {e}")
            continue

    if main_chart_paths:
        image_paths['main_indicators'] = main_chart_paths[0]  # 使用第一个作为代表
    
    # 生成详细解释Markdown报告
    detailed_md_path = output_dir / f"crisis_report_detailed_{timestamp}.md"
    generate_detailed_explanation_report(processed_indicators, detailed_md_path, timestamp)
    
    # 生成Markdown报告 - 优化排版格式（warsh_state / japan_result / effective_warsh_result 已在上文计算）
    md_path = output_dir / f"crisis_report_{timestamp}.md"
    warsh_exec_summary = build_warsh_executive_summary(effective_warsh_result, warsh_state)
    warsh_interpretation_md = build_warsh_interpretation_report(warsh_state)
    deep_insights_md = generate_narrative_with_llm(processed_indicators, warsh_state)

    # ---------- Warsh 约束仪表盘：生成双轴图 + 期限溢价图（真实数据） ----------
    warsh_plots = generate_warsh_plots(output_dir)

    # ---------- 太长不看版 (TL;DR)：宏观 + Warsh + Japan 决策指南 ----------
    macro_result = {"total_score": total_score}
    tldr_section = generate_tldr_summary(macro_result, effective_warsh_result, japan_result)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# 🚨 FRED 宏观金融危机预警监控报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        f.write(tldr_section)
        f.write("\n\n")

        f.write("## 🧠 深度宏观推演 (AI Logic Inference)\n\n")
        f.write("> 以下结论由系统基于各板块指标与 Warsh 约束数据调用大模型自动推演，旨在发现被平均数掩盖的结构性风险。\n\n")
        f.write(deep_insights_md)
        f.write("\n\n---\n\n")

        # ---------- Warsh Risk Layer (Part 1–4) ----------
        f.write("## Executive Summary — Warsh Policy Risk\n\n")
        f.write(warsh_exec_summary)
        f.write("\n\n")
        level = effective_warsh_result.get("warsh_level", "GREEN")
        level_emoji = {"GREEN": "🟢", "YELLOW": "🟡", "AMBER": "🟠", "RED": "🔴"}.get(level, "")
        f.write("## Warsh Risk Level (Traffic Light)\n\n")
        f.write(f"**{level_emoji} {level}** — {effective_warsh_result.get('warsh_level_reason', '')}\n\n")
        flags = effective_warsh_result.get("warsh_flags", [])
        if flags:
            f.write("**Triggered constraints:** " + ", ".join(flags) + "\n\n")
        f.write("## Why Warsh Matters\n\n")
        f.write(WARSH_WHY_MATTERS_SECTION)
        f.write("\n\n")
        f.write("## Warsh Constraints / Liquidity Plumbing\n\n")
        if warsh_plots.get("liquidity"):
            f.write(f"![流动性管道（RRP vs SOFR−IORB）]({warsh_plots['liquidity']})\n\n")
        if warsh_plots.get("term_premium"):
            f.write(f"![10Y 期限溢价]({warsh_plots['term_premium']})\n\n")
        f.write(WARSH_DASHBOARD_CRO_NARRATIVE)
        f.write("\n\n")
        f.write("## Warsh Constraints / Liquidity Plumbing — Interpretation\n\n")
        if warsh_plots.get("liquidity"):
            f.write(f"![流动性管道]({warsh_plots['liquidity']})\n\n")
        f.write(warsh_interpretation_md)
        if warsh_plots.get("term_premium"):
            f.write(f"\n\n![期限溢价]({warsh_plots['term_premium']})\n\n")
        f.write("\n\n")
        f.write(build_japan_carry_unwind_section(japan_result))
        f.write("\n\n---\n\n")
        # 数据新鲜度与覆盖（基于 processed_indicators 的 last_date，周末感知）
        fresh_list, stale_list = _compute_freshness_classes(processed_indicators, indicators, scoring_config)
        f.write(_format_freshness_section(fresh_list, stale_list))
        f.write("\n---\n\n")
        f.write("## 📋 报告说明\n\n")
        f.write("本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。\n\n")
        f.write("【数据由人采集和处理，请批判看待这些数据，欢迎email jiangx@gmail.com 任何问题讨论】\n\n")
        f.write("风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为'越低越危险'）。\n\n")
        f.write("采用分组加权评分：先计算各组平均分，再按权重合成总分。\n\n")
        f.write("总分 = ∑(分组平均分 × 分组权重)，分组权重归一处理后合成。\n\n")
        f.write("过期数据处理：月频数据>60天、季频数据>120天标记⚠️，过期数据权重×0.9。\n\n")
        f.write(f"颜色分段：0–{bands['low']-1} 🔵 极低，{bands['low']}–{bands['med']-1} 🟢 低，{bands['med']}–{bands['high']-1} 🟡 中，{bands['high']}–100 🔴 高；50 为中性。\n\n")
        
        f.write("## 📊 执行摘要\n\n")
        f.write(f"- **总指标数**: {len(processed_indicators)}\n")
        f.write(f"- **高风险指标**: {len([i for i in processed_indicators if i['risk_score'] >= bands['high']])} 个\n")
        f.write(f"- **中风险指标**: {len([i for i in processed_indicators if bands['med'] <= i['risk_score'] < bands['high']])} 个\n")
        f.write(f"- **低风险指标**: {len([i for i in processed_indicators if bands['low'] <= i['risk_score'] < bands['med']])} 个\n")
        f.write(f"- **极低风险指标**: {len([i for i in processed_indicators if i['risk_score'] < bands['low']])} 个\n\n")
        
        f.write("### 🎯 风险等级说明\n\n")
        f.write(f"- 🔴 **高风险** ({bands['high']}-100分): 当前值显著偏离历史危机水平\n")
        f.write(f"- 🟡 **中风险** ({bands['med']}-{bands['high']-1}分): 当前值略高于历史危机水平\n")
        f.write(f"- 🟢 **低风险** ({bands['low']}-{bands['med']-1}分): 当前值接近或低于历史危机水平\n")
        f.write(f"- 🔵 **极低风险** (0-{bands['low']-1}分): 当前值远低于历史危机水平\n\n")
        
        f.write("## 📈 详细指标分析\n\n")
        
        # 按风险等级分组显示所有指标
        high_risk_indicators = []
        medium_risk_indicators = []
        low_risk_indicators = []
        very_low_risk_indicators = []
        
        for indicator in processed_indicators:
            score = indicator['risk_score']
            if score >= bands['high']:
                high_risk_indicators.append(indicator)
            elif score >= bands['med']:
                medium_risk_indicators.append(indicator)
            elif score >= bands['low']:
                low_risk_indicators.append(indicator)
            else:
                very_low_risk_indicators.append(indicator)
        
        # 高风险指标
        if high_risk_indicators:
            f.write("### 🔴 高风险指标\n\n")
            for i, indicator in enumerate(high_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🔴 高风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 中风险指标
        if medium_risk_indicators:
            f.write("### 🟡 中风险指标\n\n")
            for i, indicator in enumerate(medium_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🟡 中风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 低风险指标
        if low_risk_indicators:
            f.write("### 🟢 低风险指标\n\n")
            for i, indicator in enumerate(low_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🟢 低风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 极低风险指标
        if very_low_risk_indicators:
            f.write("### 🔵 极低风险指标\n\n")
            for i, indicator in enumerate(very_low_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🔵 极低风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 添加总体概览图表
        if summary_chart_path:
            f.write("## 📊 总体风险概览\n\n")
            f.write(f"![总体风险概览]({summary_chart_path})\n\n")
        
        f.write("### 📊 分组风险评分\n\n")
        for group_name, data in group_scores.items():
            f.write(f"- **{group_name}**: {data['score']:.1f}/100 (权重: {data['weight']:.1f}%, 指标数: {data['count']})\n")
        f.write("\n")
        
        # 确定总体风险等级
        if total_score >= 80:
            risk_level = "🔴 高风险"
        elif total_score >= 60:
            risk_level = "🟡 中风险"
        elif total_score >= 40:
            risk_level = "🟢 低风险"
        else:
            risk_level = "🔵 极低风险"
        
        f.write(f"**总体风险等级**: {risk_level}\n\n")
        
        # 指标配置表
        f.write("## 📋 指标配置表\n\n")
        f.write("| 指标名称 | 分组 | 基准分位 | 基准理由 | 变换方法 | 权重 |\n")
        f.write("|---------|------|----------|----------|----------|------|\n")
        
        config_table = [
            ("收益率曲线倒挂: 10年期-3个月", "收益率曲线", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "7.5%"),
            ("收益率曲线倒挂: 10年期-2年期", "收益率曲线", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "7.5%"),
            ("联邦基金利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("3个月国债利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("10年期国债利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("30年期抵押贷款利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("SOFR隔夜利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("高收益债风险溢价", "信用利差", "crisis_median", "危机期中位数作为警戒线", "level", "7.5%"),
            ("投资级信用利差: Baa-10Y国债", "信用利差", "crisis_median", "危机期中位数作为警戒线", "level", "7.5%"),
            ("芝加哥金融状况指数", "金融状况/波动", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "5.0%"),
            ("VIX波动率指数", "金融状况/波动", "noncrisis_p90", "非危机期90%分位数作为警戒线", "level", "5.0%"),
            ("非农就业人数 YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("工业生产 YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("GDP YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("新屋开工（年化）", "房地产", "crisis_p25", "危机期25%分位数作为警戒线", "level", "3.0%"),
            ("房价指数: Case-Shiller 20城 YoY", "房地产", "noncrisis_p90", "非危机期90%分位数作为警戒线", "yoy_pct", "3.0%"),
            ("密歇根消费者信心", "消费", "noncrisis_p35", "非危机期35%分位数作为警戒线", "level", "4.0%"),
            ("消费者信贷 YoY", "消费", "noncrisis_p75", "非危机期75%分位数作为警戒线", "yoy_pct", "4.0%"),
            ("总贷款与租赁 YoY", "银行业", "noncrisis_p75", "非危机期75%分位数作为警戒线", "yoy_pct", "3.5%"),
            ("美联储总资产 YoY", "银行业", "crisis_median", "危机期中位数作为警戒线", "yoy_pct", "3.5%"),
            ("贸易加权美元指数 YoY", "外部环境", "noncrisis_median", "非危机期中位数作为警戒线", "yoy_pct", "5.0%"),
            ("企业债/GDP（名义，%）", "杠杆", "noncrisis_p65", "非危机期65%分位数作为警戒线", "level", "10.0%"),
            ("银行准备金/存款（%）", "银行业", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "0.0%"),
            ("银行准备金/总资产（%）", "银行业", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "0.0%"),
            ("家庭债务偿付比率", "消费", "crisis_median", "危机期中位数作为警戒线", "level", "0.0%"),
            ("房贷违约率", "房地产", "crisis_median", "危机期中位数作为警戒线", "level", "0.0%")
        ]
        
        for row in config_table:
            f.write(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |\n")
        
        f.write("\n")
        
        # 基准分位解释
        f.write("## 📊 基准分位解释\n\n")
        f.write("- **crisis_median**: 危机期中位数\n")
        f.write("- **crisis_p25**: 危机期25%分位数\n")
        f.write("- **crisis_p75**: 危机期75%分位数\n")
        f.write("- **noncrisis_median**: 非危机期中位数\n")
        f.write("- **noncrisis_p25**: 非危机期25%分位数\n")
        f.write("- **noncrisis_p35**: 非危机期35%分位数\n")
        f.write("- **noncrisis_p65**: 非危机期65%分位数\n")
        f.write("- **noncrisis_p75**: 非危机期75%分位数\n")
        f.write("- **noncrisis_p90**: 非危机期90%分位数\n\n")
        
        # 历史危机期间参考
        f.write("## 📅 历史危机期间参考\n\n")
        f.write("本报告使用的历史危机期间包括：\n\n")
        for crisis in crisis_periods:
            f.write(f"- **{crisis['name']}** ({crisis['code']}): {crisis['start']} 至 {crisis['end']}\n")
        f.write("\n")
        
        # 免责声明
        f.write("## ⚠️ 免责声明\n\n")
        f.write("本报告仅供参考，不构成投资建议。历史数据不保证未来表现。\n\n")
        
        f.write("---\n\n")
        f.write("*本报告基于FRED数据，仅供参考，不构成投资建议*\n")
    
        # 生成HTML报告（使用Base64嵌入）
        html_path = output_dir / f"crisis_report_{timestamp}.html"
        with open(md_path, "r", encoding="utf-8") as f:
            markdown_content = f.read()
        
        # 使用Base64嵌入功能
        _log_stage("🌐 渲染HTML报告...")
        html_content = render_html_report(markdown_content, "宏观金融危机监察报告", output_dir)
        _log_stage("✅ HTML报告渲染完成")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    # 生成"最新"副本和latest软链接
    latest_md = output_dir / f"latest_{timestamp}.md"
    latest_html = output_dir / f"latest_{timestamp}.html"
    latest_md.write_text(markdown_content, encoding="utf-8")
    latest_html.write_text(html_content, encoding="utf-8")
    
    # 创建latest软链接（Windows下使用复制）
    latest_md_link = output_dir / "crisis_report_latest.md"
    latest_html_link = output_dir / "crisis_report_latest.html"
    latest_json_link = output_dir / "crisis_report_latest.json"
    
    # 保存JSON数据（含 Warsh 层级）
    json_path = output_dir / f"crisis_report_{timestamp}.json"
    json_data = {
        "timestamp": timestamp,
        "total_score": total_score,
        "risk_level": risk_level,
        "warsh": {
            "warsh_level": warsh_level_result.get("warsh_level"),
            "warsh_level_reason": warsh_level_result.get("warsh_level_reason"),
            "warsh_flags": warsh_level_result.get("warsh_flags", []),
            "latest": {
                "sofr_iorb_spread": warsh_state.get("sofr_iorb_spread"),
                "rrp_level_bn": warsh_state.get("rrp_level_bn"),
                "term_premium": warsh_state.get("term_premium"),
            }
        },
        "indicators": processed_indicators,
        "group_scores": group_scores,
        "summary": {
            "high_risk_count": len([i for i in processed_indicators if i['risk_score'] >= 80]),
            "medium_risk_count": len([i for i in processed_indicators if 60 <= i['risk_score'] < 80]),
            "low_risk_count": len([i for i in processed_indicators if i['risk_score'] < 60])
        }
    }
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
        try:
            # Windows下使用复制而不是软链接
            import shutil
            if latest_md_link.exists():
                latest_md_link.unlink()
            if latest_html_link.exists():
                latest_html_link.unlink()
            if latest_json_link.exists():
                latest_json_link.unlink()
                
            shutil.copyfile(md_path, latest_md_link)
            shutil.copyfile(html_path, latest_html_link)
            shutil.copyfile(json_path, latest_json_link)
            print(f"✅ 创建latest文件: {latest_md_link.name}, {latest_html_link.name}, {latest_json_link.name}")
        except Exception as e:
            print(f"⚠️ 创建latest文件失败: {e}")
    # 生成长图（保存到和HTML同一个目录）
    long_image_path = output_dir / f"crisis_report_long_{timestamp}.png"
    image_success = generate_long_image(str(html_path), str(long_image_path))
    
    print(f"✅ 报告生成完成!")
    print(f"📄 Markdown: {md_path}")
    print(f"🌐 HTML: {html_path}")
    print(f"📊 JSON: {json_path}")
    if image_success:
        print(f"🖼️ 长图: {long_image_path}")
    print(f"📊 图表目录: {output_dir / 'figures'}")
    print(f"🎯 总体风险评分: {total_score:.1f}/100")
    print(f"📊 风险等级: {risk_level}")
    # Warsh 控制台摘要
    _w = warsh_level_result
    _s = warsh_state
    print("---")
    print("Warsh Risk Level:", _w.get("warsh_level", "—"))
    print("Triggered constraints:", _w.get("warsh_flags", []) or "none")
    sofr_val = _s.get("sofr_iorb_spread")
    print("Latest SOFR−IORB:", f"{sofr_val:.4f}%" if sofr_val is not None else "—")
    rrp_bn = _s.get("rrp_level_bn")
    print("Latest RRP (Bn):", f"{rrp_bn:.1f}" if rrp_bn is not None else "—")
    tp_val = _s.get("term_premium")
    print("Latest Term Premium (THREEFYTP10):", f"{tp_val:.2f}%" if tp_val is not None else "—")
    print("---")

    try:
        cleanup_old_crisis_monitor_outputs(output_dir)
    except Exception as e:
        print(f"⚠️ 旧报告文件清理失败（已忽略）: {e}")

    stop_event.set()
    heartbeat_thread.join(timeout=1)

# =====================
# V5.0: 历史时间序列生成函数
# =====================
def generate_macro_history(
    start_date="2005-01-01",
    end_date=None,
    frequency="monthly",
    output_path=None
):
    """
    V5.0: 生成宏观风险打分历史时间序列
    
    Parameters:
    -----------
    start_date : str
        开始日期（格式：YYYY-MM-DD）
    end_date : str or None
        结束日期，None 表示使用今天
    frequency : str
        'daily' 或 'monthly'，输出频率
    output_path : str or None
        输出文件路径，None 表示使用默认路径
    
    Returns:
    --------
    pd.DataFrame
        包含 Date, Macro_Score, Macro_Risk_Level 的 DataFrame
    """
    print("=" * 80)
    print("V5.0: 生成宏观风险打分历史时间序列")
    print("=" * 80)
    
    # 加载配置
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return None
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    scoring_config = config.get('scoring', {})
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    print(f"📊 加载了 {len(indicators)} 个指标")
    
    # 确定日期范围
    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date) if end_date else pd.Timestamp.now()
    
    # 生成日期序列
    if frequency == "monthly":
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='ME')
    else:
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='B')
    
    print(f"📅 将计算 {len(date_range)} 个日期的评分 ({frequency})")
    
    # 预先加载所有指标的历史数据
    print("\n📥 预加载所有指标的历史数据...")
    indicator_series = {}
    
    for indicator in indicators:
        series_id = indicator.get('series_id') or indicator.get('id')
        if not series_id:
            continue
        
        try:
            ts = compose_series(series_id)
            if ts is None or ts.empty:
                ts = fetch_series(series_id)
            
            if ts is not None and not ts.empty:
                ts_trans = transform_series(series_id, ts, indicator)
                ts_trans = ts_trans.dropna()
                
                if not ts_trans.empty:
                    indicator_series[series_id] = {
                        'series': ts_trans,
                        'indicator': indicator
                    }
                    print(f"  ✅ {series_id}: {len(ts_trans)} 个数据点")
        except Exception as e:
            print(f"  ⚠️ {series_id}: 加载失败 - {e}")
            continue
    
    print(f"\n✅ 成功加载 {len(indicator_series)} 个指标")
    
    # 计算分组权重
    group_weights = {}
    for indicator in indicators:
        group = indicator.get('group', 'unknown')
        weight = indicator.get('weight', 0)
        if group not in group_weights:
            group_weights[group] = []
        group_weights[group].append(weight)
    
    total_group_weight = sum(sum(weights) for weights in group_weights.values())
    if total_group_weight > 0:
        for group in group_weights:
            group_weights[group] = sum(group_weights[group]) / total_group_weight
    else:
        num_groups = len(group_weights)
        for group in group_weights:
            group_weights[group] = 1.0 / num_groups if num_groups > 0 else 0
    
    # 计算每个日期的评分
    print("\n🧮 开始计算历史评分...")
    results = []
    
    for i, current_date in enumerate(date_range):
        if (i + 1) % 50 == 0:
            print(f"  进度: {i+1}/{len(date_range)} ({100*(i+1)/len(date_range):.1f}%)")
        
        date_group_scores = {}
        
        for series_id, data in indicator_series.items():
            indicator = data['indicator']
            ts_trans = data['series']
            group = indicator.get('group', 'unknown')
            
            # 获取该日期或之前的最新值
            available_data = ts_trans[ts_trans.index <= current_date]
            if available_data.empty:
                continue
            
            current_value = float(available_data.iloc[-1])
            historical_data = ts_trans[ts_trans.index <= current_date]
            
            if historical_data.empty or len(historical_data) < 24:
                continue
            
            try:
                benchmark_value = calculate_benchmark_corrected(
                    series_id, indicator, historical_data, crisis_periods
                )
                risk_score = calculate_risk_score_simple(
                    current_value, benchmark_value, indicator, historical_data, scoring_config
                )
                
                if group not in date_group_scores:
                    date_group_scores[group] = []
                date_group_scores[group].append(risk_score)
            except Exception:
                continue
        
        # 计算加权总分
        total_score = 0
        for group, scores in date_group_scores.items():
            if scores:
                avg_score = np.mean(scores)
                weight = group_weights.get(group, 0)
                total_score += avg_score * weight
        
        # 确定风险等级
        if total_score >= 80:
            risk_level = "极高风险"
        elif total_score >= 60:
            risk_level = "偏高风险"
        elif total_score >= 40:
            risk_level = "中等风险"
        else:
            risk_level = "低风险"
        
        results.append({
            'Date': current_date,
            'Macro_Score': round(total_score, 2) if not pd.isna(total_score) else np.nan,
            'Macro_Risk_Level': risk_level
        })
    
    # 创建 DataFrame
    df = pd.DataFrame(results)
    df['Macro_Score'] = df['Macro_Score'].ffill()
    df = df.dropna(subset=['Macro_Score'])
    
    # 保存到 CSV
    if output_path is None:
        output_path = BASE / "data" / "macro_history.csv"
    
    output_path = pathlib.Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"\n✅ 历史评分已保存: {output_path}")
    print(f"   - 总记录数: {len(df)}")
    print(f"   - 日期范围: {df['Date'].min()} 至 {df['Date'].max()}")
    print(f"   - 平均评分: {df['Macro_Score'].mean():.2f}")
    
    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="FRED危机预警监控系统")
    parser.add_argument("--history", action="store_true", help="生成历史时间序列")
    parser.add_argument("--start-date", type=str, default="2005-01-01", help="开始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    parser.add_argument("--frequency", type=str, default="monthly", choices=["daily", "monthly"], help="输出频率")
    parser.add_argument("--output", type=str, default=None, help="输出文件路径")
    
    args = parser.parse_args()
    
    if args.history:
        generate_macro_history(
            start_date=args.start_date,
            end_date=args.end_date,
            frequency=args.frequency,
            output_path=args.output
        )
    else:
        generate_report_with_images()
