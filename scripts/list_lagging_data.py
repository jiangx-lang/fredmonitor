#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自检：列出所有滞后数据。
用法：先运行 sync_fred_http.py 下载数据，再运行本脚本。
"""
import os
import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

FRED_SERIES_ROOT = BASE / "data" / "fred" / "series"
YAHOO_SERIES_ROOT = BASE / "data" / "yahoo" / "series"

# 合成指标（Yahoo）对应的底层 ticker，用于检查 Yahoo 缓存
YAHOO_SYNTHETIC_TICKERS = [
    "HYG", "LQD", "KRE", "SPY", "_GSPC", "XLF", "QQQ", "BTC-USD",
    "DX-Y.NYB", "UUP", "TLT", "GLD", "USO", "_VIX", "_VIX3M", "_VIX6M", "_VIX9D",
]


def load_catalog_freshness():
    """从 catalog_fred.yaml 加载 series_id -> freshness_days。"""
    catalog_path = BASE / "config" / "catalog_fred.yaml"
    out = {}
    if not catalog_path.exists():
        return out
    with open(catalog_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for item in (data.get("series") or []):
        sid = item.get("id")
        if sid:
            out[sid] = item.get("freshness_days", 7)
    return out


def load_crisis_indicator_ids():
    """从 crisis_indicators.yaml 加载所有指标 id。"""
    config_path = BASE / "config" / "crisis_indicators.yaml"
    ids = set()
    if not config_path.exists():
        return ids
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for ind in (data.get("indicators") or []):
        sid = ind.get("id") or ind.get("series_id")
        if sid:
            ids.add(sid.strip().upper())
    return ids


def read_fred_last_date(series_id: str) -> tuple:
    """
    读 FRED raw.csv，返回 (last_date, days_old)。
    优先使用 value 列（与 crisis_monitor._fred_cache_value_column 一致）。
    """
    raw_file = FRED_SERIES_ROOT / series_id / "raw.csv"
    if not raw_file.exists():
        return None, None
    try:
        df = pd.read_csv(raw_file, index_col=0, parse_dates=True)
        if df.empty:
            return None, None
        if "value" in df.columns:
            col = df["value"]
        elif series_id in df.columns:
            col = df[series_id]
        else:
            col = df.iloc[:, 0]
        s = pd.to_numeric(col, errors="coerce").dropna()
        if s.empty:
            return None, None
        last_date = pd.to_datetime(s.index[-1])
        if getattr(last_date, "tz", None):
            last_date = last_date.tz_localize(None)
        days_old = (pd.Timestamp.now() - last_date).days
        return last_date.date(), days_old
    except Exception as e:
        print(f"  [读失败] {series_id}: {e}", file=sys.stderr)
        return None, None


def read_yahoo_last_date(safe_symbol: str) -> tuple:
    """读 Yahoo raw.csv，返回 (last_date, days_old)。"""
    raw_file = YAHOO_SERIES_ROOT / safe_symbol / "raw.csv"
    if not raw_file.exists():
        return None, None
    try:
        df = pd.read_csv(raw_file, index_col=0, parse_dates=True)
        if df.empty:
            return None, None
        last_date = pd.to_datetime(df.index[-1])
        if getattr(last_date, "tz", None):
            last_date = last_date.tz_localize(None)
        days_old = (pd.Timestamp.now() - last_date).days
        return last_date.date(), days_old
    except Exception:
        return None, None


def main():
    print("=" * 70)
    print("数据自检：滞后数据列表")
    print("=" * 70)
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    freshness = load_catalog_freshness()
    crisis_ids = load_crisis_indicator_ids()

    # 只检查在 crisis 里且本地有 FRED 目录的序列（或 catalog 里有的）
    fred_ids = set()
    for sid in crisis_ids:
        if (FRED_SERIES_ROOT / sid / "raw.csv").exists():
            fred_ids.add(sid)
        # 也包含 catalog 里有的（可能 crisis 用派生名）
        for cid in freshness:
            if cid not in crisis_ids:
                if (FRED_SERIES_ROOT / cid / "raw.csv").exists():
                    fred_ids.add(cid)
    fred_ids = sorted(fred_ids)

    rows = []
    for series_id in fred_ids:
        last_date, days_old = read_fred_last_date(series_id)
        if last_date is None:
            rows.append((series_id, "FRED", None, None, None, "无数据/读失败"))
            continue
        fd = freshness.get(series_id, 7)
        lag = "是" if days_old > fd else "否"
        rows.append((series_id, "FRED", str(last_date), days_old, fd, lag))

    # Yahoo 缓存（合成指标底层）
    print("【1】FRED 序列（crisis 相关）")
    print("-" * 70)
    lagging_fred = [r for r in rows if r[5] == "是"]
    if lagging_fred:
        print(f"{'series_id':<22} {'来源':<6} {'最后观测日':<12} {'滞后天数':>8} {'阈值':>6} {'滞后':>4}")
        for r in lagging_fred:
            print(f"{r[0]:<22} {r[1]:<6} {r[2] or '—':<12} {r[3] or 0:>8} {r[4] or 0:>6} {r[5]:>4}")
        print(f"\n共 {len(lagging_fred)} 个 FRED 序列滞后（超过各自 freshness_days 阈值）。")
    else:
        print("无 FRED 序列滞后（均在阈值内）。")

    print("\n【2】Yahoo 缓存（合成指标底层 ticker）")
    print("-" * 70)
    yahoo_rows = []
    for ticker in YAHOO_SYNTHETIC_TICKERS:
        last_date, days_old = read_yahoo_last_date(ticker)
        fd = 3  # YAHOO_CACHE_EXPIRY_DAYS
        if last_date is None:
            yahoo_rows.append((ticker, "—", None, "无文件"))
        else:
            lag = "是" if days_old > fd else "否"
            yahoo_rows.append((ticker, str(last_date), days_old, lag))
    yahoo_lag = [r for r in yahoo_rows if r[3] == "是"]
    if yahoo_lag:
        print(f"{'ticker':<14} {'最后观测日':<12} {'滞后天数':>8} {'滞后(>3d)':>10}")
        for r in yahoo_lag:
            print(f"{r[0]:<14} {r[1]:<12} {r[2] or 0:>8} {r[3]:>10}")
        print(f"\n共 {len(yahoo_lag)} 个 Yahoo 缓存滞后。")
    else:
        print("无 Yahoo 缓存滞后（均在 3 天内）。")

    # 全表：所有 FRED 序列（含未滞后的），便于核对
    print("\n【3】FRED 全部序列（最后观测日 + 是否滞后）")
    print("-" * 70)
    print(f"{'series_id':<22} {'最后观测日':<12} {'滞后天数':>8} {'阈值':>6} {'滞后':>4}")
    for r in sorted(rows, key=lambda x: (x[5] == "否", -(x[3] or 0))):
        print(f"{r[0]:<22} {r[2] or '—':<12} {r[3] or 0:>8} {r[4] or 0:>6} {r[5]:>4}")
    print("\n自检完成。")


if __name__ == "__main__":
    main()
