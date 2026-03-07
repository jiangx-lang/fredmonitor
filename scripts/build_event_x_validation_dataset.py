#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 历史验证数据集构建：按模板产出三张表。
- event_x_validation_dataset.csv：日频原始/主腿，2021-01-01 至今
- event_x_validation_features.csv：衍生特征
- event_x_validation_scenarios.csv：四类场景标签

数据源优先级：FRED > EIA(Brent) > Yahoo(BIZD)。所有外部源 fail-open，缺失不阻断。
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# 项目根
BASE = Path(__file__).resolve().parent.parent
FRED_SERIES_ROOT = BASE / "data" / "fred" / "series"
OUT_DIR = BASE / "data" / "event_x_validation"
STALE_DAYS_BREAKEVEN = 5
START_DATE = "2021-01-01"


def _load_fred_csv(series_id: str, value_col: str | None = None) -> pd.Series | None:
    """从 data/fred/series/{id}/raw.csv 读取，返回 date 索引的 Series。fail-open。"""
    p = FRED_SERIES_ROOT / series_id / "raw.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
        if "date" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.set_index("date").sort_index()
        # 取值列：value_col 指定 / 或与 series_id 同名的列 / 或 value
        if value_col and value_col in df.columns:
            col = value_col
        elif series_id in df.columns:
            col = series_id
        elif "value" in df.columns:
            col = "value"
        else:
            col = [c for c in df.columns if c != "date"][0] if len(df.columns) > 1 else df.columns[0]
        s = pd.to_numeric(df[col], errors="coerce")
        return s.dropna().reindex(s.index).sort_index()
    except Exception:
        return None


def _fetch_bizd_yahoo(start: str, end: str) -> pd.Series | None:
    """Yahoo Finance BIZD 历史收盘价。fail-open。"""
    try:
        import yfinance as yf
        t = yf.Ticker("BIZD")
        hist = t.history(start=start, end=end, auto_adjust=True)
        if hist is None or hist.empty:
            return None
        s = hist["Close"].copy()
        s.index = pd.to_datetime(s.index).tz_localize(None)
        return s
    except Exception:
        return None


def _align_to_calendar(series_dict: dict[str, pd.Series], start: str, end: str) -> pd.DataFrame:
    """按日历对齐：以 date 为索引，左连接各序列，前向填充周频等。"""
    ix = pd.date_range(start=start, end=end, freq="D")
    out = pd.DataFrame(index=ix)
    for name, s in series_dict.items():
        if s is None or s.empty:
            out[name] = np.nan
            continue
        s = s[~s.index.duplicated(keep="last")]
        reindexed = s.reindex(ix, method="ffill")
        out[name] = reindexed.values
    out.index.name = "date"
    return out


def build_raw_dataset(start: str = START_DATE, end: str | None = None) -> pd.DataFrame:
    """构建 event_x_validation_dataset 原始表。"""
    end = end or datetime.now().strftime("%Y-%m-%d")
    # FRED 主腿
    hy_oas = _load_fred_csv("BAMLH0A0HYM2")
    stlfsi4 = _load_fred_csv("STLFSI4")
    brent_spot = _load_fred_csv("DCOILBRENTEU", "DCOILBRENTEU")
    t5yie = _load_fred_csv("T5YIE", "T5YIE")
    if t5yie is None:
        t5yie = _load_fred_csv("T5YIE")
    vix = _load_fred_csv("VIXCLS")
    dgs5 = _load_fred_csv("DGS5", "DGS5")
    t5yifr = _load_fred_csv("T5YIFR", "T5YIFR")  # 可能不存在，则 NaN
    if t5yifr is None:
        t5yifr = _load_fred_csv("T5YIFR")

    bizd = _fetch_bizd_yahoo(start, end)

    raw = _align_to_calendar(
        {
            "hy_oas": hy_oas,
            "stlfsi4": stlfsi4,
            "bizd_close": bizd,
            "brent_spot": brent_spot,
            "t5yie": t5yie,
            "vix": vix,
            "dgs5": dgs5,
            "t5yifr": t5yifr,
        },
        start,
        end,
    )
    # breakeven_effective: 优先 t5yie；若 stale 则 dgs5 - t5yifr
    t5yie_last_valid = raw["t5yie"].last_valid_index() if raw["t5yie"].notna().any() else None
    raw["breakeven_effective"] = np.nan
    if t5yie is not None and not t5yie.dropna().empty:
        raw["breakeven_effective"] = raw["t5yie"]
    # Stale：若 t5yie 最后观测距今天 > STALE_DAYS_BREAKEVEN，用 proxy
    cutoff = pd.Timestamp(end) - pd.Timedelta(days=STALE_DAYS_BREAKEVEN)
    use_proxy = t5yie_last_valid is None or pd.Timestamp(t5yie_last_valid) < cutoff
    if use_proxy and dgs5 is not None and t5yifr is not None:
        proxy = raw["dgs5"] - raw["t5yifr"]
        raw.loc[raw["breakeven_effective"].isna(), "breakeven_effective"] = proxy
    raw = raw.reset_index()
    return raw


def build_features(dataset: pd.DataFrame) -> pd.DataFrame:
    """从 dataset 计算 event_x_validation_features。"""
    df = dataset.set_index("date").copy()
    # Private Credit 衍生
    df["hy_oas_5d_bp_change"] = (df["hy_oas"] - df["hy_oas"].shift(5)) * 100
    df["bizd_50dma"] = df["bizd_close"].rolling(50, min_periods=1).mean()
    df["bizd_vs_50dma_pct"] = (df["bizd_close"] / df["bizd_50dma"] - 1) * 100
    df["stlfsi4_4w_change"] = df["stlfsi4"] - df["stlfsi4"].shift(20)
    # Geopolitics 衍生
    df["brent_yoy_pct"] = df["brent_spot"].pct_change(252) * 100
    df["vix_5d_change"] = df["vix"] - df["vix"].shift(5)
    # breakeven 来源与质量（按当前 run 的 T5YIE 是否 stale 判定）
    t5yie_last = df["t5yie"].last_valid_index()
    cutoff = df.index.max() - pd.Timedelta(days=STALE_DAYS_BREAKEVEN) if len(df.index) else None
    if t5yie_last is not None and cutoff is not None and pd.Timestamp(t5yie_last) >= cutoff:
        src, stale, quality = "FRED_T5YIE", False, "HIGH"
    else:
        has_proxy = df["dgs5"].notna().any() and df["t5yifr"].notna().any()
        src = "REALTIME_PROXY" if has_proxy else "NONE"
        stale, quality = True, "MEDIUM" if has_proxy else "LOW"
    df["breakeven_source_used"] = src
    df["breakeven_is_stale"] = stale
    df["breakeven_quality"] = quality
    # 规则依赖列：占位，由后续按日重放 Event-X 规则填充
    df["private_credit_watch_flag"] = np.nan
    df["geopolitics_watch_flag"] = np.nan
    df["resonance_level"] = ""
    df["credit_stress_on"] = np.nan
    return df.reset_index()


def build_scenarios_csv() -> pd.DataFrame:
    """四类场景标签表。"""
    rows = [
        {
            "scenario_name": "Oil shock / 油价冲击期",
            "date_start": "2022-02-01",
            "date_end": "2022-06-30",
            "scenario_type": "oil_shock",
            "expected_behavior": "Geopolitics Radar 在油价与通胀预期抬升期间或之前进入 WATCH，非仅事后才 HIGH。",
            "notes": "2022 年俄乌冲突后油价与通胀预期抬升；验证 Geopolitics 是否提前或同步反应。",
        },
        {
            "scenario_name": "Credit widening / 信用利差快速走阔期",
            "date_start": "2020-03-01",
            "date_end": "2020-06-30",
            "scenario_type": "credit_widening",
            "expected_behavior": "Private Credit 在绝对阈值触发前可因 HY 5D 动量或 BIZD 弱势先亮 WATCH。",
            "notes": "COVID 信用冲击；HY OAS 快速拉大窗口。",
        },
        {
            "scenario_name": "Volatility rise without full resonance / 波动上升但未共振",
            "date_start": "2021-09-01",
            "date_end": "2021-11-30",
            "scenario_type": "vol_no_resonance",
            "expected_behavior": "仅 Geopolitics WATCH 或 PARTIAL，不误判 RED ALERT；completeness 非 HIGH；summary 显式 VIX-led。",
            "notes": "VIX 上行但 Brent/breakeven/credit 未同步确认。",
        },
        {
            "scenario_name": "True resonance / 真实共振压力期",
            "date_start": "2020-03-01",
            "date_end": "2020-05-31",
            "scenario_type": "true_resonance",
            "expected_behavior": "Resonance 升级（非 OFF）；至少一雷达 WATCH/ALERT。",
            "notes": "信用+通胀预期+流动性恶化+credit_stress=ON。",
        },
    ]
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Event-X validation dataset (3 CSVs)")
    parser.add_argument("--start", default=START_DATE, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help="Output directory")
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    print("Building raw dataset...")
    raw = build_raw_dataset(start=args.start, end=args.end)
    path_dataset = args.out_dir / "event_x_validation_dataset.csv"
    raw.to_csv(path_dataset, index=False, encoding="utf-8")
    print(f"  -> {path_dataset} ({len(raw)} rows)")

    print("Building features...")
    features = build_features(raw)
    path_features = args.out_dir / "event_x_validation_features.csv"
    features.to_csv(path_features, index=False, encoding="utf-8")
    print(f"  -> {path_features} ({len(features)} rows)")

    scenarios = build_scenarios_csv()
    path_scenarios = args.out_dir / "event_x_validation_scenarios.csv"
    scenarios.to_csv(path_scenarios, index=False, encoding="utf-8")
    print(f"  -> {path_scenarios} ({len(scenarios)} rows)")

    print("Done.")


if __name__ == "__main__":
    main()
