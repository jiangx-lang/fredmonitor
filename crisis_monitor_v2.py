#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统 V2.0（早预警版）
- 在原有输出结构上增强评分逻辑：Level + Change
- 新增早预警指数、确认矩阵、广度指标
- 保持 HTML/PNG/JSON 产出兼容
"""
from __future__ import annotations

import json
import pathlib
import re
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

import crisis_monitor as base

V2_SUMMARY: Dict[str, object] = {}
_BASE_COMPOSE = base.compose_series
SERIES_SOURCES: Dict[str, str] = {}
DATA_ERRORS: Dict[str, str] = {}


def _log_v2_stage(message: str) -> None:
    timestamp = pd.Timestamp.now(tz=base.JST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

SENSITIVITY_PROFILES: Dict[str, dict] = {
    "conservative": {
        "label": "Conservative",
        "credit_breadth_threshold": 0.3,
        "real_breadth_threshold": 0.35,
        "slow_macro_watch_threshold": 55.0,
        "fast_ew_window": 6,
        "fast_ew_required": 4,
        "confirm_min_signals": 2,
        "watch_window_days": 5,
    },
    "base": {
        "label": "Base",
        "credit_breadth_threshold": 0.2,
        "real_breadth_threshold": 0.25,
        "slow_macro_watch_threshold": 50.0,
        "fast_ew_window": 5,
        "fast_ew_required": 3,
        "confirm_min_signals": 2,
        "watch_window_days": 5,
    },
    "aggressive": {
        "label": "Aggressive",
        "credit_breadth_threshold": 0.15,
        "real_breadth_threshold": 0.2,
        "slow_macro_watch_threshold": 45.0,
        "fast_ew_window": 4,
        "fast_ew_required": 2,
        "confirm_min_signals": 2,
        "watch_window_days": 5,
    },
}

INDICATOR_METADATA: Dict[str, dict] = {
    "NCBDBIQ027S": {
        "expected_units": "%",
        "expected_value_range": (0.0, 100.0),
        "transform_chain": "corp_debt/gdp*100",
    },
    "CORPDEBT_GDP_PCT": {
        "expected_units": "%",
        "expected_value_range": (0.0, 100.0),
        "transform_chain": "corp_debt/gdp*100",
    },
    "VIX_TERM_STRUCTURE": {
        "expected_units": "ratio",
        "expected_value_range": (0.5, 2.0),
        "transform_chain": "VIX/VIX3M (or VIX9D/VIX fallback)",
    },
    "HY_OAS_MOMENTUM_RATIO": {
        "expected_units": "ratio",
        "expected_value_range": (0.5, 2.0),
        "transform_chain": "BAMLH0A0HYM2/MA20",
    },
    "HYG_LQD_RATIO": {
        "expected_units": "ratio",
        "expected_value_range": (0.6, 1.6),
        "transform_chain": "HYG/LQD",
    },
    "KRE_SPY_RATIO": {
        "expected_units": "ratio",
        "expected_value_range": (0.2, 1.2),
        "transform_chain": "KRE/SPY",
    },
    "XLF_SPY_RATIO": {
        "expected_units": "ratio",
        "expected_value_range": (0.03, 0.2),
        "transform_chain": "XLF/SPY (price ratio)",
    },
    "BTC_QQQ_RATIO": {
        "expected_units": "ratio",
        "expected_value_range": (20.0, 300.0),
        "transform_chain": "BTC-USD/QQQ (price ratio)",
    },
    "DXY_CHANGE": {
        "expected_units": "pct",
        "expected_value_range": (-10.0, 10.0),
        "transform_chain": "DXY 5d pct change (fallback UUP)",
    },
    "CROSS_ASSET_CORR_STRESS": {
        "expected_units": "score",
        "expected_value_range": (0.0, 1.0),
        "transform_chain": "avg corr + cash-king composite",
    },
}


def get_metadata(series_id: str) -> dict:
    return INDICATOR_METADATA.get(series_id.upper(), {
        "expected_units": None,
        "expected_value_range": None,
        "transform_chain": None,
    })


def _parse_numeric_series(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")
    return series.replace([np.inf, -np.inf], np.nan)


def _mark_source(series_id: str, source: str) -> None:
    SERIES_SOURCES[series_id] = source


def _fetch_yahoo_series(symbol: str) -> pd.Series:
    ts = base.fetch_yahoo_safe(symbol)
    if ts is not None and not ts.empty:
        return ts
    return pd.Series(dtype="float64")


def _ratio_series(numer: pd.Series, denom: pd.Series) -> pd.Series:
    if numer is None or denom is None or numer.empty or denom.empty:
        return pd.Series(dtype="float64")
    numer, denom = numer.align(denom, join="inner")
    return (numer / denom).dropna()


def _validate_value(series_id: str, current_value: float, ts_trans: pd.Series) -> Optional[str]:
    meta = get_metadata(series_id)
    expected_units = meta.get("expected_units")
    if expected_units == "ratio" and current_value < 0:
        return f"ratio应为非负 (current={current_value:.4g})"
    expected_range = meta.get("expected_value_range")
    if expected_range:
        low, high = expected_range
        if current_value < low or current_value > high * 5:
            return f"值超出预期范围 {expected_range} (current={current_value:.4g})"

    if ts_trans is not None and not ts_trans.empty:
        median = float(np.nanmedian(ts_trans.values))
        if median != 0:
            ratio = abs(current_value / median)
            if ratio >= 1000 or ratio <= 0.001:
                return f"疑似尺度异常 (current/median={ratio:.2e})"
    return None


def _freshness_factor(last_date: Optional[str], freq: str) -> float:
    if not last_date:
        return 0.5
    try:
        dt = pd.to_datetime(last_date).date()
    except Exception:
        return 0.5
    today = pd.Timestamp.now(tz=base.JST).date()
    lag = max(0, (today - dt).days)
    f = (freq or "D").upper()
    if f.startswith("D"):
        if lag <= 1:
            return 1.0
        return max(0.5, 1.0 - (lag - 1) / 7 * 0.5)
    if f.startswith("W"):
        if lag <= 7:
            return 1.0
        return max(0.6, 1.0 - (lag - 7) / 30 * 0.4)
    if f.startswith("M"):
        if lag <= 20:
            return 1.0
        return max(0.2, 1.0 - (lag - 20) / 40 * 0.8)
    if f.startswith("Q"):
        if lag <= 45:
            return 1.0
        return max(0.1, 1.0 - (lag - 45) / 75 * 0.9)
    return 0.5


def _sigmoid(z: float) -> float:
    return 1.0 / (1.0 + np.exp(-z))


def _infer_lookbacks(idx: pd.DatetimeIndex, freq_hint: Optional[str] = None) -> Tuple[int, int, int]:
    freq = (freq_hint or pd.infer_freq(idx) or "D").upper()
    if freq.startswith("Q"):
        return 2, 8, 2
    if freq.startswith("M"):
        return 3, 12, 3
    if freq.startswith("W"):
        return 8, 26, 8
    return 20, 60, 20


def _slope(values: np.ndarray) -> float:
    if len(values) < 2:
        return np.nan
    x = np.arange(len(values), dtype=float)
    return float(np.polyfit(x, values.astype(float), 1)[0])


def _score_from_z(z: float) -> float:
    if np.isnan(z):
        return 50.0
    return float(_sigmoid(z) * 100.0)


def compute_change_score(ts: pd.Series, indicator: dict, scoring_config: dict) -> float:
    ts = ts.dropna().astype(float)
    if ts.empty:
        return 50.0

    short_lb, long_lb, accel_lb = _infer_lookbacks(ts.index, indicator.get("freq"))
    short_lb = int(indicator.get("short_lb", short_lb))
    long_lb = int(indicator.get("long_lb", long_lb))
    accel_lb = int(indicator.get("accel_lb", accel_lb))

    direction = "up_is_risk" if indicator.get("higher_is_risk", True) else "down_is_risk"
    tail = indicator.get("tail", "single")
    eps = 1e-6

    window = ts.tail(max(long_lb, accel_lb * 2, short_lb + 2))
    if window.size < 6:
        return 50.0

    current = float(ts.iloc[-1])

    # 1) 动量比率
    min_periods = max(2, short_lb // 2)
    min_periods = min(min_periods, short_lb)
    ma_short = ts.rolling(short_lb, min_periods=min_periods).mean()
    ratio_series = ts / ma_short
    ratio = float(ratio_series.iloc[-1]) if not ratio_series.empty else np.nan
    ratio_std = float(ratio_series.tail(long_lb).std()) if ratio_series.size else np.nan
    z_ratio = (ratio - 1.0) / (ratio_std + eps) if not np.isnan(ratio) else np.nan

    # 2) 斜率
    long_values = window.tail(long_lb).values
    slope = _slope(long_values)
    std_diff = float(np.nanstd(np.diff(long_values))) if len(long_values) > 2 else np.nan
    z_slope = slope / (std_diff + eps) if not np.isnan(slope) else np.nan

    # 3) 加速度
    accel = np.nan
    if len(window) >= accel_lb * 2:
        recent = window.tail(accel_lb).values
        prior = window.tail(accel_lb * 2).head(accel_lb).values
        accel = _slope(recent) - _slope(prior)
    z_accel = accel / (std_diff + eps) if not np.isnan(accel) else np.nan

    # 4) 波动调整偏离
    mean_long = float(np.nanmean(long_values)) if len(long_values) else np.nan
    std_long = float(np.nanstd(long_values)) if len(long_values) else np.nan
    z_vol = (current - mean_long) / (std_long + eps) if not np.isnan(mean_long) else np.nan

    z_map = {
        "z_ratio": z_ratio,
        "z_slope": z_slope,
        "z_accel": z_accel,
        "z_vol": z_vol,
    }

    if indicator.get("use_velocity"):
        vel_short = int(indicator.get("vel_short", 5))
        vel_long = int(indicator.get("vel_long", 20))
        mode = indicator.get("velocity_mode", "pct")
        if len(ts) > vel_long + 2:
            current = float(ts.iloc[-1])
            past_short = float(ts.iloc[-1 - vel_short])
            past_long = float(ts.iloc[-1 - vel_long])
            prev_short = float(ts.iloc[-1 - vel_short * 2]) if len(ts) > vel_short * 2 else np.nan
            if mode == "pct":
                delta_short = (current - past_short) / (abs(past_short) + eps) * 100.0
                delta_long = (current - past_long) / (abs(past_long) + eps) * 100.0
                prev_delta_short = (past_short - prev_short) / (abs(prev_short) + eps) * 100.0 if not np.isnan(prev_short) else np.nan
            else:
                delta_short = current - past_short
                delta_long = current - past_long
                prev_delta_short = past_short - prev_short if not np.isnan(prev_short) else np.nan
            accel_vel = delta_short - prev_delta_short if not np.isnan(prev_delta_short) else np.nan
            delta_short_series = ts.diff(vel_short)
            delta_long_series = ts.diff(vel_long)
            z_vel_short = (delta_short - float(delta_short_series.tail(long_lb).mean())) / (float(delta_short_series.tail(long_lb).std()) + eps)
            z_vel_long = (delta_long - float(delta_long_series.tail(long_lb).mean())) / (float(delta_long_series.tail(long_lb).std()) + eps)
            z_accel_vel = accel_vel / (float(delta_short_series.tail(long_lb).std()) + eps) if not np.isnan(accel_vel) else np.nan
            z_map.update({
                "z_vel_short": z_vel_short,
                "z_vel_long": z_vel_long,
                "z_accel_vel": z_accel_vel,
            })
    adjusted = []
    for z in z_map.values():
        if np.isnan(z):
            continue
        if tail == "both":
            z = abs(z)
        elif direction == "down_is_risk":
            z = -z
        adjusted.append(z)

    if not adjusted:
        return 50.0

    freq = (indicator.get("freq") or pd.infer_freq(ts.index) or "D").upper()
    if freq.startswith("D"):
        weights = {"z_accel": 0.3, "z_vol": 0.25, "z_ratio": 0.15, "z_slope": 0.1}
        if indicator.get("use_velocity"):
            weights.update({"z_vel_short": 0.1, "z_vel_long": 0.05, "z_accel_vel": 0.05})
    elif freq.startswith("W"):
        weights = {"z_accel": 0.25, "z_vol": 0.2, "z_ratio": 0.2, "z_slope": 0.15}
        if indicator.get("use_velocity"):
            weights.update({"z_vel_short": 0.1, "z_vel_long": 0.05, "z_accel_vel": 0.05})
    else:
        weights = {"z_slope": 0.4, "z_ratio": 0.3, "z_accel": 0.15, "z_vol": 0.15}

    weighted_scores = []
    for key, z in z_map.items():
        if np.isnan(z):
            continue
        if tail == "both":
            z = abs(z)
        elif direction == "down_is_risk":
            z = -z
        weighted_scores.append(_score_from_z(z) * weights.get(key, 0))

    total_w = sum(weights.values())
    if not weighted_scores or total_w == 0:
        return 50.0
    return float(sum(weighted_scores) / total_w)


def infer_pillar(series_id: str, indicator: dict) -> str:
    if indicator.get("confirm_pillar"):
        return indicator["confirm_pillar"]
    sid = series_id.upper()
    funding = {
        "SOFR", "TEDRATE", "CP_MINUS_DTB3", "SOFR20DMA_MINUS_DTB3",
        "RRPONTSYD", "WTREGEN", "NET_LIQUIDITY", "DTB3", "FEDFUNDS"
    }
    credit = {
        "BAMLH0A0HYM2", "BAA10YM", "HY_OAS_MOMENTUM_RATIO", "HY_IG_RATIO",
        "HYG_LQD_RATIO"
    }
    real = {
        "PAYEMS", "MANEMP", "INDPRO", "GDP", "NEWORDER", "AWHMAN",
        "HOUST", "PERMIT", "IC4WSA", "UMCSENT", "CREDIT_CARD_DELINQUENCY",
        "TOTALSA", "TOTLL"
    }
    funding.update({"DXY_CHANGE", "KRE_SPY_RATIO", "XLF_SPY_RATIO"})
    if sid in funding:
        return "funding"
    if sid in credit:
        return "credit"
    if sid in real:
        return "real"
    return "other"


def process_single_indicator_real_v2(indicator: dict, crisis_periods: list, scoring_config: dict) -> Optional[dict]:
    series_id = indicator.get("series_id") or indicator.get("id")
    if not series_id:
        return None

    role = indicator.get("role", "score")

    ts = None
    if series_id.upper() == "NCBDBIQ027S":
        ratio_file = base.BASE / "data" / "series" / "CORPORATE_DEBT_GDP_RATIO.csv"
        if ratio_file.exists():
            try:
                ratio_df = pd.read_csv(ratio_file)
                ratio_df["date"] = pd.to_datetime(ratio_df["date"])
                ratio_df = ratio_df.set_index("date")
                ts = _parse_numeric_series(ratio_df["value"]).dropna()
                _mark_source(series_id, "precomputed_ratio")
            except Exception:
                ts = None
    if ts is None:
        ts = compose_series_v2(series_id)
    if ts is None or ts.empty:
        ts = base.fetch_series(series_id)
        if ts is not None and not ts.empty:
            _mark_source(series_id, "fred")
    if ts is None or ts.empty:
        print(f"⚠️ {series_id}: 无数据，跳过处理")
        return None

    ts_trans = base.transform_series(series_id, ts, indicator).dropna()
    if ts_trans.empty:
        return None

    current_value = float(ts_trans.iloc[-1])
    context_note = None
    if series_id.upper() in {"BTC_QQQ_RATIO", "XLF_SPY_RATIO"} and len(ts_trans) >= 20:
        ma30 = ts_trans.rolling(30, min_periods=15).mean().iloc[-1]
        if not np.isnan(ma30):
            if series_id.upper() == "BTC_QQQ_RATIO":
                context_note = f"当前值 {current_value:.1f}，{('低于' if current_value < ma30 else '高于')}30日均线 ({ma30:.1f})，提示短期动能变化。"
            else:
                context_note = f"当前值 {current_value:.4f}，{('低于' if current_value < ma30 else '高于')}30日均线 ({ma30:.4f})，提示相对强度变化。"
    last_date = ts_trans.index[-1]
    benchmark_value = base.calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)

    data_error = _validate_value(series_id, current_value, ts_trans)
    if data_error:
        DATA_ERRORS[series_id] = data_error
        # 数据异常：降权并标记
        indicator = {**indicator, "weight": 0.0}

    direction = "up_is_risk" if indicator.get("higher_is_risk", True) else "down_is_risk"
    compare_to = indicator.get("compare_to", "noncrisis_p75")
    tail = indicator.get("tail", "single")
    level_score = base.score_with_threshold(ts_trans, current_value, direction=direction, compare_to=compare_to, tail=tail)
    change_score = compute_change_score(ts_trans, indicator, scoring_config)

    freq = indicator.get("freq", "")
    freshness = _freshness_factor(str(last_date.date()), freq)
    change_score *= freshness

    w_level = float(indicator.get("w_level", scoring_config.get("w_level", 0.6)))
    w_change = float(indicator.get("w_change", scoring_config.get("w_change", 0.4)))
    w_change *= freshness
    if (w_level + w_change) <= 0:
        w_level, w_change = 0.6, 0.4
    w_sum = w_level + w_change
    w_level /= w_sum
    w_change /= w_sum

    final_score = w_level * level_score + w_change * change_score

    pillar = infer_pillar(series_id, indicator)
    ew_threshold = float(scoring_config.get("early_warning_threshold", 60))
    early_warning_flag = change_score >= ew_threshold

    return {
        "name": indicator.get("name", series_id),
        "series_id": series_id,
        "group": indicator.get("group", "unknown"),
        "current_value": current_value,
        "benchmark_value": benchmark_value,
        "risk_score": float(final_score),
        "level_score": float(level_score),
        "change_score": float(change_score),
        "final_score": float(final_score),
        "early_warning_flag": bool(early_warning_flag),
        "confirm_pillar": pillar,
        "last_date": str(last_date.date()),
        "global_weight": indicator.get("weight", 0.0),
        "effective_weight": 0.0 if data_error else indicator.get("weight", 0.0),
        "freq": indicator.get("freq"),
        "freshness_factor": round(freshness, 3),
        "higher_is_risk": indicator.get("higher_is_risk", True),
        "compare_to": compare_to,
        "plain_explainer": base.get_indicator_explanation(series_id, indicator),
        "role": role,
        "data_error": bool(data_error),
        "data_error_reason": data_error,
        "context_note": context_note,
        "expected_units": get_metadata(series_id).get("expected_units"),
        "expected_value_range": get_metadata(series_id).get("expected_value_range"),
        "transform_chain": get_metadata(series_id).get("transform_chain"),
        "data_source": SERIES_SOURCES.get(series_id, "unknown"),
    }


def compose_series_v2(series_id: str) -> Optional[pd.Series]:
    sid = series_id.upper()
    if sid == "VIX_TERM_STRUCTURE":
        vix = base.fetch_series("VIXCLS")
        if vix.empty:
            vix = _fetch_yahoo_series("^VIX")
            if not vix.empty:
                _mark_source(series_id, "yahoo")
        else:
            _mark_source(series_id, "fred")
        vix3m = base.fetch_series("VIX3M")
        if vix3m.empty:
            vix3m = _fetch_yahoo_series("^VIX3M")
            if not vix3m.empty:
                _mark_source(series_id, "yahoo")
        else:
            _mark_source(series_id, "fred")
        if vix3m.empty:
            vixx = _fetch_yahoo_series("^VXV")
            if not vixx.empty:
                _mark_source(series_id, "yahoo")
                vix3m = vixx
        if (vix.empty or vix3m.empty) and not vix.empty:
            vix9d = _fetch_yahoo_series("^VIX9D")
            if not vix9d.empty:
                _mark_source(series_id, "yahoo")
                vix, vix9d = vix.align(vix9d, join="inner")
                return (vix9d / vix).dropna()
            return None
        if vix.empty or vix3m.empty:
            return None
        vix, vix3m = vix.align(vix3m, join="inner")
        return (vix / vix3m).dropna()
    if sid == "HYG_LQD_RATIO":
        hyg = _fetch_yahoo_series("HYG")
        lqd = _fetch_yahoo_series("LQD")
        ratio = _ratio_series(hyg, lqd)
        if not ratio.empty:
            _mark_source(series_id, "yahoo")
        return ratio
    if sid == "KRE_SPY_RATIO":
        kre = _fetch_yahoo_series("KRE")
        spy = _fetch_yahoo_series("SPY")
        if spy.empty:
            spy = _fetch_yahoo_series("^GSPC")
        ratio = _ratio_series(kre, spy)
        if not ratio.empty:
            _mark_source(series_id, "yahoo")
        return ratio
    if sid == "XLF_SPY_RATIO":
        xlf = _fetch_yahoo_series("XLF")
        spy = _fetch_yahoo_series("SPY")
        if spy.empty:
            spy = _fetch_yahoo_series("^GSPC")
        ratio = _ratio_series(xlf, spy)
        if not ratio.empty:
            _mark_source(series_id, "yahoo")
        return ratio
    if sid == "BTC_QQQ_RATIO":
        btc = _fetch_yahoo_series("BTC-USD")
        qqq = _fetch_yahoo_series("QQQ")
        ratio = _ratio_series(btc, qqq)
        if not ratio.empty:
            _mark_source(series_id, "yahoo")
        return ratio
    if sid == "DXY_CHANGE":
        dxy = _fetch_yahoo_series("DX-Y.NYB")
        if dxy.empty:
            dxy = _fetch_yahoo_series("UUP")
        if dxy.empty:
            return None
        change = dxy.pct_change(5) * 100.0
        _mark_source(series_id, "yahoo")
        return change.dropna()
    if sid == "CROSS_ASSET_CORR_STRESS":
        spy = _fetch_yahoo_series("SPY")
        tlt = _fetch_yahoo_series("TLT")
        gld = _fetch_yahoo_series("GLD")
        uso = _fetch_yahoo_series("USO")
        if spy.empty or tlt.empty or gld.empty or uso.empty:
            return None
        df = pd.concat([spy, tlt, gld, uso], axis=1, join="inner")
        df.columns = ["SPY", "TLT", "GLD", "USO"]
        rets = df.pct_change().dropna()
        pairs = [("SPY", "TLT"), ("SPY", "GLD"), ("SPY", "USO"), ("TLT", "GLD"), ("TLT", "USO"), ("GLD", "USO")]
        corr_series = []
        for a, b in pairs:
            corr_series.append(rets[a].rolling(20, min_periods=10).corr(rets[b]))
        avg_corr = pd.concat(corr_series, axis=1).mean(axis=1)
        cash_king = ((rets["SPY"] < 0) & (rets["TLT"] < 0) & (rets["GLD"] < 0)).rolling(5, min_periods=3).mean()
        stress = (avg_corr.fillna(0) * 0.7 + cash_king.fillna(0) * 0.3).clip(0, 1)
        _mark_source(series_id, "yahoo")
        return stress.dropna()
    ts = _BASE_COMPOSE(series_id)
    if ts is not None and not ts.empty:
        _mark_source(series_id, "composed")
    return ts


def compute_confirmation_signals() -> Dict[str, dict]:
    signals: Dict[str, dict] = {}

    # A) Price stress: SPX below 200DMA
    spx = base.fetch_yahoo_safe("^GSPC")
    price_signal = False
    if spx is not None and not spx.empty and len(spx) >= 200:
        spx_ma200 = spx.rolling(200).mean()
        drawdown = spx / spx.rolling(252, min_periods=120).max() - 1.0
        price_signal = float(spx.iloc[-1]) < float(spx_ma200.iloc[-1]) or float(drawdown.iloc[-1]) < -0.12
    signals["price_stress"] = {"on": bool(price_signal)}

    # B) Volatility structure: VIX term structure
    vix_term = compose_series_v2("VIX_TERM_STRUCTURE")
    vol_signal = False
    if vix_term is not None and not vix_term.empty:
        vol_signal = float(vix_term.iloc[-1]) > 1.0
    signals["vol_term"] = {"on": bool(vol_signal)}

    # C) Credit stress: HYG/LQD deterioration or BAA-AAA fallback
    baa = base.fetch_series("BAA")
    aaa = base.fetch_series("AAA")
    credit_signal = False
    hyg_lqd = compose_series_v2("HYG_LQD_RATIO")
    if hyg_lqd is not None and not hyg_lqd.empty:
        roll_max = hyg_lqd.rolling(60, min_periods=20).max()
        drawdown = hyg_lqd / roll_max - 1.0
        credit_signal = float(drawdown.iloc[-1]) < -0.06
    if not credit_signal and baa is not None and not baa.empty and aaa is not None and not aaa.empty:
        spread = (baa - aaa).dropna()
        if not spread.empty:
            credit_signal = float(spread.iloc[-1]) > 2.0
    if not credit_signal:
        hy = base.fetch_series("BAMLH0A0HYM2")
        if hy is not None and not hy.empty:
            credit_signal = float(hy.iloc[-1]) > 5.0
    signals["credit_stress"] = {"on": bool(credit_signal)}

    return signals


def update_confirmation_state(
    signals: Dict[str, dict],
    output_dir: pathlib.Path,
    persistence_runs: int,
    min_signals: int,
    pillar_breadth_threshold: float,
    breadth_by_pillar: Dict[str, float],
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / "ew_state.json"
    state = {"history": []}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {"history": []}

    today = pd.Timestamp.now(tz=base.JST).date().isoformat()
    history = state.get("history", [])
    history.append({"date": today, "signals": signals})
    history = history[-max(1, persistence_runs):]

    def count_on(sig: dict) -> int:
        return sum(1 for item in sig.values() if item.get("on"))

    window = max(1, int(state.get("fast_ew_window", 5)))
    required = int(state.get("fast_ew_required", 3))
    recent = history[-window:]
    hits = sum(1 for entry in recent if count_on(entry["signals"]) >= min_signals)
    fast_ew_alert = hits >= required
    credit_breadth = breadth_by_pillar.get("credit", 0.0)
    confirmed = bool(fast_ew_alert and credit_breadth >= pillar_breadth_threshold)

    state = {
        "history": history,
        "confirmed": confirmed,
        "fast_ew_alert": fast_ew_alert,
        "fast_ew_window": window,
        "fast_ew_required": required,
        "fast_ew_hits": hits,
        "min_signals": min_signals,
        "pillar_breadth_threshold": pillar_breadth_threshold,
    }
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def _load_profile_history(output_dir: pathlib.Path) -> list:
    history_path = output_dir / "ew_profile_history.json"
    if history_path.exists():
        try:
            data = json.loads(history_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception:
            return []
    return []


def _update_profile_history(output_dir: pathlib.Path, summary: dict) -> list:
    output_dir.mkdir(parents=True, exist_ok=True)
    history = _load_profile_history(output_dir)
    today = pd.Timestamp.now(tz=base.JST).date().isoformat()
    entry = {
        "date": today,
        "fast_ew_index": summary.get("fast_ew_index"),
        "slow_macro_deterioration_index": summary.get("slow_macro_deterioration_index"),
        "early_warning_index": summary.get("early_warning_index"),
        "breadth_by_pillar": summary.get("breadth_by_pillar", {}),
        "breadth_early_warning": summary.get("breadth_early_warning", 0),
        "confirmation_signals": summary.get("confirmation_signals", {}),
    }
    if not history or history[-1].get("date") != today:
        history.append(entry)
    else:
        history[-1] = entry
    history = history[-120:]
    (output_dir / "ew_profile_history.json").write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return history


def _count_on(signals: dict) -> int:
    return sum(1 for item in signals.values() if item.get("on"))


def _profile_verdict(fast_ew_alert: bool, credit_breadth: float, slow_macro_index: float, real_breadth: float,
                     credit_breadth_threshold: float, slow_macro_watch_threshold: float, real_breadth_threshold: float) -> str:
    if fast_ew_alert and credit_breadth >= credit_breadth_threshold:
        return "Early Warning (confirmed)"
    if fast_ew_alert:
        return "Market Stress Watch"
    if slow_macro_index >= slow_macro_watch_threshold and real_breadth >= real_breadth_threshold:
        return "Macro Softening Watch (unconfirmed)"
    return "All Clear"


def _evaluate_profiles(history: list, summary: dict) -> Dict[str, dict]:
    if not history:
        return {}
    latest = history[-1]
    signals = latest.get("confirmation_signals", {})
    current_on = _count_on(signals)
    credit_breadth = latest.get("breadth_by_pillar", {}).get("credit", 0.0)
    real_breadth = latest.get("breadth_by_pillar", {}).get("real", 0.0)
    results = {}
    for name, profile in SENSITIVITY_PROFILES.items():
        window = int(profile.get("fast_ew_window", 5))
        required = int(profile.get("fast_ew_required", 3))
        min_signals = int(profile.get("confirm_min_signals", 2))
        recent = history[-window:]
        hits = sum(1 for entry in recent if _count_on(entry.get("confirmation_signals", {})) >= min_signals)
        fast_ew_alert = hits >= required
        verdict = _profile_verdict(
            fast_ew_alert,
            credit_breadth,
            float(latest.get("slow_macro_deterioration_index", 0) or 0),
            real_breadth,
            float(profile.get("credit_breadth_threshold", 0.2)),
            float(profile.get("slow_macro_watch_threshold", 50)),
            float(profile.get("real_breadth_threshold", 0.25)),
        )

        watch_window = int(profile.get("watch_window_days", 5))
        watch_recent = history[-watch_window:]
        watch_hits = 0
        for i, entry in enumerate(watch_recent):
            slice_end = len(history) - len(watch_recent) + i + 1
            slice_start = max(0, slice_end - window)
            window_slice = history[slice_start:slice_end]
            slice_hits = sum(1 for e in window_slice if _count_on(e.get("confirmation_signals", {})) >= min_signals)
            slice_alert = slice_hits >= required
            slice_credit = entry.get("breadth_by_pillar", {}).get("credit", 0.0)
            slice_real = entry.get("breadth_by_pillar", {}).get("real", 0.0)
            slice_verdict = _profile_verdict(
                slice_alert,
                slice_credit,
                float(entry.get("slow_macro_deterioration_index", 0) or 0),
                slice_real,
                float(profile.get("credit_breadth_threshold", 0.2)),
                float(profile.get("slow_macro_watch_threshold", 50)),
                float(profile.get("real_breadth_threshold", 0.25)),
            )
            if slice_verdict != "All Clear":
                watch_hits += 1

        results[name] = {
            "label": profile.get("label", name),
            "fast_ew_index": summary.get("fast_ew_index", 0),
            "slow_macro_deterioration_index": summary.get("slow_macro_deterioration_index", 0),
            "early_warning_index": summary.get("early_warning_index", 0),
            "confirm_2of3": current_on >= min_signals,
            "fast_ew_alert": fast_ew_alert,
            "credit_breadth": credit_breadth,
            "real_breadth": real_breadth,
            "funding_breadth": latest.get("breadth_by_pillar", {}).get("funding", 0.0),
            "verdict": verdict,
            "days_in_watch": watch_hits,
            "fast_ew_window": window,
            "fast_ew_required": required,
            "min_signals": min_signals,
        }
    return results


def _build_consensus_summary(profiles: Dict[str, dict]) -> str:
    if not profiles:
        return "暂无敏感度对照结果。"
    aggressive = profiles.get("aggressive", {}).get("verdict")
    base = profiles.get("base", {}).get("verdict")
    conservative = profiles.get("conservative", {}).get("verdict")
    if conservative and conservative != "All Clear":
        return "Conservative 已触发 → 高确信度预警，按危机剧本应对。"
    if base and base != "All Clear":
        return "Base 已触发 → 中等级别预警，建议降低风险暴露/关注流动性。"
    if aggressive and aggressive != "All Clear":
        return "Aggressive 已触发 → 轻微前兆，需盯确认信号（price/vol/credit）。"
    return "三档均未触发 → All Clear。"


def _build_profiles_section(profiles: Dict[str, dict], trends: dict, consensus: str, drivers_summary: dict) -> str:
    lines = [
        "## 🎚️ 敏感度对照表",
        "",
        "| Profile | fast_ew_index | slow_macro | early_warning | confirm(2/3) | credit_breadth | real_breadth | funding_breadth | Verdict | days_in_watch |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    order = ["conservative", "base", "aggressive"]
    for key in order:
        item = profiles.get(key, {})
        if not item:
            continue
        lines.append(
            f"| {item.get('label', key)} | {item.get('fast_ew_index', 0):.2f} | {item.get('slow_macro_deterioration_index', 0):.2f} | "
            f"{item.get('early_warning_index', 0):.2f} | {str(item.get('confirm_2of3', False))} | "
            f"{item.get('credit_breadth', 0):.2f} | {item.get('real_breadth', 0):.2f} | {item.get('funding_breadth', 0):.2f} | "
            f"{item.get('verdict', '')} | {item.get('days_in_watch', 0)} |"
        )
    lines.append("")
    lines.append(f"- **共识结论**: {consensus}")
    lines.append(f"- **fast_ew_trend_5d**: {trends.get('fast_ew_trend_5d', 0):.2f}")
    lines.append(f"- **breadth_trend_5d**: {trends.get('breadth_trend_5d', 0):.2f}")
    lines.append("")
    lines.append("### Top Drivers（按档位并列）")
    for key in order:
        item = profiles.get(key, {})
        if not item:
            continue
        label = item.get("label", key)
        lines.append(f"#### {label}")
        lines.append("- Level Top 5")
        for d in drivers_summary.get("top_level_drivers", []):
            lines.append(f"- {d['name']} ({d['series_id']}): {d['level_score']} | {d['group']} / {d['pillar']}")
        lines.append("- Change Top 5")
        for d in drivers_summary.get("top_change_drivers", []):
            lines.append(f"- {d['name']} ({d['series_id']}): {d['change_score']} | {d['group']} / {d['pillar']}")
    return "\n".join(lines) + "\n"

def calculate_real_fred_scores_v2(indicators_config=None, scoring_config=None):
    if scoring_config is None:
        scoring_config = {}

    if indicators_config is None:
        config_path = base.BASE / "config" / "crisis_indicators.yaml"
        config = base.load_yaml_config(config_path)
        indicators = config.get("indicators", [])
    else:
        indicators = indicators_config

    crisis_config_path = base.BASE / "config" / "crisis_periods.yaml"
    crisis_config = base.load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get("crises", [])

    deprecated = set(scoring_config.get("deprecated_series", []))
    processed_indicators = []
    for indicator in indicators:
        series_id = indicator.get("series_id") or indicator.get("id")
        if series_id in deprecated:
            continue
        try:
            result = process_single_indicator_real_v2(indicator, crisis_periods, scoring_config)
            if not result:
                continue
            processed_indicators.append(result)
        except Exception as e:
            print(f"❌ 处理指标失败 {indicator.get('name', 'Unknown')}: {e}")

    # 异常一致性校验：TOTALSA / UMCSENT 与违约率交叉验证
    anomaly_notes = []
    id_map = {i["series_id"]: i for i in processed_indicators}
    total_sa = id_map.get("TOTALSA")
    umcsent = id_map.get("UMCSENT")
    delinquency = id_map.get("CREDIT_CARD_DELINQUENCY")
    if total_sa and delinquency:
        if total_sa.get("level_score", 0) > 80 and delinquency.get("level_score", 0) < 40:
            total_sa["anomaly_note"] = "消费信贷异常未获违约率确认，可能为去杠杆或口径变化"
            total_sa["effective_weight"] = total_sa.get("effective_weight", 0) * 0.3
            anomaly_notes.append("TOTALSA 未获违约率确认：可能是去杠杆/口径变化")
    if umcsent and delinquency:
        if umcsent.get("level_score", 0) > 80 and delinquency.get("level_score", 0) < 40:
            umcsent["anomaly_note"] = "消费者信心异常未获违约率确认，谨慎解读"
            umcsent["effective_weight"] = umcsent.get("effective_weight", 0) * 0.5
            anomaly_notes.append("UMCSENT 异常未获违约率确认：谨慎解读")

    # 组内权重统计
    group_scores = {}
    for result in processed_indicators:
        group = result["group"]
        if group not in group_scores:
            group_scores[group] = {"scores": [], "weights": []}
        if result.get("role", "score") == "score" and not result.get("data_error"):
            group_scores[group]["scores"].append(result["risk_score"])
            group_scores[group]["weights"].append(result.get("effective_weight", 0))

    # 组内权重归一
    group_weights = {}
    for group, data in group_scores.items():
        if data["scores"]:
            group_weights[group] = sum(data["weights"]) if data["weights"] else 0
    group_min_weight = float(scoring_config.get("group_min_weight", 0.05))
    excluded_groups = []
    for group, data in group_scores.items():
        if data["scores"] and group_weights.get(group, 0) == 0:
            group_weights[group] = group_min_weight
        if not data["scores"]:
            excluded_groups.append(group)

    total_weight = sum(group_weights.values())
    if total_weight > 0:
        for group in group_weights:
            group_weights[group] /= total_weight
    else:
        avg_weight = 1.0 / max(1, len(group_weights))
        for group in group_weights:
            group_weights[group] = avg_weight

    final_group_scores = {}
    total_weighted_score = 0.0
    group_top_k = int(scoring_config.get("group_top_k", 3))
    for group, data in group_scores.items():
        if not data["scores"]:
            continue
        top_scores = sorted(data["scores"], reverse=True)[: max(1, min(group_top_k, len(data["scores"])))]
        avg_score = sum(top_scores) / len(top_scores)
        normalized_weight = group_weights.get(group, 0)
        final_group_scores[group] = {
            "score": avg_score,
            "weight": normalized_weight * 100,
            "count": len(data["scores"]),
        }
        total_weighted_score += avg_score * normalized_weight

    # 早预警汇总
    scored = [i for i in processed_indicators if i.get("role", "score") == "score" and not i.get("data_error")]
    weights = np.array([i.get("effective_weight", 0) for i in scored], dtype=float)
    weights = weights / weights.sum() if weights.sum() > 0 else np.ones(len(scored)) / max(1, len(scored))

    level_scores = np.array([i["level_score"] for i in scored], dtype=float) if scored else np.array([])
    change_scores = np.array([i["change_score"] for i in scored], dtype=float) if scored else np.array([])
    stress_now_index = float(np.dot(level_scores, weights)) if scored else 0.0
    # Fast vs Slow split
    fast_items = [i for i in scored if str(i.get("freq", "")).upper().startswith(("D", "W"))]
    slow_items = [i for i in scored if str(i.get("freq", "")).upper().startswith(("M", "Q"))]
    def _weighted_avg(items: list, key: str) -> float:
        if not items:
            return 0.0
        w = np.array([i.get("effective_weight", 0) for i in items], dtype=float)
        if w.sum() <= 0:
            w = np.ones(len(items)) / len(items)
        else:
            w = w / w.sum()
        vals = np.array([i.get(key, 0) for i in items], dtype=float)
        return float(np.dot(vals, w))

    fast_ew_index = _weighted_avg(fast_items, "change_score")
    slow_macro_index = _weighted_avg(slow_items, "change_score")
    early_warning_index = 0.7 * fast_ew_index + 0.3 * slow_macro_index

    ew_threshold = float(scoring_config.get("early_warning_threshold", 60))
    pillar_counts = {"funding": 0, "credit": 0, "real": 0, "other": 0}
    pillar_totals = {"funding": 0, "credit": 0, "real": 0, "other": 0}
    ew_count = 0
    for item in scored:
        pillar_totals[item.get("confirm_pillar", "other")] += 1
        if item["change_score"] >= ew_threshold:
            ew_count += 1
            pillar_counts[item.get("confirm_pillar", "other")] += 1

    breadth_early_warning = ew_count / max(1, len(scored))
    breadth_by_pillar = {
        pillar: (pillar_counts[pillar] / pillar_totals[pillar]) if pillar_totals[pillar] else 0.0
        for pillar in pillar_counts
    }
    triggered_pillars = [p for p, c in pillar_counts.items() if p != "other" and c > 0]
    confirm_min = int(scoring_config.get("confirm_pillars_min", 2))
    pillar_confirmed = len(triggered_pillars) >= confirm_min

    for item in processed_indicators:
        item["confirmed"] = False
        item["confirmation_notes"] = "watchlist"

    top_ew = sorted(scored, key=lambda x: x["change_score"], reverse=True)[:5]
    # 确认矩阵（2-of-3 + 持续性 + pillar阈值）
    output_dir = base.BASE / "outputs" / "crisis_monitor"
    confirmation_signals = compute_confirmation_signals()
    confirmation_state = update_confirmation_state(
        confirmation_signals,
        output_dir,
        persistence_runs=int(scoring_config.get("confirm_persistence_runs", 5)),
        min_signals=int(scoring_config.get("confirm_min_signals", 2)),
        pillar_breadth_threshold=float(scoring_config.get("credit_breadth_threshold", 0.2)),
        breadth_by_pillar=breadth_by_pillar,
    )
    global_confirmed = bool(confirmation_state.get("confirmed"))
    fast_ew_alert = bool(confirmation_state.get("fast_ew_alert"))
    credit_breadth = breadth_by_pillar.get("credit", 0.0)
    credit_breadth_threshold = float(scoring_config.get("credit_breadth_threshold", 0.2))
    slow_macro_threshold = float(scoring_config.get("slow_macro_watch_threshold", 50))
    real_breadth_threshold = float(scoring_config.get("real_breadth_threshold", 0.25))
    slow_macro_watch = slow_macro_index >= slow_macro_threshold and breadth_by_pillar.get("real", 0.0) >= real_breadth_threshold

    if fast_ew_alert and credit_breadth >= credit_breadth_threshold:
        status_label = "Early Warning (confirmed)"
    elif fast_ew_alert:
        status_label = "Market Stress Watch"
    elif slow_macro_watch:
        status_label = "Macro Softening Watch (unconfirmed)"
    else:
        status_label = "All Clear"

    for item in processed_indicators:
        item["confirmed"] = bool(global_confirmed and item.get("early_warning_flag") and item.get("confirm_pillar") in triggered_pillars)
        if item["confirmed"]:
            item["confirmation_notes"] = "confirmed"
        elif fast_ew_alert and item.get("early_warning_flag") and item.get("confirm_pillar") in triggered_pillars:
            item["confirmation_notes"] = "watchlist (fast EW)"
        elif item.get("early_warning_flag") and item.get("confirm_pillar") in triggered_pillars:
            item["confirmation_notes"] = "watchlist (pillar only)"

    cross_asset = id_map.get("CROSS_ASSET_CORR_STRESS")
    cash_is_king_alert = False
    if cross_asset and cross_asset.get("current_value") is not None:
        cash_is_king_alert = float(cross_asset["current_value"]) >= 0.7

    V2_SUMMARY.update({
        "stress_now_index": round(stress_now_index, 2),
        "early_warning_index": round(early_warning_index, 2),
        "fast_ew_index": round(fast_ew_index, 2),
        "slow_macro_deterioration_index": round(slow_macro_index, 2),
        "breadth_early_warning": round(breadth_early_warning, 4),
        "breadth_by_pillar": {k: round(v, 4) for k, v in breadth_by_pillar.items()},
        "pillar_counts": pillar_counts,
        "early_warning_confirmed": bool(global_confirmed),
        "status_label": status_label,
        "fast_ew_alert": fast_ew_alert,
        "credit_breadth": round(credit_breadth, 4),
        "credit_breadth_threshold": credit_breadth_threshold,
        "slow_macro_watch": bool(slow_macro_watch),
        "cash_is_king_alert": bool(cash_is_king_alert),
        "anomaly_notes": anomaly_notes,
        "early_warning_threshold": ew_threshold,
        "group_weight_notes": {
            "group_min_weight": group_min_weight,
            "excluded_groups": excluded_groups,
        },
        "data_errors": DATA_ERRORS,
        "confirmation_signals": confirmation_signals,
        "confirmation_state": confirmation_state,
        "top_change_indicators": [
            {"series_id": i["series_id"], "name": i["name"], "change_score": round(i["change_score"], 1)}
            for i in top_ew
        ],
    })

    return final_group_scores, total_weighted_score, processed_indicators


def _build_early_warning_section(summary: dict) -> str:
    status = summary.get("status_label", "Watchlist (unconfirmed)")
    lines = [
        "## 🧭 早预警指数",
        "",
        f"- **stress_now_index**: {summary.get('stress_now_index', 0)}",
        f"- **early_warning_index**: {summary.get('early_warning_index', 0)}",
        f"- **fast_ew_index**: {summary.get('fast_ew_index', 0)}",
        f"- **slow_macro_deterioration_index**: {summary.get('slow_macro_deterioration_index', 0)}",
        f"- **fast_ew_alert**: {summary.get('fast_ew_alert', False)}",
        f"- **slow_macro_watch**: {summary.get('slow_macro_watch', False)}",
        f"- **credit_breadth**: {summary.get('credit_breadth', 0)}",
        f"- **cash_is_king_alert**: {summary.get('cash_is_king_alert', False)}",
        f"- **breadth_early_warning**: {summary.get('breadth_early_warning', 0)}",
        f"- **pillar_counts**: {summary.get('pillar_counts', {})}",
        f"- **breadth_by_pillar**: {summary.get('breadth_by_pillar', {})}",
        f"- **confirmation_signals**: {summary.get('confirmation_signals', {})}",
        f"- **group_weight_notes**: {summary.get('group_weight_notes', {})}",
        f"- **confirmation**: {status}",
        "",
        "### 🔎 change_score Top 5",
    ]
    for item in summary.get("top_change_indicators", []):
        lines.append(f"- {item['name']} ({item['series_id']}): {item['change_score']}")
    return "\n".join(lines) + "\n"


def generate_executive_summary(summary: dict, profiles: Dict[str, dict]) -> dict:
    data_confidence = summary.get("data_freshness_confidence", "OK")
    conservative = profiles.get("conservative", {}).get("verdict", "All Clear")
    base_verdict = profiles.get("base", {}).get("verdict", "All Clear")
    aggressive = profiles.get("aggressive", {}).get("verdict", "All Clear")

    if conservative != "All Clear":
        regime = "高置信度预警阶段"
        severity = "high"
    elif base_verdict != "All Clear":
        regime = "中等压力阶段"
        severity = "medium"
    elif aggressive != "All Clear":
        regime = "早期边际变化阶段"
        severity = "low"
    else:
        regime = "整体风险处于低位"
        severity = "none"

    fast_ew_index = float(summary.get("fast_ew_index", 0) or 0)
    slow_macro_index = float(summary.get("slow_macro_deterioration_index", 0) or 0)
    early_warning_index = float(summary.get("early_warning_index", 0) or 0)
    fast_ew_alert = bool(summary.get("fast_ew_alert"))
    slow_macro_watch = bool(summary.get("slow_macro_watch"))

    if fast_ew_alert:
        driver = "快变量主导"
    elif slow_macro_watch or slow_macro_index >= fast_ew_index:
        driver = "慢变量主导"
    else:
        driver = "快慢变量均平稳"

    confirmation_signals = summary.get("confirmation_signals", {})
    confirmation_on = any(v.get("on") for v in confirmation_signals.values()) if confirmation_signals else False
    confirmation_text = "目前尚未形成价格、信用与波动率的多重确认" if not confirmation_on else "价格、信用或波动率已出现确认迹象"

    if severity == "high":
        action = "建议完整阅读全部章节并优先核查信用与资金链条。"
    elif severity == "medium":
        action = "建议重点查看 Fast EW 与信用/资金相关部分，其余可略读。"
    elif severity == "low":
        action = "若关注短期风险，请重点查看 Fast EW 与 Change Top 5。"
    else:
        action = "若仅需结论，可停止阅读；若为宏观判断，可略读慢变量部分。"

    weekend_note = ""
    today = pd.Timestamp.now(tz=base.JST).date()
    if data_confidence == "LOW" and today.weekday() >= 5:
        weekend_note = "结构性判断有效，但部分慢变量为滞后发布数据"

    para1 = f"{regime}，但需警惕可能的边际变化。"
    para2 = (
        f"快变量与慢变量的综合结果为 early_warning_index={early_warning_index:.1f}，"
        f"快变量为 fast_ew_index={fast_ew_index:.1f}，慢变量为 slow_macro={slow_macro_index:.1f}，"
        f"{driver}，{confirmation_text}"
    )
    if weekend_note:
        para2 = f"{para2}；{weekend_note}"
    para2 = f"{para2}。"

    drivers = summary.get("high_change_drivers", [])
    if drivers:
        names = "、".join([d["name"] for d in drivers])
        para3 = f"需要注意的是，部分对流动性高度敏感的资产正在发生快速变化（如：{names}），这通常是风险再定价或风格切换的早期信号，而非危机确认。"
    else:
        para3 = "目前未见流动性敏感资产出现显著的快速变化，短期扰动有限。"

    para4 = action

    return {
        "classification": regime,
        "paragraphs": [para1, para2, para3, para4],
        "text": "\n\n".join([para1, para2, para3, para4]),
    }


def _build_executive_summary_section(executive_summary: dict) -> str:
    lines = [
        "## 综合性结论（Executive Verdict）",
        "",
        executive_summary.get("text", "").strip(),
        "",
    ]
    return "\n".join(lines)


def _replace_summary_counts(md_text: str, counts: dict) -> str:
    md_text = re.sub(r"- \*\*高风险指标\*\*: \d+ 个", f"- **高风险指标**: {counts['high']} 个", md_text)
    md_text = re.sub(r"- \*\*中风险指标\*\*: \d+ 个", f"- **中风险指标**: {counts['med']} 个", md_text)
    md_text = re.sub(r"- \*\*低风险指标\*\*: \d+ 个", f"- **低风险指标**: {counts['low']} 个", md_text)
    md_text = re.sub(r"- \*\*极低风险指标\*\*: \d+ 个", f"- **极低风险指标**: {counts['very_low']} 个", md_text)
    return md_text


def _remove_indicator_blocks(md_text: str, series_ids: list) -> str:
    for sid in series_ids:
        pattern = rf"\n#### [^\n]*\({re.escape(sid)}\)[\s\S]*?(?=\n#### |\n### |\Z)"
        md_text = re.sub(pattern, "", md_text)
    return md_text


def _remove_empty_risk_sections(md_text: str) -> str:
    headers = ["### 🔴 高风险指标", "### 🟡 中风险指标", "### 🟢 低风险指标", "### 🔵 极低风险指标"]
    for header in headers:
        pattern = rf"\n{re.escape(header)}[\s\S]*?(?=\n### |\Z)"
        match = re.search(pattern, md_text)
        if match:
            block = match.group(0)
            if "#### " not in block:
                md_text = md_text.replace(block, "")
    return md_text


def _build_data_issues_section(items: list) -> str:
    lines = [
        "## ⚠️ Data Issues / Sentinel (Excluded)",
        "",
    ]
    if not items:
        lines.append("- 无异常数据项")
        return "\n".join(lines) + "\n"
    for item in items:
        reason = item.get("data_error_reason") or "口径异常，不纳入判断"
        lines.append(f"- {item.get('name')} ({item.get('series_id')}): 口径异常，不纳入判断（{reason}）")
    return "\n".join(lines) + "\n"


def _build_data_freshness_line(confidence: str, core_stale: list) -> str:
    today = pd.Timestamp.now(tz=base.JST).date()
    if confidence == "LOW" and today.weekday() >= 5:
        return ""
    if confidence == "LOW":
        return f"**Data Freshness Confidence**: LOW (core stale: {', '.join(core_stale)})"
    return "**Data Freshness Confidence**: OK"


def _build_data_quality_section(quality: dict) -> str:
    lines = [
        "## 🧪 Data Quality",
        "",
        f"- **missing_series**: {quality.get('missing_series', [])}",
        f"- **stale_but_acceptable**: {quality.get('stale_but_acceptable', [])}",
        f"- **deprecated_series**: {quality.get('deprecated_series', [])}",
    ]
    return "\n".join(lines) + "\n"


def _inject_context_notes(md_text: str, indicators: list) -> str:
    notes = {i.get("series_id"): i.get("context_note") for i in indicators if i.get("context_note")}
    for sid, note in notes.items():
        if not note:
            continue
        pattern = rf"(#### [^\n]*\({re.escape(sid)}\)[\s\S]*?- \*\*解释\*\*: [^\n]*\n)"
        def _add(m):
            block = m.group(1)
            if "补充说明" in block:
                return block
            return block + f"- **补充说明**: {note}\n"
        md_text = re.sub(pattern, _add, md_text)
    return md_text


def _wrap_details_section(md_text: str, header: str) -> str:
    pattern = rf"\n{re.escape(header)}\n([\s\S]*?)(?=\n### |\Z)"
    def _wrap(match):
        body = match.group(1).strip()
        summary = header.replace("### ", "")
        return f"\n<details>\n<summary>{summary}</summary>\n\n{body}\n\n</details>"
    return re.sub(pattern, _wrap, md_text)


def _upsert_section(md_text: str, section: str) -> str:
    marker = "## 🧭 早预警指数"
    if marker in md_text:
        md_text = re.sub(r"\n## 🧭 早预警指数[\s\S]*$", "", md_text).rstrip()
    return md_text + "\n\n" + section


def _build_data_freshness_section(summary: dict) -> str:
    lines = [
        "## 🧪 数据新鲜度与覆盖",
        "",
        f"- **stale_weight_pct**: {summary.get('stale_weight_pct', 0):.2%}",
        f"- **stale_series**: {summary.get('stale_series', [])}",
        f"- **updated_today**: {summary.get('updated_today', [])}",
        f"- **fallback_used**: {summary.get('fallback_used', [])}",
    ]
    return "\n".join(lines) + "\n"


def _build_top_drivers_section(summary: dict) -> str:
    lines = [
        "## 🔝 Top Drivers",
        "",
        "### Level Top 5",
    ]
    for item in summary.get("top_level_drivers", []):
        lines.append(f"- {item['name']} ({item['series_id']}): {item['level_score']} | {item['group']} / {item['pillar']}")
    lines.append("")
    lines.append("### Change Top 5")
    for item in summary.get("top_change_drivers", []):
        lines.append(f"- {item['name']} ({item['series_id']}): {item['change_score']} | {item['group']} / {item['pillar']}")
    return "\n".join(lines) + "\n"


def _score_badge(score: float) -> str:
    if score >= 80:
        return "🔴"
    if score >= 60:
        return "🟠"
    if score >= 40:
        return "🟡"
    return "🟢"


def _build_heatmap_section(indicators: list) -> str:
    scored = [i for i in indicators if i.get("role", "score") == "score"]
    scored = sorted(scored, key=lambda x: x.get("group", ""))
    cells = [f"{_score_badge(i.get('risk_score', 0))} {i.get('series_id')}" for i in scored]
    cols = 6
    rows = [cells[i:i + cols] for i in range(0, len(cells), cols)]
    lines = [
        "## 🌡️ 指标热力图（按分组粗略）",
        "",
        "| " + " | ".join([f"C{i+1}" for i in range(cols)]) + " |",
        "| " + " | ".join(["---"] * cols) + " |",
    ]
    for row in rows:
        padded = row + [""] * (cols - len(row))
        lines.append("| " + " | ".join(padded) + " |")
    lines.append("")
    lines.append("图例：🟢<40 🟡40-60 🟠60-80 🔴>=80")
    return "\n".join(lines) + "\n"


def _build_anomaly_section(anomaly_notes: list) -> str:
    lines = ["## ⚠️ 异常备注", ""]
    if not anomaly_notes:
        lines.append("- 无明显异常冲突")
        return "\n".join(lines) + "\n"
    for note in anomaly_notes:
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def postprocess_reports(output_dir: pathlib.Path, summary: dict) -> None:
    latest_json = output_dir / "crisis_report_latest.json"
    if not latest_json.exists():
        return
    _log_v2_stage("🧩 V2 后处理开始")
    def _load_json_file(path: pathlib.Path) -> Optional[dict]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        except Exception:
            return None

    json_data = _load_json_file(latest_json)
    if json_data is None:
        _log_v2_stage("⚠️ latest.json 解析失败，尝试回退")
        candidates = [p for p in output_dir.glob("crisis_report_*.json") if p.name != "crisis_report_latest.json"]
        candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
        for candidate in candidates:
            json_data = _load_json_file(candidate)
            if json_data:
                break
    if json_data is None:
        return
    _log_v2_stage("✅ JSON 读取完成")
    json_data["early_warning_index"] = summary.get("early_warning_index", 0)
    json_data["stress_now_index"] = summary.get("stress_now_index", 0)
    json_data["breadth_early_warning"] = summary.get("breadth_early_warning", 0)
    json_data["pillar_counts"] = summary.get("pillar_counts", {})
    json_data["early_warning_confirmed"] = summary.get("early_warning_confirmed", False)
    json_data["summary"]["early_warning_index"] = summary.get("early_warning_index", 0)
    json_data["summary"]["stress_now_index"] = summary.get("stress_now_index", 0)
    json_data["summary"]["fast_ew_index"] = summary.get("fast_ew_index", 0)
    json_data["summary"]["slow_macro_deterioration_index"] = summary.get("slow_macro_deterioration_index", 0)
    json_data["summary"]["fast_ew_alert"] = summary.get("fast_ew_alert", False)
    json_data["summary"]["slow_macro_watch"] = summary.get("slow_macro_watch", False)
    json_data["summary"]["credit_breadth"] = summary.get("credit_breadth", 0)
    json_data["summary"]["status_label"] = summary.get("status_label", "")
    json_data["summary"]["breadth_early_warning"] = summary.get("breadth_early_warning", 0)
    json_data["summary"]["breadth_by_pillar"] = summary.get("breadth_by_pillar", {})
    json_data["summary"]["confirmation_signals"] = summary.get("confirmation_signals", {})
    json_data["summary"]["confirmation_state"] = summary.get("confirmation_state", {})
    json_data["summary"]["group_weight_notes"] = summary.get("group_weight_notes", {})
    json_data["summary"]["data_errors"] = summary.get("data_errors", {})
    json_data["summary"]["cash_is_king_alert"] = summary.get("cash_is_king_alert", False)
    json_data["summary"]["anomaly_notes"] = summary.get("anomaly_notes", [])
    json_data["summary"]["ratio_negative_indicators"] = [
        i.get("series_id") for i in json_data.get("indicators", [])
        if i.get("data_error_reason") and "ratio应为非负" in str(i.get("data_error_reason"))
    ]

    # Data freshness & coverage
    config = base.load_yaml_config(base.BASE / "config" / "crisis_indicators.yaml")
    indicators_config = { (i.get("series_id") or i.get("id")): i for i in config.get("indicators", []) }
    deprecated_series = set(config.get("scoring", {}).get("deprecated_series", []))
    today = pd.Timestamp.now(tz=base.JST).date()
    stale_thresholds = {"D": 15, "W": 30, "M": 90, "Q": 180}
    stale_series = []
    updated_today = []
    fallback_used = []
    stale_but_acceptable = []
    stale_weight = 0.0
    total_weight = 0.0

    for item in json_data.get("indicators", []):
        series_id = item.get("series_id")
        if series_id in deprecated_series:
            continue
        last_date = item.get("last_date")
        data_source = item.get("data_source")
        cfg = indicators_config.get(series_id, {})
        freq = (cfg.get("freq") or "D").upper()
        threshold = stale_thresholds.get(freq[:1], 30)
        weight = float(item.get("effective_weight", item.get("global_weight", 0.0)))
        total_weight += weight
        if data_source == "yahoo":
            fallback_used.append(series_id)
        if last_date:
            try:
                dt = pd.to_datetime(last_date).date()
                lag = (today - dt).days
                if lag == 0:
                    updated_today.append(series_id)
                if freq.startswith("D"):
                    weekday = today.weekday()
                    max_ok = 3 if weekday in {0, 5, 6} else 1
                    if lag > 1 and lag <= 3 and weekday in {0, 5, 6}:
                        stale_but_acceptable.append(series_id)
                    elif lag > max_ok:
                        if lag > threshold:
                            stale_series.append(series_id)
                            stale_weight += weight
                        else:
                            stale_series.append(series_id)
                            stale_weight += weight
                elif lag > threshold:
                    stale_series.append(series_id)
                    stale_weight += weight
            except Exception:
                continue

    stale_weight_pct = (stale_weight / total_weight) if total_weight > 0 else 0.0
    freshness_summary = {
        "stale_weight_pct": stale_weight_pct,
        "stale_series": stale_series,
        "updated_today": updated_today,
        "fallback_used": fallback_used,
        "stale_but_acceptable": stale_but_acceptable,
    }
    json_data["summary"]["data_freshness"] = freshness_summary

    core_daily_ids = {
        "T10Y3M", "T10Y2Y", "BAA10YM", "SOFR", "DTB3",
        "SOFR20DMA_MINUS_DTB3", "CP_MINUS_DTB3", "BAMLH0A0HYM2"
    }
    core_stale_list = []
    for item in json_data.get("indicators", []):
        sid = item.get("series_id")
        if sid not in core_daily_ids or sid in deprecated_series:
            continue
        last_date = item.get("last_date")
        if not last_date:
            core_stale_list.append(sid)
            continue
        try:
            dt = pd.to_datetime(last_date).date()
            lag = (today - dt).days
            max_ok = 3 if today.weekday() in {0, 5, 6} else 1
            if lag > max_ok:
                core_stale_list.append(sid)
        except Exception:
            core_stale_list.append(sid)
    data_confidence = "LOW" if core_stale_list else "OK"
    summary["core_data_stale_list"] = core_stale_list
    summary["data_freshness_confidence"] = data_confidence
    json_data["summary"]["core_data_stale_list"] = core_stale_list
    json_data["summary"]["data_freshness_confidence"] = data_confidence
    missing_series = sorted([sid for sid in indicators_config.keys() if sid not in {i.get("series_id") for i in json_data.get("indicators", [])} and sid not in deprecated_series])
    data_quality = {
        "missing_series": missing_series,
        "stale_but_acceptable": stale_but_acceptable,
        "deprecated_series": sorted(deprecated_series),
    }
    json_data["summary"]["data_quality"] = data_quality

    # Top drivers
    indicators = json_data.get("indicators", [])
    top_level = sorted(indicators, key=lambda x: x.get("level_score", 0), reverse=True)[:5]
    top_change = sorted(indicators, key=lambda x: x.get("change_score", 0), reverse=True)[:5]
    drivers_summary = {
        "top_level_drivers": [
            {
                "series_id": i.get("series_id"),
                "name": i.get("name"),
                "level_score": round(i.get("level_score", 0), 1),
                "group": i.get("group"),
                "pillar": i.get("confirm_pillar"),
            }
            for i in top_level
        ],
        "top_change_drivers": [
            {
                "series_id": i.get("series_id"),
                "name": i.get("name"),
                "change_score": round(i.get("change_score", 0), 1),
                "group": i.get("group"),
                "pillar": i.get("confirm_pillar"),
            }
            for i in top_change
        ],
    }
    json_data["summary"]["top_drivers"] = drivers_summary
    json_data["summary"]["heatmap"] = [
        {
            "series_id": i.get("series_id"),
            "name": i.get("name"),
            "group": i.get("group"),
            "pillar": i.get("confirm_pillar"),
            "score": round(i.get("risk_score", 0), 1),
        }
        for i in indicators
        if i.get("role", "score") == "score"
    ]

    # Sensitivity profiles + stability trends
    history = _update_profile_history(output_dir, summary)
    profiles = _evaluate_profiles(history, summary)
    consensus = _build_consensus_summary(profiles)
    if len(history) >= 6:
        fast_ew_trend_5d = float(history[-1].get("fast_ew_index", 0) or 0) - float(history[-6].get("fast_ew_index", 0) or 0)
        breadth_trend_5d = float(history[-1].get("breadth_early_warning", 0) or 0) - float(history[-6].get("breadth_early_warning", 0) or 0)
    else:
        fast_ew_trend_5d = 0.0
        breadth_trend_5d = 0.0
    trends = {
        "fast_ew_trend_5d": round(fast_ew_trend_5d, 2),
        "breadth_trend_5d": round(breadth_trend_5d, 2),
    }
    for profile in profiles.values():
        profile["top_drivers"] = drivers_summary
    json_data["profiles"] = profiles
    json_data["summary"]["profiles"] = profiles
    json_data["summary"]["consensus_summary"] = consensus
    json_data["summary"]["stability"] = trends
    high_change = []
    for item in json_data.get("indicators", []):
        if item.get("data_error"):
            continue
        if item.get("series_id") in stale_series:
            continue
        if item.get("group") != "core_warning" and item.get("role") != "monitor":
            continue
        if item.get("change_score", 0) >= 45:
            high_change.append(item)
    high_change = sorted(high_change, key=lambda x: x.get("change_score", 0), reverse=True)[:2]
    summary["high_change_drivers"] = [
        {"series_id": i.get("series_id"), "name": i.get("name"), "change_score": round(i.get("change_score", 0), 1)}
        for i in high_change
    ]
    executive_summary = generate_executive_summary(summary, profiles)
    json_data["executive_summary"] = executive_summary
    json_data["summary"]["executive_summary"] = executive_summary

    timestamp = json_data.get("timestamp")
    json_path = output_dir / f"crisis_report_{timestamp}.json"
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    _log_v2_stage("✅ JSON 写入完成")

    section = _build_early_warning_section(summary)
    section = section + "\n" + _build_profiles_section(profiles, trends, consensus, drivers_summary)
    section = section + "\n" + _build_heatmap_section(indicators)
    section = section + "\n" + _build_anomaly_section(summary.get("anomaly_notes", []))
    section = section + "\n" + _build_data_freshness_section(freshness_summary)
    section = section + "\n" + _build_top_drivers_section(drivers_summary)
    md_path = output_dir / f"crisis_report_{timestamp}.md"
    latest_md = output_dir / "crisis_report_latest.md"
    if md_path.exists():
        md_text = md_path.read_text(encoding="utf-8")
        if DATA_ERRORS:
            header_note = f"**DATA ERROR**: {', '.join(sorted(DATA_ERRORS.keys()))}  \n"
            if header_note not in md_text:
                md_text = md_text.replace("# 🚨 FRED 宏观金融危机预警监控报告\n\n",
                                          "# 🚨 FRED 宏观金融危机预警监控报告\n\n" + header_note + "\n")
        data_error_items = [i for i in json_data.get("indicators", []) if i.get("data_error")]
        data_error_ids = [i.get("series_id") for i in data_error_items]
        md_text = _remove_indicator_blocks(md_text, data_error_ids)
        md_text = _remove_empty_risk_sections(md_text)
        counts = {"high": 0, "med": 0, "low": 0, "very_low": 0}
        bands = config.get("scoring", {}).get("bands", {"high": 80, "med": 60, "low": 40})
        for i in json_data.get("indicators", []):
            if i.get("data_error"):
                continue
            score = i.get("risk_score", 0)
            if score >= bands["high"]:
                counts["high"] += 1
            elif score >= bands["med"]:
                counts["med"] += 1
            elif score >= bands["low"]:
                counts["low"] += 1
            else:
                counts["very_low"] += 1
        md_text = _replace_summary_counts(md_text, counts)
        md_text = _upsert_section(md_text, section)
        executive_section = _build_executive_summary_section(executive_summary)
        data_conf_line = _build_data_freshness_line(data_confidence, core_stale_list)
        data_issues_section = _build_data_issues_section(data_error_items)
        ratio_negative = [
            i for i in data_error_items
            if isinstance(i.get("data_error_reason"), str) and "ratio应为非负" in i.get("data_error_reason")
        ]
        ratio_negative_note = ""
        if ratio_negative:
            ratio_negative_note = "注：部分比率指标出现异常值，已剔除。"
        data_quality_section = _build_data_quality_section(data_quality)
        md_text = re.sub(r"\n## 综合性结论（Executive Verdict）[\s\S]*?(?=\n## |\Z)", "\n", md_text).rstrip()
        lines = md_text.splitlines()
        header_idx = next((i for i, line in enumerate(lines) if line.startswith("# 🚨 FRED 宏观金融危机预警监控报告")), None)
        if header_idx is not None:
            insert_idx = next((i for i, line in enumerate(lines) if line.startswith("**生成时间**")), None)
            if insert_idx is None:
                insert_idx = header_idx + 1
            else:
                insert_idx += 1
            while insert_idx < len(lines) and lines[insert_idx].strip() == "":
                insert_idx += 1
            lines.insert(insert_idx, "")
            lines.insert(insert_idx + 1, executive_section.rstrip())
            lines.insert(insert_idx + 2, "")
            if ratio_negative_note:
                lines.insert(insert_idx + 3, ratio_negative_note)
                lines.insert(insert_idx + 4, "")
            if data_conf_line:
                lines.insert(insert_idx + 5, data_conf_line)
                lines.insert(insert_idx + 6, "")
            md_text = "\n".join(lines).rstrip() + "\n"
        if "## 📈 详细指标分析" in md_text:
            md_text = md_text.replace("## 📈 详细指标分析\n\n", "## 📈 详细指标分析\n\n" + data_quality_section + "\n" + data_issues_section + "\n")
        md_text = _inject_context_notes(md_text, json_data.get("indicators", []))
        md_text = _wrap_details_section(md_text, "### 🟢 低风险指标")
        md_text = _wrap_details_section(md_text, "### 🔵 极低风险指标")
        md_path.write_text(md_text, encoding="utf-8")
        latest_md.write_text(md_text, encoding="utf-8")
        _log_v2_stage("✅ Markdown 写入完成")

        html_path = output_dir / f"crisis_report_{timestamp}.html"
        latest_html = output_dir / "crisis_report_latest.html"
        html_content = base.render_html_report(md_text, "宏观金融危机监察报告", output_dir)
        html_path.write_text(html_content, encoding="utf-8")
        latest_html.write_text(html_content, encoding="utf-8")
        _log_v2_stage("✅ HTML 写入完成")


def generate_report_with_images_v2():
    _log_v2_stage("🚀 启动 V2 报告生成")
    base.calculate_real_fred_scores = calculate_real_fred_scores_v2
    base.compose_series = compose_series_v2
    base.generate_report_with_images()
    base.compose_series = _BASE_COMPOSE
    output_dir = base.BASE / "outputs" / "crisis_monitor"
    _log_v2_stage("🧠 开始 V2 后处理")
    postprocess_reports(output_dir, V2_SUMMARY)
    _log_v2_stage("✅ V2 报告生成完成")


if __name__ == "__main__":
    generate_report_with_images_v2()
