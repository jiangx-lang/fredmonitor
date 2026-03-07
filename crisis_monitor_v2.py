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
import os
import pathlib
import re
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

# 加载 macrolab.env / .env，使 DASHSCOPE_API_KEY 等对 AI 叙事可用
BASE_DIR = pathlib.Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    for _f in [BASE_DIR / "macrolab.env", BASE_DIR / ".env"]:
        if _f.exists():
            try:
                load_dotenv(_f, encoding="utf-8")
            except Exception:
                load_dotenv(_f, encoding="gbk")
except Exception:
    pass

import crisis_monitor as base
import crisis_monitor_regime as regime_module
import conflict_monitor as conflict_module
import structural_risk as structural_module
import regime_hysteresis as hysteresis_module
import event_x_resonance as event_x_resonance_module
import event_x_freshness as event_x_freshness_module
import event_x_acceptance as event_x_acceptance_module

V2_SUMMARY: Dict[str, object] = {}
_BASE_COMPOSE = base.compose_series
SERIES_SOURCES: Dict[str, str] = {}
DATA_ERRORS: Dict[str, str] = {}

# 体制乘数：评分前由 generate_report_with_images_v2 设置，供 calculate_real_fred_scores_v2 使用
REGIME_VERDICT_FOR_SCORING: Optional[str] = None


class RegimeWeightManager:
    """
    根据宏观体制动态调整风险权重（复用 Regime Dashboard 信号，避免硬编码金价/汇率绝对值）。
    体制判定来自 CrisisMonitor.evaluate_systemic_risk()，不依赖 gold>2000 等绝对值。
    """
    # 配置分组 -> 体制类别（用于乘数）
    GROUP_TO_CATEGORY = {
        "core_warning": "yield_curve",      # 收益率曲线、VIX、流动性相关
        "real_economy": "real_economy",
        "monetary_policy": "liquidity",
        "banking": "credit_spread",
        "consumers_leverage": "credit_spread",
        "monitoring": "equity_volatility",
    }

    def __init__(self) -> None:
        self.regime_multipliers = {
            "ANTI_FIAT_REGIME": {
                "gold_regime": 2.5,
                "yield_curve": 1.5,
                "credit_spread": 0.5,
                "equity_volatility": 0.8,
                "liquidity": 1.0,
                "real_economy": 1.0,
            },
            "FISCAL_DOMINANCE_ACTIVE": {
                "yield_curve": 3.0,
                "liquidity": 1.5,
                "real_economy": 0.8,
                "gold_regime": 1.0,
                "credit_spread": 1.0,
                "equity_volatility": 1.0,
            },
            "DEFLATIONARY_CRASH": {
                "credit_spread": 2.0,
                "equity_volatility": 2.0,
                "real_economy": 1.5,
                "gold_regime": 0.5,
                "yield_curve": 1.0,
                "liquidity": 1.0,
            },
        }
        # 体制 verdict 到乘数键的映射（Regime Dashboard 输出 -> 上表键）
        self.verdict_to_key = {
            "ANTI_FIAT_REGIME": "ANTI_FIAT_REGIME",
            "FISCAL_DOMINANCE_ACTIVE": "FISCAL_DOMINANCE_ACTIVE",
            "K_SHAPED_RECESSION": "DEFLATIONARY_CRASH",
            "LIQUIDITY_STRESS": "DEFLATIONARY_CRASH",
            "JAPAN_CONTAGION_CRITICAL": "DEFLATIONARY_CRASH",
            "SOVEREIGN_LIQUIDITY_CRISIS": "DEFLATIONARY_CRASH",
        }

    def get_adjusted_weights(self, base_weights: Dict[str, float], current_regime: str) -> Dict[str, float]:
        """
        输入: 按体制类别汇总的 base_weights (category -> weight)，当前体制 verdict。
        输出: 归一化后的新权重 (category -> weight)。
        """
        key = self.verdict_to_key.get(current_regime, current_regime)
        if key not in self.regime_multipliers:
            return base_weights

        multipliers = self.regime_multipliers[key]
        new_weights = {}
        for category, weight in base_weights.items():
            mult = multipliers.get(category, 1.0)
            new_weights[category] = weight * mult

        total = sum(new_weights.values())
        if total <= 0:
            return base_weights
        for cat in new_weights:
            new_weights[cat] = new_weights[cat] / total
        return new_weights

    def apply_regime_to_group_weights(
        self, group_weights: Dict[str, float], current_regime: str
    ) -> Tuple[Dict[str, float], Dict[str, object]]:
        """
        输入: 按配置分组名的 group_weights，当前体制 verdict。
        输出: (调整后并归一化的 group_weights, notes 供报告展示)。
        """
        if not current_regime or current_regime == "N/A":
            return group_weights, {"regime_applied": False, "regime": current_regime or "N/A"}

        # 1) 按体制类别汇总
        category_weights: Dict[str, float] = {}
        group_to_cat = self.GROUP_TO_CATEGORY
        for group, w in group_weights.items():
            cat = group_to_cat.get(group, "real_economy")
            category_weights[cat] = category_weights.get(cat, 0.0) + w
        for cat in ["gold_regime", "yield_curve", "credit_spread", "equity_volatility", "liquidity", "real_economy"]:
            if cat not in category_weights:
                category_weights[cat] = 0.0

        # 2) 体制乘数
        adjusted_cat = self.get_adjusted_weights(category_weights, current_regime)
        key = self.verdict_to_key.get(current_regime, current_regime)
        if key not in self.regime_multipliers:
            return group_weights, {"regime_applied": False, "regime": current_regime}

        # 3) 按组回填并保持组内相对比例
        new_group_weights: Dict[str, float] = {}
        for group, w in group_weights.items():
            cat = group_to_cat.get(group, "real_economy")
            cat_total = category_weights.get(cat, 0.0)
            if cat_total > 0 and cat in adjusted_cat:
                new_group_weights[group] = adjusted_cat[cat] * (w / cat_total)
            else:
                new_group_weights[group] = w

        total = sum(new_group_weights.values())
        if total <= 0:
            return group_weights, {"regime_applied": True, "regime": current_regime, "error": "zero_total"}
        for g in new_group_weights:
            new_group_weights[g] /= total

        notes = {
            "regime_applied": True,
            "regime": current_regime,
            "regime_key": key,
            "category_weights_before": {k: round(v, 4) for k, v in category_weights.items() if v > 0},
            "category_weights_after": {k: round(v, 4) for k, v in adjusted_cat.items() if v > 0},
        }
        return new_group_weights, notes


class AllocationRecommender:
    """
    将 Regime 映射到可执行的仓位建议（SPX / Gold / TLT / Cash 等），
    报告直接给出「该怎么做」而非让用户猜。
    """
    ALLOCATION_MAP = {
        "NORMAL": {
            "SPX": "Overweight",
            "Gold": "Neutral",
            "TLT": "Neutral",
            "Cash_BIL": "Underweight",
            "strategy": "Risk On, 做多科技",
        },
        "ANTI_FIAT_REGIME": {
            "SPX": "Sell",
            "Gold": "Strong Buy",
            "TLT": "Sell",
            "Cash_BIL": "Neutral",
            "strategy": "Long Hard Assets / Bitcoin",
        },
        "FISCAL_DOMINANCE_ACTIVE": {
            "SPX": "Neutral",
            "Gold": "Accumulate",
            "TLT": "Strong Sell",
            "Cash_BIL": "Overweight",
            "strategy": "Long Gold / Short Bonds",
        },
        "K_SHAPED_RECESSION": {
            "SPX": "Sell",
            "Gold": "Buy",
            "TLT": "Buy",
            "Cash_BIL": "Strong Buy",
            "strategy": "Cash is King",
        },
        "LIQUIDITY_STRESS": {
            "SPX": "Sell",
            "Gold": "Buy",
            "TLT": "Buy",
            "Cash_BIL": "Strong Buy",
            "strategy": "Cash is King",
        },
        "JAPAN_CONTAGION_CRITICAL": {
            "SPX": "Sell",
            "Gold": "Buy",
            "TLT": "Buy",
            "Cash_BIL": "Strong Buy",
            "strategy": "Cash is King",
        },
        "SOVEREIGN_LIQUIDITY_CRISIS": {
            "SPX": "Sell",
            "Gold": "Buy",
            "TLT": "Buy",
            "Cash_BIL": "Strong Buy",
            "strategy": "Cash is King",
        },
    }

    @classmethod
    def get_allocation_suggestion(cls, regime_type: str) -> Dict[str, str]:
        """根据体制返回仓位建议字典；未知体制按 NORMAL 处理。"""
        return dict(cls.ALLOCATION_MAP.get(regime_type, cls.ALLOCATION_MAP["NORMAL"]))


def _build_allocation_section(regime_verdict: str) -> str:
    """生成「Suggested Portfolio Stance」Markdown 段落。"""
    alloc = AllocationRecommender.get_allocation_suggestion(regime_verdict or "NORMAL")
    lines = [
        "## 🛡️ Suggested Portfolio Stance",
        "",
        "| 资产 | 建议 |",
        "|------|------|",
    ]
    for key in ("SPX", "Gold", "TLT", "Cash_BIL"):
        lines.append(f"| {key} | {alloc.get(key, '-')} |")
    lines.append("")
    lines.append(f"**策略**: {alloc.get('strategy', '-')}")
    lines.append("")
    return "\n".join(lines)


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

    # 体制乘数：根据 Regime Dashboard 的 verdict 动态调整分组权重
    regime_verdict = REGIME_VERDICT_FOR_SCORING
    regime_weight_notes: Dict[str, object] = {}
    if regime_verdict:
        mgr = RegimeWeightManager()
        group_weights, regime_weight_notes = mgr.apply_regime_to_group_weights(
            dict(group_weights), regime_verdict
        )

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
        "regime_weight_notes": regime_weight_notes,
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


def _build_event_x_reading_guide() -> str:
    """Event-X Reading Guide：术语与观测点通俗解释，供晨会/投委会非量化读者理解。"""
    lines = [
        "### Event-X Reading Guide",
        "",
        "- **BIZD** — 私募信贷/BDC 的公开市场代理；若 BIZD 明显下跌，可能表示投资者在私募估值全面反映前先行定价风险，不代表系统性危机已发生。",
        "- **HY momentum / HY spread widening** — 水平看当前垃圾债风险定价的绝对高度，动量看近期是否快速恶化；即使绝对水平不高，5 日利差快速走阔也可能表示早期信用压力在形成。",
        "- **Breakeven / breakeven proxy** — 债券市场对未来通胀的大致定价（市场用真钱投票的预期，非 CPI 本身）；FRED 数据过旧时用 proxy 补丁尽量接近实时。breakeven 单独上行代表债券市场开始担心通胀，但若油价和恐慌未配合，这更像预期波动而非已确认地缘能源冲击；因此系统允许其触发 WATCH，但不会单独升级到 ALERT/ALARM（双腿确认）。",
        "- **STLFSI4** — 金融压力综合指标；绝对水平低表示系统性资金压力不高，change_score 高时表示金融环境在收紧，即使尚未进入危险区。",
        "- **Geopolitics completeness** — 衡量地缘/通胀链关键腿是否「有效可用」而非仅「字段存在」；若仅 VIX 在动而油价/通胀预期未确认，则为 PARTIAL 或 LOW。",
        "- **Signal Confidence / Freshness Risk** — Signal Confidence 表示当前判断是否值得采信；Freshness Risk 表示关键数据是否过旧；信号可为 WATCH 但若核心数据很旧，可信度仍可能较低。",
        "",
    ]
    return "\n".join(lines)


def _build_event_x_machine_summary(
    struct_results: dict,
    resonance_result: dict,
    freshness_result: Optional[dict],
    confidence_result: Optional[dict],
    geopolitics_completeness: Optional[dict] = None,
) -> str:
    """指挥台 1–3 句：明确触发腿、未确认腿、失真腿；不重复状态枚举。"""
    pc = struct_results.get("private_credit_liquidity_radar")
    geo = struct_results.get("geopolitics_inflation_radar")
    pc_alert = pc.alert if hasattr(pc, "alert") else (pc.get("alert", "NONE") if isinstance(pc, dict) else "NONE")
    geo_alert = geo.alert if hasattr(geo, "alert") else (geo.get("alert", "NONE") if isinstance(geo, dict) else "NONE")
    _VALID_ALERTS = {"NONE", "WATCH", "ALERT", "ALARM"}
    if pc_alert not in _VALID_ALERTS:
        pc_alert = "NONE"
    if geo_alert not in _VALID_ALERTS:
        geo_alert = "NONE"
    radar_a_status = "NORMAL" if pc_alert == "NONE" else pc_alert
    radar_b_status = "NORMAL" if geo_alert == "NONE" else geo_alert
    res_status = (resonance_result or {}).get("level", "OFF")
    completeness = (geopolitics_completeness or {}).get("completeness", "LOW")
    details_pc = (getattr(pc, "details", None) or (pc if isinstance(pc, dict) else {})) if pc else {}
    details_geo = (getattr(geo, "details", None) or (geo if isinstance(geo, dict) else {})) if geo else {}

    sentences = []
    # Private Credit: 主要触发腿 + 未确认/良性说明
    if radar_a_status != "NORMAL" and isinstance(details_pc, dict):
        pc_parts = []
        if details_pc.get("watch_triggered_by_momentum") and details_pc.get("hy_oas_5d_bp_change") is not None:
            pc_parts.append("HY spread widening momentum (5D +{:.0f}bp)".format(float(details_pc["hy_oas_5d_bp_change"])))
        if details_pc.get("bizd_vs_50dma_pct") is not None:
            pc_parts.append("BIZD weakness")
        if not pc_parts:
            pc_parts.append("proxy stress")
        sentences.append(
            "Private credit watch is supported by " + " and ".join(pc_parts) + ", while absolute spreads and STLFSI4 remain benign in level terms; reflects early proxy stress, not confirmed systemic credit tightening."
        )
    elif radar_a_status == "NORMAL":
        sentences.append("Private credit spreads remain benign.")

    # Geopolitics: 区分 breakeven-led watch / dual-leg alert / full-chain alarm
    if radar_b_status != "NORMAL":
        dual_leg = isinstance(details_geo, dict) and details_geo.get("dual_leg_confirmed")
        upgrade_blocked = isinstance(details_geo, dict) and details_geo.get("upgrade_blocked_by_single_leg_rule")
        if radar_b_status == "ALARM" and dual_leg:
            sentences.append("Geopolitics full-chain alarm: Energy + Inflation + Fear confirmed.")
        elif radar_b_status == "ALERT" and dual_leg:
            sentences.append("Geopolitics dual-leg confirmed alert.")
        elif radar_b_status == "WATCH" and upgrade_blocked:
            sentences.append("Geopolitics breakeven-led watch (single leg; no upgrade without second leg).")
        elif completeness in ("LOW", "PARTIAL"):
            sentences.append(
                "Geopolitics watch is currently driven mainly by VIX; Brent is available but does not confirm oil shock, and breakeven inflation input remains stale/partial."
            )
        else:
            sentences.append("Geopolitics/inflation radar in " + radar_b_status + "; Brent and breakeven legs effective.")
    else:
        sentences.append("Geopolitics/inflation radar normal.")

    if res_status == "OFF":
        sentences.append("No systemic resonance is confirmed.")
    conf = (confidence_result or {}).get("confidence", "MEDIUM")
    summary = " ".join(sentences) + " Signal confidence: " + conf + "."
    if (freshness_result or {}).get("event_x_freshness_risk") == "HIGH":
        summary += " Freshness risk HIGH (multiple critical inputs stale)."
    if isinstance(details_geo, dict) and details_geo.get("breakeven_is_stale"):
        summary += " Breakeven input is stale; geopolitics inflation chain is only partially confirmed."
    # Plain-English 翻译层（给非专业读者）
    plain = _build_event_x_plain_english(radar_a_status, radar_b_status, res_status, completeness)
    if plain:
        summary += " In plain English: " + plain
    return summary


def _build_event_x_plain_english(
    radar_a_status: str, radar_b_status: str, res_status: str, completeness: str,
) -> str:
    """给非专业读者的简短翻译，保持专业语气。"""
    parts = []
    if radar_a_status != "NORMAL":
        parts.append("public-market proxies are showing early stress, but broad credit conditions are not yet in crisis mode.")
    if radar_b_status != "NORMAL":
        if completeness in ("LOW", "PARTIAL"):
            parts.append("markets are nervous, but oil and inflation expectations have not fully confirmed a broader inflation shock.")
        else:
            parts.append("geopolitics/inflation legs are elevated; oil and inflation expectations support the watch.")
    if res_status == "OFF":
        if not parts:
            parts.append("no warning lights are on.")
        else:
            parts.append("several warning lights are flickering, but they have not yet formed a full chain reaction.")
    if res_status != "OFF":
        parts.append("multiple risk legs are reinforcing; treat as elevated systemic concern.")
    return " ".join(parts) if parts else ""


def _build_event_x_priority_risks_section(
    struct_results: dict,
    resonance_result: dict,
    freshness_result: Optional[dict] = None,
    confidence_result: Optional[dict] = None,
    geopolitics_completeness: Optional[dict] = None,
) -> str:
    """Event-X 置顶区块：两雷达 + Resonance + Signal Confidence + Freshness Risk + Geopolitics 数据腿可见性 + Machine Summary。"""
    pc = struct_results.get("private_credit_liquidity_radar")
    geo = struct_results.get("geopolitics_inflation_radar")
    pc_alert = pc.alert if hasattr(pc, "alert") else (pc.get("alert", "NONE") if isinstance(pc, dict) else "NONE")
    geo_alert = geo.alert if hasattr(geo, "alert") else (geo.get("alert", "NONE") if isinstance(geo, dict) else "NONE")
    _VALID_ALERTS = {"NONE", "WATCH", "ALERT", "ALARM"}
    if pc_alert not in _VALID_ALERTS:
        pc_alert = "NONE"
    if geo_alert not in _VALID_ALERTS:
        geo_alert = "NONE"
    radar_a_status = "NORMAL" if pc_alert == "NONE" else pc_alert
    radar_b_status = "NORMAL" if geo_alert == "NONE" else geo_alert
    res_status = (resonance_result or {}).get("level", "OFF")

    machine_summary = _build_event_x_machine_summary(
        struct_results, resonance_result, freshness_result, confidence_result, geopolitics_completeness
    )
    conf = (confidence_result or {}).get("confidence", "MEDIUM")
    fresh_risk = (freshness_result or {}).get("event_x_freshness_risk", "LOW")
    comp = (geopolitics_completeness or {}).get("completeness", "")

    lines = [
        "## 🔴 Event-X Priority Risks",
        "",
        f"- **Private Credit Liquidity Radar**: {radar_a_status}",
        f"- **Geopolitics & Inflation Radar**: {radar_b_status}",
        f"- **Resonance Trigger**: {res_status}",
        f"- **Signal Confidence**: {conf}",
        f"- **Freshness Risk**: {fresh_risk}",
    ]
    if comp:
        lines.append(f"- **Geopolitics completeness**: {comp}")
    details_geo = (getattr(geo, "details", None) or (geo if isinstance(geo, dict) else {})) if geo else {}
    if isinstance(details_geo, dict):
        brent_last = details_geo.get("brent_last") if details_geo.get("brent_last") is not None else details_geo.get("brent")
        brent_yoy = details_geo.get("brent_yoy") if details_geo.get("brent_yoy") is not None else details_geo.get("brent_yoy_pct")
        breakeven_last = details_geo.get("breakeven_effective_last") or details_geo.get("breakeven_last") or details_geo.get("t5yie_last") or details_geo.get("t5yie_pct")
        breakeven_source = details_geo.get("breakeven_source_used") or "FRED_T5YIE"
        breakeven_stale = details_geo.get("breakeven_is_stale", True)
        missing = details_geo.get("missing_inputs") or []
        if brent_last is not None or breakeven_last is not None or "brent" in missing or "breakeven" in missing:
            parts = []
            if brent_last is not None:
                by = ""
                if brent_yoy is not None and not (isinstance(brent_yoy, (int, float)) and np.isnan(brent_yoy)):
                    try:
                        by = f", YoY {float(brent_yoy):.1f}%"
                    except (TypeError, ValueError):
                        pass
                parts.append(f"Brent {float(brent_last):.1f}{by}")
            elif "brent" in missing:
                parts.append("Brent: missing / unavailable")
            if breakeven_last is not None:
                bdate = details_geo.get("breakeven_last_date") or details_geo.get("t5yie_last_obs_date") or details_geo.get("breakeven_last_valid_date")
                src = " (realtime proxy)" if breakeven_source == "COMPUTED_DGS5_T5YIFR" else " (FRED)"
                stale_note = "; stale" if breakeven_stale else ""
                parts.append(f"Breakeven {float(breakeven_last):.2f}%{src}" + (f" as of {bdate}" if bdate else "") + stale_note)
            elif "breakeven" in missing or "t5yie" in missing:
                parts.append("Breakeven: missing / stale / unavailable")
            if details_geo.get("vix_last") is not None:
                parts.append("VIX " + str(details_geo.get("vix_last")))
            if parts:
                lines.append("- **Data legs**: " + "; ".join(parts))
    lines.append("")
    lines.append("*Machine Summary:*")
    lines.append(machine_summary)
    lines.append("")
    lines.append(_build_event_x_reading_guide())
    lines.append("")
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
    json_data["summary"]["regime_weight_notes"] = summary.get("regime_weight_notes", {})
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

    # Regime-Aware (Fiscal Dominance Era) dashboard + 迟滞
    try:
        monitor = regime_module.CrisisMonitor(base.BASE)
        monitor.run_all_checks()
        raw_verdict, regime_detail = monitor.evaluate_systemic_risk()
        stabilized_verdict, h_notes = hysteresis_module.get_stabilized_verdict(raw_verdict, output_dir)
        regime_verdict = stabilized_verdict
        for m in regime_detail.get("modules", {}).values():
            v = m.get("value")
            if v is not None and isinstance(v, (np.floating, float)) and np.isnan(v):
                m["value"] = None
            elif isinstance(v, (np.floating, np.integer)):
                m["value"] = float(v)
        json_data["regime"] = {
            "verdict": regime_verdict,
            "raw_verdict": raw_verdict,
            "hysteresis_notes": h_notes,
            **regime_detail,
        }
        regime_dashboard_md = regime_module.build_regime_dashboard_md(regime_verdict, regime_detail)
    except Exception as e:
        _log_v2_stage(f"⚠️ Regime dashboard 跳过: {e}")
        regime_verdict = "N/A"
        regime_dashboard_md = ""
        json_data["regime"] = {"verdict": "N/A", "explanations": [str(e)], "modules": {}}

    # Conflict & Divergence (Policy Incoherence) panel
    conflict_dashboard_md = ""
    try:
        conflict_mon = conflict_module.ConflictMonitor(base.BASE)
        conflict_mon.run_all_checks()
        def _safe_value(x):
            if x is None:
                return None
            try:
                f = float(x)
                return None if np.isnan(f) else f
            except (TypeError, ValueError):
                return None
        conflict_results = {k: {"name": v.name, "status": v.status, "value": _safe_value(v.value), "reason": v.reason} for k, v in conflict_mon.results.items()}
        json_data["conflict"] = {"modules": conflict_results}
        conflict_dashboard_md = conflict_module.build_conflict_panel_md(conflict_mon.results)
    except Exception as e:
        _log_v2_stage(f"⚠️ Conflict & Divergence panel 跳过: {e}")
        json_data["conflict"] = {"modules": {}, "error": str(e)}

    # Regime Layer: Structural & Regime Risks (alerts only; does not affect base score)
    structural_regime_md = ""
    event_x_section = ""
    try:
        struct_mon = structural_module.StructuralRiskMonitor(base.BASE)
        struct_mon.run_all_checks()
        def _safe_float(x):
            if x is None: return None
            try:
                f = float(x)
                return None if np.isnan(f) else f
            except (TypeError, ValueError):
                return None
        def _sanitize_details(d):
            if not d: return d
            out = {}
            for k, val in d.items():
                if isinstance(val, (int, float)) or (hasattr(val, "item") and np.isscalar(val)):
                    out[k] = _safe_float(val)
                elif isinstance(val, dict):
                    out[k] = _sanitize_details(val)
                else:
                    out[k] = val
            return out
        struct_results = {
            k: {
                "name": v.name,
                "alert": v.alert,
                "value": _safe_float(v.value),
                "reason": v.reason,
                "details": _sanitize_details(v.details),
            }
            for k, v in struct_mon.results.items()
        }
        json_data["structural_regime"] = {"modules": struct_results, "has_alert": struct_mon.has_any_alert()}
        if struct_mon.has_any_alert():
            structural_regime_md = structural_module.build_regime_alerts_md(struct_mon.results)

        # Event-X: Resonance Trigger (Layer 3) + 置顶区块数据
        pc = struct_mon.results.get("private_credit_liquidity_radar")
        geo = struct_mon.results.get("geopolitics_inflation_radar")
        credit_stress_on = bool(
            summary.get("confirmation_signals", {}).get("credit_stress", {}).get("on", False)
        )
        details_pc = (pc.details if hasattr(pc, "details") else {}) if pc else {}
        details_geo = (geo.details if hasattr(geo, "details") else {}) if geo else {}
        data_snapshot = {
            "hy_oas_weekly_change_bp": details_pc.get("hy_oas_weekly_chg_bp"),
            "t5yie": details_geo.get("breakeven_effective_last") or details_geo.get("t5yie_pct"),
            "brent": details_geo.get("brent"),
            "vix": details_geo.get("vix"),
            "bizd_vs_50dma_pct": details_pc.get("bizd_vs_50dma_pct"),
            "stlfsi4": details_pc.get("stlfsi4"),
            "credit_stress_on": credit_stress_on,
        }
        resonance_result = event_x_resonance_module.evaluate_resonance_triggers(data_snapshot)
        json_data["event_x_resonance"] = resonance_result
        # 分层新鲜度与信号可信度（fail-open）
        freshness_result = event_x_freshness_module.evaluate_data_freshness_severity(
            json_data, struct_results, resonance_result, indicators_config
        )
        json_data["event_x_freshness"] = freshness_result
        confidence_result = event_x_freshness_module.evaluate_event_x_signal_confidence(
            struct_results, resonance_result, freshness_result
        )
        json_data["event_x_signal_confidence"] = confidence_result
        # Geopolitics 数据完整性（有效可用性，非仅字段存在）
        geo = struct_mon.results.get("geopolitics_inflation_radar")
        geo_details = (getattr(geo, "details", None) or (geo if isinstance(geo, dict) else {})) if geo else {}
        geopolitics_completeness = event_x_freshness_module.evaluate_geopolitics_data_completeness(geo_details, freshness_result)
        json_data["event_x_geopolitics_completeness"] = geopolitics_completeness
        # Private Credit 明细（用于报告与动量触发说明）
        pc = struct_mon.results.get("private_credit_liquidity_radar")
        pc_details = (getattr(pc, "details", None) or (pc if isinstance(pc, dict) else {})) if pc else {}
        if isinstance(pc_details, dict):
            json_data["event_x_private_credit_detail"] = {
                k: pc_details.get(k) for k in (
                    "hy_oas_last", "hy_oas_5d_bp_change", "stlfsi4_last", "bizd_drawdown_50dma",
                    "used_inputs", "missing_inputs", "stlfsi_series_used", "watch_triggered_by_momentum",
                )
            }
        else:
            json_data["event_x_private_credit_detail"] = {}
        # event_x_status_quality: 已修好 vs 仍弱
        json_data["event_x_status_quality"] = {
            "private_credit": {
                "fixed_items": ["STLFSI4 unified", "BIZD patch active", "HY momentum input active"],
                "remaining_weaknesses": ["absolute spreads still benign", "early watch depends on proxy + momentum"],
            },
            "geopolitics": {
                "fixed_items": ["Brent connected", "VIX active", "confidence/freshness fields active"],
                "remaining_weaknesses": ["breakeven input stale unless realtime proxy succeeds", "current watch may still be VIX-led"],
            },
        }
        if geo_details.get("breakeven_is_stale"):
            json_data["event_x_status_quality"]["geopolitics"]["remaining_weaknesses"] = [
                "breakeven input stale; geopolitics inflation chain only partially confirmed",
                "current watch may still be VIX-led",
            ]
        event_x_section = _build_event_x_priority_risks_section(
            struct_mon.results, resonance_result,
            freshness_result=freshness_result, confidence_result=confidence_result,
            geopolitics_completeness=geopolitics_completeness,
        )
    except Exception as e:
        _log_v2_stage(f"⚠️ Structural & Regime Risks 跳过: {e}")
        json_data["structural_regime"] = {"modules": {}, "has_alert": False, "error": str(e)}
        json_data["event_x_resonance"] = {"level": "OFF", "detail": {}, "summary": "Event-X evaluation skipped."}
        json_data["event_x_freshness"] = {"critical": [], "important": [], "info": [], "event_x_freshness_risk": "LOW", "summary": "Skipped."}
        json_data["event_x_signal_confidence"] = {"confidence": "MEDIUM", "reasons": [], "summary": "Skipped."}
        json_data["event_x_geopolitics_completeness"] = {"core_inputs_present": 0, "core_inputs_effective": 0, "completeness": "LOW", "summary": "Skipped.", "missing_or_weak_legs": []}
        json_data["event_x_private_credit_detail"] = {}
        json_data["event_x_status_quality"] = {"private_credit": {"fixed_items": [], "remaining_weaknesses": []}, "geopolitics": {"fixed_items": [], "remaining_weaknesses": []}}

    # 资产配置映射：根据体制给出仓位建议
    json_data["allocation"] = AllocationRecommender.get_allocation_suggestion(regime_verdict or "NORMAL")
    allocation_section = _build_allocation_section(regime_verdict)

    # LLM 叙事层：仅「完整报告」时调用（每日定时发送不调用以省 API）
    # 条件：未设置 CRISIS_MONITOR_SKIP_AI_NARRATOR=1 且（CRISIS_MONITOR_FULL_REPORT=1 或 任一 API Key）
    ai_narrative_section = ""
    try:
        skip_ai = os.environ.get("CRISIS_MONITOR_SKIP_AI_NARRATOR", "").strip() == "1"
        full_report = os.environ.get("CRISIS_MONITOR_FULL_REPORT", "").strip() == "1"
        has_key = bool(
            os.environ.get("DASHSCOPE_API_KEY")
            or os.environ.get("TONGYI_API_KEY")
            or os.environ.get("OPENAI_API_KEY")
            or os.environ.get("GEMINI_API_KEY")
        )
        use_ai = not skip_ai and (full_report or has_key)
        if use_ai:
            import ai_narrator as ai_narrator_module
            narrative = ai_narrator_module.generate_narrative_from_data(json_data)
            if narrative:
                json_data["ai_narrative"] = narrative
                ai_narrative_section = "\n## 🤖 每日宏观简报 (AI)\n\n" + narrative.strip() + "\n\n"
                _log_v2_stage("✅ AI 叙事已生成并写入报告")
            elif has_key:
                _log_v2_stage("⚠️ AI Narrator 已调用但未返回内容（检查 API/网络）")
    except Exception as e:
        _log_v2_stage(f"⚠️ AI Narrator 跳过: {e}")

    # Event-X 验收与维护者摘要（fail-open）
    try:
        acceptance = event_x_acceptance_module.run_acceptance_checks(json_data)
        json_data["event_x_acceptance_status"] = acceptance
        json_data["event_x_maintainer_summary"] = event_x_acceptance_module.run_maintainer_summary(
            json_data, acceptance=acceptance
        )
    except Exception as e:
        _log_v2_stage(f"⚠️ Event-X acceptance 跳过: {e}")
        json_data["event_x_acceptance_status"] = {}
        json_data["event_x_maintainer_summary"] = {
            "non_regression_passed": False,
            "required_fields_present": False,
            "stale_downgrade_rules_passed": False,
            "smoke_tests_ready": True,
            "historical_validation_ready": False,
            "notes": [str(e)],
        }

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
            # Event-X Priority Risks 置顶（在 Executive Verdict 之上）
            if event_x_section:
                lines.insert(insert_idx, event_x_section.rstrip())
                lines.insert(insert_idx + 1, "")
                insert_idx += 2
            lines.insert(insert_idx, "")
            lines.insert(insert_idx + 1, executive_section.rstrip())
            lines.insert(insert_idx + 2, "")
            pos = 3
            if regime_dashboard_md:
                lines.insert(insert_idx + pos, regime_dashboard_md.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if conflict_dashboard_md:
                lines.insert(insert_idx + pos, conflict_dashboard_md.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if structural_regime_md:
                lines.insert(insert_idx + pos, structural_regime_md.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if allocation_section:
                lines.insert(insert_idx + pos, allocation_section.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if ai_narrative_section:
                lines.insert(insert_idx + pos, ai_narrative_section.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if ratio_negative_note:
                lines.insert(insert_idx + pos, ratio_negative_note)
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if data_conf_line:
                lines.insert(insert_idx + pos, data_conf_line)
                lines.insert(insert_idx + pos + 1, "")
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
    global REGIME_VERDICT_FOR_SCORING
    _log_v2_stage("🚀 启动 V2 报告生成")
    # 先跑 Regime 以得到 verdict，经迟滞后供评分阶段应用体制乘数
    REGIME_VERDICT_FOR_SCORING = None
    output_dir = base.BASE / "outputs" / "crisis_monitor"
    try:
        monitor = regime_module.CrisisMonitor(base.BASE)
        monitor.run_all_checks()
        raw_verdict, _ = monitor.evaluate_systemic_risk()
        stabilized, h_notes = hysteresis_module.get_stabilized_verdict(raw_verdict, output_dir)
        REGIME_VERDICT_FOR_SCORING = stabilized
        _log_v2_stage(f"📌 Regime: raw={raw_verdict} → stabilized={stabilized}（迟滞: {h_notes.get('reason', '')}）")
    except Exception as e:
        _log_v2_stage(f"⚠️ Regime 预跑失败(评分将不应用体制乘数): {e}")
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
