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
import logging
import os
import pathlib
import re
import subprocess
import sys
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

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

SERIES_ROOT = base.BASE / "data" / "fred" / "series"
import crisis_monitor_regime as regime_module
import conflict_monitor as conflict_module
import structural_risk as structural_module
import regime_hysteresis as hysteresis_module
import event_x_resonance as event_x_resonance_module
import event_x_freshness as event_x_freshness_module
import event_x_acceptance as event_x_acceptance_module
import historical_analogs as historical_analogs_module

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


# --- Canonical risk control (Layer 1 final state → Layer 4 portfolio; 唯一主决策口径) ---
CANONICAL_PORTFOLIO_BY_STATE: Dict[str, Dict[str, str]] = {
    "L0_NORMAL": {
        "SPX": "Overweight",
        "Gold": "Neutral",
        "TLT": "Neutral",
        "Cash_BIL": "Underweight",
        "strategy": "正常/全风险偏好：广度与趋势支持时可维持全仓与成长敞口（仍以自身风险承受为准）。",
    },
    "L1_STRUCTURAL_TENSION": {
        "SPX": "Neutral",
        "Gold": "Neutral",
        "TLT": "Neutral",
        "Cash_BIL": "Overweight",
        "strategy": "停止加杠杆；削减弱势广度板块；建立现金缓冲。",
    },
    "L2_DEFENSIVE_ROTATION": {
        "SPX": "Underweight",
        "Gold": "Accumulate",
        "TLT": "Underweight",
        "Cash_BIL": "Overweight",
        "strategy": "降低久期；低配成长/小盘；超配能源、硬资产与现金。",
    },
    "L3_SYSTEMIC_STRESS": {
        "SPX": "Strong Sell",
        "Gold": "Buy",
        "TLT": "Buy",
        "Cash_BIL": "Strong Buy",
        "strategy": "最大防御：现金/极短久期为主，尾部对冲优先。",
    },
}

_FINAL_STATE_ZH: Dict[str, str] = {
    "L0_NORMAL": "常态",
    "L1_STRUCTURAL_TENSION": "结构性紧张",
    "L2_DEFENSIVE_ROTATION": "防御性轮动",
    "L3_SYSTEMIC_STRESS": "系统性压力",
}


def _canonical_final_state_key_from_action_state(as_: dict) -> str:
    try:
        lvl = int(as_.get("level", as_.get("Level", 1)))
    except (TypeError, ValueError):
        lvl = 1
    keys = ("L0_NORMAL", "L1_STRUCTURAL_TENSION", "L2_DEFENSIVE_ROTATION", "L3_SYSTEMIC_STRESS")
    if 0 <= lvl < len(keys):
        return keys[lvl]
    return "L1_STRUCTURAL_TENSION"


def _canonical_portfolio_mapping(final_state: str) -> Dict[str, object]:
    row = CANONICAL_PORTFOLIO_BY_STATE.get(
        final_state, CANONICAL_PORTFOLIO_BY_STATE["L1_STRUCTURAL_TENSION"]
    )
    return {"final_state": final_state, **dict(row)}


def _derive_final_confidence(summ: dict) -> str:
    """Layer 2：与最终状态解耦，仅表示新鲜度/确认/一致性（不单独抬状态）。"""
    dc = str(summ.get("data_freshness_confidence", "OK") or "OK").upper()
    cs = summ.get("confirmation_signals") or {}
    conf_on = False
    if isinstance(cs, dict):
        conf_on = any(
            isinstance(v, dict) and v.get("on") for v in cs.values()
        )
    if dc == "LOW":
        return "LOW"
    if conf_on and summ.get("early_warning_confirmed"):
        return "HIGH"
    return "MEDIUM"


def finalize_canonical_risk_control(json_data: dict, summary: Optional[dict] = None) -> dict:
    """
    单一主决策口径：由 action_state 的 L0–L3 映射 canonical final_state，
    同步 status_label / diagnostic_scores / portfolio_mapping，消除与确认矩阵、HV 抬分之间的多语言冲突。
    """
    summ = json_data.get("summary")
    if not isinstance(summ, dict):
        summ = {}
        json_data["summary"] = summ

    as_ = json_data.get("action_state")
    if not isinstance(as_, dict):
        as_ = compute_action_state(json_data)
        json_data["action_state"] = as_

    legacy_confirmation_label = summ.get("status_label")

    hv = json_data.get("high_voltage_circuit_breaker") or {}
    breaker_active = bool(hv.get("active"))

    diag = summ.setdefault("diagnostic_scores", {})
    if summ.get("stress_now_index") is not None and "stress_now_index" not in diag:
        diag["stress_now_index"] = summ.get("stress_now_index")
    if summ.get("fast_ew_index") is not None and "fast_ew_index" not in diag:
        diag["fast_ew_index"] = summ.get("fast_ew_index")
    if summ.get("slow_macro_deterioration_index") is not None and "slow_macro_deterioration_index" not in diag:
        diag["slow_macro_deterioration_index"] = summ.get("slow_macro_deterioration_index")
    ts = json_data.get("total_score")
    if ts is not None and "total_score" not in diag:
        try:
            diag["total_score"] = round(float(ts), 2)
        except (TypeError, ValueError):
            diag["total_score"] = ts

    final_state = _canonical_final_state_key_from_action_state(as_)
    final_zh = _FINAL_STATE_ZH.get(final_state, final_state)
    state_floor_source = (
        "HIGH_VOLTAGE_CIRCUIT_BREAKER" if breaker_active else "MARKET_INTERNALS_AND_MACRO_SIGNALS"
    )
    confidence = _derive_final_confidence(summ)
    pm = _canonical_portfolio_mapping(final_state)

    rc: Dict[str, object] = {
        "final_state": final_state,
        "final_state_level": int(as_.get("level", as_.get("Level", 0))),
        "final_state_label_zh": final_zh,
        "final_confidence": confidence,
        "state_floor_source": state_floor_source,
        "breaker_active": breaker_active,
        "breaker_reasons": list(hv.get("reasons") or []),
        "breaker_override_reason": hv.get("override_reason"),
        "portfolio_mapping": pm,
        "legacy_fields": {
            # 原 status_label 仅反映确认矩阵+快慢变量，不等价于最终 L0–L3 决策口径（已弃用作 headline）
            "confirmation_status_label": legacy_confirmation_label,
        },
        "audit_trace": {
            "action_state_machine_label": as_.get("label"),
            "systemic_risk_tier": summ.get("systemic_risk_tier"),
            "early_warning_index_displayed": summ.get("early_warning_index"),
            "early_warning_index_pre_breaker": diag.get("early_warning_index_pre_breaker"),
        },
    }
    json_data["risk_control"] = rc
    summ["risk_control"] = rc
    summ["legacy_confirmation_status_label"] = legacy_confirmation_label
    summ["diagnostic_scores"] = diag
    # status_label 重新定义：始终镜像 canonical final_state（单一风险语言）
    summ["status_label"] = f"{final_zh} [{final_state}]"

    as_["canonical_final_state"] = final_state
    as_["final_confidence"] = confidence

    if isinstance(summary, dict):
        summary["risk_control"] = rc
        summary["diagnostic_scores"] = diag
        summary["legacy_confirmation_status_label"] = legacy_confirmation_label
        summary["status_label"] = summ["status_label"]

    return rc


def _build_unified_portfolio_stance_section(regime_verdict: str, risk_control: dict) -> str:
    """Layer 4：主口径仅来自 final_state；Regime 表降为附录。"""
    rc = risk_control or {}
    pm = rc.get("portfolio_mapping") or {}
    fs = rc.get("final_state", "?")
    fzh = rc.get("final_state_label_zh", "")
    lines = [
        "## 🛡️ Portfolio Stance（主口径：canonical final_state）",
        "",
        f"**最终状态**: `{fs}` — {fzh}",
        f"**置信度**: {rc.get('final_confidence', '-')}",
        f"**状态地板来源**: `{rc.get('state_floor_source', '-')}`",
        "",
        "### 主口径仓位",
        "",
        "| 资产 | 建议 |",
        "|------|------|",
    ]
    for key in ("SPX", "Gold", "TLT", "Cash_BIL"):
        lines.append(f"| {key} | {pm.get(key, '-')} |")
    lines.append("")
    lines.append(f"**策略**: {pm.get('strategy', '-')}")
    lines.append("")
    lines.append("### Regime 体制附录（仅供对照，不与主口径并列决策）")
    legacy = AllocationRecommender.get_allocation_suggestion(regime_verdict or "NORMAL")
    lines.append(f"- **Regime verdict**: `{regime_verdict}`")
    lines.append("")
    lines.append("| 资产 | 体制侧建议 |")
    lines.append("|------|------------|")
    for key in ("SPX", "Gold", "TLT", "Cash_BIL"):
        lines.append(f"| {key} | {legacy.get(key, '-')} |")
    lines.append("")
    lines.append(f"**体制策略**: {legacy.get('strategy', '-')}")
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
        "transform_chain": "VIX3M: FRED(缓存+API, 遇 ConnectionError/RetryError 静默切 Yahoo ^VIX3M); VIX/VIX3M; 备选 ^VIX9D/VIX; 终极 VIX/MA60(无 NaN 清洗); 常量 1.0 最后兜底",
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


def _fetch_vix3m_dual_source() -> pd.Series:
    """
    VIX3M 双源镜像：优先 FRED（本地缓存 + fetch_series）；若仍空或 live API 抛错（含 RetryError），
    不向外抛出，改拉 Yahoo ^VIX3M。
    """
    try:
        from scripts.clean_utils import parse_numeric_series as _pns
    except Exception:
        _pns = None  # type: ignore[assignment]

    s = pd.to_numeric(base.fetch_series("VIX3M"), errors="coerce").dropna()
    if len(s) > 0:
        return s

    try:
        from scripts.fred_http import series_observations

        data = series_observations("VIX3M", sort_order="desc")
        obs = (data or {}).get("observations") or []
        if obs and _pns is not None:
            df = pd.DataFrame(obs)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            s2 = _pns(df["value"]).dropna()
            if len(s2) > 0:
                return s2.sort_index()
    except Exception as e:
        en = type(e).__name__
        if en == "RetryError" or isinstance(e, ConnectionError):
            logger.warning("VIX3M FRED 路径失败 (%s)，切换 Yahoo ^VIX3M", en)
        else:
            logger.warning("VIX3M FRED 路径失败 (%s)，切换 Yahoo ^VIX3M", en)

    y = pd.to_numeric(_fetch_yahoo_series("^VIX3M"), errors="coerce").dropna()
    return y


def _vix_term_structure_no_nan(ratio: pd.Series) -> pd.Series:
    """期限结构比值序列：有限、>0，严禁 NaN（ffill/bfill 后仍缺则填 1.0）。"""
    r = pd.to_numeric(ratio, errors="coerce").astype(float)
    r = r.replace([np.inf, -np.inf], np.nan)
    r = r.ffill().bfill()
    r = r.fillna(1.0)
    r = r.clip(lower=1e-6, upper=1e3)
    r.name = ratio.name or "VIX_TERM_STRUCTURE"
    return r


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


def _compose_t5yie_realtime() -> pd.Series:
    """
    T5YIE 实时代理，三级降级策略：
    Level 1: ^FVX (Yahoo) - DFII5 (FRED) → 最准确
    Level 2: FRED T5YIE 原始值（若滞后 <= 7 天）
    Level 3: ^FVX 单独作为近似值（最后兜底）

    返回：pd.Series，index 为日期，values 为 breakeven 利率（%）
    附加 attribute: .source 标注数据来源
    """
    today = pd.Timestamp.now().normalize()

    # --- Level 1: ^FVX - DFII5 ---
    try:
        fvx = base.fetch_yahoo_safe("^FVX")
        if fvx is not None and len(fvx) > 0:
            fvx_latest = pd.to_datetime(fvx.index[-1])
            if (today - fvx_latest).days <= 3:
                dfii5_path = SERIES_ROOT / "DFII5" / "raw.csv"
                if dfii5_path.exists():
                    dfii5 = pd.read_csv(dfii5_path, index_col=0, parse_dates=True)
                    value_col = "value" if "value" in dfii5.columns else dfii5.columns[0]
                    dfii5_series = pd.to_numeric(dfii5[value_col], errors="coerce").dropna()
                    if not dfii5_series.empty:
                        dfii5_latest = dfii5_series.index[-1]
                        if (today - pd.Timestamp(dfii5_latest)).days <= 7:
                            fvx_close = fvx["Close"] if isinstance(fvx, pd.DataFrame) and "Close" in fvx.columns else fvx
                            if isinstance(fvx_close, pd.DataFrame):
                                fvx_close = fvx_close.squeeze()
                            fvx_close = pd.to_numeric(fvx_close, errors="coerce").dropna()
                            combined = pd.concat([fvx_close, dfii5_series], axis=1, join="inner")
                            combined.columns = ["fvx", "dfii5"]
                            breakeven = (combined["fvx"] - combined["dfii5"]).dropna()
                            if len(breakeven) > 10:
                                logger.info(
                                    "T5YIE proxy: Level 1 (^FVX - DFII5), 最新=%s, 值=%.3f%%",
                                    breakeven.index[-1].date(),
                                    breakeven.iloc[-1],
                                )
                                breakeven.name = "T5YIE"
                                breakeven.source = "FVX_MINUS_DFII5"  # type: ignore[attr-defined]
                                return breakeven
    except Exception as e:
        logger.warning("T5YIE Level 1 失败: %s", e)

    # --- Level 2: FRED T5YIE 原始值 ---
    try:
        t5yie_path = SERIES_ROOT / "T5YIE" / "raw.csv"
        if t5yie_path.exists():
            t5yie_df = pd.read_csv(t5yie_path, index_col=0, parse_dates=True)
            value_col = "value" if "value" in t5yie_df.columns else t5yie_df.columns[0]
            t5yie_series = pd.to_numeric(t5yie_df[value_col], errors="coerce").dropna()
            if not t5yie_series.empty:
                latest = t5yie_series.index[-1]
                lag = (today - pd.Timestamp(latest)).days
                if lag <= 7:
                    logger.info("T5YIE proxy: Level 2 (FRED T5YIE), 滞后=%d天", lag)
                    t5yie_series.name = "T5YIE"
                    t5yie_series.source = "FRED_T5YIE"  # type: ignore[attr-defined]
                    return t5yie_series
                logger.warning("T5YIE Level 2: FRED T5YIE 滞后 %d 天，继续降级", lag)
    except Exception as e:
        logger.warning("T5YIE Level 2 失败: %s", e)

    # --- Level 3: ^FVX 单独近似（最后兜底）---
    try:
        fvx = base.fetch_yahoo_safe("^FVX")
        if fvx is not None and len(fvx) > 0:
            fvx_close = fvx["Close"] if isinstance(fvx, pd.DataFrame) and "Close" in fvx.columns else fvx
            if isinstance(fvx_close, pd.DataFrame):
                fvx_close = fvx_close.squeeze()
            fvx_close = pd.to_numeric(fvx_close, errors="coerce").dropna()
            logger.warning(
                "T5YIE proxy: Level 3 降级 (^FVX 近似)，注意：这不是真实 breakeven，仅为名义利率"
            )
            fvx_close.name = "T5YIE"
            fvx_close.source = "FVX_APPROX"  # type: ignore[attr-defined]
            return fvx_close
    except Exception as e:
        logger.warning("T5YIE Level 3 失败: %s", e)

    logger.error("T5YIE 所有数据源均失败，返回空序列")
    empty = pd.Series(dtype="float64", name="T5YIE")
    empty.source = "FAILED"  # type: ignore[attr-defined]
    return empty


def compose_series_v2(series_id: str) -> Optional[pd.Series]:
    sid = series_id.upper()
    if sid == "T5YIE":
        return _compose_t5yie_realtime()
    if sid == "VIX_TERM_STRUCTURE":
        # 核心路径：FRED VIXCLS + VIX3M（双源：FRED→遇错静默 Yahoo ^VIX3M）
        # 备选：^VIX9D / VIX；终极：VIX/MA60（缩短窗口 min_periods 以适配短历史）；常量 1.0 兜底，序列无 NaN
        try:
            vix = base.fetch_series("VIXCLS")
            if vix is None or vix.empty:
                vix = _fetch_yahoo_series("^VIX")
                if not vix.empty:
                    _mark_source(series_id, "yahoo_vix")
            else:
                _mark_source(series_id, "fred_vixcls")

            vix3m = _fetch_vix3m_dual_source()

            vix = pd.to_numeric(vix, errors="coerce").dropna() if vix is not None else pd.Series(dtype=float)
            vix3m = (
                pd.to_numeric(vix3m, errors="coerce").dropna()
                if vix3m is not None
                else pd.Series(dtype=float)
            )

            if len(vix) > 0 and len(vix3m) > 0:
                a, b = vix.align(vix3m, join="inner")
                ratio = (a / b).replace([np.inf, -np.inf], np.nan).dropna()
                ratio = ratio[np.isfinite(ratio) & (ratio > 0)]
                if len(ratio) > 0:
                    _mark_source(series_id, "vix_over_vix3m")
                    return _vix_term_structure_no_nan(ratio)

            if len(vix) > 0:
                vix9d = _fetch_yahoo_series("^VIX9D")
                vix9d = pd.to_numeric(vix9d, errors="coerce").dropna()
                if len(vix9d) > 0:
                    a, b = vix9d.align(vix, join="inner")
                    ratio = (a / b).replace([np.inf, -np.inf], np.nan).dropna()
                    ratio = ratio[np.isfinite(ratio) & (ratio > 0)]
                    if len(ratio) > 0:
                        _mark_source(series_id, "yahoo_vix9d_over_vix")
                        return _vix_term_structure_no_nan(ratio)

                win = min(60, max(5, len(vix) - 1))
                min_p = max(3, min(20, win // 2))
                logger.warning(
                    "CRITICAL: VIX3M 双源不可用或无法对齐。使用 VIX/MA%d 作为期限结构 1:1 平替（min_periods=%d）。",
                    win,
                    min_p,
                )
                ma = vix.rolling(win, min_periods=min_p).mean()
                ratio = (vix / ma).replace([np.inf, -np.inf], np.nan)
                ratio = ratio[np.isfinite(ratio) & (ratio > 0)]
                if len(ratio) > 0:
                    _mark_source(series_id, "vix_over_ma60_term_proxy")
                    return _vix_term_structure_no_nan(ratio)

                # 单点或极短 VIX：仍输出常数曲线，避免下游 NaN
                _mark_source(series_id, "vix_term_constant_fallback")
                return _vix_term_structure_no_nan(pd.Series(1.0, index=vix.index))
        except Exception as ex:
            logger.warning("VIX_TERM_STRUCTURE 组合异常，使用中性 1.0: %s", ex)
        _mark_source(series_id, "vix_term_constant_fallback")
        return _vix_term_structure_no_nan(pd.Series([1.0], index=[pd.Timestamp.now(tz=None).normalize()]))
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
    rc = summary.get("risk_control") or {}
    leg_confirm = summary.get("legacy_confirmation_status_label")
    diag = summary.get("diagnostic_scores") or {}
    lines: list = [
        "## 🧭 早预警指数",
        "",
    ]
    if rc:
        lines += [
            f"- **final_state（主口径）**: `{rc.get('final_state')}` — {rc.get('final_state_label_zh')}",
            f"- **final_confidence**: {rc.get('final_confidence')}",
            f"- **status_label（与主口径对齐）**: {summary.get('status_label')}",
            "",
        ]
    if leg_confirm is not None:
        lines += [
            f"- **legacy_confirmation_status_label**（仅确认矩阵叙事，deprecated 作独立风险结论）: {leg_confirm}",
            "",
        ]
    lines += [
        f"- **stress_now_index**: {summary.get('stress_now_index', 0)}",
        f"- **early_warning_index**（展示值；HV 可能抬升）: {summary.get('early_warning_index', 0)}",
    ]
    if diag.get("early_warning_index_pre_breaker") is not None:
        lines.append(
            f"- **early_warning_index_pre_breaker**（诊断，HV 前）: {diag.get('early_warning_index_pre_breaker')}"
        )
    lines += [
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
        f"- **confirmation_state**: {summary.get('confirmation_state', {})}",
        "",
        "### 🔎 change_score Top 5",
    ]
    for item in summary.get("top_change_indicators", []):
        lines.append(f"- {item['name']} ({item['series_id']}): {item['change_score']}")
    return "\n".join(lines) + "\n"


def generate_executive_summary(
    summary: dict,
    profiles: Dict[str, dict],
    risk_control: Optional[dict] = None,
) -> dict:
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
    confirmation_on = False
    if isinstance(confirmation_signals, dict):
        confirmation_on = any(
            isinstance(v, dict) and v.get("on") for v in confirmation_signals.values()
        )
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
        f"（诊断）快慢变量融合标量 early_warning_index={early_warning_index:.1f}，"
        f"**非独立最终决策口径**；快变量 fast_ew_index={fast_ew_index:.1f}，慢变量 slow_macro={slow_macro_index:.1f}，"
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

    lead_paras: list = []
    if risk_control and isinstance(risk_control, dict):
        fs_zh = risk_control.get("final_state_label_zh") or ""
        fs_key = risk_control.get("final_state") or ""
        conf = risk_control.get("final_confidence") or ""
        brk = (
            "高压断路器已抬高风险状态地板。"
            if risk_control.get("breaker_active")
            else "未触发高压断路器。"
        )
        pm = (risk_control.get("portfolio_mapping") or {}).get("strategy", "")
        lead_paras.append(
            f"【主决策口径】最终状态：{fs_zh}（`{fs_key}`），置信度 {conf}。{brk}"
            f"主仓位策略：{pm}"
        )

    all_paras = lead_paras + [para1, para2, para3, para4]
    return {
        "classification": regime,
        "paragraphs": all_paras,
        "text": "\n\n".join(all_paras),
    }


def _mi_float(x: object) -> Optional[float]:
    """微观结构输出用：非有限浮点转为 None，避免 NaN 进入 JSON/下游。"""
    if x is None:
        return None
    try:
        f = float(x)
        if not np.isfinite(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _yf_close_from_download(df: pd.DataFrame, ticker: Optional[str] = None) -> Optional[pd.Series]:
    """从 yfinance 下载结果中取收盘价序列（兼容 MultiIndex 列）。"""
    if df is None or df.empty:
        return None
    price_kind: Optional[str] = None
    if isinstance(df.columns, pd.MultiIndex):
        lv0 = list(df.columns.get_level_values(0).unique())
        if "Adj Close" in lv0:
            price_kind = "Adj Close"
        elif "Close" in lv0:
            price_kind = "Close"
    else:
        if "Adj Close" in df.columns:
            price_kind = "Adj Close"
        elif "Close" in df.columns:
            price_kind = "Close"

    s: Optional[pd.Series] = None
    if isinstance(df.columns, pd.MultiIndex):
        try:
            sub = df.xs(price_kind or "Close", axis=1, level=0)
        except Exception:
            try:
                sub = df.xs("Close", axis=1, level=0)
            except Exception:
                sub = df.iloc[:, :1]
        if isinstance(sub, pd.Series):
            s = sub
        elif ticker and ticker in sub.columns:
            s = sub[ticker]
        elif sub.shape[1] > 0:
            s = sub.iloc[:, 0]
    else:
        c = price_kind or "Close"
        if c in df.columns:
            s = df[c]
        else:
            s = df.iloc[:, 0]
    if s is None:
        return None
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s = pd.to_numeric(s, errors="coerce").dropna()
    if getattr(s.index, "tz", None) is not None:
        s.index = s.index.tz_localize(None)
    return s if len(s) > 0 else None


def _yf_adj_close_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """多标的 yfinance 结果 → 宽表，列为各 ticker 收盘价（优先 Adj Close）。"""
    if df is None or df.empty:
        return pd.DataFrame()
    price_kind = None
    if isinstance(df.columns, pd.MultiIndex):
        lv0 = list(df.columns.get_level_values(0).unique())
        if "Adj Close" in lv0:
            price_kind = "Adj Close"
        elif "Close" in lv0:
            price_kind = "Close"
        if price_kind:
            try:
                sub = df.xs(price_kind, axis=1, level=0)
            except Exception:
                sub = df.xs("Close", axis=1, level=0)
        else:
            sub = df.iloc[:, :1]
    else:
        c = "Adj Close" if "Adj Close" in df.columns else "Close"
        sub = df[[c]] if c in df.columns else df.iloc[:, :1]
        sub.columns = [df.columns[0] if len(df.columns) else "close"]
    if isinstance(sub, pd.Series):
        sub = sub.to_frame()
    out = sub.apply(pd.to_numeric, errors="coerce")
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_localize(None)
    return out.sort_index().ffill()


def _fetch_panel_a_tail_and_fx() -> dict:
    """
    面板 A（独立 Yahoo 批次，失败不影响主股票微观结构）：
    - ^VVIX vs ^VIX：波动率的波动率，末日期权/做市商对冲焦虑代理
    - DX-Y.NYB vs USDCNH=X：美元篮子微动但离岸人民币快速走弱 → 亚洲美元抽水代理
    """
    import yfinance as yf

    out: dict = {
        "vol_of_vol": {"status": "UNAVAILABLE"},
        "offshore_cnh_dxy": {"status": "UNAVAILABLE"},
    }
    tickers = ["^VVIX", "^VIX", "DX-Y.NYB", "USDCNH=X"]
    try:
        raw = yf.download(tickers, period="300d", progress=False, threads=False)
        c = _yf_adj_close_matrix(raw)
        if c.empty:
            return out
        c = c.ffill().bfill(limit=3)
    except Exception as e:
        out["_panel_a_error"] = str(e)
        return out

    # ---------- VVIX / VIX ----------
    try:
        vv_col = "^VVIX" if "^VVIX" in c.columns else None
        vx_col = "^VIX" if "^VIX" in c.columns else None
        if vv_col and vx_col:
            vv = c[vv_col].dropna()
            vx = c[vx_col].reindex(vv.index).ffill()
            common = vv.index.intersection(vx.dropna().index)
            if len(common) >= 6:
                vv = vv.reindex(common)
                vx = vx.reindex(common)
                vv_last = _mi_float(vv.iloc[-1])
                vx_last = _mi_float(vx.iloc[-1])
                vv_ma5 = _mi_float(vv.iloc[-5:].mean())
                vv_ma20 = _mi_float(vv.iloc[-20:].mean()) if len(vv) >= 20 else vv_ma5
                d1 = None
                if len(vv) >= 2:
                    try:
                        d1 = (float(vv.iloc[-1]) / float(vv.iloc[-2]) - 1.0) * 100.0
                    except (TypeError, ValueError, ZeroDivisionError):
                        d1 = None
                tail_spike = bool(
                    vv_last is not None
                    and vx_last is not None
                    and vv_last >= 110.0
                    and vx_last < 20.0
                )
                elevated = bool(vv_last is not None and vv_last >= 100.0)
                if tail_spike:
                    st = "TAIL_SPIKE"
                elif elevated:
                    st = "ELEVATED"
                else:
                    st = "OK"
                out["vol_of_vol"] = {
                    "status": st,
                    "vvix_last": vv_last,
                    "vix_last": vx_last,
                    "vvix_ma5": vv_ma5,
                    "vvix_ma20": vv_ma20,
                    "vvix_1d_pct": d1,
                    "tail_hedge_panic_proxy": tail_spike,
                    "note": "VVIX 飙升而现货 VIX 仍低 → 期权市场抢购极端保护（黑天鹅前 24–48h 常见剧本，非必然）",
                }
    except Exception as e:
        out["vol_of_vol"] = {"status": "UNAVAILABLE", "error": str(e)}

    # ---------- DXY vs USDCNH ----------
    try:
        dxy_c = "DX-Y.NYB" if "DX-Y.NYB" in c.columns else None
        cnh_c = "USDCNH=X" if "USDCNH=X" in c.columns else None
        if dxy_c and cnh_c:
            dxy = c[dxy_c].dropna()
            cnh = c[cnh_c].dropna()
            common = dxy.index.intersection(cnh.index)
            if len(common) >= 6:
                dxy = dxy.reindex(common)
                cnh = cnh.reindex(common)
                dxy_5d = (float(dxy.iloc[-1]) / float(dxy.iloc[-6]) - 1.0) * 100.0
                cnh_5d = (float(cnh.iloc[-1]) / float(cnh.iloc[-6]) - 1.0) * 100.0
                # 美元篮子仅小涨/横盘，但 USDCNH 明显上行 → 离岸人民币偏弱快于广义美元
                divergent = bool(
                    (-0.35 <= dxy_5d <= 2.0)
                    and (cnh_5d >= 0.35)
                    and (cnh_5d > dxy_5d + 0.15)
                )
                st = "STRESS" if divergent else "OK"
                out["offshore_cnh_dxy"] = {
                    "status": st,
                    "dxy_pct_change_5d": round(dxy_5d, 3),
                    "usdcnh_pct_change_5d": round(cnh_5d, 3),
                    "asia_dollar_drain_proxy": divergent,
                    "note": "DXY 微动而 USDCNH 5日上行 → 亚洲端美元更紧/资金抽离代理（与 A 股北向敏感板块相关）",
                }
    except Exception as e:
        out["offshore_cnh_dxy"] = {"status": "UNAVAILABLE", "error": str(e)}

    return out


def _compute_panel_b_geopolitics(closes_main: pd.DataFrame) -> dict:
    """
    面板 B：地缘与粘性通胀（全凭市场价格，不用 NLP）。
    TIP/IEF、ITA/SPY、WTI：**CL12=F 已 delisted**，原油以 **CL=F 现货 vs MA200 乖离 + 5 日动量** 为核心（Spot-to-MA200 Proxy）。
    """
    import yfinance as yf

    def _empty() -> dict:
        return {
            "market_implied_inflation": {
                "status": "UNAVAILABLE",
                "sticky_inflation_priced_in": False,
                "tip_ief_ratio": None,
                "ratio_ma60": None,
                "momentum_5d_positive": False,
            },
            "war_premium": {
                "status": "UNAVAILABLE",
                "war_premium_active": False,
                "ita_spy_ratio": None,
                "ratio_ma60": None,
            },
            "oil_term_structure": {
                "status": "UNAVAILABLE",
                "severe_shortage": False,
                "mode": None,
                "front_12m_ratio": None,
                "cl_vs_ma200_pct": None,
                "momentum_5d_positive": False,
            },
            "gold_top_radar": {
                "gold_top_warning": False,
                "gold_vs_ma200_pct": None,
                "dxy_5d_pct": None,
                "gold_series_used": None,
            },
        }

    # 不再请求 CL12=F（Yahoo 已 delisted，避免无效拉取）
    tickers_b = ["TIP", "IEF", "ITA", "CL=F", "GC=F", "GLD", "DX-Y.NYB"]
    try:
        raw = yf.download(tickers_b, period="400d", progress=False, threads=False)
        pb = _yf_adj_close_matrix(raw)
        if pb is None or pb.empty:
            return _empty()
        pb = pb.ffill().bfill(limit=5)
    except Exception as e:
        out = _empty()
        out["_panel_b_error"] = str(e)
        return out

    out: dict = _empty()

    # ---------- A. TIP / IEF（隐含通胀粘性） ----------
    try:
        if "TIP" in pb.columns and "IEF" in pb.columns:
            sub = pb[["TIP", "IEF"]].dropna(how="any")
            ratio = (sub["TIP"] / sub["IEF"]).replace([np.inf, -np.inf], np.nan).dropna()
            if len(ratio) >= 66:
                ma60 = ratio.rolling(60).mean()
                r_last = _mi_float(ratio.iloc[-1])
                m60 = _mi_float(ma60.iloc[-1])
                mom5 = bool(float(ratio.iloc[-1]) > float(ratio.iloc[-6]))
                above_ma = bool(
                    r_last is not None
                    and m60 is not None
                    and r_last > m60
                )
                sticky = bool(above_ma and mom5)
                st_inf = "STICKY_INFLATION_PRICED_IN" if sticky else "NORMAL"
                out["market_implied_inflation"] = {
                    "status": st_inf,
                    "sticky_inflation_priced_in": sticky,
                    "tip_ief_ratio": r_last,
                    "ratio_ma60": m60,
                    "momentum_5d_positive": mom5,
                    "note": "TIP/IEF 上破 60 日均线且 5 日动量为正 → 资金押注通胀粘性（CPI 公布前即可观察）",
                }
    except Exception as e:
        out["market_implied_inflation"]["error"] = str(e)

    # ---------- B. ITA / SPY（战争溢价 / 国防硬资产） ----------
    try:
        if "ITA" in pb.columns and closes_main is not None and "SPY" in closes_main.columns:
            spy = closes_main["SPY"].dropna()
            ita = pb["ITA"].reindex(spy.index).ffill().bfill(limit=5)
            aligned = pd.DataFrame({"ITA": ita, "SPY": spy}).dropna(how="any")
            if len(aligned) >= 66:
                rr = (aligned["ITA"] / aligned["SPY"]).replace([np.inf, -np.inf], np.nan).dropna()
                if len(rr) >= 66:
                    ma60 = rr.rolling(60).mean()
                    r_last = _mi_float(rr.iloc[-1])
                    m60 = _mi_float(ma60.iloc[-1])
                    war = bool(
                        r_last is not None
                        and m60 is not None
                        and r_last > m60
                    )
                    st_w = "WAR_PREMIUM_ACTIVE" if war else "NORMAL"
                    out["war_premium"] = {
                        "status": st_w,
                        "war_premium_active": war,
                        "ita_spy_ratio": r_last,
                        "ratio_ma60": m60,
                        "note": "ITA/SPY 强于 60 日均线 → 资金涌入国防/航天硬资产，地缘溢价定价",
                    }
    except Exception as e:
        out["war_premium"]["error"] = str(e)

    # ---------- C. 原油：现货偏离度（Spot-to-MA200 Proxy，CL12=F 已弃用）----------
    try:
        cl_col = "CL=F" if "CL=F" in pb.columns else None
        oil_st = "NORMAL"
        severe = False
        mode = "Spot-to-MA200 Proxy"
        cl_ma200_pct = None
        mom_up = False

        if cl_col:
            cl = pb[cl_col].dropna()
            if len(cl) >= 205:
                ma200 = cl.rolling(200).mean()
                last_c = float(cl.iloc[-1])
                ma = float(ma200.iloc[-1])
                prev5 = float(cl.iloc[-6]) if len(cl) >= 6 else last_c
                if ma > 0:
                    dev_pct = (last_c / ma - 1.0) * 100.0
                    cl_ma200_pct = round(dev_pct, 2)
                    mom_up = bool(last_c > prev5)
                    # 核心：现货 > MA200×1.15 且 5 日动量为正 → 紧缺/逼仓代理
                    if last_c > ma * 1.15 and mom_up:
                        severe = True
                        oil_st = "SEVERE_SHORTAGE"
            elif len(cl) >= 60:
                w = min(200, len(cl))
                ma_long = cl.rolling(w).mean()
                last_c = float(cl.iloc[-1])
                ma = float(ma_long.iloc[-1])
                prev5 = float(cl.iloc[-6]) if len(cl) >= 6 else last_c
                if ma > 0:
                    dev_pct = (last_c / ma - 1.0) * 100.0
                    cl_ma200_pct = round(dev_pct, 2)
                    mom_up = bool(last_c > prev5)
                    if last_c > ma * 1.15 and mom_up:
                        severe = True
                        oil_st = "SEVERE_SHORTAGE"
                        mode = f"Spot-to-MA200 Proxy (MA{w})"

        out["oil_term_structure"] = {
            "status": oil_st,
            "severe_shortage": severe,
            "mode": mode,
            "front_12m_ratio": None,
            "cl_vs_ma200_pct": cl_ma200_pct,
            "momentum_5d_positive": mom_up,
            "note": "Mode: Spot-to-MA200 Proxy。CL12=F 已下线；以 WTI 现货相对 MA200 乖离>15%（>MA×1.15）且 5 日动量为正作为 SEVERE_SHORTAGE。",
        }
    except Exception as e:
        out["oil_term_structure"] = {
            "status": "UNAVAILABLE",
            "severe_shortage": False,
            "mode": None,
            "front_12m_ratio": None,
            "cl_vs_ma200_pct": None,
            "momentum_5d_positive": False,
            "error": str(e),
        }

    # ---------- D. 黄金见顶雷达（超买 + 强美元逆风） ----------
    try:
        gold_top = False
        g_sym = None
        gold_vs_ma200_pct = None
        dxy_5d_pct = None
        gold_s = None
        if "GC=F" in pb.columns and pb["GC=F"].notna().sum() >= 60:
            gold_s = pb["GC=F"].dropna()
            g_sym = "GC=F"
        elif "GLD" in pb.columns and pb["GLD"].notna().sum() >= 60:
            gold_s = pb["GLD"].dropna()
            g_sym = "GLD"

        if gold_s is not None and len(gold_s) >= 205:
            ma200 = gold_s.rolling(200).mean()
            last_g = float(gold_s.iloc[-1])
            ma = float(ma200.iloc[-1])
            if ma > 0:
                gold_vs_ma200_pct = round((last_g / ma - 1.0) * 100.0, 2)
        elif gold_s is not None and len(gold_s) >= 60:
            w = min(200, len(gold_s))
            ma_long = gold_s.rolling(w).mean()
            last_g = float(gold_s.iloc[-1])
            ma = float(ma_long.iloc[-1])
            if ma > 0:
                gold_vs_ma200_pct = round((last_g / ma - 1.0) * 100.0, 2)

        if "DX-Y.NYB" in pb.columns:
            dxy = pb["DX-Y.NYB"].dropna()
            if len(dxy) >= 6:
                dxy_5d_pct = (float(dxy.iloc[-1]) / float(dxy.iloc[-6]) - 1.0) * 100.0

        overbought = gold_vs_ma200_pct is not None and gold_vs_ma200_pct > 15.0
        dxy_strong = dxy_5d_pct is not None and dxy_5d_pct > 1.0
        gold_top = bool(overbought and dxy_strong)
        out["gold_top_radar"] = {
            "gold_top_warning": gold_top,
            "gold_vs_ma200_pct": gold_vs_ma200_pct,
            "dxy_5d_pct": round(dxy_5d_pct, 3) if dxy_5d_pct is not None else None,
            "gold_series_used": g_sym,
            "note": "金价显著高于长均线且 DXY 5日强反弹 → 宏观逆风下易回吐，勿追高黄金 ETF",
        }
    except Exception as e:
        out["gold_top_radar"] = {
            "gold_top_warning": False,
            "gold_vs_ma200_pct": None,
            "dxy_5d_pct": None,
            "gold_series_used": None,
            "error": str(e),
        }

    return out


def fetch_market_internals() -> dict:
    """
    Yahoo 市场微观结构（不计入 Base Layer 评分）：
    - 广度：RSP/SPY 等权相对强弱（替代已下线的 ^SPXA200R / S5TH 等）
    - 趋势参考：SPY MA200、^SPX 末值与 MA200（非广度代理）
    - 板块：XLE/XLK、IWM/SPY（沿用原逻辑，基于 300d 面板末 60 个交易日）
    - 面板 A：^VVIX / ^VIX（尾风险）、DX-Y.NYB / USDCNH=X（离岸美元紧张代理）
    - 面板 B：TIP/IEF、ITA/SPY、WTI 曲线（地缘与粘性通胀，市场价格驱动）
    """
    import yfinance as yf

    results: dict = {}
    tickers = ["SPY", "IWM", "XLE", "XLK", "^SPX", "RSP"]

    try:
        raw = yf.download(tickers, period="300d", progress=False, threads=False)
        closes = _yf_adj_close_matrix(raw)
        if closes.empty:
            raise ValueError("empty_close_matrix")
        closes = closes.ffill().bfill(limit=3)
    except Exception as e:
        return {
            "rsp_spy_breadth": {"status": "UNAVAILABLE", "error": str(e)},
            "spy_index_reference": {"status": "UNAVAILABLE"},
            "spx_index_reference": {"status": "UNAVAILABLE"},
            "energy_vs_tech": {"status": "UNAVAILABLE", "error": str(e)},
            "small_vs_large": {"status": "UNAVAILABLE", "error": str(e)},
        }

    # ---------- RSP/SPY 广度 ----------
    try:
        need = [c for c in ("RSP", "SPY") if c in closes.columns]
        if len(need) == 2:
            sub = closes[["RSP", "SPY"]].dropna(how="any")
            if len(sub) >= 25:
                ratio = sub["RSP"] / sub["SPY"]
                ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
                if len(ratio) >= 21:
                    ma20 = ratio.rolling(20).mean()
                    r_last = _mi_float(ratio.iloc[-1])
                    m20 = _mi_float(ma20.iloc[-1])
                    pct5 = _mi_float(
                        (ratio.iloc[-1] / ratio.iloc[-6] - 1.0) * 100.0
                    ) if len(ratio) > 5 else None
                    if r_last is not None and m20 is not None:
                        if r_last < m20:
                            b_status = "BREADTH_DETERIORATING"
                        elif r_last > m20 * 1.01:
                            b_status = "BREADTH_HEALTHY"
                        else:
                            b_status = "BREADTH_NEUTRAL"
                    else:
                        b_status = "UNAVAILABLE"
                    results["rsp_spy_breadth"] = {
                        "rsp_spy_ratio": r_last,
                        "rsp_spy_ma20": m20,
                        "pct_change_5d": pct5 if pct5 is not None else 0.0,
                        "status": b_status,
                    }
                else:
                    results["rsp_spy_breadth"] = {
                        "status": "UNAVAILABLE",
                        "error": "insufficient_ratio_history",
                    }
            else:
                results["rsp_spy_breadth"] = {
                    "status": "UNAVAILABLE",
                    "error": "insufficient_overlap",
                }
        else:
            results["rsp_spy_breadth"] = {
                "status": "UNAVAILABLE",
                "error": "missing_rsp_or_spy_column",
            }
    except Exception as e:
        results["rsp_spy_breadth"] = {"status": "UNAVAILABLE", "error": str(e)}

    # ---------- SPY 指数趋势参考（非广度） ----------
    try:
        if "SPY" in closes.columns:
            spy = closes["SPY"].dropna()
            if len(spy) > 200:
                ma200 = spy.rolling(200).mean()
                sp = _mi_float(spy.iloc[-1])
                m200 = _mi_float(ma200.iloc[-1])
                results["spy_index_reference"] = {
                    "spy_price": sp,
                    "spy_ma200": m200,
                    "above_ma200": bool(sp is not None and m200 is not None and sp > m200),
                    "status": "OK",
                }
            else:
                results["spy_index_reference"] = {
                    "status": "UNAVAILABLE",
                    "error": "need_more_than_200_rows",
                }
        else:
            results["spy_index_reference"] = {"status": "UNAVAILABLE", "error": "no_spy"}
    except Exception as e:
        results["spy_index_reference"] = {"status": "UNAVAILABLE", "error": str(e)}

    # ---------- ^SPX 参考 ----------
    spx_col = "^SPX" if "^SPX" in closes.columns else None
    try:
        if spx_col:
            spx = closes[spx_col].dropna()
            if len(spx) > 200:
                ma200 = spx.rolling(200).mean()
                results["spx_index_reference"] = {
                    "spx_last": _mi_float(spx.iloc[-1]),
                    "spx_ma200": _mi_float(ma200.iloc[-1]),
                    "status": "OK",
                }
            elif len(spx) > 0:
                results["spx_index_reference"] = {
                    "spx_last": _mi_float(spx.iloc[-1]),
                    "spx_ma200": None,
                    "status": "OK",
                }
            else:
                results["spx_index_reference"] = {"status": "UNAVAILABLE"}
        else:
            results["spx_index_reference"] = {"status": "UNAVAILABLE", "error": "no_spx_column"}
    except Exception as e:
        results["spx_index_reference"] = {"status": "UNAVAILABLE", "error": str(e)}

    # ---------- XLE/XLK（末 60 日） ----------
    try:
        tail = closes.iloc[-60:].copy() if len(closes) >= 10 else closes
        if "XLE" in tail.columns and "XLK" in tail.columns:
            aligned = tail[["XLE", "XLK"]].dropna(how="any")
            if len(aligned) > 5:
                ratio = aligned["XLE"] / aligned["XLK"]
                ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
                if len(ratio) > 5:
                    current = _mi_float(ratio.iloc[-1])
                    ma20 = _mi_float(ratio.rolling(20).mean().iloc[-1])
                    pct_change_5d = _mi_float(
                        (ratio.iloc[-1] / ratio.iloc[-6] - 1.0) * 100.0
                    ) or 0.0
                    if current is not None and ma20 is not None:
                        if current > ma20 * 1.05:
                            energy_status = "ENERGY_DOMINANT"
                        elif current < ma20 * 0.95:
                            energy_status = "TECH_DOMINANT"
                        else:
                            energy_status = "NEUTRAL"
                        results["energy_vs_tech"] = {
                            "xle_xlk_ratio": current,
                            "ma20": ma20,
                            "pct_change_5d": pct_change_5d,
                            "status": energy_status,
                        }
                    else:
                        results["energy_vs_tech"] = {
                            "status": "UNAVAILABLE",
                            "error": "non_finite_ratio",
                        }
                else:
                    results["energy_vs_tech"] = {
                        "status": "UNAVAILABLE",
                        "error": "insufficient_ratio_points",
                    }
            else:
                results["energy_vs_tech"] = {
                    "status": "UNAVAILABLE",
                    "error": "insufficient_overlap",
                }
        else:
            results["energy_vs_tech"] = {
                "status": "UNAVAILABLE",
                "error": "missing_series",
            }
    except Exception as e:
        results["energy_vs_tech"] = {"status": "UNAVAILABLE", "error": str(e)}

    # ---------- IWM/SPY（末 60 日） ----------
    try:
        tail = closes.iloc[-60:].copy() if len(closes) >= 10 else closes
        if "IWM" in tail.columns and "SPY" in tail.columns:
            aligned = tail[["IWM", "SPY"]].dropna(how="any")
            if len(aligned) > 5:
                ratio = aligned["IWM"] / aligned["SPY"]
                ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
                if len(ratio) > 5:
                    current = _mi_float(ratio.iloc[-1])
                    ma20 = _mi_float(ratio.rolling(20).mean().iloc[-1])
                    ma60 = (
                        _mi_float(ratio.rolling(60).mean().iloc[-1])
                        if len(ratio) >= 60
                        else ma20
                    )
                    pct_change_5d = _mi_float(
                        (ratio.iloc[-1] / ratio.iloc[-6] - 1.0) * 100.0
                    ) or 0.0
                    if current is not None and ma60 is not None:
                        if current < ma60 * 0.97:
                            size_status = "SMALL_CAP_LAGGING"
                        elif current > ma60 * 1.03:
                            size_status = "SMALL_CAP_LEADING"
                        else:
                            size_status = "NEUTRAL"
                        results["small_vs_large"] = {
                            "iwm_spy_ratio": current,
                            "ma20": ma20,
                            "ma60": ma60,
                            "pct_change_5d": pct_change_5d,
                            "status": size_status,
                        }
                    else:
                        results["small_vs_large"] = {
                            "status": "UNAVAILABLE",
                            "error": "non_finite_ratio",
                        }
                else:
                    results["small_vs_large"] = {
                        "status": "UNAVAILABLE",
                        "error": "insufficient_ratio_points",
                    }
            else:
                results["small_vs_large"] = {
                    "status": "UNAVAILABLE",
                    "error": "insufficient_overlap",
                }
        else:
            results["small_vs_large"] = {
                "status": "UNAVAILABLE",
                "error": "missing_series",
            }
    except Exception as e:
        results["small_vs_large"] = {"status": "UNAVAILABLE", "error": str(e)}

    # 面板 A：与主下载隔离，避免单个 FX 标的拖垮全表
    try:
        pa = _fetch_panel_a_tail_and_fx()
        results["panel_a_tail_and_fx"] = pa
    except Exception as e:
        results["panel_a_tail_and_fx"] = {
            "vol_of_vol": {"status": "UNAVAILABLE", "error": str(e)},
            "offshore_cnh_dxy": {"status": "UNAVAILABLE", "error": str(e)},
        }

    # 面板 B：TIP/IEF、ITA/SPY、原油现货偏离度（Spot-to-MA200；不再依赖 CL12=F）
    try:
        results["panel_b_geopolitics"] = _compute_panel_b_geopolitics(closes)
    except Exception as e:
        results["panel_b_geopolitics"] = {
            "market_implied_inflation": {"status": "UNAVAILABLE", "sticky_inflation_priced_in": False},
            "war_premium": {"status": "UNAVAILABLE", "war_premium_active": False},
            "oil_term_structure": {
                "status": "UNAVAILABLE",
                "severe_shortage": False,
                "mode": None,
                "front_12m_ratio": None,
                "cl_vs_ma200_pct": None,
                "momentum_5d_positive": False,
            },
            "gold_top_radar": {"gold_top_warning": False},
            "_panel_b_error": str(e),
        }

    return results


def render_market_internals_md(market_internals: dict) -> str:
    """生成「市场微观结构」Markdown 章节（Regime Monitor 补充，不影响 Base 分）。"""
    if not market_internals or (isinstance(market_internals, dict) and market_internals.get("_fetch_error")):
        err = (market_internals or {}).get("_fetch_error", "未知错误")
        return f"## 📊 市场微观结构（Market Internals）\n\n> 本次未能拉取 Yahoo 微观结构数据：`{err}`\n"

    lines = ["## 📊 市场微观结构（Market Internals）", ""]
    lines.append("> 高频价格行为指标；广度用 **RSP/SPY** 等权相对强弱代理（不计入 Base Layer 评分）")
    lines.append("")

    # ----- 面板 A：VVIX / 离岸人民币 -----
    pa = market_internals.get("panel_a_tail_and_fx") or {}
    vv = pa.get("vol_of_vol") or {}
    fx = pa.get("offshore_cnh_dxy") or {}
    lines.append("### 面板 A：尾风险（VVIX）与离岸美元代理（USDCNH vs DXY）")
    lines.append("")
    if vv.get("status") not in (None, "UNAVAILABLE") or vv.get("vvix_last") is not None:
        vv_st = vv.get("status", "UNAVAILABLE")
        vvl = vv.get("vvix_last")
        vxl = vv.get("vix_last")
        m5 = vv.get("vvix_ma5")
        panic = vv.get("tail_hedge_panic_proxy", False)
        emoji_vv = "🔴" if panic else ("🟡" if vv_st in ("ELEVATED", "TAIL_SPIKE") else "🟢")
        vvl_s = f"{vvl:.1f}" if vvl is not None else "N/A"
        vxl_s = f"{vxl:.1f}" if vxl is not None else "N/A"
        m5_s = f"{m5:.1f}" if m5 is not None else "N/A"
        lines.append(
            f"- **^VVIX / ^VIX** {emoji_vv}：VVIX **{vvl_s}**（5日均 {m5_s}），现货 VIX **{vxl_s}**，状态 `{vv_st}`"
        )
        if panic:
            lines.append(
                "  - ⚠️ **尾风险警报**：VVIX 高位而 VIX 仍偏低 → 期权端抢购极端保护，留意 24–48h 波动率突变。"
            )
        if vv.get("note"):
            lines.append(f"  - *{vv['note']}*")
    else:
        err = vv.get("error") or pa.get("_panel_a_error", "")
        lines.append(f"- **^VVIX / ^VIX**：不可用" + (f"（{err}）" if err else ""))
    lines.append("")
    if fx.get("dxy_pct_change_5d") is not None or fx.get("status") not in (None, "UNAVAILABLE"):
        drain = fx.get("asia_dollar_drain_proxy", False)
        emoji_fx = "🔴" if drain else "🟢"
        d5 = fx.get("dxy_pct_change_5d")
        u5 = fx.get("usdcnh_pct_change_5d")
        lines.append(
            f"- **DXY vs USDCNH** {emoji_fx}：DXY 5日 **{d5:+.2f}%**，USDCNH 5日 **{u5:+.2f}%**，"
            f"代理状态 `{fx.get('status', 'N/A')}`"
        )
        if drain:
            lines.append("  - ⚠️ **亚洲美元抽水代理**：广义美元未暴涨但离岸人民币更快走弱 → 关注 EM / A 股外资链。")
        if fx.get("note"):
            lines.append(f"  - *{fx['note']}*")
    else:
        err = fx.get("error") or ""
        lines.append(f"- **DXY vs USDCNH**：不可用" + (f"（{err}）" if err else ""))
    lines.append("")

    # ----- 面板 B：地缘溢价与粘性通胀 -----
    pb = market_internals.get("panel_b_geopolitics") or {}
    lines.append("### 面板 B：地缘战争溢价与粘性通胀 (Market-Implied Geopolitics)")
    lines.append("")
    inf = pb.get("market_implied_inflation") or {}
    if inf.get("tip_ief_ratio") is not None or inf.get("status") not in (None, "UNAVAILABLE"):
        sticky = bool(inf.get("sticky_inflation_priced_in"))
        em_i = "🔴" if sticky else "🟢"
        tr = inf.get("tip_ief_ratio")
        m60 = inf.get("ratio_ma60")
        tr_s = f"{tr:.4f}" if tr is not None else "N/A"
        m60_s = f"{m60:.4f}" if m60 is not None else "N/A"
        lines.append(
            f"- **TIP/IEF（隐含通胀粘性）** {em_i}：比值 **{tr_s}** vs MA60 **{m60_s}**，`{inf.get('status', 'N/A')}`"
        )
        if sticky:
            lines.append(
                "  - 大型资金押注通胀粘性；比单月 CPI 更领先，可联动 HALO/实物链。"
            )
        if inf.get("note"):
            lines.append(f"  - *{inf['note']}*")
    else:
        lines.append(f"- **TIP/IEF**：不可用" + (f"（{inf.get('error', '')}）" if inf.get("error") else ""))

    war = pb.get("war_premium") or {}
    if war.get("ita_spy_ratio") is not None or war.get("status") not in (None, "UNAVAILABLE"):
        w_on = bool(war.get("war_premium_active"))
        em_w = "🔴" if w_on else "🟢"
        ir = war.get("ita_spy_ratio")
        im = war.get("ratio_ma60")
        ir_s = f"{ir:.5f}" if ir is not None else "N/A"
        im_s = f"{im:.5f}" if im is not None else "N/A"
        lines.append(
            f"- **ITA/SPY（战争溢价 / 国防硬资产）** {em_w}：比值 **{ir_s}** vs MA60 **{im_s}**，`{war.get('status', 'N/A')}`"
        )
        if w_on:
            lines.append("  - 资金系统性涌入国防航天；与 XLE 等共振时常确认地缘重塑定价。")
        if war.get("note"):
            lines.append(f"  - *{war['note']}*")
    else:
        lines.append(f"- **ITA/SPY**：不可用" + (f"（{war.get('error', '')}）" if war.get("error") else ""))

    oil = pb.get("oil_term_structure") or {}
    if (
        oil.get("status") not in (None,)
        or oil.get("cl_vs_ma200_pct") is not None
        or (oil.get("mode") or "") != ""
    ):
        sev = bool(oil.get("severe_shortage"))
        em_o = "🔴" if sev else "🟢"
        mode = oil.get("mode") or "—"
        d200 = oil.get("cl_vs_ma200_pct")
        mom5 = bool(oil.get("momentum_5d_positive"))
        d200_s = f"{d200:+.1f}%" if d200 is not None else "N/A"
        lines.append(
            f"- **WTI 现货偏离度分析** {em_o}：`Mode: Spot-to-MA200 Proxy`，模式 `{mode}`，"
            f"CL vs MA200 **{d200_s}**，5日动量 {'↑' if mom5 else '—'}，`{oil.get('status', 'N/A')}`"
        )
        if sev:
            lines.append(
                "  - 现货显著高于长均线且短期动量向上 → 紧缺/逼仓定价代理（原 CL12 曲线已不可用）。"
            )
        if oil.get("note"):
            lines.append(f"  - *{oil['note']}*")
    else:
        lines.append(
            f"- **WTI 现货偏离度分析**：不可用"
            + (f"（{oil.get('error', '')}）" if oil.get("error") else "")
        )

    gtr = pb.get("gold_top_radar") or {}
    if gtr.get("gold_top_warning"):
        lines.append("")
        lines.append(
            "> ⚠️ **黄金触发见顶预警**：绝对价格极度超买（相对长均线），且面临强势美元逆风，**建议停止追高**黄金类 ETF/杠杆多头。"
        )
    if gtr.get("gold_series_used") or gtr.get("gold_vs_ma200_pct") is not None:
        gt_warn = bool(gtr.get("gold_top_warning"))
        em_g = "🔴" if gt_warn else "🟢"
        gv = gtr.get("gold_vs_ma200_pct")
        d5 = gtr.get("dxy_5d_pct")
        gs = gtr.get("gold_series_used") or "—"
        gv_s = f"{gv:+.1f}%" if gv is not None else "N/A"
        d5_s = f"{d5:+.2f}%" if d5 is not None else "N/A"
        lines.append(
            f"- **黄金见顶雷达（{gs}）** {em_g}：相对 MA200 乖离 **{gv_s}**，DXY 5日 **{d5_s}**"
            + (" → `Gold_Top_Warning`" if gt_warn else "")
        )
        if gtr.get("note") and not gt_warn:
            lines.append(f"  - *{gtr['note']}*")
    elif gtr.get("error"):
        lines.append(f"- **黄金见顶雷达**：不可用（{gtr.get('error', '')}）")

    if pb.get("_panel_b_error"):
        lines.append(f"- *面板 B 批次异常：{pb['_panel_b_error']}*")
    lines.append("")

    # RSP/SPY 广度
    rs = market_internals.get("rsp_spy_breadth") or {}
    r_ratio = rs.get("rsp_spy_ratio")
    r_ma20 = rs.get("rsp_spy_ma20")
    r_5d = rs.get("pct_change_5d", 0)
    b_status = rs.get("status", "UNAVAILABLE")
    b_emoji = {
        "BREADTH_HEALTHY": "🟢",
        "BREADTH_NEUTRAL": "🟡",
        "BREADTH_DETERIORATING": "🔴",
        "UNAVAILABLE": "⚪",
    }.get(b_status, "⚪")
    b_label = {
        "BREADTH_HEALTHY": "等权相对大盘偏强（广度较好）",
        "BREADTH_NEUTRAL": "中性",
        "BREADTH_DETERIORATING": "广度恶化（等权弱于市值加权，头部抱团/内部失血风险）",
        "UNAVAILABLE": str(b_status),
    }.get(b_status, str(b_status))
    if r_ratio is not None and r_ma20 is not None:
        lines.append(
            f"**📈 市场广度（RSP/SPY）**：比值 {r_ratio:.6f}，MA20 {r_ma20:.6f}，5日 {r_5d:+.2f}% {b_emoji} `{b_label}`"
        )
    else:
        err = rs.get("error", "")
        lines.append(f"**📈 市场广度（RSP/SPY）**：N/A {b_emoji} `UNAVAILABLE`" + (f"（{err}）" if err else ""))
    lines.append("")

    spy_ref = market_internals.get("spy_index_reference") or {}
    if spy_ref.get("spy_price") is not None and spy_ref.get("spy_ma200") is not None:
        ab = "站上" if spy_ref.get("above_ma200") else "未站上"
        lines.append(
            f"**📌 SPY 趋势参考**（非广度）：{spy_ref['spy_price']:.1f} vs MA200 {spy_ref['spy_ma200']:.1f}（{ab} MA200）"
        )
        lines.append("")

    spx_ref = market_internals.get("spx_index_reference") or {}
    if spx_ref.get("spx_last") is not None:
        m = spx_ref.get("spx_ma200")
        if m is not None:
            lines.append(f"**📌 标普500指数 ^SPX 参考**：{spx_ref['spx_last']:.2f} vs MA200 {m:.2f}")
        else:
            lines.append(f"**📌 标普500指数 ^SPX 参考**：{spx_ref['spx_last']:.2f}")
        lines.append("")

    evt = market_internals.get("energy_vs_tech") or {}
    ratio = evt.get("xle_xlk_ratio")
    evt_status = evt.get("status", "UNAVAILABLE")
    chg5d = evt.get("pct_change_5d", 0)
    evt_emoji = {"ENERGY_DOMINANT": "🔴", "TECH_DOMINANT": "🟢", "NEUTRAL": "🟡"}.get(evt_status, "⚪")
    ratio_str = f"XLE/XLK = {ratio:.4f}，5日变化 {chg5d:+.1f}%" if ratio is not None else "N/A"
    evt_label = {
        "ENERGY_DOMINANT": "能源主导（通胀/供给冲击 Regime）",
        "TECH_DOMINANT": "科技主导（增长 Regime）",
        "NEUTRAL": "中性",
    }.get(evt_status, str(evt_status))
    lines.append(f"**⚡ 能源 vs 科技相对强弱**：{ratio_str} {evt_emoji} `{evt_label}`")
    lines.append("")

    svl = market_internals.get("small_vs_large") or {}
    iwm_spy = svl.get("iwm_spy_ratio")
    svl_status = svl.get("status", "UNAVAILABLE")
    svl_chg = svl.get("pct_change_5d", 0)
    svl_emoji = {"SMALL_CAP_LAGGING": "🔴", "SMALL_CAP_LEADING": "🟢", "NEUTRAL": "🟡"}.get(svl_status, "⚪")
    svl_str = f"IWM/SPY = {iwm_spy:.4f}，5日变化 {svl_chg:+.1f}%" if iwm_spy is not None else "N/A"
    svl_label = {
        "SMALL_CAP_LAGGING": "小盘跑输（流动性紧缩/风险规避）",
        "SMALL_CAP_LEADING": "小盘领跑（风险偏好强）",
        "NEUTRAL": "中性",
    }.get(svl_status, str(svl_status))
    lines.append(f"**📉 小盘 vs 大盘分化**：{svl_str} {svl_emoji} `{svl_label}`")
    lines.append("")

    signals = [evt_status, svl_status]
    if b_status == "BREADTH_DETERIORATING":
        lines.append(
            "*广度提示：RSP/SPY 跌破自身 MA20，与「少数巨头拉指数」场景相容，请勿仅用 SPY 点位判断内部健康度。*"
        )
        lines.append("")
    if "ENERGY_DOMINANT" in signals and "SMALL_CAP_LAGGING" in signals:
        lines.append(
            "**⚠️ 综合判断**：能源主导 + 小盘跑输同时触发 → 当前处于**通胀/供给冲击 Regime**，与 Anti-Fiat 框架高度一致。建议削减科技/成长敞口，增配能源/实物资产。"
        )
    elif "TECH_DOMINANT" in signals and "SMALL_CAP_LEADING" in signals:
        lines.append("**✅ 综合判断**：科技主导 + 小盘领跑 → 市场处于**增长主导 Regime**，风险偏好健康。")
    else:
        lines.append("**🟡 综合判断**：市场内部信号混合，尚未形成明确 Regime 共识。")

    lines.append("")
    return "\n".join(lines)


def _diagnostic_early_warning_for_regime_compare(summ: dict, hv_active: bool) -> float:
    """
    Regime/结构「背离」提示用：高压断路器触发时 summary.early_warning_index 已被抬升，
    此处优先用 diagnostic_scores.early_warning_index_pre_breaker，避免误称为 Base Layer。
    """
    diag = summ.get("diagnostic_scores") or {}
    pre = diag.get("early_warning_index_pre_breaker")
    try:
        if hv_active and pre is not None:
            return float(pre)
        return float(summ.get("early_warning_index", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def check_circuit_breaker(report_data: dict) -> str:
    """
    当 Regime 层严重告警但「诊断用」快慢融合 early_warning_index（HV 前）偏低时插入警告。
    注意：该标量不等价于最终 L0–L3 决策口径（见 risk_control / action_state）。
    """
    summ = report_data.get("summary") or {}
    hv_active = bool((report_data.get("high_voltage_circuit_breaker") or {}).get("active"))
    diag_ewi = _diagnostic_early_warning_for_regime_compare(summ, hv_active)

    regime_mods = (report_data.get("regime") or {}).get("modules") or {}
    if isinstance(regime_mods, dict):
        regime_iter = regime_mods.values()
    else:
        regime_iter = regime_mods if isinstance(regime_mods, list) else []
    critical_count = sum(
        1
        for m in regime_iter
        if isinstance(m, dict) and str(m.get("status", "")).upper() in ("CRITICAL", "WARNING")
    )

    struct_mods = (report_data.get("structural_regime") or {}).get("modules") or {}
    if isinstance(struct_mods, dict):
        struct_iter = struct_mods.values()
    else:
        struct_iter = struct_mods if isinstance(struct_mods, list) else []
    alert_count = sum(
        1
        for m in struct_iter
        if isinstance(m, dict) and str(m.get("alert", "")).upper() in ("ALERT", "ALARM")
    )

    warnings = []
    rv = (report_data.get("regime") or {}).get("verdict", "N/A")

    if critical_count >= 2 and diag_ewi < 45:
        hv_note = "（高压断路器触发前分值）" if hv_active else ""
        warnings.append(
            f"⚠️ **Regime–诊断背离提示**：Regime 层检测到 {critical_count} 个 CRITICAL/WARNING 模块，"
            f"但 **诊断用** early_warning_index（快慢变量融合{hv_note}）仅 {diag_ewi:.1f}，与体制层信号严重不一致。"
            f"禁止仅用该标量否定体制层；**资产配置以 `risk_control.final_state` / Portfolio 主口径为准**。"
            f"当前 Regime verdict：`{rv}`。"
        )

    if alert_count >= 2:
        warnings.append(
            f"⚠️ **结构性风险叠加**：{alert_count} 个结构性模块触发 ALERT 及以上，"
            f"即使诊断标量偏低，结构性断裂风险不可忽视；最终动作仍以主口径状态机为准。"
        )

    return "\n\n".join(warnings)


HV_OVERRIDE_REASON = (
    "Circuit Breaker Triggered: Volatility or Credit Stress detected, ignoring slow macro data."
)

# FRED SWPT：Millions of U.S. Dollars，周三更新（非 Billions）。>2000 ≈ 20 亿美元量级，过滤日常测试盘（通常 <100 百万）。
SWPT_OVERRIDE_REASON = (
    "Circuit Breaker Triggered: Global Dollar Shortage (Central Bank Swap Lines Active)."
)
SWPT_CB_MIN_MILLIONS = 2000.0  # 安全阀：实质占用互换额度（约 $2B+）


def _swpt_swap_line_circuit_breaker() -> Tuple[bool, str, Optional[float]]:
    """
    全球美元荒「核按钮」代理：SWPT 周序列显著抬升且环比上升。
    fail-open：缺数据不触发。
    """
    try:
        s = base.fetch_series("SWPT")
        if s is None or len(s) < 2:
            return False, "", None
        s = pd.to_numeric(s, errors="coerce").dropna()
        if len(s) < 2:
            return False, "", None
        last = float(s.iloc[-1])
        prev = float(s.iloc[-2])
        if last <= SWPT_CB_MIN_MILLIONS:
            return False, "", last
        rising = last > prev
        if not rising:
            return False, "", last
        return (
            True,
            f"SWPT={last:.0f}M USD (>={SWPT_CB_MIN_MILLIONS:.0f}M) WoW rising vs {prev:.0f}M",
            last,
        )
    except Exception:
        return False, "", None


def _vix_momentum_spike_from_series() -> Tuple[bool, str]:
    """
    用 VIXCLS 原始序列判断：VIX>22 且 1 日或 5 日跳升（相对变化）。
    fail-open：异常时返回 (False, "").
    """
    try:
        s = base.fetch_series("VIXCLS")
        if s is None or len(s) < 6:
            return False, ""
        s = pd.to_numeric(s, errors="coerce").dropna()
        if len(s) < 6:
            return False, ""
        v = float(s.iloc[-1])
        prev = float(s.iloc[-2])
        w5 = float(s.iloc[-6])
        if prev <= 0 or w5 <= 0:
            return False, ""
        d1 = (v / prev) - 1.0
        d5 = (v / w5) - 1.0
        sharp = (d1 >= 0.07) or (d5 >= 0.12)
        if v > 22.0 and sharp:
            return True, f"VIX={v:.1f} (1d {d1:+.1%}, 5d {d5:+.1%})"
        return False, ""
    except Exception:
        return False, ""


def eval_high_voltage_circuit_breaker(report_data: dict) -> dict:
    """
    高压断路器：快变量（VIX 跳升、HY 利差动量爆表）或 Regime 多模块 CRITICAL
    时拒绝「慢变量稀释」的线性总分结论。
    """
    ind_list = report_data.get("indicators") or []
    ind_map: Dict[str, dict] = {}
    for i in ind_list:
        sid = i.get("series_id")
        if sid:
            ind_map[str(sid)] = i

    reasons: list = []

    vix_ok, vix_detail = _vix_momentum_spike_from_series()
    if not vix_ok:
        vi = ind_map.get("VIXCLS") or {}
        try:
            cv = vi.get("current_value")
            cs = float(vi.get("change_score", 0) or 0)
            if cv is not None and float(cv) > 22.0 and cs >= 28.0:
                vix_ok = True
                vix_detail = f"VIX={float(cv):.1f} with change_score={cs:.0f} (proxy)"
        except (TypeError, ValueError):
            pass
    if vix_ok:
        reasons.append(vix_detail or "VIX stress")

    hy_hit = False
    hi = ind_map.get("HY_OAS_MOMENTUM_RATIO") or {}
    try:
        hv = hi.get("current_value")
        if hv is not None and float(hv) > 1.05:
            hy_hit = True
            reasons.append(f"HY_OAS_MOMENTUM_RATIO={float(hv):.4f}>1.05")
    except (TypeError, ValueError):
        pass

    crit = 0
    rmods = (report_data.get("regime") or {}).get("modules") or {}
    if isinstance(rmods, dict):
        for m in rmods.values():
            if isinstance(m, dict) and str(m.get("status", "")).upper() == "CRITICAL":
                crit += 1
    regime_hit = crit >= 2
    if regime_hit:
        reasons.append(f"regime_critical_count={crit}")

    swpt_hit, swpt_detail, swpt_last_m = _swpt_swap_line_circuit_breaker()
    if swpt_hit:
        reasons.append(swpt_detail or "SWPT swap line stress")

    active = bool(vix_ok or hy_hit or regime_hit or swpt_hit)
    if active:
        if swpt_hit:
            override_reason = SWPT_OVERRIDE_REASON
        else:
            override_reason = HV_OVERRIDE_REASON
    else:
        override_reason = None

    return {
        "active": active,
        "reasons": reasons,
        "override_reason": override_reason,
        "systemic_risk_tier": "ALERT" if active else None,
        "vix_trigger": bool(vix_ok),
        "hy_oas_momentum_trigger": bool(hy_hit),
        "regime_critical_count": crit,
        "regime_critical_trigger": bool(regime_hit),
        "swpt_trigger": bool(swpt_hit),
        "swpt_last_millions": swpt_last_m,
    }


def apply_high_voltage_circuit_breaker_to_report(
    json_data: dict, summary: dict, floor_score: float = 76.0
) -> None:
    """就地提升 early_warning_index / 风险标签，并写入 summary 顶层字段。"""
    hv = json_data.get("high_voltage_circuit_breaker") or {}
    if not hv.get("active"):
        return
    summ = json_data.get("summary")
    if not isinstance(summ, dict):
        return
    try:
        cur = float(summ.get("early_warning_index", 0) or 0)
    except (TypeError, ValueError):
        cur = 0.0
    diag = summ.setdefault("diagnostic_scores", {})
    if "early_warning_index_pre_breaker" not in diag:
        diag["early_warning_index_pre_breaker"] = round(cur, 2)
    newv = max(cur, float(floor_score))
    summ["early_warning_index"] = round(newv, 2)
    diag["early_warning_index_post_breaker"] = summ["early_warning_index"]
    json_data["early_warning_index"] = summ["early_warning_index"]
    summ["systemic_risk_tier"] = "ALERT"
    summ["override_reason"] = hv.get("override_reason") or HV_OVERRIDE_REASON
    summ["high_voltage_circuit_breaker"] = hv
    if isinstance(summary, dict):
        summary["early_warning_index"] = summ["early_warning_index"]
        summary["systemic_risk_tier"] = summ["systemic_risk_tier"]
        summary["override_reason"] = summ["override_reason"]
        summary["high_voltage_circuit_breaker"] = hv
        summary["diagnostic_scores"] = diag


def compute_action_state(report_data: dict) -> dict:
    """
    4 级状态机（硬逻辑 + market_internals）。
    Level 3 优先：高压断路器（VIX/信用/Regime CRITICAL 集群）。
    """
    summ = report_data.get("summary") or {}
    base_score = float(summ.get("early_warning_index", 0) or 0)
    mi = report_data.get("market_internals") or {}
    if not isinstance(mi, dict) or mi.get("_fetch_error"):
        mi = {}

    rs = mi.get("rsp_spy_breadth") or {}
    rsp_status = str(rs.get("status") or "BREADTH_NEUTRAL")

    evt = mi.get("energy_vs_tech") or {}
    energy_status = str(evt.get("status") or "NEUTRAL")
    try:
        xle_xlk_5d = float(evt.get("pct_change_5d") or 0.0)
    except (TypeError, ValueError):
        xle_xlk_5d = 0.0
    xle_xlk_momentum_up = (energy_status == "ENERGY_DOMINANT") or (xle_xlk_5d > 0.5)

    svl = mi.get("small_vs_large") or {}
    size_status = str(svl.get("status") or "NEUTRAL")
    try:
        iwm_spy_5d = float(svl.get("pct_change_5d") or 0.0)
    except (TypeError, ValueError):
        iwm_spy_5d = 0.0
    iwm_spy_momentum_weak = (size_status == "SMALL_CAP_LAGGING") or (iwm_spy_5d < -0.5)

    spy_ref = mi.get("spy_index_reference") or {}
    spy_above_ma200 = bool(spy_ref.get("above_ma200", False))

    spx_ref = mi.get("spx_index_reference") or {}
    spx_last = spx_ref.get("spx_last")
    spx_ma200 = spx_ref.get("spx_ma200")
    spx_below_200 = False
    try:
        if spx_last is not None and spx_ma200 is not None:
            spx_below_200 = float(spx_last) < float(spx_ma200)
    except (TypeError, ValueError):
        spx_below_200 = False
    index_below_ma200 = (not spy_above_ma200) or spx_below_200

    hv = report_data.get("high_voltage_circuit_breaker") or {}
    if hv.get("active"):
        reason = HV_OVERRIDE_REASON + " | " + "; ".join(hv.get("reasons") or [])
        stance = (
            "MAXIMUM DEFENSE. Systemic liquidations detected. Shift to Cash/Very Short Duration."
        )
        return {
            "level": 3,
            "Level": 3,
            "label": "SYSTEMIC_STRESS_CB",
            "Action_Stance": stance,
            "Reason": reason,
            "instruction": stance,
            "base_score": base_score,
            "circuit_breaker_active": True,
            "override_reason": hv.get("override_reason") or HV_OVERRIDE_REASON,
            "rsp_spy_status": rsp_status,
            "xle_xlk_momentum_5d_pct": xle_xlk_5d,
            "iwm_spy_momentum_5d_pct": iwm_spy_5d,
            "spy_above_ma200": spy_above_ma200,
        }

    breadth_bad = rsp_status == "BREADTH_DETERIORATING"
    breadth_ok_for_L0 = rsp_status in ("BREADTH_HEALTHY", "BREADTH_NEUTRAL")

    if breadth_bad and xle_xlk_momentum_up and index_below_ma200:
        reason = (
            "Breadth deteriorating (RSP/SPY) + energy vs tech strength + SPY/SPX below MA200."
        )
        stance = (
            "Reduce Duration. Underweight Mega-cap/Small-caps. Overweight Energy/Hard Assets/Cash."
        )
        return {
            "level": 2,
            "Level": 2,
            "label": "REGIME_INFLATION_SHOCK",
            "Action_Stance": stance,
            "Reason": reason,
            "instruction": stance,
            "base_score": base_score,
            "circuit_breaker_active": False,
            "rsp_spy_status": rsp_status,
            "xle_xlk_momentum_5d_pct": xle_xlk_5d,
            "iwm_spy_momentum_5d_pct": iwm_spy_5d,
            "spy_above_ma200": spy_above_ma200,
        }

    if breadth_bad or iwm_spy_momentum_weak:
        reason = "RSP/SPY breadth stress and/or small-cap relative weakness."
        stance = "Stop leveraging. Trim weak breadth sectors. Build cash buffer."
        return {
            "level": 1,
            "Level": 1,
            "label": "STRUCTURAL_TENSION",
            "Action_Stance": stance,
            "Reason": reason,
            "instruction": stance,
            "base_score": base_score,
            "circuit_breaker_active": False,
            "rsp_spy_status": rsp_status,
            "xle_xlk_momentum_5d_pct": xle_xlk_5d,
            "iwm_spy_momentum_5d_pct": iwm_spy_5d,
            "spy_above_ma200": spy_above_ma200,
        }

    if breadth_ok_for_L0 and spy_above_ma200:
        reason = "Breadth neutral/healthy and SPY above MA200."
        stance = "Full Allocation. Mega-cap Growth & Broad SPX Overweight."
        return {
            "level": 0,
            "Level": 0,
            "label": "NORMAL",
            "Action_Stance": stance,
            "Reason": reason,
            "instruction": stance,
            "base_score": base_score,
            "circuit_breaker_active": False,
            "rsp_spy_status": rsp_status,
            "xle_xlk_momentum_5d_pct": xle_xlk_5d,
            "iwm_spy_momentum_5d_pct": iwm_spy_5d,
            "spy_above_ma200": spy_above_ma200,
        }

    reason = "Incomplete or mixed internals — default defensive stance."
    stance = "Stop leveraging. Trim weak breadth sectors. Build cash buffer."
    return {
        "level": 1,
        "Level": 1,
        "label": "STRUCTURAL_TENSION_DEFAULT",
        "Action_Stance": stance,
        "Reason": reason,
        "instruction": stance,
        "base_score": base_score,
        "circuit_breaker_active": False,
        "rsp_spy_status": rsp_status,
        "xle_xlk_momentum_5d_pct": xle_xlk_5d,
        "iwm_spy_momentum_5d_pct": iwm_spy_5d,
        "spy_above_ma200": spy_above_ma200,
    }


def _inject_action_state_into_tldr(md_text: str, action_state: Optional[dict]) -> str:
    """在 TL;DR 标题下插入 action_state（level + instruction）。"""
    if not action_state:
        return md_text
    marker = "## ⏱️ 太长不看版 (TL;DR) — 今日决策指南"
    if marker not in md_text:
        return md_text
    lvl = action_state.get("level", action_state.get("Level", 0))
    label = action_state.get("label", "")
    instr = action_state.get("Action_Stance") or action_state.get("instruction", "")
    reason = action_state.get("Reason", "")
    cfs = action_state.get("canonical_final_state") or label
    block = f"\n> **主口径 (canonical final_state)**：`{cfs}` · **L{lvl}** — {instr}\n"
    block += f"> **状态机 label**: `{label}`\n"
    if reason:
        block += f"> *Reason: {reason}*\n"
    pos = md_text.find(marker)
    if pos == -1:
        return md_text
    after_title = pos + len(marker)
    # 插在章节标题后的第一个空行之后（即 headline 之前）
    if md_text[after_title:after_title + 1] == "\n":
        after_title += 1
    if md_text[after_title:after_title + 1] == "\n":
        after_title += 1
    return md_text[:after_title] + block + md_text[after_title:]


_TLDR_HEADING = "## ⏱️ 太长不看版 (TL;DR) — 今日决策指南"
_AI_INFERENCE_MARKER = "## 🧠 深度宏观推演 (AI Logic Inference)"


def _apply_dynamic_tldr_from_action_state(tldr_md: str, report_data: dict) -> str:
    """
    用 action_state + 高压断路器覆盖 TL;DR 的 headline 与「是否有大风险？」行，
    避免宏观分低但 L3/HV 已触发时仍显示「否，但有结构性隐患」。
    """
    if not (tldr_md and tldr_md.strip()):
        return tldr_md
    as_ = report_data.get("action_state") if isinstance(report_data.get("action_state"), dict) else {}
    try:
        action_level = int(as_.get("level", as_.get("Level", 0)))
    except (TypeError, ValueError):
        action_level = 0
    hv = (
        report_data.get("high_voltage_circuit_breaker")
        if isinstance(report_data.get("high_voltage_circuit_breaker"), dict)
        else {}
    )
    circuit_active = bool(hv.get("active"))

    if circuit_active or action_level >= 3:
        risk_line = (
            "- **是否有大风险？** ⚠️ **是。高压断路器已触发，系统检测到非线性风险聚集，"
            "建议立即转入最大防御模式（现金/极短久期）。**"
        )
        tldr_headline = "🔴 **高压断路器触发：系统性压力已确认，最大防御模式生效。**"
    elif action_level >= 2:
        risk_line = (
            "- **是否有大风险？** ⚠️ **有结构性风险。Regime 已切换（通胀/供给冲击），"
            "建议削减科技/成长敞口，增配硬资产/能源/现金。**"
        )
        tldr_headline = "🟠 **Regime 切换：通胀/供给冲击已激活，结构性风险显著上升。**"
    elif action_level >= 1:
        risk_line = (
            "- **是否有大风险？** 暂无系统性危机，但结构性隐患明显，建议停止加杠杆，累积现金缓冲。"
        )
        tldr_headline = "🟡 **结构性张力：宏观平稳但内部裂痕扩大，停止加杠杆。**"
    else:
        risk_line = "- **是否有大风险？** 否，系统整体健康，可维持正常仓位。"
        tldr_headline = "🟢 **系统健康：可维持正常仓位。**"

    def _head_repl(m) -> str:
        return m.group(1) + tldr_headline

    out = re.sub(
        r"(## ⏱️ 太长不看版 \(TL;DR\) — 今日决策指南\s*\n\s*\n)([^\n]+)",
        _head_repl,
        tldr_md,
        count=1,
        flags=re.MULTILINE,
    )
    out = re.sub(
        r"- \*\*是否有大风险？\*\*[^\n]*",
        risk_line,
        out,
        count=1,
    )
    return out


def _extract_tldr_section(md_text: str) -> Tuple[str, str]:
    """
    从正文剥离 TL;DR 块，供后处理置顶到「综合性结论」之后。
    返回 (去除 TL;DR 后的全文, TL;DR 章节含标题，若无则 "").
    """
    pat = re.compile(
        "\n" + re.escape(_TLDR_HEADING) + r"\s*\n([\s\S]*?)(?=\n## [^#]|\Z)"
    )
    m = pat.search(md_text)
    if not m:
        return md_text, ""
    block = md_text[m.start() + 1 : m.end()]  # drop leading \n from match for cleaner join
    new_md = md_text[: m.start()] + md_text[m.end() :]
    return new_md, block.rstrip()


def _ai_inference_bucket(chunk_first_line: str) -> str:
    s = chunk_first_line.strip()
    if "最后一句" in s or s.startswith("### ✅"):
        return "final"
    if "综合三指标" in s or "系统状态判断" in s:
        return "synth"
    if "附：" in s or "辅助验证" in s:
        return "appendix"
    mnum = re.search(r"### 🔹\s*(\d+)", s)
    if mnum:
        return f"n{mnum.group(1)}"
    return "misc"


def _trim_ai_final_closing_quote(chunk: str) -> str:
    """「最后一句」段只保留引用块为真正收束，去掉模型在引用后追加的总结段。"""
    if "### ✅" not in chunk or "最后一句" not in chunk:
        return chunk
    m = re.search(
        r"(### ✅[^\n]*\n\n(?:>[^\n]*(?:\n|$))+)",
        chunk,
        re.MULTILINE,
    )
    if not m:
        return chunk
    return chunk[: m.end()].rstrip()


def _reorder_ai_logic_inference_section(md_text: str) -> str:
    """
    深度宏观推演：固定小节顺序（核心三指标 → 综合判断 → 附录 → 最后一句收束），
    并以「给所有普通人的话」引用块为章节真正结尾。
    """
    if _AI_INFERENCE_MARKER not in md_text:
        return md_text
    start = md_text.index(_AI_INFERENCE_MARKER)
    prefix = md_text[:start]
    from_marker = md_text[start:]
    after_marker = from_marker[len(_AI_INFERENCE_MARKER) :]
    m_end = re.search(r"\n## [^#\s]", after_marker)
    if m_end:
        cut = len(_AI_INFERENCE_MARKER) + m_end.start()
        sec = from_marker[:cut]
        suffix = md_text[start + cut :]
    else:
        sec = from_marker
        suffix = ""

    inner = sec[len(_AI_INFERENCE_MARKER) :].lstrip("\n")
    split_m = re.search(r"(?m)^### (?:🔹|✅)", inner)
    if not split_m:
        return md_text
    preamble = inner[: split_m.start()]
    blocks_region = inner[split_m.start() :]
    heads = list(re.finditer(r"(?m)^### (?:🔹|✅).*$", blocks_region))
    if not heads:
        return md_text
    chunks: list = []
    for j, hm in enumerate(heads):
        a = hm.start()
        b = heads[j + 1].start() if j + 1 < len(heads) else len(blocks_region)
        chunks.append(blocks_region[a:b])

    buckets: Dict[str, list] = {
        "n1": [],
        "n2": [],
        "n3": [],
        "synth": [],
        "appendix": [],
        "final": [],
        "misc": [],
    }
    for ch in chunks:
        first_ln = ch.split("\n", 1)[0] if ch.strip() else ""
        b = _ai_inference_bucket(first_ln)
        if b == "final":
            buckets["final"].append(_trim_ai_final_closing_quote(ch))
        elif b == "synth":
            buckets["synth"].append(ch)
        elif b == "appendix":
            buckets["appendix"].append(ch)
        elif b in buckets:
            buckets[b].append(ch)
        else:
            buckets["misc"].append(ch)

    ordered = (
        buckets["n1"]
        + buckets["n2"]
        + buckets["n3"]
        + buckets["synth"]
        + buckets["appendix"]
        + buckets["misc"]
        + buckets["final"]
    )
    new_sec = (
        _AI_INFERENCE_MARKER
        + "\n\n"
        + preamble.rstrip()
        + "\n"
        + "".join(ordered).rstrip()
        + "\n"
    )
    return prefix + new_sec + suffix


def _build_executive_summary_section(
    executive_summary: dict, circuit_breaker_markdown: str = ""
) -> str:
    lines = [
        "## 综合性结论（Executive Verdict）",
        "",
    ]
    cb = (circuit_breaker_markdown or "").strip()
    if cb:
        lines.append(cb)
        lines.append("")
    lines.append(executive_summary.get("text", "").strip())
    lines.append("")
    return "\n".join(lines)


def _build_historical_validation_section() -> str:
    """
    报告最下方：历史指标判断准确记录，便于阅读者判断误差率。
    读取 event_x_validation 场景结果与去噪前后对比，fail-open。
    """
    base_dir = pathlib.Path(__file__).resolve().parent
    data_dir = base_dir / "data" / "event_x_validation"
    lines = [
        "## 📋 历史指标判断准确记录（Event-X 验证）",
        "",
        "以下为历史场景验证与去噪前后对比，供阅读者判断指标误差与稳定性。",
        "",
    ]
    try:
        results_path = data_dir / "event_x_validation_scenario_results.csv"
        if results_path.exists():
            df = pd.read_csv(results_path)
            lines.append("### 场景验证结果")
            lines.append("")
            lines.append("| 场景 | 验证状态 | 首次触发日 | 首次升级日 | 峰值等级 | 是否误报过多 |")
            lines.append("|------|----------|------------|------------|----------|--------------|")
            for _, row in df.iterrows():
                name = str(row.get("scenario_name", ""))[:28]
                status = str(row.get("验证状态", ""))
                first = str(row.get("首次触发日", ""))[:10] if pd.notna(row.get("首次触发日")) and str(row.get("首次触发日")) != "-" else "-"
                upgrade = str(row.get("首次升级日", ""))[:10] if pd.notna(row.get("首次升级日")) and str(row.get("首次升级日")) != "-" else "-"
                peak = str(row.get("峰值等级", ""))[:24]
                false_alarm = "是" if row.get("是否误报过多") is True else ("否" if row.get("是否误报过多") is False else "-")
                lines.append(f"| {name} | {status} | {first} | {upgrade} | {peak} | {false_alarm} |")
            lines.append("")
            tested = (df.get("验证状态") == "TESTED").sum()
            total = len(df)
            lines.append(f"*已测试场景: {tested}/{total}；未测试(NOT_TESTED)表示该窗口无历史数据。*")
            lines.append("")

        vol_summary = data_dir / "event_x_validation_denoising_before_after_vol_summary.csv"
        oil_summary = data_dir / "event_x_validation_denoising_before_after_oil_shock_summary.csv"
        if vol_summary.exists():
            v = pd.read_csv(vol_summary)
            lines.append("### 去噪前后对比（Vol 窗口 2021-09~11）")
            lines.append("")
            for _, row in v.iterrows():
                m = row.get("metric", "")
                b, a = row.get("before", ""), row.get("after", "")
                ch = row.get("变化", "") if "变化" in row else (int(a) - int(b) if str(a).isdigit() and str(b).isdigit() else "")
                lines.append(f"- **{m}**：去噪前 {b} 天 → 去噪后 {a} 天（变化 {ch}）")
            lines.append("")
        if oil_summary.exists():
            o = pd.read_csv(oil_summary)
            lines.append("### 去噪前后对比（Oil shock 窗口 2022-02~06）")
            lines.append("")
            for _, row in o.iterrows():
                m = row.get("metric", "")
                b, a = row.get("before", ""), row.get("after", "")
                lines.append(f"- **{m}**：去噪前 {b} 天 → 去噪后 {a} 天")
            lines.append("")

        lines.append("---")
        lines.append("*历史验证数据由 `scripts/run_event_x_daily_replay.py` 与 `data/event_x_validation/` 产出；更新报告前可重新运行以刷新上述结果。*")
        lines.append("")
    except Exception:
        lines.append("*（本次未加载到历史验证结果文件，请先运行 `py scripts/run_event_x_daily_replay.py` 生成。）*")
        lines.append("")
    return "\n".join(lines)


def generate_historical_analog_section(report_data: dict) -> str:
    """
    生成《历史情景对照与普通人理解指南》完整 Markdown。
    输入 report_data 含 structural_regime.modules、event_x_resonance、summary 等；
    缺数据时优雅降级，不抛异常、不阻断报告生成。
    输出：Part A 最相似情景 + Part B 历史参照表 + Part C 距离升级还差什么 + 普通人理解 3–5 条。
    """
    try:
        struct = report_data.get("structural_regime") or {}
        modules = struct.get("modules") or {}
        pc = modules.get("private_credit_liquidity_radar") or {}
        geo = modules.get("geopolitics_inflation_radar") or {}
        de_dollar = modules.get("de_dollarization") or {}
        k_shape = modules.get("k_shaped") or {}
        resonance = (report_data.get("event_x_resonance") or {}).get("level") or "OFF"
        summary = report_data.get("summary") or {}
        confirmation = summary.get("confirmation_signals") or {}
        credit_stress = (confirmation.get("credit_stress") or {}).get("on", False)

        report_signals = {
            "private_credit": (pc.get("alert") or "").upper() or None,
            "gold_regime": (geo.get("alert") or "").upper() or None,
            "geopolitics": (geo.get("alert") or "").upper() or None,
            "de_dollarization": (de_dollar.get("alert") or "").upper() or None,
            "k_shaped": (k_shape.get("alert") or "").upper() or None,
            "japan_contagion": (modules.get("japan_contagion") or {}).get("alert") or None,
            "resonance": str(resonance).upper(),
            "credit_watch": credit_stress,
            "vix_elevated": summary.get("fast_ew_alert") or False,
        }
        matches = historical_analogs_module.match_historical_analogs(report_signals)
        confidence = historical_analogs_module.get_confidence_from_matches(matches)
        has_full_signals = bool(
            report_data.get("structural_regime")
            and report_data.get("event_x_resonance")
            and (modules.get("private_credit_liquidity_radar") is not None or modules.get("geopolitics_inflation_radar") is not None)
        )
        if not has_full_signals:
            confidence = "LOW"
    except Exception:
        return ""

    lines = [
        "## 📚 历史情景对照：如果历史重演，普通人通常会先看到什么？",
        "",
        "> ⚠️ 以下内容为历史参照，不是预测。当前匹配只表示「更像哪类历史阶段」，不表示未来一定重演。",
        "",
    ]
    if not has_full_signals:
        lines.append("**⚠️ 部分信号缺失，本次为低置信度历史参照。**")
        lines.append("")
    lines.append(f"当前匹配置信度：**{confidence}**")
    lines.append("")

    # Part A：最相似历史情景
    if not matches:
        lines.append("未匹配到显著历史情景，当前更接近常态波动阶段。")
        lines.append("")
        a = (historical_analogs_module.HISTORICAL_ANALOGS.get("ALL_CLEAR_2019") or {})
    else:
        primary = matches[0]
        lines.append("**主要匹配情景**：**" + primary.get("label", "") + "**（匹配度 " + str(primary.get("match_score", 0)) + "，" + primary.get("exact_or_partial", "partial") + "）")
        lines.append("")
        if len(matches) > 1:
            sec = matches[1]
            lines.append("**次要参考情景**：" + sec.get("label", "") + "（匹配度 " + str(sec.get("match_score", 0)) + "）")
            lines.append("")
        a = primary.get("analog", {})

    # Part B：历史参照表（无匹配时用 ALL_CLEAR_2019；不写精确跌幅，用方向+时间窗+先后顺序）
    lines.append("**历史参照表**")
    lines.append("")
    lines.append("| 维度 | 历史上更常见的情况 | 这对普通人的意思 |")
    lines.append("|------|-------------------|------------------|")
    plain = (a.get("plain_english") or "").strip()
    time_win = (a.get("time_window_text") or "").strip()
    pattern = (a.get("common_market_pattern") or "").strip()
    escalation = (a.get("escalation_condition") or "").strip()
    lines.append("| 这次更像哪类阶段 | " + (a.get("label") or "") + " | " + (plain[:100] if plain else "—") + " |")
    lines.append("| 常见演化时间窗 | " + time_win + " | 历史上反应多为周至月，不是单日见分晓 |")
    lines.append("| 常见先受影响资产 | " + (pattern[:70] if pattern else "—") + " | 通常先波动的是风险偏好高的资产，再扩散 |")
    lines.append("| 更严重升级通常需要什么 | " + (escalation[:70] if escalation else "—") + " | 需多条风险腿同时恶化，单腿不足为据 |")
    lines.append("| 若历史重演，常见市场表现 | " + (pattern[:60] if pattern else "—") + " | " + (plain[:70] if plain else "—") + " |")
    lines.append("")

    # Part C：当前距离「更严重确认」还差什么
    lines.append("**当前距离「更严重确认」还差什么**")
    lines.append("")
    lines.append("| 信号 | 当前状态 | 还需要达到什么 | 历史上通常意味着什么 |")
    lines.append("|------|----------|----------------|------------------------|")
    pc_alert = (report_signals.get("private_credit") or "—")
    if (pc_alert or "") in ("", "WATCH"):
        pc_next = "HY OAS 明显走阔、金融压力继续上升"
    else:
        pc_next = "已处于观察/警戒，若再升级则风险扩散"
    lines.append("| Private Credit | " + (str(pc_alert)[:14] or "—") + " | " + pc_next + " | 若升级，未来数周风险资产更易波动放大 |")
    geo_alert = (report_signals.get("geopolitics") or report_signals.get("gold_regime") or "—")[:12]
    lines.append("| Geopolitics | " + geo_alert + " | 地缘/通胀链多腿同步确认 | 只有升级后才更接近系统性风险 |")
    res = (report_signals.get("resonance") or "OFF")[:12]
    res_next = "多条风险腿同步确认" if res == "OFF" else "共振持续或升级"
    lines.append("| Resonance | " + res + " | " + res_next + " | 只有升级后才更接近链式反应 |")
    lines.append("| Warsh / Liquidity | （见报告正文） | 流动性 plumbing 压力抬升 | 银行间与货币市场压力 |")
    overall = "早期压力" if (report_signals.get("private_credit") or report_signals.get("geopolitics")) else "偏平稳"
    lines.append("| Overall risk | " + overall + " | 多腿共振或 WATCH→ALERT | 连续升级比单日亮灯更重要 |")
    lines.append("")
    lines.append("## 🧭 普通人如何理解今天的风险")
    lines.append("")
    lines.append("- 信号亮了，不等于明天就崩盘；连续多日维持或升级，比单日闪现更重要。")
    lines.append("- 单腿预警更像天气变化，多腿共振才更像风暴形成。")
    lines.append("- 股票、债券、商品的反应速度不同，先波动的常是风险偏好高的资产。")
    lines.append("- 真正需要提高防守的，是 WATCH 变 ALERT、或 Resonance 升级，不是任何一次小亮灯。")
    lines.append("- 单日亮灯更像天气预报，连续升级更像季节变化。")
    lines.append("")
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
            pc_parts.append("高收益利差 5 日走阔 {:.0f}bp".format(float(details_pc["hy_oas_5d_bp_change"])))
        if details_pc.get("bizd_vs_50dma_pct") is not None:
            pc_parts.append("BIZD 弱势")
        if not pc_parts:
            pc_parts.append("代理压力")
        sentences.append(
            "私募信贷关注由 " + " 与 ".join(pc_parts) + " 支撑，绝对利差与 STLFSI4 水平仍属良性；反映早期代理压力，尚未确认系统性信用收紧。"
        )
    elif radar_a_status == "NORMAL":
        sentences.append("私募信贷利差保持良性。")

    # Geopolitics: 区分 breakeven-led watch / dual-leg alert / full-chain alarm
    if radar_b_status != "NORMAL":
        dual_leg = isinstance(details_geo, dict) and details_geo.get("dual_leg_confirmed")
        upgrade_blocked = isinstance(details_geo, dict) and details_geo.get("upgrade_blocked_by_single_leg_rule")
        if radar_b_status == "ALARM" and dual_leg:
            sentences.append("地缘全链警报：能源 + 通胀预期 + 恐慌已共同确认。")
        elif radar_b_status == "ALERT" and dual_leg:
            sentences.append("地缘双腿确认告警。")
        elif radar_b_status == "WATCH" and upgrade_blocked:
            sentences.append("地缘由通胀预期单腿触发关注（单腿不升级，需第二腿确认）。")
        elif completeness in ("LOW", "PARTIAL"):
            sentences.append(
                "地缘关注目前主要由 VIX 驱动；Brent 可用但未确认油价冲击，通胀预期输入仍过旧/不完整。"
            )
        else:
            sentences.append("地缘/通胀雷达处于 " + radar_b_status + "；Brent 与通胀预期腿有效。")
    else:
        sentences.append("地缘/通胀雷达正常。")

    if res_status == "OFF":
        sentences.append("未确认系统性共振。")
    conf = (confidence_result or {}).get("confidence", "MEDIUM")
    summary = " ".join(sentences) + " 信号置信度：" + conf + "。"
    if (freshness_result or {}).get("event_x_freshness_risk") == "HIGH":
        summary += " 新鲜度风险高（多项关键输入过旧）。"
    if isinstance(details_geo, dict) and details_geo.get("breakeven_is_stale"):
        summary += " 通胀预期输入过旧；地缘通胀链仅部分确认。"
    # 通俗说法（给非专业读者）
    plain = _build_event_x_plain_chinese(radar_a_status, radar_b_status, res_status, completeness)
    if plain:
        summary += " 通俗说法：" + plain
    return summary


def _build_event_x_plain_chinese(
    radar_a_status: str, radar_b_status: str, res_status: str, completeness: str,
) -> str:
    """给非专业读者的简短通俗说明。"""
    parts = []
    if radar_a_status != "NORMAL":
        parts.append("公开市场代理显示早期压力，但整体信用环境尚未进入危机模式。")
    if radar_b_status != "NORMAL":
        if completeness in ("LOW", "PARTIAL"):
            parts.append("市场紧张，但油价与通胀预期尚未共同确认更广泛的通胀冲击。")
        else:
            parts.append("地缘/通胀腿抬升；油价与通胀预期支撑当前关注。")
    if res_status == "OFF":
        if not parts:
            parts.append("暂无预警灯亮起。")
        else:
            parts.append("若干预警灯在闪，但尚未形成完整连锁反应。")
    if res_status != "OFF":
        parts.append("多条风险腿相互强化，需按系统性风险抬升对待。")
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
        "## 🔴 Event-X 优先风险",
        "",
        f"- **私募信贷/流动性雷达**: {radar_a_status}",
        f"- **地缘/通胀雷达**: {radar_b_status}",
        f"- **共振触发**: {res_status}",
        f"- **信号置信度**: {conf}",
        f"- **新鲜度风险**: {fresh_risk}",
    ]
    if comp:
        lines.append(f"- **地缘数据完整度**: {comp}")
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
                        by = f"，同比 {float(brent_yoy):.1f}%"
                    except (TypeError, ValueError):
                        pass
                parts.append(f"Brent {float(brent_last):.1f}{by}")
            elif "brent" in missing:
                parts.append("Brent：缺失/不可用")
            if breakeven_last is not None:
                bdate = details_geo.get("breakeven_last_date") or details_geo.get("t5yie_last_obs_date") or details_geo.get("breakeven_last_valid_date")
                source_label = {
                    "FVX_MINUS_DFII5": "realtime",
                    "FRED_T5YIE": "FRED",
                    "FVX_APPROX": "⚠️approx",
                    "FAILED": "❌unavailable",
                    "COMPUTED_DGS5_T5YIFR": "realtime",
                }.get(breakeven_source, "unknown")
                stale_note = "；过旧" if breakeven_stale else ""
                parts.append(f"通胀预期 {float(breakeven_last):.2f}% ({source_label})" + (f" 截至 {bdate}" if bdate else "") + stale_note)
            elif "breakeven" in missing or "t5yie" in missing:
                parts.append("通胀预期：缺失/过旧/不可用")
            if details_geo.get("vix_last") is not None:
                parts.append("VIX " + str(details_geo.get("vix_last")))
            if parts:
                lines.append("- **数据腿**: " + "; ".join(parts))
    lines.append("")
    lines.append("*机读摘要:*")
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


def export_investigo_signals(
    json_data: dict,
    action_state: Optional[dict],
    output_dir: pathlib.Path,
) -> Optional[pathlib.Path]:
    """
    下游 Investigo 标准 JSON 出口：高度精简、防御性 .get()；构建阶段任意异常仍写出最小合法 JSON。
    """
    path = output_dir / "investigo_signals.json"

    def _minimal_investigo_payload(err: Optional[str] = None) -> dict:
        d: dict = {
            "timestamp": pd.Timestamp.now(tz=base.JST).isoformat(),
            "source": "FRED_Crisis_Monitor_V2",
            "System_Command": {"Risk_Level": 1, "Action_Stance": "Unknown"},
            "Warsh_Factor": {
                "RRP_Drain_Active": False,
                "Plumbing_Stress": False,
                "Term_Premium_Spike": False,
            },
            "Regime_Rotation": {
                "Breadth_Status": "BREADTH_NEUTRAL",
                "Anti_Fiat_Active": False,
                "Halo_Tilt_Suggested": False,
                "Market_Implied_Inflation_Active": False,
                "War_Premium_Active": False,
                "Gold_Top_Warning": False,
            },
            "ValueFramework_Bridge": {
                "EM_Liquidity_Drain": False,
                "Offshore_Panic": False,
                "A_Share_Action_Suggested": "Normal",
            },
        }
        if err:
            d["_export_error"] = err
        return d

    out: dict = _minimal_investigo_payload()
    try:
        if not isinstance(json_data, dict):
            json_data = {}

        out = {
            "timestamp": pd.Timestamp.now(tz=base.JST).isoformat(),
            "source": "FRED_Crisis_Monitor_V2",
            "System_Command": {},
            "Warsh_Factor": {},
            "Regime_Rotation": {},
        }

        as_ = action_state if isinstance(action_state, dict) else {}
        try:
            risk_level = int(as_.get("Level", as_.get("level", 1)))
        except (TypeError, ValueError):
            risk_level = 1
        action_stance = as_.get("Action_Stance") or as_.get("instruction") or ""
        if not action_stance:
            action_stance = "Unknown"

        rc = json_data.get("risk_control") if isinstance(json_data.get("risk_control"), dict) else {}
        pm = rc.get("portfolio_mapping") if isinstance(rc.get("portfolio_mapping"), dict) else {}
        if rc.get("final_state_level") is not None:
            try:
                risk_level = int(rc["final_state_level"])
            except (TypeError, ValueError):
                pass
        # 与主口径一致：优先使用 canonical portfolio 策略文案
        if pm.get("strategy"):
            action_stance = str(pm["strategy"])

        out["System_Command"] = {
            "Risk_Level": risk_level,
            "Action_Stance": action_stance,
        }
        out["Risk_Control"] = {
            "Final_State": rc.get("final_state"),
            "Final_State_Level": rc.get("final_state_level", risk_level),
            "Final_Confidence": rc.get("final_confidence"),
            "State_Floor_Source": rc.get("state_floor_source"),
            "Breaker_Active": rc.get("breaker_active"),
            "Breaker_Reasons": list(rc.get("breaker_reasons") or []),
            "Portfolio_Strategy": pm.get("strategy"),
        }

        # --- Warsh_Factor ---
        rrp_drain = False
        try:
            rrp_s = base.fetch_series("RRPONTSYD")
            if rrp_s is not None and len(rrp_s.dropna()) >= 22:
                rrp_s = rrp_s.dropna()
                last = float(rrp_s.iloc[-1])
                prev20 = float(rrp_s.iloc[-21])
                declining = last < prev20
                below_1_trillion_bn = last < 1000.0
                rrp_drain = bool(below_1_trillion_bn and declining)
        except Exception:
            pass

        plumbing_stress = False
        try:
            spread_s = compose_series_v2("SOFR_MINUS_IORB")
            if spread_s is not None and len(spread_s.dropna()) >= 5:
                spread_s = spread_s.dropna()
                last5 = spread_s.iloc[-5:]
                plumbing_stress = bool(float(last5.mean()) > 0.0)
        except Exception:
            pass

        term_premium_spike = False
        try:
            tp_s = base.fetch_series("THREEFYTP10")
            if tp_s is not None and len(tp_s.dropna()) >= 10:
                tp_s = tp_s.dropna()
                cur = float(tp_s.iloc[-1])
                prev5_mean = float(tp_s.iloc[-6:-1].mean()) if len(tp_s) >= 6 else cur
                mom = cur - prev5_mean
                term_premium_spike = bool(mom >= 0.12 and cur >= 0.35)
        except Exception:
            pass

        ind_map = {
            i.get("series_id"): i
            for i in (json_data.get("indicators") or [])
            if i.get("series_id")
        }
        tp_item = ind_map.get("THREEFYTP10")
        if tp_item:
            try:
                cs = float(tp_item.get("change_score") or 0)
                rs = float(tp_item.get("risk_score") or tp_item.get("level_score") or 0)
                term_premium_spike = term_premium_spike or (cs >= 35.0 and rs >= 45.0)
            except (TypeError, ValueError):
                pass

        out["Warsh_Factor"] = {
            "RRP_Drain_Active": rrp_drain,
            "Plumbing_Stress": plumbing_stress,
            "Term_Premium_Spike": term_premium_spike,
        }

        # --- Regime_Rotation ---
        mi = (
            json_data.get("market_internals")
            if isinstance(json_data.get("market_internals"), dict)
            else {}
        )
        rs_block = mi.get("rsp_spy_breadth") or {}
        evt = mi.get("energy_vs_tech") or {}
        breadth_status = str(
            as_.get("rsp_spy_status")
            or rs_block.get("status")
            or "BREADTH_NEUTRAL"
        )

        reg_mod = (
            (json_data.get("regime") or {}).get("modules") or {}
            if isinstance(json_data.get("regime"), dict)
            else {}
        )
        gold_mod = reg_mod.get("gold_regime") if isinstance(reg_mod.get("gold_regime"), dict) else {}
        gold_status = str(gold_mod.get("status") or "").upper()
        anti_fiat_active = gold_status == "CRITICAL"

        breadth_bad = "DETERIORAT" in breadth_status.upper()
        try:
            xle_xlk_5d = float(as_.get("xle_xlk_momentum_5d_pct") or evt.get("pct_change_5d") or 0.0)
        except (TypeError, ValueError):
            xle_xlk_5d = 0.0
        energy_strong = (str(evt.get("status") or "").upper() == "ENERGY_DOMINANT") or (xle_xlk_5d > 0.0)

        inflation_high = False
        try:
            struct = json_data.get("structural_regime") or {}
            mods = struct.get("modules") or {}
            geo = mods.get("geopolitics_inflation_radar") or {}
            if isinstance(geo, dict):
                det = geo.get("details") or {}
                if isinstance(det, dict):
                    be = det.get("breakeven_effective_last", det.get("t5yie_pct"))
                    if be is not None:
                        inflation_high = float(be) >= 2.75
                al = str(geo.get("alert") or "").upper()
                if al in ("ALERT", "CRITICAL", "HIGH", "WATCH"):
                    inflation_high = inflation_high or (al in ("ALERT", "CRITICAL", "HIGH"))
        except Exception:
            pass

        pb_geo = mi.get("panel_b_geopolitics") or {}
        minf_b = bool((pb_geo.get("market_implied_inflation") or {}).get("sticky_inflation_priced_in"))
        war_b = bool((pb_geo.get("war_premium") or {}).get("war_premium_active"))
        gold_top_w = bool((pb_geo.get("gold_top_radar") or {}).get("gold_top_warning"))

        halo_tilt = bool(
            (breadth_bad and (energy_strong or inflation_high))
            or minf_b
            or war_b
        )

        out["Regime_Rotation"] = {
            "Breadth_Status": breadth_status,
            "Anti_Fiat_Active": anti_fiat_active,
            "Halo_Tilt_Suggested": halo_tilt,
            "Market_Implied_Inflation_Active": minf_b,
            "War_Premium_Active": war_b,
            "Gold_Top_Warning": gold_top_w,
        }

        # --- ValueFramework_Bridge（A 股 / 外资链；与全球美元流动性同源） ---
        pa = mi.get("panel_a_tail_and_fx") or {}
        fx_b = pa.get("offshore_cnh_dxy") or {}
        vv_b = pa.get("vol_of_vol") or {}
        hv_cb = (
            json_data.get("high_voltage_circuit_breaker")
            if isinstance(json_data.get("high_voltage_circuit_breaker"), dict)
            else {}
        )

        em_liquidity_drain = False
        try:
            cnh5 = fx_b.get("usdcnh_pct_change_5d")
            dxy5 = fx_b.get("dxy_pct_change_5d")
            if cnh5 is not None and dxy5 is not None:
                em_liquidity_drain = bool(float(cnh5) > 1.0 and float(cnh5) > float(dxy5))
        except (TypeError, ValueError):
            pass

        em_dgs10 = False
        dgi = ind_map.get("DGS10")
        try:
            if dgi:
                cv10 = float(dgi.get("current_value") or 0)
                cs10 = float(dgi.get("change_score") or 0)
                em_dgs10 = bool(cv10 >= 4.0 and cs10 >= 28.0)
        except (TypeError, ValueError):
            pass
        if not em_dgs10:
            try:
                ds10 = base.fetch_series("DGS10")
                if ds10 is not None and len(ds10.dropna()) >= 6:
                    ds10 = ds10.dropna()
                    last10 = float(ds10.iloc[-1])
                    w5_10 = float(ds10.iloc[-6])
                    em_dgs10 = bool(last10 >= 4.0 and (last10 - w5_10) >= 0.12)
            except Exception:
                pass

        em_liquidity_drain = bool(em_liquidity_drain or em_dgs10)

        offshore_panic = False
        try:
            offshore_panic = bool(hv_cb.get("swpt_trigger"))
        except Exception:
            offshore_panic = False
        try:
            vvx = vv_b.get("vvix_last")
            if vvx is not None and float(vvx) > 110.0:
                offshore_panic = True
        except (TypeError, ValueError):
            pass

        vf_action = "Normal"
        if em_liquidity_drain or offshore_panic:
            vf_action = "Reduce_Foreign_Heavy_Bluechips_Tilt_To_Domestic/Dividend"

        out["ValueFramework_Bridge"] = {
            "EM_Liquidity_Drain": em_liquidity_drain,
            "Offshore_Panic": offshore_panic,
            "A_Share_Action_Suggested": vf_action,
        }
    except Exception as e:
        logger.exception("export_investigo_signals 构建失败，已回退最小 JSON: %s", e)
        out = _minimal_investigo_payload(str(e))

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.exception("investigo_signals.json 写盘失败: %s", e)
        return None
    return path


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
    stale_thresholds = {"D": 7, "W": 10, "M": 60, "Q": 120}
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
                    if lag <= 7:
                        if lag > 1 and lag <= 3 and weekday in {0, 5, 6}:
                            stale_but_acceptable.append(series_id)
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
    # 注意：executive_summary 必须在 apply_high_voltage_circuit_breaker_to_report 之后生成（见下文）

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

    # 资产配置与 LLM 叙事在 canonical finalize 之后执行（保证 allocation / 叙事与主口径一致）

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

    try:
        _log_v2_stage("📈 获取市场微观结构 (Yahoo internals)...")
        _mi = fetch_market_internals()
        json_data["market_internals"] = _mi
    except Exception as _e:
        _log_v2_stage(f"⚠️ market_internals 获取失败: {_e}")
        json_data["market_internals"] = {"_fetch_error": str(_e)}

    json_data["high_voltage_circuit_breaker"] = eval_high_voltage_circuit_breaker(json_data)
    if (json_data.get("high_voltage_circuit_breaker") or {}).get("active"):
        _log_v2_stage("⚡ 高压断路器触发：覆盖慢变量稀释后的总分展示")
    apply_high_voltage_circuit_breaker_to_report(json_data, summary)

    json_data["action_state"] = compute_action_state(json_data)
    finalize_canonical_risk_control(json_data, summary)

    regime_alloc_legacy = AllocationRecommender.get_allocation_suggestion(regime_verdict or "NORMAL")
    _rc = json_data.get("risk_control") or {}
    _pm = _rc.get("portfolio_mapping") if isinstance(_rc, dict) else {}
    _pm = _pm if isinstance(_pm, dict) else {}
    json_data["allocation"] = {
        "source": "canonical_final_state",
        "canonical_final_state": _pm.get("final_state") or (_rc.get("final_state") if isinstance(_rc, dict) else None),
        "SPX": _pm.get("SPX"),
        "Gold": _pm.get("Gold"),
        "TLT": _pm.get("TLT"),
        "Cash_BIL": _pm.get("Cash_BIL"),
        "strategy": _pm.get("strategy"),
        "regime_verdict_appendix": regime_verdict,
        "regime_allocation_legacy": regime_alloc_legacy,
    }
    allocation_section = _build_unified_portfolio_stance_section(
        regime_verdict, json_data.get("risk_control") or {}
    )

    # LLM 叙事层：仅「完整报告」时调用（每日定时发送不调用以省 API）
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

    # 综合性结论：必须在断路器覆盖 early_warning_index 之后生成，且与 json_data["summary"] 对齐
    jd_sum = json_data.get("summary")
    if not isinstance(jd_sum, dict):
        jd_sum = {}
    _exec_keys = (
        "early_warning_index",
        "stress_now_index",
        "fast_ew_index",
        "slow_macro_deterioration_index",
        "fast_ew_alert",
        "slow_macro_watch",
        "credit_breadth",
        "breadth_early_warning",
        "confirmation_signals",
        "status_label",
        "data_freshness_confidence",
        "high_change_drivers",
        "systemic_risk_tier",
        "override_reason",
        "pillar_counts",
        "breadth_by_pillar",
        "group_weight_notes",
        "confirmation_state",
        "cash_is_king_alert",
        "anomaly_notes",
        "risk_control",
        "diagnostic_scores",
        "legacy_confirmation_status_label",
    )
    summary_for_exec = dict(summary)
    for _k in _exec_keys:
        if _k in jd_sum:
            summary_for_exec[_k] = jd_sum[_k]
            summary[_k] = jd_sum[_k]
    json_data["summary"]["high_change_drivers"] = summary.get("high_change_drivers", [])
    executive_summary = generate_executive_summary(
        summary_for_exec,
        profiles,
        risk_control=json_data.get("risk_control"),
    )
    json_data["executive_summary"] = executive_summary
    json_data["summary"]["executive_summary"] = executive_summary

    timestamp = json_data.get("timestamp")
    json_path = output_dir / f"crisis_report_{timestamp}.json"
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    _log_v2_stage("✅ JSON 写入完成")

    try:
        export_investigo_signals(json_data, json_data.get("action_state"), output_dir)
    except Exception as _inv_e:
        _log_v2_stage(f"⚠️ investigo_signals 导出跳过: {_inv_e}")

    _md_summary = {**summary, **(json_data.get("summary") or {})}
    section = _build_early_warning_section(_md_summary)
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
        circuit_breaker_md = check_circuit_breaker(json_data)
        _hv = json_data.get("high_voltage_circuit_breaker") or {}
        if _hv.get("active"):
            _hv_lines = [
                "🚨 **高压断路器（非线性覆盖）**：慢变量加权总分可能被低估；快变量/体制已触发强制抬升。",
                f"- `{_hv.get('override_reason') or HV_OVERRIDE_REASON}`",
                f"- 子条件：`{'`; `'.join(_hv.get('reasons') or [])}`",
            ]
            circuit_breaker_md = (
                (circuit_breaker_md + "\n\n" if circuit_breaker_md.strip() else "")
                + "\n".join(_hv_lines)
            )
        executive_section = _build_executive_summary_section(
            executive_summary, circuit_breaker_markdown=circuit_breaker_md
        )
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
        md_text, tldr_extracted = _extract_tldr_section(md_text)
        if tldr_extracted:
            tldr_extracted = _apply_dynamic_tldr_from_action_state(tldr_extracted, json_data)
        md_text = _reorder_ai_logic_inference_section(md_text)
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
            # 生成时间下一行起：先综合性结论，再 TL;DR → Portfolio → Event-X → 体制/市场/结构 → AI 简报
            lines.insert(insert_idx, "")
            lines.insert(insert_idx + 1, executive_section.rstrip())
            lines.insert(insert_idx + 2, "")
            pos = 3
            if tldr_extracted:
                lines.insert(insert_idx + pos, tldr_extracted)
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if allocation_section:
                lines.insert(insert_idx + pos, allocation_section.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if event_x_section:
                lines.insert(insert_idx + pos, event_x_section.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            if regime_dashboard_md:
                lines.insert(insert_idx + pos, regime_dashboard_md.rstrip())
                lines.insert(insert_idx + pos + 1, "")
                pos += 2
            market_internals_md = render_market_internals_md(json_data.get("market_internals") or {})
            if market_internals_md.strip():
                lines.insert(insert_idx + pos, market_internals_md.rstrip())
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
        historical_section = _build_historical_validation_section()
        if historical_section:
            md_text = md_text.rstrip() + "\n\n" + historical_section
        analog_section = generate_historical_analog_section(json_data)
        if analog_section:
            if "## ⚠️ 免责声明" in md_text:
                before, after = md_text.split("## ⚠️ 免责声明", 1)
                md_text = before.rstrip() + "\n\n" + analog_section + "\n\n## ⚠️ 免责声明" + after
            else:
                md_text = md_text.rstrip() + "\n\n" + analog_section
        md_text = _inject_action_state_into_tldr(md_text, json_data.get("action_state"))
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
    # 下载由 base.generate_report_with_images() 内 run_data_pipeline() 统一执行（--before-report，30 分钟超时），此处不再重复同步
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
