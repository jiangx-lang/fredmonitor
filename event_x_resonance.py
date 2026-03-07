#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Layer 3: Resonance Trigger Overlay (Event-X).
Does NOT enter any average score; only overrides system-level alert.
Returns OFF / Level 1 (Shock Watch) / Level 2 (Shock Alert) / RED ALERT.
"""
from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd


RESONANCE_OFF = "OFF"
RESONANCE_LEVEL_1 = "LEVEL_1"   # Shock Watch
RESONANCE_LEVEL_2 = "LEVEL_2"   # Shock Alert
RESONANCE_RED = "RED_ALERT"     # Systemic Red Alert


def _safe_float(v: Any, default: float = np.nan) -> float:
    if v is None:
        return default
    if isinstance(v, (int, float)) and not np.isnan(v):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def evaluate_resonance_triggers(data_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    判定非线性休克共振等级。不参与平均分，仅用于系统级警报覆盖。

    data_snapshot 应包含:
    - hy_oas_weekly_change_bp: float (HY OAS 周度变化 bp)
    - t5yie: float (5Y breakeven %)
    - brent: float (Brent 价格)
    - vix: float
    - bizd_vs_50dma_pct: float (BIZD vs 50DMA %, 负表示回撤)
    - stlfsi4: float
    - credit_stress_on: bool (系统原有 credit_stress 确认信号是否 ON)

    Returns:
        {
            "level": "OFF" | "LEVEL_1" | "LEVEL_2" | "RED_ALERT",
            "detail": { ... },
            "summary": str
        }
    """
    hy_bp = _safe_float(data_snapshot.get("hy_oas_weekly_change_bp"))
    t5yie = _safe_float(data_snapshot.get("t5yie"))
    brent = _safe_float(data_snapshot.get("brent"))
    vix = _safe_float(data_snapshot.get("vix"))
    bizd_pct = _safe_float(data_snapshot.get("bizd_vs_50dma_pct"))
    stlfsi4 = _safe_float(data_snapshot.get("stlfsi4"))
    credit_stress_on = bool(data_snapshot.get("credit_stress_on", False))

    detail: Dict[str, Any] = {
        "hy_oas_weekly_chg_bp": hy_bp,
        "t5yie": t5yie,
        "brent": brent,
        "vix": vix,
        "bizd_vs_50dma_pct": bizd_pct,
        "stlfsi4": stlfsi4,
        "credit_stress_on": credit_stress_on,
    }

    # Level 1 (Shock Watch): 任意 2 项；BIZD 必须 NaN 安全，不隐式转 0
    c1_hy = not pd.isna(hy_bp) and hy_bp > 30
    c1_t5 = not pd.isna(t5yie) and t5yie > 2.4
    c1_brent = not pd.isna(brent) and brent > 90
    c1_vix = not pd.isna(vix) and vix > 22
    c1_bizd = False if pd.isna(bizd_pct) else (bizd_pct < -7.0)
    level1_count = sum([c1_hy, c1_t5, c1_brent, c1_vix, c1_bizd])

    # Level 2 (Shock Alert): 任意 3 项
    c2_hy = not pd.isna(hy_bp) and hy_bp > 50
    c2_t5 = not pd.isna(t5yie) and t5yie > 2.5
    c2_brent = not pd.isna(brent) and brent > 95
    c2_vix = not pd.isna(vix) and vix > 25
    c2_bizd = False if pd.isna(bizd_pct) else (bizd_pct < -10.0)
    c2_stl = False if pd.isna(stlfsi4) else (stlfsi4 > 1.0)
    level2_count = sum([c2_hy, c2_t5, c2_brent, c2_vix, c2_bizd, c2_stl])

    # Level 3 (Systemic Red): 核心闭环；BIZD/STLFSI4 NaN 安全
    core_hy = not pd.isna(hy_bp) and hy_bp > 50
    core_t5 = not pd.isna(t5yie) and t5yie > 2.5
    core_bizd_or_stl = (False if pd.isna(bizd_pct) else (bizd_pct < -10.0)) or (False if pd.isna(stlfsi4) else (stlfsi4 > 1.0))
    red_ok = core_hy and core_t5 and core_bizd_or_stl and credit_stress_on

    detail["level1_count"] = level1_count
    detail["level2_count"] = level2_count
    detail["red_core_ok"] = red_ok

    level = RESONANCE_OFF
    summary = "Resonance trigger off; no multi-factor shock."

    if red_ok:
        level = RESONANCE_RED
        summary = "Credit + inflation resonance confirmed (RED ALERT). HY OAS spike, T5YIE elevated, and credit stress ON."
    elif level2_count >= 3:
        level = RESONANCE_LEVEL_2
        summary = f"Shock Alert: {level2_count} of 6 conditions met (HY/Brent/VIX/BIZD/STLFSI4)."
    elif level1_count >= 2:
        level = RESONANCE_LEVEL_1
        summary = f"Shock Watch: {level1_count} of 5 conditions met (HY/Brent/VIX/BIZD/T5YIE)."

    return {
        "level": level,
        "detail": detail,
        "summary": summary,
    }
