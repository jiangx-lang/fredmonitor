#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 防御性检查：缺 BIZD / GLD 仍生成区块且 Resonance 不误触；RED_ALERT 条件必能打出。
"""
from __future__ import annotations

import sys
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

# 项目根加入 path
sys.path.insert(0, str(__file__).rsplit("tests", 1)[0])

import structural_risk as structural_module
import event_x_resonance as event_x_resonance_module
import crisis_monitor as base


# ---------- Check 1: BIZD 全空 + GLD fallback 失败，仍能生成 Event-X 区块且 Resonance 不误触 ----------
def test_event_x_with_no_bizd_and_no_gold_fallback():
    """BIZD 全空、GLD fallback 失败时：雷达可跑、Event-X 区块可构建、Resonance 为 OFF。"""
    with patch.object(base, "fetch_bizd_safe", return_value=pd.Series(dtype=float)):
        with patch.object(base, "get_gold_spx_rolling_corr_20d", return_value=(np.nan, "GLD", {"reason": "missing"})):
            mon = structural_module.StructuralRiskMonitor()
            mon.run_all_checks()

    # 两个 Event-X 雷达应存在且不因缺 BIZD/GLD 抛错
    assert "private_credit_liquidity_radar" in mon.results
    assert "geopolitics_inflation_radar" in mon.results
    pc = mon.results["private_credit_liquidity_radar"]
    geo = mon.results["geopolitics_inflation_radar"]

    # BIZD 为 NaN 时不应因比较误触
    assert pc.details.get("bizd_vs_50dma_pct") is None or (isinstance(pc.details["bizd_vs_50dma_pct"], float) and np.isnan(pc.details["bizd_vs_50dma_pct"]))
    # 共振输入：BIZD 空、credit_stress 关 -> 应为 OFF
    snapshot = {
        "hy_oas_weekly_change_bp": pc.details.get("hy_oas_weekly_chg_bp"),
        "t5yie": geo.details.get("t5yie_pct"),
        "brent": geo.details.get("brent"),
        "vix": geo.details.get("vix"),
        "bizd_vs_50dma_pct": pc.details.get("bizd_vs_50dma_pct"),
        "stlfsi4": pc.details.get("stlfsi4"),
        "credit_stress_on": False,
    }
    res = event_x_resonance_module.evaluate_resonance_triggers(snapshot)
    assert res["level"] == "OFF", "Resonance 应在缺 BIZD 且 credit_stress 关时为 OFF"

    # 可构建 Event-X 区块（与 _build_event_x_priority_risks_section 同构逻辑，不抛错）
    pc_alert = pc.alert
    geo_alert = geo.alert
    radar_a_status = "NORMAL" if pc_alert == "NONE" else pc_alert
    radar_b_status = "NORMAL" if geo_alert == "NONE" else geo_alert
    res_status = res.get("level", "OFF")
    line1 = f"Private Credit is {radar_a_status}; Geopolitics/Inflation is {radar_b_status}; Systemic Resonance: {res_status}."
    assert res_status == "OFF"
    assert "NONE" in (pc_alert, geo_alert) or "WATCH" in (pc_alert, geo_alert) or "ALERT" in (pc_alert, geo_alert) or "ALARM" in (pc_alert, geo_alert)


# ---------- Check 3: 人工构造 RED_ALERT 假数据，确保能打出 RED_ALERT ----------
def test_red_alert_mock_snapshot():
    """HY 周升>50bp, T5YIE>2.5, (BIZD<-10 或 STLFSI4>1), credit_stress_on=True -> 必为 RED_ALERT。"""
    snapshot = {
        "hy_oas_weekly_change_bp": 60,
        "t5yie": 2.6,
        "brent": 100,
        "vix": 30,
        "bizd_vs_50dma_pct": -12,
        "stlfsi4": 1.2,
        "credit_stress_on": True,
    }
    res = event_x_resonance_module.evaluate_resonance_triggers(snapshot)
    assert res["level"] == "RED_ALERT", f"预期 RED_ALERT，得到 {res['level']}"

    # 仅 BIZD 满足、STLFSI4 不满足也应 RED_ALERT（core_bizd_or_stl 为 True）
    snapshot2 = {
        "hy_oas_weekly_change_bp": 55,
        "t5yie": 2.55,
        "brent": 80,
        "vix": 18,
        "bizd_vs_50dma_pct": -11,
        "stlfsi4": 0.5,
        "credit_stress_on": True,
    }
    res2 = event_x_resonance_module.evaluate_resonance_triggers(snapshot2)
    assert res2["level"] == "RED_ALERT", f"预期 RED_ALERT (BIZD path)，得到 {res2['level']}"

    # 仅 STLFSI4 满足、BIZD 为 NaN 也应 RED_ALERT
    snapshot3 = {
        "hy_oas_weekly_change_bp": 52,
        "t5yie": 2.52,
        "brent": 80,
        "vix": 18,
        "bizd_vs_50dma_pct": np.nan,
        "stlfsi4": 1.5,
        "credit_stress_on": True,
    }
    res3 = event_x_resonance_module.evaluate_resonance_triggers(snapshot3)
    assert res3["level"] == "RED_ALERT", f"预期 RED_ALERT (STLFSI4 path)，得到 {res3['level']}"


if __name__ == "__main__":
    print("Check 1: BIZD 全空 + GLD 失败 -> Event-X 可生成、Resonance OFF")
    test_event_x_with_no_bizd_and_no_gold_fallback()
    print("  OK")
    print("Check 3: Mock RED_ALERT snapshot")
    test_red_alert_mock_snapshot()
    print("  OK")
    print("All Event-X checks passed.")
