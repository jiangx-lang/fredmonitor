#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 第二轮：Brent/T5YIE 数据链、Geopolitics 完整性、Private Credit 动量 Watch、STLFSI4 的 smoke tests.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))


def test_brent_t5yie_in_sync_needed():
    """DCOILBRENTEU 与 T5YIE 在 sync 需要的列表中，便于落盘。"""
    from scripts.sync_fred_http import get_daily_factors_needed
    needed = get_daily_factors_needed()
    assert "DCOILBRENTEU" in needed, "DCOILBRENTEU 应在同步列表"
    assert "T5YIE" in needed, "T5YIE 应在同步列表"
    assert "STLFSI4" in needed, "STLFSI4 应在同步列表"
    print("OK Brent / T5YIE / STLFSI4 in sync needed list")


def test_geopolitics_completeness_low_when_only_vix():
    """仅 VIX 可用时 completeness = LOW，summary 含 VIX driven。"""
    import event_x_freshness as fx
    details = {"brent_last": None, "t5yie_last": None, "vix_last": 22.0}
    out = fx.evaluate_geopolitics_data_completeness(details)
    assert out["completeness"] == "LOW"
    assert "VIX" in out["summary"] or "oil" in out["summary"].lower()
    print("OK Geopolitics only VIX -> completeness LOW")


def test_geopolitics_completeness_high_when_three():
    """Brent / T5YIE / VIX 均有值时 completeness = HIGH。"""
    import event_x_freshness as fx
    details = {"brent_last": 80.0, "t5yie_last": 2.2, "vix_last": 18.0}
    out = fx.evaluate_geopolitics_data_completeness(details)
    assert out["completeness"] == "HIGH"
    assert out["core_inputs_available"] == 3
    print("OK Geopolitics 3 legs -> completeness HIGH")


def test_private_credit_hy_5d_momentum_watch():
    """Private Credit radar details 含 hy_oas_5d_bp_change、stlfsi_series_used=STLFSI4、used_inputs。"""
    import structural_risk as sr_mod
    mon = sr_mod.StructuralRiskMonitor(BASE)
    mon.check_private_credit_liquidity_radar()
    res = mon.results["private_credit_liquidity_radar"]
    d = getattr(res, "details", {}) or {}
    assert d.get("stlfsi_series_used") == "STLFSI4", "Private Credit 必须使用 STLFSI4"
    assert "hy_oas_5d_bp_change" in d
    assert "used_inputs" in d
    assert "missing_inputs" in d
    print("OK Private Credit details: stlfsi_series_used=STLFSI4, hy_oas_5d_bp_change")


def test_stlfsi4_only_in_crisis_indicators():
    """crisis_indicators 仅 STLFSI4，无 STLFSI3。"""
    import crisis_monitor as base
    config = base.load_yaml_config(base.BASE / "config" / "crisis_indicators.yaml")
    ids = [i.get("id") for i in (config.get("indicators") or []) if i.get("id")]
    assert "STLFSI4" in ids
    assert "STLFSI3" not in ids
    print("OK crisis_indicators STLFSI4 only")


def test_brent_t5yie_geopolitics_details_keys():
    """Geopolitics radar details 含 brent_last, t5yie_last, used_inputs, missing_inputs。"""
    import structural_risk as sr
    mon = sr.StructuralRiskMonitor(BASE)
    mon.check_geopolitics_inflation_radar()
    res = mon.results.get("geopolitics_inflation_radar")
    assert res is not None
    d = getattr(res, "details", {}) or {}
    assert "brent_last" in d
    assert "t5yie_last" in d
    assert "used_inputs" in d
    assert "missing_inputs" in d
    print("OK Geopolitics details: brent_last, t5yie_last, used_inputs, missing_inputs")


if __name__ == "__main__":
    test_brent_t5yie_in_sync_needed()
    test_geopolitics_completeness_low_when_only_vix()
    test_geopolitics_completeness_high_when_three()
    test_private_credit_hy_5d_momentum_watch()
    test_stlfsi4_only_in_crisis_indicators()
    test_brent_t5yie_geopolitics_details_keys()
    print("\nOK All Event-X round-2 smoke tests passed.")
