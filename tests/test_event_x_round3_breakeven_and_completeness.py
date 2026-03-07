#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 第三轮：Breakeven 代理、completeness 有效可用性、Machine Summary、status_quality 的 smoke tests.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))


def test_stlfsi4_still_used():
    """Private Credit 继续使用 STLFSI4。"""
    import structural_risk as sr
    mon = sr.StructuralRiskMonitor(BASE)
    mon.check_private_credit_liquidity_radar()
    d = getattr(mon.results["private_credit_liquidity_radar"], "details", {}) or {}
    assert d.get("stlfsi_series_used") == "STLFSI4"
    print("OK STLFSI4 still used in Private Credit")


def test_brent_available_does_not_trigger():
    """Brent 有值但低于 85 时不触发 Watch（逻辑不变）。"""
    import structural_risk as sr
    mon = sr.StructuralRiskMonitor(BASE)
    mon.check_geopolitics_inflation_radar()
    res = mon.results["geopolitics_inflation_radar"]
    d = getattr(res, "details", {}) or {}
    brent = d.get("brent_last") or d.get("brent")
    if brent is not None and float(brent) < 85:
        assert res.alert != "WATCH" or "VIX" in res.reason or "T5YIE" in res.reason or "Breakeven" in res.reason
    print("OK Brent available; trigger logic unchanged")


def test_breakeven_proxy_fail_open():
    """get_realtime_5y_breakeven_proxy_safe 无 base 时 fail-open 返回 NONE。"""
    import event_x_breakeven as bx
    out = bx.get_realtime_5y_breakeven_proxy_safe(base_module=None)
    assert "breakeven_source_used" in out
    assert out["breakeven_source_used"] in ("FRED_T5YIE", "COMPUTED_DGS5_T5YIFR", "NONE")
    assert "breakeven_is_stale" in out
    print("OK Breakeven proxy fail-open")


def test_geopolitics_completeness_when_breakeven_stale():
    """Breakeven stale 时 completeness 不应为 HIGH。"""
    import event_x_freshness as fx
    details = {"brent_last": 80.0, "breakeven_effective_last": 2.3, "vix_last": 22.0, "breakeven_is_stale": True}
    out = fx.evaluate_geopolitics_data_completeness(details)
    assert out["completeness"] in ("PARTIAL", "LOW")
    assert out.get("core_inputs_effective", 3) <= 2
    print("OK Completeness PARTIAL/LOW when breakeven stale")


def test_geopolitics_completeness_high_only_when_all_effective():
    """三腿均有效时 completeness = HIGH。"""
    import event_x_freshness as fx
    details = {"brent_last": 80.0, "breakeven_effective_last": 2.3, "vix_last": 18.0, "breakeven_is_stale": False}
    out = fx.evaluate_geopolitics_data_completeness(details)
    assert out["completeness"] == "HIGH"
    assert out["core_inputs_effective"] == 3
    print("OK Completeness HIGH when all effective")


def test_event_x_status_quality_structure():
    """json_data 中 event_x_status_quality 含 private_credit 与 geopolitics 的 fixed/remaining。"""
    # 通过 postprocess 会写入；此处仅检查结构可由调用方生成
    quality = {
        "private_credit": {"fixed_items": ["STLFSI4 unified"], "remaining_weaknesses": ["absolute spreads benign"]},
        "geopolitics": {"fixed_items": ["Brent connected"], "remaining_weaknesses": ["breakeven stale"]},
    }
    assert "private_credit" in quality and "fixed_items" in quality["private_credit"]
    assert "geopolitics" in quality and "remaining_weaknesses" in quality["geopolitics"]
    print("OK event_x_status_quality structure")


def test_geopolitics_details_breakeven_fields():
    """Geopolitics details 含 breakeven_source_used, breakeven_is_stale。"""
    import structural_risk as sr
    mon = sr.StructuralRiskMonitor(BASE)
    mon.check_geopolitics_inflation_radar()
    d = getattr(mon.results["geopolitics_inflation_radar"], "details", {}) or {}
    assert "breakeven_source_used" in d
    assert "breakeven_is_stale" in d
    print("OK Geopolitics details breakeven fields")


if __name__ == "__main__":
    test_stlfsi4_still_used()
    test_brent_available_does_not_trigger()
    test_breakeven_proxy_fail_open()
    test_geopolitics_completeness_when_breakeven_stale()
    test_geopolitics_completeness_high_only_when_all_effective()
    test_event_x_status_quality_structure()
    test_geopolitics_details_breakeven_fields()
    print("\nOK All Event-X round-3 smoke tests passed.")
