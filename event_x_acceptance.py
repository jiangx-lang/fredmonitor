#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 上线前验收：非回退检查、必选字段、stale 降级规则、smoke 就绪状态。
所有检查 fail-open，缺失字段不抛错，仅标记为未通过。
"""
from __future__ import annotations

from typing import Any, Dict, List


REQUIRED_TOP_KEYS = [
    "event_x_resonance",
    "event_x_freshness",
    "event_x_signal_confidence",
    "event_x_geopolitics_completeness",
    "event_x_status_quality",
]
REQUIRED_PC_DETAIL_KEYS = [
    "hy_oas_last", "hy_oas_5d_bp_change", "stlfsi4_last",
    "stlfsi_series_used", "used_inputs", "missing_inputs",
]
PC_ALTERNATIVE = {"bizd_drawdown_50dma": ["bizd_vs_50dma_pct", "bizd_drawdown_50dma"]}
REQUIRED_GEO_DETAIL_KEYS = [
    "brent_last", "breakeven_last", "breakeven_source_used", "breakeven_last_date",
    "breakeven_is_stale", "breakeven_quality", "vix_last", "used_inputs", "missing_inputs",
]
# 允许 breakeven_effective_last 替代 breakeven_last；brent_yoy 可选
GEO_ALTERNATIVE = {"breakeven_last": ["breakeven_effective_last", "t5yie_last"], "brent_yoy": ["brent_yoy_pct"]}


def _has_key(d: Any, key: str, alternatives: List[str] = None) -> bool:
    if d is None or not isinstance(d, dict):
        return False
    if key in d and d.get(key) is not None:
        return True
    for alt in (alternatives or []):
        if alt in d and d.get(alt) is not None:
            return True
    return False


def run_acceptance_checks(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行验收检查，返回 event_x_acceptance_status（结构化 checklist）。
    不修改 json_data；调用方负责写入。
    """
    out = {
        "non_regression": {},
        "required_fields": {},
        "stale_downgrade_rules": {},
        "smoke_tests_ready": True,
    }
    if not json_data or not isinstance(json_data, dict):
        out["required_fields"]["top_level"] = False
        return out

    # 1) Non-regression
    nr = out["non_regression"]
    nr["signal_confidence_present"] = "event_x_signal_confidence" in json_data
    nr["freshness_risk_present"] = bool(
        json_data.get("event_x_freshness") and "event_x_freshness_risk" in (json_data.get("event_x_freshness") or {})
    )
    comp = (json_data.get("event_x_geopolitics_completeness") or {})
    nr["completeness_effective_not_field_only"] = "core_inputs_effective" in comp and comp.get("completeness") in ("HIGH", "PARTIAL", "LOW")
    modules = (json_data.get("structural_regime") or {}).get("modules") or {}
    geo_d = modules.get("geopolitics_inflation_radar") or {}
    if isinstance(geo_d, dict):
        geo_details_flat = geo_d.get("details") or geo_d
    else:
        geo_details_flat = getattr(geo_d, "details", None) or {}
    nr["breakeven_source_used_exists"] = _has_key(geo_details_flat, "breakeven_source_used")
    nr["event_x_status_quality_exists"] = "event_x_status_quality" in json_data
    pc_detail = json_data.get("event_x_private_credit_detail") or {}
    nr["stlfsi_series_used_is_stlfsi4"] = (pc_detail.get("stlfsi_series_used") == "STLFSI4")

    # 2) Required fields
    rf = out["required_fields"]
    rf["top_level"] = all(k in json_data for k in REQUIRED_TOP_KEYS)
    pc_d = pc_detail if isinstance(pc_detail, dict) else {}
    rf["private_credit_details"] = all(_has_key(pc_d, k) for k in REQUIRED_PC_DETAIL_KEYS) and (
        _has_key(pc_d, "bizd_drawdown_50dma", PC_ALTERNATIVE.get("bizd_drawdown_50dma", []))
    )
    geo_det = geo_details_flat if isinstance(geo_details_flat, dict) else {}
    rf["geopolitics_details"] = all(
        _has_key(geo_det, k, GEO_ALTERNATIVE.get(k)) for k in REQUIRED_GEO_DETAIL_KEYS
    )

    # 3) Stale downgrade rules (heuristic)
    sd = out["stale_downgrade_rules"]
    breakeven_stale = geo_det.get("breakeven_is_stale", True)
    sd["breakeven_stale_then_completeness_not_high"] = (
        not breakeven_stale or comp.get("completeness") != "HIGH"
    )
    fresh = json_data.get("event_x_freshness") or {}
    critical_stale = [r for r in (fresh.get("critical") or []) if (r.get("severity") == "STALE")]
    sd["two_critical_stale_then_confidence_low_or_freshness_high"] = (
        len(critical_stale) < 2 or (json_data.get("event_x_signal_confidence") or {}).get("confidence") in ("LOW", "MEDIUM")
        or fresh.get("event_x_freshness_risk") in ("MEDIUM", "HIGH")
    )
    summary_text = (comp.get("summary") or "") + " " + (json_data.get("event_x_geopolitics_completeness") or {}).get("summary", "")
    sd["vix_led_when_partial_must_say"] = (
        comp.get("completeness") != "PARTIAL" and comp.get("completeness") != "LOW"
        or "VIX" in summary_text or "vix" in summary_text.lower()
    )

    return out


def run_maintainer_summary(json_data: Dict[str, Any], acceptance: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    生成 event_x_maintainer_summary：非回退、必选字段、stale 规则、smoke 就绪、历史验证就绪。
    """
    if acceptance is None:
        acceptance = run_acceptance_checks(json_data or {})
    nr = acceptance.get("non_regression", {})
    rf = acceptance.get("required_fields", {})
    sd = acceptance.get("stale_downgrade_rules", {})
    non_regression_passed = all(nr.values()) if nr else False
    required_fields_present = all(rf.values()) if rf else False
    stale_downgrade_rules_passed = all(sd.values()) if sd else False
    smoke_tests_ready = acceptance.get("smoke_tests_ready", True)
    # 历史验证「就绪」指脚本与文档存在，不在此执行
    historical_validation_ready = True
    try:
        from pathlib import Path
        base = Path(__file__).resolve().parent
        historical_validation_ready = (base / "scripts" / "run_event_x_historical_validation.py").exists() or (base / "event_x_validation.py").exists()
    except Exception:
        pass
    notes = []
    if not non_regression_passed:
        notes.append("Non-regression: one or more locked items missing or reverted.")
    if not required_fields_present:
        notes.append("Required fields: one or more Event-X structures/fields missing.")
    if not stale_downgrade_rules_passed:
        notes.append("Stale downgrade: rules may be violated (e.g. completeness HIGH while breakeven stale).")
    if not notes:
        notes.append("All acceptance checks passed.")
    return {
        "non_regression_passed": non_regression_passed,
        "required_fields_present": required_fields_present,
        "stale_downgrade_rules_passed": stale_downgrade_rules_passed,
        "smoke_tests_ready": smoke_tests_ready,
        "historical_validation_ready": historical_validation_ready,
        "notes": notes,
    }
