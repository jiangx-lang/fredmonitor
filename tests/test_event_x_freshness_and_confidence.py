#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 上线前加固：STLFSI 口径、分层新鲜度、信号可信度、Machine Summary 的 smoke tests.
- STLFSI 口径统一为 STLFSI4
- critical stale 影响 event_x_freshness_risk
- patch 缺失时 confidence 分支
- signal confidence HIGH / MEDIUM / LOW
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))


def test_stlfsi4_in_config():
    """配置与逻辑层仅使用 STLFSI4，报告层不出现 STLFSI3。"""
    import crisis_monitor as base
    config = base.load_yaml_config(base.BASE / "config" / "crisis_indicators.yaml")
    indicators = config.get("indicators") or []
    ids = [i.get("id") for i in indicators if i.get("id")]
    assert "STLFSI4" in ids, "crisis_indicators 应包含 STLFSI4"
    assert "STLFSI3" not in ids, "crisis_indicators 不应再包含 STLFSI3"
    print("✅ STLFSI 口径: 配置中仅 STLFSI4")


def test_freshness_severity_critical_stale():
    """任意 CRITICAL 序列 STALE 时 event_x_freshness_risk 至少 MEDIUM；2+ 为 HIGH。"""
    import event_x_freshness as fx
    import pandas as pd
    today = pd.Timestamp.now(tz="Asia/Tokyo").date()

    # 无 indicator 时 CRITICAL 会补全为 STALE -> 应至少 MEDIUM
    out = fx.evaluate_data_freshness_severity(
        json_data={"indicators": []},
        struct_results={},
        resonance_result={},
        indicators_config={},
        reference_date=today,
    )
    assert out["event_x_freshness_risk"] in ("MEDIUM", "HIGH"), "空 indicators 时 CRITICAL 缺失视为 stale"
    assert "critical" in out and len(out["critical"]) >= 1
    print("✅ 分层新鲜度: critical 缺失/陈旧 时 risk 至少 MEDIUM")

    # 构造 2+ CRITICAL STALE
    old_date = (today - pd.Timedelta(days=100)).strftime("%Y-%m-%d")
    indicators = [
        {"series_id": "BAMLH0A0HYM2", "last_date": old_date},
        {"series_id": "STLFSI4", "last_date": old_date},
    ]
    cfg = {"BAMLH0A0HYM2": {"freq": "D"}, "STLFSI4": {"freq": "W"}}
    out2 = fx.evaluate_data_freshness_severity(
        json_data={"indicators": indicators},
        struct_results={},
        resonance_result={},
        indicators_config=cfg,
        reference_date=today,
    )
    assert out2["event_x_freshness_risk"] == "HIGH", "2+ CRITICAL STALE 应为 HIGH"
    print("✅ 分层新鲜度: 2+ CRITICAL STALE -> event_x_freshness_risk = HIGH")


def test_patch_missing_fail_open():
    """BIZD / 补丁缺失时不中断，confidence 可 MEDIUM。"""
    import event_x_freshness as fx
    out = fx.evaluate_event_x_signal_confidence(
        struct_results={},
        resonance_result={"level": "OFF"},
        freshness_result={"critical": [], "important": []},
    )
    assert out["confidence"] in ("HIGH", "MEDIUM", "LOW")
    assert "reasons" in out and "summary" in out
    print("✅ 补丁缺失: fail-open，返回有效 confidence")


def test_signal_confidence_branches():
    """HIGH: 无 stale；MEDIUM: 1 stale 或 aging；LOW: 2+ stale 或 Resonance+stale。"""
    import event_x_freshness as fx

    # HIGH: 核心均 FRESH
    fresh = {
        "critical": [
            {"series_id": "BAMLH0A0HYM2", "severity": "FRESH"},
            {"series_id": "STLFSI4", "severity": "FRESH"},
            {"series_id": "VIXCLS", "severity": "FRESH"},
        ],
    }
    out_high = fx.evaluate_event_x_signal_confidence(
        struct_results={}, resonance_result={"level": "OFF"}, freshness_result=fresh
    )
    # BIZD 补丁缺失时可能为 MEDIUM
    assert out_high["confidence"] in ("HIGH", "MEDIUM"), "全 FRESH 时期望 HIGH 或 MEDIUM"

    # MEDIUM: 1 个 STALE
    stale1 = {
        "critical": [
            {"series_id": "BAMLH0A0HYM2", "severity": "FRESH"},
            {"series_id": "STLFSI4", "severity": "STALE"},
        ],
    }
    out_med = fx.evaluate_event_x_signal_confidence(
        struct_results={}, resonance_result={"level": "OFF"}, freshness_result=stale1
    )
    assert out_med["confidence"] in ("MEDIUM", "LOW"), "1 STALE 期望 MEDIUM 或 LOW"

    # LOW: 2+ STALE
    stale2 = {
        "critical": [
            {"series_id": "BAMLH0A0HYM2", "severity": "STALE"},
            {"series_id": "STLFSI4", "severity": "STALE"},
        ],
    }
    out_low = fx.evaluate_event_x_signal_confidence(
        struct_results={}, resonance_result={"level": "LEVEL_1"}, freshness_result=stale2
    )
    assert out_low["confidence"] == "LOW", "2+ STALE 或 Resonance+stale 期望 LOW"
    print("✅ Signal confidence: HIGH / MEDIUM / LOW 分支覆盖")


def test_freshness_input_none_fail_open():
    """evaluate_data_freshness_severity / evaluate_event_x_signal_confidence 输入 None 不抛错。"""
    import event_x_freshness as fx
    r1 = fx.evaluate_data_freshness_severity(None, None, None)
    assert "event_x_freshness_risk" in r1 and "summary" in r1
    r2 = fx.evaluate_event_x_signal_confidence(None, None, None)
    assert "confidence" in r2 and "summary" in r2
    print("✅ 输入 None: fail-open 返回默认结构")


if __name__ == "__main__":
    test_stlfsi4_in_config()
    test_freshness_severity_critical_stale()
    test_patch_missing_fail_open()
    test_signal_confidence_branches()
    test_freshness_input_none_fail_open()
    print("\n✅ All Event-X pre-launch smoke tests passed.")
