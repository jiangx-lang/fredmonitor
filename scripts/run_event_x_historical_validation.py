#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 历史场景验证：行为验证（是否太迟、太敏感、仅事后解释）。
不依赖回测收益，仅根据报告 JSON 中的 Event-X 字段与场景预期做对比。
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 项目根
BASE = Path(__file__).resolve().parent.parent

# 四类场景定义：date_range 为 ISO 区间或描述，expected_behavior 为人类可读预期
SCENARIOS = [
    {
        "scenario_name": "Oil shock / 油价冲击期",
        "scenario_id": "oil_shock",
        "date_range": "e.g. 2022-02-01 / 2022-06-30",
        "expected_behavior": "Geopolitics Radar 在油价与通胀预期抬升期间或之前进入 WATCH，非仅事后才 HIGH。",
        "checks": ["geopolitics_reacts", "not_only_after_peak"],
    },
    {
        "scenario_name": "Credit widening / 信用利差快速走阔期",
        "scenario_id": "credit_widening",
        "date_range": "e.g. HY OAS 快速拉大窗口",
        "expected_behavior": "Private Credit 在绝对阈值触发前可因 HY 动量或 BIZD 弱势先亮 WATCH。",
        "checks": ["private_credit_watch_can_precede_threshold", "momentum_or_bizd_used"],
    },
    {
        "scenario_name": "Volatility rise without full resonance / 波动上升但未共振",
        "scenario_id": "vol_rise_no_resonance",
        "date_range": "VIX 上行但 Brent/breakeven/credit 未同步确认",
        "expected_behavior": "仅 Geopolitics WATCH 或 PARTIAL，不误判 RED ALERT；completeness 非 HIGH；summary 显式 VIX-led。",
        "checks": ["no_false_resonance", "completeness_not_high_when_vix_led", "vix_led_mentioned"],
    },
    {
        "scenario_name": "True resonance / 真实共振压力期",
        "scenario_id": "true_resonance",
        "date_range": "信用+通胀预期+流动性恶化+credit_stress=ON",
        "expected_behavior": "Resonance 升级（非 OFF）；至少一雷达 WATCH/ALERT。",
        "checks": ["resonance_elevated", "at_least_one_radar_watch_or_alert"],
    },
]


def _parse_report_date_from_json(data: Dict[str, Any]) -> Optional[datetime]:
    """从报告 JSON 中解析报告日期。"""
    ts = data.get("timestamp")
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(ts))
        except Exception:
            return None
    if isinstance(ts, str):
        for fmt in ("%Y%m%d_%H%M%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(ts[:19], fmt)
            except Exception:
                continue
    return None


def _get_event_x_from_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """从报告 JSON 提取 Event-X 相关字段，缺则返回空 dict。"""
    return {
        "resonance_level": (data.get("event_x_resonance") or {}).get("level", "OFF"),
        "event_x_freshness": data.get("event_x_freshness") or {},
        "event_x_signal_confidence": data.get("event_x_signal_confidence") or {},
        "event_x_geopolitics_completeness": data.get("event_x_geopolitics_completeness") or {},
        "structural_regime": data.get("structural_regime") or {},
    }


def _get_radar_status(data: Dict[str, Any], radar_key: str) -> str:
    """从 structural_regime.modules 取雷达状态。"""
    modules = (data.get("structural_regime") or {}).get("modules") or {}
    m = modules.get(radar_key) or {}
    if isinstance(m, dict):
        return (m.get("alert") or "NONE").upper()
    return getattr(m, "alert", "NONE") or "NONE"


def evaluate_scenario(
    scenario: Dict[str, Any],
    report_data: Optional[Dict[str, Any]] = None,
    report_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    对单份报告评估单场景。无报告时返回 skipped。
    返回结构：scenario_name, date_range, expected_behavior, observed_behavior,
             too_late, too_sensitive, only_explains_after, summary.
    """
    out = {
        "scenario_name": scenario.get("scenario_name", ""),
        "scenario_id": scenario.get("scenario_id", ""),
        "date_range": scenario.get("date_range", ""),
        "expected_behavior": scenario.get("expected_behavior", ""),
        "observed_behavior": "",
        "too_late": False,
        "too_sensitive": False,
        "only_explains_after": False,
        "summary": "",
    }
    if not report_data:
        out["observed_behavior"] = "（无报告数据）"
        out["summary"] = "SKIP: No report provided for this scenario."
        return out

    ex = _get_event_x_from_report(report_data)
    pc_status = _get_radar_status(report_data, "private_credit_liquidity_radar")
    geo_status = _get_radar_status(report_data, "geopolitics_inflation_radar")
    res_level = (ex.get("resonance_level") or "OFF").upper()
    comp = (ex.get("event_x_geopolitics_completeness") or {}).get("completeness", "LOW")
    comp_summary = (ex.get("event_x_geopolitics_completeness") or {}).get("summary", "")

    observed_parts = [
        f"Private Credit: {pc_status}; Geopolitics: {geo_status}; Resonance: {res_level}; Completeness: {comp}.",
    ]
    if comp_summary:
        observed_parts.append(comp_summary[:200])
    out["observed_behavior"] = " ".join(observed_parts)

    sid = scenario.get("scenario_id", "")
    # 简单启发式：根据场景类型与当前报告状态给出 too_late / too_sensitive / only_explains_after
    if sid == "oil_shock":
        if geo_status in ("WATCH", "ALERT", "ALARM"):
            out["summary"] = "PASS (heuristic): Geopolitics 有反应；需结合具体日期判断是否提前。"
        else:
            out["only_explains_after"] = True
            out["summary"] = "FAIL (heuristic): Geopolitics 未亮灯；若该窗口为油价冲击期则可能太迟或仅事后解释。"
    elif sid == "credit_widening":
        if pc_status in ("WATCH", "ALERT", "ALARM"):
            out["summary"] = "PASS (heuristic): Private Credit 有 WATCH/ALERT；需结合动量/BIZD 是否参与判断。"
        else:
            out["summary"] = "NEUTRAL: Private Credit 未亮灯；若该窗口为利差快速走阔期则可能太迟。"
    elif sid == "vol_rise_no_resonance":
        if res_level != "OFF" and comp in ("LOW", "PARTIAL"):
            out["too_sensitive"] = True
            out["summary"] = "FAIL (heuristic): Resonance 亮而 completeness 低，可能误判共振。"
        elif geo_status in ("WATCH", "ALERT") and ("VIX" in comp_summary or "vix" in comp_summary.lower()):
            out["summary"] = "PASS (heuristic): VIX-led 且未误判为共振。"
        else:
            out["summary"] = "NEUTRAL: 需结合 VIX 与 Brent/breakeven 数据判断。"
    elif sid == "true_resonance":
        if res_level != "OFF" and (pc_status not in ("NONE", "") or geo_status not in ("NONE", "")):
            out["summary"] = "PASS (heuristic): Resonance 升级且至少一雷达亮灯。"
        elif res_level == "OFF":
            out["too_late"] = True
            out["summary"] = "FAIL (heuristic): 若为真实共振期则 Resonance 未升级，可能太迟。"
        else:
            out["summary"] = "NEUTRAL: 需结合多日报告判断。"
    else:
        out["summary"] = "Scenario not evaluated (unknown scenario_id)."

    return out


def load_reports_from_dir(path: Path) -> List[Tuple[datetime, Dict[str, Any]]]:
    """从目录加载所有 crisis_report_*.json，返回 (日期, 数据) 列表。"""
    out = []
    for f in path.glob("crisis_report_*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            dt = _parse_report_date_from_json(data)
            if dt:
                out.append((dt, data))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def run_validation(
    json_path: Optional[Path] = None,
    json_dir: Optional[Path] = None,
    scenario_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    运行验证：若 json_dir 提供则按日期选报告对每场景评估；若仅 json_path 则用该报告评估所有（或指定）场景。
    """
    reports: List[Tuple[datetime, Dict[str, Any]]] = []
    if json_path and json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            dt = _parse_report_date_from_json(data)
            reports = [(dt or datetime.min, data)]
        except Exception:
            pass
    if json_dir and json_dir.exists():
        reports = load_reports_from_dir(json_dir)
    if not reports:
        return [
            {
                **evaluate_scenario(s, None, None),
                "note": "No report loaded; run with --json or --dir.",
            }
            for s in SCENARIOS
            if not scenario_ids or s.get("scenario_id") in scenario_ids
        ]

    results = []
    # 用最新一份报告评估（若多份可按日期选落在场景窗口内的，此处简化为最新）
    report_date, report_data = reports[-1]
    for s in SCENARIOS:
        if scenario_ids and s.get("scenario_id") not in scenario_ids:
            continue
        results.append(evaluate_scenario(s, report_data, report_date))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Event-X historical scenario validation")
    parser.add_argument("--json", type=Path, default=None, help="Single report JSON path")
    parser.add_argument("--dir", type=Path, default=None, help="Directory of crisis_report_*.json")
    parser.add_argument("--scenario", type=str, action="append", default=None, help="Limit to scenario_id (e.g. oil_shock)")
    parser.add_argument("--out", type=Path, default=None, help="Write results to JSON file")
    args = parser.parse_args()
    if not args.dir and not args.json:
        args.dir = BASE / "outputs" / "crisis_monitor"
    scenario_ids = args.scenario if args.scenario else None
    results = run_validation(json_path=args.json, json_dir=args.dir, scenario_ids=scenario_ids)
    out_json = [r for r in results]
    print(json.dumps(out_json, ensure_ascii=False, indent=2))
    if args.out:
        args.out.write_text(json.dumps(out_json, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
