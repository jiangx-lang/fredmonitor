#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtest harness for tactical early warning profiles.
Uses historical run history (ew_profile_history.json) and crisis periods.
"""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta

import crisis_monitor as base
from crisis_monitor_v2 import SENSITIVITY_PROFILES


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


def _load_history(history_path: pathlib.Path) -> list:
    if not history_path.exists():
        return []
    try:
        data = json.loads(history_path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _load_crises() -> list:
    cfg = base.load_yaml_config(base.BASE / "config" / "crisis_periods.yaml")
    return cfg.get("crises", [])


def _compute_profile_alerts(history: list, profile: dict) -> list:
    alerts = []
    window = int(profile.get("fast_ew_window", 5))
    required = int(profile.get("fast_ew_required", 3))
    min_signals = int(profile.get("confirm_min_signals", 2))
    credit_thr = float(profile.get("credit_breadth_threshold", 0.2))
    real_thr = float(profile.get("real_breadth_threshold", 0.25))
    slow_thr = float(profile.get("slow_macro_watch_threshold", 50))

    for idx, entry in enumerate(history):
        signals = entry.get("confirmation_signals", {})
        recent = history[max(0, idx - window + 1): idx + 1]
        hits = sum(1 for e in recent if _count_on(e.get("confirmation_signals", {})) >= min_signals)
        fast_ew_alert = hits >= required
        credit_breadth = entry.get("breadth_by_pillar", {}).get("credit", 0.0)
        real_breadth = entry.get("breadth_by_pillar", {}).get("real", 0.0)
        slow_macro_index = float(entry.get("slow_macro_deterioration_index", 0) or 0)
        verdict = _profile_verdict(fast_ew_alert, credit_breadth, slow_macro_index, real_breadth,
                                   credit_thr, slow_thr, real_thr)
        date_str = entry.get("date")
        alerts.append({
            "date": date_str,
            "verdict": verdict,
            "fast_ew_alert": fast_ew_alert,
            "signals_on": _count_on(signals),
        })
    return alerts


def _calc_persistence(alerts: list) -> dict:
    streaks = []
    current = 0
    for a in alerts:
        if a["verdict"] != "All Clear":
            current += 1
        else:
            if current > 0:
                streaks.append(current)
            current = 0
    if current > 0:
        streaks.append(current)
    avg = sum(streaks) / len(streaks) if streaks else 0
    return {"streaks": streaks, "avg_streak": round(avg, 2), "max_streak": max(streaks) if streaks else 0}


def main():
    output_dir = base.BASE / "outputs" / "crisis_monitor"
    history_path = output_dir / "ew_profile_history.json"
    history = _load_history(history_path)
    if not history:
        print("❌ 未找到历史记录 ew_profile_history.json，无法回测。")
        return

    crises = _load_crises()
    profile = SENSITIVITY_PROFILES.get("base", {})
    alerts = _compute_profile_alerts(history, profile)

    alert_dates = {a["date"] for a in alerts if a["verdict"] != "All Clear"}
    alert_dt = {datetime.fromisoformat(d) for d in alert_dates if d}

    hits = []
    false_positives = 0
    lead_days = []

    crisis_windows = []
    for c in crises:
        start = datetime.fromisoformat(c["start"])
        end = datetime.fromisoformat(c["end"])
        crisis_windows.append((start - timedelta(days=90), start))
        window_alerts = [d for d in alert_dt if (start - timedelta(days=90)) <= d <= start]
        if window_alerts:
            first_alert = min(window_alerts)
            lead = (start - first_alert).days
            hits.append((c["name"], True, lead))
            lead_days.append(lead)
        else:
            hits.append((c["name"], False, None))

    for d in alert_dt:
        if not any(start <= d <= end for start, end in crisis_windows):
            false_positives += 1

    persistence = _calc_persistence(alerts)
    avg_lead = sum(lead_days) / len(lead_days) if lead_days else 0

    md_lines = [
        "# Tactical EW Backtest Summary",
        "",
        f"- Samples: {len(history)} runs",
        f"- Alerts (Base profile): {len(alert_dates)}",
        f"- Hit rate: {sum(1 for _, hit, _ in hits if hit)}/{len(hits)}",
        f"- Avg lead days: {avg_lead:.1f}",
        f"- False positives (not within 90d window): {false_positives}",
        f"- Alert persistence avg streak: {persistence['avg_streak']} (max {persistence['max_streak']})",
        "",
        "## Crisis Coverage",
    ]
    for name, hit, lead in hits:
        if hit:
            md_lines.append(f"- {name}: ✅ lead {lead} days")
        else:
            md_lines.append(f"- {name}: ❌ no alert within 90d")

    summary_path = output_dir / "backtest_tactical_summary.md"
    summary_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    print(f"✅ 回测摘要已生成: {summary_path}")


if __name__ == "__main__":
    main()
