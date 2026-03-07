#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 数据新鲜度分层与信号可信度评估。
- 分层严重度：CRITICAL / IMPORTANT / INFO
- event_x_freshness_risk: LOW | MEDIUM | HIGH
- Signal confidence: HIGH | MEDIUM | LOW
所有逻辑 fail-open，输入缺失不中断。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Event-X 核心序列（直接影响雷达与 Resonance 判断）
EVENT_X_CRITICAL_IDS = frozenset({
    "BAMLH0A0HYM2",
    "STLFSI4",
    "DCOILBRENTEU",
    "T5YIE",
    "VIXCLS",
    "CPIENGSL",
    "DRTSCILM",
    "BIZD",  # patch，若存在
})

# 影响结构性判断但非 Event-X 核心闭环（如 GOLD/SPX 相关）
EVENT_X_IMPORTANT_IDS = frozenset({
    "GOLDAMGBD228NLBM",
    "GOLDPMGBD228NLBM",
    "SP500",
    "DGS10",
    "NFCI",
})

# 按频率的滞后天数阈值：(max_fresh, max_aging) -> 超过 max_aging 为 STALE
_FRESH_AGING_DAYS = {
    "D": (5, 14),    # 日频: 0-5 FRESH, 6-14 AGING, >14 STALE
    "W": (14, 35),   # 周频: 0-14 FRESH, 15-35 AGING, >35 STALE
    "M": (45, 75),   # 月频: 0-45 FRESH, 46-75 AGING, >75 STALE
    "Q": (120, 180), # 季频: 0-120 FRESH, 121-180 AGING, >180 STALE
}


def _freq_key(freq: str) -> str:
    if not freq:
        return "D"
    u = (freq or "D").upper()
    if u.startswith("D"):
        return "D"
    if u.startswith("W"):
        return "W"
    if u.startswith("M"):
        return "M"
    if u.startswith("Q"):
        return "Q"
    return "D"


def _lag_severity(lag_days: int, freq: str) -> str:
    """Return FRESH | AGING | STALE by lag and frequency. Fail-open: unknown freq -> D."""
    if lag_days < 0:
        return "FRESH"
    key = _freq_key(freq)
    max_fresh, max_aging = _FRESH_AGING_DAYS.get(key, (5, 14))
    if lag_days <= max_fresh:
        return "FRESH"
    if lag_days <= max_aging:
        return "AGING"
    return "STALE"


def evaluate_data_freshness_severity(
    json_data: Optional[Dict[str, Any]] = None,
    struct_results: Optional[Dict[str, Any]] = None,
    resonance_result: Optional[Dict[str, Any]] = None,
    indicators_config: Optional[Dict[str, Dict]] = None,
    reference_date: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    分层新鲜度严重度。不修改 Base 评分逻辑。
    返回: critical[], important[], info[], event_x_freshness_risk, summary
    任意输入为 None/空 时安全返回默认结构。
    """
    try:
        import pandas as pd
    except ImportError:
        pd = None
    today = reference_date
    if today is None and pd is not None:
        try:
            today = pd.Timestamp.now(tz="Asia/Tokyo").date()
        except Exception:
            today = pd.Timestamp.utcnow().date()
    if today is None:
        try:
            from datetime import date
            today = date.today()
        except Exception:
            today = None

    out = {
        "critical": [],
        "important": [],
        "info": [],
        "event_x_freshness_risk": "LOW",
        "summary": "Data freshness not evaluated (missing inputs).",
    }
    if not json_data or not today:
        return out

    indicators = json_data.get("indicators") or []
    id_to_cfg = dict(indicators_config or {})
    for item in indicators:
        sid = item.get("series_id")
        if sid and sid not in id_to_cfg:
            id_to_cfg[sid] = item
    critical_rows: List[Dict[str, Any]] = []
    important_rows: List[Dict[str, Any]] = []
    info_rows: List[Dict[str, Any]] = []

    for item in indicators:
        sid = item.get("series_id")
        if not sid:
            continue
        last_date = item.get("last_date")
        # 优先用 YAML 配置取 freq，其次用 indicator 项
        cfg = (indicators_config or {}).get(sid) or id_to_cfg.get(sid) or {}
        freq = "D"
        if isinstance(cfg, dict) and cfg.get("freq"):
            freq = _freq_key(str(cfg["freq"]))
        else:
            freq = _freq_key("D")
        lag_days = 9999
        if last_date:
            try:
                if pd is not None:
                    dt = pd.to_datetime(last_date).date()
                else:
                    from datetime import datetime
                    dt = datetime.strptime(str(last_date)[:10], "%Y-%m-%d").date()
                lag_days = (today - dt).days
            except Exception:
                pass
        severity = _lag_severity(lag_days, freq)
        row = {
            "series_id": sid,
            "last_date": last_date,
            "lag_days": lag_days,
            "freq": freq,
            "severity": severity,
        }
        if sid in EVENT_X_CRITICAL_IDS:
            critical_rows.append(row)
        elif sid in EVENT_X_IMPORTANT_IDS:
            important_rows.append(row)
        else:
            info_rows.append(row)

    # 未在 indicators 中出现的 CRITICAL/IMPORTANT 视为缺失（stale 对待）
    for sid in EVENT_X_CRITICAL_IDS:
        if not any(r["series_id"] == sid for r in critical_rows):
            critical_rows.append({
                "series_id": sid,
                "last_date": None,
                "lag_days": 9999,
                "freq": "W",
                "severity": "STALE",
            })
    for sid in EVENT_X_IMPORTANT_IDS:
        if not any(r["series_id"] == sid for r in important_rows):
            important_rows.append({
                "series_id": sid,
                "last_date": None,
                "lag_days": 9999,
                "freq": "D",
                "severity": "STALE",
            })

    critical_stale = [r["series_id"] for r in critical_rows if r["severity"] == "STALE"]
    critical_aging = [r["series_id"] for r in critical_rows if r["severity"] == "AGING"]
    # event_x_freshness_risk
    if len(critical_stale) >= 2:
        event_x_risk = "HIGH"
    elif len(critical_stale) >= 1:
        event_x_risk = "MEDIUM"
    else:
        event_x_risk = "LOW"

    summary_parts = []
    if critical_stale:
        summary_parts.append(f"Event-X 核心序列陈旧: {', '.join(sorted(critical_stale))}.")
    if struct_results and resonance_result:
        level = (resonance_result or {}).get("level") or "OFF"
        if level != "OFF" and critical_stale:
            summary_parts.append("Resonance 依赖的核心腿存在陈旧数据，结论可信度受影响.")
    if not summary_parts:
        summary_parts.append("Event-X 核心数据新鲜度可接受.")
    out["critical"] = critical_rows
    out["important"] = important_rows
    out["info"] = info_rows
    out["event_x_freshness_risk"] = event_x_risk
    out["summary"] = " ".join(summary_parts)
    return out


def evaluate_event_x_signal_confidence(
    struct_results: Optional[Dict[str, Any]] = None,
    resonance_result: Optional[Dict[str, Any]] = None,
    freshness_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    评估 Event-X 信号可信度。HIGH / MEDIUM / LOW。
    规则:
    - HIGH: 核心 CRITICAL 均为 FRESH/少量 AGING，无关键缺失，BIZD/Gold patch 有或缺失不影响闭环
    - MEDIUM: 1 个核心 STALE，或某 patch 缺失但 FRED 主腿完整，或使用了 fallback/最近有效值
    - LOW: 2+ 核心 STALE，或 Resonance 核心腿明显缺口，或多补丁缺失
    """
    out = {
        "confidence": "MEDIUM",
        "reasons": [],
        "summary": "Signal confidence not evaluated (missing inputs).",
    }
    if freshness_result is None:
        freshness_result = {}
    critical = freshness_result.get("critical") or []
    critical_stale = [r["series_id"] for r in critical if (r.get("severity") or "") == "STALE"]
    critical_aging = [r["series_id"] for r in critical if (r.get("severity") or "") == "AGING"]
    critical_fresh = [r["series_id"] for r in critical if (r.get("severity") or "") == "FRESH"]

    reasons: List[str] = []
    if len(critical_stale) >= 2:
        reasons.append("多个 Event-X 核心输入陈旧")
    if len(critical_stale) == 1:
        reasons.append(f"1 个核心输入陈旧: {critical_stale[0]}")
    if critical_aging and not critical_stale:
        reasons.append("部分核心数据为 AGING（沿用最近有效值）")
    if all(r.get("severity") == "FRESH" for r in critical if r.get("series_id") in ("BAMLH0A0HYM2", "STLFSI4", "VIXCLS", "T5YIE")):
        if not reasons:
            reasons.append("核心 FRED 主腿均为 FRESH")
    # Resonance 核心腿缺口
    if resonance_result:
        level = resonance_result.get("level") or "OFF"
        if level != "OFF" and critical_stale:
            reasons.append("Resonance 依赖序列存在陈旧，结论需谨慎解读")
    # BIZD / patch 缺失
    critical_ids = {r.get("series_id") for r in critical}
    if "BIZD" not in critical_ids and resonance_result:
        # BIZD 为 patch，缺失时可能用 fallback
        reasons.append("BIZD 补丁未参与或缺失（FRED 主腿完整则影响有限）")

    if len(critical_stale) >= 2 or (resonance_result and (resonance_result.get("level") or "OFF") != "OFF" and len(critical_stale) >= 1):
        confidence = "LOW"
    elif len(critical_stale) == 1 or critical_aging or "BIZD 补丁" in " ".join(reasons):
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"

    out["confidence"] = confidence
    out["reasons"] = reasons
    if confidence == "HIGH":
        out["summary"] = "Event-X 核心数据新鲜，判断可信度高。"
    elif confidence == "MEDIUM":
        out["summary"] = "部分输入陈旧或使用最近有效值，建议结合 Freshness Risk 解读。"
    else:
        out["summary"] = "多个核心输入陈旧，当前结论可信度较低，仅供趋势参考。"
    return out


def evaluate_geopolitics_data_completeness(
    details: Optional[Dict[str, Any]] = None,
    freshness_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    地缘模块核心链条「有效可用性」：Brent / Breakeven / VIX。
    有字段 ≠ 完整；有旧值 ≠ 可用。stale 的核心腿必须降低 completeness。
    HIGH: 三条腿都有效、无严重 stale
    PARTIAL: 三条腿中有 1 条 stale / weak / fallback quality low
    LOW: 只有 1 条腿有效，或只剩 VIX 在驱动
    """
    out = {
        "core_inputs_present": 0,
        "core_inputs_effective": 0,
        "completeness": "LOW",
        "summary": "Geopolitics data completeness not evaluated (missing details).",
        "missing_or_weak_legs": [],
    }
    if not details or not isinstance(details, dict):
        return out
    try:
        import pandas as pd
        brent_val = details.get("brent_last")
        breakeven_val = details.get("breakeven_effective_last") or details.get("t5yie_last")
        vix_val = details.get("vix_last")
        brent_present = pd.notna(brent_val)
        breakeven_present = pd.notna(breakeven_val)
        vix_present = pd.notna(vix_val)
    except Exception:
        brent_val = details.get("brent_last"); brent_present = brent_val is not None and str(brent_val) != "nan"
        breakeven_val = details.get("breakeven_effective_last") or details.get("t5yie_last"); breakeven_present = breakeven_val is not None and str(breakeven_val) != "nan"
        vix_val = details.get("vix_last"); vix_present = vix_val is not None and str(vix_val) != "nan"
    out["core_inputs_present"] = sum([brent_present, breakeven_present, vix_present])
    missing_weak = []
    # 有效 = 存在 且 非严重 stale
    brent_effective = brent_present
    breakeven_effective = breakeven_present and not details.get("breakeven_is_stale", True)
    if breakeven_present and details.get("breakeven_is_stale", True):
        missing_weak.append("breakeven_stale")
    if not breakeven_present:
        missing_weak.append("breakeven_missing")
    vix_effective = vix_present
    if not brent_present:
        missing_weak.append("brent_missing")
    if not vix_present:
        missing_weak.append("vix_missing")
    n_effective = sum([brent_effective, breakeven_effective, vix_effective])
    out["core_inputs_effective"] = n_effective
    out["missing_or_weak_legs"] = missing_weak
    if n_effective >= 3:
        out["completeness"] = "HIGH"
        out["summary"] = "Brent, breakeven, and VIX all effective (no material stale)."
    elif n_effective == 2:
        out["completeness"] = "PARTIAL"
        out["summary"] = "Two of Brent/breakeven/VIX effective; one leg stale or missing."
    else:
        out["completeness"] = "LOW"
        if vix_effective and not breakeven_effective and not brent_effective:
            out["summary"] = "Geopolitics watch is currently driven mainly by VIX; oil and breakeven inflation legs are only partially confirmed."
        elif vix_effective:
            out["summary"] = "Geopolitics watch driven mainly by VIX; oil and breakeven legs incomplete or stale."
        else:
            out["summary"] = "Only one or zero core inputs effective; oil/inflation legs incomplete."
    return out
