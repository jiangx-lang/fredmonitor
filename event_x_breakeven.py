#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-X 5Y Breakeven 输入：FRED T5YIE 易滞后，提供可选的实时/计算代理。
不删除原 T5YIE；Geopolitics 优先使用更新更近的 breakeven 用于判断。
Fail-open：任一数据源失败不中断。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

# 日频数据：超过此天数视为 stale，尝试 fallback
STALE_DAYS_THRESHOLD = 7


def _last_value_and_date(ts) -> tuple:
    """Return (last_value, last_date_str)."""
    if ts is None or getattr(ts, "empty", True):
        return (None, None)
    try:
        import pandas as pd
        valid = ts.dropna()
        if valid.empty:
            return (None, None)
        last_val = float(valid.iloc[-1])
        idx = valid.index[-1]
        last_date = str(idx.date()) if hasattr(idx, "date") else str(idx)
        return (last_val, last_date)
    except Exception:
        return (None, None)


def _days_ago(last_date_str: Optional[str]) -> Optional[int]:
    """Return days since last_date_str, or None if invalid."""
    if not last_date_str:
        return None
    try:
        import pandas as pd
        from datetime import date
        dt = pd.to_datetime(last_date_str).date()
        today = pd.Timestamp.now(tz="Asia/Tokyo").date()
        return (today - dt).days
    except Exception:
        return None


def get_realtime_5y_breakeven_proxy_safe(base_module=None) -> Dict[str, Any]:
    """
    为 Geopolitics 提供 5Y breakeven 输入：优先 FRED T5YIE，若 stale 则尝试 DGS5 - T5YIFR 计算值。
    输出结构：
      breakeven_source_used: "FRED_T5YIE" | "COMPUTED_DGS5_T5YIFR" | "NONE"
      breakeven_last: float or None
      breakeven_last_date: str or None
      breakeven_is_stale: bool
      breakeven_quality: "HIGH" | "MEDIUM" | "LOW"
    """
    out = {
        "breakeven_source_used": "NONE",
        "breakeven_last": None,
        "breakeven_last_date": None,
        "breakeven_is_stale": True,
        "breakeven_quality": "LOW",
    }
    try:
        if base_module is None:
            import crisis_monitor as base_module
        base = base_module
    except Exception:
        return out

    # 1) FRED T5YIE
    t5yie = None
    try:
        t5yie = base.fetch_series("T5YIE")
    except Exception:
        pass
    t5yie_val, t5yie_date = _last_value_and_date(t5yie)
    t5yie_days = _days_ago(t5yie_date) if t5yie_date else None
    t5yie_fresh = t5yie_days is not None and t5yie_days <= STALE_DAYS_THRESHOLD

    if t5yie_val is not None and t5yie_fresh:
        out["breakeven_source_used"] = "FRED_T5YIE"
        out["breakeven_last"] = t5yie_val
        out["breakeven_last_date"] = t5yie_date
        out["breakeven_is_stale"] = False
        out["breakeven_quality"] = "HIGH"
        return out

    # 2) Fallback: DGS5 - T5YIFR (computed breakeven)
    dgs5 = None
    t5yifr = None
    try:
        dgs5 = base.fetch_series("DGS5")
        t5yifr = base.fetch_series("T5YIFR")
    except Exception:
        pass
    d5_val, d5_date = _last_value_and_date(dgs5)
    t5_val, t5_date = _last_value_and_date(t5yifr)
    if d5_val is not None and t5_val is not None:
        try:
            comp_val = float(d5_val) - float(t5_val)
            comp_date = d5_date if (_days_ago(d5_date) or 999) <= (_days_ago(t5_date) or 999) else t5_date
            comp_days = _days_ago(comp_date)
            comp_fresh = comp_days is not None and comp_days <= STALE_DAYS_THRESHOLD
            if comp_fresh:
                out["breakeven_source_used"] = "COMPUTED_DGS5_T5YIFR"
                out["breakeven_last"] = round(comp_val, 2)
                out["breakeven_last_date"] = comp_date
                out["breakeven_is_stale"] = False
                out["breakeven_quality"] = "HIGH" if comp_days is not None and comp_days <= 3 else "MEDIUM"
                return out
        except (TypeError, ValueError):
            pass

    # 3) 若计算不可用，仍可返回 FRED T5YIE 但标 stale
    if t5yie_val is not None:
        out["breakeven_source_used"] = "FRED_T5YIE"
        out["breakeven_last"] = t5yie_val
        out["breakeven_last_date"] = t5yie_date
        out["breakeven_is_stale"] = True
        out["breakeven_quality"] = "LOW"
    return out
