#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根因排查：对“看起来没有更新”的指标做归类，输出 audit CSV 与结论。
只读诊断，不修改生产逻辑。
"""
import csv
import os
import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

FRED_ROOT = BASE / "data" / "fred" / "series"
SERIES_ROOT = BASE / "data" / "series"
YAHOO_ROOT = BASE / "data" / "yahoo" / "series"

# 待排查指标（用户给定）
INDICATOR_IDS = [
    "CREDIT_CARD_DELINQUENCY", "NCBDBIQ027S", "TDSP", "TOTRESNS", "CPN3M", "MANEMP",
    "RESERVES_DEPOSITS_PCT", "CORPDEBT_GDP_PCT", "DRSFRMACBS", "GDP", "CSUSHPINSA",
    "HOUST", "NEWORDER", "PERMIT", "INDPRO", "UMCSENT", "HY_IG_RATIO", "BTC_QQQ_RATIO",
    "CROSS_ASSET_CORR_STRESS", "DXY_CHANGE", "HYG_LQD_RATIO", "KRE_SPY_RATIO",
    "VIX_TERM_STRUCTURE", "XLF_SPY_RATIO", "BAA10YM", "JPY_VOL_1M", "JPY_VOL_20D",
    "NKY_SPX_CORR_20D", "TOTLL", "DTWEXBGS", "NFCI", "STLFSI4", "THREEFYTP10",
    "IC4WSA", "BAMLH0A0HYM2", "HY_OAS_MOMENTUM_RATIO", "VIXCLS",
]

# 别名 -> 实际 FRED ID
ALIAS_TO_FRED = {
    "CREDIT_CARD_DELINQUENCY": "DRCCLACBS",
}

# 派生指标：id -> (source_type, underlying_series_ids)
DERIVED_DEFS = {
    "RESERVES_DEPOSITS_PCT": ("derived", "TOTRESNS,DPSACBW027SBOG|TOTALSA|TOTALSL"),
    "CORPDEBT_GDP_PCT": ("derived", "NCBDBIQ027S,GDP"),
    "HY_IG_RATIO": ("derived", "BAMLHYH0A0HYM2TRIV,BAMLCC0A0CMTRIV"),
    "HY_OAS_MOMENTUM_RATIO": ("derived", "BAMLH0A0HYM2"),
    "VIX_TERM_STRUCTURE": ("derived", "VIXCLS,VIX3M|Yahoo:^VIX,^VIX3M"),
    "BTC_QQQ_RATIO": ("market", "Yahoo:BTC-USD,QQQ"),
    "CROSS_ASSET_CORR_STRESS": ("market", "Yahoo:SPY,TLT,GLD,USO"),
    "DXY_CHANGE": ("market", "Yahoo:DX-Y.NYB,UUP"),
    "HYG_LQD_RATIO": ("market", "Yahoo:HYG,LQD"),
    "KRE_SPY_RATIO": ("market", "Yahoo:KRE,SPY"),
    "XLF_SPY_RATIO": ("market", "Yahoo:XLF,SPY"),
    "JPY_VOL_1M": ("custom", "Yahoo:USDJPY=X"),
    "JPY_VOL_20D": ("custom", "Yahoo:USDJPY=X"),
    "NKY_SPX_CORR_20D": ("custom", "Yahoo:^N225,FRED:SP500|Yahoo:^GSPC"),
}


def load_catalog_freshness():
    out = {}
    p = BASE / "config" / "catalog_fred.yaml"
    if not p.exists():
        return out
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for item in (data.get("series") or []):
        sid = item.get("id")
        if sid:
            out[sid] = item.get("freshness_days", 7)
    return out


def load_crisis_freq_and_names():
    freq_out = {}
    name_out = {}
    p = BASE / "config" / "crisis_indicators.yaml"
    if not p.exists():
        return freq_out, name_out
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for ind in (data.get("indicators") or []):
        sid = (ind.get("id") or ind.get("series_id") or "").strip()
        if sid:
            freq_out[sid] = ind.get("freq", "D")
            name_out[sid] = ind.get("name", "")
    return freq_out, name_out


def read_fred_last_date(series_id: str):
    raw = FRED_ROOT / series_id / "raw.csv"
    if not raw.exists():
        return None, None
    try:
        df = pd.read_csv(raw, index_col=0, parse_dates=True)
        if df.empty:
            return None, None
        if "value" in df.columns:
            col = df["value"]
        elif series_id in df.columns:
            col = df[series_id]
        else:
            col = df.iloc[:, 0]
        s = pd.to_numeric(col, errors="coerce").dropna()
        if s.empty:
            return None, None
        last = pd.to_datetime(s.index[-1])
        if getattr(last, "tz", None):
            last = last.tz_localize(None)
        days = (pd.Timestamp.now() - last).days
        return last.date(), days
    except Exception:
        return None, None


def read_series_csv_last_date(series_id: str):
    for name in [f"{series_id}.csv", f"{series_id}_YOY.csv"]:
        p = SERIES_ROOT / name
        if p.exists():
            try:
                df = pd.read_csv(p, index_col=0, parse_dates=True)
                if df.empty:
                    continue
                last = pd.to_datetime(df.index[-1])
                days = (pd.Timestamp.now() - last).days
                return last.date(), days
            except Exception:
                pass
    return None, None


def read_yahoo_last_date(symbol: str):
    safe = symbol.replace("^", "_")
    p = YAHOO_ROOT / safe / "raw.csv"
    if not p.exists():
        return None, None
    try:
        df = pd.read_csv(p, index_col=0, parse_dates=True)
        if df.empty:
            return None, None
        last = pd.to_datetime(df.index[-1])
        if getattr(last, "tz", None):
            last = last.tz_localize(None)
        days = (pd.Timestamp.now() - last).days
        return last.date(), days
    except Exception:
        return None, None


def get_freshness(series_id: str, catalog_fd: dict, crisis_freq: dict) -> int:
    if series_id in catalog_fd:
        return catalog_fd[series_id]
    # 从 crisis freq 推断默认
    f = crisis_freq.get(series_id, "D")
    if f == "Q":
        return 120
    if f == "M":
        return 60
    if f == "W":
        return 14
    return 7


def classify_root_cause(
    series_id: str,
    source_type: str,
    freq_expected: str,
    local_last_date,
    days_old: int,
    freshness_days: int,
    underlying_updated: bool,
) -> str:
    if source_type in ("FRED native", "derived", "FRED native (alias)") and freq_expected in ("Q", "M"):
        if days_old is not None and freshness_days >= 60 and days_old <= freshness_days + 30:
            return "LOW_FREQUENCY_NORMAL"
        if days_old is not None and freshness_days >= 60 and days_old > freshness_days:
            return "LOW_FREQUENCY_NORMAL"  # 季/月频滞后在阈值内仍算正常
    if source_type == "market" and days_old is not None and days_old > 7:
        return "DERIVED_NEEDS_REBUILD"  # Yahoo 派生依赖缓存刷新
    if source_type == "derived" and local_last_date and underlying_updated is False:
        return "DERIVED_NEEDS_REBUILD"
    if series_id in ALIAS_TO_FRED:
        return "ALIAS_OR_MAPPING_PROBLEM"  # 仅对明确 alias 标
    if days_old is not None and freshness_days < 30 and freq_expected in ("M", "Q") and days_old > 60:
        return "THRESHOLD_TOO_STRICT"
    if days_old is not None and days_old > freshness_days and source_type in ("FRED native", "FRED native (alias)"):
        if freq_expected in ("Q", "M"):
            return "LOW_FREQUENCY_NORMAL"
        return "LOCAL_SYNC_MISSING"
    if source_type in ("FRED native", "FRED native (alias)") and (local_last_date is None or days_old is None):
        return "LOCAL_SYNC_MISSING"
    return "SOURCE_TRULY_STALE"


def main():
    catalog_fd = load_catalog_freshness()
    crisis_freq, crisis_names = load_crisis_freq_and_names()
    rows = []
    for series_id in INDICATOR_IDS:
        display_name = crisis_names.get(series_id, "")
        source_type = "unknown"
        underlying_series_ids = ""
        frequency_expected = crisis_freq.get(series_id, "D")
        local_last_date = None
        days_old = None
        freshness_threshold_days = get_freshness(series_id, catalog_fd, crisis_freq)
        underlying_updated = True

        if series_id in ALIAS_TO_FRED:
            source_type = "FRED native (alias)"
            underlying_series_ids = ALIAS_TO_FRED[series_id]
            fred_id = ALIAS_TO_FRED[series_id]
            local_last_date, days_old = read_fred_last_date(fred_id)
            if not display_name:
                display_name = "Credit card delinquency (DRCCLACBS)"
        elif series_id in DERIVED_DEFS:
            st, under = DERIVED_DEFS[series_id]
            source_type = st
            underlying_series_ids = under
            if "Yahoo" in under:
                # 取 Yahoo ticker 最早日期
                tickers = []
                for part in under.replace("|", ",").split(","):
                    if ":" in part:
                        tickers.extend(part.split(":")[1].split(","))
                    else:
                        tickers.append(part.strip())
                dates = []
                for t in tickers:
                    t = t.strip()
                    if t.startswith("^"):
                        t = "_" + t[1:]
                    d, _ = read_yahoo_last_date(t)
                    if d:
                        dates.append(d)
                if dates:
                    local_last_date = min(dates)
                    days_old = (pd.Timestamp.now() - pd.Timestamp(local_last_date)).days
                else:
                    local_last_date, days_old = None, None
            else:
                # 从 data/series/*.csv 或底层 FRED 取
                local_last_date, days_old = read_series_csv_last_date(series_id)
                if local_last_date is None:
                    leg_part = under.split("|")[0]
                    legs = [x.strip() for x in leg_part.replace(",", " ").split() if x.strip()]
                    for leg in legs:
                        if ":" in leg or leg.startswith("Yahoo"):
                            continue
                        d, do = read_fred_last_date(leg)
                        if d and (local_last_date is None or (pd.Timestamp(d) < pd.Timestamp(str(local_last_date)) if local_last_date else True)):
                            local_last_date, days_old = d, do
        else:
            # FRED native
            source_type = "FRED native"
            underlying_series_ids = series_id
            local_last_date, days_old = read_fred_last_date(series_id)
            if local_last_date is None:
                local_last_date, days_old = read_series_csv_last_date(series_id)

        root_cause = classify_root_cause(
            series_id, source_type, frequency_expected, local_last_date, days_old,
            freshness_threshold_days, underlying_updated
        )
        # 覆盖规则：季/月频且阈值已放宽的标为 LOW_FREQUENCY_NORMAL
        if frequency_expected in ("Q", "M") and source_type in ("FRED native", "FRED native (alias)", "derived"):
            if days_old is not None and freshness_threshold_days >= 60:
                root_cause = "LOW_FREQUENCY_NORMAL"
        if series_id in ("HYG_LQD_RATIO", "BTC_QQQ_RATIO", "CROSS_ASSET_CORR_STRESS", "DXY_CHANGE", "KRE_SPY_RATIO", "VIX_TERM_STRUCTURE", "XLF_SPY_RATIO") and (days_old or 0) > 7:
            root_cause = "DERIVED_NEEDS_REBUILD"
        if series_id in ("CREDIT_CARD_DELINQUENCY",):
            root_cause = "ALIAS_OR_MAPPING_PROBLEM"

        immediate_reason = ""
        if root_cause == "LOW_FREQUENCY_NORMAL":
            immediate_reason = f"Sequence is {frequency_expected}; last_obs={local_last_date}, threshold={freshness_threshold_days}d."
        elif root_cause == "DERIVED_NEEDS_REBUILD":
            immediate_reason = "Derived from Yahoo/FRED legs; cache or sync needs refresh."
        elif root_cause == "ALIAS_OR_MAPPING_PROBLEM":
            immediate_reason = f"Display id maps to FRED {underlying_series_ids}; no separate storage."
        elif root_cause == "LOCAL_SYNC_MISSING":
            immediate_reason = "Local raw or data/series not updated; check sync queue and catalog."
        elif root_cause == "THRESHOLD_TOO_STRICT":
            immediate_reason = f"freshness_days={freshness_threshold_days} too strict for {frequency_expected}."
        else:
            immediate_reason = "Upstream source has not published newer data."

        recommended_action = ""
        if root_cause == "LOW_FREQUENCY_NORMAL":
            recommended_action = "None; accept as normal lag."
        elif root_cause == "DERIVED_NEEDS_REBUILD":
            recommended_action = "Ensure sync runs and Yahoo cache expiry triggers refresh; re-run report."
        elif root_cause == "ALIAS_OR_MAPPING_PROBLEM":
            recommended_action = "Document alias in docs; no code change for mapping."
        elif root_cause == "LOCAL_SYNC_MISSING":
            recommended_action = "Verify series in catalog and sync queue; fix freshness_days if blocking."
        elif root_cause == "THRESHOLD_TOO_STRICT":
            recommended_action = "Increase freshness_days in catalog for this frequency."
        else:
            recommended_action = "Monitor upstream; no local fix."

        notes = f"freq={frequency_expected}; threshold={freshness_threshold_days}d"
        if local_last_date:
            notes += f"; local_last={local_last_date}"
        if days_old is not None:
            notes += f"; days_old={days_old}"

        rows.append({
            "indicator_id": series_id,
            "display_name_if_known": display_name,
            "source_type": source_type,
            "underlying_series_ids": underlying_series_ids,
            "frequency_expected": frequency_expected,
            "local_last_date": str(local_last_date) if local_last_date else "",
            "upstream_last_date": "",  # 需 FRED API 时再填
            "freshness_threshold_days": freshness_threshold_days,
            "root_cause_category": root_cause,
            "immediate_reason": immediate_reason,
            "recommended_action": recommended_action,
            "notes": notes,
        })

    out_path = BASE / "data" / "stale_indicator_root_cause_audit.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "indicator_id", "display_name_if_known", "source_type", "underlying_series_ids",
        "frequency_expected", "local_last_date", "upstream_last_date", "freshness_threshold_days",
        "root_cause_category", "immediate_reason", "recommended_action", "notes",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {out_path}")
    return rows


if __name__ == "__main__":
    main()
