#!/usr/bin/env python3
"""
从 crisis 配置与派生逻辑推导「crisis 依赖的 FRED 序列」清单，
并生成同步覆盖率审计（crisis_required_fred_series.csv / crisis_fred_sync_coverage.csv）。
不修改 Base/Event-X/评分，仅做扫描与输出。
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent
CATALOG_PATH = BASE / "config" / "catalog_fred.yaml"
CRISIS_INDICATORS_PATH = BASE / "config" / "crisis_indicators.yaml"
DERIVED_DEPS_PATH = BASE / "config" / "derived_fred_deps.yaml"
OUT_REQUIRED = BASE / "data" / "crisis_required_fred_series.csv"
OUT_COVERAGE = BASE / "data" / "crisis_fred_sync_coverage.csv"


def _load_derived_fred_deps() -> dict:
    """与 sync_fred_http 一致：从 config/derived_fred_deps.yaml 读取，失败则 fallback。"""
    fallback = {
        "CP_MINUS_DTB3": ["CPN3M", "DTB3"],
        "SOFR20DMA_MINUS_DTB3": ["SOFR", "DTB3"],
        "CORPDEBT_GDP_PCT": ["NCBDBIQ027S", "GDP"],
        "RESERVES_ASSETS_PCT": ["TOTRESNS", "WALCL"],
        "RESERVES_DEPOSITS_PCT": ["TOTRESNS", "TOTALSA"],
        "UST30Y_UST2Y_RSI": ["DGS30", "DGS2"],
        "HY_OAS_MOMENTUM_RATIO": ["BAMLH0A0HYM2"],
        "SP500_DGS10_CORR60D": ["SP500", "DGS10"],
        "NET_LIQUIDITY": ["WALCL", "WTREGEN", "RRPONTSYD"],
        "VIX_TERM_STRUCTURE": ["VIXCLS", "VIX3M"],
        "HY_IG_RATIO": ["BAMLHYH0A0HYM2TRIV", "BAMLCC0A0CMTRIV"],
        "GLOBAL_LIQUIDITY_USD": ["WALCL", "DEXUSEU", "DEXJPUS"],
        "CREDIT_CARD_DELINQUENCY": ["DRCCLACBS"],
        "US_JPY_10Y_SPREAD": ["DGS10", "IRLTLT01JPM156N"],
        "MOVE_PROXY": ["VIXCLS", "DGS10"],
        "SOFR_MINUS_IORB": ["SOFR", "IORB"],
        "RRP_DRAIN_RATE": ["RRPONTSYD"],
    }
    if DERIVED_DEPS_PATH.exists():
        try:
            with open(DERIVED_DEPS_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            raw = data.get("derived_indicators") or {}
            return {k: (v if isinstance(v, list) else list(v)) for k, v in raw.items()}
        except Exception:
            pass
    return fallback


DERIVED_FRED_DEPS = _load_derived_fred_deps()

# 仅 Yahoo/非 FRED 的 indicator id（不进入 catalog）
NON_FRED_INDICATOR_IDS = {
    "HYG_LQD_RATIO", "DXY_CHANGE", "KRE_SPY_RATIO", "XLF_SPY_RATIO",
    "BTC_QQQ_RATIO", "CROSS_ASSET_CORR_STRESS",
    "JPY_VOL_20D", "JPY_VOL_1M", "US_JPY_10Y_1W_CHG", "NKY_SPX_CORR_20D", "JGB_ETF_8282",
}


def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_crisis_indicator_ids_and_freq() -> list[tuple[str, str]]:
    """(series_id, freq) 来自 crisis_indicators.yaml"""
    data = load_yaml(CRISIS_INDICATORS_PATH)
    indicators = data.get("indicators") or []
    out = []
    for ind in indicators:
        sid = (ind.get("id") or ind.get("series_id") or "").strip()
        if not sid:
            continue
        freq = (ind.get("freq") or "D").upper()
        out.append((sid, freq))
    return out


def collect_fred_native_required() -> tuple[set[str], set[str]]:
    """
    返回 (fred_native_required, all_required_ids).
    fred_native_required = 仅 FRED 原生、应进入 catalog 的序列（含派生依赖的原始序列）。
    all_required_ids = 含派生指标 id，用于 required 表；派生 id 不要求进 catalog。
    """
    data = load_yaml(CRISIS_INDICATORS_PATH)
    indicators = data.get("indicators") or []
    derived_ids = set(DERIVED_FRED_DEPS.keys())
    ids_from_config = set()
    for ind in indicators:
        sid = (ind.get("id") or ind.get("series_id") or "").strip().upper()
        if sid and sid not in NON_FRED_INDICATOR_IDS:
            ids_from_config.add(sid)
    # 派生指标依赖的 FRED 原始序列
    for _derived, deps in DERIVED_FRED_DEPS.items():
        for s in deps:
            ids_from_config.add(s.upper())
    # FRED 原生 = 非派生、非纯 Yahoo 的 indicator id + 所有派生依赖里的原始序列
    fred_native = set()
    for sid in ids_from_config:
        if sid in derived_ids or sid in NON_FRED_INDICATOR_IDS:
            continue
        fred_native.add(sid)
    for _derived, deps in DERIVED_FRED_DEPS.items():
        for s in deps:
            fred_native.add(s.upper())
    return fred_native, ids_from_config


def get_catalog_series_ids() -> set[str]:
    ids = set()
    data = load_yaml(CATALOG_PATH)
    for ent in data.get("series") or []:
        sid = (ent.get("id") or "").strip()
        if sid:
            ids.add(sid)
    return ids


def get_current_needed_list() -> list[str]:
    """当前 sync_fred_http 使用的硬编码 needed list（不修改脚本，仅读取逻辑复现）。"""
    daily_factors = {
        "VIXCLS", "T10Y2Y", "BAMLH0A0HYM2", "DTWEXBGS", "BAMLHE00EHYIEY",
        "HOUST", "NFCI", "TEDRATE", "UMCSENT",
    }
    yoy_indicators = [
        "PAYEMS", "INDPRO", "GDP", "NEWORDER", "CSUSHPINSA",
        "TOTALSA", "TOTLL", "MANEMP", "WALCL", "DTWEXBGS",
        "PERMIT", "TOTRESNS",
    ]
    special = ["NCBDBIQ027S"]
    event_x_radar = ["DCOILBRENTEU", "T5YIE", "STLFSI4", "CPIENGSL", "GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM", "DRTSCILM"]
    needed = set()
    needed.update(daily_factors)
    needed.update(yoy_indicators)
    needed.update(special)
    needed.update(event_x_radar)
    return list(needed)


def main():
    os.makedirs(BASE / "data", exist_ok=True)
    os.makedirs(BASE / "docs", exist_ok=True)

    crisis_ids_freq = get_crisis_indicator_ids_and_freq()
    id_to_freq = {sid: freq for sid, freq in crisis_ids_freq}
    fred_native_required, all_required_ids = collect_fred_native_required()
    catalog_ids = get_catalog_series_ids()
    needed_list = get_current_needed_list()
    needed_set = set(needed_list)
    derived_ids = set(DERIVED_FRED_DEPS.keys())
    non_native = NON_FRED_INDICATOR_IDS | derived_ids

    rows_required = []
    for sid in sorted(all_required_ids):
        is_fred_native = sid in fred_native_required
        in_catalog = sid in catalog_ids
        in_needed = sid in needed_set
        used_as_indicator = id_to_freq.get(sid) is not None and sid not in non_native
        used_as_dep = any(sid in deps for deps in DERIVED_FRED_DEPS.values())
        should_sync = is_fred_native and (in_catalog or (used_as_indicator or used_as_dep))
        if is_fred_native and not in_catalog:
            should_sync = True
        source_location = "crisis_indicators"
        if sid in derived_ids:
            source_location = "derived_indicator_not_fred"
        elif used_as_dep and not used_as_indicator:
            source_location = "derived_dependency"
        used_by = "crisis_report"
        if used_as_dep and sid not in derived_ids:
            used_by = "compose_series_or_derived"
        frequency = id_to_freq.get(sid) or ""

        rows_required.append({
            "series_id": sid,
            "source_location": source_location,
            "used_by_module": used_by,
            "frequency_if_known": frequency,
            "in_catalog": str(in_catalog),
            "in_needed_list": str(in_needed),
            "should_sync": str(should_sync) if is_fred_native else "N/A_derived",
        })

    with open(OUT_REQUIRED, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["series_id", "source_location", "used_by_module", "frequency_if_known", "in_catalog", "in_needed_list", "should_sync"])
        w.writeheader()
        w.writerows(rows_required)
    print(f"Wrote {OUT_REQUIRED} ({len(rows_required)} rows)")

    # Coverage: 仅对 FRED 原生 required 做 OK/MISSING_*；派生 id 标 DERIVED_NOT_APPLICABLE
    sync_queue_ids = catalog_ids & needed_set
    rows_coverage = []
    for sid in sorted(all_required_ids):
        is_fred_native = sid in fred_native_required
        required = is_fred_native
        in_catalog = sid in catalog_ids
        in_sync_queue = sid in sync_queue_ids
        if sid in derived_ids or sid in NON_FRED_INDICATOR_IDS:
            status = "DERIVED_NOT_APPLICABLE"
        elif not in_catalog:
            status = "MISSING_IN_CATALOG"
        elif not in_sync_queue:
            status = "MISSING_IN_SYNC_QUEUE"
        else:
            status = "OK"
        rows_coverage.append({
            "series_id": sid,
            "required_by_crisis": str(required),
            "present_in_catalog": str(in_catalog),
            "present_in_sync_queue": str(in_sync_queue),
            "status": status,
        })

    with open(OUT_COVERAGE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["series_id", "required_by_crisis", "present_in_catalog", "present_in_sync_queue", "status"])
        w.writeheader()
        w.writerows(rows_coverage)
    print(f"Wrote {OUT_COVERAGE} ({len(rows_coverage)} rows)")

    missing_catalog = [r["series_id"] for r in rows_coverage if r["status"] == "MISSING_IN_CATALOG"]
    missing_queue = [r["series_id"] for r in rows_coverage if r["status"] == "MISSING_IN_SYNC_QUEUE"]
    print(f"Crisis FRED native required: {len(fred_native_required)}; in catalog: {len(catalog_ids)}; in sync queue (current): {len(sync_queue_ids)}")
    print(f"Missing in catalog: {len(missing_catalog)} -> {missing_catalog[:20]}{'...' if len(missing_catalog) > 20 else ''}")
    print(f"Missing in sync queue: {len(missing_queue)} -> {missing_queue[:20]}{'...' if len(missing_queue) > 20 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
