#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐日重放 Event-X 规则：用 features 表按日应用 Private Credit / Geopolitics / Resonance 规则，
填实占位列，再按 scenarios 窗口聚合产出：首次触发日、首次升级日、峰值等级、提前/滞后天数、是否误报过多。

不扩数据字段；只做历史行为验证。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data" / "event_x_validation"


# ----- 规则：与 structural_risk / event_x_resonance / event_x_freshness 对齐 -----

def _safe(x: Any, default: float = np.nan) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def replay_private_credit(
    hy_oas: float, stlfsi4: float, bizd_vs_50dma_pct: float,
    hy_oas_5d_bp_change: float,
) -> Tuple[str, bool, bool]:
    """
    NONE / WATCH / ALERT / ALARM。
    返回 (alert_level, watch_flag, alert_flag)。
    """
    hy = _safe(hy_oas)
    stl = _safe(stlfsi4)
    bizd = _safe(bizd_vs_50dma_pct)
    hy5 = _safe(hy_oas_5d_bp_change)

    alert = "NONE"
    # Alarm: 至少 2 项 (无 DRTSCILM 时用 HY/STLFSI4/BIZD)
    alarm_count = 0
    if not np.isnan(hy) and hy > 6.0:
        alarm_count += 1
    if not np.isnan(stl) and stl > 1.5:
        alarm_count += 1
    if not np.isnan(bizd) and bizd < -10.0:
        alarm_count += 1
    if alarm_count >= 2:
        alert = "ALARM"

    if alert == "NONE":
        if (not np.isnan(hy) and hy > 5.0) or (not np.isnan(hy5) and hy5 > 50):
            alert = "ALERT"
        if not np.isnan(stl) and stl > 1.0:
            alert = "ALERT"
        if not np.isnan(bizd) and bizd < -10.0:
            alert = "ALERT"

    if alert == "NONE":
        if not np.isnan(hy) and hy > 4.5:
            alert = "WATCH"
        if not np.isnan(stl) and stl > 0:
            alert = "WATCH"
        if not np.isnan(bizd) and bizd < -5.0:
            alert = "WATCH"
        if not np.isnan(hy5) and hy5 > 30:
            alert = "WATCH"

    watch = alert in ("WATCH", "ALERT", "ALARM")
    alert_flag = alert in ("ALERT", "ALARM")
    return alert, watch, alert_flag


def _geo_conditions(
    brent: float, brent_yoy_pct: float, breakeven_effective: float, vix: float,
) -> Tuple[bool, bool, bool]:
    """双腿确认用三条件。NaN 不参与比较。"""
    br = _safe(brent); byoy = _safe(brent_yoy_pct); be = _safe(breakeven_effective); v = _safe(vix)
    energy = (not np.isnan(br) and br > 90) or (not np.isnan(byoy) and byoy > 30)
    inflation = not np.isnan(be) and be > 2.50
    fear = not np.isnan(v) and v > 25
    return energy, inflation, fear


def replay_geopolitics(
    brent: float, brent_yoy_pct: float, breakeven_effective: float, vix: float,
    denoising: bool = True,
) -> Tuple[str, bool, bool, bool, bool, bool, bool, bool]:
    """
    Geopolitics 双腿确认（Dual-Leg Confirmation）。
    WATCH: Condition_Energy_Shock OR Condition_Inflation_Panic OR Condition_Fear（breakeven 可单独 WATCH）。
    ALERT: (Inflation AND (Energy OR Fear)) OR (Energy AND Fear)。
    ALARM: Inflation AND Energy AND Fear。
    返回 (alert_level, watch_flag, alert_flag, condition_energy_shock, condition_inflation_panic, condition_fear, dual_leg_confirmed, upgrade_blocked_by_single_leg_rule)。
    """
    energy, inflation, fear = _geo_conditions(brent, brent_yoy_pct, breakeven_effective, vix)

    if not denoising:
        # 旧逻辑兼容（before 对比用）
        alert = "NONE"
        br = _safe(brent); byoy = _safe(brent_yoy_pct); be = _safe(breakeven_effective); v = _safe(vix)
        if not np.isnan(br) and br > 105:
            alert = "ALARM"
        if not np.isnan(be) and be > 2.65:
            alert = "ALARM"
        if alert != "ALARM":
            if (not np.isnan(br) and br > 95) or (not np.isnan(byoy) and byoy > 30) or (not np.isnan(be) and be > 2.50) or (not np.isnan(v) and v > 25):
                alert = "ALERT"
        if alert == "NONE":
            if (not np.isnan(br) and br > 85) or (not np.isnan(byoy) and byoy > 20) or (not np.isnan(be) and be > 2.35) or (not np.isnan(v) and v > 20):
                alert = "WATCH"
        watch = alert in ("WATCH", "ALERT", "ALARM")
        alert_flag = alert in ("ALERT", "ALARM")
        return alert, watch, alert_flag, energy, inflation, fear, False, False

    # 双腿确认逻辑
    alert = "NONE"
    upgrade_blocked = False
    dual_leg = False

    # ALARM: 三腿齐
    if energy and inflation and fear:
        alert = "ALARM"
        dual_leg = True
    # ALERT: (Inflation AND (Energy OR Fear)) OR (Energy AND Fear)
    elif (inflation and (energy or fear)) or (energy and fear):
        alert = "ALERT"
        dual_leg = True
    else:
        # 仅单腿满足 ALERT 条件时标记被拦截
        if (inflation and not energy and not fear) or (energy and not inflation and not fear) or (fear and not inflation and not energy):
            upgrade_blocked = True

    # WATCH: 任一条即可（breakeven 单独可 WATCH）
    if alert == "NONE" and (energy or inflation or fear):
        alert = "WATCH"

    watch = alert in ("WATCH", "ALERT", "ALARM")
    alert_flag = alert in ("ALERT", "ALARM")
    return alert, watch, alert_flag, energy, inflation, fear, dual_leg, upgrade_blocked


def _resonance_level1_count(
    hy_oas_weekly_bp: float, t5yie: float, brent: float, vix: float,
    bizd_vs_50dma_pct: float, stlfsi4: float,
) -> int:
    """Resonance Level1 条件满足个数（用于 persistence 判断）。"""
    hy_bp = _safe(hy_oas_weekly_bp); t5 = _safe(t5yie); br = _safe(brent)
    v = _safe(vix); bizd = _safe(bizd_vs_50dma_pct); stl = _safe(stlfsi4)
    c1 = [
        not np.isnan(hy_bp) and hy_bp > 30, not np.isnan(t5) and t5 > 2.4,
        not np.isnan(br) and br > 90, not np.isnan(v) and v > 22,
        not np.isnan(bizd) and bizd < -7.0,
    ]
    return sum(c1)


def replay_resonance(
    hy_oas_weekly_bp: float,
    t5yie: float,
    brent: float,
    vix: float,
    bizd_vs_50dma_pct: float,
    stlfsi4: float,
    credit_stress_on: bool,
    completeness: str = "LOW",
    level1_count_prev_day: int = 0,
    denoising: bool = True,
) -> str:
    """
    OFF / LEVEL_1 / LEVEL_2 / RED_ALERT。
    denoising=True 时：LEVEL_1 需连续 2 天满足或 completeness != LOW 才允许上 LEVEL_1。
    """
    hy_bp = _safe(hy_oas_weekly_bp)
    t5 = _safe(t5yie)
    br = _safe(brent)
    v = _safe(vix)
    bizd = _safe(bizd_vs_50dma_pct)
    stl = _safe(stlfsi4)

    c1_hy = not np.isnan(hy_bp) and hy_bp > 30
    c1_t5 = not np.isnan(t5) and t5 > 2.4
    c1_brent = not np.isnan(br) and br > 90
    c1_vix = not np.isnan(v) and v > 22
    c1_bizd = not np.isnan(bizd) and bizd < -7.0
    level1 = sum([c1_hy, c1_t5, c1_brent, c1_vix, c1_bizd])

    c2_hy = not np.isnan(hy_bp) and hy_bp > 50
    c2_t5 = not np.isnan(t5) and t5 > 2.5
    c2_brent = not np.isnan(br) and br > 95
    c2_vix = not np.isnan(v) and v > 25
    c2_bizd = not np.isnan(bizd) and bizd < -10.0
    c2_stl = not np.isnan(stl) and stl > 1.0
    level2 = sum([c2_hy, c2_t5, c2_brent, c2_vix, c2_bizd, c2_stl])

    core_hy = not np.isnan(hy_bp) and hy_bp > 50
    core_t5 = not np.isnan(t5) and t5 > 2.5
    core_bizd_stl = (not np.isnan(bizd) and bizd < -10.0) or (not np.isnan(stl) and stl > 1.0)
    red_ok = core_hy and core_t5 and core_bizd_stl and credit_stress_on

    if red_ok:
        return "RED_ALERT"
    if level2 >= 3:
        return "LEVEL_2"
    if level1 >= 2:
        if not denoising:
            return "LEVEL_1"
        persistence_ok = level1_count_prev_day >= 2 or (completeness or "").upper() != "LOW"
        if persistence_ok:
            return "LEVEL_1"
    return "OFF"


def replay_signal_confidence(breakeven_is_stale: bool, n_critical_missing: int) -> str:
    """简化：按 breakeven 是否 stale 与关键缺失数。"""
    if n_critical_missing >= 2 or breakeven_is_stale and n_critical_missing >= 1:
        return "LOW"
    if breakeven_is_stale or n_critical_missing >= 1:
        return "MEDIUM"
    return "HIGH"


def replay_geopolitics_completeness(
    brent_valid: bool, breakeven_valid: bool, breakeven_stale: bool, vix_valid: bool,
) -> str:
    """HIGH / PARTIAL / LOW。"""
    be_effective = breakeven_valid and not breakeven_stale
    n = sum([brent_valid, be_effective, vix_valid])
    if n >= 3:
        return "HIGH"
    if n == 2:
        return "PARTIAL"
    return "LOW"


def run_daily_replay(features_path: Path, denoising: bool = True) -> pd.DataFrame:
    """读 features CSV，逐行重放规则，返回带填实列的 DataFrame。denoising=False 时为去噪前规则。"""
    df = pd.read_csv(features_path)
    df["date"] = pd.to_datetime(df["date"])

    # 用 5d bp 近似周度 bp（Resonance 输入）
    hy_weekly_bp = df["hy_oas_5d_bp_change"]
    # credit_stress 代理：HY 与 VIX 双高
    credit_stress = (df["hy_oas"].fillna(0) >= 5.0) & (df["vix"].fillna(0) >= 25)

    pc_levels = []
    pc_watch = []
    pc_alert = []
    geo_levels = []
    geo_watch = []
    geo_alert = []
    res_levels = []
    n_critical_missing = (
        df["hy_oas"].isna().astype(int) +
        df["stlfsi4"].isna().astype(int) +
        df["brent_spot"].isna().astype(int) +
        df["breakeven_effective"].isna().astype(int) +
        df["vix"].isna().astype(int)
    )
    conf_list = []
    comp_list = []

    level1_prev = 0
    leg_geo_vix = []
    leg_geo_brent = []
    leg_geo_breakeven = []
    leg_pc_hy_oas = []
    leg_pc_stlfsi4 = []
    leg_pc_bizd = []
    leg_pc_hy_5d = []
    geo_energy = []
    geo_inflation = []
    geo_fear = []
    geo_dual_leg = []
    geo_upgrade_blocked = []

    for i in range(len(df)):
        row = df.iloc[i]
        stale = row.get("breakeven_is_stale")
        if isinstance(stale, str):
            stale = stale in ("True", "true", "1")
        comp = replay_geopolitics_completeness(
            pd.notna(row.get("brent_spot")),
            pd.notna(row.get("breakeven_effective")),
            bool(stale),
            pd.notna(row.get("vix")),
        )
        alert_pc, watch_pc, alert_pc_f = replay_private_credit(
            row.get("hy_oas"), row.get("stlfsi4"), row.get("bizd_vs_50dma_pct"),
            row.get("hy_oas_5d_bp_change"),
        )
        _geo = replay_geopolitics(
            row.get("brent_spot"), row.get("brent_yoy_pct"), row.get("breakeven_effective"), row.get("vix"),
            denoising=denoising,
        )
        alert_geo, watch_geo, alert_geo_f = _geo[0], _geo[1], _geo[2]
        if len(_geo) >= 8:
            geo_energy.append(_geo[3]); geo_inflation.append(_geo[4]); geo_fear.append(_geo[5])
            geo_dual_leg.append(_geo[6]); geo_upgrade_blocked.append(_geo[7])
        else:
            geo_energy.append(False); geo_inflation.append(False); geo_fear.append(False)
            geo_dual_leg.append(False); geo_upgrade_blocked.append(False)
        level1_raw = _resonance_level1_count(
            row.get("hy_oas_5d_bp_change"), row.get("breakeven_effective"), row.get("brent_spot"), row.get("vix"),
            row.get("bizd_vs_50dma_pct"), row.get("stlfsi4"),
        )
        res = replay_resonance(
            row.get("hy_oas_5d_bp_change"),
            row.get("breakeven_effective"),
            row.get("brent_spot"),
            row.get("vix"),
            row.get("bizd_vs_50dma_pct"),
            row.get("stlfsi4"),
            bool(credit_stress.iloc[i]) if i < len(credit_stress) else False,
            completeness=comp,
            level1_count_prev_day=level1_prev,
            denoising=denoising,
        )
        level1_prev = level1_raw

        conf = replay_signal_confidence(bool(stale), int(n_critical_missing.iloc[i]) if i < len(n_critical_missing) else 0)

        # 每条触发腿是否为 True（便于拆误报）
        br = _safe(row.get("brent_spot")); byoy = _safe(row.get("brent_yoy_pct"))
        be = _safe(row.get("breakeven_effective")); v = _safe(row.get("vix"))
        hy = _safe(row.get("hy_oas")); stl = _safe(row.get("stlfsi4"))
        bizd = _safe(row.get("bizd_vs_50dma_pct")); hy5 = _safe(row.get("hy_oas_5d_bp_change"))
        leg_geo_vix.append(not np.isnan(v) and (v > 20 or v > 25))
        leg_geo_brent.append((not np.isnan(br) and (br > 85 or br > 95)) or (not np.isnan(byoy) and (byoy > 20 or byoy > 30)))
        leg_geo_breakeven.append(not np.isnan(be) and (be > 2.35 or be > 2.5))
        leg_pc_hy_oas.append(not np.isnan(hy) and (hy > 4.5 or hy > 5))
        leg_pc_stlfsi4.append(not np.isnan(stl) and (stl > 0 or stl > 1))
        leg_pc_bizd.append(not np.isnan(bizd) and (bizd < -5 or bizd < -10))
        leg_pc_hy_5d.append(not np.isnan(hy5) and (hy5 > 30 or hy5 > 50))

        pc_levels.append(alert_pc)
        pc_watch.append(watch_pc)
        pc_alert.append(alert_pc_f)
        geo_levels.append(alert_geo)
        geo_watch.append(watch_geo)
        geo_alert.append(alert_geo_f)
        res_levels.append(res)
        conf_list.append(conf)
        comp_list.append(comp)

    df["private_credit_alert_level"] = pc_levels
    df["private_credit_watch_flag"] = pc_watch
    df["private_credit_alert_flag"] = pc_alert
    df["geopolitics_alert_level"] = geo_levels
    df["geopolitics_watch_flag"] = geo_watch
    df["geopolitics_alert_flag"] = geo_alert
    df["resonance_level"] = res_levels
    df["signal_confidence"] = conf_list
    df["geopolitics_completeness"] = comp_list
    df["leg_geo_vix"] = leg_geo_vix
    df["leg_geo_brent"] = leg_geo_brent
    df["leg_geo_breakeven"] = leg_geo_breakeven
    df["leg_pc_hy_oas"] = leg_pc_hy_oas
    df["leg_pc_stlfsi4"] = leg_pc_stlfsi4
    df["leg_pc_bizd"] = leg_pc_bizd
    df["leg_pc_hy_5d"] = leg_pc_hy_5d
    if geo_energy:
        df["condition_energy_shock"] = geo_energy
        df["condition_inflation_panic"] = geo_inflation
        df["condition_fear"] = geo_fear
        df["dual_leg_confirmed"] = geo_dual_leg
        df["upgrade_blocked_by_single_leg_rule"] = geo_upgrade_blocked
    return df


def aggregate_by_scenarios(
    filled: pd.DataFrame,
    scenarios_path: Path,
) -> pd.DataFrame:
    """
    按 scenarios 窗口聚合：首次触发日、首次升级日、峰值等级、提前/滞后天数、是否误报过多。
    """
    scenarios = pd.read_csv(scenarios_path)
    scenarios["date_start"] = pd.to_datetime(scenarios["date_start"])
    scenarios["date_end"] = pd.to_datetime(scenarios["date_end"])
    filled["date"] = pd.to_datetime(filled["date"])

    rows = []
    for _, sc in scenarios.iterrows():
        start, end = sc["date_start"], sc["date_end"]
        win = filled[(filled["date"] >= start) & (filled["date"] <= end)].copy()
        stype = sc.get("scenario_type", "")

        # 无数据窗口：标记 NOT_TESTED，避免与「有数据但没触发/误报」混淆
        if len(win) == 0:
            rows.append({
                "scenario_name": sc["scenario_name"],
                "scenario_type": stype,
                "date_start": start,
                "date_end": end,
                "验证状态": "NOT_TESTED",
                "首次触发日": "-",
                "首次升级日": "-",
                "峰值等级": "-",
                "提前_滞后_天数": "-",
                "是否误报过多": "-",
                "expected_behavior": sc.get("expected_behavior", ""),
            })
            continue

        # 验证状态：有数据则为 TESTED
        rows.append({})  # 占位，下面统一填
        r = rows[-1]
        r["scenario_name"] = sc["scenario_name"]
        r["scenario_type"] = stype
        r["date_start"] = start
        r["date_end"] = end
        r["验证状态"] = "TESTED"

        # 首次触发日：任一脚首次 WATCH
        first_watch_pc = win.loc[win["private_credit_watch_flag"] == True, "date"]
        first_watch_geo = win.loc[win["geopolitics_watch_flag"] == True, "date"]
        first_watch_pc_date = first_watch_pc.min() if len(first_watch_pc) else pd.NaT
        first_watch_geo_date = first_watch_geo.min() if len(first_watch_geo) else pd.NaT
        candidates = [d for d in [first_watch_pc_date, first_watch_geo_date] if pd.notna(d)]
        first_trigger = min(candidates) if candidates else pd.NaT

        # 首次升级日：任一脚首次 ALERT 或 Resonance 升级
        first_alert_pc = win[win["private_credit_alert_flag"] == True]
        first_alert_geo = win[win["geopolitics_alert_flag"] == True]
        first_res = win[win["resonance_level"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT"))]
        first_alert_pc_date = first_alert_pc["date"].min() if not first_alert_pc.empty else pd.NaT
        first_alert_geo_date = first_alert_geo["date"].min() if not first_alert_geo.empty else pd.NaT
        first_res_date = first_res["date"].min() if not first_res.empty else pd.NaT
        upgrade_dates = [d for d in [first_alert_pc_date, first_alert_geo_date, first_res_date] if pd.notna(d)]
        first_upgrade = min(upgrade_dates) if upgrade_dates else pd.NaT

        # 峰值等级：窗口内最高档
        level_order = {"NONE": 0, "WATCH": 1, "ALERT": 2, "ALARM": 3}
        inv_level = {v: k for k, v in level_order.items()}
        pc_ord = win["private_credit_alert_level"].map(level_order)
        geo_ord = win["geopolitics_alert_level"].map(level_order)
        res_map = {"OFF": 0, "LEVEL_1": 1, "LEVEL_2": 2, "RED_ALERT": 3}
        inv_res = {v: k for k, v in res_map.items()}
        pc_max = pc_ord.max() if len(pc_ord) else 0
        geo_max = geo_ord.max() if len(geo_ord) else 0
        res_max = win["resonance_level"].map(res_map).max() if len(win) else 0
        peak_pc = inv_level.get(pc_max, "NONE")
        peak_geo = inv_level.get(geo_max, "NONE")
        peak_res = inv_res.get(res_max, "OFF") if pd.notna(res_max) else "OFF"
        peak_str = f"PC={peak_pc} GEO={peak_geo} RES={peak_res}"

        # 提前/滞后：首次触发日相对 date_start 的天数（负=提前，正=滞后）
        if pd.notna(first_trigger):
            lead_lag_days = (first_trigger - start).days
        else:
            lead_lag_days = None

        # 是否误报过多：按场景类型启发式
        if stype == "vol_no_resonance":
            alert_days = (win["geopolitics_alert_flag"] == True).sum() + (win["resonance_level"] == "RED_ALERT").sum()
            too_many_false = alert_days > 5 or (win["resonance_level"] == "RED_ALERT").any()
        elif stype == "oil_shock":
            too_many_false = False
        elif stype == "credit_widening":
            too_many_false = False
        elif stype == "true_resonance":
            too_many_false = not (win["resonance_level"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT")).any())
        else:
            too_many_false = False

        r["首次触发日"] = first_trigger if pd.notna(first_trigger) else ""
        r["首次升级日"] = first_upgrade if pd.notna(first_upgrade) else ""
        r["峰值等级"] = peak_str
        r["提前_滞后_天数"] = lead_lag_days
        r["是否误报过多"] = too_many_false
        r["expected_behavior"] = sc.get("expected_behavior", "")

    return pd.DataFrame(rows)


def export_denoising_before_after(features_path: Path, out_dir: Path) -> None:
    """
    去噪前后对比：2021-09~11 逐日 + Oil shock 2022-02 摘要。
    验证：去噪后误报是否减少；去噪后 Oil shock 是否被压没。
    """
    filled_before = run_daily_replay(features_path, denoising=False)
    filled_after = run_daily_replay(features_path, denoising=True)
    filled_before["date"] = pd.to_datetime(filled_before["date"])
    filled_after["date"] = pd.to_datetime(filled_after["date"])

    # 1) Vol 窗口 2021-09-01 ~ 2021-11-30 逐日 before/after
    start_vol = pd.Timestamp("2021-09-01")
    end_vol = pd.Timestamp("2021-11-30")
    b = filled_before[(filled_before["date"] >= start_vol) & (filled_before["date"] <= end_vol)][["date", "geopolitics_alert_level", "resonance_level", "leg_geo_vix", "leg_geo_brent", "leg_geo_breakeven", "geopolitics_completeness", "signal_confidence"]].copy()
    b = b.rename(columns={"geopolitics_alert_level": "Geopolitics_before", "resonance_level": "Resonance_before"})
    a = filled_after[(filled_after["date"] >= start_vol) & (filled_after["date"] <= end_vol)][["date", "geopolitics_alert_level", "resonance_level"]].copy()
    a = a.rename(columns={"geopolitics_alert_level": "Geopolitics_after", "resonance_level": "Resonance_after"})
    vol_compare = b.merge(a, on="date", how="outer")
    vol_compare = vol_compare.rename(columns={"leg_geo_vix": "腿_VIX", "leg_geo_brent": "腿_Brent", "leg_geo_breakeven": "腿_breakeven", "geopolitics_completeness": "completeness"})
    # 列顺序：日期、腿、completeness/confidence、before/after
    col_order = ["date", "腿_breakeven", "腿_VIX", "腿_Brent", "completeness", "signal_confidence", "Geopolitics_before", "Geopolitics_after", "Resonance_before", "Resonance_after"]
    vol_compare = vol_compare[[c for c in col_order if c in vol_compare.columns]]
    # 汇总行：误报是否减少
    geo_alert_alarm_before = (vol_compare["Geopolitics_before"].isin(("ALERT", "ALARM"))).sum()
    geo_alert_alarm_after = (vol_compare["Geopolitics_after"].isin(("ALERT", "ALARM"))).sum()
    res_elevated_before = (vol_compare["Resonance_before"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT"))).sum()
    res_elevated_after = (vol_compare["Resonance_after"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT"))).sum()
    summary_vol = pd.DataFrame([
        {"metric": "Geopolitics_ALERT或ALARM_天数", "before": geo_alert_alarm_before, "after": geo_alert_alarm_after, "变化": geo_alert_alarm_after - geo_alert_alarm_before},
        {"metric": "Resonance_LEVEL1及以上_天数", "before": res_elevated_before, "after": res_elevated_after, "变化": res_elevated_after - res_elevated_before},
    ])
    path_vol = out_dir / "event_x_validation_denoising_before_after_vol.csv"
    vol_compare.to_csv(path_vol, index=False, encoding="utf-8")
    path_vol_summary = out_dir / "event_x_validation_denoising_before_after_vol_summary.csv"
    summary_vol.to_csv(path_vol_summary, index=False, encoding="utf-8")

    # 2) Oil shock 窗口 2022-02-01 ~ 2022-06-30 摘要：去噪后是否仍能触发
    start_oil = pd.Timestamp("2022-02-01")
    end_oil = pd.Timestamp("2022-06-30")
    oil_before = filled_before[(filled_before["date"] >= start_oil) & (filled_before["date"] <= end_oil)]
    oil_after = filled_after[(filled_after["date"] >= start_oil) & (filled_after["date"] <= end_oil)]
    oil_summary = pd.DataFrame([
        {"metric": "Geopolitics_ALERT或ALARM_天数", "before": (oil_before["geopolitics_alert_level"].isin(("ALERT", "ALARM"))).sum(), "after": (oil_after["geopolitics_alert_level"].isin(("ALERT", "ALARM"))).sum()},
        {"metric": "Resonance_LEVEL1及以上_天数", "before": (oil_before["resonance_level"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT"))).sum(), "after": (oil_after["resonance_level"].isin(("LEVEL_1", "LEVEL_2", "RED_ALERT"))).sum()},
    ])
    path_oil = out_dir / "event_x_validation_denoising_before_after_oil_shock_summary.csv"
    oil_summary.to_csv(path_oil, index=False, encoding="utf-8")


def export_vol_window_daily_legs(filled: pd.DataFrame, out_dir: Path) -> None:
    """
    导出 2021-09~11 窗口逐日触发腿小表，便于一眼看出是哪条腿误伤。
    """
    start = pd.Timestamp("2021-09-01")
    end = pd.Timestamp("2021-11-30")
    win = filled[(filled["date"] >= start) & (filled["date"] <= end)].copy()
    if win.empty:
        return
    cols = [
        "date",
        "private_credit_alert_level",
        "geopolitics_alert_level",
        "resonance_level",
        "leg_geo_vix",
        "leg_geo_brent",
        "leg_geo_breakeven",
        "leg_pc_bizd",
        "leg_pc_hy_5d",
        "leg_pc_stlfsi4",
        "leg_pc_hy_oas",
        "geopolitics_completeness",
        "signal_confidence",
    ]
    out = win[[c for c in cols if c in win.columns]]
    out = out.rename(columns={
        "private_credit_alert_level": "Private_Credit_状态",
        "geopolitics_alert_level": "Geopolitics_状态",
        "resonance_level": "Resonance_等级",
        "leg_geo_vix": "腿_VIX",
        "leg_geo_brent": "腿_Brent",
        "leg_geo_breakeven": "腿_breakeven",
        "leg_pc_bizd": "腿_BIZD",
        "leg_pc_hy_5d": "腿_HY_OAS_5D_BP",
        "leg_pc_stlfsi4": "腿_STLFSI4",
        "leg_pc_hy_oas": "腿_HY_OAS",
        "geopolitics_completeness": "completeness",
        "signal_confidence": "signal_confidence",
    })
    path = out_dir / "event_x_validation_vol_window_daily_legs.csv"
    out.to_csv(path, index=False, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Event-X 逐日重放 + 场景聚合")
    parser.add_argument("--features", type=Path, default=DATA_DIR / "event_x_validation_features.csv")
    parser.add_argument("--scenarios", type=Path, default=DATA_DIR / "event_x_validation_scenarios.csv")
    parser.add_argument("--out-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()

    if not args.features.exists():
        print(f"Missing: {args.features}. Run build_event_x_validation_dataset.py first.")
        return

    print("Running daily replay...")
    filled = run_daily_replay(args.features)
    out_features = args.out_dir / "event_x_validation_features_filled.csv"
    filled.to_csv(out_features, index=False, encoding="utf-8")
    print(f"  -> {out_features} ({len(filled)} rows)")

    export_vol_window_daily_legs(filled, args.out_dir)
    vol_path = args.out_dir / "event_x_validation_vol_window_daily_legs.csv"
    if vol_path.exists():
        print(f"  -> {vol_path} (2021-09~11 逐日触发腿)")

    print("Building denoising before/after comparison...")
    export_denoising_before_after(args.features, args.out_dir)
    print(f"  -> {args.out_dir / 'event_x_validation_denoising_before_after_vol.csv'} (2021-09~11 逐日 before/after)")
    print(f"  -> {args.out_dir / 'event_x_validation_denoising_before_after_vol_summary.csv'} (误报是否减少)")
    print(f"  -> {args.out_dir / 'event_x_validation_denoising_before_after_oil_shock_summary.csv'} (Oil shock 是否被压没)")

    if args.scenarios.exists():
        print("Aggregating by scenario windows...")
        agg = aggregate_by_scenarios(filled, args.scenarios)
        out_agg = args.out_dir / "event_x_validation_scenario_results.csv"
        agg.to_csv(out_agg, index=False, encoding="utf-8")
        print(f"  -> {out_agg} ({len(agg)} scenarios)")
    else:
        print(f"  (no scenarios file: {args.scenarios})")

    print("Done.")


if __name__ == "__main__":
    main()
