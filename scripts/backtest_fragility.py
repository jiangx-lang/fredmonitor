#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Offline backtest / diagnostics for C-layer fragility trigger separability.

This is an independent analysis script. It does NOT modify any business code.
"""

import os
import yaml
import json
from datetime import date

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_series(sid: str):
    path = os.path.join(BASE, "data", "fred", "series", sid, "raw.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.dropna(how="all").sort_index()
    s = df.iloc[:, 0].dropna()
    s.name = sid
    return s


def reindex(s: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    if s is None:
        return pd.Series(np.nan, index=idx)
    return s.reindex(idx).ffill().bfill()


def reindex_ffill_only(s: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    """Reindex with forward fill only.

    Keep leading NaNs (do NOT backfill) to make pre-start replacement logic work.
    """
    if s is None:
        return pd.Series(np.nan, index=idx)
    return s.reindex(idx).ffill()


# ----------------------------
# First part: construct historical signal sequences
# ----------------------------

baml = load_series("BAMLH0A0HYM2")  # HY OAS
sofr = load_series("SOFR")  # 2018+
dtb3 = load_series("DTB3")  # 1954+ (rate proxy)
fedfunds = load_series("FEDFUNDS")  # proxy for pre-SOFR monetary market pressure
cpn3m = load_series("CPN3M")  # monthly
vix = load_series("VIXCLS")  # 1990+
nfci = load_series("NFCI")  # confirmed exists


# unified business-day index (1997-01-01 to today)
idx = pd.date_range("1997-01-01", date.today(), freq="B")


# Signal 1: HY_OAS_MOMENTUM_RATIO = HY_OAS / HY_OAS.rolling(20).mean()
hy_oas = reindex(baml, idx)
hy_oas_momentum = hy_oas / hy_oas.rolling(20).mean()


# Signal 2: SOFR20DMA_MINUS_DTB3
sofr_full = reindex_ffill_only(sofr, idx)
dtb3_full = reindex(dtb3, idx)
fedfunds_full = reindex(fedfunds, idx)

# baseline (pre-fix): original proxy
sofr_proxy_base = sofr_full.where(sofr_full.notna(), dtb3_full)
sofr20dma_base = sofr_proxy_base.rolling(20).mean()
sofr_minus_dtb3_base = sofr20dma_base - dtb3_full

# fixed (post-fix): 2018-04-02 前用 FEDFUNDS 作为 SOFR 代理
SOFR_START = pd.Timestamp("2018-04-02")
sofr_proxy_series = sofr_full.copy()
mask_pre2018 = sofr_proxy_series.isna() | (sofr_proxy_series.index < SOFR_START)
sofr_proxy_series[mask_pre2018] = fedfunds_full[mask_pre2018]
sofr20dma_fixed = sofr_proxy_series.rolling(20).mean()
sofr_minus_dtb3_fixed = sofr20dma_fixed - dtb3_full


# Signal 3: CP_MINUS_DTB3
cpn3m_full = reindex(cpn3m, idx)  # monthly -> ffill to daily
cp_minus_dtb3 = cpn3m_full - dtb3_full


# Signal 4: NFCI (direct)
nfci_full = reindex(nfci, idx)


# Signal 5: HYG_LQD_RATIO change proxy (HY OAS 20d percent change rate)
hy_oas_chg20 = hy_oas.pct_change(20) * 100  # percent change
hy_oas_chg20_full = reindex(hy_oas_chg20, idx)


# Signal 6: VIX_TERM_STRUCTURE proxy = VIX / VIX.rolling(60).mean()
vix_full = reindex(vix, idx)
vix_ts_proxy = vix_full / vix_full.rolling(60).mean()


# Signal 7: SP500_DGS10_CORR60D proxy (simplified correlation direction)
sp_dgs10_corr_proxy = vix_full.pct_change(1).rolling(60).corr(dtb3_full.diff(1)) * -1


# ----------------------------
# Second part: compute historical fragility_score (simplified trigger logic)
# ----------------------------

# This global variable will be swapped for baseline vs fixed run.
sofr_minus_dtb3 = sofr_minus_dtb3_base


def compute_fragility_score_historical(date_idx: pd.Timestamp):
    trigger_score = 0
    fired = []

    # Signal 1: HY_OAS_MOMENTUM_RATIO > 1.05
    v1 = hy_oas_momentum.get(date_idx, np.nan)
    if pd.notna(v1) and v1 > 1.05:
        trigger_score += 20
        fired.append(f"HY_OAS({v1:.3f})")

    # Signal 2: VIX_TERM_STRUCTURE > 1.0
    v2 = vix_ts_proxy.get(date_idx, np.nan)
    if pd.notna(v2) and v2 > 1.0:
        trigger_score += 15
        fired.append(f"VIX_TS({v2:.3f})")

    # Signal 3: SOFR20DMA_MINUS_DTB3 risk_score proxy
    v3 = sofr_minus_dtb3.get(date_idx, np.nan)
    if pd.notna(v3) and v3 > 0.15:
        trigger_score += 15
        fired.append(f"SOFR_DTB3({v3:.3f})")

    # Signal 4: CP_MINUS_DTB3 > 0.20%
    v4 = cp_minus_dtb3.get(date_idx, np.nan)
    if pd.notna(v4) and v4 > 0.20:
        trigger_score += 12
        fired.append(f"CP_DTB3({v4:.3f})")

    # Signal 5: HYG_LQD_RATIO change proxy
    v5 = hy_oas_chg20_full.get(date_idx, np.nan)
    if pd.notna(v5) and v5 > 15:
        trigger_score += 10
        fired.append(f"HY_CHG20({v5:.1f}%)")

    # Signal 6: NFCI (dual trigger simplified)
    v6 = nfci_full.get(date_idx, np.nan)
    if pd.notna(v6):
        if v6 > 0:
            trigger_score += 10
            fired.append(f"NFCI_strong({v6:.3f})")
        elif v6 > -0.3:
            trigger_score += 5
            fired.append(f"NFCI_weak({v6:.3f})")

    # Signal 7: SP500_DGS10_CORR proxy > 0.5
    v7 = sp_dgs10_corr_proxy.get(date_idx, np.nan)
    if pd.notna(v7) and v7 > 0.5:
        trigger_score += 5
        fired.append(f"CORR({v7:.3f})")

    # simplified amplification (no breadth history available)
    amp = 0
    fired_set = set(s.split("(")[0] for s in fired)
    if "HY_OAS" in fired_set and "VIX_TS" in fired_set:
        amp += 15
    if "SOFR_DTB3" in fired_set and "CP_DTB3" in fired_set:
        amp += 10
    if "CORR" in fired_set and ("SOFR_DTB3" in fired_set or "CP_DTB3" in fired_set):
        amp += 5

    total = min(trigger_score + amp, 100)

    if total < 20:
        state = "LOW"
    elif total < 45:
        state = "MEDIUM"
    elif total < 70:
        state = "HIGH"
    else:
        state = "CRITICAL"

    return total, state, fired


def build_hist_df(monthly_idx: pd.DatetimeIndex) -> pd.DataFrame:
    records = []
    for dt in monthly_idx:
        score, state, fired = compute_fragility_score_historical(dt)
        records.append(
            {
                "date": dt.date(),
                "fragility_score": score,
                "fragility_state": state,
                "triggers": "|".join(fired) if fired else "",
            }
        )
    return pd.DataFrame(records).set_index("date")


# ----------------------------
# Third part: label crisis periods
# ----------------------------

monthly_idx = pd.date_range("1997-01-01", date.today(), freq="MS")

crisis_path = os.path.join(BASE, "config", "crisis_periods.yaml")
with open(crisis_path, encoding="utf-8") as f:
    crisis_cfg = yaml.safe_load(f)

# baseline focus (pre-fix): old label set (no newly appended crisis codes)
FOCUS_CRISES_BASELINE = [
    "LTCM_1998",
    "DOTCOM_2000",
    "GFC_2008",
    "EURO_2011",
    "CHINA_2015",
    "REPO_2019",
    "COVID_2020",
    "INFLATION_2022",
    "REG_BANK_2023",
]

# fixed focus (post-fix): include newly added crisis codes
FOCUS_CRISES = [
    "ASIAN_SPILLOVER_1997",
    "LTCM_1998",
    "DOTCOM_2000",
    "GFC_2008",
    "POST_GFC_STRESS_2010",
    "EURO_2011",
    "EURO_EXTENDED_2012",
    "CHINA_2015",
    "REPO_2019",
    "COVID_2020",
    "INFLATION_2022",
    "REG_BANK_2023",
    "SUBPRIME_PRELUDE_2006",
    "OIL_CREDIT_2014",
]


def build_crisis_map(focus_codes: list) -> dict:
    # Build windows first, then insert into dict following `focus_codes` order.
    # This ensures that when crisis windows overlap, the earlier code in `focus_codes`
    # gets priority in label_crisis_with_map().
    code_to_window = {}
    for c in crisis_cfg.get("crises", []):
        code_to_window[c["code"]] = (
            pd.to_datetime(c["start"]).date(),
            pd.to_datetime(c["end"]).date(),
        )
    crisis_map = {}
    for code in focus_codes:
        if code in code_to_window:
            crisis_map[code] = code_to_window[code]
    return crisis_map


def label_crisis_with_map(dt_date, crisis_map: dict) -> str:
    for code, (s, e) in crisis_map.items():
        if s <= dt_date <= e:
            return code
    return "NORMAL"


crisis_map_before = build_crisis_map(FOCUS_CRISES_BASELINE)
crisis_map_after = build_crisis_map(FOCUS_CRISES)


# ----------------------------
# Run baseline (修正前): sofr_proxy_base
# ----------------------------
sofr_minus_dtb3 = sofr_minus_dtb3_base
hist_df_before = build_hist_df(monthly_idx)
hist_df_before["crisis_label"] = hist_df_before.index.map(
    lambda d: label_crisis_with_map(d, crisis_map_before)
)


# ----------------------------
# Run fixed (修正后): sofr_proxy_series uses FEDFUNDS pre-2018
# ----------------------------
sofr_minus_dtb3 = sofr_minus_dtb3_fixed
hist_df = build_hist_df(monthly_idx)
hist_df["crisis_label"] = hist_df.index.map(
    lambda d: label_crisis_with_map(d, crisis_map_after)
)


# ----------------------------
# First: 修正前后对比表（关键指标）
# ----------------------------

def stats_series(hist: pd.DataFrame, code: str) -> pd.Series:
    return hist[hist["crisis_label"] == code]["fragility_score"]


normal_before = stats_series(hist_df_before, "NORMAL")
normal_after = stats_series(hist_df, "NORMAL")
normal_high_before = int((normal_before >= 45).sum())
normal_high_after = int((normal_after >= 45).sum())

gfc_peak_before = stats_series(hist_df_before, "GFC_2008").max()
covid_peak_before = stats_series(hist_df_before, "COVID_2020").max()
ltcm_peak_before = stats_series(hist_df_before, "LTCM_1998").max()
china_peak_before = stats_series(hist_df_before, "CHINA_2015").max()

gfc_peak_after = stats_series(hist_df, "GFC_2008").max()
covid_peak_after = stats_series(hist_df, "COVID_2020").max()
ltcm_peak_after = stats_series(hist_df, "LTCM_1998").max()
china_peak_after = stats_series(hist_df, "CHINA_2015").max()

post_gfc_mean_after = stats_series(hist_df, "POST_GFC_STRESS_2010").mean()
euro_extended_mean_after = stats_series(hist_df, "EURO_EXTENDED_2012").mean()

print("\n" + "=" * 70)
print("修正前后对比表（关键指标）")
print("=" * 70)
print("{:<28} {:>12} {:>12}".format("指标", "修正前", "修正后"))
print("-" * 70)
print("{:<28} {:>12} {:>12}".format("NORMAL >=45(月数)", normal_high_before, normal_high_after))
print("{:<28} {:>12.0f} {:>12.0f}".format("GFC_2008 峰值", gfc_peak_before, gfc_peak_after))
print("{:<28} {:>12.0f} {:>12.0f}".format("COVID_2020 峰值", covid_peak_before, covid_peak_after))
print("{:<28} {:>12.0f} {:>12.0f}".format("LTCM_1998 峰值", ltcm_peak_before, ltcm_peak_after))
print("{:<28} {:>12.0f} {:>12.0f}".format("CHINA_2015 峰值", china_peak_before, china_peak_after))
print("{:<28} {:>12} {:>12.1f}".format("POST_GFC_STRESS_2010 均值", "无数据(前)", float(post_gfc_mean_after)))
print("{:<28} {:>12} {:>12.1f}".format("EURO_EXTENDED_2012 均值", "无数据(前)", float(euro_extended_mean_after)))


# ----------------------------
# Second: 修正后输出段（按你的模板）
# ----------------------------

print("\n" + "=" * 70)
print("修正后：各危机期 fragility_score 统计")
print("=" * 70)
print(
    f"{'危机代码':<25} {'均值':>6} {'峰值':>6} "
    f"{'HIGH+占比':>10} {'月数':>6}"
)

REPORT_ORDER = [
    "NORMAL",
    "POST_GFC_STRESS_2010",
    "LTCM_1998",
    "DOTCOM_2000",
    "GFC_2008",
    "EURO_2011",
    "EURO_EXTENDED_2012",
    "CHINA_2015",
    "REPO_2019",
    "COVID_2020",
    "INFLATION_2022",
    "REG_BANK_2023",
    "ASIAN_SPILLOVER_1997",
    "SUBPRIME_PRELUDE_2006",
    "OIL_CREDIT_2014",
]

print("-" * 70)
for label in REPORT_ORDER:
    sub = hist_df[hist_df["crisis_label"] == label]["fragility_score"]
    if len(sub) == 0:
        print(f"{label:<25} {'无数据':>6}")
        continue
    high_pct = (sub >= 45).mean() * 100
    marker = " ◀ BASELINE" if label == "NORMAL" else ""
    print(
        "{:<25} {:>6.1f} {:>6.0f} {:>9.1f}% {:>6}{}".format(
            label,
            sub.mean(),
            sub.max(),
            high_pct,
            len(sub),
            marker,
        )
    )

print("\n=== 修正后 NORMAL 期残余高分月份（>=45分）===")
normal_post = hist_df[
    (hist_df["crisis_label"] == "NORMAL") & (hist_df["fragility_score"] >= 45)
]
if len(normal_post) == 0:
    print("无残余假阳性（≥45分）")
else:
    normal_post_out = normal_post.reset_index()[["date", "fragility_score", "triggers"]]
    print(normal_post_out.to_string(index=False))

print("\n=== 区分力诊断（修正后）===")
crisis_all = hist_df[hist_df["crisis_label"] != "NORMAL"]["fragility_score"]
normal_all = hist_df[hist_df["crisis_label"] == "NORMAL"]["fragility_score"]
print(f"危机期均值: {crisis_all.mean():.1f}  正常期均值: {normal_all.mean():.1f}  差值: {crisis_all.mean()-normal_all.mean():.1f}")
print(f"危机期中位数: {crisis_all.median():.1f}  正常期中位数: {normal_all.median():.1f}")
print(f"NORMAL期HIGH+占比（修正后）: {(normal_all >= 45).mean()*100:.1f}%")
print(f"危机期HIGH+占比: {(crisis_all >= 45).mean()*100:.1f}%")

print("\n" + "=" * 70)
print("=== 次贷前兆预警验证 ===")
print("=" * 70)
sub = hist_df[hist_df["crisis_label"] == "SUBPRIME_PRELUDE_2006"]
print(
    f"SUBPRIME_PRELUDE: 均值={sub['fragility_score'].mean():.1f} "
    f"峰值={sub['fragility_score'].max():.0f} 月数={len(sub)}"
)

print("\n" + "=" * 70)
print("=== 亚洲危机溢出验证 ===")
print("=" * 70)
sub2 = hist_df[hist_df["crisis_label"] == "ASIAN_SPILLOVER_1997"]
print(
    f"ASIAN_SPILLOVER: 均值={sub2['fragility_score'].mean():.1f} "
    f"峰值={sub2['fragility_score'].max():.0f} 月数={len(sub2)}"
)

print("\n" + "=" * 70)
print("SOFR proxy 修正是否有效（1句话）")
print("=" * 70)
effect_text = "降低了" if normal_high_after < normal_high_before else "未降低"
print(
    f"SOFR proxy 修正后，NORMAL 期 fragility_score>=45 的月份从 {normal_high_before} 降到 {normal_high_after}，因此 {effect_text} 假阳性。"
)


# Save fixed history for downstream use
out_dir = os.path.join(BASE, "outputs", "crisis_monitor")
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "fragility_history_backtest.csv")
hist_df.to_csv(out_path)
print(f"\n完整历史序列已保存到: {out_path}")

