#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regime Layer: Structural & Regime Risk Monitor (Dual-Layer Architecture)
Does NOT affect base risk score (0-100). Outputs "Regime Alerts" only.
5 modules: Policy Conflict & USD Anchor, De-Dollarization, Gold-Real Rate Divergence,
           K-Shaped Recovery, Bear Steepening.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import crisis_monitor as base

BASE = pathlib.Path(__file__).parent

# Regime alert levels (for display only; no impact on base score)
class RegimeAlert:
    NONE = "NONE"
    WATCH = "WATCH"
    ALERT = "ALERT"
    ALARM = "ALARM"


@dataclass
class RegimeModuleResult:
    name: str
    alert: str  # NONE / WATCH / ALERT / ALARM
    value: Optional[float] = None
    threshold_used: Optional[str] = None
    reason: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


class StructuralRiskMonitor:
    """
    Regime Layer: monitors institutional / structural risks.
    Runs in parallel to Base Layer (CrisisScorer). Outputs Regime Alerts only.
    """

    def __init__(self, base_dir: Optional[pathlib.Path] = None):
        self.base_dir = base_dir or BASE
        self.results: Dict[str, RegimeModuleResult] = {}
        self._series_cache: Dict[str, pd.Series] = {}
        self._sustained_days = 10  # require ~2 weeks of sustained signal where applicable

    def _fetch(self, series_id: str = "", use_yahoo: bool = False, yahoo_symbol: Optional[str] = None) -> pd.Series:
        key = f"yahoo:{yahoo_symbol}" if use_yahoo and yahoo_symbol else (series_id or "")
        if key in self._series_cache:
            return self._series_cache[key]
        if use_yahoo and yahoo_symbol:
            ts = base.fetch_yahoo_safe(yahoo_symbol)
        elif series_id:
            ts = base.fetch_series(series_id)
        else:
            ts = pd.Series(dtype="float64")
        self._series_cache[key] = ts
        return ts

    def _last(self, ts: pd.Series, default: float = np.nan) -> float:
        if ts is None or ts.empty:
            return default
        v = ts.iloc[-1]
        return float(v) if pd.notna(v) else default

    def _rolling_zscore(self, s: pd.Series, window: int = 252) -> pd.Series:
        if s is None or s.empty or len(s) < max(20, window // 2):
            return pd.Series(dtype="float64")
        m = s.rolling(window, min_periods=max(20, window // 2)).mean()
        std = s.rolling(window, min_periods=max(20, window // 2)).std()
        return (s - m) / (std + 1e-8)

    def _get_term_premium_series_and_source(self) -> Tuple[pd.Series, str]:
        """Primary: ACMTP10; Fallback 1: THREEFYTP10; Fallback 2: DGS10 - DGS2 (Slope proxy). Returns (series, source_label)."""
        acmtp10 = self._fetch("ACMTP10")
        if acmtp10 is not None and not acmtp10.empty and pd.notna(self._last(acmtp10, np.nan)):
            return acmtp10, "ACMTP10"
        threefytp10 = self._fetch("THREEFYTP10")
        if threefytp10 is not None and not threefytp10.empty and pd.notna(self._last(threefytp10, np.nan)):
            return threefytp10, "THREEFYTP10 (Kim-Wright)"
        dgs10 = self._fetch("DGS10")
        dgs2 = self._fetch("DGS2")
        if dgs10 is not None and not dgs10.empty and dgs2 is not None and not dgs2.empty:
            d2_aligned = dgs2.reindex(dgs10.index).ffill().bfill()
            proxy = dgs10 - d2_aligned
            if not proxy.empty and pd.notna(self._last(proxy, np.nan)):
                return proxy, "10Y-2Y Slope (Proxy)"
        return pd.Series(dtype="float64"), ""

    # ---------- 1. Policy Conflict & USD Anchor ----------
    def check_policy_conflict(self) -> RegimeModuleResult:
        """
        Fiscal (Stimulus/Weak USD) vs Monetary (Tight/Strong USD).
        - USEPUINDXD > 90th percentile -> Policy Uncertainty Alert.
        - Rolling 60d corr(DXY, DGS2) < -0.2 -> Rates UP but USD DOWN (Trust Issue).
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        epu = self._fetch("USEPUINDXD")
        dxy = self._fetch("", use_yahoo=True, yahoo_symbol="DX-Y.NYB")
        if dxy is None or dxy.empty:
            dxy = self._fetch("DTWEXBGS")  # trade-weighted dollar
        dgs2 = self._fetch("DGS2")

        epu_val = self._last(epu, np.nan)
        if pd.notna(epu_val) and epu is not None and not epu.empty and len(epu.dropna()) >= 24:
            p90 = float(epu.quantile(0.90))
            if epu_val >= p90:
                alert = RegimeAlert.WATCH
                reason_parts.append(f"Policy Uncertainty (USEPUINDXD) at 90th pctl ({epu_val:.0f} >= {p90:.0f})")
            details["epu"] = epu_val
            details["epu_p90"] = p90

        # DXY vs 2Y rolling 60d correlation
        if dxy is not None and not dxy.empty and dgs2 is not None and not dgs2.empty:
            common = dxy.dropna().index.intersection(dgs2.dropna().index)
            if len(common) >= 60:
                dxy_a = dxy.reindex(common).ffill().bfill()
                d2_a = dgs2.reindex(common).ffill().bfill()
                corr_60 = dxy_a.rolling(60, min_periods=40).corr(d2_a)
                last_corr = float(corr_60.iloc[-1]) if not corr_60.empty else np.nan
                details["dxy_2y_corr_60d"] = last_corr
                if pd.notna(last_corr) and last_corr < -0.2:
                    if alert == RegimeAlert.NONE:
                        alert = RegimeAlert.ALERT
                    reason_parts.append(f"USD-Rate divergence: Corr(DXY,2Y)_60d={last_corr:.2f} (< -0.2, trust issue)")

        if not reason_parts:
            reason_parts.append("No policy conflict signal")

        self.results["policy_conflict"] = RegimeModuleResult(
            name="Policy Conflict & USD Anchor",
            alert=alert,
            value=epu_val if pd.notna(epu_val) else details.get("dxy_2y_corr_60d"),
            threshold_used="USEPU>p90, Corr(DXY,2Y)_60d<-0.2",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["policy_conflict"]

    # ---------- 2. De-Dollarization (Central Bank Put) ----------
    def check_de_dollarization(self) -> RegimeModuleResult:
        """
        Non-price-sensitive Gold buying.
        - Gold/TLT ratio Z-Score > 2.0 -> Flight from Treasuries to Gold.
        - Gold and DXY both rising -> DE_DOLLARIZATION_RISK (Gold as supra-sovereign).
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        gold = self._fetch("", use_yahoo=True, yahoo_symbol="GC=F")
        tlt = self._fetch("", use_yahoo=True, yahoo_symbol="TLT")
        dxy = self._fetch("", use_yahoo=True, yahoo_symbol="DX-Y.NYB")
        if dxy is None or dxy.empty:
            dxy = self._fetch("DTWEXBGS")

        if gold is not None and not gold.empty and tlt is not None and not tlt.empty:
            common = gold.dropna().index.intersection(tlt.dropna().index)
            if len(common) >= 60:
                g = gold.reindex(common).ffill().bfill()
                t = tlt.reindex(common).ffill().bfill()
                ratio = (g / t).dropna()
                if len(ratio) >= 252:
                    z = self._rolling_zscore(ratio, 252)
                    z_last = float(z.iloc[-1]) if not z.empty and pd.notna(z.iloc[-1]) else np.nan
                    details["gold_tlt_ratio_z"] = z_last
                    if pd.notna(z_last) and z_last > 2.0:
                        alert = RegimeAlert.ALERT
                        reason_parts.append(f"Gold/TLT ratio Z-Score={z_last:.2f} (>2, flight to gold)")

        # Gold and DXY both rising (e.g. 20d change both positive)
        if gold is not None and not gold.empty and dxy is not None and not dxy.empty:
            common = gold.dropna().index.intersection(dxy.dropna().index)
            if len(common) >= 21:
                g = gold.reindex(common).ffill().bfill()
                d = dxy.reindex(common).ffill().bfill()
                g_chg = (g.iloc[-1] - g.iloc[-21]) / (g.iloc[-21] + 1e-8)
                d_chg = (d.iloc[-1] - d.iloc[-21]) / (d.iloc[-21] + 1e-8)
                if g_chg > 0.02 and d_chg > 0.02:
                    if alert == RegimeAlert.NONE:
                        alert = RegimeAlert.ALERT
                    reason_parts.append("Gold and DXY both rising (Gold as supra-sovereign) -> DE_DOLLARIZATION_RISK")
                details["gold_20d_chg"] = g_chg
                details["dxy_20d_chg"] = d_chg

        if not reason_parts:
            reason_parts.append("No de-dollarization signal")

        self.results["de_dollarization"] = RegimeModuleResult(
            name="De-Dollarization",
            alert=alert,
            value=details.get("gold_tlt_ratio_z"),
            threshold_used="Gold/TLT Z>2, Gold&DXY both up",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["de_dollarization"]

    # ---------- 3. Gold-Real Rate Divergence (Anti-Fiat) ----------
    def check_gold_real_rate_divergence(self) -> RegimeModuleResult:
        """
        Gold = credit hedge, not just inflation hedge.
        - Residual model: Gold = a + b*TIPS_10Y (ref 2010-2021). Alert if residual > 3 sigma.
        - Rolling corr(Gold, TIPS_10Y). Alert if > -0.2 (breakdown of negative relationship).
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        gold = self._fetch("", use_yahoo=True, yahoo_symbol="GC=F")
        dfii10 = self._fetch("DFII10")  # 10Y TIPS real rate

        if gold is None or gold.empty or dfii10 is None or dfii10.empty:
            self.results["gold_real_rate"] = RegimeModuleResult(
                name="Gold-Real Rate Divergence",
                alert=RegimeAlert.NONE,
                value=None,
                threshold_used="Residual>3σ, Corr>-0.2",
                reason="Insufficient data",
                details={},
            )
            return self.results["gold_real_rate"]

        gold_aligned = gold.reindex(dfii10.index).ffill().bfill()
        common = gold_aligned.dropna().index.intersection(dfii10.dropna().index)
        if len(common) < 60:
            self.results["gold_real_rate"] = RegimeModuleResult(
                name="Gold-Real Rate Divergence",
                alert=RegimeAlert.NONE,
                value=None,
                threshold_used="Residual>3σ, Corr>-0.2",
                reason="Insufficient history",
                details={},
            )
            return self.results["gold_real_rate"]

        g = gold_aligned.reindex(common).ffill().bfill()
        r = dfii10.reindex(common).ffill().bfill()
        # Ref period 2010-2021 for OLS
        ref_end = pd.Timestamp("2021-12-31")
        ref_start = pd.Timestamp("2010-01-01")
        ref_mask = (common >= ref_start) & (common <= ref_end)
        ref_dates = common[ref_mask]
        if len(ref_dates) < 24:
            ref_dates = common[-min(252, len(common)):]  # fallback: last ~1y
        g_ref = g.reindex(ref_dates).dropna()
        r_ref = r.reindex(ref_dates).dropna()
        common_ref = g_ref.index.intersection(r_ref.index)
        if len(common_ref) < 24:
            self.results["gold_real_rate"] = RegimeModuleResult(
                name="Gold-Real Rate Divergence",
                alert=RegimeAlert.NONE,
                value=None,
                threshold_used="Residual>3σ, Corr>-0.2",
                reason="Ref period too short",
                details={},
            )
            return self.results["gold_real_rate"]

        y_ref = g_ref.reindex(common_ref).ffill().bfill()
        x_ref = r_ref.reindex(common_ref).ffill().bfill()
        # OLS: Gold = alpha + beta * RealRate
        x_mat = np.column_stack([np.ones(len(x_ref)), x_ref.values])
        try:
            beta, _, _, _ = np.linalg.lstsq(x_mat, y_ref.values, rcond=None)
            alpha, slope = float(beta[0]), float(beta[1])
        except Exception:
            alpha, slope = 1800.0, -400.0  # fallback from user hint
        pred_all = alpha + slope * r
        residual = g - pred_all
        res_recent = residual.dropna().tail(504)
        if len(res_recent) >= 60:
            res_mean = res_recent.mean()
            res_std = res_recent.std()
            if res_std and res_std > 1:
                z_res = (float(residual.iloc[-1]) - res_mean) / res_std if pd.notna(residual.iloc[-1]) else np.nan
                details["residual_z"] = z_res
                details["alpha"] = alpha
                details["beta"] = slope
                if pd.notna(z_res) and z_res > 3.0:
                    alert = RegimeAlert.ALARM
                    reason_parts.append(f"Gold residual > 3σ (Z={z_res:.2f}, Sanction Premium) -> ANTI_FIAT_REGIME")

        # Rolling 60d corr(Gold, TIPS)
        if len(common) >= 60:
            g_ret = g.pct_change().dropna()
            r_chg = r.diff().dropna()
            common_ret = g_ret.index.intersection(r_chg.index)[-60:]
            if len(common_ret) >= 30:
                c = g_ret.reindex(common_ret).fillna(0).corr(r_chg.reindex(common_ret).fillna(0))
                corr_val = float(c) if isinstance(c, (int, float)) else (float(c.iloc[0]) if hasattr(c, "iloc") else np.nan)
                details["corr_gold_tips_60d"] = corr_val
                if pd.notna(corr_val) and corr_val > -0.2:
                    if alert != RegimeAlert.ALARM:
                        alert = RegimeAlert.ALERT
                    reason_parts.append(f"Gold-TIPS corr breakdown: {corr_val:.2f} (> -0.2)")

        if not reason_parts:
            reason_parts.append("Gold in line with real rate")

        self.results["gold_real_rate"] = RegimeModuleResult(
            name="Gold-Real Rate Divergence",
            alert=alert,
            value=details.get("residual_z") or details.get("corr_gold_tips_60d"),
            threshold_used="Residual>3σ, Corr(Gold,TIPS)>-0.2",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["gold_real_rate"]

    # ---------- 4. K-Shaped Recovery (Social Fracture) ----------
    def check_k_shaped(self) -> RegimeModuleResult:
        """
        AI Boom masks Real Economy Bust.
        - LNS14027662 (Bachelor's Unemp) > 3.0% -> Structural break.
        - Small biz divergence: IWM/SPY ratio < 60d MA (small caps underperforming vs large caps).
        Source: IWM/SPY Ratio (Proxy) instead of NFIB (unreliable/delayed).
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        white = self._fetch("LNS14027662")
        iwm = self._fetch("", use_yahoo=True, yahoo_symbol="IWM")
        spy = self._fetch("", use_yahoo=True, yahoo_symbol="SPY")

        w_val = self._last(white, np.nan)
        if pd.notna(w_val) and float(w_val) > 3.0:
            alert = RegimeAlert.ALERT
            reason_parts.append(f"White-collar unemp (LNS14027662) = {w_val:.2f}% (> 3%, structural break)")
        details["white_collar_unemp"] = w_val
        details["small_biz_source"] = "IWM/SPY Ratio (Proxy)"

        # IWM/SPY ratio and 60-day MA: ratio < MA => small caps underperforming (K-shaped)
        if iwm is not None and not iwm.empty and spy is not None and not spy.empty:
            common = iwm.index.intersection(spy.index)
            if len(common) >= 60:
                iwm_aligned = iwm.reindex(common).ffill().bfill()
                spy_aligned = spy.reindex(common).ffill().bfill()
                ratio = iwm_aligned / (spy_aligned + 1e-8)
                ratio_ma60 = ratio.rolling(60, min_periods=30).mean()
                r_last = self._last(ratio, np.nan)
                ma_last = self._last(ratio_ma60, np.nan)
                details["small_cap_ratio"] = r_last
                details["small_cap_ratio_ma60"] = ma_last
                underperform = pd.notna(r_last) and pd.notna(ma_last) and float(r_last) < float(ma_last)
                if underperform:
                    if alert == RegimeAlert.NONE:
                        alert = RegimeAlert.WATCH
                    reason_parts.append(
                        "K型复苏预警：小盘股(IWM)相对大盘股(SPY)跑输趋势 (IWM/SPY < 60日均线)"
                    )

        if not reason_parts:
            reason_parts.append("No K-shaped signal")

        self.results["k_shaped"] = RegimeModuleResult(
            name="K-Shaped Recovery",
            alert=alert,
            value=w_val if pd.notna(w_val) else details.get("small_cap_ratio"),
            threshold_used="LNS14027662>3%; IWM/SPY < MA60 (Small biz proxy)",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["k_shaped"]

    # ---------- 5. Bear Steepening (Bond Vigilante) ----------
    def check_bear_steepening(self) -> RegimeModuleResult:
        """
        Rates rising because of cuts (Fiscal Dominance).
        Term Premium: ACMTP10 -> THREEFYTP10 -> 10Y-2Y Slope (Proxy). > 0.75% -> Alert.
        Bear Steepener: Delta_2Y < -10bps AND Delta_10Y > 10bps -> FISCAL_DOMINANCE_ALARM.
        When using Proxy, slope widening > 10bps over window as confirmation.
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        dgs2 = self._fetch("DGS2")
        dgs10 = self._fetch("DGS10")
        tp_series, tp_source = self._get_term_premium_series_and_source()

        window = max(5, self._sustained_days)
        d2_now = self._last(dgs2, np.nan)
        d10_now = self._last(dgs10, np.nan)
        delta_2y = np.nan
        delta_10y = np.nan
        delta_acm = np.nan
        if dgs2 is not None and not dgs2.empty and len(dgs2) >= window + 1:
            delta_2y = d2_now - float(dgs2.iloc[-1 - window]) if pd.notna(dgs2.iloc[-1 - window]) else np.nan
        if dgs10 is not None and not dgs10.empty and len(dgs10) >= window + 1:
            delta_10y = d10_now - float(dgs10.iloc[-1 - window]) if pd.notna(dgs10.iloc[-1 - window]) else np.nan
        acm_now = self._last(tp_series, np.nan)
        if tp_series is not None and not tp_series.empty and len(tp_series) >= window + 1:
            delta_acm = acm_now - float(tp_series.iloc[-1 - window]) if pd.notna(tp_series.iloc[-1 - window]) else np.nan

        details["delta_2y_bps"] = delta_2y * 100 if pd.notna(delta_2y) else None
        details["delta_10y_bps"] = delta_10y * 100 if pd.notna(delta_10y) else None
        details["term_premium"] = acm_now
        details["term_premium_source"] = tp_source
        details["delta_acm_bps"] = delta_acm * 100 if pd.notna(delta_acm) else None

        # Term Premium > 0.75% (for Proxy, slope > 0.75% is "elevated")
        if pd.notna(acm_now) and float(acm_now) > 0.75:
            if alert == RegimeAlert.NONE:
                alert = RegimeAlert.WATCH
            reason_parts.append(f"Term Premium ({tp_source or 'N/A'}) = {acm_now:.2%} (> 0.75%)")

        # Bear Steepener: cutting (2Y down) and long end up
        is_cutting = pd.notna(delta_2y) and float(delta_2y) < -0.10  # -10 bps
        is_rising_long = pd.notna(delta_10y) and float(delta_10y) > 0.10  # +10 bps
        risk_premium_up = pd.notna(delta_acm) and float(delta_acm) > 0.05  # +5 bps (or slope widening > 10 bps)

        if is_cutting and is_rising_long:
            alert = RegimeAlert.ALARM
            reason_parts.append("FISCAL_DOMINANCE_ALARM: Bear Steepening (2Y down, 10Y up)")
        if risk_premium_up and alert == RegimeAlert.ALARM:
            reason_parts.append("(Term Premium rising as confirmation)")

        if not reason_parts:
            reason_parts.append("No bear steepening signal")

        thresh = f"TP Source: {tp_source or 'N/A'}; TP>0.75%, Δ2Y<-10bps & Δ10Y>10bps"
        self.results["bear_steepening"] = RegimeModuleResult(
            name="Bear Steepening",
            alert=alert,
            value=acm_now if pd.notna(acm_now) else (delta_10y if pd.notna(delta_10y) else None),
            threshold_used=thresh,
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["bear_steepening"]

    def _fetch_bizd_drawdown_50dma(self) -> Tuple[float, Optional[str]]:
        """BIZD vs 50DMA drawdown in % (negative = below MA). Fail-open: (np.nan, None) on error. NaN 严格传播."""
        try:
            bizd = base.fetch_bizd_safe()
            if bizd is None or bizd.empty or len(bizd) < 50:
                return (np.nan, None)
            ma50 = bizd.rolling(50, min_periods=30).mean()
            last_p = float(bizd.iloc[-1])
            last_ma = float(ma50.iloc[-1])
            if pd.isna(last_p) or pd.isna(last_ma) or last_ma <= 0:
                return (np.nan, None)
            pct_vs_50dma = (last_p / last_ma - 1.0) * 100.0
            last_date = str(bizd.index[-1].date()) if hasattr(bizd.index[-1], "date") else str(bizd.index[-1])
            return (pct_vs_50dma, last_date)
        except Exception:
            return (np.nan, None)

    # ---------- 6. Private Credit Liquidity Radar (Event-X) ----------
    def check_private_credit_liquidity_radar(self) -> RegimeModuleResult:
        """
        Watch: HY OAS > 4.5% OR STLFSI4 > 0 OR BIZD < -5% vs 50DMA
        Alert: HY OAS > 5.0% OR HY OAS 周度变动 > +50bp OR STLFSI4 > 1.0 OR BIZD < -10% vs 50DMA
        Alarm: HY OAS > 6.0% OR STLFSI4 > 1.5 OR DRTSCILM > 30% (且至少满足上述两项共振)
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}

        hy = self._fetch("BAMLH0A0HYM2")
        stlfsi4 = self._fetch("STLFSI4")
        drtscilm = self._fetch("DRTSCILM")  # 季频，大中型企业贷款标准；取最近有效观测，不向未来 ffill
        bizd_drawdown, bizd_date = self._fetch_bizd_drawdown_50dma()

        hy_last = self._last(hy, np.nan)
        stlfsi4_last = self._last(stlfsi4, np.nan)
        # DRTSCILM 季频：取最近有效值，并记录最后观测日供报告暴露“沿用最近已知值”
        drtsc_last = np.nan
        drtscilm_last_obs_date = None
        if drtscilm is not None and not drtscilm.empty:
            valid = drtscilm.dropna()
            if not valid.empty:
                drtsc_last = float(valid.iloc[-1])
                drtscilm_last_obs_date = str(valid.index[-1].date()) if hasattr(valid.index[-1], "date") else str(valid.index[-1])
        hy_weekly_chg_bp = np.nan
        if hy is not None and not hy.empty and len(hy) >= 6:
            hy_weekly_chg_bp = (hy_last - float(hy.iloc[-6])) * 100 if pd.notna(hy_last) and pd.notna(hy.iloc[-6]) else np.nan
        # 5 个交易日变化 (bp)，用于早期动量 Watch
        hy_oas_5d_bp_change = np.nan
        if hy is not None and not hy.empty and len(hy) >= 5:
            hy_oas_5d_bp_change = (hy_last - float(hy.iloc[-5])) * 100 if pd.notna(hy_last) and pd.notna(hy.iloc[-5]) else np.nan
        is_hy_momentum_watch = pd.notna(hy_oas_5d_bp_change) and float(hy_oas_5d_bp_change) > 30

        details["hy_oas_pct"] = hy_last
        details["hy_oas_last"] = hy_last
        details["hy_oas_5d_bp_change"] = float(hy_oas_5d_bp_change) if pd.notna(hy_oas_5d_bp_change) else None
        details["stlfsi4"] = stlfsi4_last
        details["stlfsi4_last"] = stlfsi4_last
        details["stlfsi_series_used"] = "STLFSI4"
        details["drtscilm_pct"] = drtsc_last
        details["drtscilm_last_obs_date"] = drtscilm_last_obs_date
        details["bizd_vs_50dma_pct"] = bizd_drawdown
        details["bizd_drawdown_50dma"] = bizd_drawdown
        details["bizd_last_date"] = bizd_date
        details["hy_oas_weekly_chg_bp"] = hy_weekly_chg_bp
        details["used_inputs"] = [k for k, v in [("hy_oas", hy_last), ("stlfsi4", stlfsi4_last), ("bizd", bizd_drawdown)] if pd.notna(v)]
        details["missing_inputs"] = [k for k, v in [("hy_oas", hy_last), ("stlfsi4", stlfsi4_last), ("bizd", bizd_drawdown)] if pd.isna(v)]

        # Alarm: HY > 6% OR STLFSI4 > 1.5 OR (DRTSCILM > 30% 且至少两项共振)
        alarm_count = 0
        if pd.notna(hy_last) and float(hy_last) > 6.0:
            alarm_count += 1
            reason_parts.append(f"HY OAS={hy_last:.2f}% (>6%)")
        if pd.notna(stlfsi4_last) and float(stlfsi4_last) > 1.5:
            alarm_count += 1
            reason_parts.append(f"STLFSI4={stlfsi4_last:.2f} (>1.5)")
        if pd.notna(drtsc_last) and float(drtsc_last) > 30.0:
            alarm_count += 1
            reason_parts.append(f"DRTSCILM={drtsc_last:.0f}% (>30%)")
        is_bizd_alarm = False if pd.isna(bizd_drawdown) else (float(bizd_drawdown) < -10.0)
        if is_bizd_alarm:
            alarm_count += 1
        if alarm_count >= 2:
            alert = RegimeAlert.ALARM
        # Alert
        if alert != RegimeAlert.ALARM:
            if (pd.notna(hy_last) and float(hy_last) > 5.0) or (pd.notna(hy_weekly_chg_bp) and float(hy_weekly_chg_bp) > 50):
                alert = RegimeAlert.ALERT
                if pd.notna(hy_last) and float(hy_last) > 5.0:
                    reason_parts.append(f"HY OAS={hy_last:.2f}% (>5%)")
                if pd.notna(hy_weekly_chg_bp) and float(hy_weekly_chg_bp) > 50:
                    reason_parts.append(f"HY OAS 周升 {hy_weekly_chg_bp:.0f}bp")
            if pd.notna(stlfsi4_last) and float(stlfsi4_last) > 1.0:
                alert = RegimeAlert.ALERT
                reason_parts.append(f"STLFSI4={stlfsi4_last:.2f} (>1)")
            is_bizd_alert = False if pd.isna(bizd_drawdown) else (float(bizd_drawdown) < -10.0)
            if is_bizd_alert:
                alert = RegimeAlert.ALERT
                reason_parts.append(f"BIZD vs 50DMA={bizd_drawdown:.1f}% (<-10%)")
        # Watch（含动量型早期触发：HY OAS 5D 变化 > +30bp）
        watch_triggered_by_momentum = False
        if alert == RegimeAlert.NONE:
            if pd.notna(hy_last) and float(hy_last) > 4.5:
                alert = RegimeAlert.WATCH
                reason_parts.append(f"HY OAS={hy_last:.2f}% (>4.5%)")
            if pd.notna(stlfsi4_last) and float(stlfsi4_last) > 0:
                alert = RegimeAlert.WATCH
                reason_parts.append(f"STLFSI4={stlfsi4_last:.2f} (>0)")
            is_bizd_watch = False if pd.isna(bizd_drawdown) else (float(bizd_drawdown) < -5.0)
            if is_bizd_watch:
                alert = RegimeAlert.WATCH
                reason_parts.append(f"BIZD vs 50DMA={bizd_drawdown:.1f}% (<-5%)")
            if is_hy_momentum_watch:
                alert = RegimeAlert.WATCH
                watch_triggered_by_momentum = True
                reason_parts.append(f"HY OAS 5D widening +{float(hy_oas_5d_bp_change):.0f}bp")
        details["watch_triggered_by_momentum"] = watch_triggered_by_momentum

        if not reason_parts:
            reason_parts.append("No private credit liquidity signal")

        self.results["private_credit_liquidity_radar"] = RegimeModuleResult(
            name="Private Credit Liquidity Radar",
            alert=alert,
            value=hy_last if pd.notna(hy_last) else details.get("bizd_vs_50dma_pct"),
            threshold_used="HY OAS / STLFSI4 / BIZD vs 50DMA / DRTSCILM",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["private_credit_liquidity_radar"]

    # ---------- 7. Geopolitics & Inflation Radar (Event-X) ----------
    def check_geopolitics_inflation_radar(self) -> RegimeModuleResult:
        """
        双腿确认（Dual-Leg Confirmation）：breakeven 可单独触发 WATCH，不可单独 ALERT/ALARM。
        Condition_Energy_Shock = (Brent > 90) OR (Brent YoY > 30)
        Condition_Inflation_Panic = breakeven_effective > 2.50
        Condition_Fear = VIX > 25
        WATCH: 任一条；ALERT: (Inflation AND (Energy OR Fear)) OR (Energy AND Fear)；ALARM: 三腿齐。
        details 含 condition_energy_shock, condition_inflation_panic, condition_fear, dual_leg_confirmed, upgrade_blocked_by_single_leg_rule。
        """
        reason_parts: List[str] = []
        alert = RegimeAlert.NONE
        details: Dict[str, Any] = {}
        alarm_note = ""

        brent = self._fetch("DCOILBRENTEU")
        t5yie = self._fetch("T5YIE")
        vix = self._fetch("VIXCLS")
        cpieng = self._fetch("CPIENGSL")
        # 5Y Breakeven：优先用实时/计算代理，避免 FRED T5YIE 滞后失真
        try:
            import event_x_breakeven as breakeven_module
            breakeven_proxy = breakeven_module.get_realtime_5y_breakeven_proxy_safe(base)
        except Exception:
            breakeven_proxy = {"breakeven_source_used": "NONE", "breakeven_last": None, "breakeven_last_date": None, "breakeven_is_stale": True, "breakeven_quality": "LOW"}
        breakeven_effective = breakeven_proxy.get("breakeven_last")
        if breakeven_effective is None or (isinstance(breakeven_effective, float) and pd.isna(breakeven_effective)):
            breakeven_effective = self._last(t5yie, np.nan)
        # CPI MoM: 在原始频率上算再取最近值（月频）
        cpieng_mom = np.nan
        cpiengsl_last_obs_date = None
        if cpieng is not None and not cpieng.empty and len(cpieng) >= 2:
            cpieng_mom = (float(cpieng.iloc[-1]) / float(cpieng.iloc[-2]) - 1.0) * 100.0 if pd.notna(cpieng.iloc[-1]) and pd.notna(cpieng.iloc[-2]) and float(cpieng.iloc[-2]) != 0 else np.nan
            cpiengsl_last_obs_date = str(cpieng.index[-1].date()) if hasattr(cpieng.index[-1], "date") else str(cpieng.index[-1])

        brent_last = self._last(brent, np.nan)
        t5yie_last = self._last(t5yie, np.nan)
        vix_last = self._last(vix, np.nan)
        brent_yoy = np.nan
        if brent is not None and not brent.empty and len(brent) >= 252:
            old = float(brent.iloc[-252])
            if pd.notna(old) and old != 0 and pd.notna(brent_last):
                brent_yoy = (brent_last / old - 1.0) * 100.0

        # T5YIE 持续高位：有 5 日序列时用 min(last 5d) > 2.50，否则用 effective 单点
        t5yie_tail = t5yie.dropna().tail(5) if t5yie is not None and not t5yie.empty else pd.Series(dtype=float)
        if len(t5yie_tail) >= 5:
            is_t5yie_alert_sustained = float(t5yie_tail.min()) > 2.50
        else:
            is_t5yie_alert_sustained = pd.notna(breakeven_effective) and float(breakeven_effective) > 2.50

        corr_20, gold_source, corr_details = base.get_gold_spx_rolling_corr_20d()
        # 统一键名并增加可观测性：last / used_inputs / missing_inputs / 调试信息
        details["brent"] = brent_last
        details["brent_last"] = brent_last
        details["brent_yoy"] = brent_yoy
        details["brent_yoy_pct"] = brent_yoy
        details["t5yie_pct"] = t5yie_last
        details["t5yie_last"] = t5yie_last
        details["breakeven_source_used"] = breakeven_proxy.get("breakeven_source_used", "NONE")
        details["breakeven_last"] = breakeven_proxy.get("breakeven_last")
        details["breakeven_last_date"] = breakeven_proxy.get("breakeven_last_date")
        details["breakeven_is_stale"] = breakeven_proxy.get("breakeven_is_stale", True)
        details["breakeven_quality"] = breakeven_proxy.get("breakeven_quality", "LOW")
        details["breakeven_effective_last"] = float(breakeven_effective) if pd.notna(breakeven_effective) else None
        details["t5yie_sustained_alert"] = bool(is_t5yie_alert_sustained)
        details["vix"] = vix_last
        details["vix_last"] = vix_last
        details["cpieng_mom_pct"] = cpieng_mom
        details["cpi_energy_mom"] = cpieng_mom
        details["cpiengsl_last_obs_date"] = cpiengsl_last_obs_date
        details["gold_spx_corr_20d"] = corr_20
        details["gold_source"] = gold_source
        if t5yie is not None and not t5yie.empty and not t5yie.dropna().empty:
            details["t5yie_last_obs_date"] = str(t5yie.dropna().index[-1].date()) if hasattr(t5yie.dropna().index[-1], "date") else str(t5yie.dropna().index[-1])
        else:
            details["t5yie_last_obs_date"] = None
        # used_inputs / missing_inputs 与调试信息（用于报告与 completeness）
        used_inputs: List[str] = []
        missing_inputs: List[str] = []
        def _last_valid_date(ts: Optional[pd.Series]) -> Optional[str]:
            if ts is None or ts.empty or ts.dropna().empty:
                return None
            idx = ts.dropna().index[-1]
            return str(idx.date()) if hasattr(idx, "date") else str(idx)
        breakeven_used = pd.notna(breakeven_effective)
        for key, last_val, ts in [("brent", brent_last, brent), ("breakeven", breakeven_effective if breakeven_used else np.nan, t5yie), ("vix", vix_last, vix)]:
            if pd.notna(last_val):
                used_inputs.append(key)
                details[f"{key}_last_valid_date"] = _last_valid_date(ts)
                details[f"{key}_data_source"] = "FRED"
                details[f"{key}_whether_used_in_radar"] = True
            else:
                missing_inputs.append(key)
                details[f"{key}_whether_used_in_radar"] = False
        details["used_inputs"] = used_inputs
        details["missing_inputs"] = missing_inputs
        if breakeven_used:
            details["breakeven_last_valid_date"] = breakeven_proxy.get("breakeven_last_date") or details.get("breakeven_last_valid_date")
            details["breakeven_data_source"] = breakeven_proxy.get("breakeven_source_used") or "FRED"

        # 双腿确认（Dual-Leg Confirmation）：breakeven 可单独 WATCH，不可单独 ALERT/ALARM
        condition_energy_shock = (pd.notna(brent_last) and float(brent_last) > 90) or (pd.notna(brent_yoy) and float(brent_yoy) > 30)
        condition_inflation_panic = pd.notna(breakeven_effective) and float(breakeven_effective) > 2.50
        condition_fear = pd.notna(vix_last) and float(vix_last) > 25
        details["condition_energy_shock"] = condition_energy_shock
        details["condition_inflation_panic"] = condition_inflation_panic
        details["condition_fear"] = condition_fear

        dual_leg_confirmed = False
        upgrade_blocked_by_single_leg_rule = False
        # ALARM: 三腿齐
        if condition_energy_shock and condition_inflation_panic and condition_fear:
            alert = RegimeAlert.ALARM
            dual_leg_confirmed = True
            reason_parts.append("Full-chain alarm: Energy + Inflation + Fear confirmed.")
            alarm_note = "Rate Cuts Repricing Risk High"
        # ALERT: (Inflation AND (Energy OR Fear)) OR (Energy AND Fear)
        elif (condition_inflation_panic and (condition_energy_shock or condition_fear)) or (condition_energy_shock and condition_fear):
            alert = RegimeAlert.ALERT
            dual_leg_confirmed = True
            reason_parts.append("Dual-leg confirmed alert.")
        else:
            if (condition_inflation_panic and not condition_energy_shock and not condition_fear) or (condition_energy_shock and not condition_inflation_panic and not condition_fear) or (condition_fear and not condition_inflation_panic and not condition_energy_shock):
                upgrade_blocked_by_single_leg_rule = True
        details["dual_leg_confirmed"] = dual_leg_confirmed
        details["upgrade_blocked_by_single_leg_rule"] = upgrade_blocked_by_single_leg_rule

        # WATCH: 任一条即可（breakeven 单独可 WATCH）
        if alert == RegimeAlert.NONE and (condition_energy_shock or condition_inflation_panic or condition_fear):
            alert = RegimeAlert.WATCH
            if condition_inflation_panic and not condition_energy_shock and not condition_fear:
                reason_parts.append("Breakeven-led watch (inflation leg only; no upgrade without second leg).")
            else:
                if condition_energy_shock:
                    reason_parts.append(f"Brent={brent_last:.1f} or YoY={brent_yoy:.1f}% (energy shock leg).")
                if condition_inflation_panic:
                    reason_parts.append(f"Breakeven={breakeven_effective:.2f}% (>2.5%).")
                if condition_fear:
                    reason_parts.append(f"VIX={vix_last:.1f} (>25).")

        if not reason_parts:
            reason_parts.append("No geopolitics/inflation signal")
        if alarm_note:
            reason_parts.append(alarm_note)

        self.results["geopolitics_inflation_radar"] = RegimeModuleResult(
            name="Geopolitics & Inflation Radar",
            alert=alert,
            value=brent_last if pd.notna(brent_last) else (t5yie_last if pd.notna(t5yie_last) else vix_last),
            threshold_used="Brent / T5YIE / VIX / Gold-SPX corr / CPI Energy MoM",
            reason="; ".join(reason_parts),
            details=details,
        )
        return self.results["geopolitics_inflation_radar"]

    def run_all_checks(self) -> Dict[str, RegimeModuleResult]:
        self.check_policy_conflict()
        self.check_de_dollarization()
        self.check_gold_real_rate_divergence()
        self.check_k_shaped()
        self.check_bear_steepening()
        self.check_private_credit_liquidity_radar()
        self.check_geopolitics_inflation_radar()
        return self.results

    def has_any_alert(self) -> bool:
        """True if any module is WATCH / ALERT / ALARM (for conditional report section)."""
        for r in self.results.values():
            if r.alert in (RegimeAlert.WATCH, RegimeAlert.ALERT, RegimeAlert.ALARM):
                return True
        return False


def build_regime_alerts_md(results: Dict[str, RegimeModuleResult]) -> str:
    """Markdown for 'Structural & Regime Risks' section. Only meaningful when has_any_alert."""
    lines = [
        "## ⚠️ Structural & Regime Risks (Regime Layer)",
        "",
        "| Module | Alert | Value | Reason |",
        "|--------|-------|-------|--------|",
    ]
    for key, r in results.items():
        v = r.value
        val_str = f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else str(v) if v is not None else "-"
        lines.append(f"| {r.name} | {r.alert} | {val_str} | {r.reason or '-'} |")
    return "\n".join(lines) + "\n"


def build_regime_alerts_html(results: Dict[str, RegimeModuleResult]) -> str:
    """HTML snippet for Regime Alerts panel (only when alerts)."""
    def alert_color(a: str) -> str:
        if a == RegimeAlert.ALARM:
            return "#c0392b"
        if a == RegimeAlert.ALERT:
            return "#e67e22"
        if a == RegimeAlert.WATCH:
            return "#f1c40f"
        return "#95a5a6"

    rows = []
    for key, r in results.items():
        v = r.value
        val_str = f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else str(v) if v is not None else "-"
        color = alert_color(r.alert)
        rows.append(
            f"<tr><td>{r.name}</td><td style='background:{color};color:#fff;'>{r.alert}</td>"
            f"<td>{val_str}</td><td>{r.reason or '-'}</td></tr>"
        )
    table_body = "\n".join(rows)
    return f"""
<div class="regime-alerts" style="margin:1em 0; padding:1em; border:2px solid #e74c3c; border-radius:8px; background:#fdf2f2;">
  <h3>⚠️ Structural & Regime Risks (Regime Layer)</h3>
  <p>These alerts do <em>not</em> affect the base risk score. They indicate institutional / structural stress.</p>
  <table style="width:100%; border-collapse:collapse;">
    <thead><tr><th>Module</th><th>Alert</th><th>Value</th><th>Reason</th></tr></thead>
    <tbody>{table_body}</tbody>
  </table>
</div>
"""


if __name__ == "__main__":
    mon = StructuralRiskMonitor()
    mon.run_all_checks()
    print("Has alert:", mon.has_any_alert())
    print(build_regime_alerts_md(mon.results))
