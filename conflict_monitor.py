#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Conflict & Divergence Monitor (Policy Incoherence Era)
Detects stress from "Weak Dollar" Fiscal vs "Tight Money" Monetary conflict.
5 modules: Policy Incoherence, Gold Residual, Japan Contagion, K-Shaped Labor, Plumbing & Liquidity.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple

import numpy as np
import pandas as pd

import crisis_monitor as base

BASE = pathlib.Path(__file__).parent

# Status for panel display
class ConflictStatus:
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class ConflictModuleResult:
    name: str
    status: str
    value: Optional[float] = None
    threshold_used: Optional[str] = None
    reason: str = ""


class ConflictMonitor:
    """Conflict & Divergence: Policy Incoherence, Gold Residual, Japan, K-Shaped, Plumbing."""

    def __init__(self, base_dir: Optional[pathlib.Path] = None):
        self.base_dir = base_dir or BASE
        self.results: Dict[str, ConflictModuleResult] = {}
        self._series_cache: Dict[str, pd.Series] = {}

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

    # ---------- 1. Policy Incoherence (Bear Steepening Alarm) ----------
    def check_policy_incoherence(self) -> ConflictModuleResult:
        """
        Trigger: Fed is cutting (2Y yield dropping 3m) BUT Term Premium is rising.
        Term Premium: ACMTP10 -> THREEFYTP10 -> 10Y-2Y Slope (Proxy).
        """
        reason_parts: List[str] = []
        status = ConflictStatus.NORMAL

        dgs2 = self._fetch("DGS2")
        tp_series, tp_source = self._get_term_premium_series_and_source()

        # 3-month change in 2Y: need ~63 trading days for daily data
        d2_now = self._last(dgs2, np.nan)
        if dgs2 is not None and not dgs2.empty and len(dgs2) >= 64:
            d2_3m_ago = float(dgs2.iloc[-64])
            dgs2_chg_3m = d2_now - d2_3m_ago if pd.notna(d2_3m_ago) else np.nan
        else:
            dgs2_chg_3m = np.nan

        term_premium = self._last(tp_series, np.nan)

        fed_cutting = pd.notna(dgs2_chg_3m) and float(dgs2_chg_3m) < -0.25
        term_premium_spike = pd.notna(term_premium) and float(term_premium) > 0.75

        if fed_cutting and term_premium_spike:
            status = ConflictStatus.CRITICAL
            reason_parts.append(
                f"CRITICAL: POLICY_FAILURE_BEAR_STEEPENING (Fed cutting + Term Premium spike; Source: {tp_source or 'N/A'})"
            )
        elif term_premium_spike:
            status = ConflictStatus.WARNING
            reason_parts.append(f"Term Premium elevated ({term_premium:.2%} > 0.75%, Source: {tp_source or 'N/A'})")
        elif fed_cutting:
            reason_parts.append(f"2Y yield 3m change = {dgs2_chg_3m:.2f}% (Fed cutting)")

        if not reason_parts:
            reason_parts.append("No policy incoherence signal")

        thresh = f"DGS2_chg_3m<-0.25%, TP>0.75% (TP Source: {tp_source or 'N/A'})"
        self.results["policy_incoherence"] = ConflictModuleResult(
            name="Policy Incoherence (Bear Steepening)",
            status=status,
            value=term_premium if pd.notna(term_premium) else dgs2_chg_3m,
            threshold_used=thresh,
            reason="; ".join(reason_parts),
        )
        return self.results["policy_incoherence"]

    # ---------- 2. Gold Residual (Anti-Fiat / Sanction Premium) ----------
    def check_gold_residual(self) -> ConflictModuleResult:
        """
        Model: Gold ≈ 1800 - 400 * RealRate(%). Residual = Current_Gold - Predicted.
        Z-Score of residual (2-year rolling). Critical if Z > 3.
        SPX/Gold Critical < 1.45.
        """
        reason_parts: List[str] = []
        status = ConflictStatus.NORMAL

        gold = self._fetch("", use_yahoo=True, yahoo_symbol="GC=F")
        dfii10 = self._fetch("DFII10")
        spx = self._fetch("", use_yahoo=True, yahoo_symbol="^GSPC")

        gold_val = self._last(gold, np.nan)
        real_rate = self._last(dfii10, np.nan)
        spx_val = self._last(spx, np.nan)

        # Predicted gold (Pre-Sanction era approx): 1800 - 400 * RealRate(%)
        if pd.notna(real_rate):
            predicted = 1800.0 - 400.0 * float(real_rate)
            residual = gold_val - predicted if pd.notna(gold_val) else np.nan
        else:
            residual = np.nan

        # 2-year rolling Z-score of residual (align gold to dfii10 dates, compute residual series)
        z_residual = np.nan
        if gold is not None and not gold.empty and dfii10 is not None and not dfii10.empty:
            gold_aligned = gold.reindex(dfii10.index).ffill().bfill()
            common = gold_aligned.dropna().index.intersection(dfii10.dropna().index)
            if len(common) >= 60:
                r = dfii10.reindex(common).ffill().bfill()
                pred_series = 1800.0 - 400.0 * r
                res_series = gold_aligned.reindex(common).ffill().bfill() - pred_series
                window = min(504, len(res_series) - 1)  # 2Y ~504 trading days
                if window >= 60:
                    roll_mean = res_series.rolling(window, min_periods=60).mean()
                    roll_std = res_series.rolling(window, min_periods=60).std()
                    z_series = (res_series - roll_mean) / (roll_std + 1e-8)
                    z_residual = float(z_series.iloc[-1]) if not z_series.empty else np.nan

        if pd.notna(z_residual) and z_residual > 3.0:
            status = ConflictStatus.CRITICAL
            reason_parts.append(f"Gold residual Z-Score={z_residual:.2f} (non-market/CB driven)")
        elif pd.notna(z_residual) and z_residual > 2.0:
            if status != ConflictStatus.CRITICAL:
                status = ConflictStatus.WARNING
            reason_parts.append(f"Gold residual Z-Score={z_residual:.2f}")

        if pd.notna(spx_val) and pd.notna(gold_val) and gold_val > 0:
            ratio = spx_val / gold_val
            if ratio < 1.45:
                status = ConflictStatus.CRITICAL
                reason_parts.append(f"SPX/Gold={ratio:.2f} < 1.45 (systemic避险)")
            elif ratio < 1.70:
                if status != ConflictStatus.CRITICAL:
                    status = ConflictStatus.WARNING
                reason_parts.append(f"SPX/Gold={ratio:.2f}")

        if not reason_parts:
            reason_parts.append("Gold in line with real rate; SPX/Gold normal")

        self.results["gold_residual"] = ConflictModuleResult(
            name="Gold Residual (Anti-Fiat)",
            status=status,
            value=gold_val if pd.notna(gold_val) else (z_residual if pd.notna(z_residual) else None),
            threshold_used="Residual Z>3, SPX/Gold<1.45",
            reason="; ".join(reason_parts),
        )
        return self.results["gold_residual"]

    # ---------- 3. Japan Contagion (Volatility Spillover) ----------
    def check_japan_contagion(self) -> ConflictModuleResult:
        """
        JGB 10Y > 1.8% Warning; USD/JPY > 155 Intervention.
        Vol Beta: 20d correlation JGB yield chg vs UST 10Y chg; if Corr > 0.5 trigger Contagion.
        JGB: FRED IRLTLT01JPM156N (monthly) or Yahoo ^JP10Y / JP10Y.BD.
        """
        reason_parts: List[str] = []
        status = ConflictStatus.NORMAL

        usdjpy = self._fetch("DEXJPUS")
        if usdjpy is None or usdjpy.empty:
            usdjpy = self._fetch("", use_yahoo=True, yahoo_symbol="JPY=X")
        jgb10 = self._fetch("IRLTLT01JPM156N")
        if jgb10 is None or jgb10.empty:
            jgb10 = self._fetch("IRSTCI01JPM156N")
        if jgb10 is None or jgb10.empty:
            jgb10 = self._fetch("", use_yahoo=True, yahoo_symbol="^JP10Y")
        dgs10 = self._fetch("DGS10")

        usdjpy_val = self._last(usdjpy, np.nan)
        jgb10_val = self._last(jgb10, np.nan)

        if pd.notna(usdjpy_val) and float(usdjpy_val) > 155.0:
            status = ConflictStatus.CRITICAL
            reason_parts.append(f"USD/JPY={usdjpy_val:.1f} (Intervention zone)")

        if pd.notna(jgb10_val):
            if float(jgb10_val) > 1.8:
                if status != ConflictStatus.CRITICAL:
                    status = ConflictStatus.WARNING
                reason_parts.append(f"JGB 10Y={jgb10_val:.2f}% > 1.8%")

        # Vol Beta: rolling 20d corr(JGB_chg, UST10Y_chg) - need daily JGB; if we only have monthly, skip or use USD/JPY vol
        if (
            jgb10 is not None and not jgb10.empty and dgs10 is not None and not dgs10.empty
            and len(jgb10) >= 21 and len(dgs10) >= 21
        ):
            jgb_daily = jgb10.reindex(dgs10.index).ffill()
            common = jgb_daily.dropna().index.intersection(dgs10.dropna().index)
            if len(common) >= 21:
                jgb_c = jgb_daily.reindex(common).ffill()
                ust_c = dgs10.reindex(common).ffill()
                jgb_chg = jgb_c.diff().dropna()
                ust_chg = ust_c.diff().dropna()
                common_ret = jgb_chg.index.intersection(ust_chg.index)[-20:]
                if len(common_ret) >= 10:
                    corr = jgb_chg.reindex(common_ret).fillna(0).corr(ust_chg.reindex(common_ret).fillna(0))
                    if isinstance(corr, (int, float)) or (hasattr(corr, "iloc") and len(corr) > 0):
                        c_val = float(corr) if isinstance(corr, (int, float)) else float(corr.iloc[0])
                        if pd.notna(c_val) and c_val > 0.5:
                            if status != ConflictStatus.CRITICAL:
                                status = ConflictStatus.WARNING
                            reason_parts.append(f"JGB-UST yield chg corr={c_val:.2f} (Contagion)")

        if not reason_parts:
            reason_parts.append("Japan stress within normal")

        self.results["japan_contagion"] = ConflictModuleResult(
            name="Japan Contagion",
            status=status,
            value=usdjpy_val if pd.notna(usdjpy_val) else jgb10_val,
            threshold_used="JGB10Y>1.8%, USD/JPY>155, Corr>0.5",
            reason="; ".join(reason_parts),
        )
        return self.results["japan_contagion"]

    # ---------- 4. K-Shaped Labor (AI Displacement Spread) ----------
    def check_k_shaped_labor(self) -> ConflictModuleResult:
        """
        White collar: LNS14027662 (Bachelor+). Blue collar: LNS14027660 (High School).
        Trigger: White collar unemp rising >0.3% YoY while blue stable -> K_SHAPED_recession.
        """
        reason_parts: List[str] = []
        status = ConflictStatus.NORMAL

        white = self._fetch("LNS14027662")
        blue = self._fetch("LNS14027660")

        w_last = self._last(white, np.nan)
        b_last = self._last(blue, np.nan)

        white_yoy = np.nan
        blue_yoy = np.nan
        if white is not None and not white.empty and len(white) >= 13:
            white_yoy = (float(white.iloc[-1]) - float(white.iloc[-13])) if pd.notna(white.iloc[-1]) and pd.notna(white.iloc[-13]) else np.nan
        if blue is not None and not blue.empty and len(blue) >= 13:
            blue_yoy = (float(blue.iloc[-1]) - float(blue.iloc[-13])) if pd.notna(blue.iloc[-1]) and pd.notna(blue.iloc[-13]) else np.nan

        # White rising >0.3 pp YoY, blue stable (e.g. blue_yoy < 0.2 or not rising much)
        white_rising = pd.notna(white_yoy) and float(white_yoy) > 0.3
        blue_stable = pd.notna(blue_yoy) and float(blue_yoy) < 0.2

        if white_rising and blue_stable:
            status = ConflictStatus.CRITICAL
            reason_parts.append("K_SHAPED_recession: White-collar unemp rising >0.3% YoY, blue-collar stable")
        elif white_rising:
            status = ConflictStatus.WARNING
            reason_parts.append(f"White-collar unemp YoY chg = {white_yoy:.2f}%")

        spread = (w_last - b_last) if (pd.notna(w_last) and pd.notna(b_last)) else np.nan
        if not reason_parts:
            reason_parts.append("Labor spread normal")

        self.results["k_shaped_labor"] = ConflictModuleResult(
            name="K-Shaped Labor (AI Displacement)",
            status=status,
            value=spread if pd.notna(spread) else w_last,
            threshold_used="White YoY>0.3%, Blue stable",
            reason="; ".join(reason_parts),
        )
        return self.results["k_shaped_labor"]

    # ---------- 5. Plumbing & Liquidity (Freeze Detector) ----------
    def check_plumbing_liquidity(self) -> ConflictModuleResult:
        """
        FRA-OIS proxy: SOFR - DTB3. Warning > 0.40% (40 bps).
        Sovereign/inflation: T5YIFR (5y5y breakeven). Critical > 2.75%.
        """
        reason_parts: List[str] = []
        status = ConflictStatus.NORMAL

        sofr = self._fetch("SOFR")
        dtb3 = self._fetch("DTB3")
        t5yifr = self._fetch("T5YIFR")

        sofr_val = self._last(sofr, np.nan)
        dtb3_val = self._last(dtb3, np.nan)
        t5yifr_val = self._last(t5yifr, np.nan)

        if pd.notna(sofr_val) and pd.notna(dtb3_val):
            spread = float(sofr_val) - float(dtb3_val)
            if spread > 0.40:
                status = ConflictStatus.WARNING
                reason_parts.append(f"SOFR-DTB3={spread:.2%} > 40bps (plumbing stress)")

        if pd.notna(t5yifr_val) and float(t5yifr_val) > 2.75:
            status = ConflictStatus.CRITICAL
            reason_parts.append(f"5y5y Breakeven={t5yifr_val:.2f}% > 2.75% (unanchored/default risk)")

        if not reason_parts:
            reason_parts.append("Plumbing and inflation expectations normal")

        self.results["plumbing_liquidity"] = ConflictModuleResult(
            name="Plumbing & Liquidity",
            status=status,
            value=(float(sofr_val) - float(dtb3_val)) if (pd.notna(sofr_val) and pd.notna(dtb3_val)) else t5yifr_val,
            threshold_used="SOFR-DTB3>0.40%, T5YIFR>2.75%",
            reason="; ".join(reason_parts),
        )
        return self.results["plumbing_liquidity"]

    def run_all_checks(self) -> Dict[str, ConflictModuleResult]:
        self.check_policy_incoherence()
        self.check_gold_residual()
        self.check_japan_contagion()
        self.check_k_shaped_labor()
        self.check_plumbing_liquidity()
        return self.results


def build_conflict_panel_md(results: Dict[str, ConflictModuleResult]) -> str:
    """Markdown for Conflict & Divergence panel."""
    lines = [
        "## Conflict & Divergence (Policy Incoherence)",
        "",
        "| Module | Status | Value | Reason |",
        "|--------|--------|-------|--------|",
    ]
    for key, r in results.items():
        v = r.value
        val_str = f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else str(v) if v is not None else "-"
        lines.append(f"| {r.name} | {r.status} | {val_str} | {r.reason or '-'} |")
    return "\n".join(lines) + "\n"


def build_conflict_panel_html(results: Dict[str, ConflictModuleResult]) -> str:
    """HTML snippet for Conflict & Divergence panel (Red/Yellow/Green)."""
    def status_color(s: str) -> str:
        if s == ConflictStatus.CRITICAL:
            return "#c0392b"
        if s == ConflictStatus.WARNING:
            return "#f39c12"
        return "#27ae60"

    rows = []
    for key, r in results.items():
        v = r.value
        val_str = f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else str(v) if v is not None else "-"
        color = status_color(r.status)
        rows.append(
            f"<tr><td>{r.name}</td><td style='background:{color};color:#fff;'>{r.status}</td>"
            f"<td>{val_str}</td><td>{r.reason or '-'}</td></tr>"
        )
    table_body = "\n".join(rows)
    return f"""
<div class="conflict-panel" style="margin:1em 0; padding:1em; border:1px solid #34495e; border-radius:8px;">
  <h3>Conflict & Divergence</h3>
  <p>Policy Incoherence / Weak Dollar vs Tight Money</p>
  <table style="width:100%; border-collapse:collapse;">
    <thead><tr><th>Module</th><th>Status</th><th>Value</th><th>Reason</th></tr></thead>
    <tbody>{table_body}</tbody>
  </table>
</div>
"""


if __name__ == "__main__":
    mon = ConflictMonitor()
    mon.run_all_checks()
    print(build_conflict_panel_md(mon.results))
