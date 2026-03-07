#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regime-Aware Crisis Monitor (Fiscal Dominance Era)
基于研报 "Risk Logic Optimization in the Era of Fiscal Dominance" 的阈值与逻辑模块。
- 模块 A: Fiscal Dominance (Bear Steepening) [Policy Incoherence: 2Y fall + 10Y rise + ACMTP10 rise]
- 模块 B: Japan Contagion (Liquidity Fuse)
- 模块 C: Anti-Fiat & Gold (De-Dollarization: SPX/Gold + Gold-DXY reserves proxy)
- 模块 D: K-Shaped Recovery (Bachelor vs Headline unemp)
- 模块 E: Plumbing Stress (SOFR - 3M T-Bill > 40bps)
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

# 复用现有 crisis_monitor 的路径与数据获取
import crisis_monitor as base

BASE = pathlib.Path(__file__).parent


# ---------- 状态枚举 ----------
class RegimeStatus:
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    STABLE = "STABLE"
    HEDGE = "HEDGE"
    ANTI_FIAT = "ANTI_FIAT"
    REGIME_BREAK = "REGIME_BREAK_CONFIRMED"


@dataclass
class ModuleResult:
    """单模块评估结果"""
    name: str
    status: str
    value: Optional[float] = None
    threshold_used: Optional[str] = None
    reason: str = ""


# ---------- CrisisMonitor 主类 ----------
class CrisisMonitor:
    """Regime-Aware 危机监控器：管理状态并执行各专项检查。"""

    def __init__(self, base_dir: Optional[pathlib.Path] = None):
        self.base_dir = base_dir or BASE
        self.results: Dict[str, ModuleResult] = {}
        self._series_cache: Dict[str, pd.Series] = {}

    def _fetch(self, series_id: str = "", use_yahoo: bool = False, yahoo_symbol: Optional[str] = None) -> pd.Series:
        """统一获取序列：优先缓存，再 FRED / Yahoo。"""
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

    def _prev(self, ts: pd.Series, default: float = np.nan) -> float:
        if ts is None or len(ts) < 2:
            return default
        v = ts.iloc[-2]
        return float(v) if pd.notna(v) else default

    def _change_over(self, ts: pd.Series, days: int = 10) -> Optional[float]:
        """Return (last - value days ago) or None if insufficient data."""
        if ts is None or ts.empty or len(ts) < days + 1:
            return None
        a = float(ts.iloc[-1])
        b = float(ts.iloc[-1 - days])
        return (a - b) if pd.notna(a) and pd.notna(b) else None

    def _get_term_premium_series_and_source(self) -> Tuple[pd.Series, str]:
        """
        Primary: ACMTP10; Fallback 1: THREEFYTP10 (Kim-Wright); Fallback 2: DGS10 - DGS2 (Slope proxy).
        Returns (series, source_label).
        """
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

    # ---------- Module A: Fiscal Dominance (Bear Steepening) ----------
    def check_fiscal_dominance(self) -> ModuleResult:
        """
        Policy Incoherence: Bear Steepening Detector.
        Trigger alert ONLY IF all three:
        - 2Y Yield Falling (Fed cutting cycle)
        - 10Y Yield Rising
        - Term Premium Rising (ACMTP10 -> THREEFYTP10 -> 10Y-2Y Slope proxy)
        Also retain T5YIFR and 10Y-2Y spread as context.
        """
        reason_parts: List[str] = []
        status = RegimeStatus.INACTIVE
        window = 10  # ~2 weeks for trend

        dgs10 = self._fetch("DGS10")
        dgs2 = self._fetch("DGS2")
        tp_series, tp_source = self._get_term_premium_series_and_source()

        # Changes over window (in percentage points; 0.10 = 10 bps)
        delta_2y = self._change_over(dgs2, window)
        delta_10y = self._change_over(dgs10, window)
        delta_acm = self._change_over(tp_series, window)

        fed_cutting = delta_2y is not None and float(delta_2y) < -0.10   # 2Y falling ≥10 bps
        ten_y_rising = delta_10y is not None and float(delta_10y) > 0.10  # 10Y rising ≥10 bps
        # When using proxy (Slope), widening > 10 bps over window confirms bear steepening
        term_premium_rising = delta_acm is not None and float(delta_acm) > 0.05  # ≥5 bps (ACM/KW) or slope widening

        # Bear Steepening: ONLY if all three (Policy Conflict)
        if fed_cutting and ten_y_rising and term_premium_rising:
            status = RegimeStatus.ACTIVE
            label = f" (Source: {tp_source})" if tp_source else ""
            reason_parts.append(
                f"Bear Steepening (Policy Conflict): 2Y falling + 10Y rising + Term Premium rising{label}"
            )
        elif fed_cutting and ten_y_rising:
            reason_parts.append(f"2Y falling & 10Y rising (Term Premium not rising or missing; tried {tp_source or 'N/A'})")

        # 1) T5YIFR 5y5y breakeven (unchanged)
        t5yifr = self._fetch("T5YIFR")
        t5yifr_val = self._last(t5yifr, np.nan)
        if pd.notna(t5yifr_val):
            if t5yifr_val > 2.50:
                status = RegimeStatus.CRITICAL if status != RegimeStatus.ACTIVE else status
                reason_parts.append(f"5y5y通胀预期未锚定 ({t5yifr_val:.2f}% > 2.50%)")
            elif t5yifr_val > 2.25 and status == RegimeStatus.INACTIVE:
                status = RegimeStatus.WARNING
                reason_parts.append(f"5y5y通胀偏高 ({t5yifr_val:.2f}%)")

        # 2) Term Premium level (10Y-2Y spread) as context
        term_proxy = self._last(dgs10) - self._last(dgs2)
        if pd.notna(term_proxy):
            if term_proxy > 1.00:
                if status not in (RegimeStatus.ACTIVE, RegimeStatus.CRITICAL):
                    status = RegimeStatus.CRITICAL
                reason_parts.append(f"10Y-2Y利差扩大 ({term_proxy:.2f}% > 1.00%)")
            elif term_proxy > 0.75 and status == RegimeStatus.INACTIVE:
                status = RegimeStatus.WARNING
                reason_parts.append(f"10Y-2Y利差偏高 ({term_proxy:.2f}%)")

        reason = "; ".join(reason_parts) if reason_parts else "无异常"
        if status == RegimeStatus.INACTIVE and reason_parts:
            status = RegimeStatus.WARNING
        thresh = f"Bear Steepener: Δ2Y<−10bps & Δ10Y>10bps & ΔTP>5bps (TP Source: {tp_source or 'N/A'}); T5YIFR>2.5%; 10Y-2Y>1%"
        self.results["fiscal_dominance"] = ModuleResult(
            name="Fiscal Dominance",
            status=status,
            value=t5yifr_val if pd.notna(t5yifr_val) else term_proxy,
            threshold_used=thresh,
            reason=reason,
        )
        return self.results["fiscal_dominance"]

    # ---------- Module B: Japan Contagion ----------
    def check_japan_contagion(self) -> ModuleResult:
        """
        日本流动性传染：资本回流风险(卖美债买日债)。
        - JGB 10Y: Critical > 2.50%
        - USD/JPY: Critical > 155 (干预风险)
        - Repatriation: (JGB30Y - 对冲成本) - UST30Y > 0 触发 REPATRIATION_RISK
        - 无 JGB 30Y 时用 JGB 10Y 与 DGS10 利差近似。
        """
        reason_parts: List[str] = []
        status = RegimeStatus.STABLE

        # USD/JPY (FRED: DEXJPUS)
        usdjpy = self._fetch("DEXJPUS")
        usdjpy_val = self._last(usdjpy, np.nan)
        if pd.notna(usdjpy_val) and usdjpy_val > 155.0:
            status = RegimeStatus.CRITICAL
            reason_parts.append(f"USD/JPY 干预风险 ({usdjpy_val:.1f} > 155)")

        # JGB 10Y: FRED IRSTCI01JPM156N (Japan gov bond), 无则用 Yahoo ^JP10Y
        jgb10 = self._fetch("IRSTCI01JPM156N", use_yahoo=False)
        if jgb10 is None or jgb10.empty:
            jgb10 = self._fetch(series_id="", use_yahoo=True, yahoo_symbol="^JP10Y")
        jgb10_val = self._last(jgb10, np.nan)
        if pd.notna(jgb10_val):
            if jgb10_val > 2.50:
                status = RegimeStatus.CRITICAL if status != RegimeStatus.CRITICAL else status
                reason_parts.append(f"JGB 10Y 高位 ({jgb10_val:.2f}% > 2.50%)")

        # Repatriation 近似: JGB10Y - DGS10 若为正且较大，表示日本相对吸引力上升
        dgs10 = self._fetch("DGS10")
        dgs30 = self._fetch("DGS30")
        ust10_val = self._last(dgs10, np.nan)
        ust30_val = self._last(dgs30, np.nan)
        if pd.notna(jgb10_val) and pd.notna(ust30_val):
            # 简化: 用 JGB10 - UST10 正且扩大表示回流压力
            spread = jgb10_val - ust10_val
            if spread > 0.5:
                reason_parts.append(f"日美利差收窄/反转 (JGB10-UST10={spread:.2f}%) 回流风险")
                if status != RegimeStatus.CRITICAL:
                    status = RegimeStatus.WARNING

        reason = "; ".join(reason_parts) if reason_parts else "稳定"
        self.results["japan_contagion"] = ModuleResult(
            name="Japan Contagion",
            status=status,
            value=usdjpy_val if pd.notna(usdjpy_val) else jgb10_val,
            threshold_used="JGB10Y>2.5%, USD/JPY>155, Repatriation spread",
            reason=reason,
        )
        return self.results["japan_contagion"]

    # ---------- Module C: Anti-Fiat & Gold (De-Dollarization) ----------
    def check_gold_regime(self) -> ModuleResult:
        """
        黄金作为去法币化/地缘信用指标。
        - SPX/Gold: Warning < 1.70, Critical < 1.45
        - 90 日 Gold vs 10Y Real Rate 相关 > 0 -> REGIME_BREAK_CONFIRMED
        - Gold > 5000 绝对价格警报
        - Reserves Proxy (De-Dollarization): Corr(Gold, DXY)_60d > 0.2 且 Gold 与 DXY 同时上涨 -> DE_DOLLARIZATION_RISK
        """
        reason_parts: List[str] = []
        status = RegimeStatus.HEDGE

        spx = self._fetch("", use_yahoo=True, yahoo_symbol="^GSPC")
        gold = self._fetch("", use_yahoo=True, yahoo_symbol="GC=F")
        dxy = self._fetch("", use_yahoo=True, yahoo_symbol="DX-Y.NYB")
        if dxy is None or dxy.empty:
            dxy = self._fetch("DTWEXBGS")

        if spx is not None and not spx.empty and gold is not None and not gold.empty:
            spx_val = self._last(spx, np.nan)
            gold_val = self._last(gold, np.nan)
            if pd.notna(spx_val) and pd.notna(gold_val) and gold_val > 0:
                ratio = spx_val / gold_val
                if ratio < 1.45:
                    status = RegimeStatus.CRITICAL
                    reason_parts.append(f"SPX/Gold 系统性避险 ({ratio:.2f} < 1.45)")
                elif ratio < 1.70:
                    status = RegimeStatus.ANTI_FIAT
                    reason_parts.append(f"SPX/Gold 偏低 ({ratio:.2f} < 1.70)")
            if pd.notna(gold_val) and gold_val > 5000:
                status = RegimeStatus.CRITICAL if status != RegimeStatus.CRITICAL else status
                reason_parts.append(f"黄金绝对价格 > $5000 ({gold_val:.0f})")

        # Reserves Proxy: Gold and DXY both rising with Corr(Gold, DXY)_60d > 0.2 -> DE_DOLLARIZATION_RISK
        if gold is not None and not gold.empty and dxy is not None and not dxy.empty:
            common = gold.dropna().index.intersection(dxy.dropna().index)
            if len(common) >= 60:
                g = gold.reindex(common).ffill().bfill()
                d = dxy.reindex(common).ffill().bfill()
                g_chg = g.pct_change().dropna()
                d_chg = d.pct_change().dropna()
                common_ret = g_chg.index.intersection(d_chg.index)[-60:]
                if len(common_ret) >= 30:
                    corr_gd = g_chg.reindex(common_ret).fillna(0).corr(d_chg.reindex(common_ret).fillna(0))
                    corr_val = float(corr_gd) if isinstance(corr_gd, (int, float)) else (float(corr_gd.iloc[0]) if hasattr(corr_gd, "iloc") and len(corr_gd) else np.nan)
                    gold_20d = (float(g.iloc[-1]) - float(g.iloc[-21])) / (float(g.iloc[-21]) + 1e-8) if len(g) >= 21 else 0.0
                    dxy_20d = (float(d.iloc[-1]) - float(d.iloc[-21])) / (float(d.iloc[-21]) + 1e-8) if len(d) >= 21 else 0.0
                    if pd.notna(corr_val) and corr_val > 0.2 and gold_20d > 0.02 and dxy_20d > 0.02:
                        status = RegimeStatus.ANTI_FIAT if status == RegimeStatus.HEDGE else status
                        reason_parts.append(
                            f"DE_DOLLARIZATION_RISK: Gold and DXY both rising, Corr(Gold,DXY)_60d={corr_val:.2f} (>0.2, reserves proxy)"
                        )

        # Real rate (TIPS): DFII10
        dfii10 = self._fetch("DFII10")
        if gold is not None and not gold.empty and dfii10 is not None and not dfii10.empty:
            gold_aligned = gold.reindex(dfii10.index).ffill().bfill()
            common = gold_aligned.dropna().index.intersection(dfii10.dropna().index)
            if len(common) >= 60:
                g = gold_aligned.reindex(common).ffill().bfill()
                r = dfii10.reindex(common).ffill().bfill()
                g_ret = g.pct_change().dropna()
                r_ret = r.diff().dropna()
                common_ret = g_ret.index.intersection(r_ret.index)[-90:]
                if len(common_ret) >= 30:
                    corr = g_ret.reindex(common_ret).fillna(0).corr(r_ret.reindex(common_ret).fillna(0))
                    if isinstance(corr, pd.Series):
                        corr = corr.iloc[0] if len(corr) else 0.0
                    if pd.notna(corr) and corr > 0.0:
                        status = RegimeStatus.REGIME_BREAK
                        reason_parts.append(f"Gold与实际利率正相关(90d)={corr:.2f}  regime break")

        reason = "; ".join(reason_parts) if reason_parts else "常规避险"
        self.results["gold_regime"] = ModuleResult(
            name="Gold / Anti-Fiat",
            status=status,
            value=self._last(gold, np.nan) if gold is not None else np.nan,
            threshold_used="SPX/Gold<1.45/1.70, Gold>5000, Corr(Gold,RealRate)>0; Reserves: Corr(Gold,DXY)_60d>0.2 & both up",
            reason=reason,
        )
        return self.results["gold_regime"]

    # ---------- Module D: K-Shaped (White-Collar Recession) ----------
    def check_k_shaped(self) -> ModuleResult:
        """
        K 型复苏：白领失业率相对整体上升（White-Collar Recession）。
        - LNS14027662 大学及以上失业率: Critical > 3.0%
        - Alert if Bachelor_Unemp trending UP (> +0.3% YoY) while Headline (UNRATE) stable
        """
        reason_parts: List[str] = []
        status = RegimeStatus.OK

        college_unemp = self._fetch("LNS14027662")
        headline_unemp = self._fetch("UNRATE")
        val = self._last(college_unemp, np.nan)
        if pd.notna(val):
            if val > 3.0:
                status = RegimeStatus.CRITICAL
                reason_parts.append(f"大学及以上失业率 ({val:.2f}% > 3.0%)")
            elif val > 2.5:
                status = RegimeStatus.WARNING
                reason_parts.append(f"大学及以上失业率偏高 ({val:.2f}%)")

        # YoY change: Bachelor vs Headline (monthly, 12 periods = 1 year)
        if college_unemp is not None and not college_unemp.empty and len(college_unemp) >= 13:
            bach_now = float(college_unemp.iloc[-1])
            bach_yoy = float(college_unemp.iloc[-13])
            bach_yoy_chg = (bach_now - bach_yoy) if pd.notna(bach_yoy) else np.nan
            headline_stable = True
            if headline_unemp is not None and not headline_unemp.empty and len(headline_unemp) >= 13:
                h_now = float(headline_unemp.iloc[-1])
                h_yoy = float(headline_unemp.iloc[-13])
                h_yoy_chg = (h_now - h_yoy) if pd.notna(h_yoy) else np.nan
                # Headline "stable" = YoY change small (e.g. within ±0.3%)
                headline_stable = pd.isna(h_yoy_chg) or abs(float(h_yoy_chg)) < 0.3
            if pd.notna(bach_yoy_chg) and float(bach_yoy_chg) > 0.3 and headline_stable:
                if status == RegimeStatus.OK:
                    status = RegimeStatus.WARNING
                reason_parts.append(
                    f"Bachelor unemp trending UP YoY (+{bach_yoy_chg:.2f}%) while headline stable (White-Collar Recession)"
                )

        reason = "; ".join(reason_parts) if reason_parts else "正常"
        self.results["k_shaped"] = ModuleResult(
            name="K-Shaped (White-Collar)",
            status=status,
            value=val,
            threshold_used="LNS14027662>3%; Bachelor YoY>+0.3% with UNRATE stable",
            reason=reason,
        )
        return self.results["k_shaped"]

    # ---------- Module E: Plumbing Stress (Repo/Liquidity Freeze) ----------
    def check_plumbing_stress(self) -> ModuleResult:
        """
        Proxy FRA-OIS: SOFR - 3M T-Bill (DTB3).
        Alert if Spread > 0.40% (40 bps) -> liquidity/repo stress.
        """
        reason_parts: List[str] = []
        status = RegimeStatus.OK

        sofr = self._fetch("SOFR")
        dtb3 = self._fetch("DTB3")
        sofr_val = self._last(sofr, np.nan)
        dtb3_val = self._last(dtb3, np.nan)
        if pd.notna(sofr_val) and pd.notna(dtb3_val):
            spread = float(sofr_val) - float(dtb3_val)
            if spread > 0.40:
                status = RegimeStatus.CRITICAL
                reason_parts.append(f"SOFR - 3M T-Bill = {spread:.2f}% (> 40 bps, plumbing/liquidity stress)")
            elif spread > 0.25:
                status = RegimeStatus.WARNING
                reason_parts.append(f"SOFR - 3M T-Bill = {spread:.2f}% (> 25 bps, watch)")

        if not reason_parts:
            reason_parts.append("SOFR-DTB3 spread within normal")
        value = (float(sofr_val) - float(dtb3_val)) if (pd.notna(sofr_val) and pd.notna(dtb3_val)) else None
        self.results["plumbing_stress"] = ModuleResult(
            name="Plumbing / Liquidity",
            status=status,
            value=value,
            threshold_used="SOFR - DTB3 > 0.40% (40 bps)",
            reason="; ".join(reason_parts),
        )
        return self.results["plumbing_stress"]

    # ---------- 综合评估 ----------
    def evaluate_systemic_risk(self) -> Tuple[str, Dict[str, Any]]:
        """
        组合各模块信号，返回最高等级结论与解释。
        例: Japan_Status==CRITICAL 且 Fiscal_Status==ACTIVE -> "SOVEREIGN LIQUIDITY CRISIS"
        """
        if "fiscal_dominance" not in self.results:
            self.check_fiscal_dominance()
        if "japan_contagion" not in self.results:
            self.check_japan_contagion()
        if "gold_regime" not in self.results:
            self.check_gold_regime()
        if "k_shaped" not in self.results:
            self.check_k_shaped()
        if "plumbing_stress" not in self.results:
            self.check_plumbing_stress()

        japan = self.results["japan_contagion"].status
        fiscal = self.results["fiscal_dominance"].status
        gold = self.results["gold_regime"].status
        k = self.results["k_shaped"].status
        plumbing = self.results.get("plumbing_stress")
        plumbing_status = plumbing.status if plumbing else RegimeStatus.OK

        verdict = "NORMAL"
        explanation: List[str] = []

        if japan == RegimeStatus.CRITICAL and fiscal in (RegimeStatus.ACTIVE, RegimeStatus.CRITICAL):
            verdict = "SOVEREIGN LIQUIDITY CRISIS"
            explanation.append("日本传染临界且财政主导活跃：主权流动性危机")
        elif japan == RegimeStatus.CRITICAL:
            verdict = "JAPAN_CONTAGION_CRITICAL"
            explanation.append("日本流动性熔断：JGB/USDJPY 临界")
        elif plumbing_status == RegimeStatus.CRITICAL:
            verdict = "LIQUIDITY_STRESS"
            explanation.append("流动性/管道压力：SOFR-3M T-Bill 利差 > 40 bps")
        elif fiscal == RegimeStatus.CRITICAL or fiscal == RegimeStatus.ACTIVE:
            verdict = "FISCAL_DOMINANCE_ACTIVE"
            explanation.append("财政主导：Bear Steepening 或通胀预期未锚定")
        elif gold in (RegimeStatus.CRITICAL, RegimeStatus.REGIME_BREAK):
            verdict = "ANTI_FIAT_REGIME"
            explanation.append("黄金/去法币化：SPX/Gold 或 regime break")
        elif k == RegimeStatus.CRITICAL:
            verdict = "K_SHAPED_RECESSION"
            explanation.append("K 型衰退：白领失业率临界")

        for name, r in self.results.items():
            if r.reason and r.status not in (RegimeStatus.OK, RegimeStatus.STABLE, RegimeStatus.INACTIVE, RegimeStatus.HEDGE):
                explanation.append(f"{r.name}: {r.reason}")

        detail = {
            "verdict": verdict,
            "explanations": explanation,
            "modules": {k: {"status": v.status, "reason": v.reason, "value": v.value} for k, v in self.results.items()},
        }
        return verdict, detail

    def run_all_checks(self) -> Dict[str, ModuleResult]:
        """依次执行 A/B/C/D/E 并返回所有模块结果。"""
        self.check_fiscal_dominance()
        self.check_japan_contagion()
        self.check_gold_regime()
        self.check_k_shaped()
        self.check_plumbing_stress()
        return self.results


# ---------- 报告用：Regime Dashboard 文本 ----------
def build_regime_dashboard_html(verdict: str, detail: Dict[str, Any]) -> str:
    """生成 Regime Dashboard 的 HTML 片段，供报告嵌入。"""
    modules = detail.get("modules", {})
    rows = []
    for key, m in modules.items():
        status = m.get("status", "N/A")
        reason = m.get("reason", "") or "-"
        value = m.get("value")
        val_str = f"{value:.2f}" if isinstance(value, (int, float)) and pd.notna(value) else str(value) if value is not None else "-"
        rows.append(f"<tr><td>{key}</td><td>{status}</td><td>{val_str}</td><td>{reason}</td></tr>")
    table_body = "\n".join(rows)
    expl = "<br/>".join(detail.get("explanations", []))
    return f"""
<div class="regime-dashboard" style="margin:1em 0; padding:1em; border:1px solid #ccc; border-radius:8px;">
  <h3>Regime Dashboard (Fiscal Dominance Era)</h3>
  <p><strong>Composite Verdict:</strong> {verdict}</p>
  <p><strong>Explanation:</strong><br/>{expl}</p>
  <table style="width:100%; border-collapse:collapse;">
    <thead><tr><th>Module</th><th>Status</th><th>Value</th><th>Reason</th></tr></thead>
    <tbody>{table_body}</tbody>
  </table>
</div>
"""


def build_regime_dashboard_md(verdict: str, detail: Dict[str, Any]) -> str:
    """生成 Regime Dashboard 的 Markdown 片段。"""
    modules = detail.get("modules", {})
    lines = [
        "## Regime Dashboard (Fiscal Dominance Era)",
        "",
        f"**Composite Verdict:** {verdict}",
        "",
        "**Explanation:**",
    ]
    for e in detail.get("explanations", []):
        lines.append(f"- {e}")
    lines.extend(["", "| Module | Status | Value | Reason |", "|--------|--------|-------|--------|"])
    for key, m in modules.items():
        v = m.get("value")
        val_str = f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else str(v) if v is not None else "-"
        lines.append(f"| {key} | {m.get('status','')} | {val_str} | {m.get('reason','') or '-'} |")
    return "\n".join(lines)


if __name__ == "__main__":
    monitor = CrisisMonitor()
    monitor.run_all_checks()
    verdict, detail = monitor.evaluate_systemic_risk()
    print("Verdict:", verdict)
    print("Detail:", detail)
    print(build_regime_dashboard_md(verdict, detail))
