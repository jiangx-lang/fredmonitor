#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性健康检查：危机监控依赖的 FRED 序列（API 可拉取）与 Yahoo 标的（yfinance 可拉取）。
用法（项目根目录）: py -3 scripts/audit_download_health.py
"""
from __future__ import annotations

import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)


def main() -> int:
    # ---- FRED ----
    fred_ok: list[str] = []
    fred_fail: list[tuple[str, str]] = []

    try:
        from scripts.sync_fred_http import get_crisis_required_fred_series, DERIVED_FRED_DEPS
    except Exception as e:
        print(f"无法导入 sync_fred_http: {e}")
        return 1

    needed = set(get_crisis_required_fred_series())
    for _d, deps in DERIVED_FRED_DEPS.items():
        for s in deps:
            needed.add(str(s).upper())

    try:
        from scripts.fred_http import series_observations
    except Exception as e:
        print(f"无法导入 fred_http（需 FRED_API_KEY）: {e}")
        series_observations = None  # type: ignore

    if series_observations:
        for sid in sorted(needed):
            try:
                data = series_observations(sid, limit=3, sort_order="desc")
                obs = data.get("observations") or []
                if not obs:
                    fred_fail.append((sid, "无观测值"))
                else:
                    fred_ok.append(sid)
            except Exception as ex:
                fred_fail.append((sid, str(ex)[:120]))

    # ---- Yahoo（与 crisis_monitor_v2 微观结构 + 面板 A/B 一致）----
    yahoo_groups = {
        "main_internals": ["SPY", "IWM", "XLE", "XLK", "^SPX", "RSP"],
        "panel_a": ["^VVIX", "^VIX", "DX-Y.NYB", "USDCNH=X"],
        "panel_b": ["TIP", "IEF", "ITA", "CL=F", "CL12=F", "GC=F", "GLD", "DX-Y.NYB"],
        # 布伦特现货期货（部分环境作交叉验证；面板 B 当前用 WTI）
        "oil_crosscheck": ["BZ=F"],
        "compose_common": [
            "HYG", "LQD", "KRE", "XLF", "^GSPC", "^VIX", "^VIX3M", "^VIX6M", "^VIX9D",
            "QQQ", "BTC-USD", "UUP", "DX-Y.NYB",
        ],
    }
    flat_yahoo: list[str] = []
    seen = set()
    for _g, tickers in yahoo_groups.items():
        for t in tickers:
            if t not in seen:
                seen.add(t)
                flat_yahoo.append(t)

    yahoo_ok: list[str] = []
    yahoo_fail: list[tuple[str, str]] = []
    yahoo_period_note: dict[str, str] = {}  # 非 30d 才命中时用

    try:
        import yfinance as yf
    except ImportError:
        print("未安装 yfinance，跳过 Yahoo 检查")
        yf = None  # type: ignore

    # 原油/远期：Yahoo 常需更长窗口才有有效 bar；先试 30d 再试 1y/5y
    OIL_TRY_PERIODS = ("30d", "1y", "5y")

    def _yahoo_fetch(sym: str) -> tuple[bool, str]:
        periods = list(OIL_TRY_PERIODS) if sym in ("CL=F", "CL12=F", "BZ=F") else ["30d"]
        last_err = ""
        for per in periods:
            try:
                df = yf.download(
                    sym, period=per, progress=False, threads=False, auto_adjust=True
                )
                if df is not None and not df.empty:
                    if per != "30d":
                        return True, per
                    return True, "30d"
                last_err = f"empty@{per}"
            except Exception as ex:
                last_err = str(ex)[:100]
        return False, last_err

    if yf is not None:
        for sym in flat_yahoo:
            ok, detail = _yahoo_fetch(sym)
            if ok:
                yahoo_ok.append(sym)
                if detail != "30d":
                    yahoo_period_note[sym] = detail
            else:
                yahoo_fail.append((sym, detail))

    # ---- 报告 ----
    print("=== FRED（crisis 配置 + 派生依赖，API 拉取最近观测）===")
    if not series_observations:
        print("  跳过：fred_http 不可用")
    else:
        print(f"  通过: {len(fred_ok)} / {len(needed)}")
        print(f"  失败: {len(fred_fail)}")
        for sid, err in fred_fail[:40]:
            print(f"    FAIL {sid}: {err}")
        if len(fred_fail) > 40:
            print(f"    ... 另有 {len(fred_fail) - 40} 条失败")

    print("\n=== Yahoo Finance（微观结构 + 面板 + 常用合成）===")
    if yf is None:
        print("  跳过")
    else:
        print(f"  通过: {len(yahoo_ok)} / {len(flat_yahoo)}")
        print(f"  失败: {len(yahoo_fail)}")
        for sym, err in yahoo_fail:
            print(f"    FAIL {sym}: {err}")
        if yahoo_period_note:
            print("  （以下标的在更长 period 下才拉到数据，属正常）")
            for sym, per in sorted(yahoo_period_note.items()):
                print(f"    NOTE {sym}: 使用 period={per}")

    print("\n=== 石油期货重点（CL=F / CL12=F / BZ=F）===")
    if yf is None:
        print("  跳过")
    else:
        for sym in ("CL=F", "CL12=F", "BZ=F"):
            ok = sym in yahoo_ok
            st = "PASS" if ok else "FAIL"
            extra = f" [{yahoo_period_note.get(sym, '30d')}]" if ok and sym in yahoo_period_note else ""
            err = next((e for s, e in yahoo_fail if s == sym), "")
            print(f"  {st} {sym}{extra}" + (f" — {err}" if not ok and err else ""))

    if fred_fail or yahoo_fail:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
