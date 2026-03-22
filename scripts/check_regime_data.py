#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行主报告前的数据与计算预检查：
- 检查 Regime/Conflict/Structural 所需 FRED 与 Yahoo 序列是否可获取
- 跑一遍 Regime 各模块，确认计算正常并输出简要结果
"""
import sys
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

# 各监控层用到的 FRED 序列（去重）；期限溢价优先 ACMTP10，回退 THREEFYTP10
FRED_SERIES = [
    "DGS10", "DGS2", "DGS30", "ACMTP10", "THREEFYTP10", "T5YIFR",
    "DEXJPUS", "DTWEXBGS", "DFII10",
    "LNS14027662", "LNS14027660", "UNRATE",
    "SOFR", "DTB3",
    "IRSTCI01JPM156N", "IRLTLT01JPM156N",
    "USEPUINDXD",
]
# Yahoo 符号（K-Shaped 已改用 IWM/SPY 比率，不再依赖 NFIBIDX）
YAHOO_SYMBOLS = ["^GSPC", "GC=F", "DX-Y.NYB", "JPY=X", "TLT", "^JP10Y", "IWM", "SPY"]

def main():
    import crisis_monitor as base
    import crisis_monitor_regime as regime_module

    print("=" * 60)
    print("1. FRED API 与数据可用性")
    print("=" * 60)
    fred_ok = getattr(base, "FRED_AVAILABLE", False)
    print(f"   FRED_AVAILABLE: {fred_ok}")
    if not fred_ok:
        print("   ⚠️ FRED 不可用，所有 FRED 序列将为空，请检查 scripts.fred_http 与 API Key")

    print("\n2. FRED 序列检查（fetch_series）")
    print("-" * 50)
    ok_count = 0
    for sid in FRED_SERIES:
        try:
            ts = base.fetch_series(sid)
            if ts is not None and not ts.empty:
                last = ts.index[-1]
                n = len(ts)
                print(f"   ✅ {sid}: {n} 条, 最新 {last.date()}")
                ok_count += 1
            else:
                print(f"   ❌ {sid}: 无数据")
        except Exception as e:
            print(f"   ❌ {sid}: {e}")
    print(f"   合计: {ok_count}/{len(FRED_SERIES)} 可用")

    print("\n3. Yahoo 序列检查（fetch_yahoo_safe，1 次重试）")
    print("-" * 50)
    yahoo_ok = 0
    for sym in YAHOO_SYMBOLS:
        try:
            ts = base.fetch_yahoo_safe(sym, retries=1, delay=1.0)
            if ts is not None and not ts.empty:
                last = ts.index[-1]
                n = len(ts)
                print(f"   ✅ {sym}: {n} 条, 最新 {last.date()}")
                yahoo_ok += 1
            else:
                print(f"   ❌ {sym}: 无数据")
        except Exception as e:
            print(f"   ❌ {sym}: {e}")
    print(f"   合计: {yahoo_ok}/{len(YAHOO_SYMBOLS)} 可用")

    print("\n4. Regime 模块计算检查（CrisisMonitor.run_all_checks）")
    print("-" * 50)
    try:
        mon = regime_module.CrisisMonitor(base.BASE if hasattr(base, "BASE") else None)
        mon.run_all_checks()
        import numpy as np
        for key, r in mon.results.items():
            val = r.value
            try:
                if val is None or (isinstance(val, (int, float)) and np.isnan(val)):
                    val_str = "-"
                else:
                    val_str = f"{float(val):.2f}"
            except (TypeError, ValueError):
                val_str = str(val)
            print(f"   {key}: status={r.status} value={val_str}")
            print(f"      reason: {r.reason[:80]}{'...' if len(r.reason) > 80 else ''}")
        verdict, detail = mon.evaluate_systemic_risk()
        print(f"   Composite Verdict: {verdict}")
    except Exception as e:
        print(f"   ❌ Regime 运行异常: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 60)
    print("预检查结束。若上述有大量 ❌ 或 Regime 报错，请先同步 FRED 或检查网络/API 再运行主报告。")
    print("=" * 60)


if __name__ == "__main__":
    main()
