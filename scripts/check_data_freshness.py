#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查 FRED 与 Yahoo 数据新鲜度，便于判断是否需用 Yahoo 补充。"""
import sys
from datetime import datetime, timedelta

import pathlib
BASE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

def main():
    import pandas as pd
    from crisis_monitor import (
        fetch_series,
        fetch_yahoo_safe,
        get_warsh_state,
        get_japan_carry_state,
        compose_series,
    )
    from scripts.clean_utils import parse_numeric_series
    from scripts.fred_http import series_observations

    print("=" * 60)
    print("数据新鲜度检查 (FRED vs Yahoo)")
    print("=" * 60)
    now = datetime.now()
    cutoff_30d = (now - timedelta(days=30)).date()

    # 1) FRED 日本 10Y
    print("\n【1】FRED IRLTLT01JPM156N (日本 10Y 国债)")
    jgb = fetch_series("IRLTLT01JPM156N")
    if jgb is not None and not jgb.empty:
        last_ts = jgb.index[-1]
        last_date = last_ts.date() if hasattr(last_ts, "date") else last_ts
        age_days = (now.date() - last_date).days if hasattr(last_date, "days") else (now - last_ts).days
        print(f"    最后观测: {last_date}  距今约 {age_days} 天")
        print(f"    最新值:   {float(jgb.iloc[-1]):.2f}%")
        if last_date < cutoff_30d:
            print("    ⚠️ 滞后超过 30 天，建议报告中以 Yahoo 8282.T 为实时代理")
    else:
        print("    无数据")

    # 2) Yahoo 8282.T
    print("\n【2】Yahoo 8282.T (10Y JGB ETF 代理)")
    etf = fetch_yahoo_safe("8282.T")
    if etf is not None and not etf.empty:
        last_ts = etf.index[-1]
        last_date = last_ts.date() if hasattr(last_ts, "date") else last_ts
        print(f"    最后观测: {last_date}")
        print(f"    最新价:   {float(etf.iloc[-1]):.2f}")
        if len(etf) >= 6:
            pct5 = (float(etf.iloc[-1]) / float(etf.iloc[-6]) - 1.0) * 100
            print(f"    5 日涨跌: {pct5:.2f}%")
    else:
        print("    无数据")

    # 3) USD/JPY
    print("\n【3】Yahoo USDJPY=X")
    usdjpy = fetch_yahoo_safe("USDJPY=X")
    if usdjpy is not None and not usdjpy.empty:
        last_ts = usdjpy.index[-1]
        last_date = last_ts.date() if hasattr(last_ts, "date") else last_ts
        print(f"    最后观测: {last_date}  最新: {float(usdjpy.iloc[-1]):.2f}")
    else:
        print("    无数据")

    # 4) Japan carry state 汇总
    print("\n【4】Japan Carry 状态 (get_japan_carry_state)")
    state = get_japan_carry_state()
    print(f"    jgb10y:        {state.get('jgb10y')}")
    print(f"    jgb10y_cutoff: {state.get('jgb10y_cutoff')}")
    print(f"    jgb_etf_8282_5d_pct: {state.get('jgb_etf_8282_5d_pct')}")
    print(f"    jpy_vol_1m:    {state.get('jpy_vol_1m')}")
    print(f"    us_jp_spread:  {state.get('us_jp_spread')}")

    # 5) 关键 FRED 序列最后日期
    print("\n【5】其他关键 FRED 最后观测日")
    for sid, name in [("DGS10", "美10Y"), ("SOFR", "SOFR"), ("RRPONTSYD", "RRP")]:
        s = fetch_series(sid)
        if s is not None and not s.empty:
            last_ts = s.index[-1]
            last_date = last_ts.date() if hasattr(last_ts, "date") else last_ts
            print(f"    {sid}: {last_date}")
        else:
            print(f"    {sid}: 无数据")

    print("\n" + "=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())
