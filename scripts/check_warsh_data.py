#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行主报告前的 Warsh 层数据充分性检查：
- 检查 Warsh 所需 FRED 序列是否可获取、是否足够新
- 运行 get_warsh_state() 与 compute_warsh_risk_level()，验证计算正确
- 对关键数值做合理性校验（单位、量级）
"""
import sys
import os
from datetime import datetime, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
os.chdir(BASE)

# Warsh 层直接依赖的 FRED 序列
WARSH_FRED_SERIES = [
    "SOFR",      # 有担保隔夜融资利率
    "IORB",      # 准备金余额利率
    "RRPONTSYD", # 隔夜逆回购余额（百万美元）
    "THREEFYTP10", # 10 年期期限溢价
    "VIXCLS",    # VIX（债市波动代理用）
    "DGS10",     # 10 年期美债
    "DGS2",      # 2 年期美债
]

# 合理性范围（用于告警，不阻断）；RRPONTSYD 在 FRED 单位为十亿美元
RANGES = {
    "SOFR": (0.0, 10.0),           # %
    "IORB": (0.0, 10.0),           # %
    "RRPONTSYD": (0.1, 3500.0),    # 十亿美元，约 1 亿～3.5 万亿
    "THREEFYTP10": (-2.0, 4.0),   # %
    "VIXCLS": (5.0, 100.0),
    "DGS10": (0.0, 15.0),
    "DGS2": (0.0, 15.0),
}


def check_series(base_mod, series_id: str, max_stale_days: int = 30) -> tuple:
    """返回 (ok: bool, n_obs: int, last_date, last_value, message)."""
    try:
        ts = base_mod.fetch_series(series_id)
        if ts is None or ts.empty:
            return False, 0, None, None, "无数据"
        n = len(ts)
        last_date = ts.index[-1]
        last_val = float(ts.iloc[-1])
        if hasattr(last_date, "date"):
            last_date = last_date.date()
        age_days = (datetime.now().date() - last_date).days if last_date else 999
        if age_days > max_stale_days:
            return True, n, last_date, last_val, f"数据偏旧（{age_days} 天前）"
        lo, hi = RANGES.get(series_id, (None, None))
        if lo is not None and (last_val < lo or last_val > hi):
            return True, n, last_date, last_val, f"数值超出合理范围 [{lo},{hi}]，当前 {last_val}"
        return True, n, last_date, last_val, "OK"
    except Exception as e:
        return False, 0, None, None, str(e)


def main():
    import crisis_monitor as base

    print("=" * 60)
    print("Warsh 层运行前数据充分性检查")
    print("=" * 60)
    print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  工作目录: {BASE}")
    print()

    # 1. FRED 可用性
    fred_ok = getattr(base, "FRED_AVAILABLE", False)
    print("1. FRED API 可用性")
    print("-" * 50)
    print(f"   FRED_AVAILABLE: {fred_ok}")
    if not fred_ok:
        print("   ⚠️ FRED 不可用时，仅能使用本地缓存；若缓存缺失则 Warsh 指标将为空。")
    print()

    # 2. 各序列可获取性 + 新鲜度 + 合理性
    print("2. Warsh 所需 FRED 序列")
    print("-" * 50)
    ok_count = 0
    stale_series = []
    for sid in WARSH_FRED_SERIES:
        ok, n, last_date, last_val, msg = check_series(base, sid)
        if ok:
            ok_count += 1
            if "数据偏旧" in msg:
                stale_series.append(sid)
            val_str = f", 最新值 {last_val:.4f}" if last_val is not None else ""
            print(f"   ✅ {sid}: {n} 条, 截止 {last_date}{val_str}  ({msg})")
        else:
            print(f"   ❌ {sid}: {msg}")
    print(f"   合计: {ok_count}/{len(WARSH_FRED_SERIES)} 可用")
    print()

    # 3. get_warsh_state() 与 compute_warsh_risk_level()
    print("3. Warsh 状态与风险等级计算")
    print("-" * 50)
    try:
        state = base.get_warsh_state()
        result = base.compute_warsh_risk_level(state)
        level = result.get("warsh_level", "—")
        reason = result.get("warsh_level_reason", "")
        flags = result.get("warsh_flags", [])
        print(f"   Warsh Risk Level: {level}")
        print(f"   Reason: {reason[:80]}...")
        print(f"   Triggered constraints: {flags if flags else 'none'}")
        print("   最新数值:")
        print(f"     SOFR−IORB:        {state.get('sofr_iorb_spread')}")
        print(f"     SOFR−IORB 5DMA:   {state.get('sofr_iorb_spread_5dma')}")
        print(f"     RRP (十亿):       {state.get('rrp_level_bn')}")
        print(f"     RRP 20D 变化(十亿): {state.get('rrp_20d_abs_change_bn')}")
        print(f"     THREEFYTP10:      {state.get('term_premium')}")
        print(f"     Bear steepening:  {state.get('bear_steepening')}")
    except Exception as e:
        print(f"   ❌ 计算异常: {e}")
        import traceback
        traceback.print_exc()
        return 1
    print()

    # 4. 单位与量级校验（FRED RRPONTSYD 单位为十亿美元，rrp_level_bn = rrp_level）
    print("4. 单位与量级校验")
    print("-" * 50)
    rrp_bn = state.get("rrp_level_bn")
    if rrp_bn is not None:
        if 10 < rrp_bn < 3000:
            print(f"   ✅ RRP 量级合理: {rrp_bn:.1f} 十亿美元 (FRED 单位为 Billions)")
        elif rrp_bn < 100:
            print(f"   ⚠️ RRP 偏低: {rrp_bn:.1f} 十亿，可能触发 RED（<100B）")
        else:
            print(f"   RRP: {rrp_bn:.1f} 十亿美元")
    else:
        print("   — RRP 无数据，跳过单位校验")
    sofr = state.get("sofr_iorb_spread")
    if sofr is not None:
        print(f"   SOFR−IORB 为百分制: {sofr:.4f}% (正常应约在 -0.1～0.1 附近)")
    print()

    # 5. 结论与建议
    print("5. 结论与建议")
    print("-" * 50)
    if ok_count >= 5 and "get_warsh_state" in dir(base):
        print("   ✅ 数据充分性检查通过，可运行主报告。")
    elif ok_count >= 3:
        print("   ⚠️ 部分序列缺失或偏旧，Warsh 层可运行但部分指标可能为空。")
    else:
        print("   ❌ 关键序列不足，建议先执行 sync_fred_http 或检查 FRED API 后再运行主报告。")
    if stale_series:
        print("   建议: 若需最新 Warsh 读数，请先运行 py scripts/sync_fred_http.py 刷新偏旧序列。")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
