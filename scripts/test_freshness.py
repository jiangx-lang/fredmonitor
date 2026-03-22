#!/usr/bin/env python3
"""
回归测试：_compute_freshness_classes() 的阈值行为。
不依赖外部文件，用 mock 数据验证 D=7、W=10、M=60、Q=120 是否生效。
30 秒内跑完，升级后跑一遍可确认阈值未被覆盖。
"""
from datetime import datetime, timedelta
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

# 使用固定参考日期，便于计算 last_date
REF = datetime(2026, 3, 7).date()

CASES = [
    # (series_id, freq, 距今天数, 预期: "fresh" 不进⚠️ / "stale" 进⚠️)
    ("TEST_D1", "D", 5, "fresh"),
    ("TEST_D2", "D", 8, "stale"),
    ("TEST_W1", "W", 8, "fresh"),
    ("TEST_W2", "W", 12, "stale"),
    ("TEST_M1", "M", 55, "fresh"),
    ("TEST_M2", "M", 65, "stale"),
    ("TEST_Q1", "Q", 110, "fresh"),
    ("TEST_Q2", "Q", 130, "stale"),
]


def run():
    import crisis_monitor as cm

    processed_indicators = []
    indicators = []

    for sid, freq, days_ago, _ in CASES:
        last_d = REF - timedelta(days=days_ago)
        processed_indicators.append({
            "series_id": sid,
            "name": sid,
            "last_date": last_d.strftime("%Y-%m-%d"),
        })
        indicators.append({"id": sid, "freq": freq})

    scoring_config = {"deprecated_series": []}
    fresh_list, stale_list = cm._compute_freshness_classes(
        processed_indicators, indicators, scoring_config, reference_date=REF
    )
    fresh_ids = {r["series_id"] for r in fresh_list}
    stale_ids = {r["series_id"] for r in stale_list}

    failed = []
    for sid, freq, days_ago, expected in CASES:
        in_stale = sid in stale_ids
        in_fresh = sid in fresh_ids
        actual = "stale" if in_stale else "fresh"
        if actual != expected:
            failed.append((sid, freq, days_ago, expected, actual))

    if failed:
        for sid, freq, days_ago, expected, actual in failed:
            print(f"❌ {sid} (freq={freq}, 距今天数={days_ago}): 预期={expected}, 实际={actual}")
        print(f"\n❌ {len(failed)} 个用例失败")
        return len(failed)
    print("✅ 所有 freshness 阈值测试通过")
    return 0


if __name__ == "__main__":
    sys.exit(run())
