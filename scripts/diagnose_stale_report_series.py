#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
滞后指标根因诊断：对比报告中「已滞后」列表的本地最后观测 vs FRED 上游最新。
用于判断是「下载/同步问题」还是「FRED 本身发布节奏」（如季频尚未发布新季度）。

用法：在项目根目录执行 py scripts/diagnose_stale_report_series.py
需要 FRED_API_KEY（macrolab.env）。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
os.chdir(BASE)

# 报告里列出的滞后指标（与 crisis_indicators 中 id 一致）
STALE_REPORT_SERIES = [
    ("NCBDBIQ027S", "企业债/GDP（新）", "Q", "FRED", ["NCBDBIQ027S"]),
    ("TDSP", "家庭债务偿付比率", "Q", "FRED", ["TDSP"]),
    ("RESERVES_DEPOSITS_PCT", "准备金/存款%", "W", "derived", ["TOTRESNS", "TOTALSA"]),
    ("CORPDEBT_GDP_PCT", "企业债/GDP（旧）", "Q", "derived", ["NCBDBIQ027S", "GDP"]),
    ("CREDIT_CARD_DELINQUENCY", "信用卡违约率", "Q", "FRED", ["DRCCLACBS"]),
    ("DRSFRMACBS", "房贷违约率", "Q", "FRED", ["DRSFRMACBS"]),
    ("GDP", "GDP YoY", "Q", "YoY", ["GDP"]),
    ("CSUSHPINSA", "房价指数: Case-Shiller 20城 YoY", "M", "YoY", ["CSUSHPINSA"]),
    ("HOUST", "新屋开工 年化", "M", "FRED", ["HOUST"]),
    ("NEWORDER", "制造业新订单 YoY", "M", "YoY", ["NEWORDER"]),
    ("PERMIT", "住宅建筑许可 YoY", "M", "YoY", ["PERMIT"]),
    ("INDPRO", "工业生产 YoY", "M", "YoY", ["INDPRO"]),
    ("UMCSENT", "密歇根消费者信心", "M", "FRED", ["UMCSENT"]),
    ("BAA10YM", "投资级信用利差: Baa-10Y国债", "M", "FRED", ["BAA10YM"]),
    ("CPN3M", "3个月商业票据利率", "M", "FRED", ["CPN3M"]),
]

FRED_ROOT = BASE / "data" / "fred" / "series"
SERIES_ROOT = BASE / "data" / "series"


def _local_last_from_csv(path: Path, date_col: str = "date") -> str | None:
    if not path.exists():
        return None
    try:
        df = __import__("pandas").read_csv(path, nrows=0)
        if "date" in df.columns:
            idx_col = "date"
        else:
            idx_col = 0
        df = __import__("pandas").read_csv(path, index_col=idx_col, parse_dates=True)
        if df.empty:
            return None
        last = df.index[-1]
        if hasattr(last, "date"):
            return str(last.date())
        return str(last)[:10]
    except Exception:
        return None


def get_local_last(indicator_id: str, source_type: str, fred_legs: list[str]) -> str | None:
    """报告里用的最后观测日：来自评分实际读的文件。"""
    if source_type == "FRED":
        # 别名时评分读的是 fred_legs[0]（如 CREDIT_CARD_DELINQUENCY -> DRCCLACBS）
        fred_id = (fred_legs[0] if fred_legs else indicator_id)
        raw = FRED_ROOT / fred_id / "raw.csv"
        return _local_last_from_csv(raw)
    if source_type == "YoY":
        # 评分优先用 data/series/{id}_YOY.csv
        p = SERIES_ROOT / f"{indicator_id}_YOY.csv"
        last = _local_last_from_csv(p)
        if last:
            return last
        raw = FRED_ROOT / indicator_id / "raw.csv"
        return _local_last_from_csv(raw)
    if source_type == "derived":
        p = SERIES_ROOT / f"{indicator_id}.csv"
        return _local_last_from_csv(p)
    return None


def get_fred_upstream_last(fred_id: str) -> tuple[str | None, str]:
    """FRED API 该序列最新观测日。返回 (YYYY-MM-DD, error_msg)。"""
    try:
        from scripts.fred_http import series_observations
    except Exception as e:
        return None, str(e)
    try:
        from datetime import timedelta
        start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        resp = series_observations(fred_id, observation_start=start)
        obs = resp.get("observations") or []
        if not obs:
            return None, "no observations"
        dates = [o.get("date") for o in obs if o.get("date")]
        if not dates:
            return None, "no date in obs"
        return max(dates), ""
    except Exception as e:
        return None, str(e)


def main():
    print("=" * 100)
    print("滞后指标根因诊断：本地最后观测 vs FRED 上游最新")
    print("=" * 100)
    print()

    has_key = bool(os.environ.get("FRED_API_KEY"))
    if not has_key:
        try:
            from dotenv import load_dotenv
            load_dotenv(BASE / "macrolab.env")
            has_key = bool(os.environ.get("FRED_API_KEY"))
        except Exception:
            pass
    if not has_key:
        print("⚠️ 未设置 FRED_API_KEY，仅显示本地最后观测，无法对比上游。")
        print()

    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for indicator_id, name, freq, source_type, fred_legs in STALE_REPORT_SERIES:
        local_last = get_local_last(indicator_id, source_type, fred_legs)
        upstream_last = None
        err = ""
        if has_key and fred_legs:
            # 派生指标：取各腿中最新的那个作为「上游最新」（若任一脚更新，重算后可更新）
            for fid in fred_legs:
                up, e = get_fred_upstream_last(fid)
                if e and not up:
                    err = e
                if up:
                    if upstream_last is None or up > upstream_last:
                        upstream_last = up
        elif has_key and source_type == "derived":
            for fid in fred_legs:
                up, e = get_fred_upstream_last(fid)
                if e and not up:
                    err = e
                if up:
                    if upstream_last is None or up > upstream_last:
                        upstream_last = up

        # 判定
        if not local_last:
            verdict = "本地无数据"
        elif not upstream_last:
            verdict = "上游无数据或请求失败" if err else "未查上游"
        elif upstream_last > local_last:
            verdict = "→ 下载/同步问题：FRED 有更新，请执行 sync --before-report 或重算派生"
        else:
            verdict = "→ FRED 发布节奏：上游与本地一致或更旧，属正常滞后"
        rows.append({
            "id": indicator_id,
            "name": name,
            "freq": freq,
            "local": local_last or "-",
            "fred": upstream_last or "-",
            "verdict": verdict,
        })

    # 表头
    fmt = "{:<22} {:<8} {:<12} {:<12} {:<60}"
    print(fmt.format("指标 ID", "频率", "本地最后", "FRED 最新", "结论"))
    print("-" * 100)
    for r in rows:
        print(fmt.format(r["id"][:22], r["freq"], r["local"], r["fred"], r["verdict"][:58]))
    print()
    print("说明：")
    print("  - 本地最后：报告评分实际使用的数据源（raw.csv / data/series/*.csv / *_YOY.csv）的最后一笔日期。")
    print("  - 若「FRED 有更新」：请运行 py scripts/sync_fred_http.py --before-report 拉取最新 raw，")
    print("    派生指标（CORPDEBT_GDP_PCT、RESERVES_DEPOSITS_PCT）会在同次 sync 内重算。")
    print("  - YoY 指标（GDP、INDPRO 等）若 raw 已更新但报告仍滞后，需在 sync 后运行 calculate_yoy_indicators。")
    print("  - 若「FRED 发布节奏」：该序列本身为季/月频，FRED 尚未发布更新，非代码或下载问题。")
    print()
    need_sync = [r for r in rows if "下载/同步" in r["verdict"]]
    if need_sync:
        print("【建议】以下指标请执行同步后重算：")
        for r in need_sync:
            print(f"  - {r['id']}: 本地 {r['local']} -> FRED 最新 {r['fred']}")
    else:
        print("【结论】当前无「FRED 有更新但本地未拉取」的项；其余滞后为发布频率或 FRED 未发布新值。")
    print("=" * 100)


if __name__ == "__main__":
    main()
