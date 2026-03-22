#!/usr/bin/env python3
"""
Task 3 验证：对问题 A 中的 DTB3、DGS10、IC4WSA 拉取 FRED 最近 30 天数据，
打印每序列最后 5 行，用于确认 API 端最新观测日期是否足够新（修复同步后可用此脚本复验）。
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.fred_http import series_observations

def main():
    end = datetime.now().date()
    start = end - timedelta(days=30)
    start_str = start.isoformat()
    series_ids = ["DTB3", "DGS10", "IC4WSA"]
    print("FRED API 拉取最近 30 天数据，每序列显示最后 5 行")
    print(f"observation_start={start_str}, 无 observation_end（到最新）\n")
    for sid in series_ids:
        try:
            resp = series_observations(sid, observation_start=start_str)
            obs = resp.get("observations", [])
            if not obs:
                print(f"{sid}: 无数据")
                continue
            # 取最后 5 条
            last5 = obs[-5:]
            last_date = last5[-1].get("date") if last5 else None
            print(f"--- {sid} (共 {len(obs)} 条, 最后日期: {last_date}) ---")
            for row in last5:
                print(f"  {row.get('date')}  {row.get('value')}")
            print()
        except Exception as e:
            print(f"{sid}: 请求失败 - {e}\n")
    print("验证完成。若上述最后日期均 >= 当月/当周，说明 FRED 端数据正常，问题在本地同步范围。")

if __name__ == "__main__":
    main()
