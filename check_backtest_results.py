#!/usr/bin/env python3
"""查看回测结果"""
import pandas as pd

df = pd.read_csv('outputs/history_tension.csv', index_col=0, parse_dates=True)

moments = [
    ('2000-03-10', '纳指崩盘前夕'),
    ('2007-10-11', '美股07年高点/次贷爆发'),
    ('2020-02-19', '疫情熔断前夜'),
    ('2021-12-31', '2022熊市前高')
]

print('=' * 80)
print('关键时刻对比 (v2.3 Fragility Amplifier)')
print('=' * 80)

for date, name in moments:
    idx = df.index.get_indexer([pd.Timestamp(date)], method='nearest')[0]
    row = df.iloc[idx]
    actual_date = df.index[idx]
    
    print(f'\n{name} ({actual_date.strftime("%Y-%m-%d")}):')
    print(f'  State: {row["State_Score"]:.1f}, Trigger: {row["Trigger_Score"]:.1f}')
    print(f'  Momentum: {row["Trigger_Momentum"]:.1f}, Convexity: {row["Convexity_Factor"]:.2f}x')
    print(f'  Tension: {row["Tension_Index"]:.1f}')

print('\n' + '=' * 80)
print('Top 10 Highest Tension Moments:')
print('=' * 80)
top10 = df.nlargest(10, 'Tension_Index')
for idx, row in top10.iterrows():
    print(f'{idx.strftime("%Y-%m-%d")}: State={row["State_Score"]:.1f}, Trigger={row["Trigger_Score"]:.1f}, '
          f'Momentum={row["Trigger_Momentum"]:.1f}, Convexity={row["Convexity_Factor"]:.2f}x, '
          f'Tension={row["Tension_Index"]:.1f}')
