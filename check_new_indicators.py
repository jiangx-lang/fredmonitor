from scripts.fred_http import series_search
import time

# 用关键词搜索
search_terms = [
    'loan officer survey',
    'initial claims',
    'new orders',
    'building permits',
    'average weekly hours',
    'manufacturing'
]

print('🔍 关键词搜索FRED指标:')
print('=' * 50)

for term in search_terms:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'\n📋 搜索 "{term}" 的结果:')
            for i, series in enumerate(result['series'][:3]):  # 只显示前3个
                series_id = series.get('id', 'Unknown')
                title = series.get('title', 'Unknown')
                frequency = series.get('frequency_short', 'Unknown')
                print(f'  {i+1}. {series_id}: {title}')
                print(f'     频率: {frequency}')
        else:
            print(f'❌ "{term}": 未找到结果')
        time.sleep(1)  # 避免API限制
    except Exception as e:
        print(f'❌ "{term}": 查询失败 - {e}')

# 直接检查一些可能的ID
print('\n🔍 直接检查可能的FRED ID:')
print('=' * 50)

possible_ids = [
    'DRTSCILM', 'DRTSCIS',  # 银行贷款标准
    'IC4WSA', 'ICSA',       # 初请失业金
    'NEWORDER',             # 新订单
    'PERMIT', 'PERMITNSA',  # 建筑许可
    'AWHMAN',               # 制造业工时
    'DRTSCILM',             # 重复检查
]

for indicator in possible_ids:
    try:
        result = series_search(indicator)
        if result and 'series' in result and result['series']:
            series_info = result['series'][0]
            title = series_info.get('title', 'Unknown')
            frequency = series_info.get('frequency_short', 'Unknown')
            print(f'✅ {indicator}: {title}')
            print(f'   频率: {frequency}')
        else:
            print(f'❌ {indicator}: 未找到')
        time.sleep(0.5)
    except Exception as e:
        print(f'❌ {indicator}: 查询失败 - {e}')
