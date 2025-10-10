from scripts.fred_http import series_search
import time

# 根据FRED API文档，使用更精确的搜索
print('🔍 搜索学术指标 (基于FRED API):')
print('=' * 60)

# 1. 银行贷款标准 (SLOOS)
print('\n📋 1. 银行贷款标准 (SLOOS):')
search_terms_sloos = ['DRTSCILM', 'DRTSCIS', 'loan officer', 'senior loan officer']
for term in search_terms_sloos:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 2. 初请失业金
print('\n📋 2. 初请失业金:')
search_terms_claims = ['IC4WSA', 'ICSA', 'initial claims', 'unemployment insurance']
for term in search_terms_claims:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 3. 制造业新订单
print('\n📋 3. 制造业新订单:')
search_terms_orders = ['NEWORDER', 'new orders', 'manufacturing orders']
for term in search_terms_orders:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 4. 建筑许可
print('\n📋 4. 建筑许可:')
search_terms_permits = ['PERMIT', 'PERMITNSA', 'building permits', 'housing permits']
for term in search_terms_permits:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 5. 制造业工时
print('\n📋 5. 制造业工时:')
search_terms_hours = ['AWHMAN', 'average weekly hours', 'manufacturing hours']
for term in search_terms_hours:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 6. Excess Bond Premium (EBP)
print('\n📋 6. Excess Bond Premium (EBP):')
search_terms_ebp = ['EBP', 'excess bond premium', 'GZ', 'Gilchrist']
for term in search_terms_ebp:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')

# 7. 期限溢价 (ACM Term Premium)
print('\n📋 7. 期限溢价 (ACM Term Premium):')
search_terms_term_premium = ['ACM', 'term premium', 'duration premium']
for term in search_terms_term_premium:
    try:
        result = series_search(term)
        if result and 'series' in result and result['series']:
            print(f'  搜索 "{term}":')
            for series in result['series'][:2]:
                print(f'    {series.get("id", "Unknown")}: {series.get("title", "Unknown")}')
        time.sleep(0.5)
    except Exception as e:
        print(f'  搜索 "{term}" 失败: {e}')
