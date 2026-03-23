import time, json, os, glob, sys
sys.path.insert(0, ".")

print("=" * 50)
print("报告生成瓶颈诊断")
print("=" * 50)

# 1. fragility 计算速度
print("\n[1] fragility 计算速度...")
latest = max(glob.glob("outputs/crisis_monitor/crisis_report_*.json"),
             key=os.path.getmtime)
j = json.load(open(latest, encoding="utf-8"))

import importlib.util
spec = importlib.util.spec_from_file_location("cm", "crisis_monitor_v2.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

start = time.time()
for _ in range(1000):
    mod.compute_fragility_state(j, j.get("summary", {}))
elapsed = time.time() - start
print(f"    1000次: {elapsed:.3f}s  单次: {elapsed:.4f}ms  ← 可忽略")

# 2. Yahoo Finance 串行拉取速度
print("\n[2] Yahoo Finance 响应时间（串行）...")
import yfinance as yf
tickers = ["SPY", "HYG", "LQD", "KRE", "XLF", "GLD", "^VIX"]
for t in tickers:
    start = time.time()
    try:
        yf.Ticker(t).history(period="5d")
        print(f"    {t:<8} {(time.time()-start):.2f}s")
    except Exception as e:
        print(f"    {t:<8} 失败: {e}")

# 3. Playwright 是否存在
print("\n[3] Playwright 截图功能...")
try:
    from playwright.sync_api import sync_playwright
    print("    已安装 ← 生成长图截图，通常耗时 30-120秒/次")
except ImportError:
    print("    未安装")

# 4. AI narrator
print("\n[4] AI narrator 状态...")
skip = os.environ.get("CRISIS_MONITOR_SKIP_AI_NARRATOR", "未设置")
print(f"    CRISIS_MONITOR_SKIP_AI_NARRATOR = {skip}")
if skip != "1":
    print("    ← AI叙事未跳过，每次调用 Groq/OpenAI 约 5-30秒")

print("\n" + "=" * 50)
print("诊断完成")
print("=" * 50)

