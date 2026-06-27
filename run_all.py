#!/usr/bin/env python
"""一次性跑完四个市场的初始数据拉取"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from src.pipeline import Pipeline

markets = ["US", "ETF", "HK", "A"]
results = {}

pipeline = Pipeline()

for m in markets:
    print(f"\n{'='*50}")
    print(f"===== 开始拉取: {m} =====")
    print(f"{'='*50}\n")
    try:
        r = pipeline.init_market(m)
        results[m] = r
        print(f"\n[{m}] 总计: {r['total']}, 成功: {r['success']}, 失败: {r['failed']}")
        if r['errors']:
            print(f"  错误(前3):")
            for e in r['errors'][:3]:
                print(f"    - {e['ts_code']}: {e['error'][:100]}")
    except Exception as e:
        results[m] = {"error": str(e)}
        print(f"\n[{m}] 异常: {e}")

pipeline.close()

# 打印汇总
print(f"\n{'='*50}")
print("===== 汇总 =====")
for m, r in results.items():
    if "error" in r:
        print(f"  [{m}] ❌ {r['error'][:80]}")
    else:
        print(f"  [{m}] ✅ {r['success']}/{r['total']} 成功, {r['failed']} 失败")
print("===== DONE =====")
