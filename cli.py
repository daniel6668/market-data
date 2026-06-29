#!/usr/bin/env python
"""市场数据采集 CLI

用法:
  python cli.py init [--market A|ETF|HK|US|all]
  python cli.py update [--market A|ETF|HK|US|all]
  python cli.py backfill <market> <start_date> <end_date>
  python cli.py status [--market A|ETF|HK|US]
  python cli.py fundflow     ← 全市场资金流采集
  python cli.py research     ← 全市场研报采集
  python cli.py financials   ← 全市场财报采集
"""
import sys
from src.pipeline import Pipeline
from src.utils import load_config


def cmd_init(args):
    pipeline = Pipeline()
    markets = args.get("market", "all")
    if markets == "all":
        markets = ["A", "ETF", "HK", "US"]
    else:
        markets = [markets]
    
    for m in markets:
        result = pipeline.init_market(m)
        print(f"[{m}] 总计: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")
        if result["errors"]:
            print(f"  错误(前5):")
            for e in result["errors"][:5]:
                print(f"    - {e['ts_code']}: {e['error'][:80]}")
    pipeline.close()


def cmd_update(args):
    pipeline = Pipeline()
    try:
        markets = args.get("market", "all")
        if markets == "all":
            markets = ["A", "ETF", "HK", "US"]
        else:
            markets = [markets]

        for m in markets:
            result = pipeline.update_market(m)
            print(f"[{m}] 需要更新: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")
    finally:
        pipeline.close()


def cmd_backfill(args):
    pipeline = Pipeline()
    try:
        result = pipeline.backfill_market(args["market"], args["start"], args["end"])
        print(f"回补完成 — 总计: {result['total']}, 成功: {result['success']}, 失败: {result['failed']}")
    finally:
        pipeline.close()


def cmd_status(args):
    from src.db import get_connection
    conn = get_connection(load_config())
    markets = args.get("market", "all")
    if markets == "all":
        markets = ["A", "ETF", "HK", "US"]
    else:
        markets = [markets]
    
    for m in markets:
        result = conn.execute("""
            SELECT COUNT(*) as total,
                   COUNT(last_sync) as synced,
                   MIN(first_date) as earliest,
                   MAX(last_sync) as latest,
                   SUM(row_count) as total_rows
            FROM sync_status WHERE market = ?
        """, [m]).fetchone()
        print(f"[{m}] 股票数: {result[0]}, 已同步: {result[1]}, "
              f"最早: {result[2]}, 最新: {result[3]}, 总行数: {result[4]}")
    conn.close()


def cmd_fundflow(args):
    pipeline = Pipeline()
    try:
        n = pipeline.update_fund_flow("A")
        print(f"资金流采集完成: {n} 条")
    finally:
        pipeline.close()


def cmd_research(args):
    pipeline = Pipeline()
    try:
        n = pipeline.update_research("A")
        print(f"研报采集完成: {n} 篇")
    finally:
        pipeline.close()


def cmd_financials(args):
    pipeline = Pipeline()
    try:
        n = pipeline.update_financials("A")
        print(f"财报采集完成: {n} 期")
    finally:
        pipeline.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    cmd = sys.argv[1]
    args = {}
    
    # 简单参数解析
    i = 2
    while i < len(sys.argv):
        if sys.argv[i].startswith("--"):
            key = sys.argv[i][2:]
            if i + 1 < len(sys.argv) and not sys.argv[i+1].startswith("--"):
                args[key] = sys.argv[i+1]
                i += 2
            else:
                args[key] = True
                i += 1
        else:
            i += 1
    
    # 位置参数
    pos_args = [a for a in sys.argv[2:] if not a.startswith("--")]
    if cmd == "backfill" and len(pos_args) >= 3:
        args["market"] = pos_args[0]
        args["start"] = pos_args[1]
        args["end"] = pos_args[2]
    
    commands = {
        "init": cmd_init,
        "update": cmd_update,
        "backfill": cmd_backfill,
        "status": cmd_status,
        "fundflow": cmd_fundflow,
        "research": cmd_research,
        "financials": cmd_financials,
    }
    
    if cmd not in commands:
        print(f"未知命令: {cmd}")
        print(__doc__)
        return
    
    try:
        commands[cmd](args)
    except KeyboardInterrupt:
        print("\n中断。")


if __name__ == "__main__":
    main()
