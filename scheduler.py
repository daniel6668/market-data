#!/usr/bin/env python
"""定时调度器 — 收盘后自动增量更新

用法:
  python scheduler.py              # 前台运行，默认每天 16:00 更新全市场
  python scheduler.py --time 17:00 # 指定时间
  python scheduler.py --market A   # 只更新 A 股
"""
import sys
import os
import logging
import schedule

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

from src.pipeline import Pipeline
from src.utils import load_config, setup_logger

logger = logging.getLogger("market_data")


def run_update(markets: list[str]):
    """执行增量更新"""
    config = load_config()
    logger.info(f"=== 定时更新开始: {markets} ===")
    pipeline = Pipeline(config)
    try:
        for m in markets:
            try:
                result = pipeline.update_market(m)
                logger.info(f"[{m}] 更新完成: {result['success']}/{result['total']} 成功, "
                           f"{result['failed']} 失败")
            except Exception as e:
                logger.error(f"[{m}] 更新异常: {e}")
    finally:
        pipeline.close()
    logger.info("=== 定时更新结束 ===")


def run_phase4_update():
    """Phase 4 增量: 资金流 + 研报 + 财报（收盘后低频运行）"""
    config = load_config()
    logger.info("=== Phase4 采集开始 ===")
    pipeline = Pipeline(config)
    try:
        logger.info("采集资金流...")
        n = pipeline.update_fund_flow("A")
        logger.info(f"  资金流: {n} 条")

        logger.info("采集研报...")
        n = pipeline.update_research("A")
        logger.info(f"  研报: {n} 篇")

        logger.info("采集财报...")
        n = pipeline.update_financials("A")
        logger.info(f"  财报: {n} 期")
    except Exception as e:
        logger.error(f"Phase4 异常: {e}")
    finally:
        pipeline.close()
    logger.info("=== Phase4 采集结束 ===")


def run_phase1_update():
    """Phase 1 增量: 北向 + 融资融券 + 龙虎榜 + 概念板块 + 股东户数"""
    config = load_config()
    logger.info("=== Phase1 采集开始 ===")
    pipeline = Pipeline(config)
    try:
        logger.info("采集北向资金...")
        n = pipeline.update_northbound()
        logger.info(f"  北向: {n} 条")

        logger.info("采集融资融券...")
        n = pipeline.update_margin_trading("A")
        logger.info(f"  融资融券: {n} 条")

        logger.info("采集龙虎榜...")
        n = pipeline.update_dragon_tiger("A")
        logger.info(f"  龙虎榜: {n} 条")

        logger.info("更新概念板块归属...")
        n = pipeline.update_concept_blocks("A")
        logger.info(f"  概念板块: {n} 条")

        logger.info("采集股东户数...")
        n = pipeline.update_holder_num("A")
        logger.info(f"  股东户数: {n} 条")
    except Exception as e:
        logger.error(f"Phase1 异常: {e}")
    finally:
        pipeline.close()
    logger.info("=== Phase1 采集结束 ===")


def main():
    config = load_config()
    setup_logger(config)

    # 参数解析
    run_time = "16:00"
    markets = ["A", "ETF", "HK", "US"]

    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--time" and i + 1 < len(sys.argv):
            run_time = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--market" and i + 1 < len(sys.argv):
            markets = [sys.argv[i + 1]]
            i += 2
        else:
            i += 1

    logger.info(f"调度器启动: 每天 {run_time} 更新 {markets}")
    schedule.every().day.at(run_time).do(run_update, markets=markets)
    schedule.every().day.at("16:10").do(run_phase1_update)
    schedule.every().day.at("16:30").do(run_phase4_update)
    logger.info("  Phase1(北向/融资/龙虎榜/板块/股东): 每天 16:10")
    logger.info("  Phase4(资金流/研报/财报): 每天 16:30")

    # 立即跑一次（可选）
    if "--now" in sys.argv:
        run_update(markets)

    # 保持运行
    logger.info("等待调度中... (Ctrl+C 退出)")
    while True:
        schedule.run_pending()
        import time
        time.sleep(30)


if __name__ == "__main__":
    main()
