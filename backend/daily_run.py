"""
AI交易助手 — 每日定时任务

使用方式：
    python daily_run.py               # 执行今日全流程
    python daily_run.py --briefing    # 仅生成早盘速览
    python daily_run.py --monitor     # 仅执行持仓体检
    python daily_run.py --sync        # 仅触发券商同步
    python daily_run.py --no-ai       # 跳过 AI 生成（节省 token）

推荐在 crontab 中配置：
    # 工作日早上 09:31 执行（开盘后1分钟）
    31 9 * * 1-5 cd /path/to/AI交易助手 && python backend/daily_run.py >> logs/daily.log 2>&1
    # 工作日 15:05 执行（收盘后）
    5 15 * * 1-5 cd /path/to/AI交易助手 && python backend/daily_run.py --monitor >> logs/daily.log 2>&1
"""
import argparse
import logging
import sys
import os
from datetime import datetime

# 确保 backend 目录在路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_run")


def step_sync() -> bool:
    """券商账户同步"""
    try:
        from syncer.sync_manager import full_sync
        result = full_sync()
        logger.info("券商同步结果：%s", result.get("status"))
        return result.get("status") in ("success", "skipped")
    except Exception as e:
        logger.error("券商同步出错：%s", e)
        return False


def step_briefing(no_ai: bool = False) -> bool:
    """早盘速览生成"""
    try:
        if no_ai:
            from collectors.market_overview import get_market_overview
            data = get_market_overview(use_cache_hours=0)
            logger.info("早盘市场数据已获取（无AI摘要）：%s", data.get("date"))
        else:
            from analyzers.morning_briefing import generate_today_briefing
            result = generate_today_briefing(force=False)
            logger.info("早盘速览生成完成：%s", result.get("brief_date"))
        return True
    except Exception as e:
        logger.error("早盘速览出错：%s", e)
        return False


def step_monitor() -> bool:
    """持仓体检"""
    try:
        from analyzers.position_monitor import run_monitor
        result = run_monitor()
        logger.info("持仓体检完成：%d 条预警", result.get("alerts", 0))
        return True
    except Exception as e:
        logger.error("持仓体检出错：%s", e)
        return False


def step_radar() -> bool:
    """量化雷达数据采集（调用量化雷达项目的 daily_run 逻辑）"""
    try:
        # 量化雷达的收集器通过 api_server 的雷达接口提供数据
        # 若量化雷达 backend 在同一环境，可直接调用其 daily_run
        radar_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "..", "量化雷达", "backend"
        )
        if os.path.exists(radar_path):
            sys.path.insert(0, os.path.abspath(radar_path))
            from daily_run import run_all
            run_all()
            logger.info("量化雷达数据采集完成")
        else:
            logger.info("量化雷达目录不存在，跳过（路径：%s）", radar_path)
        return True
    except Exception as e:
        logger.warning("量化雷达采集出错（非致命）：%s", e)
        return True  # 非致命，不中断主流程


def run_all(args) -> None:
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("AI交易助手 每日任务开始：%s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    steps = []

    if args.sync or not any([args.briefing, args.monitor]):
        ok = step_sync()
        steps.append(("券商同步", ok))

    if args.radar or not any([args.briefing, args.monitor, args.sync]):
        ok = step_radar()
        steps.append(("量化雷达", ok))

    if args.briefing or not any([args.monitor, args.sync, args.radar]):
        ok = step_briefing(no_ai=args.no_ai)
        steps.append(("早盘速览", ok))

    if args.monitor or not any([args.briefing, args.sync, args.radar]):
        ok = step_monitor()
        steps.append(("持仓体检", ok))

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=" * 60)
    logger.info("任务完成，耗时 %.1f 秒", elapsed)
    for name, ok in steps:
        logger.info("  %-12s %s", name, "✓" if ok else "✗ 失败")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI交易助手每日定时任务")
    parser.add_argument("--briefing", action="store_true", help="仅生成早盘速览")
    parser.add_argument("--monitor",  action="store_true", help="仅执行持仓体检")
    parser.add_argument("--sync",     action="store_true", help="仅触发券商同步")
    parser.add_argument("--radar",    action="store_true", help="仅运行量化雷达采集")
    parser.add_argument("--no-ai",    action="store_true", help="跳过 AI 生成")
    args = parser.parse_args()
    run_all(args)
