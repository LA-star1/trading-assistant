"""
量化雷达 — 每日执行主脚本

使用方式：
    python daily_run.py                    # 采集今天的数据
    python daily_run.py --date 2026-03-21  # 补采指定日期
    python daily_run.py --backfill 30      # 回填最近30个交易日

执行顺序（每日）：
    1. 检查是否为交易日
    2. 采集龙虎榜数据
    3. 采集北向资金数据（如已实现）
    4. 采集大宗交易数据（如已实现）
    5. 计算因子指标（如已实现）
    6. 量化席位分析 → 信号评分
    7. 输出今日雷达报告
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

# 确保后端根目录在 sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import REPORT_DIR
import db as _db

logger = logging.getLogger(__name__)


# ── 日志配置 ──────────────────────────────────────────────────────────────────

def setup_logging(log_date: str):
    log_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "logs",
        f"daily_{log_date}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


# ── 交易日判断 ────────────────────────────────────────────────────────────────

def is_trade_day(check_date: date) -> bool:
    """
    粗略判断是否为A股交易日：
    - 排除周六、周日
    - 节假日用 AKShare 的 tool_trade_date_hist_sina() 查询

    如果 AKShare 查询失败，退化为仅排除周末。
    """
    if check_date.weekday() >= 5:  # 0=周一 … 6=周日
        return False

    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        # 返回 DataFrame，列名为 "trade_date"（日期对象）
        trade_dates = set(df["trade_date"].astype(str).tolist())
        date_str = check_date.strftime("%Y-%m-%d")
        return date_str in trade_dates
    except Exception as e:
        logger.warning("交易日历查询失败（退化为仅排除周末）：%s", e)
        return True  # 默认当作交易日处理，让采集器自己处理空数据


def get_recent_trade_days(n: int) -> list[str]:
    """返回最近 n 个交易日的日期字符串列表（降序，最新在前）"""
    result = []
    current = date.today()
    checked = 0
    while len(result) < n and checked < n * 3:
        if is_trade_day(current):
            result.append(current.strftime("%Y-%m-%d"))
        current -= timedelta(days=1)
        checked += 1
    return result


# ── 各采集器调用（带错误隔离） ─────────────────────────────────────────────────

def run_dragon_tiger(trade_date: str) -> dict:
    """运行龙虎榜采集器"""
    logger.info("▶ 采集龙虎榜数据…")
    try:
        from collectors.dragon_tiger import collect_dragon_tiger
        n = collect_dragon_tiger(trade_date)
        return {"status": "ok", "written": n}
    except Exception as e:
        logger.error("龙虎榜采集失败：%s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


def run_north_bound(trade_date: str) -> dict:
    """运行北向资金采集器"""
    logger.info("▶ 采集北向资金数据…")
    try:
        from collectors.north_bound import collect_north_bound
        n = collect_north_bound(trade_date)
        return {"status": "ok", "written": n}
    except Exception as e:
        logger.error("北向资金采集失败：%s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


def run_block_trade(trade_date: str) -> dict:
    """运行大宗交易采集器"""
    logger.info("▶ 采集大宗交易数据…")
    try:
        from collectors.block_trade import collect_block_trade
        n = collect_block_trade(trade_date)
        return {"status": "ok", "written": n}
    except Exception as e:
        logger.error("大宗交易采集失败：%s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


def run_factor_monitor(trade_date: str) -> dict:
    """运行因子监测"""
    logger.info("▶ 计算因子指标…")
    try:
        from collectors.factor_monitor import collect_factor_monitor
        ok = collect_factor_monitor(trade_date)
        return {"status": "ok" if ok else "warn", "written": 1 if ok else 0}
    except Exception as e:
        logger.error("因子监测失败：%s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


def run_seat_tracker(trade_date: str) -> dict:
    """运行量化席位分析"""
    logger.info("▶ 量化席位分析…")
    try:
        from analyzers.seat_tracker import analyze_date
        report = analyze_date(trade_date)
        return {"status": "ok", "report": report}
    except Exception as e:
        logger.error("席位分析失败：%s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


# ── 单日完整流程 ───────────────────────────────────────────────────────────────

def run_one_day(trade_date: str, skip_trade_day_check: bool = False) -> dict:
    """
    运行指定交易日的完整数据采集与分析流程。
    返回各步骤执行结果的汇总字典。
    """
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("量化雷达日任务开始：%s", trade_date)
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # 交易日检查
    if not skip_trade_day_check:
        d = datetime.strptime(trade_date, "%Y-%m-%d").date()
        if not is_trade_day(d):
            logger.info("非交易日，跳过：%s", trade_date)
            return {"date": trade_date, "skipped": True, "reason": "非交易日"}

    results = {"date": trade_date, "skipped": False, "steps": {}}

    # Step 1：龙虎榜
    results["steps"]["dragon_tiger"] = run_dragon_tiger(trade_date)
    time.sleep(1)

    # Step 2：北向资金（Phase 2）
    results["steps"]["north_bound"] = run_north_bound(trade_date)

    # Step 3：大宗交易（Phase 2）
    results["steps"]["block_trade"] = run_block_trade(trade_date)

    # Step 4：因子监测（Phase 2）
    results["steps"]["factor_monitor"] = run_factor_monitor(trade_date)

    # Step 5：量化席位分析
    tracker_result = run_seat_tracker(trade_date)
    results["steps"]["seat_tracker"] = tracker_result

    # Step 6：保存报告到文件
    if tracker_result.get("status") == "ok":
        report = tracker_result["report"]
        results["report"] = report
        _save_report(trade_date, report)

    # 打印执行摘要
    _print_summary(results)

    return results


def _save_report(trade_date: str, report: dict):
    """将今日报告保存为 JSON 文件"""
    report_file = os.path.join(REPORT_DIR, f"radar_{trade_date}.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info("报告已保存：%s", report_file)


def _print_summary(results: dict):
    """打印执行摘要"""
    print("\n" + "┄" * 50)
    print(f"  执行摘要 — {results['date']}")
    print("┄" * 50)
    for step, r in results.get("steps", {}).items():
        status = r.get("status", "?")
        icon = {"ok": "✓", "error": "✗", "skipped": "○"}.get(status, "?")
        detail = ""
        if status == "ok" and "written" in r:
            detail = f"写入 {r['written']} 条"
        elif status == "error":
            detail = r.get("error", "")[:50]
        elif status == "skipped":
            detail = r.get("reason", "")
        print(f"  {icon} {step:<20} {detail}")

    report = results.get("report", {})
    if report:
        print(f"\n  量化信号：{report.get('total_signals', 0)} 只个股")
        for msg in report.get("alert_messages", []):
            print(f"  {msg}")
    print("┄" * 50 + "\n")


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="量化雷达 — 每日数据采集与分析")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=str, help="指定采集日期（YYYY-MM-DD）")
    group.add_argument("--backfill", type=int, metavar="N",
                       help="回填最近N个交易日的数据")
    parser.add_argument("--force", action="store_true",
                        help="强制采集（不检查是否为交易日）")
    args = parser.parse_args()

    # 确保数据库已初始化
    _db.init_db()
    _db.insert_seed_seats()

    if args.backfill:
        # 回填模式
        trade_days = get_recent_trade_days(args.backfill)
        logger.info("回填模式：计划采集 %d 个交易日", len(trade_days))
        for i, d in enumerate(reversed(trade_days), 1):  # 从最早开始
            logger.info("回填进度：%d/%d — %s", i, len(trade_days), d)
            run_one_day(d, skip_trade_day_check=True)
            time.sleep(2)  # 回填时稍长间隔，避免触发限频
    else:
        # 单日模式
        target_date = args.date or date.today().strftime("%Y-%m-%d")
        run_one_day(target_date, skip_trade_day_check=bool(args.force or args.date))


if __name__ == "__main__":
    today_str = date.today().strftime("%Y-%m-%d")
    setup_logging(today_str)
    main()
