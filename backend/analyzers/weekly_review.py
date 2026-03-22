"""
周度复盘生成器

功能：
1. 统计本周成交（盈亏、胜率、偏差最大交易）
2. 持仓健康评分
3. 调用 AI 生成周度复盘报告
4. 写入 weekly_reviews 表
"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn
from analyzers.ai_engine import generate_weekly_review as _ai_generate_weekly_review

logger = logging.getLogger(__name__)


def _get_week_range(week_start: Optional[str] = None) -> tuple[str, str]:
    """返回 (week_start, week_end) 字符串，默认本周周一~周日"""
    if week_start:
        ws = datetime.strptime(week_start, "%Y-%m-%d").date()
    else:
        today = date.today()
        ws = today - timedelta(days=today.weekday())  # 周一
    we = ws + timedelta(days=6)
    return ws.strftime("%Y-%m-%d"), we.strftime("%Y-%m-%d")


def _get_week_trades(start: str, end: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT t.*, v.overall_score, v.user_thesis
            FROM trade_log t
            LEFT JOIN validation_records v ON t.validation_id = v.id
            WHERE t.trade_date BETWEEN ? AND ?
            ORDER BY t.trade_date, t.id
        """, (start, end)).fetchall()
    return [dict(r) for r in rows]


def _calc_pnl(trades: list[dict]) -> dict:
    """粗略盈亏统计（卖出金额 - 买入金额 - 手续费）"""
    buy_amount  = sum(t["amount"] for t in trades if t["direction"] == "buy")
    sell_amount = sum(t["amount"] for t in trades if t["direction"] == "sell")
    commission  = sum(float(t.get("commission") or 0) for t in trades)
    rough_pnl   = sell_amount - buy_amount - commission

    validated_sells = [t for t in trades if t["direction"] == "sell" and t.get("overall_score")]
    win_count = sum(1 for t in validated_sells if float(t["overall_score"] or 0) >= 60)
    win_rate  = win_count / len(validated_sells) * 100 if validated_sells else None

    return {
        "buy_amount":  round(buy_amount, 2),
        "sell_amount": round(sell_amount, 2),
        "commission":  round(commission, 2),
        "rough_pnl":   round(rough_pnl, 2),
        "trade_count": len(trades),
        "win_rate":    round(win_rate, 1) if win_rate is not None else None,
    }


def _get_worst_deviation(trades: list[dict]) -> Optional[dict]:
    """找出验证分数最低（执行偏差最大）的交易"""
    scored = [t for t in trades if t.get("overall_score") is not None]
    if not scored:
        return None
    worst = min(scored, key=lambda t: float(t["overall_score"] or 100))
    return {
        "trade_date": worst["trade_date"],
        "stock_code": worst["stock_code"],
        "stock_name": worst.get("stock_name", ""),
        "direction":  worst["direction"],
        "score":      worst["overall_score"],
        "thesis":     worst.get("user_thesis", ""),
    }


def _get_portfolio_health() -> dict:
    """持仓健康快照"""
    with get_conn() as conn:
        positions = conn.execute(
            "SELECT stock_code, stock_name, current_weight, buy_price FROM user_positions WHERE is_active=1"
        ).fetchall()
        alert_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM position_alerts WHERE is_read=0"
        ).fetchone()["cnt"]

    return {
        "position_count": len(positions),
        "unread_alerts":  alert_count,
        "positions":      [dict(p) for p in positions],
    }


def generate_weekly_review(week_start: Optional[str] = None, force: bool = False) -> dict:
    """生成周度复盘（api_server 通过此名称调用）"""
    ws, we = _get_week_range(week_start)

    # 检查已有
    if not force:
        with get_conn() as conn:
            existing = conn.execute(
                "SELECT id, ai_report FROM weekly_reviews WHERE week_start=?", (ws,)
            ).fetchone()
        if existing and existing["ai_report"]:
            return get_review(ws) or {"week_start": ws, "week_end": we, "from_cache": True}

    trades = _get_week_trades(ws, we)
    pnl    = _calc_pnl(trades)
    worst  = _get_worst_deviation(trades)
    health = _get_portfolio_health()

    low_score_count = len([t for t in trades if float(t.get("overall_score") or 100) < 50])

    # AI 复盘（返回字符串）
    review_data = {
        "total_pnl":            pnl["rough_pnl"],
        "total_pnl_percent":    0,
        "benchmark_return":     0,
        "alpha":                0,
        "win_rate":             pnl["win_rate"] or 0,
        "profit_loss_ratio":    0,
        "avg_validation_score": None,
        "low_score_trades":     low_score_count,
        "behavior_flags":       [],
        "trade_details": [
            {k: t.get(k) for k in
             ("trade_date", "stock_code", "stock_name", "direction", "price", "shares", "overall_score")}
            for t in trades[:20]
        ],
    }
    ai_report: str = _ai_generate_weekly_review(review_data)

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO weekly_reviews
                (week_start, week_end, total_pnl, win_rate, low_score_trades, ai_report, created_at)
            VALUES (?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(week_start) DO UPDATE SET
                total_pnl=excluded.total_pnl,
                win_rate=excluded.win_rate,
                low_score_trades=excluded.low_score_trades,
                ai_report=excluded.ai_report,
                created_at=excluded.created_at
        """, (ws, we, pnl["rough_pnl"], pnl["win_rate"], low_score_count, ai_report))

    return {
        "week_start":       ws,
        "week_end":         we,
        "from_cache":       False,
        "pnl":              pnl,
        "worst_deviation":  worst,
        "portfolio_health": health,
        "ai_report":        ai_report,
    }


def get_review(week_start: str) -> Optional[dict]:
    """读取指定周复盘"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM weekly_reviews WHERE week_start=?", (week_start,)
        ).fetchone()
    if not row:
        return None
    return {
        "week_start":       row["week_start"],
        "week_end":         row["week_end"],
        "total_pnl":        row["total_pnl"],
        "win_rate":         row["win_rate"],
        "low_score_trades": row["low_score_trades"],
        "ai_report":        row["ai_report"] or "",
        "from_cache":       True,
    }


def get_latest_review() -> Optional[dict]:
    """获取最近一周复盘"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT week_start FROM weekly_reviews ORDER BY week_start DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    return get_review(row["week_start"])
