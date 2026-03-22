"""
周度复盘生成器

功能：
1. 统计本周成交（盈亏、胜率、偏差最大交易）
2. 持仓健康评分
3. 调用 AI 生成周度复盘报告
4. 写入 weekly_reviews 表
"""
import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn
from analyzers.ai_engine import generate_weekly_review

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
    """
    简单盈亏统计（以卖出成交的 amount 减去对应买入成本估算）
    这里用 buy/sell 金额对比估算周度盈亏
    """
    buy_amount  = sum(t["amount"] for t in trades if t["direction"] == "buy")
    sell_amount = sum(t["amount"] for t in trades if t["direction"] == "sell")
    commission  = sum(float(t.get("commission") or 0) for t in trades)

    # 粗略盈亏 = 卖出金额 - 买入金额 - 手续费（不考虑期末持仓浮盈）
    rough_pnl = sell_amount - buy_amount - commission

    # 胜率：有验证评分且方向为卖出的交易，评分 >= 60 视为"胜"
    validated_sells = [t for t in trades if t["direction"] == "sell" and t.get("overall_score")]
    win_count = sum(1 for t in validated_sells if float(t["overall_score"] or 0) >= 60)
    win_rate  = win_count / len(validated_sells) if validated_sells else None

    return {
        "buy_amount":   round(buy_amount, 2),
        "sell_amount":  round(sell_amount, 2),
        "commission":   round(commission, 2),
        "rough_pnl":    round(rough_pnl, 2),
        "trade_count":  len(trades),
        "win_rate":     round(win_rate * 100, 1) if win_rate is not None else None,
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
        "position_count":  len(positions),
        "unread_alerts":   alert_count,
        "positions": [dict(p) for p in positions],
    }


def generate_weekly(week_start: Optional[str] = None, force: bool = False) -> dict:
    """生成周度复盘"""
    ws, we = _get_week_range(week_start)

    # 检查已有
    if not force:
        with get_conn() as conn:
            existing = conn.execute(
                "SELECT id, ai_content FROM weekly_reviews WHERE week_start=?", (ws,)
            ).fetchone()
        if existing and existing["ai_content"]:
            result = {"week_start": ws, "week_end": we, "from_cache": True}
            try:
                result.update(json.loads(existing["ai_content"]))
            except Exception:
                result["ai_content"] = existing["ai_content"]
            return result

    # 数据汇总
    trades  = _get_week_trades(ws, we)
    pnl     = _calc_pnl(trades)
    worst   = _get_worst_deviation(trades)
    health  = _get_portfolio_health()

    summary_data = {
        "week_start":       ws,
        "week_end":         we,
        "pnl":              pnl,
        "worst_deviation":  worst,
        "portfolio_health": health,
        "trades":           trades[:20],  # 最多传 20 条给 AI
    }

    # AI 复盘
    ai_result = generate_weekly_review(summary_data)

    # 写库
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO weekly_reviews (week_start, week_end, trade_stats, ai_content, created_at)
            VALUES (?,?,?,?,datetime('now'))
            ON CONFLICT(week_start) DO UPDATE SET
                trade_stats=excluded.trade_stats,
                ai_content=excluded.ai_content,
                created_at=excluded.created_at
        """, (ws, we,
              json.dumps(pnl, ensure_ascii=False),
              json.dumps(ai_result, ensure_ascii=False)))

    result = {
        "week_start": ws,
        "week_end":   we,
        "from_cache": False,
        "pnl":        pnl,
        "worst_deviation": worst,
        "portfolio_health": health,
    }
    result.update(ai_result)
    return result


def get_review(week_start: str) -> Optional[dict]:
    """读取指定周复盘"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM weekly_reviews WHERE week_start=?", (week_start,)
        ).fetchone()
    if not row:
        return None
    result = {"week_start": week_start, "week_end": row["week_end"]}
    if row["trade_stats"]:
        result["pnl"] = json.loads(row["trade_stats"])
    if row["ai_content"]:
        try:
            result.update(json.loads(row["ai_content"]))
        except Exception:
            result["ai_content"] = row["ai_content"]
    return result


def get_latest_review() -> Optional[dict]:
    """获取最近一周复盘"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT week_start FROM weekly_reviews ORDER BY week_start DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    return get_review(row["week_start"])
