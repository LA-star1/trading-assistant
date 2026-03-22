"""
早盘速览生成器

流程：
1. 收集市场概览（指数、北向、板块）
2. 检查用户持仓的预警信号
3. 调用 AI 生成摘要（JSON 格式）
4. 写入 morning_briefings 表
"""
import json
import logging
from datetime import date, datetime
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn
from collectors.market_overview import get_market_overview
from analyzers.ai_engine import generate_morning_briefing as _ai_generate_briefing

logger = logging.getLogger(__name__)


def _get_position_alerts_summary() -> list[dict]:
    """获取今日未读持仓预警摘要"""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT pa.stock_code, pa.stock_name, pa.alert_type, pa.description,
                   pa.severity, up.current_weight
            FROM position_alerts pa
            LEFT JOIN user_positions up ON pa.stock_code = up.stock_code AND up.is_active=1
            WHERE pa.is_read=0
            ORDER BY pa.severity DESC, pa.created_at DESC
            LIMIT 10
        """).fetchall()
    return [dict(r) for r in rows]


def _get_user_positions_brief() -> list[dict]:
    """获取持仓列表（用于 AI 上下文）"""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT stock_code, stock_name, shares, current_weight, buy_price
            FROM user_positions WHERE is_active=1
            ORDER BY current_weight DESC
            LIMIT 15
        """).fetchall()
    return [dict(r) for r in rows]


def generate_today_briefing(force: bool = False) -> dict:
    """
    生成今日早盘速览。
    force=True 时强制重新生成（覆盖已有）。
    """
    today = date.today().isoformat()

    # 检查已有
    if not force:
        with get_conn() as conn:
            existing = conn.execute(
                "SELECT us_market_summary, ai_summary, ai_focus_points FROM morning_briefings WHERE briefing_date=?",
                (today,)
            ).fetchone()
        if existing and existing["ai_summary"]:
            result: dict = {"briefing_date": today, "from_cache": True,
                            "ai_summary": existing["ai_summary"]}
            if existing["ai_focus_points"]:
                try:
                    result["ai_focus_points"] = json.loads(existing["ai_focus_points"])
                except Exception:
                    result["ai_focus_points"] = []
            if existing["us_market_summary"]:
                try:
                    result["market_data"] = json.loads(existing["us_market_summary"])
                except Exception:
                    result["market_data"] = {}
            return result

    # 1. 市场数据
    market = get_market_overview(use_cache_hours=0)

    # 2. 持仓预警
    alerts = _get_position_alerts_summary()

    # 3. 持仓列表
    positions = _get_user_positions_brief()

    # 4. 从量化雷达库取今日信号
    with get_conn() as conn:
        signals = conn.execute("""
            SELECT stock_code, stock_name, signal_type, score
            FROM quant_signals
            WHERE trade_date=? AND score >= 60
            ORDER BY score DESC LIMIT 5
        """, (today,)).fetchall()
    radar_signals = [dict(r) for r in signals]

    # 5. 组装 AI 上下文
    context = {
        "positions":     positions,
        "watchlist":     [],
        "overnight":     market,
        "position_news": [f"{a.get('alert_type','')}: {a.get('description','')}" for a in alerts],
        "northbound":    market.get("northbound", {}),
        "catalysts":     [
            f"{s.get('stock_name','')} {s.get('signal_type','')} 评分{s.get('score','')}"
            for s in radar_signals
        ],
    }
    ai_result = _ai_generate_briefing(context)
    # ai_result = {"ai_summary": str, "ai_focus_points": list[str]}

    # 6. 写库
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO morning_briefings
                (briefing_date, us_market_summary, ai_summary, ai_focus_points, created_at)
            VALUES (?,?,?,?,datetime('now'))
            ON CONFLICT(briefing_date) DO UPDATE SET
                us_market_summary=excluded.us_market_summary,
                ai_summary=excluded.ai_summary,
                ai_focus_points=excluded.ai_focus_points,
                created_at=excluded.created_at
        """, (
            today,
            json.dumps(market, ensure_ascii=False),
            ai_result.get("ai_summary", ""),
            json.dumps(ai_result.get("ai_focus_points", []), ensure_ascii=False),
        ))

    return {
        "briefing_date":  today,
        "from_cache":     False,
        "market_data":    market,
        "ai_summary":     ai_result.get("ai_summary", ""),
        "ai_focus_points": ai_result.get("ai_focus_points", []),
    }


def get_briefing(target_date: Optional[str] = None) -> Optional[dict]:
    """获取指定日期早盘速览（默认今日）"""
    target = target_date or date.today().isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM morning_briefings WHERE briefing_date=?", (target,)
        ).fetchone()
    if not row:
        return None

    result: dict = {
        "briefing_date":  target,
        "ai_summary":     row["ai_summary"] or "",
    }
    if row["ai_focus_points"]:
        try:
            result["ai_focus_points"] = json.loads(row["ai_focus_points"])
        except Exception:
            result["ai_focus_points"] = []
    if row["us_market_summary"]:
        try:
            result["market_data"] = json.loads(row["us_market_summary"])
        except Exception:
            result["market_data"] = {}
    return result
