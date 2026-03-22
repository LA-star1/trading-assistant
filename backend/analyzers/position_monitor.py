"""
持仓体检监控器

定期扫描持仓，发现以下情况时生成预警：
- 止损线触发（跌幅超过 stop_loss_pct）
- 单仓超重（超过 max_position_weight）
- 板块集中度过高（同板块持仓比例之和 > 60%）
- 量化雷达反向信号（持仓股出现卖出信号）
"""
import json
import logging
from datetime import date, datetime
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, get_user_config, get_active_positions
from collectors.stock_fundamentals import get_daily_history, get_stock_info

logger = logging.getLogger(__name__)


def _parse_config(raw: dict) -> dict:
    """将 user_config 字符串值转为浮点数"""
    return {
        "total_capital":       float(raw.get("total_capital", 1_500_000)),
        "max_single_loss":     float(raw.get("max_single_loss", 100_000)),
        "max_position_weight": float(raw.get("max_position_weight", 25)),
        "stop_loss_pct":       float(raw.get("stop_loss_pct", 7)),
    }


def _get_latest_price(stock_code: str) -> Optional[float]:
    """从缓存或实时获取最新收盘价"""
    try:
        hist = get_daily_history(stock_code, days=2)
        if not hist.empty:
            return float(hist.iloc[-1]["close"] or 0)
    except Exception as e:
        logger.warning("获取最新价失败 %s：%s", stock_code, e)
    return None


def _write_alert(stock_code: str, stock_name: str, alert_type: str,
                 message: str, severity: str = "medium"):
    """写入 position_alerts 表（同类预警当天去重）"""
    today = date.today().isoformat()
    with get_conn() as conn:
        existing = conn.execute("""
            SELECT id FROM position_alerts
            WHERE stock_code=? AND alert_type=? AND alert_date=?
        """, (stock_code, alert_type, today)).fetchone()
        if existing:
            return  # 今日同类预警已存在
        conn.execute("""
            INSERT INTO position_alerts
                (alert_date, stock_code, stock_name, alert_type, description, severity, is_read)
            VALUES (?,?,?,?,?,?,0)
        """, (today, stock_code, stock_name, alert_type, message, severity))
    logger.info("写入预警 [%s] %s %s", severity, stock_code, alert_type)


def check_stop_loss(positions: list[dict], config: dict) -> int:
    """检查止损触发"""
    count = 0
    stop_pct = config["stop_loss_pct"] / 100

    for p in positions:
        buy_price = float(p.get("buy_price") or 0)
        if buy_price <= 0:
            continue
        price = _get_latest_price(p["stock_code"])
        if not price:
            continue

        loss_pct = (price - buy_price) / buy_price
        if loss_pct <= -stop_pct:
            _write_alert(
                p["stock_code"], p["stock_name"] or p["stock_code"],
                "stop_loss",
                f"当前价 {price:.2f}，买入价 {buy_price:.2f}，"
                f"跌幅 {loss_pct*100:.1f}% 已触达止损线（{config['stop_loss_pct']}%）",
                severity="high",
            )
            count += 1
    return count


def check_overweight(positions: list[dict], config: dict) -> int:
    """检查单仓超重"""
    max_weight = config["max_position_weight"]
    count = 0
    for p in positions:
        w = float(p.get("current_weight") or 0)
        if w > max_weight:
            _write_alert(
                p["stock_code"], p["stock_name"] or p["stock_code"],
                "overweight",
                f"当前仓位 {w:.1f}%，超过最大单仓限制 {max_weight:.0f}%，请考虑减仓",
                severity="medium",
            )
            count += 1
    return count


def check_sector_concentration(positions: list[dict]) -> int:
    """检查板块集中度"""
    sector_weight: dict[str, float] = {}
    sector_stocks: dict[str, list[str]] = {}

    for p in positions:
        code = p["stock_code"]
        w = float(p.get("current_weight") or 0)
        try:
            sector = get_stock_info(code).get("sector") or "未知"
        except Exception:
            sector = "未知"
        sector_weight[sector] = sector_weight.get(sector, 0) + w
        sector_stocks.setdefault(sector, []).append(code)

    count = 0
    for sector, total_w in sector_weight.items():
        if total_w > 60:
            stocks_str = "、".join(sector_stocks.get(sector, []))
            _write_alert(
                "PORTFOLIO", "组合",
                "sector_concentration",
                f"{sector} 板块持仓合计 {total_w:.1f}%，集中度过高（>60%），涉及：{stocks_str}",
                severity="medium",
            )
            count += 1
    return count


def check_radar_signals(positions: list[dict]) -> int:
    """检查量化雷达对持仓股的反向信号"""
    today = date.today().isoformat()
    position_codes = {p["stock_code"] for p in positions}

    with get_conn() as conn:
        signals = conn.execute("""
            SELECT stock_code, stock_name, signal_type, score
            FROM quant_signals
            WHERE trade_date=? AND signal_type='quant_sell' AND score >= 65
        """, (today,)).fetchall()

    count = 0
    for sig in signals:
        code = sig["stock_code"]
        if code in position_codes:
            _write_alert(
                code, sig["stock_name"] or code,
                "radar_signal",
                f"量化雷达今日出现卖出信号（评分 {sig['score']}）",
                severity="high" if sig["score"] >= 80 else "medium",
            )
            count += 1
    return count


def run_monitor() -> dict:
    """执行全量持仓体检"""
    config    = _parse_config(get_user_config())
    positions = get_active_positions()

    if not positions:
        return {"status": "ok", "message": "当前无持仓", "alerts": 0}

    sl_count  = check_stop_loss(positions, config)
    ow_count  = check_overweight(positions, config)
    sc_count  = check_sector_concentration(positions)
    rad_count = check_radar_signals(positions)

    total = sl_count + ow_count + sc_count + rad_count
    logger.info("持仓体检完成：共 %d 条预警（止损%d 超重%d 集中%d 雷达%d）",
                total, sl_count, ow_count, sc_count, rad_count)

    return {
        "status": "ok",
        "alerts": total,
        "detail": {
            "stop_loss":            sl_count,
            "overweight":           ow_count,
            "sector_concentration": sc_count,
            "radar_signal":         rad_count,
        },
        "checked_at": datetime.now().isoformat(),
    }
