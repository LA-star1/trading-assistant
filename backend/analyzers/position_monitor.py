"""
持仓体检监控器

定期扫描持仓，发现以下情况时生成预警：
- 止损线触发（跌幅超过 max_single_loss 对应比例）
- 单仓超重（超过 max_position_ratio）
- 板块集中度过高（同板块持仓比例之和 > 60%）
- 量化雷达反向信号（持仓股出现卖出信号）
"""
import logging
from datetime import date, datetime
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn
from collectors.stock_fundamentals import get_daily_history, get_stock_info

logger = logging.getLogger(__name__)


def _get_user_config() -> dict:
    with get_conn() as conn:
        rows = conn.execute("SELECT key, value FROM user_config").fetchall()
    cfg = {r["key"]: r["value"] for r in rows}
    return {
        "total_capital":     float(cfg.get("total_capital", 1500000)),
        "max_single_loss":   float(cfg.get("max_single_loss", 100000)),
        "max_position_ratio": float(cfg.get("max_position_ratio", 30)),
        "stop_loss_pct":     float(cfg.get("stop_loss_pct", 7)),
    }


def _get_active_positions() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM user_positions WHERE is_active=1"
        ).fetchall()
    return [dict(r) for r in rows]


def _get_latest_price(stock_code: str) -> Optional[float]:
    """从缓存或实时获取最新价"""
    try:
        hist = get_daily_history(stock_code, days=2)
        if hist and len(hist) >= 1:
            return float(hist[-1].get("close", 0) or 0)
    except Exception:
        pass
    return None


def _write_alert(stock_code: str, stock_name: str, alert_type: str,
                 message: str, severity: str = "medium",
                 extra_data: Optional[dict] = None):
    """写入 position_alerts 表（同类预警当天去重）"""
    today = date.today().isoformat()
    import json
    with get_conn() as conn:
        existing = conn.execute("""
            SELECT id FROM position_alerts
            WHERE stock_code=? AND alert_type=? AND DATE(created_at)=?
        """, (stock_code, alert_type, today)).fetchone()
        if existing:
            return  # 今日同类预警已存在

        conn.execute("""
            INSERT INTO position_alerts
                (stock_code, stock_name, alert_type, alert_message, severity, extra_data, is_read, created_at)
            VALUES (?,?,?,?,?,?,0,datetime('now'))
        """, (
            stock_code, stock_name, alert_type, message, severity,
            json.dumps(extra_data, ensure_ascii=False) if extra_data else None
        ))
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
                extra_data={"current_price": price, "buy_price": buy_price, "loss_pct": loss_pct},
            )
            count += 1
    return count


def check_overweight(positions: list[dict], config: dict) -> int:
    """检查单仓超重"""
    max_ratio = config["max_position_ratio"]
    count = 0
    for p in positions:
        w = float(p.get("current_weight") or 0)
        if w > max_ratio:
            _write_alert(
                p["stock_code"], p["stock_name"] or p["stock_code"],
                "overweight",
                f"当前仓位 {w:.1f}%，超过最大单仓限制 {max_ratio:.0f}%，请考虑减仓",
                severity="medium",
                extra_data={"current_weight": w, "max_ratio": max_ratio},
            )
            count += 1
    return count


def check_sector_concentration(positions: list[dict]) -> int:
    """检查板块集中度"""
    sector_weight: dict[str, float] = {}
    sector_stocks: dict[str, list[str]] = {}
    count = 0

    for p in positions:
        code = p["stock_code"]
        w = float(p.get("current_weight") or 0)
        try:
            info = get_stock_info(code)
            sector = info.get("industry") or "未知"
        except Exception:
            sector = "未知"
        sector_weight[sector] = sector_weight.get(sector, 0) + w
        sector_stocks.setdefault(sector, []).append(code)

    for sector, total_w in sector_weight.items():
        if total_w > 60:
            stocks_str = "、".join(sector_stocks.get(sector, []))
            _write_alert(
                "PORTFOLIO", "组合",
                "sector_concentration",
                f"{sector} 板块持仓合计 {total_w:.1f}%，集中度过高（>60%），涉及：{stocks_str}",
                severity="medium",
                extra_data={"sector": sector, "total_weight": total_w},
            )
            count += 1
    return count


def check_radar_signals(positions: list[dict]) -> int:
    """检查量化雷达对持仓股的反向信号"""
    today = date.today().isoformat()
    count = 0
    position_codes = {p["stock_code"] for p in positions}
    pos_map = {p["stock_code"]: p for p in positions}

    with get_conn() as conn:
        signals = conn.execute("""
            SELECT stock_code, stock_name, signal_type, score, reason
            FROM quant_signals
            WHERE signal_date=? AND signal_type='sell' AND score >= 65
        """, (today,)).fetchall()

    for sig in signals:
        code = sig["stock_code"]
        if code in position_codes:
            p = pos_map[code]
            _write_alert(
                code, sig["stock_name"] or code,
                "radar_signal",
                f"量化雷达今日出现卖出信号（评分 {sig['score']}），原因：{sig['reason']}",
                severity="high" if sig["score"] >= 80 else "medium",
                extra_data={"signal_type": sig["signal_type"], "score": sig["score"]},
            )
            count += 1
    return count


def run_monitor() -> dict:
    """执行全量持仓体检"""
    config    = _get_user_config()
    positions = _get_active_positions()

    if not positions:
        return {"status": "ok", "message": "当前无持仓", "alerts": 0}

    sl_count   = check_stop_loss(positions, config)
    ow_count   = check_overweight(positions, config)
    sc_count   = check_sector_concentration(positions)
    rad_count  = check_radar_signals(positions)

    total = sl_count + ow_count + sc_count + rad_count
    logger.info("持仓体检完成：共 %d 条预警（止损%d 超重%d 集中%d 雷达%d）",
                total, sl_count, ow_count, sc_count, rad_count)

    return {
        "status": "ok",
        "alerts": total,
        "detail": {
            "stop_loss": sl_count,
            "overweight": ow_count,
            "sector_concentration": sc_count,
            "radar_signal": rad_count,
        },
        "checked_at": datetime.now().isoformat(),
    }
