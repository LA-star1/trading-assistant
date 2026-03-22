"""
同步管理器 — 统一调度所有同步操作

职责：
    1. 根据 sync_config 选择同步器
    2. 执行同步 → 写库（去重）
    3. 记录同步日志
    4. 触发关联更新
"""
import json
import logging
from datetime import datetime, date
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn

from .base import BaseSyncer, BrokerPosition, BrokerTrade, BrokerBalance

logger = logging.getLogger(__name__)


# ── 同步器工厂 ────────────────────────────────────────────────────────────────

def _get_syncer() -> Optional[BaseSyncer]:
    """根据 sync_config 表的配置，返回对应的同步器实例"""
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM sync_config WHERE id=1").fetchone()
    if not row:
        return None

    method = row["sync_method"]

    if method == "eastmoney_web":
        if not row["em_account"] or not row["em_password_hash"]:
            logger.warning("东财Web配置不完整（缺少账号或密码）")
            return None
        from .eastmoney_web import EastMoneyWebSyncer
        fernet_key = os.environ.get("EM_FERNET_KEY", "")
        return EastMoneyWebSyncer(row["em_account"], row["em_password_hash"], fernet_key)

    if method == "qmt":
        if not row["qmt_path"] or not row["qmt_account"]:
            logger.warning("QMT配置不完整")
            return None
        from .qmt_xtquant import QMTSyncer
        return QMTSyncer(row["qmt_path"], row["qmt_account"], row["qmt_account_type"] or "STOCK")

    # manual：不需要同步器
    return None


def _log_sync(sync_method: str, sync_type: str, status: str,
               records: int = 0, error: str = ""):
    """写同步日志"""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO sync_log (sync_time, sync_method, sync_type, status, records_synced, error_message)
            VALUES (?,?,?,?,?,?)
        """, (datetime.now().isoformat(), sync_method, sync_type, status, records, error))


# ── 持仓同步 ──────────────────────────────────────────────────────────────────

def sync_positions(syncer: BaseSyncer, sync_method: str) -> int:
    """
    同步持仓。
    券商有、系统没有 → 新建；
    券商有、系统也有 → 更新；
    券商没有且来源非manual → 标记清仓。
    """
    try:
        broker_positions = syncer.get_positions()
    except Exception as e:
        _log_sync(sync_method, "positions", "failed", error=str(e))
        logger.error("持仓同步失败：%s", e)
        return 0

    broker_codes = {p.stock_code for p in broker_positions}

    with get_conn() as conn:
        existing = {
            r["stock_code"]: dict(r)
            for r in conn.execute("SELECT * FROM user_positions WHERE is_active=1").fetchall()
        }

        for p in broker_positions:
            weight = p.market_value / sum(bp.market_value for bp in broker_positions) * 100 \
                     if broker_positions else 0
            if p.stock_code in existing:
                conn.execute("""
                    UPDATE user_positions SET
                        shares=?, current_weight=?, source=?,
                        broker_position_id=?, updated_at=datetime('now')
                    WHERE stock_code=? AND is_active=1
                """, (p.shares, round(weight, 2), sync_method,
                      p.broker_position_id, p.stock_code))
            else:
                conn.execute("""
                    INSERT INTO user_positions
                        (stock_code, stock_name, buy_price, shares, current_weight,
                         source, broker_position_id, is_active)
                    VALUES (?,?,?,?,?,?,?,1)
                """, (p.stock_code, p.stock_name, p.cost_price, p.shares,
                      round(weight, 2), sync_method, p.broker_position_id))

        # 标记已清仓（非 manual 来源的）
        for code, pos in existing.items():
            if code not in broker_codes and pos.get("source", "manual") != "manual":
                conn.execute(
                    "UPDATE user_positions SET is_active=0 WHERE stock_code=? AND source!=?",
                    (code, "manual"),
                )

    count = len(broker_positions)
    _log_sync(sync_method, "positions", "success", count)
    logger.info("持仓同步完成：%d 只", count)
    return count


# ── 成交同步 ──────────────────────────────────────────────────────────────────

def sync_trades(syncer: BaseSyncer, sync_method: str,
                start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    """同步成交记录，用 broker_order_id 去重"""
    today = date.today().strftime("%Y-%m-%d")
    start_date = start_date or today
    end_date   = end_date   or today

    try:
        if start_date == today:
            trades = syncer.get_today_trades()
        else:
            trades = syncer.get_history_trades(start_date, end_date)
    except Exception as e:
        _log_sync(sync_method, "trades", "failed", error=str(e))
        logger.error("成交同步失败：%s", e)
        return 0

    written = 0
    with get_conn() as conn:
        for t in trades:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO trade_log
                        (trade_date, stock_code, stock_name, direction,
                         price, shares, amount, commission,
                         source, broker_order_id, sync_time)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (t.trade_date, t.stock_code, t.stock_name, t.direction,
                      t.price, t.shares, t.amount, t.commission,
                      sync_method, t.broker_order_id, datetime.now().isoformat()))
                written += 1
            except Exception as e:
                logger.debug("成交记录写库跳过：%s", e)

    # 自动关联验证器记录
    _auto_link_validations()

    _log_sync(sync_method, "trades", "success", written)
    logger.info("成交同步完成：新增 %d 条", written)
    return written


# ── 资金同步 ──────────────────────────────────────────────────────────────────

def sync_balance(syncer: BaseSyncer, sync_method: str) -> bool:
    """同步账户资金，更新 user_config.total_capital"""
    try:
        balance = syncer.get_balance()
        if balance and balance.total_assets > 0:
            with get_conn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO user_config(key,value) VALUES('total_capital',?)",
                    (str(int(balance.total_assets)),),
                )
            logger.info("账户总资产：%.0f 元", balance.total_assets)
            _log_sync(sync_method, "balance", "success", 1)
            return True
    except Exception as e:
        _log_sync(sync_method, "balance", "failed", error=str(e))
        logger.warning("资金同步失败：%s", e)
    return False


# ── 完整同步 ──────────────────────────────────────────────────────────────────

def full_sync() -> dict:
    """完整同步：资金 + 持仓 + 今日成交"""
    with get_conn() as conn:
        row = conn.execute("SELECT sync_method FROM sync_config WHERE id=1").fetchone()
    sync_method = row["sync_method"] if row else "manual"

    if sync_method == "manual":
        return {"status": "skipped", "reason": "同步方式为手动，请上传交割单或手动录入持仓"}

    syncer = _get_syncer()
    if not syncer:
        return {"status": "error", "reason": "同步器初始化失败，请检查配置"}

    result = {"sync_method": sync_method, "steps": {}}

    # 连接
    try:
        connected = syncer.connect()
        if not connected:
            _log_sync(sync_method, "full", "failed", error="连接失败")
            syncer.disconnect()
            return {"status": "error", "reason": "连接券商失败，请检查账号密码"}
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    try:
        result["steps"]["balance"]   = sync_balance(syncer, sync_method)
        result["steps"]["positions"] = sync_positions(syncer, sync_method)
        result["steps"]["trades"]    = sync_trades(syncer, sync_method)
        result["status"] = "success"
        result["sync_time"] = datetime.now().isoformat()
    except Exception as e:
        result["status"] = "partial"
        result["error"]  = str(e)
        logger.error("完整同步出错：%s", e)
    finally:
        syncer.disconnect()

    return result


# ── Excel 导入（方案C）────────────────────────────────────────────────────────

def import_from_excel(file_path_or_bytes) -> dict:
    """从交割单文件导入持仓和成交记录"""
    from .excel_import import ExcelImporter

    importer = ExcelImporter(file_path_or_bytes)
    if not importer.connect():
        return {"status": "error", "reason": "文件解析失败，请检查文件格式"}

    trades    = importer.get_history_trades("2000-01-01", date.today().strftime("%Y-%m-%d"))
    positions = importer.get_positions()

    written_trades = 0
    with get_conn() as conn:
        for t in trades:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO trade_log
                        (trade_date, stock_code, stock_name, direction,
                         price, shares, amount, commission, source, broker_order_id, sync_time)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (t.trade_date, t.stock_code, t.stock_name, t.direction,
                      t.price, t.shares, t.amount, t.commission,
                      "excel", t.broker_order_id, datetime.now().isoformat()))
                written_trades += 1
            except Exception:
                pass

    written_pos = 0
    with get_conn() as conn:
        for p in positions:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO user_positions
                        (stock_code, stock_name, buy_price, shares, source,
                         broker_position_id, is_active)
                    VALUES (?,?,?,?,?,?,1)
                """, (p.stock_code, p.stock_name, p.cost_price, p.shares,
                      "excel", p.broker_position_id))
                written_pos += 1
            except Exception:
                pass

    _log_sync("excel", "full", "success", written_trades + written_pos)
    _auto_link_validations()

    return {
        "status":           "success",
        "trades_imported":  written_trades,
        "positions_updated": written_pos,
        "date_range":       importer.parse_result().get("date_range"),
    }


# ── 辅助：自动关联验证器 ──────────────────────────────────────────────────────

def _auto_link_validations():
    """将 trade_log 中的成交与 validation_records 自动关联"""
    with get_conn() as conn:
        unlinked = conn.execute("""
            SELECT t.id, t.trade_date, t.stock_code, t.direction
            FROM trade_log t
            WHERE t.validation_id IS NULL
        """).fetchall()

        for t in unlinked:
            vr = conn.execute("""
                SELECT id, overall_score FROM validation_records
                WHERE validate_date=? AND stock_code=? AND direction=?
                ORDER BY created_at DESC LIMIT 1
            """, (t["trade_date"], t["stock_code"], t["direction"])).fetchone()

            if vr:
                conn.execute("""
                    UPDATE trade_log SET validation_id=?, validation_score=?
                    WHERE id=?
                """, (vr["id"], vr["overall_score"], t["id"]))
