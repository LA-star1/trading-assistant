"""
AI 交易助手 — FastAPI HTTP 服务
运行：python api_server.py  （或 uvicorn api_server:app --port 8888 --reload）
"""
import json
import logging
from datetime import datetime, date, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import API_HOST, API_PORT, API_CORS_ORIGINS
from db import get_conn, get_user_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="AI交易助手", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=API_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 请求/响应模型 ─────────────────────────────────────────────────────────────

class ValidateRequest(BaseModel):
    stock_code:  str
    direction:   str          # 'buy' / 'sell'
    user_thesis: Optional[str] = ""
    call_ai:     Optional[bool] = True

class PositionCreate(BaseModel):
    stock_code:        str
    stock_name:        Optional[str] = ""
    buy_date:          Optional[str] = None
    buy_price:         Optional[float] = None
    shares:            Optional[int] = None
    current_weight:    Optional[float] = None
    stop_loss_price:   Optional[float] = None
    take_profit_price: Optional[float] = None
    notes:             Optional[str] = ""

class WatchlistAdd(BaseModel):
    stock_code: str
    stock_name: Optional[str] = ""
    reason:     Optional[str] = ""

class TradeRecord(BaseModel):
    trade_date:  str
    stock_code:  str
    stock_name:  Optional[str] = ""
    direction:   str
    price:       float
    shares:      int
    amount:      Optional[float] = None
    commission:  Optional[float] = 0

class ConfigUpdate(BaseModel):
    key:   str
    value: str

class SyncConfigRequest(BaseModel):
    sync_method:    str
    em_account:     Optional[str] = None
    em_password:    Optional[str] = None   # 明文，后端加密存储
    qmt_path:       Optional[str] = None
    qmt_account:    Optional[str] = None
    qmt_account_type: Optional[str] = "STOCK"


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _today() -> str:
    return date.today().strftime("%Y-%m-%d")

def _date_ago(days: int) -> str:
    return (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

def _rows_to_list(rows) -> list[dict]:
    return [dict(r) for r in rows]


# ── 健康检查 ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "AI交易助手", "time": datetime.now().isoformat()}


# ── 交易验证器 ─────────────────────────────────────────────────────────────────

@app.post("/api/validate")
def validate_trade(req: ValidateRequest):
    """运行完整的交易验证流程"""
    from analyzers.trade_validator import validate_trade as _validate
    try:
        result = _validate(
            stock_code=req.stock_code.strip(),
            direction=req.direction,
            user_thesis=req.user_thesis or "",
            call_ai=req.call_ai,
        )
        return {"status": "ok", "data": result}
    except Exception as e:
        logger.exception("交易验证失败")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/validate/history")
def get_validation_history(days: int = 30):
    """历史验证记录"""
    start = _date_ago(days)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM validation_records WHERE validate_date>=? ORDER BY created_at DESC",
            (start,)
        ).fetchall()
    return {"data": _rows_to_list(rows)}


# ── 持仓管理 ──────────────────────────────────────────────────────────────────

@app.get("/api/positions")
def get_positions():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM user_positions WHERE is_active=1 ORDER BY updated_at DESC").fetchall()
    return {"data": _rows_to_list(rows)}


@app.post("/api/positions")
def add_position(pos: PositionCreate):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO user_positions
                (stock_code, stock_name, buy_date, buy_price, shares,
                 current_weight, stop_loss_price, take_profit_price, notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,'manual')
        """, (pos.stock_code, pos.stock_name, pos.buy_date or _today(),
              pos.buy_price, pos.shares, pos.current_weight,
              pos.stop_loss_price, pos.take_profit_price, pos.notes))
    return {"status": "ok"}


@app.put("/api/positions/{position_id}")
def update_position(position_id: int, pos: PositionCreate):
    with get_conn() as conn:
        conn.execute("""
            UPDATE user_positions SET
                stock_name=?, buy_date=?, buy_price=?, shares=?,
                current_weight=?, stop_loss_price=?, take_profit_price=?,
                notes=?, updated_at=datetime('now')
            WHERE id=?
        """, (pos.stock_name, pos.buy_date, pos.buy_price, pos.shares,
              pos.current_weight, pos.stop_loss_price, pos.take_profit_price,
              pos.notes, position_id))
    return {"status": "ok"}


@app.delete("/api/positions/{position_id}")
def delete_position(position_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE user_positions SET is_active=0 WHERE id=?", (position_id,))
    return {"status": "ok"}


# ── 关注列表 ──────────────────────────────────────────────────────────────────

@app.get("/api/watchlist")
def get_watchlist():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM user_watchlist ORDER BY added_at DESC").fetchall()
    return {"data": _rows_to_list(rows)}


@app.post("/api/watchlist")
def add_watchlist(item: WatchlistAdd):
    with get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO user_watchlist(stock_code,stock_name,reason) VALUES(?,?,?)",
                (item.stock_code, item.stock_name, item.reason)
            )
        except Exception:
            raise HTTPException(status_code=409, detail="已在关注列表中")
    return {"status": "ok"}


@app.delete("/api/watchlist/{stock_code}")
def remove_watchlist(stock_code: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM user_watchlist WHERE stock_code=?", (stock_code,))
    return {"status": "ok"}


# ── 持仓告警 ──────────────────────────────────────────────────────────────────

@app.get("/api/alerts")
def get_unread_alerts():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM position_alerts WHERE is_read=0 ORDER BY created_at DESC"
        ).fetchall()
    return {"data": _rows_to_list(rows)}


@app.get("/api/alerts/history")
def get_alert_history(days: int = 30):
    start = _date_ago(days)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM position_alerts WHERE alert_date>=? ORDER BY created_at DESC",
            (start,)
        ).fetchall()
    return {"data": _rows_to_list(rows)}


@app.put("/api/alerts/{alert_id}/read")
def mark_alert_read(alert_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE position_alerts SET is_read=1 WHERE id=?", (alert_id,))
    return {"status": "ok"}


# ── 量化雷达 ──────────────────────────────────────────────────────────────────

@app.get("/api/radar/today")
def radar_today():
    from analyzers.seat_tracker import analyze_date
    from collectors.factor_monitor import factor_status
    today = _today()
    try:
        report = analyze_date(today)
    except Exception:
        report = {}
    factor = None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM factor_monitor ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
    if row:
        factor = dict(row)
        factor["status"] = factor_status(factor)
    return {"data": {"radar": report, "factors": factor}}


@app.get("/api/radar/signals")
def radar_signals(date: Optional[str] = None, min_score: float = 0):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM quant_signals WHERE trade_date=? AND score>=? ORDER BY score DESC",
            (date or _today(), min_score)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        try:
            d["seat_names"] = json.loads(d.get("seat_names", "[]"))
        except Exception:
            pass
        data.append(d)
    return {"data": data}


@app.get("/api/radar/stock/{stock_code}")
def radar_stock_history(stock_code: str):
    start = _date_ago(30)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM quant_signals WHERE stock_code=? AND trade_date>=? ORDER BY trade_date DESC",
            (stock_code, start)
        ).fetchall()
    return {"data": _rows_to_list(rows)}


@app.get("/api/radar/factors")
def radar_factors(days: int = 20):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM factor_monitor ORDER BY trade_date DESC LIMIT ?", (days,)
        ).fetchall()
    return {"data": list(reversed(_rows_to_list(rows)))}


@app.get("/api/radar/seats")
def radar_seats():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM quant_seats WHERE is_active=1 ORDER BY total_appearances DESC"
        ).fetchall()
    return {"data": _rows_to_list(rows)}


# ── 交易日志 ──────────────────────────────────────────────────────────────────

@app.post("/api/trades")
def record_trade(trade: TradeRecord):
    amount = trade.amount or round(trade.price * trade.shares, 2)
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO trade_log (trade_date,stock_code,stock_name,direction,price,shares,amount,commission)
            VALUES (?,?,?,?,?,?,?,?)
        """, (trade.trade_date, trade.stock_code, trade.stock_name,
              trade.direction, trade.price, trade.shares, amount, trade.commission))
    return {"status": "ok"}


@app.get("/api/trades")
def get_trades(week: Optional[str] = None):
    """按周查看交易（week 格式：2026-W12）"""
    with get_conn() as conn:
        if week:
            try:
                year, wn = week.split("-W")
                from datetime import datetime as dt
                week_start = dt.strptime(f"{year}-W{int(wn):02d}-1", "%Y-W%W-%w").strftime("%Y-%m-%d")
                week_end   = dt.strptime(f"{year}-W{int(wn):02d}-5", "%Y-W%W-%w").strftime("%Y-%m-%d")
                rows = conn.execute(
                    "SELECT * FROM trade_log WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date DESC",
                    (week_start, week_end)
                ).fetchall()
            except Exception:
                rows = conn.execute("SELECT * FROM trade_log ORDER BY trade_date DESC LIMIT 50").fetchall()
        else:
            rows = conn.execute("SELECT * FROM trade_log ORDER BY trade_date DESC LIMIT 50").fetchall()
    return {"data": _rows_to_list(rows)}


# ── 周度复盘 ──────────────────────────────────────────────────────────────────

@app.get("/api/review/latest")
def get_latest_review():
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM weekly_reviews ORDER BY week_start DESC LIMIT 1").fetchone()
    return {"data": dict(row) if row else None}


@app.get("/api/review/{week_start}")
def get_review_by_week(week_start: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM weekly_reviews WHERE week_start=?", (week_start,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="该周复盘不存在")
    return {"data": dict(row)}


@app.post("/api/review/generate")
def generate_review():
    """手动触发生成本周复盘"""
    try:
        from analyzers.weekly_review import generate_weekly_review
        result = generate_weekly_review()
        return {"status": "ok", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 早盘速览 ──────────────────────────────────────────────────────────────────

@app.get("/api/briefing/today")
def get_today_briefing():
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM morning_briefings WHERE briefing_date=?", (_today(),)
        ).fetchone()
    return {"data": dict(row) if row else None}


@app.get("/api/briefing/{date_str}")
def get_briefing_by_date(date_str: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM morning_briefings WHERE briefing_date=?", (date_str,)
        ).fetchone()
    return {"data": dict(row) if row else None}


# ── 用户配置 ──────────────────────────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    return {"data": get_user_config()}


@app.put("/api/config")
def update_config(item: ConfigUpdate):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_config(key,value) VALUES(?,?)",
            (item.key, item.value)
        )
    return {"status": "ok"}


# ── 同步管理 ──────────────────────────────────────────────────────────────────

@app.get("/api/sync/status")
def get_sync_status():
    with get_conn() as conn:
        cfg = conn.execute("SELECT * FROM sync_config WHERE id=1").fetchone()
        last_log = conn.execute(
            "SELECT * FROM sync_log ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    return {
        "sync_method":      cfg["sync_method"] if cfg else "manual",
        "last_sync_time":   last_log["sync_time"] if last_log else None,
        "last_sync_status": last_log["status"] if last_log else None,
        "records_synced":   last_log["records_synced"] if last_log else 0,
    }


@app.post("/api/sync/trigger")
def trigger_sync():
    from syncer.sync_manager import full_sync
    result = full_sync()
    return result


@app.post("/api/sync/config")
def configure_sync(req: SyncConfigRequest):
    """配置同步方式（密码在后端加密存储）"""
    enc_password = ""
    if req.sync_method == "eastmoney_web" and req.em_password:
        try:
            from syncer.eastmoney_web import EastMoneyWebSyncer
            fernet_key = os.environ.get("EM_FERNET_KEY", "")
            if not fernet_key:
                fernet_key = EastMoneyWebSyncer.generate_fernet_key()
                logger.warning("EM_FERNET_KEY 未设置，已生成临时密钥（重启后密码将失效）")
            enc_password = EastMoneyWebSyncer.encrypt_password(req.em_password, fernet_key)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"密码加密失败：{e}")

    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO sync_config
                (id, sync_method, em_account, em_password_hash, qmt_path, qmt_account, qmt_account_type, updated_at)
            VALUES (1,?,?,?,?,?,?,datetime('now'))
        """, (req.sync_method, req.em_account, enc_password,
              req.qmt_path, req.qmt_account, req.qmt_account_type))
    return {"status": "ok", "sync_method": req.sync_method}


@app.post("/api/sync/upload")
async def upload_excel(file: UploadFile = File(...)):
    """上传交割单 Excel 文件导入持仓和成交"""
    from syncer.sync_manager import import_from_excel
    content = await file.read()
    result = import_from_excel(content)
    return result


@app.get("/api/sync/log")
def get_sync_log(days: int = 7):
    start = _date_ago(days)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM sync_log WHERE created_at>=? ORDER BY created_at DESC LIMIT 100",
            (start,)
        ).fetchall()
    return {"data": _rows_to_list(rows)}


# ── AI token 用量 ──────────────────────────────────────────────────────────────

@app.get("/api/ai/usage")
def get_ai_usage():
    from analyzers.ai_engine import get_token_usage_today
    return {"data": get_token_usage_today()}


# ── 启动 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from db import init_db, insert_seed_data
    init_db()
    insert_seed_data()
    logger.info("启动 AI 交易助手 API 服务：http://%s:%d", API_HOST, API_PORT)
    uvicorn.run("api_server:app", host=API_HOST, port=API_PORT, reload=True)
