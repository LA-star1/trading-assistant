"""
AI交易助手 — SQLite 数据库初始化
直接运行：python db.py
"""
import sqlite3
import logging
from contextlib import contextmanager
from config import DB_PATH, DEFAULT_USER_CONFIG, SEED_QUANT_SEATS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DDL = """
-- ── 用户配置 ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    buy_date TEXT,
    buy_price REAL,
    shares INTEGER,
    current_weight REAL,
    stop_loss_price REAL,
    take_profit_price REAL,
    notes TEXT,
    is_active INTEGER DEFAULT 1,
    source TEXT DEFAULT 'manual',
    broker_position_id TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL UNIQUE,
    stock_name TEXT,
    reason TEXT,
    added_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_config (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- ── 量化雷达 ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dragon_tiger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    reason TEXT,
    seat_name TEXT,
    buy_amount REAL,
    sell_amount REAL,
    net_amount REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code, seat_name, reason)
);

CREATE TABLE IF NOT EXISTS quant_seats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seat_name TEXT NOT NULL UNIQUE,
    linked_fund TEXT,
    confidence TEXT DEFAULT 'medium',
    notes TEXT,
    is_active INTEGER DEFAULT 1,
    last_seen_date TEXT,
    total_appearances INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quant_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    signal_type TEXT NOT NULL,
    seat_names TEXT,
    total_buy_amount REAL,
    total_sell_amount REAL,
    net_amount REAL,
    seat_count INTEGER,
    score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS north_bound (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    net_buy_amount REAL,
    buy_amount REAL,
    sell_amount REAL,
    holding_shares REAL,
    holding_ratio REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code)
);

CREATE TABLE IF NOT EXISTS factor_monitor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL UNIQUE,
    csi1000_return REAL,
    csi300_return REAL,
    small_minus_large REAL,
    gem_return REAL,
    momentum_top20_return REAL,
    momentum_bottom20_return REAL,
    momentum_spread REAL,
    volume_ratio REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS block_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    price REAL,
    close_price REAL,
    discount_rate REAL,
    volume REAL,
    amount REAL,
    buyer_seat TEXT,
    seller_seat TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code, buyer_seat, seller_seat, amount)
);

-- ── 交易验证器 ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS validation_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    validate_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    direction TEXT NOT NULL,
    user_thesis TEXT,
    momentum_20d REAL,
    pe_percentile REAL,
    volume_trend TEXT,
    northbound_change REAL,
    correlation_with_portfolio REAL,
    sector_exposure_after REAL,
    devils_advocate_text TEXT,
    suggested_weight_min REAL,
    suggested_weight_max REAL,
    stop_loss_reference REAL,
    max_loss_amount REAL,
    similar_pattern_count INTEGER,
    win_rate_10d REAL,
    avg_return_10d REAL,
    max_drawdown REAL,
    overall_score REAL,
    user_decision TEXT,
    user_decision_note TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ── 早盘速览 & 持仓体检 ────────────────────────────────────
CREATE TABLE IF NOT EXISTS morning_briefings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    briefing_date TEXT NOT NULL UNIQUE,
    us_market_summary TEXT,
    hk_market_summary TEXT,
    northbound_yesterday TEXT,
    position_related_news TEXT,
    catalyst_today TEXT,
    sector_signals TEXT,
    ai_summary TEXT,
    ai_focus_points TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS position_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_date TEXT NOT NULL,
    alert_time TEXT,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    alert_type TEXT NOT NULL,
    severity TEXT DEFAULT 'medium',
    description TEXT,
    ai_interpretation TEXT,
    historical_reference TEXT,
    is_read INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ── 交易日志 & 周度复盘 ────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    direction TEXT NOT NULL,
    price REAL NOT NULL,
    shares INTEGER NOT NULL,
    amount REAL,
    commission REAL,
    validation_id INTEGER,
    validation_score REAL,
    pnl REAL,
    pnl_percent REAL,
    alpha_attribution REAL,
    beta_attribution REAL,
    tags TEXT,
    review_note TEXT,
    source TEXT DEFAULT 'manual',
    broker_order_id TEXT,
    sync_time TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS weekly_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_start TEXT NOT NULL,
    week_end TEXT NOT NULL,
    total_pnl REAL,
    total_pnl_percent REAL,
    benchmark_return REAL,
    alpha REAL,
    win_rate REAL,
    profit_loss_ratio REAL,
    max_drawdown REAL,
    avg_validation_score REAL,
    low_score_trades INTEGER,
    decision_quality_score REAL,
    behavior_flags TEXT,
    ai_report TEXT,
    ai_key_lessons TEXT,
    ai_next_week_focus TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(week_start)
);

-- ── 行情缓存 ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_daily_cache (
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    volume REAL, amount REAL, turnover REAL,
    PRIMARY KEY (stock_code, trade_date)
);

CREATE TABLE IF NOT EXISTS stock_info_cache (
    stock_code TEXT PRIMARY KEY,
    stock_name TEXT,
    sector TEXT,
    market_cap REAL,
    float_cap REAL,
    pe_ttm REAL,
    pb REAL,
    updated_at TEXT DEFAULT (datetime('now'))
);

-- ── 同步模块 ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sync_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_method TEXT NOT NULL DEFAULT 'manual',
    em_account TEXT,
    em_password_hash TEXT,
    em_session_token TEXT,
    em_token_expires TEXT,
    qmt_path TEXT,
    qmt_account TEXT,
    qmt_account_type TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_time TEXT NOT NULL,
    sync_method TEXT NOT NULL,
    sync_type TEXT NOT NULL,
    status TEXT NOT NULL,
    records_synced INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_conn() as conn:
        conn.executescript(DDL)
    logger.info("数据库表初始化完成：%s", DB_PATH)

def insert_seed_data():
    with get_conn() as conn:
        # 用户配置
        for k, v in DEFAULT_USER_CONFIG.items():
            conn.execute("INSERT OR IGNORE INTO user_config(key,value) VALUES(?,?)", (k, v))
        # 量化席位种子
        for s in SEED_QUANT_SEATS:
            conn.execute(
                "INSERT OR IGNORE INTO quant_seats(seat_name,linked_fund,confidence) VALUES(?,?,?)",
                (s["seat_name"], s.get("linked_fund",""), s.get("confidence","medium"))
            )
        # 默认同步配置
        conn.execute("INSERT OR IGNORE INTO sync_config(id,sync_method) VALUES(1,'manual')")
    logger.info("种子数据写入完成")

def get_user_config() -> dict:
    with get_conn() as conn:
        rows = conn.execute("SELECT key, value FROM user_config").fetchall()
    return {r["key"]: r["value"] for r in rows}

def get_active_positions() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM user_positions WHERE is_active=1"
        ).fetchall()
    return [dict(r) for r in rows]

if __name__ == "__main__":
    logger.info("初始化 AI 交易助手数据库…")
    init_db()
    insert_seed_data()
    with get_conn() as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
    logger.info("已创建 %d 张表：%s", len(tables), tables)
    logger.info("数据库路径：%s", DB_PATH)
