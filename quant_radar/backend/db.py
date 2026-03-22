"""
量化雷达 — SQLite 数据库初始化与操作工具

直接运行此文件可初始化数据库并插入种子数据：
    python db.py
"""
import sqlite3
import json
import logging
from contextlib import contextmanager
from datetime import datetime

from config import DB_PATH, SEED_QUANT_SEATS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
-- 龙虎榜原始数据（每行 = 一只股票 × 一个席位 × 一条上榜原因）
CREATE TABLE IF NOT EXISTS dragon_tiger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date      TEXT NOT NULL,
    stock_code      TEXT NOT NULL,
    stock_name      TEXT,
    reason          TEXT,
    seat_name       TEXT,           -- 交易营业部名称
    buy_amount      REAL,           -- 买入金额（万元）
    sell_amount     REAL,           -- 卖出金额（万元）
    net_amount      REAL,           -- 净额（万元）= buy - sell
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code, seat_name, reason)
);

-- 量化私募关联席位库（核心表）
CREATE TABLE IF NOT EXISTS quant_seats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    seat_name           TEXT NOT NULL UNIQUE,
    linked_fund         TEXT,
    confidence          TEXT DEFAULT 'medium',
    notes               TEXT,
    is_active           INTEGER DEFAULT 1,
    last_seen_date      TEXT,
    total_appearances   INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- 量化席位交易信号（分析结果表）
CREATE TABLE IF NOT EXISTS quant_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date          TEXT NOT NULL,
    stock_code          TEXT NOT NULL,
    stock_name          TEXT,
    signal_type         TEXT NOT NULL,
    seat_names          TEXT,
    total_buy_amount    REAL,
    total_sell_amount   REAL,
    net_amount          REAL,
    seat_count          INTEGER,
    score               REAL,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- 北向资金个股数据
CREATE TABLE IF NOT EXISTS north_bound (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date      TEXT NOT NULL,
    stock_code      TEXT NOT NULL,
    stock_name      TEXT,
    net_buy_amount  REAL,
    buy_amount      REAL,
    sell_amount     REAL,
    holding_shares  REAL,
    holding_ratio   REAL,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code)
);

-- 因子表现追踪
CREATE TABLE IF NOT EXISTS factor_monitor (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date              TEXT NOT NULL,
    csi1000_return          REAL,
    csi300_return           REAL,
    small_minus_large       REAL,
    gem_return              REAL,
    momentum_top20_return   REAL,
    momentum_bottom20_return REAL,
    momentum_spread         REAL,
    volume_ratio            REAL,
    created_at              TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date)
);

-- 大宗交易数据
CREATE TABLE IF NOT EXISTS block_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date      TEXT NOT NULL,
    stock_code      TEXT NOT NULL,
    stock_name      TEXT,
    price           REAL,
    close_price     REAL,
    discount_rate   REAL,
    volume          REAL,
    amount          REAL,
    buyer_seat      TEXT,
    seller_seat     TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date, stock_code, buyer_seat, seller_seat, amount)
);
"""

# ── 连接管理 ──────────────────────────────────────────────────────────────────

@contextmanager
def get_conn():
    """上下文管理器：自动提交/回滚，关闭连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row          # 支持按列名访问
    conn.execute("PRAGMA journal_mode=WAL") # 提升并发写入性能
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── 初始化 ────────────────────────────────────────────────────────────────────

def init_db():
    """创建所有表（幂等操作，可重复执行）"""
    with get_conn() as conn:
        conn.executescript(CREATE_TABLES_SQL)
    logger.info("数据库表初始化完成：%s", DB_PATH)


def insert_seed_seats():
    """插入量化席位种子数据（已存在则跳过）"""
    with get_conn() as conn:
        inserted = 0
        skipped = 0
        for seat in SEED_QUANT_SEATS:
            try:
                conn.execute(
                    """
                    INSERT INTO quant_seats
                        (seat_name, linked_fund, confidence, notes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        seat["seat_name"],
                        seat.get("linked_fund", ""),
                        seat.get("confidence", "medium"),
                        seat.get("notes", ""),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1  # UNIQUE 冲突 → 已存在，跳过
    logger.info("种子席位：新增 %d 条，已存在跳过 %d 条", inserted, skipped)


# ── 通用写入工具 ───────────────────────────────────────────────────────────────

def upsert_dragon_tiger(records: list[dict]) -> int:
    """
    批量写入龙虎榜数据（INSERT OR IGNORE，重复行跳过）
    返回实际写入行数
    """
    if not records:
        return 0
    sql = """
        INSERT OR IGNORE INTO dragon_tiger
            (trade_date, stock_code, stock_name, reason,
             seat_name, buy_amount, sell_amount, net_amount)
        VALUES
            (:trade_date, :stock_code, :stock_name, :reason,
             :seat_name, :buy_amount, :sell_amount, :net_amount)
    """
    with get_conn() as conn:
        before = conn.execute("SELECT COUNT(*) FROM dragon_tiger").fetchone()[0]
        conn.executemany(sql, records)
        after = conn.execute("SELECT COUNT(*) FROM dragon_tiger").fetchone()[0]
    return after - before


def upsert_quant_signals(records: list[dict]) -> int:
    """批量写入量化信号（同日同股先删后插，保持最新分析结果）"""
    if not records:
        return 0
    with get_conn() as conn:
        for r in records:
            # 同日同股的旧信号先删除
            conn.execute(
                "DELETE FROM quant_signals WHERE trade_date=? AND stock_code=?",
                (r["trade_date"], r["stock_code"]),
            )
        sql = """
            INSERT INTO quant_signals
                (trade_date, stock_code, stock_name, signal_type,
                 seat_names, total_buy_amount, total_sell_amount,
                 net_amount, seat_count, score)
            VALUES
                (:trade_date, :stock_code, :stock_name, :signal_type,
                 :seat_names, :total_buy_amount, :total_sell_amount,
                 :net_amount, :seat_count, :score)
        """
        conn.executemany(sql, records)
    return len(records)


def upsert_north_bound(records: list[dict]) -> int:
    """批量写入北向资金数据（INSERT OR REPLACE）"""
    if not records:
        return 0
    sql = """
        INSERT OR REPLACE INTO north_bound
            (trade_date, stock_code, stock_name, net_buy_amount,
             buy_amount, sell_amount, holding_shares, holding_ratio)
        VALUES
            (:trade_date, :stock_code, :stock_name, :net_buy_amount,
             :buy_amount, :sell_amount, :holding_shares, :holding_ratio)
    """
    with get_conn() as conn:
        conn.executemany(sql, records)
    return len(records)


def upsert_factor_monitor(record: dict) -> bool:
    """写入单日因子数据（INSERT OR REPLACE）"""
    sql = """
        INSERT OR REPLACE INTO factor_monitor
            (trade_date, csi1000_return, csi300_return, small_minus_large,
             gem_return, momentum_top20_return, momentum_bottom20_return,
             momentum_spread, volume_ratio)
        VALUES
            (:trade_date, :csi1000_return, :csi300_return, :small_minus_large,
             :gem_return, :momentum_top20_return, :momentum_bottom20_return,
             :momentum_spread, :volume_ratio)
    """
    with get_conn() as conn:
        conn.execute(sql, record)
    return True


def upsert_block_trades(records: list[dict]) -> int:
    """批量写入大宗交易数据（INSERT OR IGNORE）"""
    if not records:
        return 0
    sql = """
        INSERT OR IGNORE INTO block_trades
            (trade_date, stock_code, stock_name, price, close_price,
             discount_rate, volume, amount, buyer_seat, seller_seat)
        VALUES
            (:trade_date, :stock_code, :stock_name, :price, :close_price,
             :discount_rate, :volume, :amount, :buyer_seat, :seller_seat)
    """
    with get_conn() as conn:
        before = conn.execute("SELECT COUNT(*) FROM block_trades").fetchone()[0]
        conn.executemany(sql, records)
        after = conn.execute("SELECT COUNT(*) FROM block_trades").fetchone()[0]
    return after - before


# ── 查询工具 ───────────────────────────────────────────────────────────────────

def get_all_quant_seats() -> list[dict]:
    """返回所有活跃的量化席位"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM quant_seats WHERE is_active=1"
        ).fetchall()
    return [dict(r) for r in rows]


def get_dragon_tiger_by_date(trade_date: str) -> list[dict]:
    """
    返回指定日期的龙虎榜数据（每个 stock+seat 去重，取最大买卖金额）。
    同一股票+席位可能因多条上榜原因出现多次，聚合后返回。
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT trade_date, stock_code, stock_name,
                   seat_name,
                   SUM(buy_amount) AS buy_amount,
                   SUM(sell_amount) AS sell_amount,
                   SUM(net_amount) AS net_amount
            FROM dragon_tiger
            WHERE trade_date=?
            GROUP BY trade_date, stock_code, seat_name
            """,
            (trade_date,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_signals_by_date(trade_date: str, min_score: float = 0) -> list[dict]:
    """返回指定日期、最低分数的量化信号，按评分降序"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM quant_signals
            WHERE trade_date=? AND score>=?
            ORDER BY score DESC
            """,
            (trade_date, min_score),
        ).fetchall()
    return [dict(r) for r in rows]


def get_north_bound_by_date(trade_date: str) -> list[dict]:
    """返回指定日期北向资金数据"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM north_bound WHERE trade_date=?", (trade_date,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_factor_by_date(trade_date: str) -> dict | None:
    """返回指定日期因子数据"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM factor_monitor WHERE trade_date=?", (trade_date,)
        ).fetchone()
    return dict(row) if row else None


def update_seat_stats(seat_name: str, trade_date: str):
    """更新席位的最后出现日期和累计上榜次数"""
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE quant_seats
            SET last_seen_date = ?,
                total_appearances = total_appearances + 1
            WHERE seat_name = ?
            """,
            (trade_date, seat_name),
        )


def get_consecutive_signal_days(stock_code: str, before_date: str) -> int:
    """
    计算某只股票在 before_date 之前连续出现量化信号的天数
    用于信号评分的"连续信号加分"维度
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT trade_date FROM quant_signals
            WHERE stock_code=? AND trade_date < ?
            ORDER BY trade_date DESC
            LIMIT 10
            """,
            (stock_code, before_date),
        ).fetchall()

    if not rows:
        return 0

    from datetime import date, timedelta
    dates = [datetime.strptime(r["trade_date"], "%Y-%m-%d").date() for r in rows]
    ref = datetime.strptime(before_date, "%Y-%m-%d").date()

    consecutive = 0
    for d in dates:
        diff = (ref - d).days
        if diff == consecutive + 1:
            consecutive += 1
            ref = d
        else:
            break
    return consecutive


# ── 入口：直接运行初始化 ───────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("开始初始化量化雷达数据库…")
    init_db()
    insert_seed_seats()

    # 验证
    with get_conn() as conn:
        seat_count = conn.execute("SELECT COUNT(*) FROM quant_seats").fetchone()[0]
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()

    logger.info("已创建表：%s", [t[0] for t in tables])
    logger.info("量化席位种子数量：%d", seat_count)
    logger.info("数据库初始化完成 ✓  路径：%s", DB_PATH)
