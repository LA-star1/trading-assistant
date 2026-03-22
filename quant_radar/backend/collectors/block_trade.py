"""
大宗交易数据采集器

使用 AKShare 接口（已验证）：
    ak.stock_dzjy_mrmx(symbol='A股', start_date='YYYYMMDD', end_date='YYYYMMDD')
        → 当日 A 股大宗交易个股明细
        列名：序号, 交易日期, 证券代码, 证券简称, 涨跌幅, 收盘价, 成交价,
              折溢率, 成交量, 成交额, 成交额/流通市值, 买方营业部, 卖方营业部
        注意：成交量单位=股，成交额单位=元，折溢率=小数（如 -0.084 = -8.4%）

存储：block_trades 表，金额统一为万元。
"""
import logging
import time
from datetime import datetime

import akshare as ak
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import REQUEST_INTERVAL, RETRY_DELAYS
from db import upsert_block_trades

logger = logging.getLogger(__name__)


def _retry(func, *args, **kwargs):
    last_exc = None
    for i, delay in enumerate([0] + RETRY_DELAYS):
        if delay:
            logger.warning("第 %d 次重试，等待 %ds…", i, delay)
            time.sleep(delay)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning("调用失败：%s", e)
    raise last_exc


def _clean_code(code) -> str:
    s = str(code).strip()
    return s.zfill(6)[:6]


def _yuan_to_wan(val) -> float:
    try:
        return round(float(val) / 10000, 4)
    except (TypeError, ValueError):
        return 0.0


def _shares_to_wan(val) -> float:
    """股 → 万股"""
    try:
        return round(float(val) / 10000, 4)
    except (TypeError, ValueError):
        return 0.0


def collect_block_trade(trade_date: str) -> int:
    """
    采集指定交易日的大宗交易数据，写入 block_trades 表。
    返回：实际新写入行数
    """
    logger.info("═══ 开始采集大宗交易：%s ═══", trade_date)

    date_fmt = trade_date.replace("-", "")
    time.sleep(REQUEST_INTERVAL)

    try:
        df = _retry(
            ak.stock_dzjy_mrmx,
            symbol="A股",
            start_date=date_fmt,
            end_date=date_fmt,
        )
    except Exception as e:
        logger.error("大宗交易接口调用失败：%s", e)
        return 0

    if df is None or df.empty:
        logger.warning("大宗交易数据为空：%s", trade_date)
        return 0

    logger.info("大宗交易原始行数：%d", len(df))
    logger.debug("列名：%s", df.columns.tolist())

    # 列名适配
    col_code = _find_col(df.columns, ["证券代码", "股票代码", "代码"])
    col_name = _find_col(df.columns, ["证券简称", "股票名称", "名称"])
    col_price = _find_col(df.columns, ["成交价", "价格"])
    col_close = _find_col(df.columns, ["收盘价"])
    col_discount = _find_col(df.columns, ["折溢率"])
    col_volume = _find_col(df.columns, ["成交量", "成交数量"])
    col_amount = _find_col(df.columns, ["成交额"])
    col_buyer = _find_col(df.columns, ["买方营业部", "买方"])
    col_seller = _find_col(df.columns, ["卖方营业部", "卖方"])

    if not col_code:
        logger.error("无法识别证券代码列，实际列名：%s", df.columns.tolist())
        return 0

    records = []
    for _, row in df.iterrows():
        code = _clean_code(row[col_code])
        name = str(row[col_name]).strip() if col_name else ""
        price = float(row[col_price]) if col_price else 0.0
        close_price = float(row[col_close]) if col_close else 0.0

        # 折价率：接口返回小数形式（如 -0.084），转为百分比（-8.4）
        discount_raw = row.get(col_discount, 0) if col_discount else 0
        try:
            discount_rate = round(float(discount_raw) * 100, 4)
        except (TypeError, ValueError):
            discount_rate = 0.0

        volume_wan = _shares_to_wan(row[col_volume]) if col_volume else 0.0
        amount_wan = _yuan_to_wan(row[col_amount]) if col_amount else 0.0
        buyer = str(row[col_buyer]).strip() if col_buyer else ""
        seller = str(row[col_seller]).strip() if col_seller else ""

        records.append({
            "trade_date": trade_date,
            "stock_code": code,
            "stock_name": name,
            "price": price,
            "close_price": close_price,
            "discount_rate": discount_rate,
            "volume": volume_wan,
            "amount": amount_wan,
            "buyer_seat": buyer,
            "seller_seat": seller,
        })

    written = upsert_block_trades(records)
    logger.info(
        "大宗交易采集完成：解析 %d 条，新写入 %d 条",
        len(records), written,
    )

    if records:
        s = records[0]
        logger.info(
            "样例 → %s %s  成交价:%.2f  收盘:%.2f  折价率:%.2f%%  成交额:%.0f万  买方:%s",
            s["stock_code"], s["stock_name"],
            s["price"], s["close_price"], s["discount_rate"],
            s["amount"], s["buyer_seat"],
        )

    return written


def _find_col(columns, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


# ── 统计分析（供 API 层调用）──────────────────────────────────────────────────

def get_block_trade_summary(trade_date: str, db_conn=None) -> dict:
    """
    返回指定日期大宗交易的摘要统计：
    - 总成交额（万元）
    - 折价/溢价成交额及占比
    - 成交笔数
    - 活跃营业部排行（前5）
    """
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import get_conn

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM block_trades WHERE trade_date=?", (trade_date,)
        ).fetchall()

    if not rows:
        return {"date": trade_date, "total_amount": 0, "count": 0}

    total_amount = sum(r["amount"] for r in rows)
    discount_amount = sum(r["amount"] for r in rows if r["discount_rate"] < 0)
    premium_amount = sum(r["amount"] for r in rows if r["discount_rate"] > 0)

    # 活跃买方席位
    from collections import Counter
    buyer_counter = Counter(r["buyer_seat"] for r in rows if r["buyer_seat"])
    top_buyers = buyer_counter.most_common(5)

    return {
        "date": trade_date,
        "count": len(rows),
        "total_amount_wan": round(total_amount, 0),
        "discount_amount_wan": round(discount_amount, 0),
        "premium_amount_wan": round(premium_amount, 0),
        "discount_ratio": round(discount_amount / total_amount * 100, 2) if total_amount else 0,
        "top_buyers": [{"seat": s, "count": c} for s, c in top_buyers],
    }


# ── 快捷测试入口 ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-20"
    import db as _db
    _db.init_db()

    n = collect_block_trade(test_date)
    logger.info("测试完成，写入行数：%d", n)

    summary = get_block_trade_summary(test_date)
    print("\n大宗交易摘要：")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
