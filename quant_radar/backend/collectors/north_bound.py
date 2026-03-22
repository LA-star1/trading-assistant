"""
北向资金数据采集器

使用 AKShare 接口（已验证）：

汇总数据（每日）：
    ak.stock_hsgt_fund_flow_summary_em()
        → 返回当日沪股通+深股通资金流向汇总
        列名：交易日, 类型, 板块, 资金方向, 交易状态,
              成交净买额, 资金净流入, 当日资金余额, 上涨数, 持平数, 下跌数, 相关指数, 指数涨跌幅

历史数据：
    ak.stock_hsgt_hist_em(symbol='北向资金')
        → 历史日线净流入（近期部分字段为 NaN）
        用于回填历史因子数据

存储方案：
    - 汇总数据存入 north_bound 表（stock_code='__TOTAL__' 表示北向资金合计）
    - 沪股通和深股通分别存一行
    - 暂不支持个股北向明细（AKShare 历史接口近期数据不稳定）
"""
import logging
import time
from datetime import datetime

import akshare as ak
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import REQUEST_INTERVAL, RETRY_DELAYS
from db import upsert_north_bound

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


def _to_wan(val) -> float:
    """亿元 → 万元（东财北向资金单位为亿元）"""
    try:
        return round(float(val) * 10000, 2)
    except (TypeError, ValueError):
        return 0.0


# ── 当日汇总数据采集 ──────────────────────────────────────────────────────────

def fetch_daily_summary() -> pd.DataFrame:
    """
    获取当日沪深港通资金流向汇总。
    返回 DataFrame，过滤只保留北向资金行（板块含'股通'且资金方向='北向'）。
    """
    logger.info("请求北向资金汇总数据…")
    df = _retry(ak.stock_hsgt_fund_flow_summary_em)
    if df is None or df.empty:
        logger.warning("北向资金汇总数据为空")
        return pd.DataFrame()
    logger.debug("汇总数据原始行数：%d", len(df))
    return df


def parse_summary_to_records(df: pd.DataFrame, trade_date: str) -> list[dict]:
    """
    解析汇总 DataFrame，转为标准格式写入 north_bound 表。

    策略：
    - 沪股通（北向）→ stock_code='__SH_NORTH__'
    - 深股通（北向）→ stock_code='__SZ_NORTH__'
    - 合计（北向）  → stock_code='__TOTAL_NORTH__'

    金额：东财原始单位为亿元 → 换算为万元
    """
    records = []

    # 过滤北向资金行
    col_direction = "资金方向"
    col_board = "板块"
    col_net_buy = "成交净买额"

    if col_direction not in df.columns:
        logger.error("北向资金汇总数据列名不符，实际列：%s", df.columns.tolist())
        return []

    north_rows = df[df[col_direction] == "北向"]

    sh_net = 0.0
    sz_net = 0.0

    for _, row in north_rows.iterrows():
        board = str(row.get(col_board, "")).strip()
        net_yi = row.get(col_net_buy, 0) or 0  # 亿元
        net_wan = _to_wan(net_yi)

        if "沪" in board:
            code = "__SH_NORTH__"
            sh_net = net_wan
        elif "深" in board:
            code = "__SZ_NORTH__"
            sz_net = net_wan
        else:
            continue

        records.append({
            "trade_date": trade_date,
            "stock_code": code,
            "stock_name": board,
            "net_buy_amount": net_wan,
            "buy_amount": 0.0,    # 汇总数据中不区分买卖
            "sell_amount": 0.0,
            "holding_shares": 0.0,
            "holding_ratio": 0.0,
        })

    # 合计行
    total_net = sh_net + sz_net
    records.append({
        "trade_date": trade_date,
        "stock_code": "__TOTAL_NORTH__",
        "stock_name": "北向资金合计",
        "net_buy_amount": total_net,
        "buy_amount": 0.0,
        "sell_amount": 0.0,
        "holding_shares": 0.0,
        "holding_ratio": 0.0,
    })

    return records


# ── 历史北向资金数据（用于回填） ──────────────────────────────────────────────

def fetch_hist_north_bound() -> pd.DataFrame:
    """
    获取北向资金历史净买额数据（东财，较完整）。
    近期部分字段为 NaN（接口限制），取最后有效数据。
    """
    logger.info("请求北向资金历史数据…")
    time.sleep(REQUEST_INTERVAL)
    df = _retry(ak.stock_hsgt_hist_em, symbol="北向资金")
    return df


def parse_hist_to_records(df: pd.DataFrame, target_date: str) -> list[dict]:
    """
    从历史数据中提取指定日期的北向资金数据。
    """
    if df is None or df.empty:
        return []

    col_date = "日期"
    col_net = "当日成交净买额"
    col_buy = "买入成交额"
    col_sell = "卖出成交额"

    row = df[df[col_date].astype(str).str[:10] == target_date]
    if row.empty:
        logger.warning("历史北向数据中未找到日期：%s", target_date)
        return []

    r = row.iloc[0]
    net_wan = _to_wan(r.get(col_net, 0))  # 历史数据单位为亿
    buy_wan = _to_wan(r.get(col_buy, 0))
    sell_wan = _to_wan(r.get(col_sell, 0))

    if net_wan == 0 and buy_wan == 0:
        # 该日字段为 NaN（接口数据缺失）
        return []

    return [
        {
            "trade_date": target_date,
            "stock_code": "__TOTAL_NORTH__",
            "stock_name": "北向资金合计（历史）",
            "net_buy_amount": net_wan,
            "buy_amount": buy_wan,
            "sell_amount": sell_wan,
            "holding_shares": 0.0,
            "holding_ratio": 0.0,
        }
    ]


# ── 主采集入口 ────────────────────────────────────────────────────────────────

def collect_north_bound(trade_date: str) -> int:
    """
    采集指定交易日的北向资金数据。

    策略：
    1. 优先使用当日汇总接口（实时准确）
    2. 若汇总接口返回的日期与目标日期不符（历史回填），改用历史接口

    返回：写入行数
    """
    logger.info("═══ 开始采集北向资金：%s ═══", trade_date)

    records = []

    # Step 1：尝试当日汇总
    try:
        summary_df = fetch_daily_summary()
        if not summary_df.empty:
            # 检查返回的日期是否与目标日期一致
            if "交易日" in summary_df.columns:
                returned_date = str(summary_df["交易日"].iloc[0])[:10]
                if returned_date == trade_date:
                    records = parse_summary_to_records(summary_df, trade_date)
                    logger.info("使用当日汇总数据，日期匹配：%s", trade_date)
                else:
                    logger.info(
                        "汇总数据日期=%s，目标=%s，改用历史接口",
                        returned_date, trade_date,
                    )
            else:
                records = parse_summary_to_records(summary_df, trade_date)
    except Exception as e:
        logger.warning("当日汇总接口失败：%s", e)

    # Step 2：汇总失败或日期不符，用历史接口
    if not records:
        try:
            hist_df = fetch_hist_north_bound()
            records = parse_hist_to_records(hist_df, trade_date)
            if records:
                logger.info("使用历史接口数据")
        except Exception as e:
            logger.warning("历史北向接口也失败：%s", e)

    if not records:
        logger.warning("北向资金数据采集失败，日期：%s", trade_date)
        return 0

    written = upsert_north_bound(records)
    logger.info("北向资金写入：%d 条", written)

    for r in records:
        logger.info(
            "  %s: 净买入=%.0f万",
            r["stock_name"], r["net_buy_amount"],
        )

    return written


# ── 快捷测试入口 ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-20"
    import db as _db
    _db.init_db()
    n = collect_north_bound(test_date)
    logger.info("测试完成，写入行数：%d", n)
