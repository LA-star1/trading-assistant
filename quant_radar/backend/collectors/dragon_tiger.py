"""
龙虎榜数据采集器

AKShare 接口（已验证）：
    ak.stock_lhb_detail_em(start_date, end_date)
        → 当日上榜个股列表（含股票代码、名称、上榜原因）
        列名：序号, 代码, 名称, 上榜日, 解读, 收盘价, 涨跌幅,
              龙虎榜净买额, 龙虎榜买入额, 龙虎榜卖出额, 龙虎榜成交额,
              市场总成交额, 净买额占总成交比, 成交额占总成交比,
              换手率, 流通市值, 上榜原因, 上榜后1日, …

    ak.stock_lhb_stock_detail_em(symbol, date)
        → 单只股票当日龙虎榜席位明细
        列名：序号, 交易营业部名称, 买入金额, 买入金额-占总成交比例,
              卖出金额, 卖出金额-占总成交比例, 净额, 类型
        注意：金额单位为"元"，需转换为万元。
              同一股票因多条上榜原因会出现重复行，写库时以 UNIQUE 去重。

存储结构：dragon_tiger 表，每行 = 股票 × 席位 × 上榜原因。
"""
import logging
import time
from datetime import date, datetime

import akshare as ak
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import REQUEST_INTERVAL, RETRY_DELAYS
from db import upsert_dragon_tiger

logger = logging.getLogger(__name__)


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _retry(func, *args, **kwargs):
    """带递增延迟的重试，失败超过 RETRY_DELAYS 次则抛出最后一个异常"""
    last_exc = None
    for i, delay in enumerate([0] + RETRY_DELAYS):
        if delay:
            logger.warning("第 %d 次重试，等待 %ds…", i, delay)
            time.sleep(delay)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning("调用 %s 失败：%s", getattr(func, "__name__", "?"), e)
    raise last_exc


def _clean_code(code) -> str:
    """股票代码统一为6位纯数字字符串"""
    s = str(code).strip()
    for prefix in ("SH", "SZ", "BJ"):
        s = s.replace(prefix, "").replace("." + prefix, "").replace(prefix + ".", "")
    return s.zfill(6)[:6]


def _yuan_to_wan(value) -> float:
    """元 → 万元"""
    try:
        return round(float(value) / 10000, 4)
    except (TypeError, ValueError):
        return 0.0


# ── Step 1：获取上榜个股列表 ──────────────────────────────────────────────────

def fetch_lhb_stocks(trade_date: str) -> list[dict]:
    """
    调用 stock_lhb_detail_em 获取当日上榜个股列表。
    返回去重后的 {stock_code, stock_name, reason} 列表。
    同一股票因多条上榜原因会出现多行；这里保留所有 reason，在后续按原因分别采集席位。
    """
    date_fmt = trade_date.replace("-", "")
    logger.info("请求上榜个股列表：%s", trade_date)

    df = _retry(ak.stock_lhb_detail_em, start_date=date_fmt, end_date=date_fmt)

    if df is None or df.empty:
        logger.warning("上榜个股列表为空：%s", trade_date)
        return []

    logger.info("上榜记录原始行数：%d（含多条原因重复）", len(df))

    # 标准化列名
    col_code = _find_col(df.columns, ["代码", "股票代码", "证券代码"])
    col_name = _find_col(df.columns, ["名称", "股票名称", "证券名称"])
    col_reason = _find_col(df.columns, ["上榜原因", "原因"])

    if not col_code:
        logger.error("无法识别代码列，实际列名：%s", df.columns.tolist())
        return []

    result = []
    seen = set()  # (stock_code, reason) 去重
    for _, row in df.iterrows():
        code = _clean_code(row[col_code])
        name = str(row[col_name]).strip() if col_name else ""
        reason = str(row[col_reason]).strip() if col_reason else ""

        key = (code, reason)
        if key in seen:
            continue
        seen.add(key)

        result.append({"stock_code": code, "stock_name": name, "reason": reason})

    logger.info("去重后个股×原因组合数：%d", len(result))
    return result


# ── Step 2：获取单股席位明细 ──────────────────────────────────────────────────

def fetch_seat_detail(stock_code: str, trade_date: str) -> pd.DataFrame | None:
    """
    调用 stock_lhb_stock_detail_em 获取单只股票当日席位明细。
    列名：序号, 交易营业部名称, 买入金额, 买入金额-占总成交比例,
          卖出金额, 卖出金额-占总成交比例, 净额, 类型
    金额单位：元
    """
    date_fmt = trade_date.replace("-", "")
    time.sleep(REQUEST_INTERVAL)  # 限频保护

    try:
        df = _retry(ak.stock_lhb_stock_detail_em,
                    symbol=stock_code, date=date_fmt)
        return df
    except Exception as e:
        logger.warning("股票 %s 席位明细获取失败：%s", stock_code, e)
        return None


def parse_seat_rows(df: pd.DataFrame, stock_code: str) -> list[dict]:
    """
    解析席位明细 DataFrame，返回标准化记录列表。
    每条记录：{seat_name, buy_amount(万元), sell_amount(万元), net_amount(万元), reason}
    """
    if df is None or df.empty:
        return []

    # 列名适配
    col_seat = _find_col(df.columns, ["交易营业部名称", "营业部名称", "营业部", "名称"])
    col_buy = _find_col(df.columns, ["买入金额", "买入额"])
    col_sell = _find_col(df.columns, ["卖出金额", "卖出额"])
    col_net = _find_col(df.columns, ["净额", "净买额", "净买入"])
    col_type = _find_col(df.columns, ["类型", "上榜原因", "原因"])

    if not col_seat:
        logger.warning("股票 %s 席位明细无法识别席位名称列：%s",
                       stock_code, df.columns.tolist())
        return []

    rows = []
    for _, row in df.iterrows():
        seat_name = str(row[col_seat]).strip() if col_seat else ""
        buy_wan = _yuan_to_wan(row[col_buy]) if col_buy else 0.0
        sell_wan = _yuan_to_wan(row[col_sell]) if col_sell else 0.0
        net_wan = _yuan_to_wan(row[col_net]) if col_net else round(buy_wan - sell_wan, 4)
        reason = str(row[col_type]).strip() if col_type else ""

        if not seat_name:
            continue

        rows.append({
            "seat_name": seat_name,
            "buy_amount": buy_wan,
            "sell_amount": sell_wan,
            "net_amount": net_wan,
            "reason": reason,
        })

    return rows


# ── 主采集入口 ────────────────────────────────────────────────────────────────

def collect_dragon_tiger(trade_date: str) -> int:
    """
    采集指定交易日的龙虎榜数据，写入 dragon_tiger 表。

    流程：
    1. 获取当日上榜个股列表（含上榜原因）
    2. 对每只股票获取席位明细
    3. 整合为标准格式，写入数据库

    返回：实际新写入行数
    """
    logger.info("═══ 开始采集龙虎榜：%s ═══", trade_date)

    # Step 1：上榜个股列表
    stock_list = fetch_lhb_stocks(trade_date)
    if not stock_list:
        logger.warning("无上榜个股数据，跳过：%s", trade_date)
        return 0

    # 按 stock_code 去重，用于席位明细请求
    unique_codes = list({s["stock_code"]: s for s in stock_list}.values())
    logger.info("需要获取席位明细的个股数：%d", len(unique_codes))

    # 构建 stock_code → stock_info 映射
    stock_info = {}
    for s in stock_list:
        code = s["stock_code"]
        if code not in stock_info:
            stock_info[code] = {"stock_name": s["stock_name"]}

    # Step 2：逐股获取席位明细
    all_records = []
    for idx, code_info in enumerate(unique_codes):
        stock_code = code_info["stock_code"]
        stock_name = code_info["stock_name"]

        seat_df = fetch_seat_detail(stock_code, trade_date)
        seat_rows = parse_seat_rows(seat_df, stock_code)

        if not seat_rows:
            # 席位接口返回空：记录一条占位行（方便知道该股当日上榜）
            all_records.append({
                "trade_date": trade_date,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "reason": "",
                "seat_name": "",
                "buy_amount": 0.0,
                "sell_amount": 0.0,
                "net_amount": 0.0,
            })
        else:
            for r in seat_rows:
                all_records.append({
                    "trade_date": trade_date,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "reason": r["reason"],
                    "seat_name": r["seat_name"],
                    "buy_amount": r["buy_amount"],
                    "sell_amount": r["sell_amount"],
                    "net_amount": r["net_amount"],
                })

        if (idx + 1) % 10 == 0 or idx + 1 == len(unique_codes):
            logger.info("席位明细进度：%d/%d", idx + 1, len(unique_codes))

    # Step 3：写入数据库
    written = upsert_dragon_tiger(all_records)
    total = len(all_records)
    logger.info(
        "龙虎榜采集完成：解析 %d 条记录，新写入 %d 条（重复跳过 %d 条）",
        total, written, total - written,
    )

    # 打印样例数据供验证
    non_empty = [r for r in all_records if r["seat_name"]]
    if non_empty:
        sample = non_empty[0]
        logger.info(
            "样例 → %s %s(%s)  席位:%s  买入:%.2f万  卖出:%.2f万  净:%.2f万",
            sample["trade_date"], sample["stock_code"], sample["stock_name"],
            sample["seat_name"],
            sample["buy_amount"], sample["sell_amount"], sample["net_amount"],
        )

    return written


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _find_col(columns, candidates: list[str]) -> str | None:
    """在列名列表中找第一个匹配的候选列名"""
    for c in candidates:
        if c in columns:
            return c
    return None


# ── 快捷测试入口 ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-20"
    logger.info("测试采集日期：%s", test_date)

    import db as _db
    _db.init_db()

    n = collect_dragon_tiger(test_date)
    logger.info("测试完成，新写入行数：%d", n)

    # 查询验证
    rows = _db.get_dragon_tiger_by_date(test_date)
    logger.info("数据库验证：%s 共聚合后 %d 条 (股票×席位)", test_date, len(rows))
    # 打印前3条
    for r in rows[:3]:
        logger.info(
            "  %s %s  席位:%s  买入:%.2f万  卖出:%.2f万",
            r["stock_code"], r["stock_name"],
            r["seat_name"], r["buy_amount"], r["sell_amount"],
        )
