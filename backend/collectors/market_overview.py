"""
早盘市场概览数据收集器

收集：A股整体涨跌分布、情绪指标、北向资金、热门板块
"""
import logging
import time
from datetime import date, datetime
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn

logger = logging.getLogger(__name__)


def get_market_breadth() -> dict:
    """
    A股涨跌家数、量能概览
    Returns: {up, down, flat, limit_up, limit_down, total, up_ratio}
    """
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return {}

        # 列：名称, 最新价, 涨跌幅, ...
        pct_col = "涨跌幅"
        if pct_col not in df.columns:
            pct_col = df.columns[3]  # fallback

        pct = df[pct_col].astype(float)
        up       = int((pct >  0).sum())
        down     = int((pct <  0).sum())
        flat     = int((pct == 0).sum())
        limit_up = int((pct >= 9.5).sum())
        limit_down = int((pct <= -9.5).sum())
        total    = len(df)

        return {
            "up": up,
            "down": down,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "total": total,
            "up_ratio": round(up / total * 100, 1) if total else 0,
        }
    except Exception as e:
        logger.warning("市场宽度数据获取失败：%s", e)
        return {}


def get_index_snapshot() -> list[dict]:
    """
    主要指数快照：上证、深证、创业板、沪深300
    """
    index_map = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000300": "沪深300",
        "sh000016": "上证50",
        "sz399905": "中证500",
    }
    result = []
    try:
        import akshare as ak
        df = ak.stock_zh_index_spot_em()
        if df is None or df.empty:
            return result

        for _, row in df.iterrows():
            code = str(row.get("代码", "")).lower()
            if code in index_map:
                result.append({
                    "code": code,
                    "name": index_map[code],
                    "price": float(row.get("最新价", 0) or 0),
                    "change_pct": float(row.get("涨跌幅", 0) or 0),
                    "volume": float(row.get("成交量", 0) or 0),
                    "amount": float(row.get("成交额", 0) or 0),
                })
    except Exception as e:
        logger.warning("指数快照获取失败：%s", e)

    return result


def get_north_bound_today() -> dict:
    """
    今日北向资金（沪股通+深股通）
    """
    try:
        import akshare as ak
        df = ak.stock_hsgt_fund_flow_summary_em()
        if df is None or df.empty:
            return {}

        today_str = date.today().strftime("%Y-%m-%d")
        # 取最新一行
        row = df.iloc[-1]
        return {
            "date": str(row.get("日期", today_str)),
            "sh_net": float(row.get("沪股通", 0) or 0),   # 亿元
            "sz_net": float(row.get("深股通", 0) or 0),
            "total_net": float(row.get("北向资金", 0) or 0),
        }
    except Exception as e:
        logger.warning("北向资金获取失败：%s", e)
        return {}


def get_hot_sectors() -> list[dict]:
    """
    板块涨跌排行（申万行业或东财概念，取涨幅前5/跌幅前5）
    """
    try:
        import akshare as ak
        df = ak.stock_board_industry_name_em()
        if df is None or df.empty:
            return []

        df["涨跌幅"] = df["涨跌幅"].astype(float)
        top5_up   = df.nlargest(5, "涨跌幅")
        top5_down = df.nsmallest(5, "涨跌幅")
        combined  = []

        for _, row in top5_up.iterrows():
            combined.append({
                "name": str(row.get("板块名称", "")),
                "change_pct": float(row.get("涨跌幅", 0)),
                "direction": "up",
            })
        for _, row in top5_down.iterrows():
            combined.append({
                "name": str(row.get("板块名称", "")),
                "change_pct": float(row.get("涨跌幅", 0)),
                "direction": "down",
            })
        return combined
    except Exception as e:
        logger.warning("热门板块获取失败：%s", e)
        return []


def get_market_overview(use_cache_hours: float = 0.5) -> dict:
    """
    汇总市场概览，优先读缓存（默认30分钟内有效）
    """
    today = date.today().isoformat()

    # 尝试读当日缓存（morning_briefings 表里 market_data 字段）
    with get_conn() as conn:
        row = conn.execute(
            "SELECT market_data, created_at FROM morning_briefings WHERE brief_date=? LIMIT 1",
            (today,)
        ).fetchone()
    if row and row["market_data"]:
        import json
        try:
            cached = json.loads(row["market_data"])
            # 检查缓存是否在有效期内
            created = datetime.fromisoformat(row["created_at"])
            age_hours = (datetime.now() - created).total_seconds() / 3600
            if age_hours < use_cache_hours:
                return cached
        except Exception:
            pass

    # 实时拉取
    overview = {
        "date": today,
        "breadth": get_market_breadth(),
        "indices": get_index_snapshot(),
        "north_bound": get_north_bound_today(),
        "hot_sectors": get_hot_sectors(),
        "fetched_at": datetime.now().isoformat(),
    }
    return overview
