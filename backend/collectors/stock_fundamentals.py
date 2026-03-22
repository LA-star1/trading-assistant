"""
个股基本面数据采集器

已验证的 AKShare 接口：
    ak.stock_individual_info_em(symbol)
        → 基本信息（行业、总市值、流通市值等）

    ak.stock_zh_a_daily(symbol='sz002594', adjust='qfq')
        → 日线行情（新浪源，稳定）
        列：date, open, high, low, close, volume, amount, outstanding_share, turnover

    ak.stock_financial_analysis_indicator(symbol, start_year)
        → 财务分析指标（EPS、ROE、净利率等）

股票代码前缀规则：
    sz = 深交所（002xxx, 300xxx, 000xxx, 001xxx 等）
    sh = 上交所（600xxx, 601xxx, 603xxx, 688xxx 等）
"""
import logging
import time
from datetime import datetime, date, timedelta

import akshare as ak
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import REQUEST_INTERVAL, RETRY_DELAYS
from db import get_conn

logger = logging.getLogger(__name__)


def _retry(func, *args, **kwargs):
    last_exc = None
    for i, delay in enumerate([0] + RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning("调用 %s 失败（第%d次）：%s", getattr(func,"__name__","?"), i+1, e)
    raise last_exc


def _get_exchange_prefix(code: str) -> str:
    """根据股票代码推断交易所前缀"""
    c = code.strip()
    if c.startswith(("6", "9")):
        return "sh"
    return "sz"


def _ak_symbol(code: str) -> str:
    """拼接 AKShare 新浪源所需的 symbol，如 'sz002594'"""
    return _get_exchange_prefix(code) + code


# ── 个股基本信息 ──────────────────────────────────────────────────────────────

def get_stock_info(stock_code: str) -> dict:
    """
    获取个股基本信息（行业、市值等），优先读缓存（当日有效）。
    返回 dict: {stock_name, sector, market_cap, float_cap, pe_ttm, pb}
    """
    # 先查缓存
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM stock_info_cache WHERE stock_code=?", (stock_code,)
        ).fetchone()
        if row:
            updated = row["updated_at"][:10]
            if updated == date.today().strftime("%Y-%m-%d"):
                return dict(row)

    # 调 AKShare
    time.sleep(REQUEST_INTERVAL)
    try:
        df = _retry(ak.stock_individual_info_em, symbol=stock_code)
        # df: item | value，两列
        info = {r["item"]: r["value"] for _, r in df.iterrows()}

        result = {
            "stock_code":  stock_code,
            "stock_name":  str(info.get("股票简称", "")),
            "sector":      str(info.get("行业", "")),
            "market_cap":  float(info.get("总市值", 0) or 0) / 1e8,   # 元→亿
            "float_cap":   float(info.get("流通市值", 0) or 0) / 1e8,
            "pe_ttm":      None,   # 需要额外计算
            "pb":          None,
        }

        # 写缓存
        with get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stock_info_cache
                    (stock_code, stock_name, sector, market_cap, float_cap, pe_ttm, pb)
                VALUES (?,?,?,?,?,?,?)
            """, (result["stock_code"], result["stock_name"], result["sector"],
                  result["market_cap"], result["float_cap"],
                  result["pe_ttm"], result["pb"]))
        logger.debug("股票基本信息：%s %s 行业=%s", stock_code, result["stock_name"], result["sector"])
        return result

    except Exception as e:
        logger.warning("获取股票基本信息失败 %s：%s", stock_code, e)
        return {"stock_code": stock_code, "stock_name": "", "sector": "", "market_cap": 0, "float_cap": 0}


# ── 个股日线行情 ──────────────────────────────────────────────────────────────

def get_daily_history(stock_code: str, days: int = 60) -> pd.DataFrame:
    """
    获取个股近 N 日日线行情（前复权，新浪源）。
    先查 stock_daily_cache，缓存不足则从 AKShare 补充。

    返回 DataFrame，列：date(str), open, high, low, close, volume, amount, turnover
    """
    # 从缓存读取
    start = (date.today() - timedelta(days=days * 2)).strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM stock_daily_cache WHERE stock_code=? AND trade_date>=? ORDER BY trade_date",
            (stock_code, start)
        ).fetchall()

    if rows and len(rows) >= days:
        df = pd.DataFrame([dict(r) for r in rows])
        df = df.rename(columns={"trade_date": "date"})
        return df.tail(days).reset_index(drop=True)

    # 缓存不足，调 AKShare
    time.sleep(REQUEST_INTERVAL)
    try:
        symbol = _ak_symbol(stock_code)
        df_raw = _retry(ak.stock_zh_a_daily, symbol=symbol, adjust="qfq")
        # 列：date, open, high, low, close, volume, amount, outstanding_share, turnover
        df_raw["date"] = df_raw["date"].astype(str).str[:10]
        df_raw = df_raw.sort_values("date").reset_index(drop=True)

        # 写缓存（只写最近 120 日避免过大）
        cutoff = (date.today() - timedelta(days=120)).strftime("%Y-%m-%d")
        df_cache = df_raw[df_raw["date"] >= cutoff]
        rows_to_insert = [
            (stock_code, row["date"], row.get("open"), row.get("high"),
             row.get("low"), row.get("close"), row.get("volume"),
             row.get("amount"), row.get("turnover"))
            for _, row in df_cache.iterrows()
        ]
        with get_conn() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO stock_daily_cache
                    (stock_code, trade_date, open, high, low, close, volume, amount, turnover)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, rows_to_insert)

        logger.debug("日线数据 %s：共 %d 行", stock_code, len(df_raw))
        return df_raw.tail(days).reset_index(drop=True)

    except Exception as e:
        logger.warning("获取日线数据失败 %s：%s", stock_code, e)
        return pd.DataFrame()


# ── PE 分位计算 ────────────────────────────────────────────────────────────────

def get_pe_percentile(stock_code: str) -> float | None:
    """
    计算该股当前 PE_TTM 在近 3 年历史中的分位数（%）。

    方法：
    1. 获取近 3 年财务指标（EPS 季报）
    2. 获取近 3 年价格日线
    3. 用 价格/EPS_TTM 计算历史 PE 序列
    4. 当前 PE 在历史序列中的百分位

    返回：0-100 的浮点数，或 None（数据不足时）
    """
    try:
        # 获取财务指标（近3年）
        time.sleep(REQUEST_INTERVAL)
        start_year = str(date.today().year - 3)
        fin_df = _retry(
            ak.stock_financial_analysis_indicator,
            symbol=stock_code,
            start_year=start_year,
        )
        if fin_df is None or fin_df.empty:
            return None

        # 找 EPS 列
        eps_col = None
        for c in ["摊薄每股收益(元)", "加权每股收益(元)", "每股收益_调整后(元)"]:
            if c in fin_df.columns:
                eps_col = c
                break
        if not eps_col:
            return None

        fin_df = fin_df[["日期", eps_col]].dropna()
        fin_df["日期"] = fin_df["日期"].astype(str).str[:10]
        fin_df = fin_df.sort_values("日期")

        # 最近一期年化 EPS（取最近两个季报做滚动四季度）
        recent_eps = fin_df[eps_col].tail(4).sum()  # 最近4个季度 EPS
        if recent_eps <= 0:
            return None

        # 获取价格日线
        price_df = get_daily_history(stock_code, days=750)
        if price_df.empty:
            return None

        # 计算 PE 序列（用年化 EPS 近似）
        price_df["pe"] = price_df["close"] / recent_eps
        pe_series = price_df["pe"].dropna()

        current_price_row = price_df.iloc[-1]
        current_pe = current_price_row["close"] / recent_eps

        pct = float((pe_series < current_pe).sum() / len(pe_series) * 100)
        logger.debug("%s PE_TTM≈%.1f，近3年分位数=%.1f%%", stock_code, current_pe, pct)
        return round(pct, 1)

    except Exception as e:
        logger.warning("PE分位计算失败 %s：%s", stock_code, e)
        return None


# ── 动量计算 ──────────────────────────────────────────────────────────────────

def get_momentum(stock_code: str, days: int = 20) -> float | None:
    """计算 N 日价格动量（%）= (今日收盘 - N日前收盘) / N日前收盘 * 100"""
    df = get_daily_history(stock_code, days=days + 5)
    if df.empty or len(df) < days:
        return None
    try:
        curr = df.iloc[-1]["close"]
        prev = df.iloc[-(days + 1)]["close"]
        return round((curr - prev) / prev * 100, 2)
    except Exception:
        return None


# ── 成交量趋势 ────────────────────────────────────────────────────────────────

def get_volume_trend(stock_code: str) -> dict:
    """
    成交量趋势分析。
    返回：{trend: '放量'/'缩量'/'正常', ratio: float, score: int}
    """
    df = get_daily_history(stock_code, days=25)
    if df.empty or len(df) < 6:
        return {"trend": "未知", "ratio": 1.0, "score": 50}

    recent5  = df["volume"].tail(5).mean()
    past20   = df["volume"].tail(25).head(20).mean()
    ratio    = recent5 / past20 if past20 > 0 else 1.0

    if ratio > 1.3:
        trend, score = "放量", 70
    elif ratio < 0.7:
        trend, score = "缩量", 40
    else:
        trend, score = "正常", 55

    return {"trend": trend, "ratio": round(ratio, 2), "score": score}


# ── 历史相似走势匹配 ──────────────────────────────────────────────────────────

def get_historical_similarity(stock_code: str, lookback: int = 20) -> dict:
    """
    在近 3 年历史中查找与当前 lookback 日走势相似的片段。
    特征：近 lookback 日涨幅、波动率、换手率趋势。

    返回：
    {
        count: 找到的相似段数,
        win_rate_10d: 这些段之后10日上涨概率,
        avg_return_10d: 平均10日收益率,
        max_drawdown: 这些段中最大回撤,
    }
    """
    df = get_daily_history(stock_code, days=750)
    if df.empty or len(df) < lookback + 30:
        return {"count": 0, "win_rate_10d": None, "avg_return_10d": None, "max_drawdown": None}

    try:
        # 计算当前特征
        recent = df.tail(lookback)
        cur_return   = (recent["close"].iloc[-1] - recent["close"].iloc[0]) / recent["close"].iloc[0]
        cur_vol      = recent["close"].pct_change().std()
        cur_turnover = recent.get("turnover", pd.Series([0]*lookback)).mean()

        # 在历史中滑动匹配
        results_10d = []
        n = len(df)
        for i in range(lookback, n - 15):
            window = df.iloc[i - lookback:i]
            w_return   = (window["close"].iloc[-1] - window["close"].iloc[0]) / window["close"].iloc[0]
            w_vol      = window["close"].pct_change().std()
            w_turnover = window.get("turnover", pd.Series([0]*lookback)).mean()

            # 特征距离（简单加权欧氏距离）
            dist = (abs(w_return - cur_return) * 2
                    + abs(w_vol - cur_vol) * 3
                    + abs(w_turnover - cur_turnover))
            if dist < 0.05:   # 相似度阈值
                future = df.iloc[i:i + 10]
                if len(future) >= 10:
                    ret_10d = (future["close"].iloc[-1] - future["close"].iloc[0]) / future["close"].iloc[0] * 100
                    dd = ((future["close"].cummax() - future["close"]) / future["close"].cummax()).max() * 100
                    results_10d.append({"ret": ret_10d, "dd": dd})

        if not results_10d:
            return {"count": 0, "win_rate_10d": 50.0, "avg_return_10d": 0.0, "max_drawdown": 0.0}

        rets = [r["ret"] for r in results_10d]
        win_rate = sum(1 for r in rets if r > 0) / len(rets) * 100
        avg_ret  = sum(rets) / len(rets)
        max_dd   = max(r["dd"] for r in results_10d)

        return {
            "count":        len(results_10d),
            "win_rate_10d": round(win_rate, 1),
            "avg_return_10d": round(avg_ret, 2),
            "max_drawdown":   round(max_dd, 2),
        }
    except Exception as e:
        logger.warning("历史相似走势计算失败 %s：%s", stock_code, e)
        return {"count": 0, "win_rate_10d": None, "avg_return_10d": None, "max_drawdown": None}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import json
    code = "002594"
    print("=== 基本信息 ===")
    print(json.dumps(get_stock_info(code), ensure_ascii=False, indent=2))
    print("\n=== 20日动量 ===", get_momentum(code))
    print("\n=== 成交量趋势 ===", get_volume_trend(code))
    print("\n=== PE分位 ===", get_pe_percentile(code))
    print("\n=== 历史走势匹配 ===", get_historical_similarity(code))
