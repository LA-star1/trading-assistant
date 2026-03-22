"""
因子拥挤度监测器

使用 AKShare 接口（已验证）：
    ak.stock_zh_index_daily(symbol=code)  — 新浪源指数日线（稳定）

指数代码：
    sh000300 = 沪深300   sh000016 = 上证50
    sz399852 = 中证1000  sz399905 = 中证500   sz399006 = 创业板指

计算指标：
    1. 小盘因子   = 中证1000日收益 - 沪深300日收益
    2. 动量近似   = 创业板指日收益 - 上证50日收益（成长动量代理）
    3. 成交额比值 = 今日全市场成交额 / 近20日平均成交额
       → 使用中证500成交量作为全市场代理（覆盖面广）
    4. 各指数日收益率及5日累计
"""
import logging
import time
from datetime import datetime, date, timedelta

import akshare as ak
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import REQUEST_INTERVAL, RETRY_DELAYS, FACTOR_THRESHOLDS
from db import upsert_factor_monitor

logger = logging.getLogger(__name__)


# ── 指数配置 ──────────────────────────────────────────────────────────────────

INDEX_MAP = {
    "csi300":  "sh000300",   # 沪深300
    "csi1000": "sz399852",   # 中证1000
    "csi500":  "sz399905",   # 中证500
    "gem":     "sz399006",   # 创业板指
    "sz50":    "sh000016",   # 上证50
}


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


def _calc_return(df: pd.DataFrame, trade_date: str) -> float | None:
    """
    从指数日线 DataFrame 中计算指定日期的日收益率（%）。
    df 列：date, open, high, low, close, volume
    """
    df = df.copy()
    df["date"] = df["date"].astype(str).str[:10]
    df = df.sort_values("date")

    target_idx = df[df["date"] == trade_date].index
    if target_idx.empty:
        return None

    pos = df.index.get_loc(target_idx[0])
    if pos == 0:
        return None

    curr_close = df.iloc[pos]["close"]
    prev_close = df.iloc[pos - 1]["close"]

    if prev_close == 0:
        return None

    return round((curr_close - prev_close) / prev_close * 100, 4)


def _calc_amount_ratio(df: pd.DataFrame, trade_date: str, window: int = 20) -> float | None:
    """
    计算当日成交额 / 近 N 日平均成交额。
    df 需有 date 和 amount 列（新浪源只有 volume，无 amount）。
    → 用 volume 代替 amount 计算比值（比值本身即可）。
    """
    df = df.copy()
    df["date"] = df["date"].astype(str).str[:10]
    df = df.sort_values("date").reset_index(drop=True)

    target_mask = df["date"] == trade_date
    if not target_mask.any():
        return None

    pos = df[target_mask].index[0]
    if pos < window:
        return None

    # 用 volume（成交量）作比值（若有 amount 用 amount）
    amt_col = "amount" if "amount" in df.columns else "volume"
    today_amt = df.iloc[pos][amt_col]
    past_avg = df.iloc[pos - window:pos][amt_col].mean()

    if past_avg == 0:
        return None

    return round(float(today_amt) / float(past_avg), 4)


# ── 获取指数数据 ──────────────────────────────────────────────────────────────

def fetch_index(symbol_key: str) -> pd.DataFrame | None:
    """获取单个指数的日线数据（新浪源）"""
    code = INDEX_MAP[symbol_key]
    time.sleep(REQUEST_INTERVAL)
    try:
        df = _retry(ak.stock_zh_index_daily, symbol=code)
        logger.debug("指数 %s(%s) 数据行数：%d", symbol_key, code, len(df))
        return df
    except Exception as e:
        logger.warning("指数 %s 获取失败：%s", symbol_key, e)
        return None


# ── 主计算逻辑 ────────────────────────────────────────────────────────────────

def compute_factors(trade_date: str) -> dict | None:
    """
    计算指定交易日的因子指标。

    返回格式（存入 factor_monitor 表）：
    {
        trade_date, csi1000_return, csi300_return, small_minus_large,
        gem_return, momentum_top20_return, momentum_bottom20_return,
        momentum_spread, volume_ratio
    }
    """
    logger.info("开始计算因子：%s", trade_date)

    results = {"trade_date": trade_date}

    # ── 指数日收益率 ──────────────────────────────────────────
    for key in ["csi300", "csi1000", "gem", "sz50", "csi500"]:
        df = fetch_index(key)
        if df is not None:
            ret = _calc_return(df, trade_date)
            if key == "csi300":
                results["csi300_return"] = ret
            elif key == "csi1000":
                results["csi1000_return"] = ret
            elif key == "gem":
                results["gem_return"] = ret
        else:
            if key == "csi300":
                results["csi300_return"] = None
            elif key == "csi1000":
                results["csi1000_return"] = None
            elif key == "gem":
                results["gem_return"] = None

    # ── 小盘因子 = 中证1000 - 沪深300 ──────────────────────────
    r1000 = results.get("csi1000_return")
    r300 = results.get("csi300_return")
    if r1000 is not None and r300 is not None:
        results["small_minus_large"] = round(r1000 - r300, 4)
    else:
        results["small_minus_large"] = None

    # ── 动量因子近似：创业板指 - 上证50 ─────────────────────────
    # 真实动量因子需要个股数据（太慢），这里用成长/价值指数spread近似
    df_gem = fetch_index("gem")
    df_sz50 = fetch_index("sz50")
    gem_ret = _calc_return(df_gem, trade_date) if df_gem is not None else None
    sz50_ret = _calc_return(df_sz50, trade_date) if df_sz50 is not None else None

    if gem_ret is not None and sz50_ret is not None:
        spread = round(gem_ret - sz50_ret, 4)
        results["momentum_top20_return"] = gem_ret    # 成长代理
        results["momentum_bottom20_return"] = sz50_ret  # 价值代理
        results["momentum_spread"] = spread
    else:
        results["momentum_top20_return"] = None
        results["momentum_bottom20_return"] = None
        results["momentum_spread"] = None

    # ── 成交额比值（用中证500成交额代理全市场）───────────────────
    df_csi500 = fetch_index("csi500")
    if df_csi500 is not None:
        vol_ratio = _calc_amount_ratio(df_csi500, trade_date, window=20)
        results["volume_ratio"] = vol_ratio
    else:
        results["volume_ratio"] = None

    # 日志汇总
    logger.info(
        "因子计算结果 → 小盘因子=%.2f%% 动量spread=%.2f%% 量比=%.2f",
        results.get("small_minus_large") or 0,
        results.get("momentum_spread") or 0,
        results.get("volume_ratio") or 0,
    )

    return results


def factor_status(results: dict) -> dict:
    """
    根据因子数值生成状态描述（供前端展示）。
    """
    status = {}

    # 小盘因子状态
    sml = results.get("small_minus_large")
    if sml is None:
        status["small_cap"] = "数据缺失"
    elif sml <= FACTOR_THRESHOLDS["small_cap_reversal_alert"]:
        status["small_cap"] = "踩踏预警"
    elif sml > 0:
        status["small_cap"] = "顺风"
    elif sml > -1:
        status["small_cap"] = "中性"
    else:
        status["small_cap"] = "逆风"

    # 动量因子状态
    spread = results.get("momentum_spread")
    if spread is None:
        status["momentum"] = "数据缺失"
    elif spread > 1.5:
        status["momentum"] = "成长强势"
    elif spread > 0:
        status["momentum"] = "中性偏成长"
    elif spread > -1.5:
        status["momentum"] = "中性偏价值"
    else:
        status["momentum"] = "价值强势"

    # 成交额比值状态
    vol = results.get("volume_ratio")
    if vol is None:
        status["volume"] = "数据缺失"
    elif vol > FACTOR_THRESHOLDS["volume_high"]:
        status["volume"] = "活跃"
    elif vol < FACTOR_THRESHOLDS["volume_low"]:
        status["volume"] = "缩量"
    else:
        status["volume"] = "正常"

    return status


# ── 主采集入口 ────────────────────────────────────────────────────────────────

def collect_factor_monitor(trade_date: str) -> bool:
    """
    采集并计算指定交易日的因子指标，写入 factor_monitor 表。
    返回：是否成功写入
    """
    logger.info("═══ 开始采集因子数据：%s ═══", trade_date)

    try:
        factors = compute_factors(trade_date)
    except Exception as e:
        logger.error("因子计算失败：%s", e, exc_info=True)
        return False

    if not factors:
        return False

    # 检查关键字段是否有效
    if factors.get("csi300_return") is None and factors.get("csi1000_return") is None:
        logger.warning("所有关键指数数据均缺失，跳过写入")
        return False

    try:
        success = upsert_factor_monitor(factors)
    except Exception as e:
        logger.error("因子数据写库失败：%s", e)
        return False

    # 打印状态摘要
    status = factor_status(factors)
    logger.info(
        "因子状态 → 小盘:%s(%.2f%%) 动量:%s(%.2f%%) 量比:%s(%.2f)",
        status["small_cap"], factors.get("small_minus_large") or 0,
        status["momentum"], factors.get("momentum_spread") or 0,
        status["volume"], factors.get("volume_ratio") or 0,
    )

    return success


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

    ok = collect_factor_monitor(test_date)
    logger.info("测试完成，写入成功：%s", ok)

    record = _db.get_factor_by_date(test_date)
    if record:
        print("\n因子数据：")
        print(json.dumps(record, ensure_ascii=False, indent=2))
