"""
交易验证器 — 用户主动调用，在下单前使用

输入：stock_code, direction('buy'/'sell'), user_thesis
输出：多因子扫描 + 仓位建议 + 历史走势匹配 + 魔鬼代言人 + 综合评分
结果写入 validation_records 表
"""
import json
import logging
import math
from datetime import datetime, date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import VALIDATOR_WEIGHTS
from db import get_conn, get_user_config, get_active_positions
from collectors.stock_fundamentals import (
    get_stock_info, get_momentum, get_volume_trend,
    get_pe_percentile, get_historical_similarity, get_daily_history,
)
from analyzers.ai_engine import generate_devils_advocate

logger = logging.getLogger(__name__)


# ── 各因子评分 ────────────────────────────────────────────────────────────────

def _score_momentum(momentum_20d: float | None, direction: str) -> float:
    """动量评分 0-100"""
    if momentum_20d is None:
        return 50.0
    if direction == "buy":
        if momentum_20d > 5:   return 80.0
        if momentum_20d >= 0:  return 60.0
        return 40.0
    else:  # sell
        if momentum_20d < -5:  return 80.0
        if momentum_20d <= 0:  return 60.0
        return 40.0


def _score_valuation(pe_percentile: float | None, direction: str) -> float:
    """估值分位评分 0-100（买入时低估=高分，卖出时高估=高分）"""
    if pe_percentile is None:
        return 50.0
    if direction == "buy":
        if pe_percentile < 30:  return 80.0
        if pe_percentile < 70:  return 60.0
        return 40.0
    else:
        if pe_percentile > 70:  return 80.0
        if pe_percentile > 30:  return 60.0
        return 40.0


def _score_northbound(nb_net_wan: float | None, direction: str) -> float:
    """北向资金评分 0-100"""
    if nb_net_wan is None:
        return 50.0
    if direction == "buy":
        return 70.0 if nb_net_wan > 0 else 35.0
    else:
        return 70.0 if nb_net_wan < 0 else 35.0


def _score_correlation(stock_code: str, positions: list[dict]) -> tuple[float, float]:
    """
    持仓相关性评分（相关性低=高分）。
    同时返回买入后行业暴露最大占比（%），用于超限检测。
    """
    if not positions:
        return 80.0, 0.0

    try:
        target_sector = get_stock_info(stock_code).get("sector", "")
        # Fetch all sectors in one pass to avoid N separate calls
        sector_map = {p["stock_code"]: get_stock_info(p["stock_code"]).get("sector", "")
                      for p in positions}
        same_sector_weight = sum(
            p.get("current_weight", 0) or 0
            for p in positions
            if sector_map.get(p["stock_code"]) == target_sector
        )
        # 买入后该行业暴露（假设新仓位 5%）
        sector_exposure_after = same_sector_weight + 5.0
        # 相关性高（同行业已有持仓）→ 减分
        score = 80.0 if same_sector_weight < 10 else (60.0 if same_sector_weight < 20 else 35.0)
        return score, sector_exposure_after
    except Exception as e:
        logger.warning("相关性评分失败：%s", e)
        return 50.0, 0.0


def _score_historical_win(similarity: dict) -> float:
    """历史相似胜率评分 0-100"""
    win_rate = similarity.get("win_rate_10d")
    if win_rate is None or similarity.get("count", 0) < 3:
        return 50.0
    if win_rate >= 65:  return 80.0
    if win_rate >= 50:  return 60.0
    return 40.0


# ── 仓位计算 ──────────────────────────────────────────────────────────────────

def _calc_position_sizing(
    current_price: float,
    cfg: dict,
) -> dict:
    """
    基于凯利公式简化版计算建议仓位。

    公式：
        shares = max_single_loss / (current_price * stop_loss_pct / 100)
        weight = shares * current_price / total_capital
        限制在 max_position_weight 以内
    """
    try:
        total_capital     = float(cfg.get("total_capital", 1_500_000))
        max_single_loss   = float(cfg.get("max_single_loss", 100_000))
        stop_loss_pct     = float(cfg.get("stop_loss_pct", 7))
        max_pos_weight    = float(cfg.get("max_position_weight", 25))

        stop_loss_dist    = current_price * stop_loss_pct / 100
        shares_suggested  = max_single_loss / stop_loss_dist if stop_loss_dist > 0 else 0
        weight_suggested  = shares_suggested * current_price / total_capital * 100
        weight_capped     = min(weight_suggested, max_pos_weight)

        return {
            "suggested_weight_min":  round(weight_capped * 0.7, 1),
            "suggested_weight_max":  round(weight_capped, 1),
            "stop_loss_reference":   round(current_price * (1 - stop_loss_pct / 100), 2),
            "max_loss_amount":       round(weight_capped / 100 * total_capital * stop_loss_pct / 100, 0),
        }
    except Exception as e:
        logger.warning("仓位计算失败：%s", e)
        return {
            "suggested_weight_min": 3.0, "suggested_weight_max": 5.0,
            "stop_loss_reference": current_price * 0.93, "max_loss_amount": 0,
        }


# ── 综合评分 ──────────────────────────────────────────────────────────────────

def _composite_score(
    scores: dict,
    sector_exposure_after: float,
    max_sector_weight: float,
    stop_loss_pct: float,
) -> float:
    w = VALIDATOR_WEIGHTS
    total = (
        w["momentum"]       * scores.get("momentum", 50)
        + w["valuation"]    * scores.get("valuation", 50)
        + w["volume"]       * scores.get("volume", 50)
        + w["northbound"]   * scores.get("northbound", 50)
        + w["correlation"]  * scores.get("correlation", 50)
        + w["historical_win"] * scores.get("historical_win", 50)
    )
    # 行业暴露超限 → 分数上限 60
    if sector_exposure_after > max_sector_weight:
        total = min(total, 60.0)
    # 止损距离 > 10% → 扣10分
    if stop_loss_pct > 10:
        total -= 10
    return round(max(0, min(100, total)), 1)


# ── 主入口 ────────────────────────────────────────────────────────────────────

def validate_trade(
    stock_code: str,
    direction: str,
    user_thesis: str = "",
    call_ai: bool = True,
) -> dict:
    """
    运行完整的交易验证流程。

    参数：
        stock_code:  6位股票代码
        direction:   'buy' 或 'sell'
        user_thesis: 用户输入的买入/卖出理由
        call_ai:     是否调用魔鬼代言人（可关闭节省API消耗）

    返回：完整验证结果 dict，同时写入 validation_records 表
    """
    logger.info("═══ 交易验证开始：%s %s ═══", direction, stock_code)
    today = date.today().strftime("%Y-%m-%d")
    cfg = get_user_config()

    # ── Step 1：基本信息 ──────────────────────────────────────
    stock_info = get_stock_info(stock_code)
    stock_name = stock_info.get("stock_name", stock_code)
    sector     = stock_info.get("sector", "未知")

    # 获取最新价
    df = get_daily_history(stock_code, days=25)
    current_price = float(df.iloc[-1]["close"]) if not df.empty else 0.0
    logger.info("当前价格：%.2f  行业：%s", current_price, sector)

    # ── Step 2：因子计算 ──────────────────────────────────────
    momentum_20d   = get_momentum(stock_code, 20)
    pe_percentile  = get_pe_percentile(stock_code)
    vol_info       = get_volume_trend(stock_code)
    volume_trend   = vol_info["trend"]

    # 北向资金（从数据库读取）
    nb_net = _get_northbound_net(stock_code)

    # 持仓相关性
    positions = get_active_positions()
    corr_score, sector_exposure_after = _score_correlation(stock_code, positions)

    # 历史走势匹配
    similarity = get_historical_similarity(stock_code)

    logger.info(
        "因子：动量=%.1f%% PE分位=%.0f%% 量能=%s 北向=%.0f万",
        momentum_20d or 0, pe_percentile or 0, volume_trend, nb_net or 0,
    )

    # ── Step 3：因子评分 ──────────────────────────────────────
    scores = {
        "momentum":       _score_momentum(momentum_20d, direction),
        "valuation":      _score_valuation(pe_percentile, direction),
        "volume":         vol_info["score"],
        "northbound":     _score_northbound(nb_net, direction),
        "correlation":    corr_score,
        "historical_win": _score_historical_win(similarity),
    }

    max_sector_weight = float(cfg.get("max_sector_weight", 40))
    stop_loss_pct     = float(cfg.get("stop_loss_pct", 7))
    overall_score = _composite_score(scores, sector_exposure_after, max_sector_weight, stop_loss_pct)
    logger.info("综合评分：%.1f", overall_score)

    # ── Step 4：仓位建议 ──────────────────────────────────────
    sizing = _calc_position_sizing(current_price, cfg)

    # ── Step 5：魔鬼代言人（AI） ──────────────────────────────
    devils_text = ""
    if call_ai:
        factor_data = {
            "momentum_20d":    momentum_20d,
            "pe_percentile":   pe_percentile,
            "volume_trend":    volume_trend,
            "northbound_amount": nb_net,
            "sector":          sector,
        }
        devils_text = generate_devils_advocate(
            stock_code, stock_name, direction, user_thesis, factor_data
        )
        logger.info("魔鬼代言人：%s…", devils_text[:50])

    # ── Step 6：检查量化席位信号 ──────────────────────────────
    quant_signal = _get_quant_signal(stock_code)

    # ── Step 7：组装结果 ──────────────────────────────────────
    result = {
        "validate_date":   today,
        "stock_code":      stock_code,
        "stock_name":      stock_name,
        "direction":       direction,
        "current_price":   current_price,
        "sector":          sector,
        "user_thesis":     user_thesis,
        # 因子数据
        "momentum_20d":              momentum_20d,
        "pe_percentile":             pe_percentile,
        "volume_trend":              volume_trend,
        "northbound_change":         nb_net,
        "correlation_with_portfolio": corr_score,
        "sector_exposure_after":     sector_exposure_after,
        # 评分明细
        "factor_scores":   scores,
        "overall_score":   overall_score,
        # 仓位建议
        **sizing,
        # 历史走势
        "similar_pattern_count": similarity.get("count", 0),
        "win_rate_10d":          similarity.get("win_rate_10d"),
        "avg_return_10d":        similarity.get("avg_return_10d"),
        "max_drawdown":          similarity.get("max_drawdown"),
        # AI 魔鬼代言人
        "devils_advocate_text":  devils_text,
        # 量化席位信号
        "quant_signal": quant_signal,
        # 警告
        "warnings": _build_warnings(
            sector_exposure_after, max_sector_weight,
            stop_loss_pct, overall_score, quant_signal,
        ),
    }

    # ── Step 8：写库 ──────────────────────────────────────────
    _save_validation(result)

    return result


def _get_northbound_net(stock_code: str) -> float | None:
    """从数据库读取近5日北向资金净买入（万元）"""
    # Use subquery so LIMIT applies before SUM (not after)
    sql = """
        SELECT SUM(net_buy_amount) AS total
        FROM (
            SELECT net_buy_amount FROM north_bound
            WHERE stock_code=? ORDER BY trade_date DESC LIMIT 5
        )
    """
    with get_conn() as conn:
        row = conn.execute(sql, (stock_code,)).fetchone()
        if row and row["total"] is not None:
            return float(row["total"])
        # 回退：用市场总体北向方向
        row = conn.execute(sql, ("__TOTAL_NORTH__",)).fetchone()
    return float(row["total"]) if row and row["total"] else None


def _get_quant_signal(stock_code: str) -> dict | None:
    """查询该股近5日量化席位信号"""
    from datetime import timedelta
    start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM quant_signals
            WHERE stock_code=? AND trade_date>=?
            ORDER BY trade_date DESC
            LIMIT 3
        """, (stock_code, start)).fetchall()
    if not rows:
        return None
    latest = dict(rows[0])
    return {
        "signal_type": latest["signal_type"],
        "net_amount":  latest["net_amount"],
        "score":       latest["score"],
        "date":        latest["trade_date"],
    }


def _build_warnings(
    sector_exposure_after: float,
    max_sector_weight: float,
    stop_loss_pct: float,
    overall_score: float,
    quant_signal: dict | None,
) -> list[str]:
    warnings = []
    if sector_exposure_after > max_sector_weight:
        warnings.append(f"⚠️ 买入后行业暴露 {sector_exposure_after:.0f}% > 上限 {max_sector_weight:.0f}%")
    if stop_loss_pct > 10:
        warnings.append(f"⚠️ 默认止损距离 {stop_loss_pct:.0f}% 偏大，建议压缩至7%以内")
    if overall_score < 40:
        warnings.append("🔴 综合评分较低（< 40分），请仔细评估")
    if quant_signal and quant_signal.get("signal_type") == "quant_sell":
        warnings.append(f"⚠️ 近期量化席位净卖出 {abs(quant_signal.get('net_amount',0)):.0f}万，注意逆势风险")
    return warnings


def _save_validation(result: dict):
    """将验证结果写入 validation_records 表"""
    try:
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO validation_records (
                    validate_date, stock_code, stock_name, direction, user_thesis,
                    momentum_20d, pe_percentile, volume_trend, northbound_change,
                    correlation_with_portfolio, sector_exposure_after,
                    devils_advocate_text,
                    suggested_weight_min, suggested_weight_max,
                    stop_loss_reference, max_loss_amount,
                    similar_pattern_count, win_rate_10d, avg_return_10d, max_drawdown,
                    overall_score
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                result["validate_date"], result["stock_code"], result["stock_name"],
                result["direction"], result["user_thesis"],
                result.get("momentum_20d"), result.get("pe_percentile"),
                result.get("volume_trend"), result.get("northbound_change"),
                result.get("correlation_with_portfolio"), result.get("sector_exposure_after"),
                result.get("devils_advocate_text"),
                result.get("suggested_weight_min"), result.get("suggested_weight_max"),
                result.get("stop_loss_reference"), result.get("max_loss_amount"),
                result.get("similar_pattern_count"), result.get("win_rate_10d"),
                result.get("avg_return_10d"), result.get("max_drawdown"),
                result.get("overall_score"),
            ))
        logger.info("验证记录已保存")
    except Exception as e:
        logger.warning("验证记录写库失败：%s", e)


if __name__ == "__main__":
    import json, sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from db import init_db, insert_seed_data
    init_db()
    insert_seed_data()

    code = sys.argv[1] if len(sys.argv) > 1 else "002594"
    result = validate_trade(code, "buy", "看好新能源车3月销量超预期", call_ai=True)
    print("\n验证结果：")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
