"""
量化席位追踪分析器 — 整个系统的核心

功能：
    1. 席位匹配：将当日龙虎榜席位名称与 quant_seats 表做模糊匹配
    2. 信号评分：多维度综合评分（0-100）
    3. 信号汇总：生成当日量化雷达报告
"""
import json
import logging
import re
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    SCORE_WEIGHTS,
    CONFIDENCE_MULTIPLIER,
    MIN_SIGNAL_SCORE,
)
from db import (
    get_all_quant_seats,
    get_dragon_tiger_by_date,
    get_north_bound_by_date,
    get_signals_by_date,
    get_consecutive_signal_days,
    upsert_quant_signals,
    update_seat_stats,
)

logger = logging.getLogger(__name__)


# ── 文本标准化 ────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """
    席位名称标准化：
    - 去除首尾空格
    - 全角转半角
    - 统一公司类型简写
    """
    s = text.strip()
    # 全角转半角
    s = s.translate(str.maketrans(
        "　（）【】「」『』",
        " ()[]\"\"\"\"",
    ))
    # 统一常见简写差异
    s = s.replace("有限责任公司", "有限公司")
    s = s.replace("股份有限公司", "有限公司")  # 先统一为有限公司再重建规范
    # 重建规范：证券公司全称中的"股份有限公司"恢复
    # （实际情况是席位名带"股份有限公司"的多，不做此替换，只做空格清理）
    s = re.sub(r"\s+", "", s)  # 去除所有内部空格
    return s


def _edit_distance(a: str, b: str) -> int:
    """计算两个字符串的编辑距离（Levenshtein Distance）"""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    m, n = len(a), len(b)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            temp = dp[j]
            if a[i - 1] == b[j - 1]:
                dp[j] = prev
            else:
                dp[j] = 1 + min(prev, dp[j], dp[j - 1])
            prev = temp
    return dp[n]


# ── 席位匹配器 ────────────────────────────────────────────────────────────────

class SeatMatcher:
    """
    量化席位匹配器。
    初始化时加载 quant_seats 表，提供快速匹配接口。
    """

    # 编辑距离阈值：归一化距离（edit_distance / max_len）< 此值则认为匹配
    EDIT_DISTANCE_THRESHOLD = 0.15

    def __init__(self):
        self._seats = get_all_quant_seats()
        # 预计算标准化名称
        self._normalized = {
            seat["seat_name"]: _normalize(seat["seat_name"])
            for seat in self._seats
        }
        logger.info("席位匹配器初始化，已加载 %d 个量化席位", len(self._seats))

    def match(self, raw_seat_name: str) -> dict | None:
        """
        将龙虎榜席位名称与量化席位库匹配。

        匹配优先级：
        1. 精确匹配（原始名称）
        2. 标准化后精确匹配
        3. 包含关系匹配（一方包含另一方的关键词）
        4. 编辑距离模糊匹配

        返回匹配到的席位记录，或 None（未匹配）。
        """
        if not raw_seat_name or not raw_seat_name.strip():
            return None

        norm_raw = _normalize(raw_seat_name)

        # 1. 精确匹配
        for seat in self._seats:
            if seat["seat_name"] == raw_seat_name:
                return seat

        # 2. 标准化后精确匹配
        for seat in self._seats:
            if self._normalized[seat["seat_name"]] == norm_raw:
                return seat

        # 3. 包含关系匹配（关键词包含）
        # 提取席位名中的"机构关键词"（证券公司名+营业部地址）
        for seat in self._seats:
            norm_seat = self._normalized[seat["seat_name"]]
            # 如果输入名称包含库中席位名（或反向），认为匹配
            if len(norm_seat) >= 8 and len(norm_raw) >= 8:
                if norm_seat in norm_raw or norm_raw in norm_seat:
                    logger.debug("包含匹配：'%s' ↔ '%s'", raw_seat_name, seat["seat_name"])
                    return seat

        # 4. 编辑距离模糊匹配（对较长名称效果好）
        if len(norm_raw) >= 10:
            best_seat = None
            best_ratio = self.EDIT_DISTANCE_THRESHOLD

            for seat in self._seats:
                norm_seat = self._normalized[seat["seat_name"]]
                if abs(len(norm_seat) - len(norm_raw)) > 5:
                    continue  # 长度差异过大，跳过
                dist = _edit_distance(norm_raw, norm_seat)
                max_len = max(len(norm_raw), len(norm_seat))
                ratio = dist / max_len
                if ratio < best_ratio:
                    best_ratio = ratio
                    best_seat = seat

            if best_seat:
                logger.debug(
                    "编辑距离匹配（距离比=%.2f）：'%s' ↔ '%s'",
                    best_ratio, raw_seat_name, best_seat["seat_name"],
                )
                return best_seat

        return None

    def reload(self):
        """重新从数据库加载席位列表"""
        self.__init__()


# ── 信号评分 ──────────────────────────────────────────────────────────────────

def _calc_amount_score(net_amount_wan: float) -> float:
    """
    根据净买入金额（万元）计算分项分数（0-100）。
    使用对数缩放：1亿=60分，5亿≈90分，10亿=100分封顶。
    """
    if net_amount_wan <= 0:
        return 0.0
    import math
    # 1亿 = 10000万 → 约60分
    score = min(100.0, 20 * math.log10(net_amount_wan + 1))
    return round(score, 2)


def _calc_seat_count_score(seat_count: int) -> float:
    """
    根据参与量化席位数量计算分项分数（0-100）。
    1席位=30分，2席位=60分，3席位=80分，4+席位=100分。
    """
    mapping = {1: 30, 2: 60, 3: 80}
    return float(mapping.get(seat_count, 100 if seat_count >= 4 else 0))


def _calc_confidence_score(seats_info: list[dict]) -> float:
    """
    根据参与席位的置信度加权平均计算分项分数（0-100）。
    high=100, medium=50, low=25。
    """
    conf_scores = {"high": 100, "medium": 50, "low": 25}
    if not seats_info:
        return 0.0
    total = sum(
        conf_scores.get(s.get("confidence", "medium"), 50)
        for s in seats_info
    )
    return round(total / len(seats_info), 2)


def _calc_northbound_score(stock_code: str, signal_type: str, nb_data: dict) -> float:
    """
    北向资金共振评分（0-100）。

    评分逻辑（两层）：
    1. 个股层：北向对该股净买/净卖方向与量化信号一致 → 100分（暂缺，接口不稳定）
    2. 市场层（fallback）：北向总体净流入 + 量化净买 → 60分；反向 → 0分；中性 → 30分

    nb_data 结构：
        {'__TOTAL_NORTH__': {'net_buy_amount': 万元}, ...}
    """
    # 个股层（若有数据）
    if stock_code in nb_data:
        nb_net = nb_data[stock_code].get("net_buy_amount", 0)
        if nb_net != 0:
            if signal_type == "quant_buy" and nb_net > 0:
                return 100.0
            if signal_type == "quant_sell" and nb_net < 0:
                return 100.0
            if signal_type == "quant_both":
                return 50.0
            return 0.0

    # 市场层 fallback（使用北向总流入方向）
    total = nb_data.get("__TOTAL_NORTH__", {})
    total_net = total.get("net_buy_amount", 0)

    if total_net == 0:
        return 0.0

    nb_positive = total_net > 0  # 北向今日净流入

    if signal_type == "quant_buy" and nb_positive:
        return 60.0   # 市场层共振，打折
    if signal_type == "quant_sell" and not nb_positive:
        return 60.0
    if signal_type == "quant_both":
        return 30.0   # 双向操作，弱共振

    return 0.0


def _calc_consecutive_score(consecutive_days: int) -> float:
    """
    连续信号天数评分（0-100）。
    1天=0, 2天=40, 3天=70, 4天=90, 5天+=100。
    """
    mapping = {0: 0, 1: 0, 2: 40, 3: 70, 4: 90}
    return float(mapping.get(consecutive_days, 100 if consecutive_days >= 5 else 0))


def compute_signal_score(
    net_amount_wan: float,
    seat_count: int,
    seats_info: list[dict],
    signal_type: str,
    stock_code: str,
    nb_data: dict,
    consecutive_days: int,
) -> float:
    """
    综合信号评分（0-100）。
    各维度加权求和：净买入金额 + 席位数量 + 席位置信度 + 北向共振 + 连续信号。
    """
    w = SCORE_WEIGHTS
    amount_score = _calc_amount_score(abs(net_amount_wan))
    seat_cnt_score = _calc_seat_count_score(seat_count)
    conf_score = _calc_confidence_score(seats_info)
    nb_score = _calc_northbound_score(stock_code, signal_type, nb_data)
    consec_score = _calc_consecutive_score(consecutive_days)

    total = (
        w["net_amount"] * amount_score
        + w["seat_count"] * seat_cnt_score
        + w["confidence"] * conf_score
        + w["northbound_sync"] * nb_score
        + w["consecutive_days"] * consec_score
    )
    return round(total, 2)


# ── 主分析函数 ────────────────────────────────────────────────────────────────

def analyze_date(trade_date: str) -> dict:
    """
    对指定交易日运行量化席位分析：
    1. 从数据库读取当日龙虎榜数据
    2. 席位匹配
    3. 计算信号 + 评分
    4. 写入 quant_signals 表
    5. 返回当日汇总报告

    返回格式（同文档设计）：
    {
        "date": "2026-03-22",
        "quant_buy_stocks": [...],
        "quant_sell_stocks": [...],
        "top_signals": [...],
        "sector_heatmap": {},
        "alert_messages": [],
    }
    """
    logger.info("═══ 开始量化席位分析：%s ═══", trade_date)

    # 读取原始数据
    dt_rows = get_dragon_tiger_by_date(trade_date)
    if not dt_rows:
        logger.warning("龙虎榜数据为空，跳过分析：%s", trade_date)
        return _empty_report(trade_date)

    logger.info("读取龙虎榜记录：%d 条", len(dt_rows))

    # 读取北向资金数据（供共振评分使用）
    nb_rows = get_north_bound_by_date(trade_date)
    nb_data = {r["stock_code"]: r for r in nb_rows}
    logger.info("读取北向资金记录：%d 条", len(nb_rows))

    # 初始化席位匹配器
    matcher = SeatMatcher()

    # ── 按股票聚合量化席位交易 ──────────────────────────────
    # stock_code → {buy_seats, sell_seats, buy_amount, sell_amount, stock_name}
    stock_agg: dict[str, dict] = {}

    matched_count = 0
    for row in dt_rows:
        stock_code = row["stock_code"]
        if stock_code not in stock_agg:
            stock_agg[stock_code] = {
                "stock_name": row["stock_name"] or "",
                "buy_seats": [],   # [(seat_record, amount_wan)]
                "sell_seats": [],
                "total_buy": 0.0,
                "total_sell": 0.0,
            }

        agg = stock_agg[stock_code]
        seat_name = row.get("seat_name", "")

        if not seat_name:
            continue

        seat = matcher.match(seat_name)
        if not seat:
            continue

        matched_count += 1
        buy_amt = row.get("buy_amount") or 0.0
        sell_amt = row.get("sell_amount") or 0.0

        # 同一席位在一只股票中既买又卖（做 T）→ 分别计入
        if buy_amt > 0:
            agg["buy_seats"].append((seat, buy_amt))
            agg["total_buy"] += buy_amt
        if sell_amt > 0:
            agg["sell_seats"].append((seat, sell_amt))
            agg["total_sell"] += sell_amt

        update_seat_stats(seat["seat_name"], trade_date)

    logger.info("席位匹配完成：%d 条记录命中量化席位", matched_count)

    # ── 生成量化信号 ──────────────────────────────────────────
    signal_records = []

    for stock_code, agg in stock_agg.items():
        buy_seats = agg["buy_seats"]
        sell_seats = agg["sell_seats"]

        if not buy_seats and not sell_seats:
            continue  # 该股票无量化席位参与

        # 确定信号类型
        if buy_seats and sell_seats:
            signal_type = "quant_both"
        elif buy_seats:
            signal_type = "quant_buy"
        else:
            signal_type = "quant_sell"

        net_amount = agg["total_buy"] - agg["total_sell"]

        # 参与席位去重（同一席位可能在多行记录中出现）
        all_seat_records_buy = [s for s, _ in buy_seats]
        all_seat_records_sell = [s for s, _ in sell_seats]
        all_seats_info = all_seat_records_buy + all_seat_records_sell

        # 去重（按席位名）
        seen = set()
        unique_seats_info = []
        for s in all_seats_info:
            if s["seat_name"] not in seen:
                unique_seats_info.append(s)
                seen.add(s["seat_name"])

        seat_count = len(unique_seats_info)
        seat_names_list = [s["seat_name"] for s in unique_seats_info]

        # 查询连续信号天数
        consecutive_days = get_consecutive_signal_days(stock_code, trade_date)

        # 计算综合评分
        score = compute_signal_score(
            net_amount_wan=net_amount,
            seat_count=seat_count,
            seats_info=unique_seats_info,
            signal_type=signal_type,
            stock_code=stock_code,
            nb_data=nb_data,
            consecutive_days=consecutive_days,
        )

        if score < MIN_SIGNAL_SCORE:
            continue  # 过滤低分信号

        signal_records.append({
            "trade_date": trade_date,
            "stock_code": stock_code,
            "stock_name": agg["stock_name"],
            "signal_type": signal_type,
            "seat_names": json.dumps(seat_names_list, ensure_ascii=False),
            "total_buy_amount": round(agg["total_buy"], 2),
            "total_sell_amount": round(agg["total_sell"], 2),
            "net_amount": round(net_amount, 2),
            "seat_count": seat_count,
            "score": score,
        })

    # 按评分排序
    signal_records.sort(key=lambda x: x["score"], reverse=True)

    # 写入数据库
    written = upsert_quant_signals(signal_records)
    logger.info("量化信号写入：%d 条（过滤后）", written)

    # ── 生成汇总报告 ──────────────────────────────────────────
    report = _build_report(trade_date, signal_records, nb_data)

    # 打印精简版到控制台
    _print_report(report)

    return report


def _build_report(trade_date: str, signals: list[dict], nb_data: dict) -> dict:
    """构建当日量化雷达汇总报告"""
    buy_stocks = [s for s in signals if s["signal_type"] == "quant_buy"]
    sell_stocks = [s for s in signals if s["signal_type"] == "quant_sell"]
    both_stocks = [s for s in signals if s["signal_type"] == "quant_both"]

    top10 = signals[:10]

    # 异动提醒
    alerts = []
    if any(s["score"] >= 80 for s in signals):
        high_score = [s for s in signals if s["score"] >= 80]
        alerts.append(
            f"⚠️ {len(high_score)} 只股票量化信号强度≥80分，请重点关注：" +
            "、".join(s["stock_code"] + s["stock_name"] for s in high_score[:3])
        )

    if len(buy_stocks) >= 5:
        alerts.append(f"📈 量化资金今日净买入覆盖 {len(buy_stocks)} 只股票，整体偏多")

    return {
        "date": trade_date,
        "total_signals": len(signals),
        "quant_buy_stocks": [_signal_brief(s) for s in buy_stocks],
        "quant_sell_stocks": [_signal_brief(s) for s in sell_stocks],
        "quant_both_stocks": [_signal_brief(s) for s in both_stocks],
        "top_signals": [_signal_brief(s) for s in top10],
        "sector_heatmap": {},  # Phase 2 补充（需要行业数据）
        "alert_messages": alerts,
    }


def _signal_brief(s: dict) -> dict:
    """信号简报（去掉冗余字段，前端友好格式）"""
    seat_names = json.loads(s["seat_names"]) if isinstance(s["seat_names"], str) else s["seat_names"]
    return {
        "stock_code": s["stock_code"],
        "stock_name": s["stock_name"],
        "signal_type": s["signal_type"],
        "seat_names": seat_names,
        "seat_count": s["seat_count"],
        "total_buy_amount": s["total_buy_amount"],
        "total_sell_amount": s["total_sell_amount"],
        "net_amount": s["net_amount"],
        "score": s["score"],
    }


def _empty_report(trade_date: str) -> dict:
    return {
        "date": trade_date,
        "total_signals": 0,
        "quant_buy_stocks": [],
        "quant_sell_stocks": [],
        "quant_both_stocks": [],
        "top_signals": [],
        "sector_heatmap": {},
        "alert_messages": ["当日无龙虎榜数据（可能是非交易日）"],
    }


def _print_report(report: dict):
    """控制台打印精简报告"""
    print("\n" + "═" * 60)
    print(f"  量化雷达 — {report['date']}")
    print("═" * 60)
    print(f"  今日量化信号总数：{report['total_signals']}")
    print(f"  量化净买入：{len(report['quant_buy_stocks'])} 只")
    print(f"  量化净卖出：{len(report['quant_sell_stocks'])} 只")
    print(f"  量化双向：{len(report['quant_both_stocks'])} 只")

    if report["top_signals"]:
        print("\n  TOP 信号（按评分降序）：")
        for i, s in enumerate(report["top_signals"][:5], 1):
            net_str = f"+{s['net_amount']:.0f}万" if s["net_amount"] > 0 else f"{s['net_amount']:.0f}万"
            seats_str = "、".join(s["seat_names"][:2])
            if len(s["seat_names"]) > 2:
                seats_str += f" 等{len(s['seat_names'])}席位"
            print(f"  {i}. [{s['score']:.0f}分] {s['stock_code']} {s['stock_name']} "
                  f"净{net_str}  席位:{seats_str}")

    if report["alert_messages"]:
        print("\n  异动提醒：")
        for msg in report["alert_messages"]:
            print(f"  {msg}")

    print("═" * 60 + "\n")


# ── 快捷测试入口 ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-21"
    report = analyze_date(test_date)

    print(f"\n完整报告JSON：")
    print(json.dumps(report, ensure_ascii=False, indent=2))
