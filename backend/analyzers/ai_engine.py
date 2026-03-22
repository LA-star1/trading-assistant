"""
DeepSeek API 统一封装

所有 AI 功能通过此模块调用，支持优雅降级（无 API Key 时返回占位文本）。
"""
import json
import logging
import time
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL,
    DEEPSEEK_MODEL, DEEPSEEK_MAX_TOKENS, DEEPSEEK_TEMPERATURE,
)

logger = logging.getLogger(__name__)

# 每次 AI 调用记录 token 消耗
_token_log: list[dict] = []


def _has_api_key() -> bool:
    return bool(DEEPSEEK_API_KEY and DEEPSEEK_API_KEY.strip())


def call_deepseek(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = DEEPSEEK_MAX_TOKENS,
    temperature: float = DEEPSEEK_TEMPERATURE,
) -> str:
    """
    调用 DeepSeek API，返回纯文本响应。

    错误处理：
    - 未配置 API Key → 返回占位文本
    - 网络超时 → 重试3次，间隔2s
    - API限频 → 等待后重试
    - 其他错误 → 返回占位文本并记日志
    """
    if not _has_api_key():
        return "AI分析暂不可用（请在环境变量 DEEPSEEK_API_KEY 中配置 DeepSeek API Key）"

    try:
        import httpx
    except ImportError:
        return "AI分析暂不可用（请执行 pip install httpx）"

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    last_exc = None
    for attempt in range(3):
        if attempt > 0:
            time.sleep(2 * attempt)
        try:
            resp = httpx.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()

            # 记录 token 消耗
            usage = data.get("usage", {})
            _token_log.append({
                "time": datetime.now().isoformat(),
                "prompt_tokens":     usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens":      usage.get("total_tokens", 0),
            })

            content = data["choices"][0]["message"]["content"]
            # 长度限制（防止异常长文本）
            return content[:3000] if content else "AI返回空响应"

        except Exception as e:
            last_exc = e
            logger.warning("DeepSeek API 第%d次调用失败：%s", attempt + 1, e)

    logger.error("DeepSeek API 调用彻底失败：%s", last_exc)
    return "AI分析暂不可用（网络或API异常，请稍后重试）"


# ── 业务级函数 ────────────────────────────────────────────────────────────────

def generate_devils_advocate(
    stock_code: str,
    stock_name: str,
    direction: str,
    user_thesis: str,
    factor_data: dict,
) -> str:
    """
    魔鬼代言人：为用户的交易找反面论据。

    factor_data 键：momentum_20d, pe_percentile, volume_trend,
                    northbound_amount, sector
    """
    direction_cn = "买入" if direction == "buy" else "卖出"
    nb_dir = "净买入" if (factor_data.get("northbound_amount", 0) or 0) > 0 else "净卖出"
    nb_amt = abs(factor_data.get("northbound_amount", 0) or 0)

    system = "你是一个专业的A股分析师，扮演"魔鬼代言人"角色——专门寻找反面论据，帮助用户审视决策盲点。"
    user = f"""用户想要{direction_cn}{stock_name}（{stock_code}）。
用户的理由是：{user_thesis or "（未提供）"}

当前多因子数据：
- 20日动量：{factor_data.get('momentum_20d', 'N/A')}%
- PE分位（近3年）：{factor_data.get('pe_percentile', 'N/A')}%
- 成交量趋势：{factor_data.get('volume_trend', 'N/A')}
- 北向资金近5日：{nb_dir} {nb_amt:.0f}万元
- 所属行业：{factor_data.get('sector', '未知')}

请站在反方立场，指出这笔交易最可能失败的3个原因。要求：
1. 每个原因具体、有数据支撑
2. 如有近期利空事件或风险因素必须提及
3. 最后一句总结"如果用户错了，最可能是因为___"
4. 语气直接但不刻薄，像敬业的风控同事
5. 不要给出买入/卖出建议，只提供反面视角
6. 限制在150字以内"""

    return call_deepseek(system, user, max_tokens=300)


def generate_position_alert_interpretation(
    stock_name: str,
    stock_code: str,
    buy_price: float,
    current_price: float,
    pnl_percent: float,
    alert_type: str,
    alert_description: str,
) -> str:
    """持仓异动 AI 解读"""
    system = "你是A股交易助手，负责对持仓异动做冷静客观的解读。"
    user = f"""用户持有{stock_name}（{stock_code}），买入价{buy_price:.2f}元，当前价{current_price:.2f}元，盈亏{pnl_percent:+.1f}%。

触发异动告警：{alert_type}
具体情况：{alert_description}

请用3-4句话解读：
1. 发生了什么（事实）
2. 历史上类似情况通常怎么演变
3. 用户可能需要考虑什么（不给具体买卖建议）

语气冷静客观，不制造恐慌，也不轻描淡写。"""

    return call_deepseek(system, user, max_tokens=300)


def generate_morning_briefing(context: dict) -> dict:
    """
    早盘速览 AI 摘要。
    返回 {ai_summary: str, ai_focus_points: list[str]}
    """
    system = "你是A股交易助手。请根据以下信息生成简洁的开盘前摘要。"
    user = f"""用户当前持仓：{json.dumps(context.get('positions', []), ensure_ascii=False)}
用户关注列表：{json.dumps(context.get('watchlist', []), ensure_ascii=False)}

隔夜外盘：{json.dumps(context.get('overnight', {}), ensure_ascii=False)}
持仓相关新闻：{json.dumps(context.get('position_news', []), ensure_ascii=False)}
北向资金动态：{json.dumps(context.get('northbound', {}), ensure_ascii=False)}
今日催化事件：{json.dumps(context.get('catalysts', []), ensure_ascii=False)}

请：
1. 用1-2句话总结今日最需要关注的事项（和用户持仓直接相关的优先）
2. 列出今日3个关注重点（按优先级）
3. 如果持仓有利空，第一条必须提及

语言简洁直接，不给买卖建议，只描述事实和需要关注的点。
返回JSON格式：{{"ai_summary": "...", "ai_focus_points": ["...", "...", "..."]}}"""

    result = call_deepseek(system, user, max_tokens=400)
    try:
        # 尝试解析 JSON
        parsed = json.loads(result.strip().lstrip("```json").rstrip("```").strip())
        return parsed
    except Exception:
        return {"ai_summary": result, "ai_focus_points": []}


def generate_weekly_review(review_data: dict) -> str:
    """周度复盘报告"""
    system = "你是A股交易教练，负责帮助用户提升决策质量。"
    user = f"""本周绩效：
- 总盈亏：{review_data.get('total_pnl', 0):.0f}元（{review_data.get('total_pnl_percent', 0):.1f}%）
- 基准（沪深300）：{review_data.get('benchmark_return', 0):.1f}%
- 超额收益：{review_data.get('alpha', 0):.1f}%
- 胜率：{review_data.get('win_rate', 0):.0f}%
- 盈亏比：{review_data.get('profit_loss_ratio', 0):.1f}

本周交易明细：{json.dumps(review_data.get('trade_details', []), ensure_ascii=False)}

决策质量：
- 平均验证器评分：{review_data.get('avg_validation_score', 'N/A')}
- 低分仍执行次数：{review_data.get('low_score_trades', 0)}

检测到的行为偏差：{json.dumps(review_data.get('behavior_flags', []), ensure_ascii=False)}

请生成500字以内的周度复盘报告，包含：
1. 本周表现一句话总结
2. 做得好的地方（具体到哪笔交易）
3. 需要改进的地方（具体到哪笔交易）
4. 行为偏差提醒（如有）
5. 下周应该关注的1-2个重点

对事不对人，直接指出问题。做得好就表扬，不要每次都找问题。不说"建议买入/卖出"。"""

    return call_deepseek(system, user, max_tokens=800)


def get_token_usage_today() -> dict:
    """返回今日 token 消耗统计"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_logs = [l for l in _token_log if l["time"].startswith(today)]
    if not today_logs:
        return {"calls": 0, "total_tokens": 0, "estimated_cost_cny": 0}
    total = sum(l["total_tokens"] for l in today_logs)
    # DeepSeek-chat: 约 0.001元/1k tokens（参考价格）
    cost = total / 1000 * 0.001
    return {"calls": len(today_logs), "total_tokens": total, "estimated_cost_cny": round(cost, 4)}
