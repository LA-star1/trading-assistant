"""
量化雷达 — 全局配置
"""
import os

# ── 路径配置 ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "quant_radar.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
REPORT_DIR = os.path.join(BASE_DIR, "reports")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ── AKShare 请求配置 ──────────────────────────────────────
REQUEST_INTERVAL = 1.5      # 每次请求间隔（秒），避免触发限频
RETRY_DELAYS = [1, 3, 5]    # 重试等待时间（秒）

# ── 信号评分权重 ──────────────────────────────────────────
SCORE_WEIGHTS = {
    "net_amount": 0.35,       # 净买入金额权重
    "seat_count": 0.25,       # 席位数量权重
    "confidence": 0.20,       # 席位置信度权重
    "northbound_sync": 0.12,  # 北向共振权重
    "consecutive_days": 0.08, # 连续信号权重
}

# 置信度对应的评分乘数
CONFIDENCE_MULTIPLIER = {
    "high": 2.0,
    "medium": 1.0,
    "low": 0.5,
}

# 信号最低分数阈值（低于此值不入库）
MIN_SIGNAL_SCORE = 10.0

# ── 因子预警阈值 ──────────────────────────────────────────
FACTOR_THRESHOLDS = {
    "small_cap_reversal_alert": -2.0,  # 小盘因子单日反转预警（%）
    "small_cap_bull_days": 5,          # 连续N日为正 → 顺风
    "volume_high": 1.3,                # 成交额比值 > 此值 → 活跃
    "volume_low": 0.7,                 # 成交额比值 < 此值 → 缩量
}

# ── 量化私募关联席位初始种子库 ────────────────────────────
SEED_QUANT_SEATS = [
    # 高置信度（研报明确识别）
    {
        "seat_name": "中国中金财富证券有限公司北京宋庄路证券营业部",
        "linked_fund": "疑似幻方/明汯",
        "confidence": "high",
        "notes": "龙虎榜高频出现，单次金额大，偏好中小盘",
    },
    {
        "seat_name": "华泰证券股份有限公司总部",
        "linked_fund": "疑似量化私募M",
        "confidence": "high",
        "notes": "总部席位，频繁出现在量化活跃时期",
    },
    {
        "seat_name": "中国国际金融股份有限公司上海黄浦区湖滨路证券营业部",
        "linked_fund": "疑似量化私募M",
        "confidence": "high",
        "notes": "中金湖滨路，量化研报重点标注席位",
    },
    {
        "seat_name": "招商证券股份有限公司深圳深南东路证券营业部",
        "linked_fund": "疑似量化私募H",
        "confidence": "high",
        "notes": "深南东路席位，历史上榜频率极高",
    },
    {
        "seat_name": "国信证券股份有限公司深圳振华路证券营业部",
        "linked_fund": "疑似知名量化",
        "confidence": "high",
        "notes": "振华路席位，交易规律与量化策略高度吻合",
    },
    # 中置信度（统计推断）
    {
        "seat_name": "中国国际金融股份有限公司上海分公司",
        "linked_fund": "疑似量化",
        "confidence": "medium",
        "notes": "中金上海分公司，统计上与量化活跃期相关",
    },
    {
        "seat_name": "机构专用",
        "linked_fund": "机构（含量化）",
        "confidence": "medium",
        "notes": "机构专用席位，可能包含量化私募",
    },
    {
        "seat_name": "中信证券股份有限公司总部(非营业场所)",
        "linked_fund": "疑似量化",
        "confidence": "medium",
        "notes": "中信总部非营业场所，量化托管席位",
    },
    {
        "seat_name": "华泰证券股份有限公司上海分公司",
        "linked_fund": "疑似量化",
        "confidence": "medium",
        "notes": "华泰上海分公司",
    },
    {
        "seat_name": "中信建投证券股份有限公司总部(非营业场所)",
        "linked_fund": "疑似量化",
        "confidence": "medium",
        "notes": "中信建投总部非营业场所",
    },
]

# ── API 服务配置 ───────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8888
API_CORS_ORIGINS = ["http://localhost:5173", "http://localhost:3000"]
