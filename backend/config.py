"""
AI交易助手 — 全局配置
"""
import os

# ── 路径 ──────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "trading_assistant.db")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
REPORT_DIR = os.path.join(BASE_DIR, "reports")

for d in (LOG_DIR, REPORT_DIR):
    os.makedirs(d, exist_ok=True)

# ── DeepSeek API ──────────────────────────────────────────
# 申请地址：https://platform.deepseek.com/
DEEPSEEK_API_KEY  = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL    = "deepseek-chat"
DEEPSEEK_MAX_TOKENS = 1000
DEEPSEEK_TEMPERATURE = 0.3   # 低温度 = 更保守，减少胡说

# ── API 服务 ──────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8888
API_CORS_ORIGINS = ["http://localhost:5173", "http://localhost:3000"]

# ── AKShare 请求配置 ──────────────────────────────────────
REQUEST_INTERVAL = 1.5          # 每次请求间隔（秒）
RETRY_DELAYS     = [1, 3, 5]   # 重试等待时间序列

# ── 用户默认配置（写入 user_config 表，可通过 API 修改）────
DEFAULT_USER_CONFIG = {
    "total_capital":        "1500000",   # 总资金（元）
    "max_single_loss":      "100000",    # 单笔最大可承受亏损（元）
    "max_position_weight":  "25",        # 单只最大仓位占比（%）
    "max_sector_weight":    "40",        # 单行业最大占比（%）
    "risk_preference":      "moderate",  # conservative / moderate / aggressive
    "stop_loss_pct":        "7",         # 默认止损百分比（%）
}

# ── 交易验证器评分权重 ────────────────────────────────────
VALIDATOR_WEIGHTS = {
    "momentum":         0.20,
    "valuation":        0.20,
    "volume":           0.10,
    "northbound":       0.15,
    "correlation":      0.15,  # 相关性低=高分（分散化）
    "historical_win":   0.20,
}

# ── 量化私募关联席位种子库 ────────────────────────────────
SEED_QUANT_SEATS = [
    {"seat_name": "中国中金财富证券有限公司北京宋庄路证券营业部",     "linked_fund": "疑似幻方/明汯",    "confidence": "high"},
    {"seat_name": "华泰证券股份有限公司总部",                         "linked_fund": "疑似量化私募M",    "confidence": "high"},
    {"seat_name": "中国国际金融股份有限公司上海黄浦区湖滨路证券营业部","linked_fund": "疑似量化私募M",    "confidence": "high"},
    {"seat_name": "招商证券股份有限公司深圳深南东路证券营业部",       "linked_fund": "疑似量化私募H",    "confidence": "high"},
    {"seat_name": "国信证券股份有限公司深圳振华路证券营业部",         "linked_fund": "疑似知名量化",     "confidence": "high"},
    {"seat_name": "中国国际金融股份有限公司上海分公司",               "linked_fund": "疑似量化",         "confidence": "medium"},
    {"seat_name": "机构专用",                                         "linked_fund": "机构（含量化）",   "confidence": "medium"},
    {"seat_name": "中信证券股份有限公司总部(非营业场所)",             "linked_fund": "疑似量化",         "confidence": "medium"},
    {"seat_name": "华泰证券股份有限公司上海分公司",                   "linked_fund": "疑似量化",         "confidence": "medium"},
    {"seat_name": "中信建投证券股份有限公司总部(非营业场所)",         "linked_fund": "疑似量化",         "confidence": "medium"},
]
