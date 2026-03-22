"""
同步器抽象基类 — 定义所有同步器必须实现的统一接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BrokerPosition:
    """券商返回的持仓数据，统一格式"""
    stock_code:          str    # 6位纯数字
    stock_name:          str
    shares:              int    # 持股数量
    available_shares:    int    # 可卖数量
    cost_price:          float  # 成本价
    current_price:       float  # 最新价
    market_value:        float  # 市值（元）
    pnl:                 float  # 浮动盈亏（元）
    pnl_percent:         float  # 浮动盈亏比例（%）
    broker_position_id:  str = ""


@dataclass
class BrokerTrade:
    """券商返回的成交数据，统一格式"""
    trade_date:      str    # YYYY-MM-DD
    trade_time:      str    # HH:MM:SS
    stock_code:      str
    stock_name:      str
    direction:       str    # 'buy' / 'sell'
    price:           float
    shares:          int
    amount:          float  # 成交金额（元）
    commission:      float  # 佣金（元）
    broker_order_id: str = ""


@dataclass
class BrokerBalance:
    """账户资金信息"""
    total_assets:   float   # 总资产（元）
    available_cash: float   # 可用资金（元）
    market_value:   float   # 持仓市值（元）
    frozen_cash:    float   # 冻结资金（元）


class BaseSyncer(ABC):
    """所有同步器的基类"""

    @abstractmethod
    def connect(self) -> bool:
        """建立连接/登录，返回是否成功"""

    @abstractmethod
    def get_positions(self) -> list[BrokerPosition]:
        """获取当前持仓列表"""

    @abstractmethod
    def get_today_trades(self) -> list[BrokerTrade]:
        """获取今日成交记录"""

    @abstractmethod
    def get_history_trades(self, start_date: str, end_date: str) -> list[BrokerTrade]:
        """获取历史成交记录（start_date/end_date 格式 YYYY-MM-DD）"""

    @abstractmethod
    def get_balance(self) -> Optional[BrokerBalance]:
        """获取账户资金信息"""

    @abstractmethod
    def disconnect(self):
        """断开连接/清理"""

    # ── 工具方法（子类可直接用）───────────────────────────
    @staticmethod
    def clean_code(raw: str) -> str:
        """统一股票代码为6位纯数字"""
        s = str(raw).strip().upper()
        for prefix in ("SH", "SZ", "BJ"):
            s = s.replace(prefix, "")
        s = s.strip(".")
        return s.zfill(6)[:6]

    @staticmethod
    def parse_direction(raw: str) -> str:
        """将中文买卖方向转为 'buy'/'sell'，无法识别返回空字符串"""
        if any(k in raw for k in ("买入", "买", "B", "buy")):
            return "buy"
        if any(k in raw for k in ("卖出", "卖", "S", "sell")):
            return "sell"
        return ""
