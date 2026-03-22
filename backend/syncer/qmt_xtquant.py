"""
方案B：QMT xtquant API 同步器（预留框架）

当用户开通支持 QMT 的券商账户后（如国金证券）启用此同步器。
当前返回 NotImplementedError，等用户开通 QMT 后实现。

前置条件：
    1. 安装 QMT 客户端并登录（需后台运行）
    2. pip install xtquant
    3. 在 sync_config 中配置 qmt_path 和 qmt_account
"""
from typing import Optional
from .base import BaseSyncer, BrokerPosition, BrokerTrade, BrokerBalance


class QMTSyncer(BaseSyncer):
    """QMT xtquant 同步器（预留，暂未实现）"""

    def __init__(self, qmt_path: str, account: str, account_type: str = "STOCK"):
        self._qmt_path     = qmt_path
        self._account      = account
        self._account_type = account_type
        self._trader       = None

    def connect(self) -> bool:
        raise NotImplementedError(
            "QMT 同步器尚未实现。请先开通支持 QMT 的券商账户（如国金证券），"
            "安装 QMT 客户端后，在此处实现 xtquant 接口调用。\n"
            "参考文档：https://dict.thinktrader.net/nativeApi/"
        )

    def get_positions(self) -> list[BrokerPosition]:
        raise NotImplementedError("QMT 同步器尚未实现")

    def get_today_trades(self) -> list[BrokerTrade]:
        raise NotImplementedError("QMT 同步器尚未实现")

    def get_history_trades(self, start_date: str, end_date: str) -> list[BrokerTrade]:
        raise NotImplementedError("QMT 同步器尚未实现")

    def get_balance(self) -> Optional[BrokerBalance]:
        raise NotImplementedError("QMT 同步器尚未实现")

    def disconnect(self):
        if self._trader:
            try:
                self._trader.stop()
            except Exception:
                pass

    # ── 待实现的参考代码框架 ──────────────────────────────────────────────
    # （等用户开通 QMT 后参考此框架实现）
    #
    # def connect(self) -> bool:
    #     import random
    #     from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
    #     from xtquant.xttype import StockAccount
    #
    #     session_id = random.randint(100000, 999999)
    #     self._trader = XtQuantTrader(self._qmt_path, session_id)
    #     self._stock_account = StockAccount(self._account, self._account_type)
    #
    #     callback = XtQuantTraderCallback()
    #     self._trader.register_callback(callback)
    #     self._trader.start()
    #
    #     connect_result = self._trader.connect()
    #     if connect_result != 0:
    #         return False
    #
    #     subscribe_result = self._trader.subscribe(self._stock_account)
    #     return subscribe_result == 0
    #
    # def get_positions(self) -> list[BrokerPosition]:
    #     positions = self._trader.query_stock_positions(self._stock_account)
    #     result = []
    #     for p in (positions or []):
    #         result.append(BrokerPosition(
    #             stock_code=self.clean_code(p.stock_code),
    #             stock_name=p.stock_name,
    #             shares=p.volume,
    #             available_shares=p.can_use_volume,
    #             cost_price=p.open_price,
    #             current_price=p.market_value / p.volume if p.volume else 0,
    #             market_value=p.market_value,
    #             pnl=p.profit,
    #             pnl_percent=p.profit_rate * 100,
    #             broker_position_id=f"qmt_{p.stock_code}",
    #         ))
    #     return result
