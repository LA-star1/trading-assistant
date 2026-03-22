"""
方案C：交割单 Excel/CSV 导入器（保底方案，永远可用）

支持东方财富导出的交割单格式。
"""
import logging
from datetime import datetime, date
from io import BytesIO
from typing import Optional

import pandas as pd

from .base import BaseSyncer, BrokerPosition, BrokerTrade, BrokerBalance

logger = logging.getLogger(__name__)

# 字段名模糊匹配映射
COLUMN_MAPPINGS = {
    "trade_date":  ["成交日期", "交收日期", "日期", "发生日期", "date"],
    "trade_time":  ["成交时间", "时间", "time"],
    "stock_code":  ["证券代码", "股票代码", "代码", "code"],
    "stock_name":  ["证券名称", "股票名称", "名称", "name"],
    "direction":   ["买卖方向", "操作", "方向", "摘要", "买卖标志"],
    "price":       ["成交价格", "成交均价", "价格", "price"],
    "shares":      ["成交数量", "数量", "volume", "成交量"],
    "amount":      ["成交金额", "金额", "amount", "发生金额"],
    "commission":  ["佣金", "手续费", "commission", "交易费用"],
}

# 非买卖记录关键词（忽略这些行）
NON_TRADE_KEYWORDS = ["股息", "红利", "分红", "申购", "配股", "转债", "利息", "融资"]


class ExcelImporter(BaseSyncer):
    """从交割单文件导入数据"""

    def __init__(self, file_path_or_bytes):
        """
        file_path_or_bytes: 文件路径（str）或文件二进制内容（bytes/BytesIO）
        """
        self._source = file_path_or_bytes
        self._trades: list[BrokerTrade] = []
        self._positions: list[BrokerPosition] = []

    # ── BaseSyncer 接口实现 ────────────────────────────────────────────────

    def connect(self) -> bool:
        """解析文件"""
        try:
            self._raw_df = self._read_file()
            return self._raw_df is not None and not self._raw_df.empty
        except Exception as e:
            logger.error("文件解析失败：%s", e)
            return False

    def get_positions(self) -> list[BrokerPosition]:
        """根据成交记录推算当前持仓"""
        return self._calc_positions_from_trades()

    def get_today_trades(self) -> list[BrokerTrade]:
        today = date.today().strftime("%Y-%m-%d")
        return [t for t in self._trades if t.trade_date == today]

    def get_history_trades(self, start_date: str, end_date: str) -> list[BrokerTrade]:
        return [
            t for t in self._trades
            if start_date <= t.trade_date <= end_date
        ]

    def get_balance(self) -> Optional[BrokerBalance]:
        return None  # Excel 导入无法获取资金信息

    def disconnect(self):
        pass

    # ── 核心解析逻辑 ──────────────────────────────────────────────────────

    def _read_file(self) -> pd.DataFrame:
        """读取文件，自动检测编码和表头"""
        src = self._source

        # 读取原始数据
        if isinstance(src, (bytes, BytesIO)):
            bio = BytesIO(src) if isinstance(src, bytes) else src
            try:
                df = pd.read_excel(bio, header=None)
            except Exception:
                bio.seek(0)
                try:
                    df = pd.read_csv(bio, encoding="gbk", header=None)
                except Exception:
                    bio.seek(0)
                    df = pd.read_csv(bio, encoding="utf-8", header=None)
        else:
            fname = str(src).lower()
            if fname.endswith((".xlsx", ".xls")):
                df = pd.read_excel(src, header=None)
            else:
                try:
                    df = pd.read_csv(src, encoding="gbk", header=None)
                except UnicodeDecodeError:
                    df = pd.read_csv(src, encoding="utf-8", header=None)

        # 自动检测表头行（找第一行包含"日期"或"代码"的行）
        header_row = 0
        for i, row in df.iterrows():
            row_str = " ".join(str(v) for v in row.values)
            if any(k in row_str for k in ["日期", "代码", "证券"]):
                header_row = i
                break

        df.columns = df.iloc[header_row].astype(str).str.strip()
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        df = df.dropna(how="all")

        logger.info("文件解析完成，原始行数：%d，列：%s", len(df), df.columns.tolist()[:8])
        self._trades = self._parse_trades(df)
        return df

    def _find_col(self, columns: list[str], field: str) -> Optional[str]:
        """模糊匹配列名"""
        candidates = COLUMN_MAPPINGS.get(field, [])
        for c in candidates:
            if c in columns:
                return c
            # 部分匹配
            for col in columns:
                if c in col or col in c:
                    return col
        return None

    def _parse_trades(self, df: pd.DataFrame) -> list[BrokerTrade]:
        """解析 DataFrame 为 BrokerTrade 列表"""
        cols = df.columns.tolist()
        cm = {f: self._find_col(cols, f) for f in COLUMN_MAPPINGS}

        trades = []
        for _, row in df.iterrows():
            # 过滤非买卖记录
            raw_dir = str(row.get(cm["direction"], "") if cm["direction"] else "")
            if any(k in raw_dir for k in NON_TRADE_KEYWORDS):
                continue
            direction = self.parse_direction(raw_dir)
            if not direction:
                continue

            try:
                # 日期
                raw_date = str(row.get(cm["trade_date"], "") if cm["trade_date"] else "")
                trade_date = self._parse_date(raw_date)
                if not trade_date:
                    continue

                stock_code = self.clean_code(str(row.get(cm["stock_code"], "") if cm["stock_code"] else ""))
                if len(stock_code) != 6 or not stock_code.isdigit():
                    continue

                shares = int(float(str(row.get(cm["shares"], 0) if cm["shares"] else 0).replace(",", "") or 0))
                if shares <= 0:
                    continue

                price  = float(str(row.get(cm["price"], 0) if cm["price"] else 0).replace(",", "") or 0)
                amount = float(str(row.get(cm["amount"], 0) if cm["amount"] else 0).replace(",", "") or 0)
                if amount == 0:
                    amount = price * shares
                commission = float(str(row.get(cm["commission"], 0) if cm["commission"] else 0).replace(",", "") or 0)

                # broker_order_id：用组合键构建唯一标识
                oid = f"{trade_date}_{stock_code}_{direction}_{price}_{shares}"

                trades.append(BrokerTrade(
                    trade_date=trade_date,
                    trade_time=str(row.get(cm["trade_time"], "15:00:00") if cm["trade_time"] else "15:00:00"),
                    stock_code=stock_code,
                    stock_name=str(row.get(cm["stock_name"], "") if cm["stock_name"] else "").strip(),
                    direction=direction,
                    price=price,
                    shares=shares,
                    amount=abs(amount),
                    commission=abs(commission),
                    broker_order_id=oid,
                ))
            except Exception as e:
                logger.debug("跳过一行（解析错误）：%s", e)

        logger.info("解析成交记录：%d 条", len(trades))
        return trades

    def _parse_date(self, raw: str) -> Optional[str]:
        """将各种日期格式统一转为 YYYY-MM-DD"""
        raw = str(raw).strip().replace("/", "-")
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%m-%d-%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(raw[:10], fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _calc_positions_from_trades(self) -> list[BrokerPosition]:
        """根据历史成交推算当前持仓（FIFO 成本法）"""
        from collections import defaultdict

        holdings: dict[str, dict] = defaultdict(lambda: {
            "shares": 0, "cost_total": 0.0, "name": ""
        })

        for t in sorted(self._trades, key=lambda x: (x.trade_date, x.trade_time)):
            h = holdings[t.stock_code]
            h["name"] = t.stock_name or h["name"]
            if t.direction == "buy":
                h["cost_total"] += t.price * t.shares
                h["shares"]     += t.shares
            else:
                if h["shares"] > 0:
                    ratio = min(t.shares / h["shares"], 1.0)
                    h["cost_total"] -= h["cost_total"] * ratio
                    h["shares"] = max(0, h["shares"] - t.shares)

        positions = []
        for code, h in holdings.items():
            if h["shares"] > 0:
                cost_price = h["cost_total"] / h["shares"] if h["shares"] > 0 else 0
                positions.append(BrokerPosition(
                    stock_code=code,
                    stock_name=h["name"],
                    shares=h["shares"],
                    available_shares=h["shares"],
                    cost_price=round(cost_price, 3),
                    current_price=cost_price,   # 无实时价，用成本价占位
                    market_value=cost_price * h["shares"],
                    pnl=0.0,
                    pnl_percent=0.0,
                    broker_position_id=f"excel_{code}",
                ))
        logger.info("推算持仓：%d 只", len(positions))
        return positions

    def parse_result(self) -> dict:
        """返回完整解析结果摘要"""
        return {
            "trades_count": len(self._trades),
            "positions_count": len(self._calc_positions_from_trades()),
            "date_range": (
                min((t.trade_date for t in self._trades), default=""),
                max((t.trade_date for t in self._trades), default=""),
            ),
        }
