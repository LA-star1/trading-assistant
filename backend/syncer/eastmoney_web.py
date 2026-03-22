"""
方案A：东方财富 Web 交易接口同步器（MVP 默认）

通过模拟登录东财 Web 交易页面获取持仓和成交数据（只读查询）。
密码使用 Fernet 对称加密存储，密钥存本地 .env 文件。

依赖：pip install requests cryptography
验证码识别（可选）：pip install ddddocr
"""
import base64
import hashlib
import json
import logging
import os
import time
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    from cryptography.fernet import Fernet
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False

from .base import BaseSyncer, BrokerPosition, BrokerTrade, BrokerBalance

# 东财 Web 交易基础 URL
_BASE_URL = "https://jywg.18.cn"


class EastMoneyWebSyncer(BaseSyncer):
    """
    东方财富 Web 交易接口同步器。

    注意：这是非官方接口，只做只读查询，不进行下单/撤单操作。
    接口可能随东财 Web 页面变更而失效，失败时自动降级到手动模式。
    """

    def __init__(self, account: str, encrypted_password: str, fernet_key: Optional[str] = None):
        self._account   = account
        self._enc_pwd   = encrypted_password
        self._key       = fernet_key or os.environ.get("EM_FERNET_KEY", "")
        self._session   = None
        self._connected = False

    # ── 密码加密工具（静态方法，供配置页面调用）───────────────────────────
    @staticmethod
    def generate_fernet_key() -> str:
        """生成新的 Fernet 密钥（首次配置时调用）"""
        if not _HAS_CRYPTO:
            raise ImportError("请安装 cryptography：pip install cryptography")
        return Fernet.generate_key().decode()

    @staticmethod
    def encrypt_password(password: str, fernet_key: str) -> str:
        """加密交易密码"""
        if not _HAS_CRYPTO:
            raise ImportError("请安装 cryptography：pip install cryptography")
        f = Fernet(fernet_key.encode())
        return f.encrypt(password.encode()).decode()

    def _decrypt_password(self) -> str:
        if not _HAS_CRYPTO or not self._key:
            raise RuntimeError("未配置加密密钥，无法解密密码")
        f = Fernet(self._key.encode())
        return f.decrypt(self._enc_pwd.encode()).decode()

    # ── 连接 / 登录 ────────────────────────────────────────────────────────

    def connect(self) -> bool:
        if not _HAS_REQUESTS:
            logger.error("请安装 requests：pip install requests")
            return False

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": _BASE_URL,
        })

        try:
            password = self._decrypt_password()
        except Exception as e:
            logger.error("密码解密失败：%s", e)
            return False

        # 获取验证码
        yzm_text = self._get_captcha()
        if not yzm_text:
            logger.warning("验证码识别失败，尝试空值（某些环境下不需要）")
            yzm_text = ""

        # 登录
        try:
            resp = self._session.post(
                f"{_BASE_URL}/Login/Authentication",
                data={
                    "userId":       self._account,
                    "password":     self._rsa_encrypt(password),
                    "identifyCode": yzm_text,
                    "randNumber":   self._get_rand_number(),
                },
                timeout=15,
            )
            result = resp.json()
            if result.get("Status") == 0:
                self._connected = True
                logger.info("东财Web登录成功，账号：%s", self._account[:4] + "****")
                return True
            else:
                logger.error("东财登录失败：%s", result.get("Message", "未知错误"))
                return False
        except Exception as e:
            logger.error("东财登录请求失败：%s", e)
            return False

    def _get_captcha(self) -> str:
        """获取并识别验证码"""
        try:
            resp = self._session.get(f"{_BASE_URL}/Login/YZM", timeout=10)
            img_bytes = resp.content

            # 尝试用 ddddocr 识别
            try:
                import ddddocr
                ocr = ddddocr.DdddOcr(show_ad=False)
                return ocr.classification(img_bytes)
            except ImportError:
                logger.warning("ddddocr 未安装，验证码自动识别不可用。"
                               "可手动在 sync_config 中粘贴 session token 绕过登录。")
                return ""
        except Exception as e:
            logger.warning("验证码获取失败：%s", e)
            return ""

    def _rsa_encrypt(self, password: str) -> str:
        """东财密码 RSA 加密（使用公钥加密）"""
        try:
            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_v1_5
            import binascii

            # 东财使用的 RSA 公钥（截至2024年，可能变化）
            PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDHY0eeGzPKJiSlNy7JY2kZsVEY
KkrlXSZPMCKblh6mGxO8QEqLSfqGbVfOIkHvMxkLfhBbA5Bx0OzRVEkV7/I+v5r
1IxaGKQ/zxQr8IiJzKopKnZ1oUGiHKZABEHJECBtOVm8cQxGQEtOl8Q50Xk0rZ3
fCPiDXgzxBxhWoA+1QIDAQAB
-----END PUBLIC KEY-----"""
            key = RSA.import_key(PUBLIC_KEY)
            cipher = PKCS1_v1_5.new(key)
            encrypted = cipher.encrypt(password.encode())
            return base64.b64encode(encrypted).decode()
        except ImportError:
            logger.warning("pycryptodome 未安装，密码将以明文传输（安全性降低）")
            return password
        except Exception as e:
            logger.warning("RSA 加密失败：%s，使用明文", e)
            return password

    def _get_rand_number(self) -> str:
        """获取登录所需的随机数"""
        try:
            resp = self._session.get(f"{_BASE_URL}/Login/GetRandNumber", timeout=10)
            return resp.json().get("Message", "")
        except Exception:
            return ""

    # ── 数据查询 ───────────────────────────────────────────────────────────

    def get_positions(self) -> list[BrokerPosition]:
        if not self._connected:
            return []
        try:
            resp = self._session.get(
                f"{_BASE_URL}/Com/queryAssetAndPositionV1",
                timeout=15,
            )
            data = resp.json()
            logger.debug("持仓查询原始响应：%s", str(data)[:200])

            raw_positions = data.get("Data", {}).get("StockList", []) or []
            result = []
            for p in raw_positions:
                try:
                    code = self.clean_code(str(p.get("Gpdm", "") or p.get("证券代码", "")))
                    result.append(BrokerPosition(
                        stock_code=code,
                        stock_name=str(p.get("Gpmc", "") or p.get("证券名称", "")),
                        shares=int(p.get("Zqsl", 0) or p.get("证券数量", 0) or 0),
                        available_shares=int(p.get("Kysl", 0) or p.get("可用数量", 0) or 0),
                        cost_price=float(p.get("Cbjg", 0) or p.get("成本价", 0) or 0),
                        current_price=float(p.get("Zxjg", 0) or p.get("最新价", 0) or 0),
                        market_value=float(p.get("Zxsz", 0) or p.get("市值", 0) or 0),
                        pnl=float(p.get("Ljyk", 0) or p.get("盈亏", 0) or 0),
                        pnl_percent=float(p.get("Ykbl", 0) or p.get("盈亏比例", 0) or 0),
                        broker_position_id=f"em_{code}",
                    ))
                except Exception as e:
                    logger.warning("解析持仓记录失败：%s", e)
            logger.info("东财持仓查询：%d 只", len(result))
            return result
        except Exception as e:
            logger.error("持仓查询失败：%s", e)
            return []

    def get_today_trades(self) -> list[BrokerTrade]:
        if not self._connected:
            return []
        try:
            resp = self._session.get(
                f"{_BASE_URL}/Search/GetDealData",
                timeout=15,
            )
            data = resp.json()
            return self._parse_trade_data(data.get("Data", []) or [])
        except Exception as e:
            logger.error("今日成交查询失败：%s", e)
            return []

    def get_history_trades(self, start_date: str, end_date: str) -> list[BrokerTrade]:
        if not self._connected:
            return []
        try:
            resp = self._session.get(
                f"{_BASE_URL}/Search/GetHisDealData",
                params={
                    "st": start_date.replace("-", ""),
                    "et": end_date.replace("-", ""),
                },
                timeout=15,
            )
            data = resp.json()
            return self._parse_trade_data(data.get("Data", []) or [])
        except Exception as e:
            logger.error("历史成交查询失败：%s", e)
            return []

    def _parse_trade_data(self, raw_list: list) -> list[BrokerTrade]:
        trades = []
        for t in raw_list:
            try:
                raw_dir = str(t.get("Mmbz", "") or t.get("买卖标志", ""))
                direction = self.parse_direction(raw_dir)
                if not direction:
                    continue

                code = self.clean_code(str(t.get("Gpdm", "") or t.get("证券代码", "")))
                date_str = str(t.get("Cjrq", "") or t.get("成交日期", ""))
                parsed_date = date_str[:4] + "-" + date_str[4:6] + "-" + date_str[6:8] if len(date_str) >= 8 else date_str
                oid = str(t.get("Wth", "") or t.get("委托号", "") or f"{parsed_date}_{code}_{direction}")

                trades.append(BrokerTrade(
                    trade_date=parsed_date,
                    trade_time=str(t.get("Cjsj", "15:00:00") or "15:00:00"),
                    stock_code=code,
                    stock_name=str(t.get("Gpmc", "") or t.get("证券名称", "")),
                    direction=direction,
                    price=float(t.get("Cjjg", 0) or t.get("成交价格", 0) or 0),
                    shares=int(t.get("Cjsl", 0) or t.get("成交数量", 0) or 0),
                    amount=abs(float(t.get("Cjje", 0) or t.get("成交金额", 0) or 0)),
                    commission=abs(float(t.get("Sxf", 0) or t.get("手续费", 0) or 0)),
                    broker_order_id=oid,
                ))
            except Exception as e:
                logger.debug("跳过成交记录：%s", e)
        return trades

    def get_balance(self) -> Optional[BrokerBalance]:
        if not self._connected:
            return None
        try:
            resp = self._session.get(
                f"{_BASE_URL}/Com/queryAssetAndPositionV1",
                timeout=15,
            )
            data = resp.json().get("Data", {}) or {}
            return BrokerBalance(
                total_assets=float(data.get("Zzc", 0) or 0),
                available_cash=float(data.get("Kyzj", 0) or 0),
                market_value=float(data.get("Gpsz", 0) or 0),
                frozen_cash=float(data.get("Djzj", 0) or 0),
            )
        except Exception as e:
            logger.error("资金查询失败：%s", e)
            return None

    def disconnect(self):
        if self._session:
            try:
                self._session.get(f"{_BASE_URL}/Login/LoginOut", timeout=5)
            except Exception:
                pass
            self._session = None
        self._connected = False
        logger.info("东财Web会话已关闭")

    def keepalive(self):
        """心跳请求，防止 session 过期（每10分钟调用一次）"""
        if self._connected and self._session:
            try:
                self._session.get(f"{_BASE_URL}/Trade/Position", timeout=5)
            except Exception:
                self._connected = False
                logger.warning("心跳失败，session 可能已过期")
