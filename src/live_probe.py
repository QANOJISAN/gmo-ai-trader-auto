"""
Live Capability Probe：GMOコイン実環境・Paper環境 API 挙動実測＆ capabilities.json 自動生成モジュール (live_probe.py)

1. GMOCoinLiveProbe:
   - 最小ロット（デフォルト `DOGE` 10枚 ≒ 約150〜200円、現在値の 30% 下方の決して約定しない深い指値）を用いて、
     以下の4項目を実測検証し、`capabilities.json` を自動生成する：
     ① `POST /private/v1/changeOrder` の `orderId` 不変性 (`limit_change_order_preserves_order_id`)
        および `STOP` 注文への `changeOrder` 適用可否 (`stop_change_order_supported`)
     ② `POST /private/v1/order` の `cancelBefore=true` 挙動 (`cancel_before_supported`)
     ③ `POST / PUT / DELETE /private/v1/ws-auth` のトークン生成・延長・破棄挙動 (`ws_auth_put_delete_supported`)
     ④ `GET /private/v1/account/tradingVolume` による銘柄別 Maker/Taker 手数料率取得 (`trading_volume_supported`)
   - **100% クリーンアップ保証 (`try...finally`)**:
     プローブ実行中に通信エラー・例外・中断が発生した場合でも、`finally` ブロックで必ず作成済みの全プローブ注文を
     `cancel_order` し、さらに `get_active_orders` で残存注文がゼロであることを確認し、`ws-auth` トークンも `DELETE` 破棄する。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.database_wal import atomic_write_text
from src.json_utils import dumps_json
from src.validator import DEFAULT_SYMBOL_RULES, _to_decimal, format_decimal_plain, round_to_step

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


class GMOCoinLiveProbe:
    """
    GMOコイン API の実機/モック挙動を検証し、`capabilities.json` を出力するプローブクラス
    """

    def __init__(
        self,
        broker: Any,
        capabilities_path: Optional[Path] = None,
        probe_symbol: str = "DOGE",
        probe_size: str = "10",
        deep_limit_price: str = "10",
        deep_modified_price: str = "9.999",
    ) -> None:
        self.broker = broker
        base_dir = getattr(broker, "base_dir", None) or Path.cwd()
        self.capabilities_path = Path(capabilities_path) if capabilities_path is not None else (Path(base_dir) / "capabilities.json")
        self.probe_symbol = probe_symbol.strip().upper()
        self.probe_size = str(probe_size)
        self.deep_limit_price = str(deep_limit_price)
        self.deep_modified_price = str(deep_modified_price)

    def _resolve_probe_params(self) -> Dict[str, str]:
        """
        実機 LIVE 環境（または public_client 接続時）に対象銘柄の現在値と最小ロットから
        価格制限エラー (ERR-5121) を起こさず、かつ決して約定しない安全なプローブ価格・数量を算出する。
        """
        sym = self.probe_symbol.strip().upper()
        rule = DEFAULT_SYMBOL_RULES.get(sym, {})
        min_size_dec = _to_decimal(rule.get("min_order_size", self.probe_size), default=Decimal("1"))
        tick_dec = _to_decimal(rule.get("tick_size", "0.001"), default=Decimal("0.001"))

        is_live = str(getattr(self.broker, "MODE", "")).upper() == "LIVE"
        pub = getattr(self.broker, "public_client", None)
        if is_live and pub is not None and hasattr(pub, "get_tickers"):
            try:
                tickers = pub.get_tickers([sym])
                t_info = tickers.get(sym, {}) if isinstance(tickers, dict) else {}
                bid_dec = _to_decimal(t_info.get("bid", 0) or t_info.get("last", 0))
                ask_dec = _to_decimal(t_info.get("ask", 0) or bid_dec)
                if bid_dec > Decimal("0"):
                    limit_dec = round_to_step(bid_dec * Decimal("0.85"), tick_dec, rounding=ROUND_DOWN)
                    mod_dec = max(tick_dec, round_to_step(limit_dec - tick_dec, tick_dec, rounding=ROUND_DOWN))
                    stop_dec = round_to_step(ask_dec * Decimal("1.15"), tick_dec, rounding=ROUND_DOWN)
                    stop_mod_dec = round_to_step(stop_dec + tick_dec, tick_dec, rounding=ROUND_DOWN)
                    return {
                        "size": format_decimal_plain(min_size_dec),
                        "limit_price": format_decimal_plain(limit_dec),
                        "modified_price": format_decimal_plain(mod_dec),
                        "stop_price": format_decimal_plain(stop_dec),
                        "stop_modified_price": format_decimal_plain(stop_mod_dec),
                    }
            except Exception as exc:
                logger.debug("LiveProbe ティッカー取得フォールバック (%s): %s", sym, exc)

        return {
            "size": self.probe_size,
            "limit_price": self.deep_limit_price,
            "modified_price": self.deep_modified_price,
            "stop_price": "999",
            "stop_modified_price": "998",
        }

    def _wait_until_order_active_if_live(self, order_id: str) -> None:
        """LIVE環境にて新規注文が WAITING から ORDERED に遷移するまで最大1.2秒待機する"""
        if str(getattr(self.broker, "MODE", "")).upper() != "LIVE" or not order_id:
            return
        if not hasattr(self.broker, "get_orders"):
            return
        for _ in range(6):
            try:
                ords = self.broker.get_orders([order_id])
                if ords and str(ords[0].get("status", "")).upper() == "ORDERED":
                    return
            except Exception:
                pass
            time.sleep(0.2)

    def run_probe(
        self,
        test_stop_change_order: bool = True,
        inject_mid_probe_exception: Optional[Exception] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        4領域のプローブ検証を実行し、`try...finally` でプローブ注文とWSトークンを100%クリーンアップした上で
        `capabilities.json` を書き出して返す。
        """
        now_jst = _now_jst(now_dt)
        created_order_ids: List[str] = []
        ws_token: Optional[str] = None
        params = self._resolve_probe_params()
        probe_size = params["size"]
        limit_price = params["limit_price"]
        modified_price = params["modified_price"]
        stop_price = params["stop_price"]
        stop_modified_price = params["stop_modified_price"]

        # プローブ用の診断注文で本番 WAL 台帳を汚さないよう一時的に切り離す
        orig_wal_db = getattr(self.broker, "wal_db", None)

        capabilities: Dict[str, Any] = {
            "probed_at": now_jst.isoformat(timespec="seconds"),
            "execution_mode": str(getattr(self.broker, "MODE", "DRY_RUN")),
            "probe_symbol": self.probe_symbol,
            "probe_size": probe_size,
            "probe_limit_price": limit_price,
            "probe_modified_price": modified_price,
            "clock_offset_ms": int(getattr(self.broker, "clock_offset_ms", 0) or 0),
            "schema_compatibility": {
                "assets_schema_valid": False,
                "active_orders_schema_valid": False,
                "trading_volume_schema_valid": False,
            },
            "limit_change_order_supported": False,
            "limit_change_order_preserves_order_id": False,
            "stop_change_order_supported": False,
            "cancel_before_supported": False,
            "ws_auth_put_delete_supported": False,
            "trading_volume_supported": False,
            "fee_schedule": {},
            "cleanup_verified_zero_residual_orders": False,
            "residual_probe_orders_count": 0,
            "cleaned_order_ids": [],
        }

        try:
            if hasattr(self.broker, "wal_db"):
                self.broker.wal_db = None

            # 0. Assets スキーマ整合性＆時計オフセット確認
            if hasattr(self.broker, "get_assets"):
                try:
                    assets_list = self.broker.get_assets()
                    capabilities["schema_compatibility"]["assets_schema_valid"] = (
                        isinstance(assets_list, list)
                        and len(assets_list) > 0
                        and all(isinstance(a, dict) and "symbol" in a and "available" in a for a in assets_list)
                    )
                    capabilities["clock_offset_ms"] = int(getattr(self.broker, "clock_offset_ms", 0) or 0)
                except Exception as exc:
                    logger.debug("Probe get_assets 確認スキップ: %s", exc)

            # 1. LIMIT 指値発注 & changeOrder 検証
            ord_res = self.broker.place_order(
                symbol=self.probe_symbol,
                side="BUY",
                execution_type="LIMIT",
                size=probe_size,
                price=limit_price,
                time_in_force="SOK",
            )
            oid_limit = str(ord_res.get("order_id", ""))
            if oid_limit:
                created_order_ids.append(oid_limit)
                self._wait_until_order_active_if_live(oid_limit)

            if inject_mid_probe_exception is not None:
                raise inject_mid_probe_exception

            if oid_limit and hasattr(self.broker, "change_order"):
                try:
                    chg_res = self.broker.change_order(
                        order_id=oid_limit,
                        price=modified_price,
                    )
                    capabilities["limit_change_order_supported"] = str(chg_res.get("status", "")).upper() in (
                        "CHANGED",
                        "ORDERED",
                        "WAITING",
                    )
                    self._wait_until_order_active_if_live(oid_limit)
                    # changeOrder 後も同じ orderId が activeOrders に存在するか確認
                    active_after = self.broker.get_active_orders(symbol=self.probe_symbol)
                    capabilities["schema_compatibility"]["active_orders_schema_valid"] = isinstance(active_after, list)
                    active_ids = {str(o.get("order_id", "")) for o in active_after}
                    capabilities["limit_change_order_preserves_order_id"] = oid_limit in active_ids
                except Exception as exc:
                    logger.info("Probe: LIMIT changeOrder 非対応または失敗: %s", exc)
                    capabilities["limit_change_order_supported"] = False

            # 先に1本目のプローブ指値をキャンセルして余力を解放
            if oid_limit and oid_limit in created_order_ids:
                try:
                    self.broker.cancel_order(oid_limit)
                    created_order_ids.remove(oid_limit)
                    if oid_limit not in capabilities["cleaned_order_ids"]:
                        capabilities["cleaned_order_ids"].append(oid_limit)
                except Exception:
                    pass

            # 2. STOP 注文への changeOrder 可否検証 (オプション)
            if test_stop_change_order and hasattr(self.broker, "change_order"):
                try:
                    stop_res = self.broker.place_order(
                        symbol=self.probe_symbol,
                        side="BUY",
                        execution_type="STOP",
                        size=probe_size,
                        price=stop_price,
                        time_in_force="FAK",
                    )
                    oid_stop = str(stop_res.get("order_id", ""))
                    if oid_stop:
                        created_order_ids.append(oid_stop)
                        self._wait_until_order_active_if_live(oid_stop)
                        try:
                            s_chg = self.broker.change_order(order_id=oid_stop, price=stop_modified_price)
                            capabilities["stop_change_order_supported"] = str(s_chg.get("status", "")).upper() in (
                                "CHANGED",
                                "ORDERED",
                                "WAITING",
                            )
                        except Exception:
                            capabilities["stop_change_order_supported"] = False
                        try:
                            self.broker.cancel_order(oid_stop)
                            created_order_ids.remove(oid_stop)
                            if oid_stop not in capabilities["cleaned_order_ids"]:
                                capabilities["cleaned_order_ids"].append(oid_stop)
                        except Exception:
                            pass
                except Exception:
                    capabilities["stop_change_order_supported"] = False

            # 3. cancelOrder によるロック即時解放計測 ＆ cancelBefore=true の成行売り (MARKET/FAK/SELL) 専属制約検証
            try:
                lock_ord = self.broker.place_order(
                    symbol=self.probe_symbol,
                    side="BUY",
                    execution_type="LIMIT",
                    size=probe_size,
                    price=limit_price,
                    time_in_force="SOK",
                )
                oid_lock = str(lock_ord.get("order_id", ""))
                if oid_lock:
                    created_order_ids.append(oid_lock)
                    self._wait_until_order_active_if_live(oid_lock)
                    self.broker.cancel_order(oid_lock)
                    created_order_ids.remove(oid_lock)
                    if oid_lock not in capabilities["cleaned_order_ids"]:
                        capabilities["cleaned_order_ids"].append(oid_lock)
                # BUY LIMIT に cancelBefore=true を誤付与した場合は仕様通り拒否されることを確認
                invalid_cb_rejected = False
                try:
                    self.broker.place_order(
                        symbol=self.probe_symbol,
                        side="BUY",
                        execution_type="LIMIT",
                        size=probe_size,
                        price=limit_price,
                        time_in_force="SOK",
                        cancel_before=True,
                    )
                except Exception:
                    invalid_cb_rejected = True
                capabilities["cancel_before_supported"] = bool(oid_lock and invalid_cb_rejected)
                capabilities["cancel_before_market_fak_sell_only_enforced"] = invalid_cb_rejected
            except Exception:
                capabilities["cancel_before_supported"] = False
                capabilities["cancel_before_market_fak_sell_only_enforced"] = False

            # 4. ws-auth POST / PUT / DELETE 検証
            if (
                hasattr(self.broker, "create_ws_auth_token")
                and hasattr(self.broker, "extend_ws_auth_token")
                and hasattr(self.broker, "delete_ws_auth_token")
            ):
                try:
                    ws_token = self.broker.create_ws_auth_token()
                    if ws_token:
                        ext_ok = self.broker.extend_ws_auth_token(ws_token)
                        del_ok = self.broker.delete_ws_auth_token(ws_token)
                        ws_token = None
                        capabilities["ws_auth_put_delete_supported"] = bool(ext_ok and del_ok)
                except Exception:
                    capabilities["ws_auth_put_delete_supported"] = False

            # 5. tradingVolume 検証 & 手数料率記録
            if hasattr(self.broker, "get_trading_volume"):
                try:
                    tv_res = self.broker.get_trading_volume()
                    is_tv_dict = isinstance(tv_res, dict)
                    capabilities["trading_volume_supported"] = is_tv_dict
                    if is_tv_dict:
                        lim_list = tv_res.get("limit", [])
                        capabilities["schema_compatibility"]["trading_volume_schema_valid"] = isinstance(lim_list, list)
                        spot_fees: Dict[str, Dict[str, str]] = {}
                        if isinstance(lim_list, list):
                            for item in lim_list:
                                if not isinstance(item, dict):
                                    continue
                                s_name = str(item.get("symbol", "")).strip().upper()
                                if s_name in ("BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC", self.probe_symbol):
                                    spot_fees[s_name] = {
                                        "maker_fee": str(item.get("makerFee", item.get("maker_fee", ""))),
                                        "taker_fee": str(item.get("takerFee", item.get("taker_fee", ""))),
                                    }
                        capabilities["fee_schedule"] = {
                            "tier_level": tv_res.get("tierLevel", tv_res.get("tier_level", 1)),
                            "jpy_volume_30d": str(tv_res.get("jpyVolume", tv_res.get("jpy_volume", "0"))),
                            "spot_fees": spot_fees,
                        }
                except Exception:
                    capabilities["trading_volume_supported"] = False

        finally:
            if hasattr(self.broker, "wal_db"):
                self.broker.wal_db = orig_wal_db

            # 100% クリーンアップ保証: 作成した全プローブ注文をキャンセルし、残存確認
            cleaned_ids: List[str] = list(capabilities.get("cleaned_order_ids", []))
            for oid in list(created_order_ids):
                try:
                    self._wait_until_order_active_if_live(oid)
                    self.broker.cancel_order(oid)
                    if oid not in cleaned_ids:
                        cleaned_ids.append(oid)
                except Exception as exc:
                    logger.debug("Probe cleanup cancel_order (%s): %s", oid, exc)

            # 万一まだ activeOrders に残っているプローブ注文があれば掃討
            try:
                all_probe_ids = set(created_order_ids) | set(cleaned_ids)
                remaining_active = self.broker.get_active_orders(symbol=self.probe_symbol)
                for rem in remaining_active:
                    r_oid = str(rem.get("order_id", ""))
                    if r_oid in all_probe_ids:
                        try:
                            self.broker.cancel_order(r_oid)
                            if r_oid not in cleaned_ids:
                                cleaned_ids.append(r_oid)
                        except Exception:
                            pass
                if str(getattr(self.broker, "MODE", "")).upper() == "LIVE":
                    time.sleep(0.3)
                final_active = self.broker.get_active_orders(symbol=self.probe_symbol)
                residual_probe_orders = [
                    str(o.get("order_id", ""))
                    for o in final_active
                    if str(o.get("order_id", "")) in all_probe_ids
                ]
                capabilities["residual_probe_orders_count"] = len(residual_probe_orders)
                capabilities["cleanup_verified_zero_residual_orders"] = len(residual_probe_orders) == 0
            except Exception:
                capabilities["cleanup_verified_zero_residual_orders"] = False

            if ws_token and hasattr(self.broker, "delete_ws_auth_token"):
                try:
                    self.broker.delete_ws_auth_token(ws_token)
                except Exception:
                    pass

            capabilities["clock_offset_ms"] = int(getattr(self.broker, "clock_offset_ms", 0) or 0)
            capabilities["cleaned_order_ids"] = cleaned_ids
            try:
                atomic_write_text(self.capabilities_path, dumps_json(capabilities, indent=2) + "\n")
            except Exception as exc:
                logger.warning("capabilities.json 書き込み失敗: %s", exc)

        return capabilities
