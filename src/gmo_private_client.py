"""
GMOコイン Private API クライアント ＆ 仮想取引所シミュレータ (gmo_private_client.py)

- `GMOPrivateClient`:
  HMAC-SHA256署名（`API-KEY`, `API-TIMESTAMP`, `API-SIGN`）による GMOコイン公式 Private API クライアント。
  * `get_assets()` (`GET /private/v1/account/assets`)
  * `place_order(...)` (`POST /private/v1/order` — Post-Only `SOK` 即時失効時の1ティック調整・再試行機能付き)
  * `get_active_orders(symbol)` (`GET /private/v1/activeOrders`)
  * `get_orders(order_ids)` (`GET /private/v1/orders`)
  * `cancel_order(order_id)` (`POST /private/v1/cancelOrder`)
  * `get_latest_executions(symbol)` (`GET /private/v1/latestExecutions`)

- `PaperBroker`:
  `EXECUTION_MODE=DRY_RUN` 時に Private API を一切呼ばず、Public API のリアルタイム Ticker (`ask`, `bid`, `last`) と照合して
  ローカルの `paper_orders.json` 上で
  * 指値刺さり（`last <= limit_price` または `ask <= limit_price`）
  * SL発動（`bid <= stop_loss_price` または `last <= stop_loss_price`）
  * TP到達（`bid >= take_profit_price` または `last >= take_profit_price`）
  * GMOコイン現物特有の「SL逆指値発注中の保有数量ロック（取消しない限り別注文を出せない仕様）」
  * Post-Only (`SOK`) 指値が `ask` 以上で発注された際の即時失効（`CANCELED`）と1ティック再調整
  を本物そっくりに再現する仮想取引所クラス。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests

from src.gmo_client import GMOPublicClient, SystemSSLAdapter
from src.json_utils import load_json_file, write_json_file
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    _to_decimal,
    format_decimal_plain,
    round_to_step,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


class GMOPrivateAPIError(RuntimeError):
    """GMOコイン Private API エラー例外"""

    def __init__(self, message: str, messages: Optional[List[Dict[str, Any]]] = None) -> None:
        super().__init__(message)
        self.messages = messages or []


class InsufficientAvailableAssetError(GMOPrivateAPIError):
    """現物保有数量または日本円余力が別注文（SL等）でロックされており発注できない場合の例外 (ERR-201相当)"""


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def _normalize_v1_path(path: str) -> str:
    """`/private/v1/order` や `v1/order` を署名用パス `/v1/order` に正規化する"""
    p = path.strip()
    if p.startswith("/private/"):
        p = p[len("/private") :]
    if not p.startswith("/"):
        p = "/" + p
    if not p.startswith("/v1/"):
        p = "/v1/" + p.lstrip("/")
    return p


class GMOPrivateClient:
    """GMOコイン 公式 Private API クライアント (LIVE モード用)"""

    BASE_URL = "https://api.coin.z.com/private"
    MODE = "LIVE"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        public_client: Optional[GMOPublicClient] = None,
        timeout: int = 10,
        max_retries: int = 2,
        retry_delay: float = 0.5,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.getenv("GMO_API_KEY", "")).strip()
        self.api_secret = (api_secret if api_secret is not None else os.getenv("GMO_API_SECRET", "")).strip()
        self.public_client = public_client or GMOPublicClient(timeout=timeout)
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.session = requests.Session()
        self.session.mount("https://", SystemSSLAdapter())
        self.session.headers.update(
            {
                "User-Agent": "gmo-ai-trader-v2/2.0",
                "Accept": "application/json",
            }
        )

    def has_credentials(self) -> bool:
        return (
            bool(self.api_key)
            and bool(self.api_secret)
            and not self.api_key.startswith("your_")
            and not self.api_secret.startswith("your_")
        )

    def _build_auth_headers(self, method: str, v1_path: str, body_str: str = "") -> Dict[str, str]:
        if not self.has_credentials():
            raise GMOPrivateAPIError("GMO_API_KEY または GMO_API_SECRET が未設定です。")
        timestamp = str(int(time.time() * 1000))
        method_up = method.upper()
        text = f"{timestamp}{method_up}{v1_path}{body_str}"
        sign = hmac.new(
            self.api_secret.encode("utf-8"),
            text.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "API-KEY": self.api_key,
            "API-TIMESTAMP": timestamp,
            "API-SIGN": sign,
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        v1_path = _normalize_v1_path(path)
        url = f"{self.BASE_URL}{v1_path}"
        method_up = method.upper()
        body_str = json.dumps(body, separators=(",", ":")) if (body is not None and method_up == "POST") else ""

        last_err: Optional[Exception] = None
        # POST (注文・キャンセル) は二重発注を防ぐため原則リトライせず1回のみ実行、GETのみリトライ許容
        attempts = self.max_retries if method_up == "GET" else 1

        for attempt in range(1, attempts + 1):
            try:
                headers = self._build_auth_headers(method_up, v1_path, body_str)
                if method_up == "GET":
                    resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                else:
                    resp = self.session.post(url, data=body_str, headers=headers, timeout=self.timeout)
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("status") != 0:
                    messages = payload.get("messages", []) or []
                    msg_str = f"GMO Private API エラー (path={v1_path}): {messages}"
                    if any(str(m.get("message_code", "")) == "ERR-201" for m in messages if isinstance(m, dict)):
                        raise InsufficientAvailableAssetError(msg_str, messages=messages)
                    raise GMOPrivateAPIError(msg_str, messages=messages)
                return payload.get("data")
            except GMOPrivateAPIError:
                raise
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "GMO Private API 通信エラー (%d/%d) %s %s: %s",
                    attempt,
                    attempts,
                    method_up,
                    v1_path,
                    exc,
                )
                if attempt < attempts:
                    time.sleep(self.retry_delay * attempt)

        raise GMOPrivateAPIError(f"GMO Private API 通信失敗 ({method_up} {v1_path}): {last_err}") from last_err

    def get_assets(self) -> List[Dict[str, Any]]:
        """口座資産残高一覧を取得する (`GET /private/v1/account/assets`)"""
        data = self._request("GET", "/private/v1/account/assets")
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("list"), list):
            return data["list"]
        return []

    def get_jpy_and_spot_balances(self) -> Dict[str, Any]:
        """日本円余力と現物各銘柄の保有数量・利用可能数量を整形して返す"""
        assets = self.get_assets()
        jpy_amount = 0.0
        jpy_available = 0.0
        spot_assets: Dict[str, Dict[str, float]] = {}

        for item in assets:
            sym = str(item.get("symbol", "")).strip().upper()
            amt = float(item.get("amount", 0) or 0)
            avail = float(item.get("available", 0) or 0)
            if sym == "JPY":
                jpy_amount = amt
                jpy_available = avail
            elif sym and not sym.endswith("_JPY") and (amt > 0 or avail > 0):
                spot_assets[sym] = {
                    "amount": amt,
                    "available": avail,
                    "locked": max(0.0, round(amt - avail, 8)),
                }

        return {
            "jpy_amount": jpy_amount,
            "jpy_available": jpy_available,
            "spot_assets": spot_assets,
        }

    def get_active_orders(
        self,
        symbol: Optional[str] = None,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """有効注文一覧を取得する (`GET /private/v1/activeOrders`)"""
        symbols_to_query = [symbol.upper()] if symbol else list(DEFAULT_SYMBOL_RULES.keys())
        results: List[Dict[str, Any]] = []
        for sym in symbols_to_query:
            data = self._request(
                "GET",
                "/private/v1/activeOrders",
                params={"symbol": sym, "page": page, "count": count},
            )
            raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in raw_list:
                results.append(self._normalize_order_dict(item))
        return results

    def get_orders(self, order_ids: Union[str, int, List[Union[str, int]]]) -> List[Dict[str, Any]]:
        """注文ID指定で注文情報を取得する (`GET /private/v1/orders`)"""
        if isinstance(order_ids, (str, int)):
            id_list = [str(order_ids)]
        else:
            id_list = [str(x) for x in order_ids if str(x).strip()]
        if not id_list:
            return []
        data = self._request(
            "GET",
            "/private/v1/orders",
            params={"orderId": ",".join(id_list[:10])},
        )
        raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return [self._normalize_order_dict(item) for item in raw_list]

    def cancel_order(self, order_id: Union[str, int]) -> Dict[str, Any]:
        """注文をキャンセルする (`POST /private/v1/cancelOrder`)"""
        oid = int(order_id) if str(order_id).isdigit() else order_id
        self._request("POST", "/private/v1/cancelOrder", body={"orderId": oid})
        return {"order_id": str(order_id), "status": "CANCELED"}

    def get_latest_executions(
        self,
        symbol: str,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """最新約定一覧を取得する (`GET /private/v1/latestExecutions`)"""
        data = self._request(
            "GET",
            "/private/v1/latestExecutions",
            params={"symbol": symbol.upper(), "page": page, "count": count},
        )
        raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        out: List[Dict[str, Any]] = []
        for item in raw_list:
            out.append(
                {
                    "execution_id": str(item.get("executionId", "")),
                    "order_id": str(item.get("orderId", "")),
                    "symbol": str(item.get("symbol", symbol)).upper(),
                    "side": str(item.get("side", "")).upper(),
                    "execution_type": str(item.get("executionType", "")).upper(),
                    "size": float(item.get("size", 0) or 0),
                    "price": float(item.get("price", 0) or 0),
                    "fee": float(item.get("fee", 0) or 0),
                    "timestamp": str(item.get("timestamp", "")),
                }
            )
        return out

    def place_order(
        self,
        symbol: str,
        side: str,
        execution_type: str,
        size: Union[str, int, float, Decimal],
        price: Optional[Union[str, int, float, Decimal]] = None,
        losscut_price: Optional[Union[str, int, float, Decimal]] = None,
        time_in_force: Optional[str] = None,
        cancel_before: bool = False,
        max_post_only_retries: int = 2,
    ) -> Dict[str, Any]:
        """
        新規注文を発注する (`POST /private/v1/order`)。
        - 買付指値: `executionType="LIMIT"`, `timeInForce="SOK"` (Post-Only)
          ※板が動いて SOK が即時キャンセルされた場合、最新 ticker の best_bid / best_ask - 1 tick へ
            1ティック調整して最大 `max_post_only_retries` 回まで再試行する。
        - 損切逆指値: `executionType="STOP"`, `timeInForce="FAK"`, `price`
        - 利確・成行決済: `executionType="LIMIT"` または `"MARKET"`
        """
        sym = symbol.strip().upper()
        side_up = side.strip().upper()
        exec_up = execution_type.strip().upper()
        rule = DEFAULT_SYMBOL_RULES.get(sym, {})
        tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
        size_step = _to_decimal(rule.get("size_step", "0.0001"), default=Decimal("0.0001"))

        size_dec = round_to_step(_to_decimal(size), size_step, rounding=ROUND_DOWN)
        size_str = format_decimal_plain(size_dec)

        if time_in_force:
            tif = time_in_force.strip().upper()
        elif exec_up == "LIMIT" and side_up == "BUY":
            tif = "SOK"
        elif exec_up in ("STOP", "MARKET"):
            tif = "FAK"
        else:
            tif = "FAS"

        price_dec = round_to_step(_to_decimal(price or 0), tick_size, rounding=ROUND_DOWN) if price is not None else Decimal("0")
        adjust_count = 0
        post_only_adjusted = False

        max_attempts = (1 + max(0, int(max_post_only_retries))) if (exec_up == "LIMIT" and tif == "SOK") else 1

        for attempt in range(max_attempts):
            body: Dict[str, Any] = {
                "symbol": sym,
                "side": side_up,
                "executionType": exec_up,
                "timeInForce": tif,
                "size": size_str,
            }
            if exec_up in ("LIMIT", "STOP"):
                body["price"] = format_decimal_plain(price_dec)
            if losscut_price is not None and _to_decimal(losscut_price) > 0:
                body["losscutPrice"] = format_decimal_plain(
                    round_to_step(_to_decimal(losscut_price), tick_size, rounding=ROUND_DOWN)
                )
            if cancel_before:
                body["cancelBefore"] = True

            raw_order_id = self._request("POST", "/private/v1/order", body=body)
            order_id = str(raw_order_id.get("orderId", raw_order_id) if isinstance(raw_order_id, dict) else raw_order_id)

            # SOK (Post-Only) の場合は即時キャンセルされていないか確認
            order_info: Dict[str, Any] = {
                "order_id": order_id,
                "symbol": sym,
                "side": side_up,
                "execution_type": exec_up,
                "time_in_force": tif,
                "price": float(price_dec) if exec_up in ("LIMIT", "STOP") else 0.0,
                "size": float(size_dec),
                "executed_size": 0.0,
                "status": "WAITING",
                "post_only_adjusted": post_only_adjusted,
                "adjust_count": adjust_count,
                "timestamp": _now_jst().isoformat(timespec="seconds"),
            }

            try:
                fetched = self.get_orders([order_id])
                if fetched:
                    order_info.update(fetched[0])
                    order_info["post_only_adjusted"] = post_only_adjusted
                    order_info["adjust_count"] = adjust_count
            except Exception as exc:
                logger.debug("発注直後の get_orders 確認スキップ (order_id=%s): %s", order_id, exc)

            if tif == "SOK" and order_info.get("status") == "CANCELED" and attempt + 1 < max_attempts:
                adjust_count += 1
                post_only_adjusted = True
                try:
                    tickers = self.public_client.get_tickers([sym])
                    t_sym = tickers.get(sym, {})
                    best_ask = _to_decimal(t_sym.get("ask", 0) or 0)
                    best_bid = _to_decimal(t_sym.get("bid", 0) or 0)
                    if best_ask > 0 and best_bid > 0:
                        price_dec = min(best_bid, best_ask - tick_size)
                    else:
                        price_dec = max(tick_size, price_dec - tick_size)
                except Exception:
                    price_dec = max(tick_size, price_dec - tick_size)
                price_dec = max(tick_size, round_to_step(price_dec, tick_size, rounding=ROUND_DOWN))
                logger.info(
                    "Post-Only (SOK) 即時失効のため1ティック調整して再発注します (%d/%d): %s price=%s",
                    adjust_count,
                    max_post_only_retries,
                    sym,
                    format_decimal_plain(price_dec),
                )
                continue

            return order_info

        return order_info

    @staticmethod
    def _normalize_order_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
        size_f = float(raw.get("size", 0) or 0)
        exec_f = float(raw.get("executedSize", raw.get("executed_size", 0)) or 0)
        return {
            "order_id": str(raw.get("orderId", raw.get("order_id", ""))),
            "symbol": str(raw.get("symbol", "")).upper(),
            "side": str(raw.get("side", "")).upper(),
            "execution_type": str(raw.get("executionType", raw.get("execution_type", ""))).upper(),
            "time_in_force": str(raw.get("timeInForce", raw.get("time_in_force", ""))).upper(),
            "price": float(raw.get("price", 0) or 0),
            "losscut_price": float(raw.get("losscutPrice", raw.get("losscut_price", 0)) or 0),
            "size": size_f,
            "executed_size": exec_f,
            "remaining_size": max(0.0, round(size_f - exec_f, 8)),
            "status": str(raw.get("status", "WAITING")).upper(),
            "cancel_type": str(raw.get("cancelType", raw.get("cancel_type", ""))),
            "timestamp": str(raw.get("timestamp", "")),
        }


class PaperBroker:
    """
    `EXECUTION_MODE=DRY_RUN` 用の仮想取引所シミュレータ。
    - Private API を一切呼ばず、`paper_orders.json` と `portfolio.json` 上で注文・残高ロック・約定を管理する。
    - Public API のリアルタイム板 (`ask`, `bid`, `last`) と照合し、
      * 買い指値 (`LIMIT` / `SOK`) の Post-Only 即時失効＆1ティック自動補正
      * 指値刺さり (`last <= limit_price` または `ask <= limit_price`) と Maker リベート手数料の反映
      * 損切り逆指値 (`STOP` / `FAK`) の発動 (`bid <= stop_loss_price` または `last <= stop_loss_price`) と Taker 手数料の反映
      * 利確売り (`LIMIT` / `MARKET`) の約定 (`bid >= take_profit_price` または成行即時約定)
      * GMOコイン現物特有の「SL注文発注中は保有数量がロックされ、cancel_order しない限り別の売り注文を出せない仕様」
      を完全に再現する。
    """

    MODE = "DRY_RUN"

    def __init__(
        self,
        paper_orders_path: Union[str, Path] = "paper_orders.json",
        portfolio_path: Union[str, Path] = "portfolio.json",
        public_client: Optional[GMOPublicClient] = None,
    ) -> None:
        self.paper_orders_path = Path(paper_orders_path)
        self.portfolio_path = Path(portfolio_path)
        self.public_client = public_client
        self._latest_tickers: Dict[str, Dict[str, Any]] = {}
        self._ensure_state_file()

    def has_credentials(self) -> bool:
        return True

    def update_market_tickers(self, tickers: Dict[str, Dict[str, Any]]) -> None:
        """最新の Public API Ticker スナップショットを更新する"""
        if tickers:
            for k, v in tickers.items():
                self._latest_tickers[str(k).upper()] = dict(v)

    def _ensure_state_file(self) -> Dict[str, Any]:
        if not self.paper_orders_path.exists():
            init_state = {
                "next_order_id": 9100000001,
                "next_execution_id": 7100000001,
                "orders": [],
                "executions": [],
            }
            write_json_file(self.paper_orders_path, init_state)
            return init_state
        try:
            data = load_json_file(self.paper_orders_path)
            if not isinstance(data, dict):
                raise ValueError("Invalid paper_orders.json root")
            data.setdefault("next_order_id", 9100000001)
            data.setdefault("next_execution_id", 7100000001)
            data.setdefault("orders", [])
            data.setdefault("executions", [])
            return data
        except Exception:
            init_state = {
                "next_order_id": 9100000001,
                "next_execution_id": 7100000001,
                "orders": [],
                "executions": [],
            }
            write_json_file(self.paper_orders_path, init_state)
            return init_state

    def _save_state(self, state: Dict[str, Any]) -> None:
        write_json_file(self.paper_orders_path, state)

    def _load_portfolio(self) -> Dict[str, Any]:
        if not self.portfolio_path.exists():
            return {
                "initial_capital_jpy": 10000,
                "jpy_balance": 10000,
                "realized_pnl_total_jpy": 0,
                "total_realized_pnl_jpy": 0,
                "positions": [],
                "closed_trades": [],
                "pending_orders": [],
            }
        return load_json_file(self.portfolio_path)

    def get_assets(self) -> List[Dict[str, Any]]:
        """`portfolio.json` と現在有効な `paper_orders.json` のロック数量から資産一覧を返す"""
        portfolio = self._load_portfolio()
        state = self._ensure_state_file()
        active_orders = [
            o for o in state.get("orders", []) if str(o.get("status", "")).upper() in ("WAITING", "ORDERED")
        ]

        jpy_balance = _to_decimal(portfolio.get("jpy_balance", 10000))
        locked_jpy = Decimal("0")
        locked_spot: Dict[str, Decimal] = {}

        for o in active_orders:
            side = str(o.get("side", "")).upper()
            sym = str(o.get("symbol", "")).upper()
            rem_size = _to_decimal(o.get("size", 0)) - _to_decimal(o.get("executed_size", 0))
            if rem_size <= 0:
                continue
            if side == "BUY":
                price_d = _to_decimal(o.get("price", 0))
                locked_jpy += price_d * rem_size
            elif side == "SELL":
                locked_spot[sym] = locked_spot.get(sym, Decimal("0")) + rem_size

        jpy_avail = max(Decimal("0"), jpy_balance - locked_jpy)
        assets: List[Dict[str, Any]] = [
            {
                "symbol": "JPY",
                "amount": str(int(jpy_balance.to_integral_value(rounding=ROUND_HALF_UP))),
                "available": str(int(jpy_avail.to_integral_value(rounding=ROUND_HALF_UP))),
                "conversionRate": "1",
            }
        ]

        spot_totals: Dict[str, Decimal] = {}
        for pos in portfolio.get("positions", []) or []:
            sym = str(pos.get("symbol", "")).upper()
            sz = _to_decimal(pos.get("size", 0))
            if sym and sz > 0:
                spot_totals[sym] = spot_totals.get(sym, Decimal("0")) + sz

        for sym, total_sz in spot_totals.items():
            lck = locked_spot.get(sym, Decimal("0"))
            avail_sz = max(Decimal("0"), total_sz - lck)
            assets.append(
                {
                    "symbol": sym,
                    "amount": format_decimal_plain(total_sz),
                    "available": format_decimal_plain(avail_sz),
                    "conversionRate": "1",
                }
            )

        return assets

    def get_jpy_and_spot_balances(self) -> Dict[str, Any]:
        assets = self.get_assets()
        jpy_amount = 0.0
        jpy_available = 0.0
        spot_assets: Dict[str, Dict[str, float]] = {}

        for item in assets:
            sym = str(item.get("symbol", "")).upper()
            amt = float(item.get("amount", 0) or 0)
            avail = float(item.get("available", 0) or 0)
            if sym == "JPY":
                jpy_amount = amt
                jpy_available = avail
            elif sym:
                spot_assets[sym] = {
                    "amount": amt,
                    "available": avail,
                    "locked": max(0.0, round(amt - avail, 8)),
                }
        return {
            "jpy_amount": jpy_amount,
            "jpy_available": jpy_available,
            "spot_assets": spot_assets,
        }

    def _get_symbol_ticker(self, symbol: str) -> Dict[str, Any]:
        sym = symbol.upper()
        if sym in self._latest_tickers:
            return self._latest_tickers[sym]
        if self.public_client is not None:
            try:
                fetched = self.public_client.get_tickers([sym])
                if sym in fetched:
                    self._latest_tickers[sym] = fetched[sym]
                    return fetched[sym]
            except Exception:
                pass
        return {}

    def place_order(
        self,
        symbol: str,
        side: str,
        execution_type: str,
        size: Union[str, int, float, Decimal],
        price: Optional[Union[str, int, float, Decimal]] = None,
        losscut_price: Optional[Union[str, int, float, Decimal]] = None,
        time_in_force: Optional[str] = None,
        cancel_before: bool = False,
        max_post_only_retries: int = 2,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        PaperBroker 上で注文を発注する。
        - SELL 時は現物保有余力 (`available`) をチェックし、既存の SL 注文等でロックされている場合は
          本物の GMOコインと同じく `InsufficientAvailableAssetError` を送出する。
        - BUY `LIMIT` (`SOK` Post-Only) 時に `price >= ask` の場合は SOK 即時キャンセルをシミュレートし、
          `max_post_only_retries > 0` なら `min(bid, ask - tick_size)` へ1ティック調整して再発注する。
        - `MARKET` 注文は発注と同時に最新レート（BUYなら `ask`、SELLなら `bid`）で即時約定させる。
        """
        now_str = _now_jst(now_dt).isoformat(timespec="seconds")
        sym = symbol.strip().upper()
        side_up = side.strip().upper()
        exec_up = execution_type.strip().upper()
        rule = DEFAULT_SYMBOL_RULES.get(sym, {})
        tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
        size_step = _to_decimal(rule.get("size_step", "0.0001"), default=Decimal("0.0001"))

        size_dec = round_to_step(_to_decimal(size), size_step, rounding=ROUND_DOWN)
        if size_dec <= 0:
            raise GMOPrivateAPIError(f"注文数量が 0 以下です: {size}")

        if time_in_force:
            tif = time_in_force.strip().upper()
        elif exec_up == "LIMIT" and side_up == "BUY":
            tif = "SOK"
        elif exec_up in ("STOP", "MARKET"):
            tif = "FAK"
        else:
            tif = "FAS"

        price_dec = (
            round_to_step(_to_decimal(price or 0), tick_size, rounding=ROUND_DOWN)
            if price is not None
            else Decimal("0")
        )

        balances = self.get_jpy_and_spot_balances()
        if side_up == "SELL":
            spot_info = balances["spot_assets"].get(sym, {"amount": 0.0, "available": 0.0, "locked": 0.0})
            avail_dec = _to_decimal(spot_info.get("available", 0.0))
            if avail_dec + Decimal("1e-12") < size_dec:
                raise InsufficientAvailableAssetError(
                    f"[ERR-201 PaperBroker] {sym} の売却可能数量が不足しています "
                    f"(要求={format_decimal_plain(size_dec)}, 利用可能={format_decimal_plain(avail_dec)}, "
                    f"SLロック中={format_decimal_plain(spot_info.get('locked', 0.0))})。"
                    "先に既存の逆指値(STOP)注文を cancel_order してください。"
                )
        elif side_up == "BUY":
            jpy_avail_dec = _to_decimal(balances.get("jpy_available", 0.0))
            est_price = price_dec if price_dec > 0 else _to_decimal(self._get_symbol_ticker(sym).get("ask", 0) or 0)
            req_jpy = est_price * size_dec
            if est_price > 0 and req_jpy > jpy_avail_dec + Decimal("1"):
                raise InsufficientAvailableAssetError(
                    f"[ERR-201 PaperBroker] 日本円余力が不足しています "
                    f"(必要額≈{int(req_jpy):,}円, 利用可能={int(jpy_avail_dec):,}円)"
                )

        state = self._ensure_state_file()
        ticker = self._get_symbol_ticker(sym)
        best_ask = _to_decimal(ticker.get("ask", 0) or 0)
        best_bid = _to_decimal(ticker.get("bid", 0) or 0)
        last_price = _to_decimal(ticker.get("last", 0) or 0)

        adjust_count = 0
        post_only_adjusted = False
        max_attempts = (1 + max(0, int(max_post_only_retries))) if (exec_up == "LIMIT" and tif == "SOK") else 1

        for attempt in range(max_attempts):
            order_id = str(state["next_order_id"])
            state["next_order_id"] = int(state["next_order_id"]) + 1

            # Post-Only (SOK) 板ぶつかりチェック
            is_sok_crossed = False
            if exec_up == "LIMIT" and tif == "SOK":
                if side_up == "BUY" and best_ask > 0 and price_dec >= best_ask:
                    is_sok_crossed = True
                elif side_up == "SELL" and best_bid > 0 and price_dec <= best_bid:
                    is_sok_crossed = True

            if is_sok_crossed:
                canceled_order = {
                    "order_id": order_id,
                    "symbol": sym,
                    "side": side_up,
                    "execution_type": exec_up,
                    "time_in_force": tif,
                    "price": float(price_dec),
                    "size": float(size_dec),
                    "executed_size": 0.0,
                    "remaining_size": 0.0,
                    "status": "CANCELED",
                    "cancel_type": "SOK_POST_ONLY_REJECTED",
                    "post_only_adjusted": post_only_adjusted,
                    "adjust_count": adjust_count,
                    "timestamp": now_str,
                }
                state["orders"].append(canceled_order)
                self._save_state(state)

                if attempt + 1 < max_attempts:
                    adjust_count += 1
                    post_only_adjusted = True
                    if side_up == "BUY":
                        target_p = min(best_bid, best_ask - tick_size) if best_bid > 0 else (best_ask - tick_size)
                        price_dec = max(tick_size, round_to_step(target_p, tick_size, rounding=ROUND_DOWN))
                    else:
                        target_p = max(best_ask, best_bid + tick_size) if best_ask > 0 else (best_bid + tick_size)
                        price_dec = max(tick_size, round_to_step(target_p, tick_size, rounding=ROUND_DOWN))
                    logger.info(
                        "[PaperBroker] SOK (Post-Only) が板にぶつかったため1ティック調整して再発注します (%d/%d): %s price=%s",
                        adjust_count,
                        max_post_only_retries,
                        sym,
                        format_decimal_plain(price_dec),
                    )
                    continue
                return canceled_order

            order_record: Dict[str, Any] = {
                "order_id": order_id,
                "symbol": sym,
                "side": side_up,
                "execution_type": exec_up,
                "time_in_force": tif,
                "price": float(price_dec) if exec_up in ("LIMIT", "STOP") else 0.0,
                "size": float(size_dec),
                "executed_size": 0.0,
                "remaining_size": float(size_dec),
                "status": "WAITING",
                "cancel_type": "",
                "post_only_adjusted": post_only_adjusted,
                "adjust_count": adjust_count,
                "timestamp": now_str,
            }

            # MARKET 注文（または FAS LIMIT で既に条件到達している利確売り）は即時約定
            if exec_up == "MARKET":
                fill_price = (
                    float(best_bid if best_bid > 0 else (last_price if last_price > 0 else price_dec))
                    if side_up == "SELL"
                    else float(best_ask if best_ask > 0 else (last_price if last_price > 0 else price_dec))
                )
                self._fill_order_in_state(
                    state=state,
                    order_record=order_record,
                    fill_price=fill_price,
                    is_maker=False,
                    now_str=now_str,
                )
            elif (
                exec_up == "LIMIT"
                and side_up == "SELL"
                and tif != "SOK"
                and ((best_bid > 0 and best_bid >= price_dec) or (last_price > 0 and last_price >= price_dec))
            ):
                fill_price = float(max(price_dec, best_bid))
                self._fill_order_in_state(
                    state=state,
                    order_record=order_record,
                    fill_price=fill_price,
                    is_maker=True,
                    now_str=now_str,
                )

            state["orders"].append(order_record)
            self._save_state(state)
            return dict(order_record)

        raise GMOPrivateAPIError("注文処理が完遂しませんでした。")

    def _fill_order_in_state(
        self,
        state: Dict[str, Any],
        order_record: Dict[str, Any],
        fill_price: float,
        is_maker: bool,
        now_str: str,
        fill_size: Optional[float] = None,
    ) -> Dict[str, Any]:
        sym = str(order_record.get("symbol", "")).upper()
        rule = DEFAULT_SYMBOL_RULES.get(sym, {})
        maker_fee_rate = _to_decimal(rule.get("maker_fee", "-0.0001"))
        taker_fee_rate = _to_decimal(rule.get("taker_fee", "0.0005"))
        fee_rate = maker_fee_rate if is_maker else taker_fee_rate

        total_size_d = _to_decimal(order_record.get("size", 0))
        prev_exec_d = _to_decimal(order_record.get("executed_size", 0))
        rem_d = max(Decimal("0"), total_size_d - prev_exec_d)
        exec_size_d = _to_decimal(fill_size) if fill_size is not None else rem_d
        if exec_size_d > rem_d:
            exec_size_d = rem_d

        fill_price_d = _to_decimal(fill_price)
        notional_d = fill_price_d * exec_size_d
        fee_jpy_d = (notional_d * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        new_exec_d = prev_exec_d + exec_size_d
        new_rem_d = max(Decimal("0"), total_size_d - new_exec_d)

        order_record["executed_size"] = float(new_exec_d)
        order_record["remaining_size"] = float(new_rem_d)
        order_record["executed_price"] = float(fill_price_d)
        order_record["status"] = "EXECUTED" if new_rem_d == 0 else "ORDERED"
        order_record["executed_at"] = now_str

        exec_id = str(state["next_execution_id"])
        state["next_execution_id"] = int(state["next_execution_id"]) + 1
        execution_entry = {
            "execution_id": exec_id,
            "order_id": str(order_record["order_id"]),
            "symbol": sym,
            "side": str(order_record.get("side", "")).upper(),
            "execution_type": str(order_record.get("execution_type", "")).upper(),
            "size": float(exec_size_d),
            "price": float(fill_price_d),
            "fee": float(fee_jpy_d),
            "is_maker": bool(is_maker),
            "timestamp": now_str,
        }
        state["executions"].append(execution_entry)
        return execution_entry

    def evaluate_paper_orders(
        self,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Public API のリアルタイム Ticker (`ask`, `bid`, `last`) と照合し、
        待機中の Paper 注文の約定判定を実行する。
        - BUY `LIMIT`: `last <= limit_price` または `ask <= limit_price` で約定 (Maker)
        - SELL `STOP`: `bid <= stop_loss_price` または `last <= stop_loss_price` で逆指値発動・約定 (Taker)
        - SELL `LIMIT`: `bid >= take_profit_price` または `last >= take_profit_price` で利確約定 (Maker)
        """
        if tickers:
            self.update_market_tickers(tickers)

        now_str = _now_jst(now_dt).isoformat(timespec="seconds")
        state = self._ensure_state_file()
        new_executions: List[Dict[str, Any]] = []
        changed = False

        for order in state.get("orders", []):
            status = str(order.get("status", "")).upper()
            if status not in ("WAITING", "ORDERED"):
                continue

            sym = str(order.get("symbol", "")).upper()
            t = self._latest_tickers.get(sym) or self._get_symbol_ticker(sym)
            if not t:
                continue

            ask = float(t.get("ask", 0) or 0)
            bid = float(t.get("bid", 0) or 0)
            last = float(t.get("last", 0) or 0)
            side = str(order.get("side", "")).upper()
            exec_type = str(order.get("execution_type", "")).upper()
            order_price = float(order.get("price", 0) or 0)

            if side == "BUY" and exec_type == "LIMIT" and order_price > 0:
                if (last > 0 and last <= order_price) or (ask > 0 and ask <= order_price):
                    exec_entry = self._fill_order_in_state(
                        state=state,
                        order_record=order,
                        fill_price=order_price,
                        is_maker=True,
                        now_str=now_str,
                    )
                    new_executions.append(exec_entry)
                    changed = True

            elif side == "SELL" and exec_type == "STOP" and order_price > 0:
                if (bid > 0 and bid <= order_price) or (last > 0 and last <= order_price):
                    fill_p = min(bid, order_price) if bid > 0 else (min(last, order_price) if last > 0 else order_price)
                    exec_entry = self._fill_order_in_state(
                        state=state,
                        order_record=order,
                        fill_price=fill_p,
                        is_maker=False,
                        now_str=now_str,
                    )
                    new_executions.append(exec_entry)
                    changed = True

            elif side == "SELL" and exec_type == "LIMIT" and order_price > 0:
                if (bid > 0 and bid >= order_price) or (last > 0 and last >= order_price):
                    fill_p = max(bid, order_price) if bid > 0 else order_price
                    exec_entry = self._fill_order_in_state(
                        state=state,
                        order_record=order,
                        fill_price=fill_p,
                        is_maker=True,
                        now_str=now_str,
                    )
                    new_executions.append(exec_entry)
                    changed = True

        if changed:
            self._save_state(state)

        return new_executions

    def get_active_orders(
        self,
        symbol: Optional[str] = None,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        state = self._ensure_state_file()
        sym_filter = symbol.strip().upper() if symbol else None
        out: List[Dict[str, Any]] = []
        for o in state.get("orders", []):
            if str(o.get("status", "")).upper() not in ("WAITING", "ORDERED"):
                continue
            if sym_filter and str(o.get("symbol", "")).upper() != sym_filter:
                continue
            out.append(dict(o))
        return out[:count]

    def get_orders(self, order_ids: Union[str, int, List[Union[str, int]]]) -> List[Dict[str, Any]]:
        if isinstance(order_ids, (str, int)):
            id_set = {str(order_ids)}
        else:
            id_set = {str(x) for x in order_ids}
        state = self._ensure_state_file()
        return [dict(o) for o in state.get("orders", []) if str(o.get("order_id", "")) in id_set]

    def cancel_order(self, order_id: Union[str, int], now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        oid = str(order_id)
        now_str = _now_jst(now_dt).isoformat(timespec="seconds")
        state = self._ensure_state_file()
        for o in state.get("orders", []):
            if str(o.get("order_id", "")) == oid:
                if str(o.get("status", "")).upper() in ("WAITING", "ORDERED"):
                    o["status"] = "CANCELED"
                    o["remaining_size"] = 0.0
                    o["canceled_at"] = now_str
                    self._save_state(state)
                return dict(o)
        raise GMOPrivateAPIError(f"[PaperBroker] 指定された注文IDが見つかりません: {oid}")

    def get_latest_executions(
        self,
        symbol: str,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        sym = symbol.strip().upper()
        state = self._ensure_state_file()
        execs = [dict(e) for e in state.get("executions", []) if str(e.get("symbol", "")).upper() == sym]
        execs.reverse()
        return execs[:count]


def create_broker_client(
    execution_mode: Optional[str] = None,
    base_dir: Optional[Path] = None,
    public_client: Optional[GMOPublicClient] = None,
) -> Union[GMOPrivateClient, PaperBroker]:
    """
    `EXECUTION_MODE` (`DRY_RUN` または `LIVE`) に応じて適切なブローカークライアントを生成する。
    デフォルトは常に安全な `DRY_RUN` (`PaperBroker`)。
    """
    mode = (execution_mode or os.getenv("EXECUTION_MODE", "DRY_RUN")).strip().upper()
    root = Path(base_dir) if base_dir is not None else Path(__file__).resolve().parent.parent
    if mode == "LIVE":
        return GMOPrivateClient(public_client=public_client)
    return PaperBroker(
        paper_orders_path=root / "paper_orders.json",
        portfolio_path=root / "portfolio.json",
        public_client=public_client,
    )
