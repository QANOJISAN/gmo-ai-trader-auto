"""
GMOコイン Private API クライアント ＆ 仮想取引所シミュレータ (gmo_private_client.py)

【v2 フルスペック機構】
1. 優先度付きトークンバケット (`PriorityTokenBucket`):
   Tier 1 上限 (20 req/s) に対し安全マージンを設けた 4段階優先度制御:
   - Priority 1: 緊急成行決済・SL設置 (緊急リザーブ枠まで無制限利用可)
   - Priority 2: 注文取消・SL変更 (`changeOrder`, `cancelOrder`, `cancelOrders`, `cancelBulkOrder`)
   - Priority 3: 3者照合・状態照会 (`assets`, `activeOrders`, `orders`, `latestExecutions`)
   - Priority 4: 新規買い発注・一般データ取得 (`tradingVolume` 等)
2. クリスティアンのアルゴリズムによるサーバー時計オフセット自己校正 (`clock_offset_ms`):
   - JSONレスポンスの `responsetime`（ミリ秒UTC）および HTTP `Date` ヘッダから通信往復遅延 $\\text{RTT}/2$ を補正して
     常時 `clock_offset_ms` を算出し、`API-TIMESTAMP` を自動補正。
   - `ERR-5008` / `ERR-5009` 受信時は、そのエラー応答自身の `responsetime` / `Date` から即座にオフセットを自己校正して再試行。
3. 署名対象 JSON 文字列の直接送信 ＆ 指数表記 (`1e-05`) の完全排除。
4. 全エンドポイント実装:
   - `POST /private/v1/changeOrder`, `POST /private/v1/cancelOrders`, `POST /private/v1/cancelBulkOrder`,
     `GET /private/v1/account/tradingVolume`, `POST/PUT/DELETE /private/v1/ws-auth`
   - ※ `PUT/DELETE /private/v1/ws-auth` はリクエスト body に `{"token": ...}` を送るが、
     **HMAC 署名対象文字列には body を含めない (`timestamp + method + /v1/ws-auth`)** GMO 固有仕様を厳守。
5. 認証エラー (`ERR-5010〜5012`) 時は絶対にドライラン残高へフォールバックせず、即座にシステムを `HALT`（台帳不変更）にする。
6. POST 発注時のタイムアウトや `ERR-5003` は盲目的に再送せず、必ず `intents` を `UNKNOWN` にして
   `activeOrders` と `latestExecutions` の照合を先に行う。
"""

from __future__ import annotations

import email.utils
import hashlib
import hmac
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

from src.database_wal import DatabaseWAL
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
    """GMOコイン Private API エラー基底例外"""

    def __init__(
        self,
        message: str,
        messages: Optional[List[Dict[str, Any]]] = None,
        error_codes: Optional[List[str]] = None,
    ) -> None:
        super().__init__(message)
        self.messages = messages or []
        self.error_codes = error_codes or [
            str(m.get("message_code", "")) for m in (messages or []) if isinstance(m, dict)
        ]


class InsufficientAvailableAssetError(GMOPrivateAPIError):
    """現物保有数量または日本円余力が別注文（SL等）でロックされており発注できない場合の例外 (ERR-201)"""


class GMOAuthHaltError(GMOPrivateAPIError):
    """認証エラー (ERR-5010〜5012)・権限エラーによりシステムを即時 HALT させる致命的例外"""


class GMORateLimitError(GMOPrivateAPIError):
    """レートリミット超過 (ERR-5003 / HTTP 429) 例外"""


class GMOMaintenanceError(GMOPrivateAPIError):
    """メンテナンス中・プレオープン中 (ERR-5201〜5206 / ERR-5203) 例外"""


class GMOOrderOutcomeUnknownError(GMOPrivateAPIError):
    """POST 発注時のタイムアウト等で約定・注文成否が不明 (UNKNOWN) となり照合が必要な例外"""


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


def parse_server_time_ms(
    responsetime_str: Optional[str] = None,
    http_date_header: Optional[str] = None,
) -> Optional[int]:
    """
    JSON の `responsetime`（例: `"2026-10-21T12:00:00.123Z"`）または HTTP `Date` ヘッダから
    サーバー時刻の Unix ミリ秒を抽出する。
    """
    if responsetime_str:
        s = str(responsetime_str).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass
    if http_date_header:
        try:
            dt = email.utils.parsedate_to_datetime(str(http_date_header).strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass
    return None


class PriorityTokenBucket:
    """
    Tier 1 上限 (20 req/s) に対して安全マージンを設けた 4段階優先度付きトークンバケット。
    - Priority 1 (緊急決済・SL設置): 予約リザーブ 0（最後の1トークンまで使用可能）
    - Priority 2 (注文取消・SL変更): 予約リザーブ 2（緊急用2トークンを残す）
    - Priority 3 (3者照合・状態照会): 予約リザーブ 4
    - Priority 4 (新規買い・一般取得): 予約リザーブ 6
    """

    RESERVE_BY_PRIORITY: Dict[int, float] = {
        1: 0.0,
        2: 2.0,
        3: 4.0,
        4: 6.0,
    }

    def __init__(self, rate_per_sec: float = 15.0, capacity: float = 16.0) -> None:
        self.rate_per_sec = float(rate_per_sec)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()
        self.total_consumed_by_priority: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
        self.total_waits_by_priority: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill)
        self.last_refill = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)

    def acquire(self, priority: int = 3, cost: float = 1.0, max_wait_sec: float = 5.0) -> float:
        """
        指定された優先度でトークンを取得する。
        低優先度リクエストはリザーブ閾値を下回ると自動的に待機し、Priority 1/2 の帯域を100%保護する。
        戻り値: 待機した秒数
        """
        prio = min(4, max(1, int(priority)))
        reserve = self.RESERVE_BY_PRIORITY.get(prio, 4.0)
        required = reserve + float(cost)
        start_t = time.monotonic()
        waited = 0.0

        while True:
            with self._lock:
                self._refill()
                if self.tokens >= required:
                    self.tokens -= float(cost)
                    self.total_consumed_by_priority[prio] = self.total_consumed_by_priority.get(prio, 0) + 1
                    return waited
                deficit = required - self.tokens
                sleep_sec = min(0.25, max(0.005, deficit / self.rate_per_sec))

            if (time.monotonic() - start_t) + sleep_sec > max_wait_sec:
                with self._lock:
                    self._refill()
                    if prio == 1:
                        # Priority 1（緊急決済・SL設置）はタイムアウト時も強制通過を許可
                        self.tokens = max(0.0, self.tokens - float(cost))
                        self.total_consumed_by_priority[1] += 1
                        return time.monotonic() - start_t
                raise GMORateLimitError(
                    f"PriorityTokenBucket 待機上限超過 (priority={prio}, tokens={self.tokens:.2f}, required={required:.2f})"
                )

            with self._lock:
                self.total_waits_by_priority[prio] = self.total_waits_by_priority.get(prio, 0) + 1
            time.sleep(sleep_sec)
            waited = time.monotonic() - start_t


class GMOPrivateClient:
    """GMOコイン 公式 Private API クライアント (LIVE モード用・自己修復時計オフセット＆優先度バケット内蔵)"""

    BASE_URL = "https://api.coin.z.com/private"
    MODE = "LIVE"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        public_client: Optional[GMOPublicClient] = None,
        wal_db: Optional[DatabaseWAL] = None,
        token_bucket: Optional[PriorityTokenBucket] = None,
        timeout: int = 10,
        max_retries: int = 2,
        retry_delay: float = 0.5,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.getenv("GMO_API_KEY", "")).strip()
        self.api_secret = (api_secret if api_secret is not None else os.getenv("GMO_API_SECRET", "")).strip()
        self.public_client = public_client or GMOPublicClient(timeout=timeout)
        self.wal_db = wal_db
        self.token_bucket = token_bucket or PriorityTokenBucket()
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.clock_offset_ms: int = 0
        self.last_rtt_ms: float = 0.0
        self.halted: bool = False
        self.halt_reason: str = ""

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

    def _current_timestamp_ms(self) -> int:
        return int(time.time() * 1000) + int(self.clock_offset_ms)

    def update_clock_offset_cristian(
        self,
        t_send_ms: float,
        t_recv_ms: float,
        responsetime_str: Optional[str] = None,
        http_date_header: Optional[str] = None,
    ) -> Optional[int]:
        """
        クリスティアンのアルゴリズムにより通信往復遅延 $\\text{RTT}/2$ を補正して
        サーバー時刻オフセット `clock_offset_ms` を更新する。
        $\\text{offset} = (T_{\\text{server}} + \\text{RTT}/2) - T_{\\text{recv}}$
        """
        server_ms = parse_server_time_ms(responsetime_str, http_date_header)
        if server_ms is None:
            return None
        rtt_ms = max(0.0, float(t_recv_ms) - float(t_send_ms))
        self.last_rtt_ms = rtt_ms
        estimated_server_now_ms = float(server_ms) + (rtt_ms / 2.0)
        self.clock_offset_ms = int(round(estimated_server_now_ms - float(t_recv_ms)))
        return self.clock_offset_ms

    def _build_auth_headers(
        self,
        method: str,
        v1_path: str,
        body_str: str = "",
        timestamp_ms: Optional[int] = None,
        exclude_body_from_sign: bool = False,
    ) -> Dict[str, str]:
        """
        HMAC-SHA256 署名ヘッダを生成する。
        ※ GMOコイン仕様: `PUT /v1/ws-auth` および `DELETE /v1/ws-auth` は body に token を送るが、
          署名対象文字列 (`timestamp + method + path`) には body を含めない (`exclude_body_from_sign=True`)。
        """
        if self.halted:
            raise GMOAuthHaltError(f"クライアントは HALT 状態のためリクエストを拒絶しました: {self.halt_reason}")
        if not self.has_credentials():
            raise GMOAuthHaltError("GMO_API_KEY または GMO_API_SECRET が未設定です。")

        ts_val = int(timestamp_ms if timestamp_ms is not None else self._current_timestamp_ms())
        timestamp = str(ts_val)
        method_up = method.upper()
        sign_body = "" if exclude_body_from_sign else body_str
        text = f"{timestamp}{method_up}{v1_path}{sign_body}"
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

    @staticmethod
    def serialize_body_strict(body: Optional[Dict[str, Any]]) -> str:
        """
        署名対象 JSON 文字列を生成する。
        - 空白なし `separators=(",", ":")`
        - 数値の指数表記 (`1e-05`) が混入しないよう Decimal / float を事前正規化
        """
        if body is None:
            return ""
        cleaned: Dict[str, Any] = {}
        for k, v in body.items():
            if isinstance(v, Decimal):
                cleaned[k] = format_decimal_plain(v)
            elif isinstance(v, float):
                cleaned[k] = format_decimal_plain(v)
            else:
                cleaned[k] = v
        body_str = json.dumps(cleaned, separators=(",", ":"), ensure_ascii=False)
        return body_str

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        priority: int = 3,
        exclude_body_from_sign: bool = False,
        allow_post_clock_retry: bool = True,
    ) -> Any:
        if self.halted:
            raise GMOAuthHaltError(f"システムは HALT 状態です: {self.halt_reason}")

        v1_path = _normalize_v1_path(path)
        url = f"{self.BASE_URL}{v1_path}"
        method_up = method.upper()
        body_str = self.serialize_body_strict(body) if (body is not None and method_up in ("POST", "PUT", "DELETE")) else ""

        attempts = self.max_retries if method_up == "GET" else 1
        clock_recalibrated = False
        last_err: Optional[Exception] = None

        attempt = 0
        while attempt < attempts:
            attempt += 1
            self.token_bucket.acquire(priority=priority)
            t_send_ms = time.time() * 1000.0
            try:
                headers = self._build_auth_headers(
                    method=method_up,
                    v1_path=v1_path,
                    body_str=body_str,
                    exclude_body_from_sign=exclude_body_from_sign,
                )
                if method_up == "GET":
                    resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                elif method_up == "PUT":
                    resp = self.session.put(
                        url,
                        data=body_str.encode("utf-8") if body_str else None,
                        headers=headers,
                        timeout=self.timeout,
                    )
                elif method_up == "DELETE":
                    resp = self.session.delete(
                        url,
                        data=body_str.encode("utf-8") if body_str else None,
                        headers=headers,
                        timeout=self.timeout,
                    )
                else:
                    resp = self.session.post(
                        url,
                        data=body_str.encode("utf-8") if body_str else None,
                        headers=headers,
                        timeout=self.timeout,
                    )
                t_recv_ms = time.time() * 1000.0

                payload: Dict[str, Any] = {}
                try:
                    payload = resp.json() if resp.text else {}
                except Exception:
                    payload = {}

                # 正常応答・エラー応答を問わず responsetime / Date ヘッダから時計オフセットを更新
                self.update_clock_offset_cristian(
                    t_send_ms=t_send_ms,
                    t_recv_ms=t_recv_ms,
                    responsetime_str=payload.get("responsetime") if isinstance(payload, dict) else None,
                    http_date_header=resp.headers.get("Date") if hasattr(resp, "headers") else None,
                )

                if resp.status_code == 429:
                    raise GMORateLimitError(f"HTTP 429 Too Many Requests ({v1_path})")
                resp.raise_for_status()

                if payload.get("status") != 0:
                    messages = payload.get("messages", []) or []
                    codes = [str(m.get("message_code", "")) for m in messages if isinstance(m, dict)]
                    msg_str = f"GMO Private API エラー (path={v1_path}, codes={codes}): {messages}"

                    # 1) ERR-5008 / ERR-5009 (時刻ズレエラー): 即座にオフセットを自己校正して再試行
                    if any(c in ("ERR-5008", "ERR-5009") for c in codes) and not clock_recalibrated:
                        clock_recalibrated = True
                        if method_up == "GET" or allow_post_clock_retry:
                            logger.warning(
                                "時計オフセット誤差 (%s) を検知しました。自己校正 (offset=%dms) して再試行します。",
                                codes,
                                self.clock_offset_ms,
                            )
                            attempt -= 1
                            continue

                    # 2) ERR-5010〜5012 (APIキー・署名・IP制限等の認証エラー): 即座に HALT（ドライランへのフォールバック厳禁）
                    if any(c in ("ERR-5010", "ERR-5011", "ERR-5012", "ERR-5001", "ERR-5002") for c in codes):
                        self.halted = True
                        self.halt_reason = msg_str
                        raise GMOAuthHaltError(msg_str, messages=messages, error_codes=codes)

                    # 3) ERR-201 (利用可能残高・ロック不足)
                    if "ERR-201" in codes:
                        raise InsufficientAvailableAssetError(msg_str, messages=messages, error_codes=codes)

                    # 4) ERR-5003 (APIレートリミット超過)
                    if "ERR-5003" in codes:
                        raise GMORateLimitError(msg_str, messages=messages, error_codes=codes)

                    # 5) ERR-5201〜5206 (メンテナンス・プレオープン ERR-5203)
                    if any(c.startswith("ERR-520") for c in codes):
                        raise GMOMaintenanceError(msg_str, messages=messages, error_codes=codes)

                    raise GMOPrivateAPIError(msg_str, messages=messages, error_codes=codes)

                return payload.get("data")

            except (GMOAuthHaltError, InsufficientAvailableAssetError, GMOMaintenanceError):
                raise
            except GMORateLimitError:
                if method_up != "GET":
                    raise
                last_err = GMORateLimitError(f"Rate limit on {v1_path}")
                if attempt < attempts:
                    time.sleep(self.retry_delay * attempt)
            except GMOPrivateAPIError:
                raise
            except Exception as exc:
                last_err = exc
                if method_up != "GET":
                    # POST 等の状態変更リクエストで通信タイムアウトが発生した場合は盲目的に再送せず例外を送出
                    raise GMOOrderOutcomeUnknownError(
                        f"POST/変更系リクエストで通信タイムアウトまたは不明エラーが発生しました ({method_up} {v1_path}): {exc}"
                    ) from exc
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

    # ------------------------------------------------------------------
    # 口座・取引高・WebSocket 認証エンドポイント
    # ------------------------------------------------------------------
    def get_assets(self) -> List[Dict[str, Any]]:
        """口座資産残高一覧を取得する (`GET /private/v1/account/assets`, Priority 3)"""
        data = self._request("GET", "/private/v1/account/assets", priority=3)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("list"), list):
            return data["list"]
        return []

    def get_trading_volume(self) -> Dict[str, Any]:
        """取引高・手数料率情報を取得する (`GET /private/v1/account/tradingVolume`, Priority 4)"""
        data = self._request("GET", "/private/v1/account/tradingVolume", priority=4)
        return data if isinstance(data, dict) else {}

    def create_ws_auth_token(self) -> str:
        """Private WebSocket 認証トークンを新規発行する (`POST /private/v1/ws-auth`, Priority 3)"""
        data = self._request("POST", "/private/v1/ws-auth", body={}, priority=3)
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            return str(data.get("token", ""))
        return ""

    def extend_ws_auth_token(self, token: str) -> bool:
        """
        Private WebSocket 認証トークンを延長する (`PUT /private/v1/ws-auth`, Priority 3)。
        ※ GMOコイン仕様: リクエスト body には `{"token": token}` を送るが、
          HMAC 署名文字列には body を含めない (`exclude_body_from_sign=True`)。
        """
        self._request(
            "PUT",
            "/private/v1/ws-auth",
            body={"token": token},
            priority=3,
            exclude_body_from_sign=True,
        )
        return True

    def delete_ws_auth_token(self, token: str) -> bool:
        """
        Private WebSocket 認証トークンを削除する (`DELETE /private/v1/ws-auth`, Priority 3)。
        ※ `PUT` と同様、HMAC 署名文字列には body を含めない。
        """
        self._request(
            "DELETE",
            "/private/v1/ws-auth",
            body={"token": token},
            priority=3,
            exclude_body_from_sign=True,
        )
        return True

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
            "jpy_balance": jpy_amount,
            "jpy_available": jpy_available,
            "jpy_locked": max(0.0, round(jpy_amount - jpy_available, 2)),
            "spot_assets": spot_assets,
        }

    # ------------------------------------------------------------------
    # 注文照会・変更・取消・一括取消エンドポイント
    # ------------------------------------------------------------------
    def get_active_orders(
        self,
        symbol: Optional[str] = None,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """有効注文一覧を取得する (`GET /private/v1/activeOrders`, Priority 3)"""
        symbols_to_query = [symbol.upper()] if symbol else list(DEFAULT_SYMBOL_RULES.keys())
        results: List[Dict[str, Any]] = []
        for sym in symbols_to_query:
            data = self._request(
                "GET",
                "/private/v1/activeOrders",
                params={"symbol": sym, "page": page, "count": count},
                priority=3,
            )
            raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in raw_list:
                results.append(self._normalize_order_dict(item))
        return results

    def get_orders(self, order_ids: Union[str, int, List[Union[str, int]]]) -> List[Dict[str, Any]]:
        """注文ID指定で注文情報を取得する (`GET /private/v1/orders`, Priority 3)"""
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
            priority=3,
        )
        raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return [self._normalize_order_dict(item) for item in raw_list]

    def change_order(
        self,
        order_id: Union[str, int],
        price: Union[str, int, float, Decimal],
        losscut_price: Optional[Union[str, int, float, Decimal]] = None,
    ) -> Dict[str, Any]:
        """注文価格を変更する (`POST /private/v1/changeOrder`, Priority 2)"""
        oid = int(order_id) if str(order_id).isdigit() else order_id
        body: Dict[str, Any] = {
            "orderId": oid,
            "price": format_decimal_plain(price),
        }
        if losscut_price is not None and _to_decimal(losscut_price) > 0:
            body["losscutPrice"] = format_decimal_plain(losscut_price)
        self._request("POST", "/private/v1/changeOrder", body=body, priority=2)
        return {
            "order_id": str(order_id),
            "price": float(_to_decimal(price)),
            "status": "CHANGED",
            "order_status": "ORDERED",
        }

    def cancel_order(self, order_id: Union[str, int], now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """単一注文をキャンセルする (`POST /private/v1/cancelOrder`, Priority 2)"""
        oid = int(order_id) if str(order_id).isdigit() else order_id
        self._request("POST", "/private/v1/cancelOrder", body={"orderId": oid}, priority=2)
        return {"order_id": str(order_id), "status": "CANCELED"}

    def cancel_orders(self, order_ids: List[Union[str, int]]) -> Dict[str, Any]:
        """複数注文（最大10件ずつ）を一括キャンセルする (`POST /private/v1/cancelOrders`, Priority 2)"""
        normalized_ids = [int(x) if str(x).isdigit() else x for x in order_ids if str(x).strip()]
        if not normalized_ids:
            return {"success": [], "failed": []}
        data = self._request(
            "POST",
            "/private/v1/cancelOrders",
            body={"orderIds": normalized_ids[:10]},
            priority=2,
        )
        return data if isinstance(data, dict) else {"success": normalized_ids[:10], "failed": []}

    def cancel_bulk_order(
        self,
        symbols: List[str],
        side: Optional[str] = None,
        execution_type: Optional[str] = None,
        desc: Optional[str] = None,
    ) -> List[str]:
        """指定銘柄の有効注文を一括キャンセルする (`POST /private/v1/cancelBulkOrder`, Priority 2)"""
        syms = [s.strip().upper() for s in symbols if s.strip()]
        body: Dict[str, Any] = {"symbols": syms}
        if side:
            body["side"] = side.strip().upper()
        if execution_type:
            body["executionType"] = execution_type.strip().upper()
        if desc:
            body["desc"] = desc.strip().upper()
        data = self._request("POST", "/private/v1/cancelBulkOrder", body=body, priority=2)
        if isinstance(data, list):
            return [str(x) for x in data]
        return []

    def get_latest_executions(
        self,
        symbol: str,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """最新約定一覧を取得する (`GET /private/v1/latestExecutions`, Priority 3)"""
        if not symbol or not str(symbol).strip():
            raise ValueError("get_latest_executions には有効な symbol 指定が必須です。")
        sym_up = str(symbol).strip().upper()
        data = self._request(
            "GET",
            "/private/v1/latestExecutions",
            params={"symbol": sym_up, "page": page, "count": count},
            priority=3,
        )
        raw_list = data.get("list", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        out: List[Dict[str, Any]] = []
        for item in raw_list:
            out.append(
                {
                    "execution_id": str(item.get("executionId", item.get("execution_id", ""))),
                    "order_id": str(item.get("orderId", item.get("order_id", ""))),
                    "symbol": str(item.get("symbol", sym_up)).upper(),
                    "side": str(item.get("side", "")).upper(),
                    "execution_type": str(item.get("executionType", item.get("execution_type", ""))).upper(),
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
        now_dt: Optional[datetime] = None,
        intent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        新規注文を発注する (`POST /private/v1/order`)。
        - Priority 1: `STOP` 逆指値、または `MARKET` / `cancelBefore=True` の緊急決済
        - Priority 4: 新規 `BUY` 指値
        - タイムアウトや `ERR-5003` 発生時は盲目的に再送せず、WAL `intents` を `UNKNOWN` に記録して
          `get_active_orders(sym)` と `get_latest_executions(sym)` を先に照合し、二重発注を100%防ぐ。
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

        if cancel_before and not (side_up == "SELL" and exec_up == "MARKET" and tif == "FAK"):
            raise GMOPrivateAPIError(
                f"GMO公式API仕様違反: cancelBefore=true は現物の成行売り (side=SELL, executionType=MARKET, timeInForce=FAK) のみ使用可能です "
                f"(指定値: side={side_up}, executionType={exec_up}, timeInForce={tif})"
            )

        prio = 1 if (side_up == "SELL" and (exec_up in ("STOP", "MARKET") or cancel_before)) else 4
        price_dec = round_to_step(_to_decimal(price or 0), tick_size, rounding=ROUND_DOWN) if price is not None else Decimal("0")
        adjust_count = 0
        post_only_adjusted = False
        max_attempts = (1 + max(0, int(max_post_only_retries))) if (exec_up == "LIMIT" and tif == "SOK") else 1

        if self.wal_db is not None and intent_id is None:
            intent_id = self.wal_db.create_intent(
                kind=f"PLACE_{side_up}_{exec_up}",
                symbol=sym,
                payload={
                    "symbol": sym,
                    "side": side_up,
                    "execution_type": exec_up,
                    "time_in_force": tif,
                    "size": size_str,
                    "price": format_decimal_plain(price_dec),
                    "cancel_before": bool(cancel_before),
                },
            )

        for attempt in range(max_attempts):
            body: Dict[str, Any] = {
                "symbol": sym,
                "side": side_up,
                "execution_type": exec_up,
                "executionType": exec_up,
                "timeInForce": tif,
                "size": size_str,
            }
            # GMO Private API の正式キー名のみ残す
            body.pop("execution_type", None)
            if exec_up in ("LIMIT", "STOP"):
                body["price"] = format_decimal_plain(price_dec)
            if losscut_price is not None and _to_decimal(losscut_price) > 0:
                body["losscutPrice"] = format_decimal_plain(
                    round_to_step(_to_decimal(losscut_price), tick_size, rounding=ROUND_DOWN)
                )
            if cancel_before:
                body["cancelBefore"] = True

            try:
                raw_order_id = self._request(
                    "POST",
                    "/private/v1/order",
                    body=body,
                    priority=prio,
                    allow_post_clock_retry=True,
                )
            except (GMOOrderOutcomeUnknownError, GMORateLimitError) as exc:
                if self.wal_db is not None and intent_id:
                    self.wal_db.update_intent_state(intent_id, "UNKNOWN")
                # 盲目的再送を行わず、先に activeOrders と latestExecutions を照合して注文が通っていたか確認
                recovered = self._reconcile_unknown_order_outcome(
                    symbol=sym,
                    side=side_up,
                    execution_type=exec_up,
                    size_dec=size_dec,
                    price_dec=price_dec,
                    intent_id=intent_id,
                )
                if recovered is not None:
                    return recovered
                raise
            except Exception:
                if self.wal_db is not None and intent_id:
                    self.wal_db.update_intent_state(intent_id, "REJECTED")
                raise

            order_id = str(raw_order_id.get("orderId", raw_order_id) if isinstance(raw_order_id, dict) else raw_order_id)
            if self.wal_db is not None and intent_id:
                self.wal_db.update_intent_state(intent_id, "SENT_OK", order_id=order_id)

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
                "intent_id": intent_id or "",
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

            if self.wal_db is not None:
                self.wal_db.upsert_order(
                    order_id=order_id,
                    symbol=sym,
                    side=side_up,
                    execution_type=exec_up,
                    state=str(order_info.get("status", "WAITING")),
                    price=price_dec,
                    size=size_dec,
                    executed_size=order_info.get("executed_size", 0.0),
                    time_in_force=tif,
                    intent_id=intent_id or "",
                    raw_data=order_info,
                )
                if intent_id:
                    self.wal_db.update_intent_state(intent_id, "COMMITTED", order_id=order_id)

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
                continue

            return order_info

        return order_info

    def _reconcile_unknown_order_outcome(
        self,
        symbol: str,
        side: str,
        execution_type: str,
        size_dec: Decimal,
        price_dec: Decimal,
        intent_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST 発注時にタイムアウトまたは `ERR-5003` が起きた際、
        盲目的に再送せず `activeOrders` と `latestExecutions` を照合して既に通っていた注文を特定する。
        """
        try:
            active_list = self.get_active_orders(symbol)
            for ao in active_list:
                if (
                    str(ao.get("side", "")).upper() == side
                    and str(ao.get("execution_type", "")).upper() == execution_type
                    and abs(_to_decimal(ao.get("size", 0)) - size_dec) <= Decimal("1e-9")
                ):
                    if self.wal_db is not None and intent_id:
                        self.wal_db.update_intent_state(intent_id, "COMMITTED", order_id=str(ao["order_id"]))
                    return ao

            exec_list = self.get_latest_executions(symbol)
            for ex in exec_list:
                if (
                    str(ex.get("side", "")).upper() == side
                    and abs(_to_decimal(ex.get("size", 0)) - size_dec) <= Decimal("1e-9")
                ):
                    oid = str(ex.get("order_id", ""))
                    if self.wal_db is not None and intent_id:
                        self.wal_db.update_intent_state(intent_id, "COMMITTED", order_id=oid)
                    return {
                        "order_id": oid,
                        "symbol": symbol,
                        "side": side,
                        "execution_type": execution_type,
                        "price": float(ex.get("price", price_dec)),
                        "size": float(size_dec),
                        "executed_size": float(ex.get("size", size_dec)),
                        "status": "EXECUTED",
                        "recovered_from_unknown": True,
                    }
        except Exception as rec_exc:
            logger.warning("UNKNOWN 注文の照合中にエラーが発生しました: %s", rec_exc)
        return None

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
    - `GMOPrivateClient` と完全同一のメソッドインターフェース（`change_order`, `cancel_orders`,
      `cancel_bulk_order`, `cancel_before=True`, `ws-auth` 含む）を提供。
    """

    MODE = "DRY_RUN"

    def __init__(
        self,
        paper_orders_path: Union[str, Path] = "paper_orders.json",
        portfolio_path: Union[str, Path] = "portfolio.json",
        public_client: Optional[GMOPublicClient] = None,
        wal_db: Optional[DatabaseWAL] = None,
    ) -> None:
        self.paper_orders_path = Path(paper_orders_path)
        self.portfolio_path = Path(portfolio_path)
        self.public_client = public_client
        self.wal_db = wal_db
        self.clock_offset_ms: int = 0
        self.halted: bool = False
        self.halt_reason: str = ""
        self._latest_tickers: Dict[str, Dict[str, Any]] = {}
        self._ensure_state_file()

    def has_credentials(self) -> bool:
        return True

    def update_market_tickers(self, tickers: Dict[str, Dict[str, Any]]) -> None:
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
        portfolio = self._load_portfolio()
        state = self._ensure_state_file()
        active_orders = [
            o for o in state.get("orders", []) if str(o.get("status", "")).upper() in ("WAITING", "ORDERED")
        ]
        pending_order_ids = {
            str(p.get("order_id", "")) for p in (portfolio.get("pending_orders", []) or []) if p.get("order_id")
        }

        has_explicit_state_jpy = "jpy_balance" in state
        jpy_balance = _to_decimal(
            state["jpy_balance"] if has_explicit_state_jpy else portfolio.get("jpy_balance", 10000)
        )
        locked_jpy = Decimal("0")
        locked_spot: Dict[str, Decimal] = {}
        partial_buy_spot: Dict[str, Decimal] = {}

        for o in active_orders:
            side = str(o.get("side", "")).upper()
            sym = str(o.get("symbol", "")).upper()
            oid = str(o.get("order_id", ""))
            exec_sz = _to_decimal(o.get("executed_size", 0))
            rem_size = _to_decimal(o.get("size", 0)) - exec_sz
            if side == "BUY" and exec_sz > 0 and oid in pending_order_ids:
                exec_px = _to_decimal(o.get("executed_price") or o.get("price", 0))
                rule = DEFAULT_SYMBOL_RULES.get(sym, {})
                maker_fee_rate = _to_decimal(rule.get("maker_fee", "-0.0001"))
                partial_cost = (exec_px * exec_sz * (Decimal("1") + maker_fee_rate)).to_integral_value(
                    rounding=ROUND_HALF_UP
                )
                if not has_explicit_state_jpy:
                    jpy_balance -= partial_cost
                partial_buy_spot[sym] = partial_buy_spot.get(sym, Decimal("0")) + exec_sz
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
        for sym, p_sz in partial_buy_spot.items():
            if sym and p_sz > 0:
                spot_totals[sym] = spot_totals.get(sym, Decimal("0")) + p_sz

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

    def get_trading_volume(self) -> Dict[str, Any]:
        return {"jpyVolume": "0", "tierLevel": 1, "limit": []}

    def create_ws_auth_token(self) -> str:
        return "paper-ws-token-001"

    def extend_ws_auth_token(self, token: str) -> bool:
        return bool(token)

    def delete_ws_auth_token(self, token: str) -> bool:
        return bool(token)

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
            "jpy_balance": jpy_amount,
            "jpy_available": jpy_available,
            "jpy_locked": max(0.0, round(jpy_amount - jpy_available, 2)),
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
        intent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self.halted:
            raise GMOAuthHaltError(f"PaperBroker is HALTED: {self.halt_reason}")

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

        # cancel_before=True が指定された場合、GMO公式仕様 (SELL + MARKET + FAK 限定) を検証した上で既存有効注文を原子的にキャンセルする
        if cancel_before:
            if not (side_up == "SELL" and exec_up == "MARKET" and tif == "FAK"):
                raise GMOPrivateAPIError(
                    f"GMO公式API仕様違反: cancelBefore=true は現物の成行売り (side=SELL, executionType=MARKET, timeInForce=FAK) のみ使用可能です "
                    f"(指定値: side={side_up}, executionType={exec_up}, timeInForce={tif})"
                )
            self.cancel_bulk_order([sym], side=side_up)

        balances = self.get_jpy_and_spot_balances()
        if side_up == "SELL":
            spot_info = balances["spot_assets"].get(sym, {"amount": 0.0, "available": 0.0, "locked": 0.0})
            avail_dec = _to_decimal(spot_info.get("available", 0.0))
            if avail_dec + Decimal("1e-12") < size_dec:
                raise InsufficientAvailableAssetError(
                    f"[ERR-201 PaperBroker] {sym} の売却可能数量が不足しています "
                    f"(要求={format_decimal_plain(size_dec)}, 利用可能={format_decimal_plain(avail_dec)}, "
                    f"SLロック中={format_decimal_plain(spot_info.get('locked', 0.0))})。"
                    "先に既存の逆指値(STOP)注文を cancel_order するか cancel_before=True を使用してください。",
                    messages=[{"message_code": "ERR-201", "message_string": "Insufficient available asset"}],
                )
        elif side_up == "BUY":
            jpy_avail_dec = _to_decimal(balances.get("jpy_available", 0.0))
            est_price = price_dec if price_dec > 0 else _to_decimal(self._get_symbol_ticker(sym).get("ask", 0) or 0)
            req_jpy = est_price * size_dec
            if est_price > 0 and req_jpy > jpy_avail_dec + Decimal("1"):
                raise InsufficientAvailableAssetError(
                    f"[ERR-201 PaperBroker] 日本円余力が不足しています "
                    f"(必要額≈{int(req_jpy):,}円, 利用可能={int(jpy_avail_dec):,}円)",
                    messages=[{"message_code": "ERR-201", "message_string": "Insufficient available JPY"}],
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
                    "losscut_price": float(_to_decimal(losscut_price or 0)),
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
                    continue
                return canceled_order

            order_record: Dict[str, Any] = {
                "order_id": order_id,
                "symbol": sym,
                "side": side_up,
                "execution_type": exec_up,
                "time_in_force": tif,
                "price": float(price_dec) if exec_up in ("LIMIT", "STOP") else 0.0,
                "losscut_price": float(_to_decimal(losscut_price or 0)),
                "size": float(size_dec),
                "executed_size": 0.0,
                "remaining_size": float(size_dec),
                "status": "WAITING",
                "cancel_type": "",
                "post_only_adjusted": post_only_adjusted,
                "adjust_count": adjust_count,
                "timestamp": now_str,
            }

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

    def change_order(
        self,
        order_id: Union[str, int],
        price: Union[str, int, float, Decimal],
        losscut_price: Optional[Union[str, int, float, Decimal]] = None,
    ) -> Dict[str, Any]:
        oid = str(order_id)
        state = self._ensure_state_file()
        for o in state.get("orders", []):
            if str(o.get("order_id", "")) == oid:
                if str(o.get("status", "")).upper() not in ("WAITING", "ORDERED"):
                    raise GMOPrivateAPIError(f"非アクティブ注文は変更できません: {oid}")
                sym = str(o.get("symbol", "")).upper()
                tick = _to_decimal(DEFAULT_SYMBOL_RULES.get(sym, {}).get("tick_size", "1"))
                p_dec = round_to_step(_to_decimal(price), tick, rounding=ROUND_DOWN)
                o["price"] = float(p_dec)
                if losscut_price is not None:
                    o["losscut_price"] = float(_to_decimal(losscut_price))
                self._save_state(state)
                res = dict(o)
                res["order_status"] = str(o.get("status", "ORDERED"))
                res["status"] = "CHANGED"
                return res
        raise GMOPrivateAPIError(f"[PaperBroker] 変更対象の注文IDが見つかりません: {oid}")

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
        if self.wal_db is not None:
            try:
                self.wal_db.record_execution(
                    execution_id=exec_id,
                    order_id=str(order_record["order_id"]),
                    symbol=sym,
                    side=str(order_record.get("side", "")).upper(),
                    size=exec_size_d,
                    price=fill_price_d,
                    fee_jpy=fee_jpy_d,
                    execution_type=str(order_record.get("execution_type", "")).upper(),
                )
            except Exception:
                pass
        return execution_entry

    def simulate_partial_fill(
        self,
        order_id: Union[str, int],
        filled_size: float,
        fill_price: Optional[float] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        oid = str(order_id)
        now_str = _now_jst(now_dt).isoformat(timespec="seconds")
        state = self._ensure_state_file()
        for o in state.get("orders", []):
            if str(o.get("order_id", "")) == oid:
                p = float(fill_price if fill_price is not None else (o.get("price") or 0))
                is_maker = str(o.get("execution_type", "")).upper() == "LIMIT"
                exec_entry = self._fill_order_in_state(
                    state=state,
                    order_record=o,
                    fill_price=p,
                    is_maker=is_maker,
                    now_str=now_str,
                    fill_size=filled_size,
                )
                self._save_state(state)
                return exec_entry
        raise GMOPrivateAPIError(f"[PaperBroker] 部分約定対象の注文IDが見つかりません: {oid}")

    def evaluate_paper_orders(
        self,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
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
            id_set = {str(order_ids)} if str(order_ids).strip() else set()
        else:
            id_set = {str(x) for x in order_ids if str(x).strip()}
        if not id_set:
            return []
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

    def cancel_orders(self, order_ids: List[Union[str, int]]) -> Dict[str, Any]:
        success = []
        failed = []
        for oid in order_ids[:10]:
            try:
                self.cancel_order(oid)
                success.append(str(oid))
            except Exception as exc:
                failed.append({"orderId": str(oid), "error": str(exc)})
        return {"success": success, "failed": failed}

    def cancel_bulk_order(
        self,
        symbols: List[str],
        side: Optional[str] = None,
        execution_type: Optional[str] = None,
        desc: Optional[str] = None,
    ) -> List[str]:
        sym_set = {s.strip().upper() for s in symbols if s.strip()}
        side_up = side.strip().upper() if side else None
        exec_up = execution_type.strip().upper() if execution_type else None
        state = self._ensure_state_file()
        canceled_ids: List[str] = []
        now_str = _now_jst().isoformat(timespec="seconds")

        for o in state.get("orders", []):
            if str(o.get("status", "")).upper() not in ("WAITING", "ORDERED"):
                continue
            if sym_set and str(o.get("symbol", "")).upper() not in sym_set:
                continue
            if side_up and str(o.get("side", "")).upper() != side_up:
                continue
            if exec_up and str(o.get("execution_type", "")).upper() != exec_up:
                continue
            o["status"] = "CANCELED"
            o["remaining_size"] = 0.0
            o["canceled_at"] = now_str
            canceled_ids.append(str(o["order_id"]))

        if canceled_ids:
            self._save_state(state)
        return canceled_ids

    def get_latest_executions(
        self,
        symbol: str,
        page: int = 1,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        if not symbol or not str(symbol).strip():
            raise ValueError("get_latest_executions には有効な symbol 指定が必須です。")
        sym = symbol.strip().upper()
        state = self._ensure_state_file()
        execs = [dict(e) for e in state.get("executions", []) if str(e.get("symbol", "")).upper() == sym]
        execs.reverse()
        return execs[:count]


def create_broker_client(
    execution_mode: Optional[str] = None,
    base_dir: Optional[Path] = None,
    public_client: Optional[GMOPublicClient] = None,
    wal_db: Optional[DatabaseWAL] = None,
) -> Union[GMOPrivateClient, PaperBroker]:
    mode = (execution_mode or os.getenv("EXECUTION_MODE", "DRY_RUN")).strip().upper()
    root = Path(base_dir) if base_dir is not None else Path(__file__).resolve().parent.parent
    if mode == "LIVE":
        return GMOPrivateClient(public_client=public_client, wal_db=wal_db)
    return PaperBroker(
        paper_orders_path=root / "paper_orders.json",
        portfolio_path=root / "portfolio.json",
        public_client=public_client,
        wal_db=wal_db,
    )
