"""
GMOコイン Public API 取得モジュール (gmo_client.py)

対象区分: GMOコイン「取引所（現物・板取引）」のみ
（販売所およびレバレッジ取引の銘柄 `*_JPY` は除外）
"""

from __future__ import annotations

import logging
import ssl
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# コア銘柄およびデフォルトの監視対象銘柄（GMOコイン 取引所・現物）
CORE_SPOT_SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]
DEFAULT_SPOT_SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE"]

# 取扱廃止予定銘柄および超低流動性・特殊銘柄（スクリーニング対象から常時除外）
DELISTING_SPOT_SYMBOLS = {"SAND", "CHZ", "FIL"}
LOW_LIQUIDITY_OR_SPECIAL_SYMBOLS = {"FCR", "NAC", "WILD", "ASTR", "ZPG", "ZPGAG", "ZPGPT"}
EXCLUDED_SPOT_SYMBOLS = DELISTING_SPOT_SYMBOLS | LOW_LIQUIDITY_OR_SPECIAL_SYMBOLS

# 4時間足・日足など日付指定が YYYY（年単位）となるインターバル
YEARLY_INTERVALS = {"4hour", "8hour", "12hour", "1day", "1week", "1month"}
# 1時間足以下など日付指定が YYYYMMDD（日単位）となるインターバル
DAILY_INTERVALS = {"1min", "5min", "10min", "15min", "30min", "1hour"}


class SystemSSLAdapter(HTTPAdapter):
    """OS標準の証明書ストアとcertifiバンドルの双方を信頼するHTTPSアダプタ"""

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any) -> Any:
        ctx = ssl.create_default_context()
        try:
            import certifi

            ctx.load_verify_locations(cafile=certifi.where())
        except Exception:
            pass
        pool_kwargs["ssl_context"] = ctx
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)


class GMOPublicClient:
    """GMOコイン Public API クライアント"""

    BASE_URL = "https://api.coin.z.com/public/v1"

    def __init__(self, timeout: int = 10, max_retries: int = 3, retry_delay: float = 1.0) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.mount("https://", SystemSSLAdapter())
        self.session.headers.update(
            {
                "User-Agent": "gmo-ai-trader/1.0",
                "Accept": "application/json",
            }
        )

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Public API への GET リクエスト（リトライ付き）"""
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if payload.get("status") != 0:
                    messages = payload.get("messages", [])
                    # Phase 22 M-2: /klines で指定日・指定年のローソク足が未生成 (ERR-5207 Not found) の場合は
                    # 通信障害ではないためリトライ待機を行わず即座に空リストを返す
                    if path.lstrip("/") == "klines" and any(
                        isinstance(m, dict) and str(m.get("message_code", "")) == "ERR-5207"
                        for m in (messages if isinstance(messages, list) else [])
                    ):
                        return []
                    raise RuntimeError(f"GMO API エラー応答 (path={path}, params={params}): {messages}")
                return payload.get("data")
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "GMO API 通信リトライ (%d/%d) path=%s params=%s reason=%s",
                    attempt,
                    self.max_retries,
                    path,
                    params,
                    exc,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)

        raise RuntimeError(f"GMO Public API 取得失敗 (path={path}): {last_error}") from last_error

    def get_status(self) -> str:
        """取引所の稼働ステータスを取得する（例: 'OPEN', 'MAINTENANCE'）"""
        data = self._get("/status")
        return str(data.get("status", "UNKNOWN"))

    def get_symbols(self, spot_only: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        取扱銘柄の取引ルール（最小注文数量・呼値・手数料等）を取得する。
        spot_only=True の場合、レバレッジ銘柄（末尾が `_JPY`）を除外し現物取引のみ返す。
        """
        raw_list: List[Dict[str, Any]] = self._get("/symbols") or []
        rules: Dict[str, Dict[str, Any]] = {}
        for item in raw_list:
            symbol = str(item.get("symbol", ""))
            if not symbol:
                continue
            if spot_only and symbol.endswith("_JPY"):
                continue
            rules[symbol] = {
                "symbol": symbol,
                "min_order_size": str(item.get("minOrderSize", "0")),
                "max_order_size": str(item.get("maxOrderSize", "0")),
                "size_step": str(item.get("sizeStep", "0")),
                "tick_size": str(item.get("tickSize", "1")),
                "taker_fee": str(item.get("takerFee", "0")),
                "maker_fee": str(item.get("makerFee", "0")),
            }
        return rules

    def get_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        現物銘柄の最新レート（Ticker）を取得する。
        symbols=None の場合はレバレッジ銘柄（`*_JPY`）を除く全現物銘柄を返す。
        24時間円換算売買代金は必ず `float(volume) * float(last)` で算出する。
        """
        target_set = set(symbols) if symbols else None
        raw_list: List[Dict[str, Any]] = self._get("/ticker") or []
        tickers: Dict[str, Dict[str, Any]] = {}

        for item in raw_list:
            sym = str(item.get("symbol", ""))
            if not sym or sym.endswith("_JPY"):
                continue
            if target_set is not None and sym not in target_set:
                continue
            ask = float(item.get("ask", 0) or 0)
            bid = float(item.get("bid", 0) or 0)
            last = float(item.get("last", 0) or 0)
            volume = float(item.get("volume", 0) or 0)
            spread = ask - bid if (ask > 0 and bid > 0) else 0.0
            spread_pct = ((ask - bid) / bid * 100.0) if (ask > 0 and bid > 0) else 999.0
            # APIの volume（取引枚数）ではなく、必ず float(volume) * float(last) で24時間円換算売買代金を算出
            trading_value_24h = float(volume) * float(last)

            tickers[sym] = {
                "symbol": sym,
                "ask": ask,
                "bid": bid,
                "high": float(item.get("high", 0) or 0),
                "low": float(item.get("low", 0) or 0),
                "last": last,
                "volume": volume,
                "trading_value_24h": round(trading_value_24h, 2),
                "spread": spread,
                "spread_pct": round(spread_pct, 4),
                "timestamp": item.get("timestamp", ""),
            }
        return tickers

    def screen_spot_symbols(
        self,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        all_tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        core_symbols: Optional[List[str]] = None,
        max_spread_pct: float = 0.35,
        min_count: int = 8,
        max_count: int = 10,
    ) -> Dict[str, Any]:
        """
        GMOコインの全現物銘柄から以下の基準で分析対象銘柄（計8〜10銘柄）を動的にスクリーニングする:
        1. `/v1/symbols` の現物銘柄（`_JPY` 除外）から、取扱廃止予定銘柄（SAND, CHZ, FIL）および
           超低流動性・特殊銘柄（FCR, NAC, WILD, ASTR, ZPG, ZPGAG, ZPGPT）を除外
        2. コア銘柄（BTC, ETH, SOL, XRP）を優先採用
        3. スプレッド比率 `((ask - bid) / bid) * 100 <= max_spread_pct (0.35%)` を満たす銘柄を抽出
        4. 24時間円換算売買代金 (`float(volume) * float(last)`) の降順でソートし、上位銘柄を選定
        """
        rules = symbol_rules if symbol_rules is not None else self.get_symbols(spot_only=True)
        spot_symbols = list(rules.keys())
        tickers = all_tickers if all_tickers is not None else self.get_tickers(spot_symbols)
        cores = [
            s.upper()
            for s in (core_symbols or CORE_SPOT_SYMBOLS)
            if s.upper() in rules and s.upper() not in EXCLUDED_SPOT_SYMBOLS
        ]

        selected: List[str] = []
        rejected: List[Dict[str, Any]] = []
        candidates: List[Dict[str, Any]] = []

        def _calc_jpy_trading_value(t_dict: Dict[str, Any]) -> float:
            vol = float(t_dict.get("volume", 0) or 0)
            lst = float(t_dict.get("last", 0) or 0)
            if vol > 0 and lst > 0:
                return float(vol) * float(lst)
            return float(t_dict.get("trading_value_24h", 0.0) or 0.0)

        # コア銘柄をまず確保
        for sym in cores:
            t = tickers.get(sym)
            if t and float(t.get("last", 0) or 0) > 0 and sym not in selected:
                t["trading_value_24h"] = round(_calc_jpy_trading_value(t), 2)
                selected.append(sym)

        # 残りの現物銘柄を除外リスト・スプレッド比率でフィルタリングし、円換算売買代金順にソート
        for sym in spot_symbols:
            if sym in selected:
                continue
            if sym in DELISTING_SPOT_SYMBOLS:
                rejected.append({"symbol": sym, "reason": "取扱廃止予定銘柄のため除外"})
                continue
            if sym in LOW_LIQUIDITY_OR_SPECIAL_SYMBOLS:
                rejected.append({"symbol": sym, "reason": "超低流動性・特殊銘柄のため除外"})
                continue

            t = tickers.get(sym)
            if not t or float(t.get("last", 0) or 0) <= 0 or float(t.get("bid", 0) or 0) <= 0:
                rejected.append({"symbol": sym, "reason": "価格データなし"})
                continue

            ask = float(t.get("ask", 0) or 0)
            bid = float(t.get("bid", 0) or 0)
            spread_pct = (
                float(t["spread_pct"])
                if "spread_pct" in t and t["spread_pct"] is not None
                else (((ask - bid) / bid * 100.0) if (ask > 0 and bid > 0) else 999.0)
            )
            tv_24h = _calc_jpy_trading_value(t)
            t["spread_pct"] = round(spread_pct, 4)
            t["trading_value_24h"] = round(tv_24h, 2)

            if spread_pct <= max_spread_pct:
                candidates.append(t)
            else:
                rejected.append(
                    {
                        "symbol": sym,
                        "spread_pct": spread_pct,
                        "trading_value_24h": tv_24h,
                        "reason": f"スプレッド超過 ({spread_pct:.3f}% > {max_spread_pct:.2f}%)",
                    }
                )

        candidates.sort(key=_calc_jpy_trading_value, reverse=True)

        for cand in candidates:
            if len(selected) >= max_count:
                break
            selected.append(str(cand["symbol"]))

        # 万一フィルタ通過銘柄が少なく min_count 未満の場合、除外リスト外の銘柄から売買代金上位を補充
        if len(selected) < min_count:
            fallback_pool = [
                tickers[r["symbol"]]
                for r in rejected
                if r["symbol"] not in EXCLUDED_SPOT_SYMBOLS
                and r["symbol"] in tickers
                and float(tickers[r["symbol"]].get("last", 0) or 0) > 0
            ]
            fallback_pool.sort(key=_calc_jpy_trading_value, reverse=True)
            for fb in fallback_pool:
                if len(selected) >= min_count:
                    break
                sym = str(fb["symbol"])
                if sym not in selected:
                    selected.append(sym)

        selected_details = [
            {
                "symbol": s,
                "is_core": s in cores,
                "last": tickers[s]["last"],
                "spread_pct": tickers[s].get("spread_pct", 0.0),
                "trading_value_24h": _calc_jpy_trading_value(tickers[s]),
            }
            for s in selected
            if s in tickers
        ]

        return {
            "total_spot_symbols": len(spot_symbols),
            "max_spread_pct": max_spread_pct,
            "selected_symbols": selected,
            "selected_details": selected_details,
            "rejected_symbols": rejected,
        }

    def get_orderbook(self, symbol: str, depth: int = 5) -> Dict[str, Any]:
        """
        指定銘柄の板情報（Orderbook）を取得し、上位気配および±1%圏内の板の厚み（円換算流動性）を集計する。
        ※板インバランスは17時時点の一瞬のスナップショットであり見せ板ノイズを含むため、
          方向予測の主根拠とせず「±1%圏内の板の厚み（流動性・スリッページ確認）」として扱う。
        """
        data = self._get("/orderbooks", params={"symbol": symbol}) or {}
        all_asks_raw = data.get("asks", []) or []
        all_bids_raw = data.get("bids", []) or []
        asks_raw = all_asks_raw[:depth]
        bids_raw = all_bids_raw[:depth]

        asks = [{"price": float(a["price"]), "size": float(a["size"])} for a in asks_raw]
        bids = [{"price": float(b["price"]), "size": float(b["size"])} for b in bids_raw]

        ask_depth_size = sum(a["size"] for a in asks)
        bid_depth_size = sum(b["size"] for b in bids)
        best_ask = asks[0]["price"] if asks else 0.0
        best_bid = bids[0]["price"] if bids else 0.0
        mid_price = (best_ask + best_bid) / 2.0 if (best_ask > 0 and best_bid > 0) else (best_ask or best_bid)

        # ±1%圏内の板の厚み（円換算および数量：流動性・スリッページ確認用）
        ask_depth_1pct_jpy = 0.0
        bid_depth_1pct_jpy = 0.0
        if mid_price > 0:
            upper_1pct = mid_price * 1.01
            lower_1pct = mid_price * 0.99
            for a in all_asks_raw:
                p = float(a.get("price", 0) or 0)
                s = float(a.get("size", 0) or 0)
                if 0 < p <= upper_1pct:
                    ask_depth_1pct_jpy += p * s
            for b in all_bids_raw:
                p = float(b.get("price", 0) or 0)
                s = float(b.get("size", 0) or 0)
                if p >= lower_1pct:
                    bid_depth_1pct_jpy += p * s

        imbalance = 0.0
        total_depth = ask_depth_size + bid_depth_size
        if total_depth > 0:
            imbalance = (bid_depth_size - ask_depth_size) / total_depth

        return {
            "symbol": symbol,
            "best_ask": best_ask,
            "best_bid": best_bid,
            "asks": asks,
            "bids": bids,
            "ask_depth_size": ask_depth_size,
            "bid_depth_size": bid_depth_size,
            "ask_depth_1pct_jpy": round(ask_depth_1pct_jpy, 0),
            "bid_depth_1pct_jpy": round(bid_depth_1pct_jpy, 0),
            "orderbook_imbalance": round(imbalance, 4),
        }

    def get_klines(
        self,
        symbol: str,
        interval: str = "1day",
        limit: int = 100,
        reference_dt: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        指定銘柄のローソク足（Klines）を取得し、時系列昇順の DataFrame として返す。
        年またぎ・日またぎやシステム時計のズレにも対応し、十分な本数が集まるまで遡って結合する。
        """
        if interval not in YEARLY_INTERVALS and interval not in DAILY_INTERVALS:
            raise ValueError(f"未対応のインターバルです: {interval}")

        now_jst = reference_dt or datetime.now(JST)
        # Phase 22 M-2: GMOコインのKlines日付区分 (YYYY / YYYYMMDD) は朝06:00 JSTに切り替わるため、
        # 00:00〜05:59 JSTの間はGMO取引日基準 (now_jst - 6時間) を起点にして未生成日への無駄なリクエストを防ぐ
        trading_dt = now_jst - timedelta(hours=6) if now_jst.hour < 6 else now_jst
        collected_rows: List[Dict[str, Any]] = []

        if interval in YEARLY_INTERVALS:
            # 当年から最大4年前まで遡ってデータを収集（システム時計が未来年の場合もカバー）
            start_year = trading_dt.year
            for year_offset in range(0, 5):
                target_date = f"{start_year - year_offset:04d}"
                try:
                    rows = self._get(
                        "/klines",
                        params={"symbol": symbol, "interval": interval, "date": target_date},
                    )
                    if rows:
                        collected_rows.extend(rows)
                        if len(collected_rows) >= limit:
                            break
                except Exception as exc:
                    logger.debug("Klines 取得スキップ (symbol=%s, date=%s): %s", symbol, target_date, exc)
        else:
            # 日単位指定インターバルは直近から遡って取得
            for day_offset in range(0, 10):
                target_dt = trading_dt - timedelta(days=day_offset)
                target_date = target_dt.strftime("%Y%m%d")
                try:
                    rows = self._get(
                        "/klines",
                        params={"symbol": symbol, "interval": interval, "date": target_date},
                    )
                    if rows:
                        collected_rows.extend(rows)
                        if len(collected_rows) >= limit:
                            break
                except Exception as exc:
                    logger.debug("Klines 取得スキップ (symbol=%s, date=%s): %s", symbol, target_date, exc)

        if not collected_rows:
            return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(collected_rows)
        df = df.rename(columns={"openTime": "open_time"})
        df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["open_time", "close"]).drop_duplicates(subset=["open_time"])
        df = df.sort_values("open_time", ascending=True).reset_index(drop=True)
        df["datetime_jst"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(JST)

        if len(df) > limit:
            df = df.iloc[-limit:].reset_index(drop=True)

        return df
