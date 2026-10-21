"""
Public/Private WebSocket ＋ 4フェーズ先行購読バッファリング同期エンジン (ws_hybrid_engine.py)

【コア仕様】
1. Private WS トークンの 35分周期自動延長 (`PUT /private/v1/ws-auth`):
   - 延長失敗時は直ちに `POST /private/v1/ws-auth` で新トークンを再取得し、4フェーズ再同期を実行。
2. 1.2秒間隔のシリアル購読レート制限遵守 (`SUBSCRIPTION_INTERVAL_SEC = 1.2`):
   - Public / Private WS の `subscribe` リクエスト送信時に必ず 1.2秒間隔を空け、`ERR-5003` 切断を防止。
3. 75秒無音検知 (`SILENCE_TIMEOUT_SEC = 75.0`) ＋ 能動 Ping & 自動再接続。
4. 4フェーズ先行購読バッファリング同期 (`run_four_phase_sync`):
   - Phase 1: `SyncState.SYNCING` に設定し、WS購読を完了させて受信イベントを `ws_event_buffer` に退避開始。
   - Phase 2: REST で `assets`, `activeOrders`, `latestExecutions`、および「ローカルで未終端なのに `activeOrders` から消えた注文の `GET /v1/orders`」を一括取得。
   - Phase 3: `executionId` の単調増加ハイウォーターマークと `orders` の状態ラティスで REST データをべき等適用。
   - Phase 4: `ws_event_buffer` を先頭から順番にドレインして同じべき等関数に通し、完了後に `SyncState.LIVE` へ遷移。
"""

from __future__ import annotations

import collections
import enum
import json
import logging
import time
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Union

from src.database_wal import DatabaseWAL
from src.gmo_private_client import GMOPrivateClient, PaperBroker

logger = logging.getLogger(__name__)


class SyncState(str, enum.Enum):
    DISCONNECTED = "DISCONNECTED"
    SYNCING = "SYNCING"
    LIVE = "LIVE"
    RECONNECTING = "RECONNECTING"


class WSHybridEngine:
    """Public/Private WebSocket ＆ 4フェーズ先行購読バッファリング同期エンジン"""

    PUBLIC_WS_URL = "wss://api.coin.z.com/ws/public/v1"
    PRIVATE_WS_URL_BASE = "wss://api.coin.z.com/ws/private/v1"

    TOKEN_REFRESH_INTERVAL_SEC = 35 * 60.0  # 35分周期 (有効期限60分に対し25分の安全マージン)
    SUBSCRIPTION_INTERVAL_SEC = 1.2         # 1.2秒間隔シリアル購読 (1 req/sec 制限遵守)
    SILENCE_TIMEOUT_SEC = 75.0              # 75秒無音検知で能動Ping / 再接続

    def __init__(
        self,
        broker: Union[GMOPrivateClient, PaperBroker],
        wal_db: DatabaseWAL,
        watched_symbols: Optional[List[str]] = None,
        subscription_interval_sec: float = 1.2,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.broker = broker
        self.wal_db = wal_db
        self.watched_symbols: List[str] = [s.strip().upper() for s in (watched_symbols or ["BTC", "ETH", "SOL", "XRP"])]
        self.subscription_interval_sec = max(0.0, float(subscription_interval_sec))
        self._sleep = sleep_fn or time.sleep

        self.sync_state: SyncState = SyncState.DISCONNECTED
        self.ws_token: str = ""
        self.token_obtained_ts: float = 0.0
        self.last_message_ts: float = time.monotonic()
        self.last_subscribe_ts: float = 0.0
        self.ping_sent_count: int = 0
        self.reconnect_count: int = 0
        self.malformed_message_count: int = 0

        self.ws_event_buffer: Deque[Dict[str, Any]] = collections.deque()
        self.subscribed_private_channels: List[str] = []
        self.subscribed_public_channels: List[Dict[str, Any]] = []
        self.subscription_timestamps: List[float] = []
        self.phase_trace: List[str] = []
        self.high_watermark_execution_id: int = self.wal_db.get_high_watermark_execution_id()

    # ------------------------------------------------------------------
    # 1. トークン管理 (35分周期自動延長 PUT ➔ 失敗時 POST 再取得)
    # ------------------------------------------------------------------
    def obtain_or_refresh_token(self, force_new: bool = False, now_mono: Optional[float] = None) -> str:
        now = float(now_mono if now_mono is not None else time.monotonic())
        if not force_new and self.ws_token:
            elapsed = now - self.token_obtained_ts
            if elapsed < self.TOKEN_REFRESH_INTERVAL_SEC:
                return self.ws_token
            # 35分経過: PUT /private/v1/ws-auth で延長を試行
            try:
                ok = self.broker.extend_ws_auth_token(self.ws_token)
                if ok:
                    self.token_obtained_ts = now
                    logger.info("Private WS トークンを延長しました (PUT /v1/ws-auth)")
                    return self.ws_token
            except Exception as exc:
                logger.warning("Private WS トークン延長 (PUT) 失敗のため POST 再取得へフォールバックします: %s", exc)

        # 新規取得またはフォールバック再取得 (POST /private/v1/ws-auth)
        new_token = self.broker.create_ws_auth_token()
        if not new_token:
            raise RuntimeError("Private WS トークンの取得 (POST /v1/ws-auth) に失敗しました。")
        self.ws_token = new_token
        self.token_obtained_ts = now
        return self.ws_token

    # ------------------------------------------------------------------
    # 2. 1.2秒間隔シリアル購読
    # ------------------------------------------------------------------
    def build_public_subscription_payloads(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Public WS 購読ペイロードを生成する。
        `trades` チャンネルは必ず `"option": "TAKER_ONLY"` を指定して約定の二重カウントを防止する。
        """
        syms = [s.strip().upper() for s in (symbols or self.watched_symbols)]
        payloads: List[Dict[str, Any]] = []
        for sym in syms:
            payloads.append({"command": "subscribe", "channel": "orderbooks", "symbol": sym})
            payloads.append({"command": "subscribe", "channel": "trades", "symbol": sym, "option": "TAKER_ONLY"})
        return payloads

    def subscribe_channels_serially(
        self,
        private_channels: Optional[List[str]] = None,
        public_symbols: Optional[List[str]] = None,
        send_fn: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> List[Dict[str, Any]]:
        """
        1.2秒以上の間隔 (`self.subscription_interval_sec`) を厳守して購読コマンドを直列送信する。
        """
        priv_chans = private_channels if private_channels is not None else ["executionEvents", "orderEvents"]
        pub_payloads = self.build_public_subscription_payloads(public_symbols)

        sent_messages: List[Dict[str, Any]] = []
        all_requests: List[Dict[str, Any]] = [
            {"command": "subscribe", "channel": ch} for ch in priv_chans
        ] + pub_payloads

        for idx, req in enumerate(all_requests):
            if idx > 0 and self.subscription_interval_sec > 0:
                self._sleep(self.subscription_interval_sec)
            ts = time.monotonic()
            self.last_subscribe_ts = ts
            self.subscription_timestamps.append(ts)
            if send_fn is not None:
                send_fn(req)
            sent_messages.append(req)

        self.subscribed_private_channels = list(priv_chans)
        self.subscribed_public_channels = list(pub_payloads)
        return sent_messages

    # ------------------------------------------------------------------
    # 3. 75秒無音検知 ＆ 能動 Ping / 再接続判定
    # ------------------------------------------------------------------
    def check_heartbeat(
        self,
        now_mono: Optional[float] = None,
        ping_fn: Optional[Callable[[], bool]] = None,
    ) -> str:
        """
        最後のメッセージ受信から 75秒以上経過していないか検査する。
        - 75秒未満: `"ALIVE"`
        - 75秒以上経過し Ping 成功: `"PING_SENT"`
        - 75秒以上経過し Ping 失敗または応答なし: `"RECONNECT_REQUIRED"`
        """
        now = float(now_mono if now_mono is not None else time.monotonic())
        silence_sec = now - self.last_message_ts
        if silence_sec < self.SILENCE_TIMEOUT_SEC:
            return "ALIVE"

        self.ping_sent_count += 1
        if ping_fn is not None:
            try:
                if ping_fn():
                    self.last_message_ts = now
                    return "PING_SENT"
            except Exception:
                pass
        self.sync_state = SyncState.RECONNECTING
        return "RECONNECT_REQUIRED"

    # ------------------------------------------------------------------
    # 4. 4フェーズ先行購読バッファリング同期 (Phase 1 -> Phase 2 -> Phase 3 -> Phase 4)
    # ------------------------------------------------------------------
    def on_raw_ws_message(self, raw_msg: Union[str, bytes, Dict[str, Any]]) -> Dict[str, Any]:
        """
        WebSocket から受信した生メッセージを処理する。
        - 不正な JSON フレームは例外でループを落とさず安全に破棄してカウント。
        - `SyncState.SYNCING` 中は `ws_event_buffer` に退避。
        - `SyncState.LIVE` 中は即座にべき等適用関数 (`apply_ws_event`) に通す。
        """
        self.last_message_ts = time.monotonic()
        if isinstance(raw_msg, dict):
            event = raw_msg
        else:
            try:
                text = raw_msg.decode("utf-8") if isinstance(raw_msg, bytes) else str(raw_msg)
                event = json.loads(text)
                if not isinstance(event, dict):
                    self.malformed_message_count += 1
                    return {"status": "IGNORED_MALFORMED"}
            except Exception:
                self.malformed_message_count += 1
                logger.warning("不正な WS JSON フレームを破棄しました: %r", raw_msg)
                return {"status": "IGNORED_MALFORMED"}

        if self.sync_state != SyncState.LIVE:
            self.ws_event_buffer.append(event)
            return {"status": "BUFFERED", "buffer_size": len(self.ws_event_buffer)}

        applied = self.apply_ws_event(event)
        return {"status": "APPLIED_LIVE", "detail": applied}

    def apply_ws_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        単一の WS イベント (`executionEvents` / `orderEvents`) を
        `executionId` ハイウォーターマークと `orders` 状態ラティスに従ってべき等に SQLite WAL へ適用する。
        """
        channel = str(event.get("channel", "")).strip()
        sym = str(event.get("symbol", "")).strip().upper()

        # 1) executionEvents (約定イベント)
        if channel == "executionEvents" or "executionId" in event or "execution_id" in event:
            exec_id = str(event.get("executionId", event.get("execution_id", ""))).strip()
            order_id = str(event.get("orderId", event.get("order_id", ""))).strip()
            side = str(event.get("side", "BUY")).strip().upper()
            exec_type = str(event.get("executionType", event.get("execution_type", "LIMIT"))).strip().upper()
            size = event.get("size", event.get("executionSize", 0))
            price = event.get("price", event.get("executionPrice", 0))
            fee = event.get("fee", 0)

            inserted, row = self.wal_db.record_execution(
                execution_id=exec_id,
                order_id=order_id,
                symbol=sym,
                side=side,
                size=size,
                price=price,
                fee_jpy=fee,
                execution_type=exec_type,
                raw_data=event,
            )
            if exec_id.isdigit():
                self.high_watermark_execution_id = max(self.high_watermark_execution_id, int(exec_id))

            # 注文状態もあればラティス更新
            order_status = str(event.get("orderStatus", event.get("status", ""))).strip().upper()
            if order_id and order_status:
                self.wal_db.upsert_order(
                    order_id=order_id,
                    symbol=sym,
                    side=side,
                    execution_type=exec_type,
                    state=order_status,
                    price=event.get("orderPrice", price),
                    size=event.get("orderSize", size),
                    executed_size=event.get("orderExecutedSize", size),
                    raw_data=event,
                )
            return {"type": "EXECUTION", "execution_id": exec_id, "inserted": inserted}

        # 2) orderEvents (注文イベント)
        if channel == "orderEvents" or "orderId" in event or "order_id" in event:
            order_id = str(event.get("orderId", event.get("order_id", ""))).strip()
            order_status = str(event.get("orderStatus", event.get("status", "ORDERED"))).strip().upper()
            side = str(event.get("side", "BUY")).strip().upper()
            exec_type = str(event.get("executionType", event.get("execution_type", "LIMIT"))).strip().upper()
            updated = self.wal_db.upsert_order(
                order_id=order_id,
                symbol=sym,
                side=side,
                execution_type=exec_type,
                state=order_status,
                price=event.get("orderPrice", event.get("price", 0)),
                size=event.get("orderSize", event.get("size", 0)),
                executed_size=event.get("orderExecutedSize", event.get("executed_size", 0)),
                time_in_force=str(event.get("timeInForce", event.get("time_in_force", "SOK"))),
                raw_data=event,
            )
            return {"type": "ORDER", "order_id": order_id, "updated": updated}

        return {"type": "PUBLIC_OR_OTHER", "channel": channel}

    def run_four_phase_sync(
        self,
        symbols: Optional[List[str]] = None,
        on_phase1_ready_hook: Optional[Callable[["WSHybridEngine"], None]] = None,
    ) -> Dict[str, Any]:
        """
        起動時・再接続時の 4フェーズ先行購読バッファリング同期を実行する:
        - Phase 1: `SyncState.SYNCING` に設定し、WS購読を完了させて受信イベントを `ws_event_buffer` に退避開始。
        - Phase 2: REST で `assets`, `activeOrders`, `latestExecutions`、および
                   ローカルで未終端なのに `activeOrders` から消えた注文の `GET /v1/orders` を一括取得。
        - Phase 3: `executionId` の単調増加ハイウォーターマークと `orders` の状態ラティスで REST データをべき等適用。
        - Phase 4: `ws_event_buffer` を先頭から順番にドレインして同じべき等関数に通し、完了後に `SyncState.LIVE` へ遷移。
        """
        target_symbols = [s.strip().upper() for s in (symbols or self.watched_symbols)]
        self.phase_trace = []

        # ------------------------------------------------------------------
        # Phase 1: SYNCING 設定 ＆ 先行 WS 購読完了 ＆ バッファリング開始
        # ------------------------------------------------------------------
        self.sync_state = SyncState.SYNCING
        self.phase_trace.append("PHASE_1_SYNCING_AND_SUBSCRIBED")
        self.obtain_or_refresh_token(force_new=not bool(self.ws_token))
        self.subscribe_channels_serially(
            private_channels=["executionEvents", "orderEvents"],
            public_symbols=target_symbols,
        )
        # テスト・カオス監査で Phase 1〜Phase 2 の間に到着する WS イベントを注入できるフック
        if on_phase1_ready_hook is not None:
            on_phase1_ready_hook(self)

        # ------------------------------------------------------------------
        # Phase 2: REST スナップショット一括取得 (消えた未終端注文の個別照会を含む)
        # ------------------------------------------------------------------
        self.phase_trace.append("PHASE_2_REST_SNAPSHOT_FETCH")
        rest_assets = self.broker.get_assets()
        rest_active_orders: List[Dict[str, Any]] = []
        seen_order_ids: Set[str] = set()
        for sym in target_symbols:
            for ao in self.broker.get_active_orders(sym):
                oid = str(ao.get("order_id", ""))
                if oid and oid not in seen_order_ids:
                    seen_order_ids.add(oid)
                    rest_active_orders.append(ao)

        # ローカル DB で未終端 (state_rank < 90) なのに REST activeOrders に存在しない注文を特定して GET /v1/orders 照会
        local_open_orders = self.wal_db.get_active_orders_in_db()
        missing_order_ids = [
            str(lo["order_id"])
            for lo in local_open_orders
            if str(lo["order_id"]) not in seen_order_ids
        ]
        rest_missing_orders: List[Dict[str, Any]] = []
        if missing_order_ids:
            for i in range(0, len(missing_order_ids), 10):
                batch_ids = missing_order_ids[i : i + 10]
                rest_missing_orders.extend(self.broker.get_orders(batch_ids))

        rest_executions: List[Dict[str, Any]] = []
        for sym in target_symbols:
            rest_executions.extend(self.broker.get_latest_executions(sym))

        # ------------------------------------------------------------------
        # Phase 3: REST スナップショットのべき等適用 (executionId 昇順 ＆ 注文ラティス)
        # ------------------------------------------------------------------
        self.phase_trace.append("PHASE_3_APPLY_REST_IDEMPOTENT")
        rest_executions_sorted = sorted(
            rest_executions,
            key=lambda e: int(str(e.get("execution_id", "0"))) if str(e.get("execution_id", "")).isdigit() else 0,
        )
        rest_exec_inserted = 0
        for ex in rest_executions_sorted:
            eid = str(ex.get("execution_id", ex.get("executionId", ""))).strip()
            if not eid:
                continue
            inserted, _ = self.wal_db.record_execution(
                execution_id=eid,
                order_id=str(ex.get("order_id", ex.get("orderId", ""))),
                symbol=str(ex.get("symbol", "")),
                side=str(ex.get("side", "BUY")),
                size=ex.get("size", 0),
                price=ex.get("price", 0),
                fee_jpy=ex.get("fee", 0),
                execution_type=str(ex.get("execution_type", ex.get("executionType", "LIMIT"))),
                raw_data=ex,
            )
            if inserted:
                rest_exec_inserted += 1
            if eid.isdigit():
                self.high_watermark_execution_id = max(self.high_watermark_execution_id, int(eid))

        rest_orders_updated = 0
        for ord_item in rest_active_orders + rest_missing_orders:
            oid = str(ord_item.get("order_id", ord_item.get("orderId", ""))).strip()
            if not oid:
                continue
            ok = self.wal_db.upsert_order(
                order_id=oid,
                symbol=str(ord_item.get("symbol", "")),
                side=str(ord_item.get("side", "BUY")),
                execution_type=str(ord_item.get("execution_type", ord_item.get("executionType", "LIMIT"))),
                state=str(ord_item.get("status", "ORDERED")),
                price=ord_item.get("price", 0),
                size=ord_item.get("size", 0),
                executed_size=ord_item.get("executed_size", ord_item.get("executedSize", 0)),
                time_in_force=str(ord_item.get("time_in_force", ord_item.get("timeInForce", "SOK"))),
                raw_data=ord_item,
            )
            if ok:
                rest_orders_updated += 1

        # ------------------------------------------------------------------
        # Phase 4: ws_event_buffer の FIFO ドレイン適用 ➔ SyncState.LIVE 遷移
        # ------------------------------------------------------------------
        self.phase_trace.append("PHASE_4_DRAIN_WS_BUFFER_AND_GO_LIVE")
        drained_count = 0
        while self.ws_event_buffer:
            buffered_ev = self.ws_event_buffer.popleft()
            self.apply_ws_event(buffered_ev)
            drained_count += 1

        self.sync_state = SyncState.LIVE
        return {
            "status": "LIVE",
            "sync_state": self.sync_state.value,
            "phase_trace": list(self.phase_trace),
            "rest_assets_count": len(rest_assets),
            "rest_active_orders_count": len(rest_active_orders),
            "rest_missing_orders_queried": len(rest_missing_orders),
            "rest_executions_inserted": rest_exec_inserted,
            "rest_orders_updated": rest_orders_updated,
            "ws_buffer_drained_count": drained_count,
            "high_watermark_execution_id": self.high_watermark_execution_id,
        }
