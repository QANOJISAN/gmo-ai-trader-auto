"""
3段階分離リモート緊急制御・ワンタイム nonce 2段階承認・通知3階層モジュール (remote_control.py)

1. パニックコマンドの3段階分離 (`RemoteControlManager`):
   - `PAUSE`: 新規 `BUY` 発注のみ一時停止。保有中ポジションおよび取引所の有効な `STOP` 逆指値注文は絶対にキャンセルせず維持する。
   - `CANCEL_ENTRIES`: 未約定の `BUY` 指値注文 (`PENDING_BUY` / `ENTRY_PENDING`) のみ全キャンセル。保有中ポジションと `STOP` 注文は維持する。
   - `FLATTEN_ALL` (2段階承認必須):
     * Step 1 (`FLATTEN_REQUEST` / `request_flatten_all`): ワンタイム `nonce` トークン（有効期限 180秒）を発行し、
       `FLATTEN_CONFIRM <nonce>` の入力を求める確認応答を返す（単発の誤タップでは絶対に全決済しない）。
     * Step 2 (`FLATTEN_CONFIRM <nonce>` / `confirm_flatten_all`):
       SQLite `control_commands` テーブルの `nonce UNIQUE` 制約と有効期限・発行者IDを検証し、
       一度だけ全待機注文キャンセル＋全保有現物ポジションの緊急成行決済を実行する（同一 `nonce` のリプレイは完全拒否）。
   - すべてのリモートコマンドは `ALLOWED_OWNER_DISCORD_ID`（設定されている場合）と `actor_id` の一致検証を必須とする。

2. 通知3階層フィルタ (`TieredNotificationDispatcher`):
   - `[CRITICAL]`: SL未保護・キルスイッチ発動・認証停止 (`ERR-5010〜5012`)・3者照合不一致 ➔ `@everyone` / オーナーメンション付き即時送信
   - `[TRADE]`: 新規指値・約定・トレーリングSL更新・TP/SL決済 ➔ 通常即時送信
   - `[DAILY_DIGEST]`: 15分ごとの `HOLD` や定期ヘルスチェック・再接続ログは即時通知を抑制してバッファリングし、日次ダイジェストとして集約送信（通知スパム・オオカミ少年化を完全防止）。
"""

from __future__ import annotations

import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.database_wal import DatabaseWAL

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

NOTIFICATION_TIERS = ("CRITICAL", "TRADE", "DAILY_DIGEST")


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


class UnauthorizedCommandError(PermissionError):
    """許可されたオーナーID以外からのリモートコマンドを拒否する例外"""


class ReplayOrExpiredNonceError(ValueError):
    """使用済み・未発行・または有効期限切れのワンタイム nonce を拒否する例外"""


class TieredNotificationDispatcher:
    """
    通知を `[CRITICAL]`, `[TRADE]`, `[DAILY_DIGEST]` の3階層に分類し、
    15分サイクルの `HOLD` や軽微なログで通知がスパム化することを防ぐディスパッチャ。
    """

    MAX_DIGEST_BUFFER = 500
    MAX_SENT_HISTORY = 500

    def __init__(
        self,
        notifier: Optional[Any] = None,
        owner_discord_id: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        self.notifier = notifier
        self.owner_discord_id = (owner_discord_id if owner_discord_id is not None else os.getenv("ALLOWED_OWNER_DISCORD_ID", "")).strip()
        self.dry_run = bool(dry_run)
        self.digest_buffer: List[Dict[str, str]] = []
        self.sent_history: List[Dict[str, Any]] = []

    def dispatch(
        self,
        tier: str,
        title: str,
        message: str,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        tier_up = str(tier or "DAILY_DIGEST").strip().upper()
        if tier_up not in NOTIFICATION_TIERS:
            tier_up = "DAILY_DIGEST"

        if tier_up == "DAILY_DIGEST":
            entry = {
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "tier": tier_up,
                "title": title,
                "message": message,
            }
            self.digest_buffer.append(entry)
            if len(self.digest_buffer) > self.MAX_DIGEST_BUFFER:
                self.digest_buffer = self.digest_buffer[-self.MAX_DIGEST_BUFFER :]
            return {
                "sent_immediately": False,
                "tier": tier_up,
                "buffered_count": len(self.digest_buffer),
            }

        prefix = f"[{tier_up}]"
        mention = f"<@{self.owner_discord_id}> " if (tier_up == "CRITICAL" and self.owner_discord_id) else ""
        formatted_title = f"{prefix} {title}"
        formatted_msg = f"{mention}{message}" if mention else message

        if self.notifier is not None:
            level = "ERROR" if tier_up == "CRITICAL" else "INFO"
            self.notifier.notify_event(
                title=formatted_title,
                message=formatted_msg,
                level=level,
                dry_run=self.dry_run,
            )

        record = {
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "tier": tier_up,
            "title": formatted_title,
            "message": formatted_msg,
            "sent_immediately": True,
        }
        self.sent_history.append(record)
        if len(self.sent_history) > self.MAX_SENT_HISTORY:
            self.sent_history = self.sent_history[-self.MAX_SENT_HISTORY :]
        return record

    def flush_daily_digest(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        count = len(self.digest_buffer)
        if count == 0:
            return {"flushed_count": 0, "sent": False}

        lines = [f"・[{item['timestamp']}] {item['title']}: {item['message']}" for item in self.digest_buffer[-20:]]
        summary_msg = f"過去集計件数: {count} 件\n" + "\n".join(lines)
        self.digest_buffer.clear()

        if self.notifier is not None:
            self.notifier.notify_event(
                title="[DAILY_DIGEST] 日次稼働ダイジェスト・ハートビート報告",
                message=summary_msg,
                level="INFO",
                dry_run=self.dry_run,
            )
        record = {
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "tier": "DAILY_DIGEST",
            "flushed_count": count,
            "sent": True,
            "summary": summary_msg,
        }
        self.sent_history.append(record)
        if len(self.sent_history) > self.MAX_SENT_HISTORY:
            self.sent_history = self.sent_history[-self.MAX_SENT_HISTORY :]
        return record


class RemoteControlManager:
    """
    `PAUSE`, `CANCEL_ENTRIES`, `FLATTEN_ALL`（2段階 nonce 承認）を管理するリモート緊急制御クラス
    """

    MAX_CONSUMED_NONCES = 2000
    MAX_PENDING_NONCES = 500

    def __init__(
        self,
        oco_manager: Any,
        db_wal: Optional[DatabaseWAL] = None,
        allowed_owner_discord_id: Optional[str] = None,
        nonce_ttl_seconds: int = 180,
        dispatcher: Optional[TieredNotificationDispatcher] = None,
    ) -> None:
        self.oco_manager = oco_manager
        self.db_wal = db_wal or getattr(oco_manager, "db_wal", None)
        self.allowed_owner_discord_id = (
            allowed_owner_discord_id
            if allowed_owner_discord_id is not None
            else os.getenv("ALLOWED_OWNER_DISCORD_ID", "")
        ).strip()
        self.nonce_ttl_seconds = int(nonce_ttl_seconds)
        self.dispatcher = dispatcher or TieredNotificationDispatcher(
            notifier=getattr(oco_manager, "notifier", None),
            owner_discord_id=self.allowed_owner_discord_id,
            dry_run=getattr(oco_manager, "dry_run_notify", True),
        )

        self.is_paused: bool = False
        try:
            pf_init = self.oco_manager.load_portfolio()
            if isinstance(pf_init, dict):
                self.is_paused = bool(pf_init.get("is_paused", False))
        except Exception:
            pass
        # (nonce -> {"actor_id": str, "expires_at": datetime, "consumed": bool})
        self._pending_flatten_nonces: Dict[str, Dict[str, Any]] = {}
        self._consumed_nonces: set[str] = set()
        self._consumed_nonces_order: List[str] = []

    def _record_consumed_nonce(self, nonce: str) -> None:
        if nonce not in self._consumed_nonces:
            self._consumed_nonces.add(nonce)
            self._consumed_nonces_order.append(nonce)
            while len(self._consumed_nonces_order) > self.MAX_CONSUMED_NONCES:
                oldest = self._consumed_nonces_order.pop(0)
                self._consumed_nonces.discard(oldest)

    def _prune_pending_nonces(self, now_jst: datetime) -> None:
        if len(self._pending_flatten_nonces) <= self.MAX_PENDING_NONCES:
            return
        expired_keys = [
            k
            for k, v in self._pending_flatten_nonces.items()
            if v.get("consumed") or (isinstance(v.get("expires_at"), datetime) and _now_jst(v["expires_at"]) < now_jst)
        ]
        for k in expired_keys:
            self._pending_flatten_nonces.pop(k, None)
        while len(self._pending_flatten_nonces) > self.MAX_PENDING_NONCES:
            oldest_key = next(iter(self._pending_flatten_nonces))
            self._pending_flatten_nonces.pop(oldest_key, None)

    def _verify_owner(self, actor_id: Optional[str]) -> str:
        actor = str(actor_id or "").strip()
        if self.allowed_owner_discord_id:
            if actor != self.allowed_owner_discord_id:
                raise UnauthorizedCommandError(
                    f"未許可のユーザーIDからのコマンドを拒否しました: actor_id='{actor}' != ALLOWED_OWNER_DISCORD_ID"
                )
        return actor or "LOCAL_OWNER"

    def execute_command(
        self,
        command_text: str,
        actor_id: Optional[str] = None,
        command_nonce: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        テキスト形式または構造化コマンド (`PAUSE`, `RESUME`, `CANCEL_ENTRIES`, `FLATTEN_ALL`, `FLATTEN_CONFIRM <nonce>`)
        を検証・実行する。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        parts = (command_text or "").strip().split()
        if not parts:
            raise ValueError("空のコマンドは実行できません。")

        cmd = parts[0].upper()
        arg_nonce = parts[1].strip() if len(parts) > 1 else command_nonce

        # 汎用コマンドの nonce リプレイ防止チェック
        if command_nonce and cmd not in ("FLATTEN_CONFIRM",):
            if command_nonce in self._consumed_nonces:
                raise ReplayOrExpiredNonceError(f"リプレイ攻撃検知: nonce '{command_nonce}' は既に使用済みです。")
            if self.db_wal is not None:
                inserted = self.db_wal.register_control_command(
                    nonce=command_nonce,
                    command=cmd,
                    actor_id=verified_actor,
                    require_confirm=False,
                    now_dt=now_jst,
                )
                if not inserted:
                    raise ReplayOrExpiredNonceError(f"SQLite 台帳リプレイ検知: nonce '{command_nonce}' は既に使用済みです。")
                self.db_wal.confirm_and_execute_command(nonce=command_nonce, actor_id=verified_actor, now_dt=now_jst)
            self._record_consumed_nonce(command_nonce)

        if cmd == "PAUSE":
            return self.pause_new_entries(actor_id=verified_actor, now_dt=now_jst)
        if cmd == "RESUME":
            return self.resume_new_entries(actor_id=verified_actor, now_dt=now_jst)
        if cmd == "CANCEL_ENTRIES":
            return self.cancel_pending_entries_only(actor_id=verified_actor, now_dt=now_jst)
        if cmd in ("FLATTEN_ALL", "FLATTEN_REQUEST"):
            return self.request_flatten_all(actor_id=verified_actor, now_dt=now_jst)
        if cmd == "FLATTEN_CONFIRM":
            if not arg_nonce:
                raise ReplayOrExpiredNonceError("FLATTEN_CONFIRM にはワンタイム nonce の指定が必須です。")
            return self.confirm_flatten_all(nonce=arg_nonce, actor_id=verified_actor, now_dt=now_jst)

        raise ValueError(f"未知のリモート制御コマンドです: {cmd}")

    def pause_new_entries(
        self,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Level 1 (`PAUSE`): 新規 `BUY` エントリーのみ一時停止する。
        保有中のポジションおよび取引所の有効な `STOP` 逆指値注文は一切キャンセルせず維持する。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        self.is_paused = True
        pf = self.oco_manager.load_portfolio()
        pf["is_paused"] = True
        self.oco_manager.save_portfolio(pf, now_dt=now_jst)
        protected_sl_ids = [
            str(p.get("sl_order_id", ""))
            for p in (pf.get("positions", []) or [])
            if str(p.get("sl_order_id", ""))
        ]
        return {
            "command": "PAUSE",
            "status": "PAUSED",
            "actor_id": verified_actor,
            "is_paused": True,
            "preserved_sl_order_ids": protected_sl_ids,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def resume_new_entries(
        self,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        self.is_paused = False
        pf = self.oco_manager.load_portfolio()
        pf["is_paused"] = False
        self.oco_manager.save_portfolio(pf, now_dt=now_jst)
        return {
            "command": "RESUME",
            "status": "ACTIVE",
            "actor_id": verified_actor,
            "is_paused": False,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def cancel_pending_entries_only(
        self,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Level 2 (`CANCEL_ENTRIES`): 未約定の `BUY` 指値のみ全キャンセルし、
        既存ポジションの `STOP` 逆指値注文は絶対にキャンセルせず維持する。
        もしキャンセル直前に一部または全部が約定していた場合 (`executed_size > 0`) は、
        約定分を孤児化させずに `SL_ACTIVE`（または `DUST_QUARANTINED`）へ安全に昇格する。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        pf = self.oco_manager.load_portfolio()

        canceled_order_ids: List[str] = []
        remaining_pending: List[Dict[str, Any]] = []
        for p_ord in list(pf.get("pending_orders", []) or []):
            oid = str(p_ord.get("order_id", ""))
            side = str(p_ord.get("side", "BUY")).upper()
            sym = str(p_ord.get("symbol", "")).strip().upper()
            if side == "BUY" and oid:
                try:
                    self.oco_manager.broker.cancel_order(oid)
                except Exception as exc:
                    logger.warning("CANCEL_ENTRIES 注文取消警告 (order_id=%s): %s", oid, exc)
                canceled_order_ids.append(oid)

                exec_size = 0.0
                exec_price = float(p_ord.get("limit_price", 0) or 0)
                try:
                    if hasattr(self.oco_manager, "_query_orders_by_id"):
                        ord_info = self.oco_manager._query_orders_by_id([oid]).get(oid)
                    else:
                        raw_orders = self.oco_manager.broker.get_orders([oid])
                        ord_info = raw_orders[0] if isinstance(raw_orders, list) and raw_orders else None
                    if ord_info is not None:
                        exec_size = float(ord_info.get("executed_size", 0) or 0)
                        exec_price = float(
                            ord_info.get("executed_price")
                            or ord_info.get("price")
                            or p_ord.get("limit_price")
                            or 0
                        )
                except Exception:
                    exec_size = 0.0

                if exec_size > 0 and hasattr(self.oco_manager, "_activate_position_with_sl"):
                    rule = (getattr(self.oco_manager, "symbol_rules", None) or {}).get(sym, {})
                    min_order_size = float(rule.get("min_order_size", 0.0001) or 0.0001)
                    try:
                        if exec_size + 1e-12 >= min_order_size:
                            self.oco_manager._activate_position_with_sl(
                                pf=pf,
                                pending=p_ord,
                                filled_size=exec_size,
                                filled_price=exec_price,
                                now_jst=now_jst,
                                partial_expired=True,
                            )
                        else:
                            self.oco_manager._quarantine_partial_buy_dust(
                                pf=pf,
                                pending=p_ord,
                                dust_size=exec_size,
                                exec_price=exec_price,
                                min_order_size=min_order_size,
                                now_jst=now_jst,
                            )
                    except Exception as act_exc:
                        logger.error(
                            "CANCEL_ENTRIES 時の部分約定昇格エラー (order_id=%s, sym=%s): %s",
                            oid,
                            sym,
                            act_exc,
                        )
                else:
                    if self.db_wal is not None:
                        self.db_wal.upsert_order(
                            order_id=oid,
                            symbol=sym,
                            side="BUY",
                            execution_type="LIMIT",
                            state="CANCELED",
                            price=str(p_ord.get("limit_price", "0")),
                            size=str(p_ord.get("size", "0")),
                        )
            else:
                remaining_pending.append(p_ord)

        pf["pending_orders"] = remaining_pending
        self.oco_manager.save_portfolio(pf, now_dt=now_jst)

        preserved_sl_ids = [
            str(p.get("sl_order_id", ""))
            for p in (pf.get("positions", []) or [])
            if str(p.get("sl_order_id", ""))
        ]
        return {
            "command": "CANCEL_ENTRIES",
            "status": "ENTRIES_CANCELED",
            "actor_id": verified_actor,
            "canceled_buy_order_ids": canceled_order_ids,
            "preserved_sl_order_ids": preserved_sl_ids,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def request_flatten_all(
        self,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Level 3 Step 1 (`FLATTEN_ALL` 要求):
        即座には全決済せず、ワンタイム `nonce` を発行して 2段階承認 (`FLATTEN_CONFIRM <nonce>`) を要求する。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        nonce = f"flt_{secrets.token_hex(6)}"
        expires_at = now_jst + timedelta(seconds=self.nonce_ttl_seconds)

        self._pending_flatten_nonces[nonce] = {
            "actor_id": verified_actor,
            "issued_at": now_jst,
            "expires_at": expires_at,
            "consumed": False,
        }
        self._prune_pending_nonces(now_jst)
        if self.db_wal is not None:
            self.db_wal.register_control_command(
                nonce=nonce,
                command="FLATTEN_ALL",
                actor_id=verified_actor,
                require_confirm=True,
                now_dt=now_jst,
            )

        return {
            "command": "FLATTEN_ALL",
            "status": "AWAITING_CONFIRMATION",
            "executed": False,
            "nonce": nonce,
            "confirm_command": f"FLATTEN_CONFIRM {nonce}",
            "expires_at": expires_at.isoformat(timespec="seconds"),
            "actor_id": verified_actor,
        }

    def confirm_flatten_all(
        self,
        nonce: str,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Level 3 Step 2 (`FLATTEN_CONFIRM <nonce>`):
        ワンタイム `nonce` の一致・有効期限・未使用状態を検証し、全注文キャンセル＋全ポジション成行決済を実行する。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        clean_nonce = str(nonce or "").strip()

        if not clean_nonce or clean_nonce in self._consumed_nonces:
            raise ReplayOrExpiredNonceError(f"使用済みまたは無効なワンタイム nonce です: '{clean_nonce}'")

        meta = self._pending_flatten_nonces.get(clean_nonce)
        if meta is None and self.db_wal is not None and hasattr(self.db_wal, "get_control_command_by_nonce"):
            db_row = self.db_wal.get_control_command_by_nonce(clean_nonce)
            if db_row is not None and str(db_row.get("command", "")).upper() == "FLATTEN_ALL":
                created_ms = int(db_row.get("created_ts_ms", 0) or 0)
                issued_dt = datetime.fromtimestamp(created_ms / 1000.0, tz=JST) if created_ms > 0 else now_jst
                meta = {
                    "actor_id": str(db_row.get("requested_by", verified_actor)),
                    "issued_at": issued_dt,
                    "expires_at": issued_dt + timedelta(seconds=self.nonce_ttl_seconds),
                    "consumed": str(db_row.get("status", "")).upper() == "EXECUTED",
                }
                self._pending_flatten_nonces[clean_nonce] = meta

        if meta is None:
            raise ReplayOrExpiredNonceError(f"未発行のワンタイム nonce です: '{clean_nonce}'")

        if meta.get("consumed"):
            raise ReplayOrExpiredNonceError(f"既に使用済みのワンタイム nonce です: '{clean_nonce}'")

        if now_jst > meta["expires_at"]:
            raise ReplayOrExpiredNonceError(f"有効期限切れのワンタイム nonce です: '{clean_nonce}'")

        if self.db_wal is not None:
            ok = self.db_wal.confirm_and_execute_command(
                nonce=clean_nonce,
                actor_id=verified_actor,
                now_dt=now_jst,
            )
            if not ok:
                raise ReplayOrExpiredNonceError(f"SQLite 台帳上で承認できない nonce です: '{clean_nonce}'")

        meta["consumed"] = True
        self._record_consumed_nonce(clean_nonce)
        self.is_paused = True

        # 1. 未約定の買い指値を全キャンセル
        cancel_res = self.cancel_pending_entries_only(actor_id=verified_actor, now_dt=now_jst)

        # 2. 保有中の全ポジション（DUST_QUARANTINED 除く・最小注文数量以上）を緊急成行決済
        pf = self.oco_manager.load_portfolio()
        pf["is_paused"] = True
        self.oco_manager.save_portfolio(pf, now_dt=now_jst)
        rules_map = getattr(self.oco_manager, "symbol_rules", None) or {}
        symbols_to_close = [
            str(p.get("symbol", "")).upper()
            for p in (pf.get("positions", []) or [])
            if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
            and float(p.get("size", 0) or 0)
            >= float(rules_map.get(str(p.get("symbol", "")).upper(), {}).get("min_order_size", 0.0001) or 0.0001)
        ]
        closed_trades: List[Dict[str, Any]] = []
        for sym in symbols_to_close:
            closed = self.oco_manager.close_position_immediately(
                symbol=sym,
                reason="REMOTE_FLATTEN_ALL",
                now_dt=now_jst,
            )
            if closed:
                closed_trades.append(closed)

        self.dispatcher.dispatch(
            tier="CRITICAL",
            title="リモート緊急全決済 (FLATTEN_ALL) 実行完了",
            message=(
                f"・実行者: `{verified_actor}` (nonce=`{clean_nonce}`)\n"
                f"・キャンセルした待機買い注文: {len(cancel_res['canceled_buy_order_ids'])} 件\n"
                f"・成行決済したポジション: {len(closed_trades)} 件"
            ),
            now_dt=now_jst,
        )

        return {
            "command": "FLATTEN_CONFIRM",
            "status": "FLATTENED",
            "executed": True,
            "nonce": clean_nonce,
            "actor_id": verified_actor,
            "canceled_buy_order_ids": cancel_res["canceled_buy_order_ids"],
            "closed_trades": closed_trades,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def enqueue_or_execute_command(
        self,
        command_text: str,
        is_daemon_running: bool = False,
        actor_id: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        CLI (`--remote-cmd`) から呼び出されるエントリポイント。
        - 本体デーモンが停止中の場合 (`is_daemon_running=False`): 即座に `execute_command` を実行する。
        - 本体デーモンが稼働中の場合 (`is_daemon_running=True`):
          * `FLATTEN_ALL` / `FLATTEN_REQUEST` は SQLite WAL 上に確認待ち `nonce` (`AWAITING_CONFIRM`) を即時発行して返す。
          * `PAUSE`, `RESUME`, `CANCEL_ENTRIES`, `FLATTEN_CONFIRM <nonce>` は SQLite WAL の `control_commands` テーブルに
            `PENDING` としてエンキューし、本体デーモンの次サイクル (`poll_and_execute_db_commands`) でロック競合なく原子的に実行させる。
        """
        now_jst = _now_jst(now_dt)
        verified_actor = self._verify_owner(actor_id)
        parts = (command_text or "").strip().split()
        if not parts:
            raise ValueError("空のコマンドは実行できません。")

        cmd = parts[0].upper()
        arg_nonce = parts[1].strip() if len(parts) > 1 else ""

        if not is_daemon_running or self.db_wal is None or cmd in ("FLATTEN_ALL", "FLATTEN_REQUEST"):
            return self.execute_command(
                command_text=command_text,
                actor_id=verified_actor,
                now_dt=now_jst,
            )

        if cmd not in ("PAUSE", "RESUME", "CANCEL_ENTRIES", "FLATTEN_CONFIRM"):
            raise ValueError(f"未知のリモート制御コマンドです: {cmd}")
        if cmd == "FLATTEN_CONFIRM" and not arg_nonce:
            raise ReplayOrExpiredNonceError("FLATTEN_CONFIRM にはワンタイム nonce の指定が必須です。")

        queue_nonce = f"cli_{secrets.token_hex(6)}"
        payload = {"target_nonce": arg_nonce} if arg_nonce else {}
        self.db_wal.enqueue_control_command(
            command=cmd,
            nonce=queue_nonce,
            requested_by=verified_actor,
            status="PENDING",
            payload=payload,
            now_ts_ms=int(now_jst.timestamp() * 1000),
        )
        # PAUSE / RESUME の場合はデーモンの次サイクル (最大15秒後) を待たずとも
        # Webダッシュボード・ポートフォリオ台帳に即座に反映されるよう is_paused フラグも同期する
        if cmd in ("PAUSE", "RESUME"):
            target_paused = cmd == "PAUSE"
            self.is_paused = target_paused
            try:
                pf_sync = self.oco_manager.load_portfolio()
                if isinstance(pf_sync, dict):
                    pf_sync["is_paused"] = target_paused
                    self.oco_manager.save_portfolio(pf_sync, now_dt=now_jst)
            except Exception as sync_exc:
                logger.warning("PAUSE/RESUME 即時台帳同期警告: %s", sync_exc)
        return {
            "command": cmd,
            "status": "QUEUED_FOR_DAEMON",
            "queued_to_wal": True,
            "is_paused": (True if cmd == "PAUSE" else (False if cmd == "RESUME" else self.is_paused)),
            "queue_nonce": queue_nonce,
            "target_nonce": arg_nonce,
            "actor_id": verified_actor,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def poll_and_execute_db_commands(
        self,
        now_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        SQLite WAL の `control_commands` テーブルに CLI / 外部連携から登録された
        `PENDING` コマンドをポーリングして実行する。
        """
        if self.db_wal is None:
            return []
        now_jst = _now_jst(now_dt)
        executed_results: List[Dict[str, Any]] = []
        rows = self.db_wal.fetch_pending_control_commands()
        for r in rows:
            nonce = str(r.get("nonce", ""))
            raw_cmd = str(r.get("command", "")).strip()
            parts = raw_cmd.split()
            cmd = parts[0].upper() if parts else ""
            actor = str(r.get("requested_by", "") or "")
            payload_dict: Dict[str, Any] = {}
            try:
                raw_payload = r.get("payload_json")
                if raw_payload:
                    payload_dict = json.loads(str(raw_payload)) if isinstance(raw_payload, str) else dict(raw_payload)
            except Exception:
                payload_dict = {}
            try:
                if cmd == "PAUSE":
                    res = self.pause_new_entries(actor_id=actor, now_dt=now_jst)
                elif cmd == "RESUME":
                    res = self.resume_new_entries(actor_id=actor, now_dt=now_jst)
                elif cmd == "CANCEL_ENTRIES":
                    res = self.cancel_pending_entries_only(actor_id=actor, now_dt=now_jst)
                elif cmd == "FLATTEN_CONFIRM":
                    target_nonce = str(payload_dict.get("target_nonce", "") or (parts[1] if len(parts) > 1 else "")).strip()
                    res = self.confirm_flatten_all(nonce=target_nonce, actor_id=actor, now_dt=now_jst)
                else:
                    if nonce and hasattr(self.db_wal, "mark_control_command"):
                        self.db_wal.mark_control_command(
                            nonce=nonce,
                            status="REJECTED",
                            now_ts_ms=int(now_jst.timestamp() * 1000),
                        )
                    self._record_consumed_nonce(nonce)
                    continue
                self.db_wal.confirm_and_execute_command(nonce=nonce, actor_id=actor, now_dt=now_jst)
                self._record_consumed_nonce(nonce)
                executed_results.append(res)
            except Exception as exc:
                if nonce and hasattr(self.db_wal, "mark_control_command"):
                    try:
                        self.db_wal.mark_control_command(
                            nonce=nonce,
                            status="REJECTED",
                            now_ts_ms=int(now_jst.timestamp() * 1000),
                        )
                    except Exception:
                        pass
                self._record_consumed_nonce(nonce)
                logger.warning("control_commands ポーリング実行スキップ (nonce=%s, cmd=%s): %s", nonce, cmd, exc)
        return executed_results
