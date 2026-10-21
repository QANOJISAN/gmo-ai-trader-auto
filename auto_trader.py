"""
v2：24時間常時監視・GMOコイン現物全自動売買メインデーモン (auto_trader.py)

- 全12コア機構統合版:
  1. SQLite WAL 永続化台帳 (`DatabaseWAL`) ＆ アトミック `portfolio.json` ミラー
  2. 4段階優先度付き Token Bucket・Cristian 時刻オフセット自己校正・認証エラー即時 HALT (`GMOPrivateClient`)
  3. WebSocket 4フェーズ Pre-Subscription バッファ同期＆35分トークン延長 (`WSHybridEngine`)
  4. 3層マイクロストラクチャ逆選択防御フィルタ (`AdverseSelectionGuard`)
  5. 擬似OCO 8状態マシン・0.2s Fast Recovery Guardian・4段階トレーリング・端数残高隔離 (`PseudoOCOManager`)
  6. 3-Way Reconciliation（起動時・5分周期・土曜11:05メンテ明け）＆ 06:00 JST 手数料端数調整自動吸収 (`ThreeWayReconciler`)
  7. 4段階ゲート・動的 `/public/v1/symbols` Decimal 量子化・マーケットレジーム (`TechnicalRadar`, `MarketRegimeFilter`)
  8. 反対仮説優先スキーマ・conviction_score >= 75 Veto・Stale Signal Gate・シャドー評価 (`AIJudge`, `LLMShadowEvaluator`)
  9. 5段階サーキットブレーカー (L1〜L5: 保守的 Best Bid * (1 - Taker Fee) 3回連続判定) ＆ Windows 常駐耐障害性 (`MultiTierCircuitBreaker`)
  10. 3段階分離リモート緊急制御 (`PAUSE`, `CANCEL_ENTRIES`, `FLATTEN_ALL` ワンタイム nonce 2段階承認) ＆ 通知3階層 (`RemoteControlManager`)
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

from generate_report import generate_report
from src.adverse_selection_guard import AdverseSelectionGuard
from src.ai_judge import AIJudge
from src.circuit_breaker import (
    MultiTierCircuitBreaker,
    SingleInstanceLock,
    check_memory_rss_and_should_restart,
    is_in_saturday_pre_maintenance_clear_window,
    prevent_windows_sleep,
)
from src.database_wal import DatabaseWAL
from src.gmo_client import GMOPublicClient
from src.gmo_private_client import (
    GMOAuthHaltError,
    GMOMaintenanceError,
    GMOPrivateClient,
    PaperBroker,
    create_broker_client,
)
from src.json_utils import write_json_file
from src.live_probe import GMOCoinLiveProbe
from src.llm_shadow_evaluator import LLMShadowEvaluator
from src.notifier import Notifier, build_manual_order_guide
from src.pseudo_oco_manager import PseudoOCOManager
from src.radar import TechnicalRadar
from src.reconciler import ThreeWayReconciler
from src.regime_filter import MarketRegimeFilter
from src.remote_control import RemoteControlManager, TieredNotificationDispatcher
from src.validator import DEFAULT_SYMBOL_RULES, validate_and_adjust_decision
from src.wfa_backtester import ConservativeSpotBacktester
from src.ws_hybrid_engine import WSHybridEngine

BASE_DIR = Path(__file__).resolve().parent
PORTFOLIO_PATH = BASE_DIR / "portfolio.json"
HISTORY_CSV_PATH = BASE_DIR / "history_log.csv"
LATEST_DECISION_PATH = BASE_DIR / "latest_decision.json"
LOG_DIR = BASE_DIR / "logs"

JST = timezone(timedelta(hours=9))

CSV_HEADERS = [
    "timestamp",
    "action",
    "symbol",
    "order_type",
    "limit_price",
    "size",
    "take_profit_price",
    "stop_loss_price",
    "expected_cost_jpy",
    "expected_loss_jpy",
    "risk_reward_ratio",
    "confidence",
    "provider",
    "jpy_balance",
    "open_positions_count",
    "reasoning",
]

logger = logging.getLogger("auto_trader")


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def is_in_saturday_maintenance_window(now_dt: Optional[datetime] = None) -> bool:
    """
    毎週土曜日 08:55 〜 11:05 JST（GMOコイン定期メンテナンス枠）かどうかを判定する。
    `08:55 <= 時刻 < 11:05` の間は `True` を返し、`11:05:00` ちょうど以降は `False`（メンテ明け照合再開）となる。
    """
    now_jst = _now_jst(now_dt)
    if now_jst.weekday() != 5:  # 0=Mon ... 5=Sat, 6=Sun
        return False
    minutes_of_day = now_jst.hour * 60 + now_jst.minute
    start_min = 8 * 60 + 55   # 08:55 JST (535)
    end_min = 11 * 60 + 5     # 11:05 JST (665)
    return start_min <= minutes_of_day < end_min


def append_history_csv(
    csv_path: Path,
    decision: Dict[str, Any],
    portfolio: Dict[str, Any],
) -> None:
    """末尾改行欠落による行連結破損を防ぎつつ history_log.csv に判定履歴を追記する"""
    file_exists = csv_path.exists() and csv_path.stat().st_size > 0
    if file_exists:
        with csv_path.open("rb") as fb:
            fb.seek(-1, os.SEEK_END)
            last_byte = fb.read(1)
        if last_byte not in (b"\n", b"\r"):
            with csv_path.open("a", encoding="utf-8", newline="") as fa:
                fa.write("\n")

    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": decision.get("timestamp", ""),
                "action": decision.get("action", "HOLD"),
                "symbol": decision.get("symbol", "NONE"),
                "order_type": decision.get("order_type", "NONE"),
                "limit_price": decision.get("limit_price", 0),
                "size": decision.get("size", 0),
                "take_profit_price": decision.get("take_profit_price", 0),
                "stop_loss_price": decision.get("stop_loss_price", 0),
                "expected_cost_jpy": decision.get("expected_cost_jpy", 0),
                "expected_loss_jpy": decision.get("expected_loss_jpy", 0),
                "risk_reward_ratio": decision.get("risk_reward_ratio", 0.0),
                "confidence": decision.get("confidence", 0.0),
                "provider": decision.get("provider", "unknown"),
                "jpy_balance": int(portfolio.get("jpy_balance", 0) or 0),
                "open_positions_count": len(portfolio.get("positions", []) or []),
                "reasoning": str(decision.get("reasoning", "")).replace("\r", " ").replace("\n", " "),
            }
        )


class AutoTraderDaemon:
    """24時間常時監視・GMOコイン現物全自動売買デーモン（全12コア機構統合）"""

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        execution_mode: Optional[str] = None,
        public_client: Optional[GMOPublicClient] = None,
        broker: Optional[Union[GMOPrivateClient, PaperBroker]] = None,
        ai_judge: Optional[AIJudge] = None,
        radar: Optional[TechnicalRadar] = None,
        notifier: Optional[Notifier] = None,
        db_wal: Optional[DatabaseWAL] = None,
        fast_loop_sec: Optional[int] = None,
        radar_loop_sec: Optional[int] = None,
        order_expiry_hours: Optional[float] = None,
        max_daily_trades: Optional[int] = None,
        max_consecutive_api_errors: Optional[int] = None,
        dry_run_notify: bool = False,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else BASE_DIR
        self.portfolio_path = self.base_dir / "portfolio.json"
        self.history_csv_path = self.base_dir / "history_log.csv"
        self.latest_decision_path = self.base_dir / "latest_decision.json"
        self.db_path = self.base_dir / "v2_state_wal.db"
        self.lock_path = self.base_dir / "v2_daemon.lock"

        self.db_wal = db_wal or DatabaseWAL(
            self.db_path,
            portfolio_json_path=self.portfolio_path,
            auto_sync_files=True,
        )
        self.execution_mode = (execution_mode or os.getenv("EXECUTION_MODE", "DRY_RUN")).strip().upper()
        self.public_client = public_client or GMOPublicClient()
        self.broker = broker or create_broker_client(
            execution_mode=self.execution_mode,
            base_dir=self.base_dir,
            public_client=self.public_client,
            wal_db=self.db_wal,
        )
        self.notifier = notifier or Notifier()
        self.ai_judge = ai_judge or AIJudge()
        self.radar = radar or TechnicalRadar()
        self.regime_filter = getattr(self.radar, "regime_filter", None) or MarketRegimeFilter()
        self.dry_run_notify = dry_run_notify

        self.fast_loop_sec = int(fast_loop_sec if fast_loop_sec is not None else os.getenv("FAST_LOOP_SEC", "15"))
        self.radar_loop_sec = int(radar_loop_sec if radar_loop_sec is not None else os.getenv("RADAR_LOOP_SEC", "900"))
        self.order_expiry_hours = float(
            order_expiry_hours if order_expiry_hours is not None else os.getenv("ORDER_EXPIRY_HOURS", "4")
        )
        self.max_daily_trades = int(
            max_daily_trades if max_daily_trades is not None else os.getenv("MAX_DAILY_TRADES", "4")
        )
        self.max_consecutive_api_errors = int(
            max_consecutive_api_errors
            if max_consecutive_api_errors is not None
            else os.getenv("MAX_CONSECUTIVE_API_ERRORS", "5")
        )

        self.oco_manager = PseudoOCOManager(
            broker=self.broker,
            portfolio_path=self.portfolio_path,
            notifier=self.notifier,
            symbol_rules=DEFAULT_SYMBOL_RULES,
            order_expiry_hours=self.order_expiry_hours,
            dry_run_notify=self.dry_run_notify,
            wal_db=self.db_wal,
        )
        self.reconciler = ThreeWayReconciler(
            broker=self.broker,
            oco_manager=self.oco_manager,
            db_wal=self.db_wal,
            notifier=self.notifier,
            symbol_rules=DEFAULT_SYMBOL_RULES,
            dry_run_notify=self.dry_run_notify,
        )
        self.circuit_breaker = MultiTierCircuitBreaker(
            l2_max_consecutive_api_errors=self.max_consecutive_api_errors,
            symbol_rules=DEFAULT_SYMBOL_RULES,
        )
        self.adverse_guard = AdverseSelectionGuard()
        self.shadow_evaluator = LLMShadowEvaluator(db_wal=self.db_wal)
        self.notification_dispatcher = TieredNotificationDispatcher(
            notifier=self.notifier,
            dry_run=self.dry_run_notify,
        )
        self.remote_control = RemoteControlManager(
            oco_manager=self.oco_manager,
            db_wal=self.db_wal,
            dispatcher=self.notification_dispatcher,
        )
        self.ws_engine = WSHybridEngine(
            broker=self.broker,
            wal_db=self.db_wal,
        )
        self.live_probe = GMOCoinLiveProbe(
            broker=self.broker,
            capabilities_path=self.base_dir / "capabilities.json",
        )
        self.backtester = ConservativeSpotBacktester()
        self.instance_lock = SingleInstanceLock(self.lock_path)

        # 内部状態
        self.in_maintenance_sleep: bool = False
        self.pre_maintenance_cleared: bool = False
        self.last_ai_call_at: Optional[datetime] = None
        self.last_radar_run_at: Optional[datetime] = None
        self.last_reconcile_run_at: Optional[datetime] = None
        self.consecutive_api_errors: int = 0
        self.circuit_breaker_paused_until: Optional[datetime] = None
        self.auth_halted: bool = False
        self._daily_trade_date: str = ""
        self._daily_trade_count: int = 0
        self._last_digest_date: str = ""

    def _get_daily_trade_count(self, now_jst: datetime) -> int:
        # GMO取引日基準 (06:00 JST) で日次カウントをリセット
        trading_dt = now_jst - timedelta(hours=6) if now_jst.hour < 6 else now_jst
        today_str = trading_dt.strftime("%Y-%m-%d")
        if self._daily_trade_date != today_str:
            self._daily_trade_date = today_str
            self._daily_trade_count = 0
        return self._daily_trade_count

    def _increment_daily_trade_count(self, now_jst: datetime) -> int:
        self._get_daily_trade_count(now_jst)
        self._daily_trade_count += 1
        return self._daily_trade_count

    def record_api_success(self) -> None:
        self.consecutive_api_errors = 0
        self.circuit_breaker_paused_until = None
        self.circuit_breaker.record_api_success()

    def record_api_error(self, exc: Exception, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        if isinstance(exc, GMOAuthHaltError):
            self.auth_halted = True
            self.notification_dispatcher.dispatch(
                tier="CRITICAL",
                title="GMO Private API 認証・権限エラー即時HALT (ERR-5010〜5012)",
                message=f"認証エラーを検知したため全自動発注を即時停止しました（DRY_RUNへの勝手なフォールバックは禁止）: `{exc}`",
                now_dt=now_jst,
            )
        # 臨時・水曜メンテナンスエラー (ERR-5201〜5206 / GMOMaintenanceError) は連続エラーとしてカウントせずメンテスリープへ移行
        err_text = str(exc)
        if isinstance(exc, GMOMaintenanceError) or any(
            code in err_text for code in ("ERR-5201", "ERR-5202", "ERR-5203", "ERR-5204", "ERR-5205", "ERR-5206")
        ):
            self.in_maintenance_sleep = True
            self.circuit_breaker.record_api_error(exc, now_dt=now_jst)
            logger.info("🔧 GMOコイン臨時・定期メンテナンス応答を検知したため MAINTENANCE_SLEEP へ移行します: %s", exc)
            return {
                "consecutive_api_errors": self.consecutive_api_errors,
                "circuit_breaker_triggered": False,
                "maintenance_sleep": True,
                "auth_halted": self.auth_halted,
                "paused_until": None,
            }
        self.consecutive_api_errors += 1
        self.circuit_breaker.record_api_error(exc, now_dt=now_jst)
        logger.error(
            "API エラー発生 (%d/%d): %s",
            self.consecutive_api_errors,
            self.max_consecutive_api_errors,
            exc,
        )
        triggered = False
        if self.consecutive_api_errors >= self.max_consecutive_api_errors:
            triggered = True
            self.circuit_breaker_paused_until = now_jst + timedelta(minutes=15)
            self.notifier.notify_event(
                title="API連続エラー・サーキットブレーカー発動（自動一時停止）",
                message=(
                    f"・連続エラー回数: {self.consecutive_api_errors} 回 (閾値 {self.max_consecutive_api_errors} 回)\n"
                    f"・最新エラー: `{exc}`\n"
                    f"・一時停止解除予定: `{self.circuit_breaker_paused_until.isoformat(timespec='seconds')}`"
                ),
                level="ERROR",
                dry_run=self.dry_run_notify,
            )
        return {
            "consecutive_api_errors": self.consecutive_api_errors,
            "circuit_breaker_triggered": triggered,
            "auth_halted": self.auth_halted,
            "paused_until": (
                self.circuit_breaker_paused_until.isoformat(timespec="seconds")
                if self.circuit_breaker_paused_until
                else None
            ),
        }

    def clear_pending_buys_before_maintenance(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """土曜 08:50〜08:55 JST に未約定の `BUY` 指値を事前クリアする（既存 SL 注文は維持）"""
        now_jst = _now_jst(now_dt)
        res = self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
        self.pre_maintenance_cleared = True
        return res

    def reconcile_after_maintenance(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        土曜メンテナンス明け（11:05 JST）やデーモン再起動時に、
        `ThreeWayReconciler` および口座・有効注文照合を実行して `portfolio.json` と実口座を修復する。
        - 保有ポジションの `sl_order_id` が有効注文一覧に存在しない場合は、直ちに `STOP` SL を再発注する。
        """
        now_jst = _now_jst(now_dt)
        assets_summary = self.broker.get_jpy_and_spot_balances()
        active_orders_before = self.broker.get_active_orders()
        rec_res = self.reconciler.reconcile(now_dt=now_jst)
        active_orders_after = self.broker.get_active_orders()
        reconciled_actions = list(rec_res.get("actions_taken", []))

        summary = {
            "status": "RECONCILED",
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "jpy_available": assets_summary.get("jpy_available", 0.0),
            "spot_assets": assets_summary.get("spot_assets", {}),
            "active_orders_count": len(active_orders_before),
            "active_orders_after_count": len(active_orders_after),
            "reconciled_actions": reconciled_actions,
            "three_way_detail": rec_res,
        }
        self.notifier.notify_event(
            title="メンテナンス明け口座・有効注文照合 (Reconciliation) 完了",
            message=(
                f"・日本円余力: {int(assets_summary.get('jpy_available', 0)):,} 円\n"
                f"・有効注文数: {len(active_orders_before)} 件\n"
                f"・照合アクション: {', '.join(reconciled_actions) if reconciled_actions else '異常なし（完全一致）'}"
            ),
            level="MAINT",
            dry_run=self.dry_run_notify,
        )
        return summary

    def run_fast_cycle(
        self,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
        exchange_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        15秒間隔の高速ループ1サイクルを実行する。
        - 土曜 08:50〜08:55 JST は未約定 `BUY` 指値を事前クリア
        - 土曜 08:55〜11:05 JST のメンテナンス枠内は API 発注・変更を自動スリープ
        - 11:05 JST メンテナンス明けは `status == "OPEN"` (`PREOPEN` 待機) 確認後に `reconcile_after_maintenance()` を実行
        """
        now_jst = _now_jst(now_dt)
        prevent_windows_sleep()

        if self.auth_halted or getattr(self.broker, "halted", False):
            self.auth_halted = True
            return {
                "status": "AUTH_HALTED",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": [],
            }

        # 0. SQLite WAL のリモート制御コマンドポーリング & 06:00 JST デイリーダイジェスト送信
        self.remote_control.poll_and_execute_db_commands(now_dt=now_jst)
        if now_jst.hour >= 6:
            digest_day = now_jst.strftime("%Y-%m-%d")
            if self._last_digest_date != digest_day:
                self._last_digest_date = digest_day
                self.notification_dispatcher.flush_daily_digest(now_dt=now_jst)

        # 0b. 土曜 08:50〜08:55 JST 未約定 BUY 事前クリア
        if is_in_saturday_pre_maintenance_clear_window(now_jst) and not self.pre_maintenance_cleared:
            self.clear_pending_buys_before_maintenance(now_dt=now_jst)

        # 1. 土曜メンテナンス枠チェック (08:55〜11:05 JST) または 臨時・水曜メンテナンスステータス検知
        ex_stat_clean = (exchange_status or "").strip().upper()
        if is_in_saturday_maintenance_window(now_jst) or (not self.in_maintenance_sleep and ex_stat_clean in ("MAINTENANCE", "PREOPEN")):
            if not self.in_maintenance_sleep:
                self.in_maintenance_sleep = True
                logger.info("🔧 取引所メンテナンス枠 (土曜定時または臨時/水曜メンテ) に入ったため自動スリープを開始します。")
                self.notifier.notify_event(
                    title="取引所メンテナンス自動スリープ開始",
                    message="GMOコインメンテナンス枠のため、再開確認まで API 発注・変更を一時休止します。",
                    level="MAINT",
                    dry_run=self.dry_run_notify,
                )
            return {
                "status": "MAINTENANCE_SLEEP",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": [],
            }

        # メンテナンス明け (11:05 JST 以降 または 臨時メンテ終了後) の PREOPEN チェック＆自動照合
        reconcile_result: Optional[Dict[str, Any]] = None
        if self.in_maintenance_sleep:
            ex_stat = ex_stat_clean or "OPEN"
            if ex_stat in ("MAINTENANCE", "PREOPEN"):
                return {
                    "status": f"WAITING_EXCHANGE_OPEN_{ex_stat}",
                    "timestamp": now_jst.isoformat(timespec="seconds"),
                    "events": [],
                }
            self.in_maintenance_sleep = False
            self.pre_maintenance_cleared = False
            logger.info("🔧 メンテナンス枠が終了したため、資産・注文照合を実行して再開します。")
            reconcile_result = self.reconcile_after_maintenance(now_dt=now_jst)

        # APIエラー・サーキットブレーカー一時停止中か確認
        if self.circuit_breaker_paused_until and now_jst < self.circuit_breaker_paused_until:
            return {
                "status": "CIRCUIT_BREAKER_PAUSED",
                "paused_until": self.circuit_breaker_paused_until.isoformat(timespec="seconds"),
                "events": [],
            }

        try:
            current_tickers = tickers if tickers is not None else self.public_client.get_tickers()

            # 0.2s Fast Recovery ガーディアン（ノーガード建玉の即時復旧）
            guardian_events = self.oco_manager.run_fast_recovery_guardian(
                tickers=current_tickers,
                now_dt=now_jst,
            )

            # 板置きのまま滞留している PENDING_BUY の逆選択（毒性フロー）キャンセル判定
            adverse_cancel_events: List[Dict[str, Any]] = []
            pf_pre = self.oco_manager.load_portfolio()
            remaining_pending: List[Dict[str, Any]] = []
            pending_modified = False
            for p_ord in pf_pre.get("pending_orders", []) or []:
                p_sym = str(p_ord.get("symbol", "")).upper()
                p_oid = str(p_ord.get("order_id", ""))
                if p_oid and (
                    p_sym in self.adverse_guard._latest_orderbook
                    or p_sym in self.adverse_guard._trades_by_symbol
                ):
                    ob_cached = self.adverse_guard._latest_orderbook.get(p_sym)
                    if isinstance(ob_cached, dict) and ob_cached.get("is_rest_snapshot"):
                        ob_cached["timestamp_sec"] = now_jst.timestamp()
                    cancel_chk = self.adverse_guard.should_cancel_resting_buy_order(
                        symbol=p_sym,
                        order_price=float(p_ord.get("limit_price", 0) or 0),
                        order_size=float(p_ord.get("size", 0) or 0),
                        atr_5m=float(p_ord.get("atr_5m", 100.0) or 100.0),
                        now_sec=now_jst.timestamp(),
                    )
                    if cancel_chk.get("should_cancel"):
                        try:
                            self.broker.cancel_order(p_oid)
                        except Exception as exc:
                            logger.warning("逆選択ガードによる待機買い注文取消警告 (order_id=%s): %s", p_oid, exc)
                        adverse_cancel_events.append(
                            {
                                "type": "ADVERSE_SELECTION_RESTING_BUY_CANCELED",
                                "symbol": p_sym,
                                "order_id": p_oid,
                                "reason": ", ".join(cancel_chk.get("cancel_reasons", [])),
                                "timestamp": now_jst.isoformat(timespec="seconds"),
                            }
                        )
                        pending_modified = True
                        continue
                remaining_pending.append(p_ord)
            if pending_modified:
                pf_pre["pending_orders"] = remaining_pending
                self.oco_manager.save_portfolio(pf_pre, now_dt=now_jst)

            step_events = self.oco_manager.step(tickers=current_tickers, now_dt=now_jst)
            events = guardian_events + adverse_cancel_events + step_events

            # SL 約定イベント発生時は該当銘柄に180分クールダウンを記録
            for ev in events:
                if ev.get("type") in (
                    "SL_EXECUTED",
                    "BOT_REDUNDANT_SL_EXECUTED",
                    "SL_PARTIAL_FAK_SWEPT",
                    "FAST_RECOVERY_MARKET_EXIT",
                ):
                    ev_sym = str(ev.get("symbol", "")).upper()
                    if ev_sym:
                        self.ai_judge.record_sl_hit_cooldown(ev_sym, now_dt=now_jst)

            # L5 ハード・キルスイッチ (Best Bid * (1 - Taker Fee) で3回連続 < 8,500円) チェック
            pf_now = self.oco_manager.load_portfolio()
            l5_res = self.circuit_breaker.evaluate_l5_hard_kill_switch(pf_now, current_tickers)
            if l5_res["newly_triggered"]:
                self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                for l5_pos in list(pf_now.get("positions", []) or []):
                    if str(l5_pos.get("state", "PROTECTED")).upper() == "DUST_QUARANTINED":
                        continue
                    l5_sym = str(l5_pos.get("symbol", "")).strip().upper()
                    if l5_sym:
                        try:
                            closed_l5 = self.oco_manager.close_position_immediately(
                                symbol=l5_sym,
                                reason="L5_KILL_SWITCH",
                                now_dt=now_jst,
                            )
                            if closed_l5:
                                events.append(
                                    {
                                        "type": "L5_KILL_SWITCH_FLATTENED",
                                        "symbol": l5_sym,
                                        "exit_price": closed_l5.get("exit_price", 0),
                                        "pnl_jpy": closed_l5.get("pnl_jpy", 0),
                                        "timestamp": now_jst.isoformat(timespec="seconds"),
                                    }
                                )
                        except Exception as l5_exc:
                            logger.error("L5 キルスイッチ発動時のポジション決済エラー (%s): %s", l5_sym, l5_exc)
                self.notification_dispatcher.dispatch(
                    tier="CRITICAL",
                    title="L5 ハード・キルスイッチ発動（3回連続 Best Bid 清算価値 < 8,500円）",
                    message=(
                        f"・保守的清算総資産: {l5_res['conservative_equity_jpy']:,.0f} 円 "
                        f"(< {l5_res['threshold_jpy']:,.0f} 円 × 3回連続)\n"
                        "・未約定の買い指値を全キャンセルし、全保有ポジションを即時成行決済の上、新規BUYを恒久ロックしました。"
                    ),
                    now_dt=now_jst,
                )

            self.record_api_success()
            return {
                "status": "OK",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": events,
                "reconciled": reconcile_result,
                "l5_kill_switch": l5_res,
            }
        except Exception as exc:
            cb_info = self.record_api_error(exc, now_dt=now_jst)
            return {
                "status": "API_ERROR",
                "error": str(exc),
                "circuit_breaker": cb_info,
                "events": [],
            }

    def run_radar_and_ai_cycle(
        self,
        market_summaries: Optional[Dict[str, Dict[str, Any]]] = None,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        force_ai: bool = False,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        15分間隔の中速レーダー＆AI判定＆自動発注サイクルを実行する。
        """
        now_jst = _now_jst(now_dt)

        if self.auth_halted or getattr(self.broker, "halted", False):
            self.auth_halted = True
            return {
                "status": "AUTH_HALTED",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "ai_called": False,
            }

        if is_in_saturday_maintenance_window(now_jst):
            self.in_maintenance_sleep = True
            return {
                "status": "MAINTENANCE_SLEEP",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "ai_called": False,
            }

        if self.in_maintenance_sleep:
            self.in_maintenance_sleep = False
            self.reconcile_after_maintenance(now_dt=now_jst)

        if self.circuit_breaker_paused_until and now_jst < self.circuit_breaker_paused_until:
            return {
                "status": "CIRCUIT_BREAKER_PAUSED",
                "paused_until": self.circuit_breaker_paused_until.isoformat(timespec="seconds"),
                "ai_called": False,
            }

        if self.reconciler.degraded_reconciling:
            return {
                "status": "DEGRADED_RECONCILING",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "ai_called": False,
            }

        try:
            pf = self.oco_manager.load_portfolio()
            held_symbols = [str(p.get("symbol", "")).upper() for p in pf.get("positions", [])]

            if market_summaries is None:
                market_summaries, symbol_rules, tickers = self.radar.collect_market_summaries(
                    public_client=self.public_client,
                    target_symbols_env=os.getenv("TARGET_SYMBOLS", "ALL"),
                    max_screened_symbols=int(os.getenv("MAX_SCREENED_SYMBOLS", "10")),
                    max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.35")),
                    held_symbols=held_symbols,
                    reference_dt=now_jst,
                )
            rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}

            # まず最新 Ticker で擬似OCOのステップ評価を実行
            if tickers:
                step_evs = self.oco_manager.step(tickers=tickers, now_dt=now_jst)
                for ev in step_evs:
                    if ev.get("type") in (
                        "SL_EXECUTED",
                        "BOT_REDUNDANT_SL_EXECUTED",
                        "SL_PARTIAL_FAK_SWEPT",
                        "FAST_RECOVERY_MARKET_EXIT",
                    ):
                        ev_sym = str(ev.get("symbol", "")).upper()
                        if ev_sym:
                            self.ai_judge.record_sl_hit_cooldown(ev_sym, now_dt=now_jst)
                pf = self.oco_manager.load_portfolio()

            # 未評価のシャドー評価レコードがあれば直近ローソク足から MFE/MAE・仮想損益を更新
            self.shadow_evaluator.evaluate_pending_records(
                market_summaries=market_summaries,
                now_dt=now_jst,
            )

            radar_eval = self.radar.evaluate_triggers(
                market_summaries=market_summaries,
                portfolio=pf,
                symbol_rules=rules,
                last_ai_call_at=self.last_ai_call_at,
                now_dt=now_jst,
            )
            self.last_radar_run_at = now_jst

            # RISK_OFF_CRASH 検知時は未約定の PENDING_BUY を即時キャンセル
            regime_info = radar_eval.get("regime_info", {})
            if regime_info.get("should_cancel_pending_buys"):
                self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                pf = self.oco_manager.load_portfolio()

            if not radar_eval["should_call_ai"] and not force_ai:
                logger.info("📡 レーダースキャン完了: %s", radar_eval["reason_summary"])
                self.record_api_success()
                return {
                    "status": "RADAR_SKIPPED_AI",
                    "ai_called": False,
                    "radar": radar_eval,
                    "timestamp": now_jst.isoformat(timespec="seconds"),
                }

            logger.info("🚀 レーダートリガー発火 (%s) ➔ AIJudge を起動します", radar_eval["reason_summary"])
            raw_decision = self.ai_judge.judge(
                market_summaries=market_summaries,
                portfolio=pf,
                symbol_rules=rules,
                now_dt=now_jst,
            )
            validated = validate_and_adjust_decision(
                decision=raw_decision,
                portfolio=pf,
                symbol_rules=rules,
                market_summaries=market_summaries,
            )
            self.last_ai_call_at = now_jst

            # L3/L4/L5 サーキットブレーカー & PAUSE 状態 & 1日最大取引回数ガード (BUY時)
            action = str(validated.get("action", "HOLD")).upper()
            if action == "BUY":
                l3_res = self.circuit_breaker.check_l3_daily_loss(pf, tickers, now_dt=now_jst)
                l4_res = self.circuit_breaker.check_l4_weekly_and_streak(pf, now_dt=now_jst)
                current_daily_trades = self._get_daily_trade_count(now_jst)

                block_reason = ""
                if self.remote_control.is_paused:
                    block_reason = "[リモートPAUSE中によりHOLDへ自動補正] "
                elif self.circuit_breaker.l5_kill_switch_latched:
                    block_reason = "[L5ハード・キルスイッチ発動中によりHOLDへ自動補正] "
                elif l3_res["blocked"]:
                    block_reason = f"[L3日次損失制限 ({l3_res['daily_total_pnl_jpy']:+.0f}円 <= -300円) により翌06:00までHOLD] "
                elif l4_res["blocked"]:
                    block_reason = f"[L4連敗/週間損失クールダウンによりHOLD] "
                elif current_daily_trades >= self.max_daily_trades:
                    block_reason = f"[1日の最大取引回数上限 ({self.max_daily_trades}回) 到達によりHOLDへ自動補正] "
                    validated["daily_trade_limit_reached"] = True

                if block_reason:
                    validated["action"] = "HOLD"
                    validated["order_type"] = "NONE"
                    validated["limit_price"] = 0
                    validated["size"] = 0
                    validated["take_profit_price"] = 0
                    validated["stop_loss_price"] = 0
                    validated["expected_cost_jpy"] = 0
                    validated["expected_loss_jpy"] = 0
                    validated["risk_reward_ratio"] = 0.0
                    validated["reasoning"] = block_reason + str(validated.get("reasoning", ""))
                    action = "HOLD"

            # 逆選択（毒性フロー）3層防御フィルタ (AdverseSelectionGuard) による発注直前チェック
            if action == "BUY":
                buy_sym = str(validated.get("symbol", "")).upper()
                sym_summary = (market_summaries or {}).get(buy_sym, {})
                ob_data = sym_summary.get("orderbook") or {}
                trades_data = sym_summary.get("recent_trades") or []
                now_sec_val = now_jst.timestamp()
                if ob_data and (ob_data.get("asks") or ob_data.get("bids")):
                    self.adverse_guard.record_orderbook(
                        symbol=buy_sym,
                        asks=ob_data.get("asks", []),
                        bids=ob_data.get("bids", []),
                        timestamp_sec=now_sec_val,
                        is_rest_snapshot=True,
                    )
                for tr in trades_data:
                    self.adverse_guard.record_taker_trade(
                        symbol=buy_sym,
                        side=str(tr.get("side", "BUY")),
                        price=float(tr.get("price", 0) or 0),
                        size=float(tr.get("size", 0) or 0),
                        timestamp_sec=float(tr.get("timestamp_sec", now_sec_val)),
                    )
                if (
                    ob_data
                    or trades_data
                    or buy_sym in self.adverse_guard._latest_orderbook
                    or buy_sym in self.adverse_guard._trades_by_symbol
                ):
                    atr_5m_val = float(
                        sym_summary.get("m5_indicators", {}).get("atr_14", 0)
                        or sym_summary.get("m15_indicators", {}).get("atr_14", 0)
                        or max(1.0, float(validated.get("limit_price", 100) or 100) * 0.005)
                    )
                    guard_res = self.adverse_guard.evaluate_entry_guard(
                        symbol=buy_sym,
                        proposed_limit_price=validated.get("limit_price", 0),
                        atr_5m=atr_5m_val,
                        now_sec=now_sec_val,
                    )
                    validated["adverse_selection_guard"] = guard_res
                    if not guard_res["allow_entry"]:
                        validated["action"] = "HOLD"
                        validated["order_type"] = "NONE"
                        validated["limit_price"] = 0
                        validated["size"] = 0
                        validated["take_profit_price"] = 0
                        validated["stop_loss_price"] = 0
                        validated["expected_cost_jpy"] = 0
                        validated["expected_loss_jpy"] = 0
                        validated["risk_reward_ratio"] = 0.0
                        validated["reasoning"] = (
                            f"[逆選択防御フィルタによりABORT ({', '.join(guard_res['reject_reasons'])})] "
                            + str(validated.get("reasoning", ""))
                        )
                        action = "HOLD"
                    elif guard_res.get("passive_offset_applied"):
                        validated["limit_price"] = guard_res["adjusted_limit_price"]

            validated["radar_triggers"] = radar_eval["triggers"]
            validated["execution_mode"] = getattr(self.broker, "MODE", self.execution_mode)
            validated["manual_instruction"] = build_manual_order_guide(validated)
            validated["status"] = "SUCCESS"

            # シャドー評価台帳への登録
            target_sym = str(validated.get("symbol", "NONE")).upper()
            if target_sym in market_summaries:
                ref_p = float(market_summaries[target_sym].get("price", {}).get("last", 0) or 0)
                if ref_p > 0:
                    self.shadow_evaluator.record_decision(
                        symbol=target_sym,
                        ai_action=action,
                        conviction_score=int(validated.get("conviction_score", 75) or 75),
                        entry_price=float(validated.get("limit_price") or ref_p),
                        tp_price=float(validated.get("take_profit_price") or 0.0),
                        sl_price=float(validated.get("stop_loss_price") or 0.0),
                        evaluated_at=now_jst,
                    )

            # 自動発注執行
            execution_result: Dict[str, Any] = {"action": action, "status": "NOOP"}
            if action == "BUY":
                execution_result = self.oco_manager.place_new_buy_order(
                    decision=validated,
                    tickers=tickers,
                    now_dt=now_jst,
                )
                if execution_result.get("status") == "PENDING_BUY":
                    self._increment_daily_trade_count(now_jst)
                    self.ai_judge.record_symbol_ai_call(target_sym, now_dt=now_jst)
            elif action == "MODIFY":
                mod_pos = self.oco_manager.modify_position_tp_sl(
                    symbol=str(validated.get("symbol", "")),
                    new_tp=float(validated.get("take_profit_price", 0) or 0),
                    new_sl=float(validated.get("stop_loss_price", 0) or 0),
                    now_dt=now_jst,
                )
                execution_result = {"action": "MODIFY", "status": "MODIFIED" if mod_pos else "NOT_FOUND", "position": mod_pos}
            elif action == "SELL":
                closed = self.oco_manager.close_position_immediately(
                    symbol=str(validated.get("symbol", "")),
                    reason="AI_SELL",
                    now_dt=now_jst,
                )
                execution_result = {"action": "SELL", "status": "CLOSED" if closed else "NOT_FOUND", "closed_trade": closed}

            validated["execution_result"] = execution_result
            pf_after = self.oco_manager.load_portfolio()
            write_json_file(self.latest_decision_path, validated)
            append_history_csv(self.history_csv_path, validated, pf_after)
            generate_report(
                output_path=self.base_dir / "PERFORMANCE_REPORT.md",
                notify=False,
                dry_run=True,
                print_console=False,
                portfolio_path=self.portfolio_path,
                history_csv_path=self.history_csv_path,
            )
            # 3階層通知: HOLD の場合はスパム防止のため DAILY_DIGEST バッファへ送り、BUY/SELL/MODIFY は即時通知
            if action == "HOLD":
                self.notification_dispatcher.dispatch(
                    tier="DAILY_DIGEST",
                    title=f"AI判定 HOLD ({target_sym})",
                    message=str(validated.get("reasoning", ""))[:200],
                    now_dt=now_jst,
                )
            else:
                self.notifier.notify(validated, dry_run=self.dry_run_notify)
            self.record_api_success()

            return {
                "status": "AI_EXECUTED",
                "ai_called": True,
                "radar": radar_eval,
                "decision": validated,
                "execution_result": execution_result,
                "timestamp": now_jst.isoformat(timespec="seconds"),
            }
        except Exception as exc:
            cb_info = self.record_api_error(exc, now_dt=now_jst)
            return {
                "status": "API_ERROR",
                "ai_called": False,
                "error": str(exc),
                "circuit_breaker": cb_info,
            }

    def run_forever(self, max_cycles: Optional[int] = None) -> None:
        """24時間常駐メインループ（15秒ごとに高速ループ、5分ごとに3-Way照合、15分ごとに中速レーダーループを実行）"""
        if not self.instance_lock.acquire():
            raise RuntimeError(f"AutoTraderDaemon は既に別プロセスで起動中です (lock={self.lock_path})")

        try:
            prevent_windows_sleep()
            logger.info(
                "=== GMO AI Trader v2 デーモン起動 (mode=%s, fast=%ds, radar=%ds, expiry=%.1fh) ===",
                getattr(self.broker, "MODE", self.execution_mode),
                self.fast_loop_sec,
                self.radar_loop_sec,
                self.order_expiry_hours,
            )
            # 起動時 3-Way Reconciliation
            self.reconciler.reconcile(now_dt=_now_jst())
            self.last_reconcile_run_at = _now_jst()

            cycle_count = 0
            while True:
                now_jst = _now_jst()
                self.run_fast_cycle(now_dt=now_jst)

                # 5分 (300秒) ごとの 3-Way Reconciliation
                if (
                    not self.in_maintenance_sleep
                    and (
                        self.last_reconcile_run_at is None
                        or (now_jst - self.last_reconcile_run_at).total_seconds() >= 300.0
                    )
                ):
                    self.reconciler.reconcile(now_dt=now_jst)
                    self.last_reconcile_run_at = now_jst

                should_run_radar = (
                    self.last_radar_run_at is None
                    or (now_jst - self.last_radar_run_at).total_seconds() >= self.radar_loop_sec
                )
                if should_run_radar:
                    self.run_radar_and_ai_cycle(now_dt=now_jst)

                # RSS メモリ監視（ポジション・待機注文がゼロの安全なタイミングでのみ計画再起動）
                pf_chk = self.oco_manager.load_portfolio()
                has_active = bool((pf_chk.get("positions") or []) or (pf_chk.get("pending_orders") or []))
                mem_status = check_memory_rss_and_should_restart(has_open_positions_or_orders=has_active)
                if mem_status["should_graceful_restart"]:
                    logger.info("メモリ閾値到達かつポジションフラットのため計画再起動を実行します: %s", mem_status)
                    break

                cycle_count += 1
                if max_cycles is not None and cycle_count >= max_cycles:
                    break
                time.sleep(self.fast_loop_sec)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """デーモン終了時のリソース解放（WSトークン失効・WALクローズ・ロック解放）"""
        try:
            if getattr(self, "ws_engine", None) is not None:
                self.ws_engine.shutdown()
        except Exception:
            pass
        try:
            if getattr(self, "db_wal", None) is not None:
                self.db_wal.close()
        except Exception:
            pass
        try:
            if getattr(self, "instance_lock", None) is not None:
                self.instance_lock.release()
        except Exception:
            pass


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"v2_auto_{_now_jst().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    load_dotenv(BASE_DIR / ".env")
    setup_logging()

    parser = argparse.ArgumentParser(description="GMOコイン 24時間常時監視・現物全自動売買デーモン (v2)")
    parser.add_argument("--once", action="store_true", help="1サイクルのみ実行して終了する（動作確認用）")
    parser.add_argument("--force-ai", action="store_true", help="レーダー条件に関わらずAI判定を1回強制実行する")
    parser.add_argument("--dry-run-notify", action="store_true", help="Discord/LINEへの外部通知送信をスキップする")
    parser.add_argument("--probe-live", action="store_true", help="極小ロット実機プローブ検証 (capabilities.json 生成) を実行して終了する")
    parser.add_argument("--probe-symbol", type=str, default="XRP", help="プローブ検証に使用する銘柄 (既定: XRP)")
    parser.add_argument("--backtest", action="store_true", help="WFA・Purged/Embargo保守的現物バックテスト検証を実行して終了する")
    parser.add_argument("--remote-cmd", type=str, default="", help="リモート緊急制御コマンド (PAUSE, RESUME, CANCEL_ENTRIES, FLATTEN_ALL 等) を実行して終了する")
    args = parser.parse_args()

    daemon = AutoTraderDaemon(dry_run_notify=args.dry_run_notify)
    if args.probe_live:
        res = daemon.live_probe.run_probe()
        logger.info("Live Probe 完了: %s", res)
        return 0
    if args.backtest:
        logger.info("ConservativeSpotBacktester 準備完了: symbol_rules=%d 銘柄", len(daemon.backtester.symbol_rules))
        return 0
    if args.remote_cmd:
        res = daemon.remote_control.execute_command(args.remote_cmd)
        logger.info("Remote Control 実行完了: %s", res)
        return 0
    if args.once:
        daemon.run_fast_cycle()
        daemon.run_radar_and_ai_cycle(force_ai=args.force_ai)
        return 0

    daemon.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
