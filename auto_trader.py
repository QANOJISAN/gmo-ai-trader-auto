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
import gc
import logging
import os
import sys
import threading
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
from src.dpapi_vault import load_and_unprotect_dotenv
from src.gmo_client import GMOPublicClient
from src.gmo_private_client import (
    GMOAuthHaltError,
    GMOMaintenanceError,
    GMOPrivateClient,
    PaperBroker,
    create_broker_client,
    is_in_daily_fee_settlement_freeze_window,
)
from src.json_utils import load_json_file, write_json_file
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


DEFAULT_INITIAL_CAPITAL_JPY = 20000
DEFAULT_INITIAL_EQUITY_JPY = 20025
DEFAULT_PHASE1_END_JST = "2027-07-09T00:00:00+09:00"
DEFAULT_PHASE2_START_JST = "2027-07-10T00:00:00+09:00"
DEFAULT_PHASE2_END_JST = "2027-12-10T00:00:00+09:00"
DEFAULT_PRE_DEADLINE_FREEZE_HOURS = 2.0


def _parse_jst_iso(ts_str: Optional[Union[str, datetime]], default_iso: str) -> datetime:
    if isinstance(ts_str, datetime):
        return _now_jst(ts_str)
    raw = str(ts_str or "").strip() or default_iso
    try:
        return _now_jst(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except Exception:
        return _now_jst(datetime.fromisoformat(default_iso))


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
                "open_positions_count": len(
                    [
                        p
                        for p in (portfolio.get("positions", []) or [])
                        if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
                    ]
                ),
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
        phase1_end_jst: Optional[Union[str, datetime]] = None,
        phase2_start_jst: Optional[Union[str, datetime]] = None,
        phase2_end_jst: Optional[Union[str, datetime]] = None,
        pre_deadline_freeze_hours: Optional[float] = None,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else BASE_DIR
        if (self.base_dir / ".env").exists():
            load_and_unprotect_dotenv(self.base_dir / ".env")
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
        self.phase1_end_jst = _parse_jst_iso(
            phase1_end_jst if phase1_end_jst is not None else os.getenv("PHASE1_END_JST", DEFAULT_PHASE1_END_JST),
            DEFAULT_PHASE1_END_JST,
        )
        self.phase2_start_jst = _parse_jst_iso(
            phase2_start_jst if phase2_start_jst is not None else os.getenv("PHASE2_START_JST", DEFAULT_PHASE2_START_JST),
            DEFAULT_PHASE2_START_JST,
        )
        self.phase2_end_jst = _parse_jst_iso(
            phase2_end_jst if phase2_end_jst is not None else os.getenv("PHASE2_END_JST", DEFAULT_PHASE2_END_JST),
            DEFAULT_PHASE2_END_JST,
        )
        self.pre_deadline_freeze_hours = float(
            pre_deadline_freeze_hours
            if pre_deadline_freeze_hours is not None
            else os.getenv("CAMPAIGN_PRE_DEADLINE_FREEZE_HOURS", str(DEFAULT_PRE_DEADLINE_FREEZE_HOURS))
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
        self._last_fee_settlement_date: str = ""
        self._last_wal_truncate_date: str = ""
        self._phase1_pre_freeze_cleared: bool = False
        self._phase2_pre_freeze_cleared: bool = False
        self.campaign_phase: str = "PHASE1_ACTIVE"

    def absorb_daily_fee_fraction_settlement(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        毎朝 06:00 JST の GMOコイン手数料端数精算ウィンドウ（05:59:30〜06:00:30 JST）直後の 06:00:30 JST に、
        数十円の残高微増（<= 50 JPY）を `DAILY_FEE_FRACTION_SETTLEMENT` として台帳・L3基準値・HWMへ自動吸収する。
        """
        now_jst = _now_jst(now_dt)
        pf = self.oco_manager.load_portfolio()
        pending_buys = [
            o
            for o in (pf.get("pending_orders", []) or [])
            if str(o.get("side", "BUY")).upper() == "BUY"
            and str(o.get("state", "PENDING_BUY")).upper() in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED")
        ]
        if pending_buys:
            return {"status": "SKIPPED_PENDING_BUYS", "delta_jpy": 0}

        try:
            balances = self.broker.get_jpy_and_spot_balances()
            actual_jpy = int(round(float(balances.get("jpy_available", pf.get("jpy_balance", 0)) or 0)))
        except Exception as exc:
            logger.debug("06:00:30 手数料端数精算時の残高取得スキップ: %s", exc)
            return {"status": "SKIPPED_ERROR", "delta_jpy": 0}

        expected_jpy = int(round(float(pf.get("jpy_balance", actual_jpy) or actual_jpy)))
        delta_jpy = actual_jpy - expected_jpy
        if 0 < delta_jpy <= 50:
            pf["jpy_balance"] = actual_jpy
            pf["realized_pnl_total_jpy"] = int(pf.get("realized_pnl_total_jpy", 0) or 0) + delta_jpy
            pf["total_realized_pnl_jpy"] = pf["realized_pnl_total_jpy"]
            if pf.get("day_start_equity_jpy") is not None:
                pf["day_start_equity_jpy"] = float(pf.get("day_start_equity_jpy", 0) or 0) + delta_jpy
            if getattr(self.circuit_breaker, "_l3_day_start_equity_jpy", None) is not None:
                self.circuit_breaker._l3_day_start_equity_jpy = (
                    float(self.circuit_breaker._l3_day_start_equity_jpy or 0.0) + delta_jpy
                )
            prev_peak = float(pf.get("peak_equity_jpy", 0) or 0)
            if float(actual_jpy) > prev_peak:
                pf["peak_equity_jpy"] = actual_jpy
            pf["last_fee_fraction_settlement"] = {
                "type": "DAILY_FEE_FRACTION_SETTLEMENT",
                "delta_jpy": delta_jpy,
                "new_jpy_balance": actual_jpy,
                "timestamp": now_jst.isoformat(timespec="seconds"),
            }
            if self.db_wal is not None:
                try:
                    self.db_wal.record_fee_adjustment(
                        date_str=now_jst.strftime("%Y-%m-%d"),
                        amount_jpy=delta_jpy,
                        note="DAILY_FEE_FRACTION_SETTLEMENT (06:00:30 JST)",
                    )
                except Exception as wal_exc:
                    logger.warning("DAILY_FEE_FRACTION_SETTLEMENT WAL記録警告: %s", wal_exc)
            self.oco_manager.save_portfolio(pf, now_dt=now_jst)
            return {
                "status": "ABSORBED",
                "type": "DAILY_FEE_FRACTION_SETTLEMENT",
                "delta_jpy": delta_jpy,
                "new_jpy_balance": actual_jpy,
                "peak_equity_jpy": pf.get("peak_equity_jpy", actual_jpy),
            }
        return {"status": "NO_ADJUSTMENT", "delta_jpy": delta_jpy}

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

    def _execute_deadline_settlement(
        self,
        phase_label: str,
        settlement_reason: str,
        report_filename: str,
        settled_flag_key: str,
        now_dt: datetime,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        運用期限（第1回: 2027-07-09 00:00 JST / 第2回: 2027-12-10 00:00 JST）到達時に
        未約定注文の全件キャンセル・保有ポジションの即時成行全決済・総合レポート確定とDiscord通知を1回限り実行する。
        """
        now_jst = _now_jst(now_dt)
        settle_events: List[Dict[str, Any]] = []

        if tickers is None:
            try:
                tickers = self.public_client.get_tickers()
            except Exception:
                tickers = {}
        if tickers and hasattr(self.broker, "update_market_tickers"):
            try:
                self.broker.update_market_tickers(tickers)
            except Exception:
                pass

        # 1. 未約定の買い指値注文を全件キャンセル（部分約定分は安全にポジションへ昇格）
        try:
            cancel_res = self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
            if cancel_res.get("canceled_order_ids"):
                settle_events.append(
                    {
                        "type": f"{phase_label}_DEADLINE_PENDING_CANCELED",
                        "canceled_order_ids": cancel_res.get("canceled_order_ids", []),
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                )
        except Exception as exc:
            logger.error("%s 期限締め時の待機注文キャンセルエラー: %s", phase_label, exc)

        # 2. 保有中の全有効ポジション（DUST_QUARANTINED 除く）を即時成行全決済して日本円に確定
        pf_now = self.oco_manager.load_portfolio()
        for pos in list(pf_now.get("positions", []) or []):
            if str(pos.get("state", "PROTECTED")).strip().upper() == "DUST_QUARANTINED":
                continue
            sym = str(pos.get("symbol", "")).strip().upper()
            min_sz = float(DEFAULT_SYMBOL_RULES.get(sym, {}).get("min_order_size", 0.0001) or 0.0001)
            if float(pos.get("size", 0) or 0) < min_sz or not sym:
                continue
            try:
                closed_tr = self.oco_manager.close_position_immediately(
                    symbol=sym,
                    reason=settlement_reason,
                    now_dt=now_jst,
                )
                if closed_tr:
                    settle_events.append(
                        {
                            "type": settlement_reason,
                            "symbol": sym,
                            "size": closed_tr.get("size", 0),
                            "exit_price": closed_tr.get("exit_price", 0),
                            "pnl_jpy": closed_tr.get("pnl_jpy", 0),
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
            except Exception as exc:
                logger.error("%s 期限締め時のポジション全決済エラー (%s): %s", phase_label, sym, exc)

        # 3. 決済完了フラグの永続化（重複実行防止）
        pf_settled = self.oco_manager.load_portfolio()
        pf_settled[settled_flag_key] = True
        pf_settled[f"{settled_flag_key}_at"] = now_jst.isoformat(timespec="seconds")
        self.oco_manager.save_portfolio(pf_settled, now_dt=now_jst)
        if self.db_wal is not None:
            try:
                self.db_wal.set_meta(settled_flag_key, True)
            except Exception:
                pass

        # 4. PERFORMANCE_REPORT.md および フェーズ別アーカイブレポートの確定出力
        report_archive_path = self.base_dir / report_filename
        for out_p in (self.base_dir / "PERFORMANCE_REPORT.md", report_archive_path):
            try:
                generate_report(
                    output_path=out_p,
                    notify=False,
                    dry_run=True,
                    print_console=False,
                    portfolio_path=self.portfolio_path,
                    history_csv_path=self.history_csv_path,
                )
            except Exception as rep_exc:
                logger.error("%s レポート出力エラー (%s): %s", phase_label, out_p, rep_exc)

        # 5. 初期資金からの最終総合結果サマリーを Discord へ通知
        init_cap_val = int(pf_settled.get("initial_capital_jpy", DEFAULT_INITIAL_CAPITAL_JPY) or DEFAULT_INITIAL_CAPITAL_JPY)
        init_eq_val = int(pf_settled.get("initial_equity_jpy", init_cap_val + 25) or (init_cap_val + 25))
        final_jpy_val = int(pf_settled.get("jpy_balance", init_cap_val) or init_cap_val)
        total_pnl_val = final_jpy_val - init_cap_val
        total_pnl_pct = (total_pnl_val / init_cap_val * 100.0) if init_cap_val > 0 else 0.0
        closed_count = len(pf_settled.get("closed_trades", []) or [])
        settle_events.append(
            {
                "type": f"{phase_label}_SETTLEMENT_COMPLETED",
                "final_jpy_balance": final_jpy_val,
                "total_pnl_jpy": total_pnl_val,
                "report_archive": str(report_archive_path.name),
                "timestamp": now_jst.isoformat(timespec="seconds"),
            }
        )
        self.notification_dispatcher.dispatch(
            tier="CRITICAL",
            title=f"🏁 {phase_label} 運用期間満了・全ポジション決済＆最終総合レポート確定",
            message=(
                f"・満了時刻: `{now_jst.isoformat(timespec='seconds')}`\n"
                f"・初期運用資金: {init_cap_val:,} 円（実初期資産: {init_eq_val:,} 円）\n"
                f"・最終確定日本円残高: **{final_jpy_val:,} 円**（通算損益: **{total_pnl_val:+,} 円 / {total_pnl_pct:+.2f}%**）\n"
                f"・累計決済トレード数: {closed_count} 回\n"
                f"・確定アーカイブレポート: `{report_archive_path.name}`"
            ),
            now_dt=now_jst,
        )
        logger.info(
            "🏁 %s 運用期間満了決済＆レポート確定完了: 最終残高=%d円 (%+d円), report=%s",
            phase_label,
            final_jpy_val,
            total_pnl_val,
            report_archive_path.name,
        )
        return settle_events

    def evaluate_and_enforce_campaign_schedule(
        self,
        now_dt: Optional[datetime] = None,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        第1回（〜2027-07-09 00:00 JST）・検証休止期間（2027-07-09 00:00〜2027-07-10 00:00 JST）・
        第2回（2027-07-10 00:00〜2027-12-10 00:00 JST）の運用スケジュールを判定・自動執行する。
        """
        now_jst = _now_jst(now_dt)
        p1_end = self.phase1_end_jst
        p1_freeze_start = p1_end - timedelta(hours=self.pre_deadline_freeze_hours)
        p2_start = self.phase2_start_jst
        p2_end = self.phase2_end_jst
        p2_freeze_start = p2_end - timedelta(hours=self.pre_deadline_freeze_hours)

        events: List[Dict[str, Any]] = []

        # 0. portfolio.json に campaign_schedule メタデータが未登録の場合は自動初期化 ＆ SQLite WAL とフラグ双方向同期
        pf_init = self.oco_manager.load_portfolio()
        pf_dirty = False
        camp_meta = pf_init.get("campaign_schedule")
        if not isinstance(camp_meta, dict) or not camp_meta.get("phase1_end_jst"):
            pf_init["campaign_schedule"] = {
                "phase1_end_jst": p1_end.isoformat(timespec="seconds"),
                "phase2_start_jst": p2_start.isoformat(timespec="seconds"),
                "phase2_end_jst": p2_end.isoformat(timespec="seconds"),
                "pre_deadline_freeze_hours": self.pre_deadline_freeze_hours,
                "current_phase": self.campaign_phase,
            }
            pf_dirty = True

        for flag_k in ("phase1_settled", "phase2_started", "phase2_settled"):
            pf_flag = bool(pf_init.get(flag_k, False))
            wal_flag = False
            if self.db_wal is not None:
                try:
                    wal_flag = bool(self.db_wal.get_meta(flag_k, False))
                except Exception:
                    wal_flag = False
            if wal_flag and not pf_flag:
                pf_init[flag_k] = True
                pf_dirty = True
            elif pf_flag and not wal_flag and self.db_wal is not None:
                try:
                    self.db_wal.set_meta(flag_k, True)
                except Exception:
                    pass

        if pf_dirty:
            self.oco_manager.save_portfolio(pf_init, now_dt=now_jst)

        # 0b. 2027-07-09 00:00:00 JST 以降にまだ Phase 1 全決済が未完了の場合（24時間超の停電復旧時も含む）は最優先で Phase 1 締めを実行
        if now_jst >= p1_end and not bool(pf_init.get("phase1_settled", False)):
            events.extend(
                self._execute_deadline_settlement(
                    phase_label="PHASE1",
                    settlement_reason="PHASE1_DEADLINE_SETTLEMENT",
                    report_filename="PERFORMANCE_REPORT_PHASE1.md",
                    settled_flag_key="phase1_settled",
                    now_dt=now_jst,
                    tickers=tickers,
                )
            )
            pf_init = self.oco_manager.load_portfolio()

        # 1. 第1回 通常稼働期間 (< 2027-07-08 22:00:00 JST)
        if now_jst < p1_freeze_start:
            self.campaign_phase = "PHASE1_ACTIVE"
            return {
                "phase": "PHASE1_ACTIVE",
                "allow_new_buy": True,
                "halt_cycle": False,
                "events": events,
            }

        # 2. 第1回 終了2時間前エントリー凍結 & 2段階手仕舞い (2027-07-08 22:00:00 <= now < 2027-07-09 00:00:00 JST)
        if p1_freeze_start <= now_jst < p1_end:
            self.campaign_phase = "PHASE1_PRE_DEADLINE_FREEZE"
            pf = self.oco_manager.load_portfolio()
            if (pf.get("pending_orders") or []) or not self._phase1_pre_freeze_cleared:
                c_res = self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                self._phase1_pre_freeze_cleared = True
                if c_res.get("canceled_order_ids"):
                    events.append(
                        {
                            "type": "PHASE1_PRE_DEADLINE_PENDING_CANCELED",
                            "canceled_order_ids": c_res.get("canceled_order_ids", []),
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
            p1_graceful_start = p1_end - timedelta(minutes=30)
            p1_final_sweep_start = p1_end - timedelta(minutes=1)
            if p1_graceful_start <= now_jst < p1_final_sweep_start:
                unwind_res = self.oco_manager.graceful_maker_unwind_positions(
                    tickers=tickers,
                    now_dt=now_jst,
                    phase_label="PHASE1",
                )
                if unwind_res.get("unwound_orders") or unwind_res.get("closed_trades"):
                    events.append(
                        {
                            "type": "PHASE1_GRACEFUL_MAKER_UNWIND_STEP",
                            "detail": unwind_res,
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
            elif p1_final_sweep_start <= now_jst < p1_end:
                pf_s1 = self.oco_manager.load_portfolio()
                for pos_s1 in list(pf_s1.get("positions", []) or []):
                    if str(pos_s1.get("state", "")).strip().upper() == "DUST_QUARANTINED":
                        continue
                    s1_sym = str(pos_s1.get("symbol", "")).strip().upper()
                    if s1_sym:
                        closed_s1 = self.oco_manager.close_position_immediately(
                            symbol=s1_sym,
                            reason="PHASE1_FINAL_MARKET_SWEEP",
                            now_dt=now_jst,
                        )
                        if closed_s1:
                            events.append(
                                {
                                    "type": "PHASE1_FINAL_MARKET_SWEEP",
                                    "symbol": s1_sym,
                                    "exit_price": closed_s1.get("exit_price", 0),
                                    "pnl_jpy": closed_s1.get("pnl_jpy", 0),
                                    "timestamp": now_jst.isoformat(timespec="seconds"),
                                }
                            )
            return {
                "phase": "PHASE1_PRE_DEADLINE_FREEZE",
                "allow_new_buy": False,
                "halt_cycle": False,
                "events": events,
            }

        # 3. 第1回 運用期限到達＆24時間検証・改良休止ウィンドウ (2027-07-09 00:00:00 <= now < 2027-07-10 00:00:00 JST)
        if p1_end <= now_jst < p2_start:
            self.campaign_phase = "PHASE1_COMPLETED_INTERMISSION"
            return {
                "phase": "PHASE1_COMPLETED_INTERMISSION",
                "allow_new_buy": False,
                "halt_cycle": True,
                "events": events,
            }

        # 4. 第2回 通常稼働期間 (2027-07-10 00:00:00 <= now < 2027-12-09 22:00:00 JST)
        if p2_start <= now_jst < p2_freeze_start:
            self.campaign_phase = "PHASE2_ACTIVE"
            pf = self.oco_manager.load_portfolio()
            if not pf.get("phase2_started", False):
                pf["phase2_started"] = True
                pf["phase2_started_at"] = now_jst.isoformat(timespec="seconds")
                self.oco_manager.save_portfolio(pf, now_dt=now_jst)
                if self.db_wal is not None:
                    try:
                        self.db_wal.set_meta("phase2_started", True)
                    except Exception:
                        pass
                events.append(
                    {
                        "type": "PHASE2_CAMPAIGN_STARTED",
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                )
                logger.info("🚀 第2回（Phase 2）運用期間を開始しました (%s)", now_jst.isoformat(timespec="seconds"))
            return {
                "phase": "PHASE2_ACTIVE",
                "allow_new_buy": True,
                "halt_cycle": False,
                "events": events,
            }

        # 5. 第2回 終了2時間前エントリー凍結 & 2段階手仕舞い (2027-12-09 22:00:00 <= now < 2027-12-10 00:00:00 JST)
        if p2_freeze_start <= now_jst < p2_end:
            self.campaign_phase = "PHASE2_PRE_DEADLINE_FREEZE"
            pf = self.oco_manager.load_portfolio()
            if (pf.get("pending_orders") or []) or not self._phase2_pre_freeze_cleared:
                c_res = self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                self._phase2_pre_freeze_cleared = True
                if c_res.get("canceled_order_ids"):
                    events.append(
                        {
                            "type": "PHASE2_PRE_DEADLINE_PENDING_CANCELED",
                            "canceled_order_ids": c_res.get("canceled_order_ids", []),
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
            p2_graceful_start = p2_end - timedelta(minutes=30)
            p2_final_sweep_start = p2_end - timedelta(minutes=1)
            if p2_graceful_start <= now_jst < p2_final_sweep_start:
                unwind_res = self.oco_manager.graceful_maker_unwind_positions(
                    tickers=tickers,
                    now_dt=now_jst,
                    phase_label="PHASE2",
                )
                if unwind_res.get("unwound_orders") or unwind_res.get("closed_trades"):
                    events.append(
                        {
                            "type": "PHASE2_GRACEFUL_MAKER_UNWIND_STEP",
                            "detail": unwind_res,
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
            elif p2_final_sweep_start <= now_jst < p2_end:
                pf_s2 = self.oco_manager.load_portfolio()
                for pos_s2 in list(pf_s2.get("positions", []) or []):
                    if str(pos_s2.get("state", "")).strip().upper() == "DUST_QUARANTINED":
                        continue
                    s2_sym = str(pos_s2.get("symbol", "")).strip().upper()
                    if s2_sym:
                        closed_s2 = self.oco_manager.close_position_immediately(
                            symbol=s2_sym,
                            reason="PHASE2_FINAL_MARKET_SWEEP",
                            now_dt=now_jst,
                        )
                        if closed_s2:
                            events.append(
                                {
                                    "type": "PHASE2_FINAL_MARKET_SWEEP",
                                    "symbol": s2_sym,
                                    "exit_price": closed_s2.get("exit_price", 0),
                                    "pnl_jpy": closed_s2.get("pnl_jpy", 0),
                                    "timestamp": now_jst.isoformat(timespec="seconds"),
                                }
                            )
            return {
                "phase": "PHASE2_PRE_DEADLINE_FREEZE",
                "allow_new_buy": False,
                "halt_cycle": False,
                "events": events,
            }

        # 6. 第2回 運用期限満了＆安全待機 (now >= 2027-12-10 00:00:00 JST)
        self.campaign_phase = "PHASE2_COMPLETED_HALT"
        pf = self.oco_manager.load_portfolio()
        if not pf.get("phase2_settled", False):
            events.extend(
                self._execute_deadline_settlement(
                    phase_label="PHASE2",
                    settlement_reason="PHASE2_DEADLINE_SETTLEMENT",
                    report_filename="PERFORMANCE_REPORT_PHASE2.md",
                    settled_flag_key="phase2_settled",
                    now_dt=now_jst,
                    tickers=tickers,
                )
            )
        return {
            "phase": "PHASE2_COMPLETED_HALT",
            "allow_new_buy": False,
            "halt_cycle": True,
            "events": events,
        }

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

        # 0. SQLite WAL のリモート制御コマンドポーリング & 06:00 JST デイリーダイジェスト送信 / 06:00:30 端数精算 / 06:05 WAL TRUNCATE
        self.remote_control.poll_and_execute_db_commands(now_dt=now_jst)
        if now_jst.hour >= 6:
            digest_day = now_jst.strftime("%Y-%m-%d")
            if self._last_digest_date != digest_day:
                self._last_digest_date = digest_day
                self.notification_dispatcher.flush_daily_digest(now_dt=now_jst)
                try:
                    self.db_wal.checkpoint_wal("PASSIVE")
                except Exception as wal_exc:
                    logger.warning("日次 WAL チェックポイント警告: %s", wal_exc)
            if (now_jst.hour > 6 or now_jst.minute > 0 or now_jst.second >= 30) and self._last_fee_settlement_date != digest_day:
                self._last_fee_settlement_date = digest_day
                try:
                    self.absorb_daily_fee_fraction_settlement(now_dt=now_jst)
                except Exception as fee_exc:
                    logger.warning("06:00:30 日次手数料端数精算警告: %s", fee_exc)
            if (now_jst.hour > 6 or now_jst.minute >= 5) and self._last_wal_truncate_date != digest_day:
                self._last_wal_truncate_date = digest_day
                try:
                    self.db_wal.checkpoint_truncate()
                except Exception as tr_exc:
                    logger.warning("06:05 日次 WAL TRUNCATE チェックポイント警告: %s", tr_exc)

        # 0a. 第1回・第2回 運用期限スケジュール判定・自動全決済チェック
        campaign_res = self.evaluate_and_enforce_campaign_schedule(now_dt=now_jst, tickers=tickers)
        if campaign_res.get("halt_cycle"):
            return {
                "status": campaign_res["phase"],
                "campaign_phase": campaign_res["phase"],
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": campaign_res.get("events", []),
            }

        # 0b. 土曜 08:50〜08:55 JST 未約定 BUY 事前クリア
        if is_in_saturday_pre_maintenance_clear_window(now_jst) and not self.pre_maintenance_cleared:
            self.clear_pending_buys_before_maintenance(now_dt=now_jst)

        # 1. 土曜メンテナンス枠チェック (08:55〜11:05 JST) または 臨時・水曜メンテナンスステータス検知
        ex_stat_clean = (exchange_status or "").strip().upper()
        if is_in_saturday_maintenance_window(now_jst) or (not self.in_maintenance_sleep and ex_stat_clean in ("MAINTENANCE", "PREOPEN")):
            if not self.in_maintenance_sleep:
                self.in_maintenance_sleep = True
                logger.info("🔧 取引所メンテナンス枠 (土曜定時または臨時/水曜メンテ) に入ったため自動スリープを開始します。")
                try:
                    self.db_wal.checkpoint_truncate()
                except Exception:
                    pass
                gc.collect()
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
                        exec_size_pre = float(p_ord.get("executed_size", 0) or 0)
                        try:
                            fetched_pre = self.broker.get_orders([p_oid])
                            if fetched_pre:
                                exec_size_pre = float(fetched_pre[0].get("executed_size", exec_size_pre) or 0)
                        except Exception:
                            pass
                        min_sz_pre = float(
                            DEFAULT_SYMBOL_RULES.get(p_sym, {}).get("min_order_size", 0.0001) or 0.0001
                        )
                        if 0 < exec_size_pre < min_sz_pre and not bool(p_ord.get("force_cancel_on_crash", False)):
                            remaining_pending.append(p_ord)
                            continue
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
                        if exec_size_pre > 0:
                            # 部分約定または全約定していた場合は step() で SL_ACTIVE / DUST_QUARANTINED へ処理させる
                            remaining_pending.append(p_ord)
                        else:
                            if self.db_wal is not None:
                                self.db_wal.upsert_order(
                                    order_id=p_oid,
                                    symbol=p_sym,
                                    side="BUY",
                                    execution_type="LIMIT",
                                    state="CANCELED",
                                    price=str(p_ord.get("limit_price", "0")),
                                    size=str(p_ord.get("size", "0")),
                                )
                            pending_modified = True
                        continue
                remaining_pending.append(p_ord)
            if pending_modified:
                pf_pre["pending_orders"] = remaining_pending
                self.oco_manager.save_portfolio(pf_pre, now_dt=now_jst)

            step_events = self.oco_manager.step(tickers=current_tickers, now_dt=now_jst)
            events = list(campaign_res.get("events", [])) + guardian_events + adverse_cancel_events + step_events

            # 待機中BUY指値が逆選択ガードや期限切れでキャンセルされた場合は latest_decision.json の表示状態も同期
            for ev in adverse_cancel_events + step_events:
                ev_type = str(ev.get("type", "")).upper()
                if ev_type in ("ADVERSE_SELECTION_RESTING_BUY_CANCELED", "BUY_ORDER_EXPIRED"):
                    c_oid = str(ev.get("order_id", "") or "")
                    c_reason = (
                        f"逆選択ガード発動 ({ev.get('reason', '')})"
                        if ev_type == "ADVERSE_SELECTION_RESTING_BUY_CANCELED"
                        else f"有効期限({self.order_expiry_hours:.1f}h)タイムアウト自動取消"
                    )
                    self._sync_latest_decision_on_pending_cancel(
                        canceled_order_ids=[c_oid] if c_oid else [],
                        cancel_reason=c_reason,
                        now_dt=now_jst,
                    )

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

            # L5 ハード・キルスイッチ (Best Bid * (1 - Taker Fee) で3回連続 < max(17000, peak_equity * 0.85)) チェック
            pf_now = self.oco_manager.load_portfolio()
            l5_res = self.circuit_breaker.evaluate_l5_hard_kill_switch(pf_now, current_tickers)
            if l5_res.get("peak_updated"):
                self.oco_manager.save_portfolio(pf_now, now_dt=now_jst)
            if l5_res["newly_triggered"]:
                self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                pf_now = self.oco_manager.load_portfolio()
                for l5_pos in list(pf_now.get("positions", []) or []):
                    if str(l5_pos.get("state", "PROTECTED")).upper() == "DUST_QUARANTINED":
                        continue
                    l5_sym = str(l5_pos.get("symbol", "")).strip().upper()
                    min_sz = float(DEFAULT_SYMBOL_RULES.get(l5_sym, {}).get("min_order_size", 0.0001) or 0.0001)
                    if float(l5_pos.get("size", 0) or 0) < min_sz:
                        continue
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
                    title=f"L5 ハード・キルスイッチ発動（3回連続 Best Bid 清算価値 < {l5_res['threshold_jpy']:,.0f}円）",
                    message=(
                        f"・保守的清算総資産: {l5_res['conservative_equity_jpy']:,.0f} 円 "
                        f"(< {l5_res['threshold_jpy']:,.0f} 円 × 3回連続)\n"
                        "・未約定の買い指値を全キャンセルし、全保有ポジションを即時成行決済の上、新規BUYを恒久ロックしました。"
                    ),
                    now_dt=now_jst,
                )

            if events:
                self._sync_pwa_snapshot(include_charts=False, force_push=True)

            self.record_api_success()
            return {
                "status": "OK",
                "campaign_phase": campaign_res["phase"],
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

        campaign_res = self.evaluate_and_enforce_campaign_schedule(now_dt=now_jst, tickers=tickers)
        if campaign_res.get("halt_cycle"):
            return {
                "status": campaign_res["phase"],
                "campaign_phase": campaign_res["phase"],
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "ai_called": False,
                "events": campaign_res.get("events", []),
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
            self.remote_control.poll_and_execute_db_commands(now_dt=now_jst)
            pf = self.oco_manager.load_portfolio()
            held_symbols = [
                str(p.get("symbol", "")).upper()
                for p in (pf.get("positions", []) or [])
                if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
            ]

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

            cooldown_syms = {
                str(s).strip().upper()
                for s in (market_summaries or {}).keys()
                if self.ai_judge.is_symbol_on_ai_cooldown(s, now_dt=now_jst)[0]
                or self.ai_judge.is_symbol_on_sl_cooldown(s, now_dt=now_jst)[0]
            }
            radar_eval = self.radar.evaluate_triggers(
                market_summaries=market_summaries,
                portfolio=pf,
                symbol_rules=rules,
                last_ai_call_at=self.last_ai_call_at,
                now_dt=now_jst,
                cooldown_symbols=cooldown_syms,
            )
            self.last_radar_run_at = now_jst

            if not campaign_res.get("allow_new_buy", True):
                filtered_trigs = [t for t in radar_eval.get("triggers", []) if t != "NEW_ENTRY_OPPORTUNITY"]
                radar_eval["triggers"] = filtered_trigs
                radar_eval["candidate_symbols"] = []
                if not filtered_trigs:
                    radar_eval["should_call_ai"] = False
                    radar_eval["reason_summary"] = (
                        f"{campaign_res['phase']}: 運用期限2時間前エントリー凍結中（新規買い停止・AI呼び出しスキップ）"
                    )

            # RISK_OFF_CRASH 検知時は未約定の PENDING_BUY を即時キャンセル
            regime_info = radar_eval.get("regime_info", {})
            if regime_info.get("should_cancel_pending_buys") and (pf.get("pending_orders") or []):
                c_res = self.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=now_jst)
                pf = self.oco_manager.load_portfolio()
                canceled_ids = c_res.get("canceled_buy_order_ids", []) or []
                if canceled_ids:
                    crash_reason_str = " / ".join(regime_info.get("reasons", []) or ["RISK_OFF_CRASH"])
                    logger.warning(
                        "⚠️ RISK_OFF_CRASH 検知により待機中BUY指値を自動キャンセルしました: order_ids=%s (%s)",
                        canceled_ids,
                        crash_reason_str,
                    )
                    self.notifier.notify_event(
                        title="相場急変ガード (RISK_OFF_CRASH) による待機中指値自動取消",
                        message=(
                            f"・取消注文ID: `{', '.join(canceled_ids)}`\n"
                            f"・発動理由: {crash_reason_str}\n"
                            "・未約定の新規買い指値を安全のため自動キャンセルしました（手動操作は不要です）。"
                        ),
                        level="WARN",
                        dry_run=self.dry_run_notify,
                    )
                    self._sync_latest_decision_on_pending_cancel(
                        canceled_order_ids=canceled_ids,
                        cancel_reason=f"RISK_OFF_CRASH ({crash_reason_str})",
                        now_dt=now_jst,
                    )

            if not radar_eval["should_call_ai"] and not force_ai:
                logger.info("📡 レーダースキャン完了: %s", radar_eval["reason_summary"])
                self._sync_pwa_snapshot()
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

                is_paused_now = bool(self.remote_control.is_paused or pf.get("is_paused", False))
                self.remote_control.is_paused = is_paused_now

                block_reason = ""
                if not campaign_res.get("allow_new_buy", True):
                    block_reason = f"[{campaign_res['phase']}: 運用期限2時間前エントリー凍結中によりHOLDへ自動補正] "
                elif is_in_daily_fee_settlement_freeze_window(now_jst):
                    block_reason = "[05:59:30〜06:00:30 JST 手数料端数精算ウィンドウ保護によりHOLDへ自動補正] "
                    validated["deferred_by_fee_settlement_window"] = True
                elif is_paused_now:
                    block_reason = "[リモートPAUSE中によりHOLDへ自動補正] "
                elif self.circuit_breaker.l5_kill_switch_latched:
                    block_reason = "[L5ハード・キルスイッチ発動中によりHOLDへ自動補正] "
                elif l3_res["blocked"]:
                    block_reason = f"[L3日次損失制限 ({l3_res['daily_total_pnl_jpy']:+.0f}円 <= {l3_res.get('daily_loss_limit_jpy', -300.0):.0f}円) により翌06:00までHOLD] "
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
                try:
                    execution_result = self.oco_manager.place_new_buy_order(
                        decision=validated,
                        tickers=tickers,
                        now_dt=now_jst,
                    )
                except Exception as ord_exc:
                    logger.warning("⚠️ BUY候補検知 (%s) ➔ 発注例外により見送り: %s", target_sym, ord_exc)
                    execution_result = {
                        "action": "BUY",
                        "status": "ORDER_REJECTED",
                        "error": str(ord_exc),
                    }

                if execution_result.get("status") == "PENDING_BUY":
                    self._increment_daily_trade_count(now_jst)
                    self.ai_judge.record_symbol_ai_call(target_sym, now_dt=now_jst)
                else:
                    skip_reason = str(
                        execution_result.get("error")
                        or execution_result.get("reason")
                        or f"Post-Only即時約定拒否または発注失効 (status={execution_result.get('status', 'SKIPPED')})"
                    )
                    logger.warning("⚠️ BUY候補検知 (%s) ➔ 見送り（理由：%s）", target_sym, skip_reason)
                    validated["original_action"] = "BUY"
                    validated["action"] = "HOLD"
                    validated["order_type"] = "NONE"
                    validated["status"] = f"HOLD ({execution_result.get('status', 'ORDER_SKIPPED')})"
                    validated["skip_reason"] = skip_reason
                    validated["reasoning"] = (
                        f"【⚠️ BUY候補検知 ({target_sym}) ➔ 見送り（理由：{skip_reason}）】 "
                        + str(validated.get("reasoning", ""))
                    )
                    action = "HOLD"
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
            else:
                # HOLD 判定時も NEW_ENTRY_OPPORTUNITY 候補銘柄にクールダウンを記録し、15分ごとの同一銘柄API連打・429枯渇を防止
                for cand_sym in radar_eval.get("candidate_symbols", []) or []:
                    self.ai_judge.record_symbol_ai_call(str(cand_sym), now_dt=now_jst)

            validated["execution_result"] = execution_result
            validated["manual_instruction"] = build_manual_order_guide(validated)
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
            self._sync_pwa_snapshot(include_charts=True, force_push=True)
            # 3階層通知: HOLD の場合はスパム防止のため DAILY_DIGEST バッファへ送り、BUY/SELL/MODIFY（またはBUY見送り警告）は通知
            if action == "HOLD" and validated.get("original_action") != "BUY":
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

    def _sync_latest_decision_on_pending_cancel(
        self,
        canceled_order_ids: List[str],
        cancel_reason: str,
        now_dt: Optional[datetime] = None,
    ) -> None:
        """
        待機中だった BUY 指値がリスクオフ・逆選択ガード・有効期限等で自動キャンセルされた際、
        `latest_decision.json` の表示が「発注中 (BUY / PENDING_BUY)」のまま残らないよう
        `HOLD (ORDER_CANCELED)` へ自動同期する。
        """
        if not self.latest_decision_path.exists():
            return
        try:
            now_jst = _now_jst(now_dt)
            latest = load_json_file(self.latest_decision_path)
            if not isinstance(latest, dict):
                return
            exec_res = latest.get("execution_result") if isinstance(latest.get("execution_result"), dict) else {}
            pending_ord = exec_res.get("pending_order") if isinstance(exec_res.get("pending_order"), dict) else {}
            latest_oid = str(pending_ord.get("order_id") or exec_res.get("order", {}).get("order_id") or "").strip()
            latest_act = str(latest.get("action", "")).upper()
            latest_exec_st = str(exec_res.get("status", "")).upper()

            if latest_act != "BUY" and latest_exec_st != "PENDING_BUY":
                return
            if canceled_order_ids and latest_oid and latest_oid not in {str(x).strip() for x in canceled_order_ids}:
                return

            sym = str(latest.get("symbol", "NONE"))
            oid_label = latest_oid or (str(canceled_order_ids[0]) if canceled_order_ids else "不明")
            latest["original_action"] = "BUY"
            latest["action"] = "HOLD"
            latest["status"] = "HOLD (ORDER_CANCELED)"
            latest["skip_reason"] = f"注文ID:{oid_label} 自動取消 ({cancel_reason})"
            if not isinstance(latest.get("execution_result"), dict):
                latest["execution_result"] = {}
            latest["execution_result"]["status"] = "CANCELED"
            latest["execution_result"]["cancel_reason"] = cancel_reason
            latest["execution_result"]["canceled_at"] = now_jst.isoformat(timespec="seconds")
            prefix = f"【⚠️ BUY候補検知・指値発注後自動取消 ({sym} #{oid_label}) ➔ 見送り（理由：{cancel_reason}）】 "
            old_reasoning = str(latest.get("reasoning", ""))
            if not old_reasoning.startswith("【⚠️"):
                latest["reasoning"] = prefix + old_reasoning
            latest["manual_instruction"] = build_manual_order_guide(latest)
            write_json_file(self.latest_decision_path, latest)
        except Exception as exc:
            logger.debug("latest_decision.json 取消ステータス同期スキップ: %s", exc)

    def _sync_pwa_snapshot(self, include_charts: bool = True, force_push: bool = False) -> None:
        """
        本番ワークスペース稼働時に docs/ 以下の静的PWAスナップショットを最新状態へ同期し、
        GitHub Pagesへ非同期Pushする（メイン売買ループを0msもブロックしない完全バックグラウンド実行）。
        """
        if self.base_dir != BASE_DIR:
            return

        def _bg_export() -> None:
            try:
                from web_dashboard import WebDashboardService

                svc = WebDashboardService(base_dir=self.base_dir, public_client=self.public_client)
                try:
                    svc.export_static_pwa_bundle(
                        output_dir=self.base_dir / "docs",
                        include_charts=include_charts,
                        auto_push=True,
                        force_push=force_push,
                    )
                finally:
                    svc.close()
            except Exception as exc:
                logger.debug("PWA snapshot sync skipped: %s", exc)

        threading.Thread(target=_bg_export, daemon=True).start()

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
            # 起動時 3-Way Reconciliation（土曜定期メンテナンス枠を除く）
            now_start = _now_jst()
            if not is_in_saturday_maintenance_window(now_start):
                try:
                    self.reconciler.reconcile(now_dt=now_start)
                    self.last_reconcile_run_at = now_start
                except Exception as rec_exc:
                    logger.warning("起動時 3-Way Reconciliation スキップ (メンテ中等): %s", rec_exc)

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
                    rec_periodic = self.reconciler.reconcile(now_dt=now_jst)
                    self.last_reconcile_run_at = now_jst
                    if rec_periodic.get("ghost_orders_resolved"):
                        pf_post_rec = self.oco_manager.load_portfolio()
                        if not (pf_post_rec.get("pending_orders") or []):
                            self._sync_latest_decision_on_pending_cancel(
                                canceled_order_ids=rec_periodic.get("ghost_orders_resolved", []),
                                cancel_reason="取引所有効注文照合による失効・取消同期",
                                now_dt=now_jst,
                            )

                should_run_radar = (
                    not self.in_maintenance_sleep
                    and not is_in_saturday_maintenance_window(now_jst)
                    and (
                        self.last_radar_run_at is None
                        or (now_jst - self.last_radar_run_at).total_seconds() >= self.radar_loop_sec
                    )
                )
                if should_run_radar:
                    self.run_radar_and_ai_cycle(now_dt=now_jst)

                # RSS メモリ監視（ポジション・待機注文がゼロの安全なタイミングでのみ計画再起動）
                pf_chk = self.oco_manager.load_portfolio()
                active_pos_list = [
                    p
                    for p in (pf_chk.get("positions") or [])
                    if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
                ]
                has_active = bool(active_pos_list or (pf_chk.get("pending_orders") or []))
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


class _JSTDailyFileHandler(logging.FileHandler):
    """常駐デーモンが日付を跨いでも自動的に当日日付の `v2_auto_YYYYMMDD.log` へ切り替えるハンドラ"""

    def __init__(self, log_dir: Path, encoding: str = "utf-8") -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._current_date_str = _now_jst().strftime("%Y%m%d")
        initial_path = self.log_dir / f"v2_auto_{self._current_date_str}.log"
        super().__init__(initial_path, encoding=encoding)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            new_date_str = _now_jst().strftime("%Y%m%d")
            if new_date_str != self._current_date_str:
                self._current_date_str = new_date_str
                new_path = self.log_dir / f"v2_auto_{new_date_str}.log"
                if self.stream:
                    try:
                        self.stream.flush()
                        self.stream.close()
                    except Exception:
                        pass
                    self.stream = None
                self.baseFilename = os.fspath(new_path)
                self.stream = self._open()
        except Exception:
            pass
        super().emit(record)


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            _JSTDailyFileHandler(LOG_DIR, encoding="utf-8"),
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

    load_and_unprotect_dotenv(BASE_DIR / ".env")
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
    try:
        if args.probe_symbol:
            daemon.live_probe.probe_symbol = str(args.probe_symbol).strip().upper()
        if args.probe_live:
            res = daemon.live_probe.run_probe()
            logger.info("Live Probe 完了: %s", res)
            return 0
        if args.backtest:
            logger.info("ConservativeSpotBacktester 準備完了: symbol_rules=%d 銘柄", len(daemon.backtester.symbol_rules))
            return 0
        if args.remote_cmd:
            daemon_running = daemon.instance_lock.is_locked_by_another_process()
            res = daemon.remote_control.enqueue_or_execute_command(
                args.remote_cmd,
                is_daemon_running=daemon_running,
            )
            logger.info("Remote Control 実行完了: %s", res)
            return 0
        if args.once:
            rec_res = daemon.reconciler.reconcile()
            logger.info("初回 3-Way Reconciliation 完了: %s", rec_res)
            daemon.run_fast_cycle()
            cycle_res = daemon.run_radar_and_ai_cycle(force_ai=args.force_ai)
            if not cycle_res.get("ai_called"):
                pf_now = daemon.oco_manager.load_portfolio()
                if not daemon.latest_decision_path.exists():
                    radar_info = cycle_res.get("radar", {})
                    write_json_file(
                        daemon.latest_decision_path,
                        {
                            "timestamp": cycle_res.get("timestamp", _now_jst().isoformat(timespec="seconds")),
                            "action": "HOLD",
                            "symbol": "NONE",
                            "order_type": "NONE",
                            "limit_price": 0,
                            "size": 0,
                            "take_profit_price": 0,
                            "stop_loss_price": 0,
                            "expected_cost_jpy": 0,
                            "expected_loss_jpy": 0,
                            "risk_reward_ratio": 0.0,
                            "confidence": 0.5,
                            "provider": "radar_gate_hold",
                            "execution_mode": getattr(daemon.broker, "MODE", daemon.execution_mode),
                            "radar_triggers": radar_info.get("triggers", []),
                            "regime_info": radar_info.get("regime_info", {}),
                            "reasoning": str(radar_info.get("reason_summary", "レーダー条件未達のためAI呼び出しスキップ（HOLD待機）")),
                            "status": "RADAR_SKIPPED_AI",
                        },
                    )
                generate_report(
                    output_path=daemon.base_dir / "PERFORMANCE_REPORT.md",
                    notify=False,
                    dry_run=True,
                    print_console=False,
                    portfolio_path=daemon.portfolio_path,
                    history_csv_path=daemon.history_csv_path,
                )
            return 0

        daemon.run_forever()
        return 0
    finally:
        daemon.shutdown()


if __name__ == "__main__":
    sys.exit(main())
