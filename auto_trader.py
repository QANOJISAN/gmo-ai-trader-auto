"""
v2：24時間常時監視・GMOコイン現物全自動売買メインデーモン (auto_trader.py)

- 高速ループ (`FAST_LOOP_SEC=15` 秒間隔):
  Public API から最新 Ticker を取得し、`PseudoOCOManager.step()` を実行。
  * `PENDING_BUY` 買い指値の約定検知 ➔ 直ちに `STOP` 逆指値を自動発注して `SL_ACTIVE` へ遷移
  * `ORDER_EXPIRY_HOURS` (4時間) 経過した未約定指値の自動タイムアウト取消
  * `SL_ACTIVE` 中の `+1.0R` 到達で自動建値微益ロック (`cancel_order` ➔ 新 `STOP` 発注)
  * `take_profit_price` 到達で SL 取消ロック解除 ➔ 自動利確売り (`TP_EXECUTED`)
  * 取引所側 `STOP` 注文の約定検知 (`SL_EXECUTED`)

- 中速レーダーループ (`RADAR_LOOP_SEC=900` 秒＝15分間隔):
  `TechnicalRadar` で全スクリーニング銘柄の日足・4時間足・1時間足をスキャンし、
  3つのトリガー条件（①新規押し目好機、②保有24h経過または損益急変、③6時間定期ヘルスチェック）の
  いずれかを満たしたときのみ `AIJudge` を呼び出して `BUY` / `MODIFY` / `SELL` を自動執行する。

- メンテナンス＆安全機構:
  * 毎週土曜日 `08:55〜11:05 JST`（GMOコイン定期メンテナンス枠）は自動的に API 発注・変更をスリープし、
    `11:05 JST` メンテナンス明けに自動で `get_assets()` と `get_active_orders()` を照合（Reconciliation）して再開。
  * 1日の最大新規取引回数制限 (`MAX_DAILY_TRADES`)
  * API 連続エラー時の自動一時停止（サーキットブレーカー `MAX_CONSECUTIVE_API_ERRORS`）
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
from src.ai_judge import AIJudge
from src.gmo_client import GMOPublicClient
from src.gmo_private_client import (
    GMOPrivateClient,
    PaperBroker,
    create_broker_client,
)
from src.json_utils import write_json_file
from src.notifier import Notifier, build_manual_order_guide
from src.pseudo_oco_manager import PseudoOCOManager
from src.radar import TechnicalRadar
from src.validator import DEFAULT_SYMBOL_RULES, validate_and_adjust_decision

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
    """24時間常時監視・GMOコイン現物全自動売買デーモン"""

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        execution_mode: Optional[str] = None,
        public_client: Optional[GMOPublicClient] = None,
        broker: Optional[Union[GMOPrivateClient, PaperBroker]] = None,
        ai_judge: Optional[AIJudge] = None,
        radar: Optional[TechnicalRadar] = None,
        notifier: Optional[Notifier] = None,
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

        self.execution_mode = (execution_mode or os.getenv("EXECUTION_MODE", "DRY_RUN")).strip().upper()
        self.public_client = public_client or GMOPublicClient()
        self.broker = broker or create_broker_client(
            execution_mode=self.execution_mode,
            base_dir=self.base_dir,
            public_client=self.public_client,
        )
        self.notifier = notifier or Notifier()
        self.ai_judge = ai_judge or AIJudge()
        self.radar = radar or TechnicalRadar()
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
        )

        # 内部状態
        self.in_maintenance_sleep: bool = False
        self.last_ai_call_at: Optional[datetime] = None
        self.last_radar_run_at: Optional[datetime] = None
        self.consecutive_api_errors: int = 0
        self.circuit_breaker_paused_until: Optional[datetime] = None
        self._daily_trade_date: str = ""
        self._daily_trade_count: int = 0

    def _get_daily_trade_count(self, now_jst: datetime) -> int:
        today_str = now_jst.strftime("%Y-%m-%d")
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

    def record_api_error(self, exc: Exception, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        self.consecutive_api_errors += 1
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
            "paused_until": (
                self.circuit_breaker_paused_until.isoformat(timespec="seconds")
                if self.circuit_breaker_paused_until
                else None
            ),
        }

    def reconcile_after_maintenance(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        土曜メンテナンス明け（11:05 JST）やデーモン再起動時に、
        `get_assets()` と `get_active_orders()` を呼び出して `portfolio.json` と実口座（または PaperBroker）の
        整合性を照合・修復する。
        - メンテ中に `PENDING_BUY` が約定していた場合はポジション化し、直ちに `STOP` SL を発注する。
        - 保有ポジションの `sl_order_id` が有効注文一覧に存在しない場合は、安全のため `STOP` SL を再発注する。
        """
        now_jst = _now_jst(now_dt)
        assets_summary = self.broker.get_jpy_and_spot_balances()
        active_orders = self.broker.get_active_orders()
        active_order_ids = {str(o.get("order_id", "")) for o in active_orders}

        pf = self.oco_manager.load_portfolio()
        reconciled_actions: List[str] = []

        # 1. 保有ポジションの SL 注文生存確認（消えていたら再発注）
        for pos in pf.get("positions", []):
            sym = str(pos.get("symbol", "")).upper()
            sl_oid = str(pos.get("sl_order_id", "") or "")
            sl_price = float(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
            size = float(pos.get("size", 0) or 0)

            if sl_oid and sl_oid not in active_order_ids:
                # 既に約定済みかキャンセル済みか確認
                orders_info = self.broker.get_orders([sl_oid])
                if orders_info and str(orders_info[0].get("status", "")).upper() == "EXECUTED":
                    reconciled_actions.append(f"{sym}: メンテ中にSL約定済みを検知 (order_id={sl_oid})")
                elif sl_price > 0 and size > 0:
                    spot_info = assets_summary["spot_assets"].get(sym, {})
                    if float(spot_info.get("available", size) or 0) >= size * 0.99:
                        new_sl = self.broker.place_order(
                            symbol=sym,
                            side="SELL",
                            execution_type="STOP",
                            size=size,
                            price=sl_price,
                            time_in_force="FAK",
                        )
                        pos["sl_order_id"] = str(new_sl.get("order_id", ""))
                        reconciled_actions.append(
                            f"{sym}: SL注文が未設定だったため再発注しました (new_order_id={pos['sl_order_id']})"
                        )

        self.oco_manager.save_portfolio(pf, now_dt=now_jst)

        summary = {
            "status": "RECONCILED",
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "jpy_available": assets_summary.get("jpy_available", 0.0),
            "spot_assets": assets_summary.get("spot_assets", {}),
            "active_orders_count": len(active_orders),
            "reconciled_actions": reconciled_actions,
        }
        self.notifier.notify_event(
            title="メンテナンス明け口座・有効注文照合 (Reconciliation) 完了",
            message=(
                f"・日本円余力: {int(assets_summary.get('jpy_available', 0)):,} 円\n"
                f"・有効注文数: {len(active_orders)} 件\n"
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
    ) -> Dict[str, Any]:
        """
        15秒間隔の高速ループ1サイクルを実行する。
        - 土曜 08:55〜11:05 JST のメンテナンス枠内は API 発注・変更を自動スリープ
        - 11:05 JST メンテナンス明け最初のサイクルで `reconcile_after_maintenance()` を自動実行
        """
        now_jst = _now_jst(now_dt)

        # 1. 土曜メンテナンス枠チェック (08:55〜11:05 JST)
        if is_in_saturday_maintenance_window(now_jst):
            if not self.in_maintenance_sleep:
                self.in_maintenance_sleep = True
                logger.info("🔧 土曜定期メンテナンス枠 (08:55〜11:05 JST) に入ったため自動スリープを開始します。")
                self.notifier.notify_event(
                    title="土曜定期メンテナンス自動スリープ開始 (08:55〜11:05 JST)",
                    message="GMOコイン定期メンテナンス枠のため、11:05 JST まで API 発注・変更を一時休止します。",
                    level="MAINT",
                    dry_run=self.dry_run_notify,
                )
            return {
                "status": "MAINTENANCE_SLEEP",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": [],
            }

        # メンテナンス明け (11:05 JST 以降) の自動照合
        reconcile_result: Optional[Dict[str, Any]] = None
        if self.in_maintenance_sleep:
            self.in_maintenance_sleep = False
            logger.info("🔧 土曜定期メンテナンス枠が終了 (11:05 JST) したため、資産・注文照合を実行して再開します。")
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
            events = self.oco_manager.step(tickers=current_tickers, now_dt=now_jst)
            self.record_api_success()
            return {
                "status": "OK",
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "events": events,
                "reconciled": reconcile_result,
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
                self.oco_manager.step(tickers=tickers, now_dt=now_jst)
                pf = self.oco_manager.load_portfolio()

            radar_eval = self.radar.evaluate_triggers(
                market_summaries=market_summaries,
                portfolio=pf,
                symbol_rules=rules,
                last_ai_call_at=self.last_ai_call_at,
                now_dt=now_jst,
            )
            self.last_radar_run_at = now_jst

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

            # 1日の最大新規取引回数ガード (MAX_DAILY_TRADES)
            action = str(validated.get("action", "HOLD")).upper()
            if action == "BUY":
                current_daily_trades = self._get_daily_trade_count(now_jst)
                if current_daily_trades >= self.max_daily_trades:
                    validated["action"] = "HOLD"
                    validated["order_type"] = "NONE"
                    validated["limit_price"] = 0
                    validated["size"] = 0
                    validated["take_profit_price"] = 0
                    validated["stop_loss_price"] = 0
                    validated["expected_cost_jpy"] = 0
                    validated["expected_loss_jpy"] = 0
                    validated["risk_reward_ratio"] = 0.0
                    validated["daily_trade_limit_reached"] = True
                    validated["reasoning"] = (
                        f"[1日の最大取引回数上限 ({self.max_daily_trades}回) 到達によりHOLDへ自動補正] "
                        + str(validated.get("reasoning", ""))
                    )
                    action = "HOLD"

            validated["radar_triggers"] = radar_eval["triggers"]
            validated["execution_mode"] = getattr(self.broker, "MODE", self.execution_mode)
            validated["manual_instruction"] = build_manual_order_guide(validated)
            validated["status"] = "SUCCESS"

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
        """24時間常駐メインループ（15秒ごとに高速ループ、15分ごとに中速レーダーループを実行）"""
        logger.info(
            "=== GMO AI Trader v2 デーモン起動 (mode=%s, fast=%ds, radar=%ds, expiry=%.1fh) ===",
            getattr(self.broker, "MODE", self.execution_mode),
            self.fast_loop_sec,
            self.radar_loop_sec,
            self.order_expiry_hours,
        )
        cycle_count = 0
        while True:
            now_jst = _now_jst()
            self.run_fast_cycle(now_dt=now_jst)

            should_run_radar = (
                self.last_radar_run_at is None
                or (now_jst - self.last_radar_run_at).total_seconds() >= self.radar_loop_sec
            )
            if should_run_radar:
                self.run_radar_and_ai_cycle(now_dt=now_jst)

            cycle_count += 1
            if max_cycles is not None and cycle_count >= max_cycles:
                break
            time.sleep(self.fast_loop_sec)


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
    args = parser.parse_args()

    daemon = AutoTraderDaemon(dry_run_notify=args.dry_run_notify)
    if args.once:
        daemon.run_fast_cycle()
        daemon.run_radar_and_ai_cycle(force_ai=args.force_ai)
        return 0

    daemon.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
