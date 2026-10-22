"""
GMOコイン 24時間全自動売買システム v2 — ローカルWebダッシュボード (web_dashboard.py)

機能概要:
- ローカルホスト (`http://127.0.0.1:8080`) 専用で動作する軽量・高速なWeb管理コンソール。
- 稼働中の `auto_trader.py` デーモンとファイルロックやSQLiteトランザクションで競合しないよう、
  状態読み取り (`portfolio.json`, `latest_decision.json`, `v2_state_wal.db`, `logs/`) と
  `RemoteControlManager` / `DatabaseWAL` (`control_commands` テーブル) 経由の安全なコマンド連携を行う。
- 外部パッケージの追加インストール不要（Python標準ライブラリ `http.server.ThreadingHTTPServer` ＋ 既存 `src/` モジュール）。

提供API:
- `GET  /`            : ダークモード・トレーディングダッシュボード UI (HTML5 Canvas チャート内蔵)
- `GET  /api/status`  : デーモン稼働状態・資産サマリー・パターンA複利枠・第1回/第2回期限カウントダウン・建玉・ログ一括取得
- `GET  /api/chart`   : 指定銘柄 (`symbol=BTC` 等)・時間足 (`interval=15min|1hour|4hour|1day`) のローソク足＋建玉水平ライン取得
- `POST /api/control` : `PAUSE` / `RESUME` / `CANCEL_ENTRIES` / `FLATTEN_ALL` / `FLATTEN_CONFIRM` の安全な発行・即時状態同期
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

sys.dont_write_bytecode = True

from dotenv import load_dotenv

from src.circuit_breaker import MultiTierCircuitBreaker, SingleInstanceLock
from src.database_wal import DatabaseWAL
from src.gmo_client import GMOPublicClient
from src.gmo_private_client import create_broker_client
from src.json_utils import load_json_file
from src.pseudo_oco_manager import PseudoOCOManager
from src.remote_control import (
    RemoteControlManager,
    ReplayOrExpiredNonceError,
    UnauthorizedCommandError,
)
from src.validator import DEFAULT_SYMBOL_RULES

logger = logging.getLogger("web_dashboard")

JST = timezone(timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_DRAWDOWN_KILL_SWITCH_JPY = 8500.0
DEFAULT_MAX_ALLOCATION_PCT = 0.45

SUPPORTED_CHART_SYMBOLS = ("BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC")
SUPPORTED_CHART_INTERVALS = ("15min", "1hour", "4hour", "1day")

DEFAULT_PHASE1_END_JST = "2027-07-09T00:00:00+09:00"
DEFAULT_PHASE2_START_JST = "2027-07-10T00:00:00+09:00"
DEFAULT_PHASE2_END_JST = "2027-12-10T00:00:00+09:00"
DEFAULT_PRE_DEADLINE_FREEZE_HOURS = 2.0


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def _parse_iso_jst(iso_str: str, default_iso: str) -> datetime:
    raw = (iso_str or default_iso).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=JST)
        return dt.astimezone(JST)
    except Exception:
        dt_def = datetime.fromisoformat(default_iso)
        return dt_def.astimezone(JST)


class WebDashboardService:
    """
    Webダッシュボードのバックエンドロジック層。
    HTTPハンドラおよび単体テストの双方から直接呼び出し可能。
    """

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        public_client: Optional[GMOPublicClient] = None,
        ticker_cache_ttl_sec: float = 5.0,
        kline_cache_ttl_sec: float = 10.0,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else BASE_DIR
        load_dotenv(self.base_dir / ".env")

        self.portfolio_path = self.base_dir / "portfolio.json"
        self.latest_decision_path = self.base_dir / "latest_decision.json"
        self.capabilities_path = self.base_dir / "capabilities.json"
        self.wal_db_path = self.base_dir / "v2_state_wal.db"
        self.daemon_lock_path = self.base_dir / "v2_daemon.lock"
        self.launcher_lock_path = self.base_dir / "v2_launcher.lock"
        self.log_dir = self.base_dir / "logs"

        self.public_client = public_client or GMOPublicClient(timeout=6, max_retries=2, retry_delay=0.5)
        self.ticker_cache_ttl_sec = float(ticker_cache_ttl_sec)
        self.kline_cache_ttl_sec = float(kline_cache_ttl_sec)

        self._cache_lock = threading.Lock()
        self._cached_tickers: Dict[str, Dict[str, Any]] = {}
        self._cached_tickers_at: float = 0.0
        self._cached_klines: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]]]] = {}

        # RemoteControl 用の軽量インスタンス（PAUSE/RESUME/FLATTEN_ALL キュー操作用）
        self.db_wal = DatabaseWAL(
            self.wal_db_path,
            portfolio_json_path=self.portfolio_path,
            auto_sync_files=True,
        )
        # Web側からは直接取引所へ新規発注しないため DRY_RUN ブローカーまたは設定ブローカーを保持し、
        # デーモン稼働中は SQLite control_commands キュー経由で本体デーモンに実行させる
        self._dummy_broker = create_broker_client(
            execution_mode="DRY_RUN",
            base_dir=self.base_dir,
            public_client=self.public_client,
            wal_db=self.db_wal,
        )
        self.oco_manager = PseudoOCOManager(
            broker=self._dummy_broker,
            portfolio_path=self.portfolio_path,
            symbol_rules=DEFAULT_SYMBOL_RULES,
            notifier=None,
            dry_run_notify=True,
            wal_db=self.db_wal,
        )
        self.remote_control = RemoteControlManager(
            oco_manager=self.oco_manager,
            db_wal=self.db_wal,
            allowed_owner_discord_id="",  # ローカルホスト (127.0.0.1) コンソール操作のためローカルオーナー許可
            nonce_ttl_seconds=60,
        )
        self.circuit_breaker = MultiTierCircuitBreaker(
            symbol_rules=DEFAULT_SYMBOL_RULES,
            l5_kill_switch_equity_jpy=float(os.getenv("DRAWDOWN_KILL_SWITCH_JPY", str(DEFAULT_DRAWDOWN_KILL_SWITCH_JPY))),
        )

    def close(self) -> None:
        try:
            if self.db_wal is not None:
                self.db_wal.close()
        except Exception:
            pass

    def _is_daemon_running(self) -> Tuple[bool, Optional[int]]:
        """v2_daemon.lock の OS ファイルロック状態および v2_launcher.lock の PID を確認する"""
        probe_lock = SingleInstanceLock(self.daemon_lock_path)
        locked_by_daemon = probe_lock.is_locked_by_another_process()
        launcher_pid: Optional[int] = None
        if self.launcher_lock_path.exists():
            try:
                raw_pid = self.launcher_lock_path.read_text(encoding="ascii", errors="ignore").strip()
                if raw_pid.isdigit():
                    launcher_pid = int(raw_pid)
            except Exception:
                launcher_pid = None
        return locked_by_daemon, launcher_pid

    def _get_cached_tickers(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        now_mono = time.monotonic()
        with self._cache_lock:
            if (
                not force
                and self._cached_tickers
                and (now_mono - self._cached_tickers_at) < self.ticker_cache_ttl_sec
            ):
                return dict(self._cached_tickers)

        try:
            tickers = self.public_client.get_tickers(list(SUPPORTED_CHART_SYMBOLS))
            if tickers:
                with self._cache_lock:
                    self._cached_tickers = dict(tickers)
                    self._cached_tickers_at = now_mono
                return dict(tickers)
        except Exception as exc:
            logger.debug("Public ticker 取得失敗（キャッシュフォールバック使用）: %s", exc)

        with self._cache_lock:
            return dict(self._cached_tickers)

    def _evaluate_campaign_info(self, pf: Dict[str, Any], now_jst: datetime) -> Dict[str, Any]:
        camp_cfg = pf.get("campaign_schedule") if isinstance(pf.get("campaign_schedule"), dict) else {}
        p1_end = _parse_iso_jst(
            str(camp_cfg.get("phase1_end_jst") or os.getenv("PHASE1_END_JST", DEFAULT_PHASE1_END_JST)),
            DEFAULT_PHASE1_END_JST,
        )
        p2_start = _parse_iso_jst(
            str(camp_cfg.get("phase2_start_jst") or os.getenv("PHASE2_START_JST", DEFAULT_PHASE2_START_JST)),
            DEFAULT_PHASE2_START_JST,
        )
        p2_end = _parse_iso_jst(
            str(camp_cfg.get("phase2_end_jst") or os.getenv("PHASE2_END_JST", DEFAULT_PHASE2_END_JST)),
            DEFAULT_PHASE2_END_JST,
        )
        freeze_hours = float(
            camp_cfg.get("pre_deadline_freeze_hours")
            or os.getenv("PRE_DEADLINE_FREEZE_HOURS", str(DEFAULT_PRE_DEADLINE_FREEZE_HOURS))
        )
        p1_freeze = p1_end - timedelta(hours=freeze_hours)
        p2_freeze = p2_end - timedelta(hours=freeze_hours)

        if now_jst < p1_freeze:
            phase = "PHASE1_ACTIVE"
            phase_label_ja = "第1回 運用期間（通常稼働中）"
            target_deadline = p1_end
            target_label = "第1回締め（2027/07/09 00:00）まで"
        elif now_jst < p1_end:
            phase = "PHASE1_PRE_DEADLINE_FREEZE"
            phase_label_ja = "第1回 終了2時間前（新規BUY凍結・SL/TP監視中）"
            target_deadline = p1_end
            target_label = "第1回 全決済（2027/07/09 00:00）まで"
        elif now_jst < p2_start:
            phase = "PHASE1_COMPLETED_INTERMISSION"
            phase_label_ja = "第1回満了・検証休止期間（24時間インターミッション）"
            target_deadline = p2_start
            target_label = "第2回自動再開（2027/07/10 00:00）まで"
        elif now_jst < p2_freeze:
            phase = "PHASE2_ACTIVE"
            phase_label_ja = "第2回 運用期間（通常稼働中）"
            target_deadline = p2_end
            target_label = "第2回締め（2027/12/10 00:00）まで"
        elif now_jst < p2_end:
            phase = "PHASE2_PRE_DEADLINE_FREEZE"
            phase_label_ja = "第2回 終了2時間前（新規BUY凍結・SL/TP監視中）"
            target_deadline = p2_end
            target_label = "第2回 全決済（2027/12/10 00:00）まで"
        else:
            phase = "PHASE2_COMPLETED_HALT"
            phase_label_ja = "第2回 運用期間満了（全日程完了・待機中）"
            target_deadline = p2_end
            target_label = "全日程完了"

        rem_sec = max(0, int((target_deadline - now_jst).total_seconds()))
        rem_days = rem_sec // 86400
        rem_hours = (rem_sec % 86400) // 3600
        rem_minutes = (rem_sec % 3600) // 60

        if rem_sec > 0:
            countdown_text = f"{target_label}: 残り {rem_days}日 {rem_hours:02d}時間 {rem_minutes:02d}分"
        else:
            countdown_text = "第1回・第2回 全運用期間完了"

        return {
            "current_phase": phase,
            "phase_label_ja": phase_label_ja,
            "phase1_end_jst": p1_end.isoformat(timespec="seconds"),
            "phase1_freeze_start_jst": p1_freeze.isoformat(timespec="seconds"),
            "phase2_start_jst": p2_start.isoformat(timespec="seconds"),
            "phase2_end_jst": p2_end.isoformat(timespec="seconds"),
            "phase2_freeze_start_jst": p2_freeze.isoformat(timespec="seconds"),
            "pre_deadline_freeze_hours": freeze_hours,
            "remaining_days": rem_days,
            "remaining_hours": rem_hours,
            "remaining_minutes": rem_minutes,
            "remaining_total_seconds": rem_sec,
            "countdown_text": countdown_text,
            "phase1_settled": bool(pf.get("phase1_settled", False)),
            "phase2_settled": bool(pf.get("phase2_settled", False)),
        }

    def _read_recent_logs(self, max_lines: int = 30) -> List[str]:
        if not self.log_dir.exists():
            return []
        try:
            log_files = sorted(
                self.log_dir.glob("v2_auto_*.log"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not log_files:
                return []
            content = log_files[0].read_text(encoding="utf-8", errors="replace")
            lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
            return lines[-max_lines:]
        except Exception:
            return []

    def get_status_payload(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """`GET /api/status` 用の全ステータス辞書を構築する"""
        now_jst = _now_jst(now_dt)
        pf = self.oco_manager.load_portfolio()
        latest_dec: Dict[str, Any] = {}
        if self.latest_decision_path.exists():
            try:
                loaded_dec = load_json_file(self.latest_decision_path)
                if isinstance(loaded_dec, dict):
                    latest_dec = loaded_dec
            except Exception:
                latest_dec = {}

        tickers = self._get_cached_tickers()
        daemon_running, launcher_pid = self._is_daemon_running()
        is_paused = bool(pf.get("is_paused", False))
        exec_mode = str(pf.get("execution_mode") or os.getenv("EXECUTION_MODE", "LIVE")).strip().upper()

        # ポジション評価（有効ポジションと DUST_QUARANTINED を明確に分離）
        active_positions: List[Dict[str, Any]] = []
        quarantined_dust: List[Dict[str, Any]] = []
        positions_eval_jpy = 0.0
        unrealized_pnl_total_jpy = 0.0

        for pos in pf.get("positions", []) or []:
            if not isinstance(pos, dict):
                continue
            sym = str(pos.get("symbol", "")).strip().upper()
            state = str(pos.get("state", "PROTECTED")).strip().upper()
            size = float(pos.get("size", 0) or 0)
            entry_price = float(pos.get("entry_price", 0) or 0)
            t_info = tickers.get(sym, {})
            cur_bid = float(t_info.get("bid") or t_info.get("last") or pos.get("current_price") or entry_price or 0.0)
            cur_last = float(t_info.get("last") or cur_bid or 0.0)

            if state == "DUST_QUARANTINED":
                quarantined_dust.append(
                    {
                        "symbol": sym,
                        "size": size,
                        "state": state,
                        "current_bid": cur_bid,
                        "estimated_value_jpy": round(size * cur_bid, 1),
                        "opened_at": str(pos.get("opened_at", "")),
                    }
                )
                continue

            eval_jpy = size * cur_bid
            cost_jpy = size * entry_price
            unr_pnl_jpy = eval_jpy - cost_jpy
            unr_pnl_pct = ((cur_bid - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
            positions_eval_jpy += eval_jpy
            unrealized_pnl_total_jpy += unr_pnl_jpy

            if pos.get("trailing_activated"):
                trail_stage = "STAGE_2_TRAILING (+2.2% ATR追従)"
            elif pos.get("breakeven_activated"):
                trail_stage = "STAGE_1_BREAKEVEN (+1.2% 建値ロック)"
            else:
                trail_stage = "INITIAL_SL (初期逆指値保護)"

            active_positions.append(
                {
                    "symbol": sym,
                    "side": str(pos.get("side", "BUY")).upper(),
                    "size": size,
                    "entry_price": entry_price,
                    "current_bid": cur_bid,
                    "current_last": cur_last,
                    "eval_jpy": round(eval_jpy, 0),
                    "unrealized_pnl_jpy": round(unr_pnl_jpy, 0),
                    "unrealized_pnl_pct": round(unr_pnl_pct, 2),
                    "stop_loss_price": float(pos.get("stop_loss_price", 0) or 0),
                    "take_profit_price": float(pos.get("take_profit_price", 0) or 0),
                    "sl_order_id": str(pos.get("sl_order_id", "")),
                    "state": state,
                    "trailing_stage": trail_stage,
                    "opened_at": str(pos.get("opened_at", "")),
                }
            )

        # quarantine リストに別記されているものもマージ（重複除外）
        seen_dust_syms = {d["symbol"] for d in quarantined_dust}
        for qd in pf.get("quarantined_dust", []) or []:
            if isinstance(qd, dict):
                q_sym = str(qd.get("symbol", "")).strip().upper()
                if q_sym and q_sym not in seen_dust_syms:
                    q_sz = float(qd.get("size", 0) or 0)
                    t_info = tickers.get(q_sym, {})
                    cur_bid = float(t_info.get("bid") or t_info.get("last") or 0.0)
                    quarantined_dust.append(
                        {
                            "symbol": q_sym,
                            "size": q_sz,
                            "state": "DUST_QUARANTINED",
                            "current_bid": cur_bid,
                            "estimated_value_jpy": round(q_sz * cur_bid, 1),
                            "opened_at": str(qd.get("quarantined_at", "")),
                        }
                    )

        # 待機中注文と拘束資金の集計
        pending_orders: List[Dict[str, Any]] = []
        locked_pending_jpy = 0.0
        for p_ord in pf.get("pending_orders", []) or []:
            if not isinstance(p_ord, dict):
                continue
            p_sym = str(p_ord.get("symbol", "")).strip().upper()
            p_limit = float(p_ord.get("limit_price", 0) or 0)
            p_size = float(p_ord.get("size", 0) or 0)
            p_exec = float(p_ord.get("executed_size", 0) or 0)
            rem_size = max(0.0, p_size - p_exec)
            locked_jpy = float(p_ord.get("reserved_jpy", 0) or (rem_size * p_limit))
            locked_pending_jpy += locked_jpy
            pending_orders.append(
                {
                    "order_id": str(p_ord.get("order_id", "")),
                    "symbol": p_sym,
                    "side": str(p_ord.get("side", "BUY")).upper(),
                    "limit_price": p_limit,
                    "size": p_size,
                    "executed_size": p_exec,
                    "remaining_size": rem_size,
                    "reserved_jpy": round(locked_jpy, 0),
                    "stop_loss_price": float(p_ord.get("stop_loss_price", 0) or 0),
                    "take_profit_price": float(p_ord.get("take_profit_price", 0) or 0),
                    "state": str(p_ord.get("state", "PENDING_BUY")),
                    "created_at": str(p_ord.get("created_at", "")),
                }
            )

        init_cap_jpy = float(pf.get("initial_capital_jpy", 10000) or 10000.0)
        jpy_balance = float(pf.get("jpy_balance", init_cap_jpy) or 0.0)
        total_equity_jpy = jpy_balance + locked_pending_jpy + positions_eval_jpy

        # サーキットブレーカー状態 (L3 / L4 / L5)
        l3_status = self.circuit_breaker.check_l3_daily_loss(pf, tickers, now_dt=now_jst)
        l4_status = self.circuit_breaker.check_l4_weekly_and_streak(pf, now_dt=now_jst)
        cons_eq_dec, _ = self.circuit_breaker.calculate_conservative_equity_jpy(pf, tickers)
        cons_eq = float(cons_eq_dec)
        peak_eq = max(
            float(pf.get("peak_equity_jpy", 0.0) or 0.0),
            init_cap_jpy,
            total_equity_jpy,
        )
        l5_floor_jpy = max(
            float(DEFAULT_DRAWDOWN_KILL_SWITCH_JPY),
            round(peak_eq * 0.85, 2),
        )
        alloc_pct = float(pf.get("max_allocation_per_trade_pct", DEFAULT_MAX_ALLOCATION_PCT) or DEFAULT_MAX_ALLOCATION_PCT)
        max_pos_jpy = round(total_equity_jpy * alloc_pct, 0)
        risk_1r_normal_jpy = round(max(150.0, total_equity_jpy * 0.015), 0)
        risk_1r_stressed_jpy = round(max(200.0, total_equity_jpy * 0.020), 0)

        # 確定損益推移（エクイティカーブ）の構築
        closed_raw = [t for t in (pf.get("closed_trades", []) or []) if isinstance(t, dict)]
        equity_curve: List[Dict[str, Any]] = [
            {
                "index": 0,
                "label": "開始",
                "equity_jpy": round(init_cap_jpy, 0),
                "pnl_jpy": 0,
                "symbol": "INIT",
            }
        ]
        running_eq = init_cap_jpy
        realized_sum = 0.0
        for idx, tr in enumerate(closed_raw, start=1):
            pnl = float(tr.get("pnl_jpy", 0) or 0)
            running_eq += pnl
            realized_sum += pnl
            c_at = str(tr.get("closed_at", ""))
            lbl = c_at[5:16].replace("T", " ") if len(c_at) >= 16 else f"#{idx}"
            equity_curve.append(
                {
                    "index": idx,
                    "label": lbl,
                    "equity_jpy": round(running_eq, 0),
                    "pnl_jpy": round(pnl, 0),
                    "symbol": str(tr.get("symbol", "")),
                    "reason": str(tr.get("reason", "")),
                }
            )
        # 現在時点の総資産も末尾に追加
        if len(equity_curve) == 1 or abs(total_equity_jpy - running_eq) >= 1.0:
            equity_curve.append(
                {
                    "index": len(equity_curve),
                    "label": "現在",
                    "equity_jpy": round(total_equity_jpy, 0),
                    "pnl_jpy": round(total_equity_jpy - init_cap_jpy, 0),
                    "symbol": "NOW",
                    "reason": "CURRENT",
                }
            )

        regime_info = latest_dec.get("regime_info") if isinstance(latest_dec.get("regime_info"), dict) else {}
        market_regime = str(regime_info.get("regime", "NEUTRAL") or "NEUTRAL").upper()

        campaign_info = self._evaluate_campaign_info(pf, now_jst)

        return {
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "daemon": {
                "is_running": daemon_running,
                "launcher_pid": launcher_pid,
                "execution_mode": exec_mode,
                "is_paused": is_paused,
                "market_regime": market_regime,
                "regime_detail": regime_info,
                "circuit_breakers": {
                    "l1_ws_sequence": "OK",
                    "l2_api_health": "OK",
                    "l3_daily_loss": l3_status,
                    "l4_weekly_streak": l4_status,
                    "l5_kill_switch": {
                        "latched": bool(self.circuit_breaker.l5_kill_switch_latched),
                        "conservative_equity_jpy": round(cons_eq, 0),
                        "peak_equity_jpy": round(peak_eq, 0),
                        "threshold_jpy": round(l5_floor_jpy, 0),
                        "headroom_jpy": round(cons_eq - l5_floor_jpy, 0),
                    },
                },
            },
            "equity_summary": {
                "initial_capital_jpy": round(init_cap_jpy, 0),
                "jpy_balance": round(jpy_balance, 0),
                "locked_pending_jpy": round(locked_pending_jpy, 0),
                "positions_eval_jpy": round(positions_eval_jpy, 0),
                "total_equity_jpy": round(total_equity_jpy, 0),
                "conservative_liquidation_jpy": round(cons_eq, 0),
                "total_pnl_jpy": round(total_equity_jpy - init_cap_jpy, 0),
                "total_pnl_pct": round(((total_equity_jpy - init_cap_jpy) / init_cap_jpy * 100.0) if init_cap_jpy > 0 else 0.0, 2),
                "realized_pnl_total_jpy": round(float(pf.get("realized_pnl_total_jpy", realized_sum) or 0.0), 0),
                "unrealized_pnl_total_jpy": round(unrealized_pnl_total_jpy, 0),
                "peak_equity_jpy": round(peak_eq, 0),
                "l5_kill_switch_jpy": round(l5_floor_jpy, 0),
                "pattern_a": {
                    "max_allocation_pct": alloc_pct,
                    "max_position_jpy": max_pos_jpy,
                    "max_concurrent_positions": int(pf.get("max_concurrent_positions", 2) or 2),
                    "risk_1r_normal_jpy": risk_1r_normal_jpy,
                    "risk_1r_stressed_jpy": risk_1r_stressed_jpy,
                },
                "equity_curve": equity_curve,
            },
            "campaign": campaign_info,
            "positions": {
                "active": active_positions,
                "quarantined_dust": quarantined_dust,
            },
            "pending_orders": pending_orders,
            "closed_trades": list(reversed(closed_raw[-30:])),
            "latest_decision": latest_dec,
            "logs": self._read_recent_logs(max_lines=30),
            "tickers": tickers,
        }

    def get_chart_payload(self, symbol: str = "BTC", interval: str = "1hour", limit: int = 72) -> Dict[str, Any]:
        """`GET /api/chart?symbol=BTC&interval=1hour` 用のローソク足＋水平オーバーレイ辞書を返す"""
        sym = str(symbol or "BTC").strip().upper()
        if sym not in SUPPORTED_CHART_SYMBOLS and sym not in DEFAULT_SYMBOL_RULES:
            sym = "BTC"
        iv = str(interval or "1hour").strip().lower()
        if iv not in SUPPORTED_CHART_INTERVALS:
            iv = "1hour"
        lim = max(20, min(150, int(limit or 72)))

        now_mono = time.monotonic()
        cache_key = (sym, iv)
        candles: List[Dict[str, Any]] = []

        with self._cache_lock:
            cached = self._cached_klines.get(cache_key)
            if cached and (now_mono - cached[0]) < self.kline_cache_ttl_sec:
                candles = list(cached[1])

        if not candles:
            try:
                df = self.public_client.get_klines(symbol=sym, interval=iv, limit=lim)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        open_ms = int(float(row.get("open_time", 0) or 0))
                        dt_jst = datetime.fromtimestamp(open_ms / 1000.0, tz=JST) if open_ms > 0 else _now_jst()
                        lbl = dt_jst.strftime("%m/%d %H:%M") if iv != "1day" else dt_jst.strftime("%Y-%m-%d")
                        candles.append(
                            {
                                "time": int(open_ms // 1000),
                                "time_label": lbl,
                                "open": float(row.get("open", 0) or 0),
                                "high": float(row.get("high", 0) or 0),
                                "low": float(row.get("low", 0) or 0),
                                "close": float(row.get("close", 0) or 0),
                                "volume": float(row.get("volume", 0) or 0),
                            }
                        )
                    with self._cache_lock:
                        self._cached_klines[cache_key] = (now_mono, list(candles))
            except Exception as exc:
                logger.warning("Chart klines 取得失敗 (%s, %s): %s", sym, iv, exc)

        tickers = self._get_cached_tickers()
        ticker_info = tickers.get(sym, {})

        # 万一 Public Klines が空の場合でも現在 Ticker からフォールバック点を生成
        if not candles and ticker_info:
            last_p = float(ticker_info.get("last", 0) or 0)
            if last_p > 0:
                now_jst = _now_jst()
                candles.append(
                    {
                        "time": int(now_jst.timestamp()),
                        "time_label": now_jst.strftime("%m/%d %H:%M"),
                        "open": last_p,
                        "high": float(ticker_info.get("high", last_p) or last_p),
                        "low": float(ticker_info.get("low", last_p) or last_p),
                        "close": last_p,
                        "volume": float(ticker_info.get("volume", 0) or 0),
                    }
                )

        # 該当銘柄の保有ポジションおよび待機指値オーバーレイ情報
        pf = self.oco_manager.load_portfolio()
        overlays: Dict[str, Any] = {
            "has_position": False,
            "has_pending_order": False,
            "entry_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
            "limit_price": None,
            "position_size": 0.0,
            "trailing_stage": "",
        }
        for pos in pf.get("positions", []) or []:
            if not isinstance(pos, dict):
                continue
            if str(pos.get("symbol", "")).strip().upper() == sym and str(pos.get("state", "")).upper() != "DUST_QUARANTINED":
                overlays["has_position"] = True
                overlays["entry_price"] = float(pos.get("entry_price", 0) or 0) or None
                overlays["stop_loss_price"] = float(pos.get("stop_loss_price", 0) or 0) or None
                overlays["take_profit_price"] = float(pos.get("take_profit_price", 0) or 0) or None
                overlays["position_size"] = float(pos.get("size", 0) or 0)
                overlays["trailing_stage"] = (
                    "STAGE_2_TRAILING"
                    if pos.get("trailing_activated")
                    else ("STAGE_1_BREAKEVEN" if pos.get("breakeven_activated") else "INITIAL_SL")
                )
                break

        for p_ord in pf.get("pending_orders", []) or []:
            if not isinstance(p_ord, dict):
                continue
            if str(p_ord.get("symbol", "")).strip().upper() == sym:
                overlays["has_pending_order"] = True
                overlays["limit_price"] = float(p_ord.get("limit_price", 0) or 0) or None
                if not overlays["stop_loss_price"]:
                    overlays["stop_loss_price"] = float(p_ord.get("stop_loss_price", 0) or 0) or None
                if not overlays["take_profit_price"]:
                    overlays["take_profit_price"] = float(p_ord.get("take_profit_price", 0) or 0) or None
                break

        return {
            "symbol": sym,
            "interval": iv,
            "ticker": ticker_info,
            "candles": candles,
            "overlays": overlays,
            "timestamp": _now_jst().isoformat(timespec="seconds"),
        }

    def handle_control_command(
        self,
        command: str,
        nonce: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        `POST /api/control` のコマンド処理。
        - `PAUSE` / `RESUME` / `CANCEL_ENTRIES` / `FLATTEN_ALL` / `FLATTEN_CONFIRM` を受け付ける。
        - デーモン稼働中は `enqueue_or_execute_command(..., is_daemon_running=True)` により
          SQLite `control_commands` テーブルへエンキューしつつ、`PAUSE` / `RESUME` は即座に `portfolio.json` と
          SQLite `account_meta` の `is_paused` フラグも同期して UI に 0秒で反映させる。
        """
        now_jst = _now_jst(now_dt)
        cmd_clean = str(command or "").strip().upper()
        nonce_clean = str(nonce or "").strip()

        if cmd_clean not in ("PAUSE", "RESUME", "CANCEL_ENTRIES", "FLATTEN_ALL", "FLATTEN_REQUEST", "FLATTEN_CONFIRM"):
            raise ValueError(f"未対応の制御コマンドです: {cmd_clean}")

        full_cmd = f"{cmd_clean} {nonce_clean}".strip() if (cmd_clean == "FLATTEN_CONFIRM" and nonce_clean) else cmd_clean
        daemon_running, _ = self._is_daemon_running()

        res = self.remote_control.enqueue_or_execute_command(
            command_text=full_cmd,
            is_daemon_running=daemon_running,
            actor_id=self.remote_control.allowed_owner_discord_id or "LOCAL_WEB_OWNER",
            now_dt=now_jst,
        )
        pf_after = self.oco_manager.load_portfolio()
        is_paused_after = bool(pf_after.get("is_paused", False))

        # わかりやすい日本語メッセージを生成
        if cmd_clean == "PAUSE":
            msg_ja = "⏸️ 新規買いエントリーを一時停止（PAUSE）しました。保有中ポジションと取引所の逆指値（STOP）は安全に維持されます。"
        elif cmd_clean == "RESUME":
            msg_ja = "▶️ 自動売買（新規買いエントリー監視）を再開（RESUME）しました。"
        elif cmd_clean == "CANCEL_ENTRIES":
            msg_ja = (
                "🧹 未約定の待機買い指値クリア（CANCEL_ENTRIES）を要求しました。"
                if daemon_running
                else f"🧹 未約定の待機買い指値を {len(res.get('canceled_buy_order_ids', []))} 件キャンセルしました。"
            )
        elif cmd_clean in ("FLATTEN_ALL", "FLATTEN_REQUEST"):
            msg_ja = f"⚠️ 緊急全決済のワンタイム確認コード（{res.get('nonce')}）を発行しました。60秒以内に最終承認ボタンを押すと執行されます。"
        else:
            msg_ja = "🚨 緊急全決済（FLATTEN_CONFIRM）を承認・要求しました。全待機注文の取消と保有建玉の成行決済を実行します。"

        return {
            "ok": True,
            "command": cmd_clean,
            "message": msg_ja,
            "is_paused": is_paused_after,
            "daemon_running": daemon_running,
            "result": res,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GMO AI Trader v2 — 24/7 自動売買ローカルコンソール</title>
<style>
  :root {
    --bg-main: #0b0f19;
    --bg-card: #111827;
    --bg-card-alt: #1e293b;
    --border-color: #26334d;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --accent-blue: #38bdf8;
    --accent-green: #10b981;
    --accent-red: #ef4444;
    --accent-amber: #f59e0b;
    --accent-purple: #a855f7;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg-main);
    color: var(--text-primary);
    font-family: "Segoe UI", "Hiragino Sans", "Yu Gothic UI", "Meiryo", sans-serif;
    line-height: 1.5;
    padding-bottom: 40px;
  }
  .topbar {
    background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%);
    border-bottom: 1px solid var(--border-color);
    padding: 14px 24px;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .brand h1 {
    font-size: 1.2rem;
    font-weight: 700;
    letter-spacing: 0.02em;
  }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 11px;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 700;
    border: 1px solid transparent;
  }
  .badge-live { background: rgba(239,68,68,0.18); color: #fca5a5; border-color: rgba(239,68,68,0.45); }
  .badge-dry { background: rgba(56,189,248,0.18); color: #7dd3fc; border-color: rgba(56,189,248,0.45); }
  .badge-active { background: rgba(16,185,129,0.18); color: #6ee7b7; border-color: rgba(16,185,129,0.45); }
  .badge-paused { background: rgba(245,158,11,0.22); color: #fcd34d; border-color: rgba(245,158,11,0.55); }
  .badge-stopped { background: rgba(148,163,184,0.18); color: #cbd5e1; border-color: rgba(148,163,184,0.4); }
  .badge-regime { background: rgba(168,85,247,0.18); color: #d8b4fe; border-color: rgba(168,85,247,0.45); }
  .badge-campaign { background: rgba(56,189,248,0.14); color: #bae6fd; border-color: rgba(56,189,248,0.35); }

  .container {
    max-width: 1480px;
    margin: 0 auto;
    padding: 18px 20px;
    display: flex;
    flex-direction: column;
    gap: 18px;
  }

  /* Control Bar */
  .control-bar {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 14px 18px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.25);
  }
  .control-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
  }
  .btn {
    cursor: pointer;
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 9px 16px;
    font-size: 0.88rem;
    font-weight: 700;
    color: var(--text-primary);
    background: var(--bg-card-alt);
    transition: all 0.15s ease;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  .btn:hover:not(:disabled) { transform: translateY(-1px); filter: brightness(1.12); }
  .btn:disabled { opacity: 0.42; cursor: not-allowed; }
  .btn-pause { background: #b45309; border-color: #f59e0b; color: #fff; }
  .btn-resume { background: #047857; border-color: #10b981; color: #fff; }
  .btn-cancel { background: #1e3a8a; border-color: #3b82f6; color: #dbeafe; }
  .btn-flatten { background: #7f1d1d; border-color: #ef4444; color: #fee2e2; }
  .btn-refresh { background: #1e293b; border-color: #475569; color: #e2e8f0; }

  .toast {
    padding: 8px 14px;
    border-radius: 8px;
    font-size: 0.85rem;
    font-weight: 600;
    background: rgba(56,189,248,0.14);
    border: 1px solid rgba(56,189,248,0.4);
    color: #e0f2fe;
    display: none;
  }

  /* Summary KPI Grid */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(225px, 1fr));
    gap: 14px;
  }
  .kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 15px 18px;
  }
  .kpi-label {
    font-size: 0.78rem;
    color: var(--text-secondary);
    margin-bottom: 4px;
    display: flex;
    justify-content: space-between;
  }
  .kpi-value {
    font-size: 1.45rem;
    font-weight: 800;
    letter-spacing: 0.01em;
  }
  .kpi-sub {
    font-size: 0.76rem;
    color: var(--text-secondary);
    margin-top: 4px;
  }
  .pos-val { color: var(--accent-green); }
  .neg-val { color: var(--accent-red); }

  /* Main 2-Column Layout */
  .main-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 18px;
  }
  @media (max-width: 1100px) {
    .main-grid { grid-template-columns: 1fr; }
  }
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 16px 18px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .card-header {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    padding-bottom: 10px;
  }
  .card-title {
    font-size: 0.96rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  /* Chart Controls */
  .chart-toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .chip {
    cursor: pointer;
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 0.78rem;
    font-weight: 700;
    background: var(--bg-card-alt);
    color: var(--text-secondary);
    border: 1px solid var(--border-color);
  }
  .chip.active {
    background: rgba(56,189,248,0.2);
    color: #38bdf8;
    border-color: #38bdf8;
  }
  .chart-canvas-wrap {
    position: relative;
    width: 100%;
    height: 360px;
    background: #090d16;
    border: 1px solid #1e293b;
    border-radius: 8px;
    overflow: hidden;
  }
  .eq-canvas-wrap {
    position: relative;
    width: 100%;
    height: 170px;
    background: #090d16;
    border: 1px solid #1e293b;
    border-radius: 8px;
    overflow: hidden;
  }
  canvas { width: 100%; height: 100%; display: block; }

  /* Tables */
  .table-wrap { overflow-x: auto; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
  }
  th, td {
    padding: 8px 10px;
    text-align: left;
    border-bottom: 1px solid rgba(255,255,255,0.06);
  }
  th { color: var(--text-secondary); font-weight: 600; background: rgba(255,255,255,0.02); }

  /* Log Viewer */
  .log-box {
    background: #070a12;
    border: 1px solid #1e293b;
    border-radius: 8px;
    padding: 10px 12px;
    font-family: "Consolas", "Monaco", monospace;
    font-size: 0.76rem;
    color: #cbd5e1;
    max-height: 230px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-all;
  }

  /* Modal for 2-step FLATTEN_ALL */
  .modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.75);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 200;
    padding: 20px;
  }
  .modal {
    background: var(--bg-card);
    border: 2px solid var(--accent-red);
    border-radius: 12px;
    max-width: 520px;
    width: 100%;
    padding: 22px;
    display: flex;
    flex-direction: column;
    gap: 14px;
    box-shadow: 0 20px 50px rgba(0,0,0,0.6);
  }
  .modal h3 { color: #fca5a5; font-size: 1.1rem; }
  .modal p { font-size: 0.86rem; color: var(--text-secondary); }
  .nonce-box {
    background: #090d16;
    border: 1px dashed #ef4444;
    padding: 10px;
    border-radius: 8px;
    font-family: monospace;
    font-size: 0.9rem;
    color: #fde047;
    text-align: center;
  }
</style>
</head>
<body>

<header class="topbar">
  <div class="brand">
    <h1>🤖 GMO AI Trader v2 — 24/7 全自動売買コンソール</h1>
    <span id="badgeMode" class="badge badge-live">LIVE</span>
    <span id="badgeDaemon" class="badge badge-active">🟢 デーモン稼働中</span>
    <span id="badgeRegime" class="badge badge-regime">REGIME: NEUTRAL</span>
  </div>
  <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
    <span id="badgeCampaign" class="badge badge-campaign">🏁 第1回締めまで計算中...</span>
    <span id="clockJst" style="font-size:0.82rem; color:var(--text-secondary); font-family:monospace;">--:--:-- JST</span>
  </div>
</header>

<main class="container">

  <!-- 操作コントロールバー -->
  <section class="control-bar">
    <div>
      <div style="font-size:0.9rem; font-weight:700;">⚡ リアルタイム運用制御（デーモン無停止連携）</div>
      <div id="controlStatusDesc" style="font-size:0.78rem; color:var(--text-secondary);">
        一時停止（PAUSE）中も保有建玉の逆指値（STOP）と利確（TP）監視は継続されます。
      </div>
    </div>
    <div class="control-buttons">
      <button id="btnPause" class="btn btn-pause" onclick="sendControl('PAUSE')">⏸️ 一時停止 (PAUSE)</button>
      <button id="btnResume" class="btn btn-resume" onclick="sendControl('RESUME')">▶️ 売買再開 (RESUME)</button>
      <button id="btnCancelEntries" class="btn btn-cancel" onclick="confirmCancelEntries()">🧹 待機買い指値クリア</button>
      <button id="btnFlattenAll" class="btn btn-flatten" onclick="openFlattenModal()">🚨 緊急全決済 (2段階承認)</button>
      <button class="btn btn-refresh" onclick="refreshAll(true)">🔄 今すぐ更新</button>
    </div>
    <div id="toastMsg" class="toast"></div>
  </section>

  <!-- 資産・パターンA複利・防衛ライン KPI -->
  <section class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label"><span>💰 現在の総資産 (Total Equity)</span><span id="kpiInitCap">初期 10,000円</span></div>
      <div id="kpiTotalEquity" class="kpi-value">10,025 円</div>
      <div id="kpiCashDetail" class="kpi-sub">利用可能現金: 10,025 円 / 拘束中: 0 円</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>📊 通算損益 (確定 / 含み)</span><span id="kpiTotalPnlPct">+0.25%</span></div>
      <div id="kpiTotalPnl" class="kpi-value pos-val">+25 円</div>
      <div id="kpiRealizedDetail" class="kpi-sub">累計確定: +0 円 / 保有含み: +0 円</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>📈 パターンA 自動複利枠 (45%)</span><span>最大2銘柄</span></div>
      <div id="kpiMaxAlloc" class="kpi-value" style="color:var(--accent-blue);">4,511 円 / 枠</div>
      <div id="kpiRisk1R" class="kpi-sub">1R許容損失(1.5%): 150 円 (最大2.0%: 200 円)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>🛡️ High-Water Mark & L5防衛線</span><span id="kpiL5Status">正常</span></div>
      <div id="kpiL5Floor" class="kpi-value" style="color:var(--accent-amber);">8,521 円</div>
      <div id="kpiPeakDetail" class="kpi-sub">過去最高資産(HWM): 10,025 円 (余裕 +1,504 円)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>🚦 L3 日次 / L4 週次ブレーカー</span><span id="kpiCbBadge">稼働許可</span></div>
      <div id="kpiDailyPnl" class="kpi-value" style="font-size:1.18rem;">日次: +0 円 / 上限 -301 円</div>
      <div id="kpiWeeklyPnl" class="kpi-sub">週次: +0 円 (上限 -602 円) / 連敗: 0 回</div>
    </div>
  </section>

  <!-- メイン2カラム: 左=価格推移チャート、右=資産推移＆AI判定 -->
  <section class="main-grid">
    <!-- 価格推移ローソク足チャート -->
    <div class="card">
      <div class="card-header">
        <div class="card-title">
          <span>📉 GMOコイン 現物リアルタイムチャート</span>
          <span id="chartTickerSummary" style="font-size:0.84rem; color:var(--accent-blue); font-family:monospace;"></span>
        </div>
        <div class="chart-toolbar">
          <div id="symbolChips" style="display:flex; gap:4px; flex-wrap:wrap;"></div>
          <span style="color:#475569;">|</span>
          <div id="intervalChips" style="display:flex; gap:4px;"></div>
        </div>
      </div>
      <div class="chart-canvas-wrap">
        <canvas id="priceChartCanvas"></canvas>
      </div>
      <div id="chartOverlayLegend" style="font-size:0.78rem; color:var(--text-secondary); display:flex; justify-content:space-between; flex-wrap:wrap;">
        <span>💡 チャート上に保有ポジションの Entry (青) / TP利確 (緑) / SL逆指値 (赤) / 待機指値 (黄) を自動描画します</span>
        <span id="chartUpdatedTime"></span>
      </div>
    </div>

    <!-- 右カラム: 資産推移グラフ ＆ 最新AI判定カード -->
    <div style="display:flex; flex-direction:column; gap:18px;">
      <div class="card">
        <div class="card-header">
          <div class="card-title">📈 総資産推移 (Equity Curve)</div>
          <span id="campaignPhaseBadge" style="font-size:0.76rem; color:var(--accent-blue);">PHASE1_ACTIVE</span>
        </div>
        <div class="eq-canvas-wrap">
          <canvas id="equityChartCanvas"></canvas>
        </div>
        <div id="campaignScheduleText" style="font-size:0.75rem; color:var(--text-secondary);">
          ・第1回締め: 2027-07-09 00:00 JST (2時間前エントリー凍結 ➔ 全決済)<br>
          ・第2回期間: 2027-07-10 00:00 〜 2027-12-10 00:00 JST
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <div class="card-title">🧠 最新レーダー＆AI判定 (`latest_decision.json`)</div>
          <span id="aiDecAction" class="badge badge-stopped">HOLD</span>
        </div>
        <div id="aiDecMeta" style="font-size:0.78rem; color:var(--text-secondary);"></div>
        <div id="aiDecReasoning" style="font-size:0.82rem; background:#090d16; padding:10px; border-radius:8px; border:1px solid #1e293b; max-height:140px; overflow-y:auto;">
          読み込み中...
        </div>
      </div>
    </div>
  </section>

  <!-- 保有ポジション ＆ 待機注文一覧 -->
  <section class="main-grid">
    <div class="card">
      <div class="card-header">
        <div class="card-title">🛡️ 保有中ポジション (有効建玉 ＆ 逆指値保護状態)</div>
        <span id="activePosCount" style="font-size:0.78rem; color:var(--text-secondary);">0 銘柄</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>銘柄</th>
              <th>数量</th>
              <th>取得単価</th>
              <th>現在Bid</th>
              <th>評価額</th>
              <th>含み損益</th>
              <th>SL逆指値 / TP利確</th>
              <th>トレーリング段階</th>
            </tr>
          </thead>
          <tbody id="activePositionsBody">
            <tr><td colspan="8" style="text-align:center; color:var(--text-secondary);">現在保有中の有効ポジションはありません</td></tr>
          </tbody>
        </table>
      </div>
      <div id="dustQuarantineBox" style="font-size:0.76rem; color:var(--text-secondary); padding-top:4px;"></div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-title">⏳ 待機中指値注文 (`pending_orders`)</div>
        <span id="pendingOrdCount" style="font-size:0.78rem; color:var(--text-secondary);">0 件</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>銘柄</th>
              <th>指値価格</th>
              <th>数量 (約定済)</th>
              <th>拘束額</th>
              <th>予定SL / TP</th>
              <th>状態</th>
            </tr>
          </thead>
          <tbody id="pendingOrdersBody">
            <tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">現在待機中の指値注文はありません</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- 決済履歴 ＆ リアルタイム稼働ログ -->
  <section class="main-grid">
    <div class="card">
      <div class="card-header">
        <div class="card-title">🏆 直近の確定決済履歴 (`closed_trades`)</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>決済日時</th>
              <th>銘柄</th>
              <th>数量</th>
              <th>Entry ➔ Exit</th>
              <th>確定損益</th>
              <th>決済理由</th>
            </tr>
          </thead>
          <tbody id="closedTradesBody">
            <tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">まだ確定した決済履歴はありません</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-title">📜 リアルタイム稼働ログ (`logs/v2_auto_*.log`)</div>
      </div>
      <div id="logViewer" class="log-box">ログ読み込み中...</div>
    </div>
  </section>

</main>

<!-- 2段階承認 FLATTEN_ALL モーダル -->
<div id="flattenModal" class="modal-backdrop">
  <div class="modal">
    <h3>🚨 緊急全決済 (FLATTEN_ALL) — 2段階承認</h3>
    <p>
      この操作を実行すると、<b>全ての未約定買い指値をキャンセル</b>し、<b>現在保有中の全ポジションを即時成行（MARKET）で全決済</b>した上で、新規エントリーを一時停止（PAUSE）します。
    </p>
    <div id="flattenNonceDisplay" class="nonce-box">ワンタイム承認コード発行中...</div>
    <div style="display:flex; justify-content:flex-end; gap:10px;">
      <button class="btn" onclick="closeFlattenModal()">キャンセル（戻る）</button>
      <button id="btnConfirmFlattenNow" class="btn btn-flatten" disabled onclick="confirmFlattenAll()">⚠️ 本当に全決済を実行する</button>
    </div>
  </div>
</div>

<script>
const SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC"];
const INTERVALS = [
  { key: "15min", label: "15分足" },
  { key: "1hour", label: "1時間足" },
  { key: "4hour", label: "4時間足" },
  { key: "1day",  label: "日足" }
];

let selectedSymbol = "BTC";
let selectedInterval = "1hour";
let currentFlattenNonce = "";
let lastChartData = null;
let lastEquityCurve = [];

function fmtJpy(val, showSign=false) {
  const n = Number(val || 0);
  const sign = showSign && n > 0 ? "+" : "";
  return sign + Math.round(n).toLocaleString("ja-JP") + " 円";
}

function fmtPrice(val) {
  const n = Number(val || 0);
  if (n >= 1000) return n.toLocaleString("ja-JP", { maximumFractionDigits: 1 });
  return n.toLocaleString("ja-JP", { maximumFractionDigits: 4 });
}

function showToast(msg, isError=false) {
  const el = document.getElementById("toastMsg");
  el.style.display = "block";
  el.style.borderColor = isError ? "#ef4444" : "#38bdf8";
  el.style.background = isError ? "rgba(239,68,68,0.18)" : "rgba(56,189,248,0.16)";
  el.textContent = msg;
  setTimeout(() => { el.style.display = "none"; }, 6000);
}

function renderToolbarChips(tickers={}) {
  const symWrap = document.getElementById("symbolChips");
  symWrap.innerHTML = "";
  SYMBOLS.forEach(sym => {
    const btn = document.createElement("button");
    btn.className = "chip" + (sym === selectedSymbol ? " active" : "");
    const t = tickers[sym];
    const pStr = t && t.last ? ` (${fmtPrice(t.last)})` : "";
    btn.textContent = sym + pStr;
    btn.onclick = () => {
      selectedSymbol = sym;
      renderToolbarChips(tickers);
      fetchChart();
    };
    symWrap.appendChild(btn);
  });

  const ivWrap = document.getElementById("intervalChips");
  ivWrap.innerHTML = "";
  INTERVALS.forEach(item => {
    const btn = document.createElement("button");
    btn.className = "chip" + (item.key === selectedInterval ? " active" : "");
    btn.textContent = item.label;
    btn.onclick = () => {
      selectedInterval = item.key;
      renderToolbarChips(tickers);
      fetchChart();
    };
    ivWrap.appendChild(btn);
  });
}

async function fetchStatus() {
  try {
    const res = await fetch("/api/status");
    const data = await res.json();
    updateDashboardUI(data);
  } catch (err) {
    console.error("Status fetch error:", err);
  }
}

async function fetchChart() {
  try {
    const res = await fetch(`/api/chart?symbol=${encodeURIComponent(selectedSymbol)}&interval=${encodeURIComponent(selectedInterval)}`);
    const data = await res.json();
    lastChartData = data;
    drawPriceChart(data);
  } catch (err) {
    console.error("Chart fetch error:", err);
  }
}

function updateDashboardUI(data) {
  if (!data || !data.daemon) return;
  const d = data.daemon;
  const eq = data.equity_summary || {};
  const camp = data.campaign || {};

  document.getElementById("clockJst").textContent = (data.timestamp || "").replace("T", " ");

  // Mode badge
  const bMode = document.getElementById("badgeMode");
  bMode.textContent = d.execution_mode || "LIVE";
  bMode.className = "badge " + (d.execution_mode === "LIVE" ? "badge-live" : "badge-dry");

  // Daemon & Pause state
  const bDaemon = document.getElementById("badgeDaemon");
  const btnPause = document.getElementById("btnPause");
  const btnResume = document.getElementById("btnResume");

  if (d.is_paused) {
    bDaemon.textContent = "⏸️ 一時停止中 (PAUSED: 新規BUY停止・SL監視継続)";
    bDaemon.className = "badge badge-paused";
    btnPause.disabled = true;
    btnResume.disabled = false;
  } else if (d.is_running) {
    bDaemon.textContent = "🟢 デーモン稼働中 (ACTIVE)";
    bDaemon.className = "badge badge-active";
    btnPause.disabled = false;
    btnResume.disabled = true;
  } else {
    bDaemon.textContent = "⚪ デーモン待機中 (タスク次回起動待ち)";
    bDaemon.className = "badge badge-stopped";
    btnPause.disabled = false;
    btnResume.disabled = false;
  }

  // Regime
  const bRegime = document.getElementById("badgeRegime");
  bRegime.textContent = `REGIME: ${d.market_regime || "NEUTRAL"}`;

  // Campaign
  document.getElementById("badgeCampaign").textContent = `🏁 ${camp.countdown_text || ""}`;
  document.getElementById("campaignPhaseBadge").textContent = `${camp.current_phase || "PHASE1_ACTIVE"} (${camp.phase_label_ja || ""})`;

  // KPI cards
  document.getElementById("kpiInitCap").textContent = `初期 ${fmtJpy(eq.initial_capital_jpy)}`;
  document.getElementById("kpiTotalEquity").textContent = fmtJpy(eq.total_equity_jpy);
  document.getElementById("kpiCashDetail").textContent =
    `現金: ${fmtJpy(eq.jpy_balance)} / 拘束: ${fmtJpy(eq.locked_pending_jpy)} / 建玉評価: ${fmtJpy(eq.positions_eval_jpy)}`;

  const totalPnl = Number(eq.total_pnl_jpy || 0);
  const elPnl = document.getElementById("kpiTotalPnl");
  elPnl.textContent = fmtJpy(totalPnl, true);
  elPnl.className = "kpi-value " + (totalPnl >= 0 ? "pos-val" : "neg-val");
  const pnlPct = Number(eq.total_pnl_pct || 0);
  document.getElementById("kpiTotalPnlPct").textContent = (pnlPct >= 0 ? "+" : "") + pnlPct.toFixed(2) + "%";
  document.getElementById("kpiRealizedDetail").textContent =
    `累計確定: ${fmtJpy(eq.realized_pnl_total_jpy, true)} / 保有含み: ${fmtJpy(eq.unrealized_pnl_total_jpy, true)}`;

  const pa = eq.pattern_a || {};
  document.getElementById("kpiMaxAlloc").textContent = `${fmtJpy(pa.max_position_jpy)} / 枠`;
  document.getElementById("kpiRisk1R").textContent =
    `1R許容損失(1.5%): ${fmtJpy(pa.risk_1r_normal_jpy)} (上限2.0%: ${fmtJpy(pa.risk_1r_stressed_jpy)})`;

  const l5 = (d.circuit_breakers || {}).l5_kill_switch || {};
  document.getElementById("kpiL5Floor").textContent = fmtJpy(eq.l5_kill_switch_jpy);
  document.getElementById("kpiPeakDetail").textContent =
    `最高到達資産(HWM): ${fmtJpy(eq.peak_equity_jpy)} (防衛余裕 ${fmtJpy(l5.headroom_jpy, true)})`;
  document.getElementById("kpiL5Status").textContent = l5.latched ? "🚨 L5発動中" : "🛡️ 正常保護";

  const l3 = (d.circuit_breakers || {}).l3_daily_loss || {};
  const l4 = (d.circuit_breakers || {}).l4_weekly_streak || {};
  document.getElementById("kpiDailyPnl").textContent =
    `日次: ${fmtJpy(l3.daily_total_pnl_jpy, true)} / 制限 ${fmtJpy(l3.daily_loss_limit_jpy)}`;
  document.getElementById("kpiWeeklyPnl").textContent =
    `週次: ${fmtJpy(l4.weekly_pnl_jpy, true)} (制限 ${fmtJpy(l4.weekly_loss_limit_jpy)}) / 連敗: ${l4.consecutive_losses || 0}回`;
  document.getElementById("kpiCbBadge").textContent = (l3.blocked || l4.blocked || l5.latched) ? "⛔ 制限中" : "🟢 稼働許可";

  // Toolbar with live prices
  renderToolbarChips(data.tickers || {});

  // Equity curve
  lastEquityCurve = eq.equity_curve || [];
  drawEquityChart(lastEquityCurve);

  // Latest AI Decision
  const dec = data.latest_decision || {};
  const act = String(dec.action || "HOLD").toUpperCase();
  const elAct = document.getElementById("aiDecAction");
  elAct.textContent = `${act} (${dec.symbol || "NONE"})`;
  elAct.className = "badge " + (act === "BUY" ? "badge-active" : (act === "SELL" ? "badge-live" : "badge-stopped"));
  document.getElementById("aiDecMeta").textContent =
    `判定時刻: ${(dec.timestamp || "--").replace("T", " ")} | 確信度: ${dec.confidence !== undefined ? dec.confidence : "--"} | ステータス: ${dec.status || "--"}`;
  document.getElementById("aiDecReasoning").textContent = dec.reasoning || "判定履歴なし";

  // Active Positions Table
  const actPos = (data.positions && data.positions.active) || [];
  document.getElementById("activePosCount").textContent = `${actPos.length} / ${(pa.max_concurrent_positions || 2)} 枠`;
  const posBody = document.getElementById("activePositionsBody");
  if (actPos.length === 0) {
    posBody.innerHTML = `<tr><td colspan="8" style="text-align:center; color:var(--text-secondary);">現在保有中の有効ポジションはありません</td></tr>`;
  } else {
    posBody.innerHTML = actPos.map(p => {
      const cls = p.unrealized_pnl_jpy >= 0 ? "pos-val" : "neg-val";
      return `<tr>
        <td><b>${p.symbol}</b></td>
        <td>${p.size}</td>
        <td>${fmtPrice(p.entry_price)} 円</td>
        <td>${fmtPrice(p.current_bid)} 円</td>
        <td>${fmtJpy(p.eval_jpy)}</td>
        <td class="${cls}"><b>${fmtJpy(p.unrealized_pnl_jpy, true)} (${p.unrealized_pnl_pct >= 0 ? "+" : ""}${p.unrealized_pnl_pct}%)</b></td>
        <td>SL: <span style="color:#fca5a5;">${fmtPrice(p.stop_loss_price)}</span> / TP: <span style="color:#6ee7b7;">${fmtPrice(p.take_profit_price)}</span></td>
        <td><span class="badge badge-campaign">${p.trailing_stage}</span></td>
      </tr>`;
    }).join("");
  }

  // Quarantined Dust
  const dustList = (data.positions && data.positions.quarantined_dust) || [];
  const dustBox = document.getElementById("dustQuarantineBox");
  if (dustList.length > 0) {
    dustBox.innerHTML = `📦 最小注文単位未満の隔離端数 (DUST_QUARANTINED — 保有枠・SL対象外): ` +
      dustList.map(d => `${d.symbol}: ${d.size} (約 ${d.estimated_value_jpy} 円)`).join(" / ");
  } else {
    dustBox.textContent = "";
  }

  // Pending Orders Table
  const pend = data.pending_orders || [];
  document.getElementById("pendingOrdCount").textContent = `${pend.length} 件`;
  const pendBody = document.getElementById("pendingOrdersBody");
  if (pend.length === 0) {
    pendBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">現在待機中の指値注文はありません</td></tr>`;
  } else {
    pendBody.innerHTML = pend.map(o => `<tr>
      <td><b>${o.symbol}</b></td>
      <td>${fmtPrice(o.limit_price)} 円</td>
      <td>${o.size} (${o.executed_size})</td>
      <td>${fmtJpy(o.reserved_jpy)}</td>
      <td>SL: ${fmtPrice(o.stop_loss_price)} / TP: ${fmtPrice(o.take_profit_price)}</td>
      <td><span class="badge badge-paused">${o.state}</span></td>
    </tr>`).join("");
  }

  // Closed Trades Table
  const closed = data.closed_trades || [];
  const closedBody = document.getElementById("closedTradesBody");
  if (closed.length === 0) {
    closedBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">まだ確定した決済履歴はありません</td></tr>`;
  } else {
    closedBody.innerHTML = closed.slice(0, 12).map(t => {
      const pnl = Number(t.pnl_jpy || 0);
      const cls = pnl >= 0 ? "pos-val" : "neg-val";
      return `<tr>
        <td>${String(t.closed_at || "").replace("T", " ").slice(0, 19)}</td>
        <td><b>${t.symbol || ""}</b></td>
        <td>${t.size || 0}</td>
        <td>${fmtPrice(t.entry_price)} ➔ ${fmtPrice(t.exit_price)}</td>
        <td class="${cls}"><b>${fmtJpy(pnl, true)}</b></td>
        <td>${t.reason || ""}</td>
      </tr>`;
    }).join("");
  }

  // Logs
  const logLines = data.logs || [];
  const logEl = document.getElementById("logViewer");
  logEl.textContent = logLines.length ? logLines.join("\n") : "ログはまだありません";
  logEl.scrollTop = logEl.scrollHeight;
}

function drawPriceChart(chartData) {
  const canvas = document.getElementById("priceChartCanvas");
  if (!canvas || !chartData) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.fillStyle = "#090d16";
  ctx.fillRect(0, 0, W, H);

  const candles = chartData.candles || [];
  const t = chartData.ticker || {};
  const ov = chartData.overlays || {};

  document.getElementById("chartTickerSummary").textContent =
    t.last ? `${chartData.symbol}/JPY  現在値: ${fmtPrice(t.last)} 円  (Bid: ${fmtPrice(t.bid)} / Ask: ${fmtPrice(t.ask)} / Spread: ${t.spread_pct || 0}%)` : `${chartData.symbol}/JPY`;
  document.getElementById("chartUpdatedTime").textContent =
    `更新: ${(chartData.timestamp || "").replace("T", " ")}`;

  if (candles.length === 0) {
    ctx.fillStyle = "#94a3b8";
    ctx.font = "14px sans-serif";
    ctx.fillText("ローソク足データがありません", W / 2 - 80, H / 2);
    return;
  }

  const padL = 16, padR = 88, padT = 20, padB = 28;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  let minP = Infinity, maxP = -Infinity;
  candles.forEach(c => {
    if (c.low < minP) minP = c.low;
    if (c.high > maxP) maxP = c.high;
  });
  [ov.entry_price, ov.stop_loss_price, ov.take_profit_price, ov.limit_price].forEach(p => {
    if (p && p > 0) {
      if (p < minP) minP = p;
      if (p > maxP) maxP = p;
    }
  });
  if (maxP <= minP) { maxP = minP + 10; minP = Math.max(0, minP - 10); }
  const margin = (maxP - minP) * 0.08;
  minP -= margin;
  maxP += margin;

  const yOf = (price) => padT + plotH - ((price - minP) / (maxP - minP)) * plotH;

  // Horizontal grid
  ctx.strokeStyle = "rgba(148,163,184,0.1)";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#64748b";
  ctx.font = "11px monospace";
  for (let i = 0; i <= 4; i++) {
    const pVal = minP + ((maxP - minP) * i) / 4;
    const y = yOf(pVal);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillText(fmtPrice(pVal), padL + plotW + 6, y + 4);
  }

  // Draw candlesticks
  const n = candles.length;
  const stepX = plotW / Math.max(1, n);
  const bodyW = Math.max(2, Math.min(12, stepX * 0.68));

  candles.forEach((c, idx) => {
    const cx = padL + idx * stepX + stepX / 2;
    const isUp = c.close >= c.open;
    const col = isUp ? "#10b981" : "#ef4444";
    ctx.strokeStyle = col;
    ctx.fillStyle = col;

    // Wick
    ctx.beginPath();
    ctx.moveTo(cx, yOf(c.high));
    ctx.lineTo(cx, yOf(c.low));
    ctx.stroke();

    // Body
    const yOpen = yOf(c.open);
    const yClose = yOf(c.close);
    const topY = Math.min(yOpen, yClose);
    const bh = Math.max(1.5, Math.abs(yClose - yOpen));
    ctx.fillRect(cx - bodyW / 2, topY, bodyW, bh);

    // X-axis time labels
    if (idx === 0 || idx === Math.floor(n / 2) || idx === n - 1) {
      ctx.fillStyle = "#64748b";
      ctx.font = "10px monospace";
      ctx.fillText(c.time_label || "", Math.max(padL, cx - 28), H - 8);
    }
  });

  // Draw horizontal overlay lines (Entry, TP, SL, Pending Limit)
  const drawHorizontalOverlay = (price, color, label) => {
    if (!price || price <= 0) return;
    const y = yOf(price);
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([5, 4]);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.font = "bold 11px monospace";
    ctx.fillText(`${label}: ${fmtPrice(price)}`, padL + 8, Math.max(14, y - 5));
    ctx.restore();
  };

  drawHorizontalOverlay(ov.entry_price, "#38bdf8", "ENTRY");
  drawHorizontalOverlay(ov.take_profit_price, "#10b981", "TP利確");
  drawHorizontalOverlay(ov.stop_loss_price, "#ef4444", "SL逆指値");
  drawHorizontalOverlay(ov.limit_price, "#f59e0b", "待機指値");
}

function drawEquityChart(curve) {
  const canvas = document.getElementById("equityChartCanvas");
  if (!canvas || !curve || curve.length === 0) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.fillStyle = "#090d16";
  ctx.fillRect(0, 0, W, H);

  const padL = 14, padR = 72, padT = 18, padB = 22;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  let minE = Infinity, maxE = -Infinity;
  curve.forEach(pt => {
    const v = Number(pt.equity_jpy || 10000);
    if (v < minE) minE = v;
    if (v > maxE) maxE = v;
  });
  if (maxE <= minE) { maxE = minE + 100; minE = Math.max(0, minE - 100); }
  const margin = (maxE - minE) * 0.15;
  minE -= margin;
  maxE += margin;

  const yOf = (val) => padT + plotH - ((val - minE) / (maxE - minE)) * plotH;
  const xOf = (idx) => padL + (curve.length <= 1 ? plotW / 2 : (idx / (curve.length - 1)) * plotW);

  // Area + Line
  ctx.beginPath();
  curve.forEach((pt, idx) => {
    const x = xOf(idx), y = yOf(pt.equity_jpy);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#38bdf8";
  ctx.lineWidth = 2.2;
  ctx.stroke();

  // Points & Labels
  curve.forEach((pt, idx) => {
    const x = xOf(idx), y = yOf(pt.equity_jpy);
    ctx.fillStyle = "#38bdf8";
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fill();
    if (idx === 0 || idx === curve.length - 1) {
      ctx.fillStyle = "#cbd5e1";
      ctx.font = "11px monospace";
      ctx.fillText(fmtJpy(pt.equity_jpy), padL + plotW + 6, y + 4);
    }
  });
}

async function sendControl(command, nonce="") {
  try {
    const res = await fetch("/api/control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command, nonce })
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      showToast(`エラー: ${data.error || "コマンド実行失敗"}`, true);
      return null;
    }
    showToast(data.message || `コマンド ${command} を実行しました`);
    await fetchStatus();
    return data;
  } catch (err) {
    showToast(`通信エラー: ${err}`, true);
    return null;
  }
}

function confirmCancelEntries() {
  if (!confirm("現在待機中の未約定買い指値（PENDING_BUY）をすべてキャンセルしますか？\n（※保有中ポジションと逆指値STOP注文は維持されます）")) {
    return;
  }
  sendControl("CANCEL_ENTRIES");
}

async function openFlattenModal() {
  const modal = document.getElementById("flattenModal");
  const box = document.getElementById("flattenNonceDisplay");
  const btn = document.getElementById("btnConfirmFlattenNow");
  modal.style.display = "flex";
  box.textContent = "ワンタイム承認コードを発行中...";
  btn.disabled = true;
  currentFlattenNonce = "";

  const data = await sendControl("FLATTEN_ALL");
  if (data && data.result && data.result.nonce) {
    currentFlattenNonce = data.result.nonce;
    box.textContent = `承認コード: ${currentFlattenNonce} (有効期限: ${(data.result.expires_at || "").replace("T", " ")})`;
    btn.disabled = false;
  } else {
    box.textContent = "承認コードの発行に失敗しました";
  }
}

function closeFlattenModal() {
  document.getElementById("flattenModal").style.display = "none";
  currentFlattenNonce = "";
}

async function confirmFlattenAll() {
  if (!currentFlattenNonce) return;
  const nonce = currentFlattenNonce;
  closeFlattenModal();
  await sendControl("FLATTEN_CONFIRM", nonce);
}

async function refreshAll(manual=false) {
  await Promise.all([fetchStatus(), fetchChart()]);
  if (manual) showToast("最新の稼働状態とチャートを更新しました");
}

window.addEventListener("resize", () => {
  if (lastChartData) drawPriceChart(lastChartData);
  if (lastEquityCurve) drawEquityChart(lastEquityCurve);
});

// 初回ロード＆15秒自動更新
refreshAll(false);
setInterval(() => { refreshAll(false); }, 15000);
</script>
</body>
</html>
"""


def make_dashboard_handler(service: WebDashboardService):
    """`WebDashboardService` をバインドした HTTP リクエストハンドラクラスを生成する"""

    class DashboardRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            # コンソール汚染を防ぐため DEBUG ログへ出力
            logger.debug("HTTP %s - %s", self.address_string(), format % args)

        def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def _send_html(self, status_code: int, html_text: str) -> None:
            raw = html_text.encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = parse_qs(parsed.query)

            if path == "/":
                self._send_html(HTTPStatus.OK, DASHBOARD_HTML)
                return

            if path == "/api/status":
                try:
                    data = service.get_status_payload()
                    self._send_json(HTTPStatus.OK, data)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            if path == "/api/chart":
                sym = (qs.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
                iv = (qs.get("interval", ["1hour"])[0] or "1hour").strip().lower()
                try:
                    data = service.get_chart_payload(symbol=sym, interval=iv)
                    self._send_json(HTTPStatus.OK, data)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Not found: {path}"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"

            if path == "/api/control":
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                    raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                    body = json.loads(raw_body) if raw_body.strip() else {}
                    cmd = str(body.get("command", "")).strip().upper()
                    nonce = str(body.get("nonce", "")).strip()
                    res = service.handle_control_command(command=cmd, nonce=nonce)
                    self._send_json(HTTPStatus.OK, res)
                except (ValueError, ReplayOrExpiredNonceError, UnauthorizedCommandError) as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Not found: {path}"})

    return DashboardRequestHandler


def create_dashboard_http_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    service: Optional[WebDashboardService] = None,
) -> Tuple[ThreadingHTTPServer, WebDashboardService]:
    svc = service or WebDashboardService()
    handler_cls = make_dashboard_handler(svc)
    server = ThreadingHTTPServer((host, int(port)), handler_cls)
    return server, svc


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="GMO AI Trader v2 ローカルWebダッシュボード")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="バインドホスト (既定: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="バインドポート (既定: 8080)")
    parser.add_argument("--open-browser", action="store_true", help="起動時に既定のブラウザでダッシュボードを開く")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    server, svc = create_dashboard_http_server(host=args.host, port=args.port)
    actual_port = server.server_address[1]
    url = f"http://{args.host}:{actual_port}"
    logger.info("🌐 GMO AI Trader v2 Webダッシュボード起動: %s", url)

    if args.open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Webダッシュボードを停止します...")
    finally:
        server.server_close()
        svc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
