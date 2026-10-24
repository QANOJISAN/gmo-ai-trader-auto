"""
5段階サーキットブレーカー (L1〜L5) ＆ Windows 24時間常駐耐障害性モジュール (circuit_breaker.py)

1. MultiTierCircuitBreaker:
   - L1 (個別銘柄一時除外): スプレッド急拡大 (> 0.50%) または銘柄データ遅延 (> 5.0秒)。
   - L2 (API通信・レート制限・データ鮮度異常): API連続エラー (>= 5回) またはデータ鮮度 (> 45秒) で
     新規エントリーを一時停止（SL監視・保護は継続）。
   - L3 (日次損失制限): GMO取引日基準（毎朝 06:00 JST 区切り）の当日確定損益＋現在含み損益が
     `-300円`（-3.0% / 2R）以下に達した場合、翌 06:00 JST まで新規 `BUY` を停止。
   - L4 (週間損失制限・連敗クールダウン):
     * 4連敗到達で 12時間の新規 `BUY` クールダウン
     * 直近7日間の累計損失が `-600円`（-6.0%）以下で 24時間の新規 `BUY` 停止
   - L5 (ハード・キルスイッチ `Total Equity < 8,500円`):
     * 総資産の算出には `last`（最終約定値）ではなく、必ず保守的な即時清算価値
       `Best Bid * (1 - Taker Fee)` を用いる。
     * 瞬間的なヒゲやAPI欠損（Best Bid = 0 等）による誤爆を防ぐため、有効な Best Bid が揃った状態で
       **15秒間隔×3回連続（45秒間）** 8,500円を下回った場合にのみ発動（ラッチ）する。
     * 発動時は未約定の `BUY` 指値を全キャンセルし、手動解除されるまで新規 `BUY` を恒久ロックする。

2. Windows 24時間常駐耐障害性ユーティリティ:
   - `prevent_windows_sleep()`: `SetThreadExecutionState(0x80000041)` によるOSスリープ・Awayモード抑止
   - `SingleInstanceLock`: 排他ロックファイル (`v2_daemon.lock` / `msvcrt.locking` + PID検証) による多重起動完全防止
   - `check_memory_rss_and_should_restart()`: RSSメモリ使用量監視（上限超過かつポジション・待機注文ゼロの安全なタイミングでのみ計画再起動フラグを返す）
   - 土曜メンテナンス事前クリア (`08:50〜08:55 JST`) ＆ `PREOPEN` / `ERR-5203` 待機ガード
"""

from __future__ import annotations

import ctypes
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.llm_shadow_evaluator import SHADOW_DISCOUNT_WEIGHT, evaluate_mintrl_freeze_stage
from src.regime_filter import BLACKLISTED_SYMBOLS, TIER1_SYMBOLS, TIER2_SYMBOLS
from src.validator import DEFAULT_SYMBOL_RULES, _to_decimal, compute_anti_martingale_risk_params

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Wald's SPRT 定数 (p0=0.55 vs p1=0.40, alpha=0.05, beta=0.10)
SPRT_P0_HEALTHY_WIN_RATE = 0.55
SPRT_P1_DEGRADED_WIN_RATE = 0.40
SPRT_ALPHA = 0.05
SPRT_BETA = 0.10
SPRT_WIN_LLR_COEFF = -0.3185        # ln(0.40 / 0.55)
SPRT_LOSS_LLR_COEFF = 0.2877        # ln(0.60 / 0.45)
SPRT_LOWER_BOUNDARY_B = -2.2513     # ln(beta / (1 - alpha)) = ln(0.10 / 0.95)
SPRT_UPPER_BOUNDARY_A = 2.8904      # ln((1 - beta) / alpha) = ln(0.90 / 0.05)

# Page's Lower CUSUM 定数 (mu0=+0.15, sigma_R=1.05, k=0.5, h=4.0, 1R=300 JPY)
CUSUM_MU_0 = 0.15
CUSUM_SIGMA_R = 1.05
CUSUM_SLACK_K = 0.50
CUSUM_THRESHOLD_H = 4.00
CUSUM_BASE_1R_JPY = 300.0


def evaluate_losing_streak_ladder(
    consecutive_losses: int,
    base_slot_cap_jpy: float = 9_000.0,
) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装3】: 連敗段階ガード (Losing Streak Ladder)
    - 0〜4連敗 (Green/Normal): Tier 1 + Tier 2 許可、1枠上限 100% (9,000円)
    - 5連敗 (Yellow)         : 対象を `Tier 1 (4銘柄: BTC, ETH, XRP, SOL)` のみに限定
    - 6連敗 (Orange)         : `Tier 1` のみ ＋ 1枠上限を `50%` (`4,500円`) に縮小
    - 7連敗 (Red)            : 新規発注を一時停止し Shadow 追跡モードへ移行
    - 8連敗以上 (Halt)       : システム保護 Halt (実発注完全停止)
    """
    streak = max(0, int(consecutive_losses or 0))
    base_cap = max(1000.0, float(base_slot_cap_jpy or 9000.0))

    if streak < 5:
        return {
            "consecutive_losses": streak,
            "color": "GREEN",
            "ladder_stage": "GREEN_NORMAL",
            "allowed_tiers": ["TIER1", "TIER2"],
            "allowed_symbols": sorted(TIER1_SYMBOLS | TIER2_SYMBOLS),
            "tier1_only": False,
            "slot_cap_mult": 1.00,
            "max_slot_cap_jpy": round(base_cap, 2),
            "max_slot_jpy": round(base_cap, 2),
            "allow_live_buy": True,
            "shadow_only_mode": False,
            "system_halt": False,
        }
    if streak == 5:
        return {
            "consecutive_losses": streak,
            "color": "YELLOW",
            "ladder_stage": "YELLOW_TIER1_ONLY",
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": sorted(TIER1_SYMBOLS),
            "tier1_only": True,
            "slot_cap_mult": 1.00,
            "max_slot_cap_jpy": round(base_cap, 2),
            "max_slot_jpy": round(base_cap, 2),
            "allow_live_buy": True,
            "shadow_only_mode": False,
            "system_halt": False,
        }
    if streak == 6:
        return {
            "consecutive_losses": streak,
            "color": "ORANGE",
            "ladder_stage": "ORANGE_HALF_SLOT",
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": sorted(TIER1_SYMBOLS),
            "tier1_only": True,
            "slot_cap_mult": 0.50,
            "max_slot_cap_jpy": 4500.0,
            "max_slot_jpy": 4500.0,
            "allow_live_buy": True,
            "shadow_only_mode": False,
            "system_halt": False,
        }
    if streak == 7:
        return {
            "consecutive_losses": streak,
            "color": "RED",
            "ladder_stage": "RED_SHADOW_ONLY",
            "allowed_tiers": [],
            "allowed_symbols": [],
            "tier1_only": True,
            "slot_cap_mult": 0.00,
            "max_slot_cap_jpy": 0.0,
            "max_slot_jpy": 0.0,
            "allow_live_buy": False,
            "shadow_only_mode": True,
            "system_halt": False,
        }
    return {
        "consecutive_losses": streak,
        "color": "HALT",
        "ladder_stage": "HALT_SYSTEM_PROTECTION",
        "allowed_tiers": [],
        "allowed_symbols": [],
        "tier1_only": True,
        "slot_cap_mult": 0.00,
        "max_slot_cap_jpy": 0.0,
        "max_slot_jpy": 0.0,
        "allow_live_buy": False,
        "shadow_only_mode": True,
        "system_halt": True,
    }


def compute_wald_sprt_step(
    prev_llr: float,
    is_win: bool,
    weight: float = 1.0,
) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装3】: Wald's SPRT (p0=0.55 vs p1=0.40, alpha=0.05, beta=0.10)
    - トレード決済ごとに対数尤度比 LLR_n = -0.3185 * W_n + 0.2877 * L_n を更新
    - 下側健全境界 B = -2.2513 以下で LLR_n = 0 にリセット (HEALTHY_RESET_ZERO)
    - 上側劣化境界 A = +2.8904 以上で勝率劣化アラームを発火 (DEGRADATION_ALARM: ロット50%縮小またはShadow移行)
    """
    w = max(0.0, float(weight))
    delta = (SPRT_WIN_LLR_COEFF if is_win else SPRT_LOSS_LLR_COEFF) * w
    raw_llr = float(prev_llr) + delta

    if raw_llr <= SPRT_LOWER_BOUNDARY_B:
        return {
            "llr_before_reset": round(raw_llr, 4),
            "llr": 0.0,
            "delta_llr": round(delta, 4),
            "status": "HEALTHY_RESET_ZERO",
            "healthy_reset_triggered": True,
            "sprt_alarm": False,
            "alarm_triggered": False,
            "recommended_action": "NORMAL_OPERATION",
            "lot_scale_mult": 1.0,
            "lower_boundary_b": SPRT_LOWER_BOUNDARY_B,
            "upper_boundary_a": SPRT_UPPER_BOUNDARY_A,
        }
    if raw_llr >= SPRT_UPPER_BOUNDARY_A:
        return {
            "llr_before_reset": round(raw_llr, 4),
            "llr": round(raw_llr, 4),
            "delta_llr": round(delta, 4),
            "status": "DEGRADATION_ALARM",
            "healthy_reset_triggered": False,
            "sprt_alarm": True,
            "alarm_triggered": True,
            "recommended_action": "HALF_LOT_OR_SHADOW",
            "lot_scale_mult": 0.50,
            "lower_boundary_b": SPRT_LOWER_BOUNDARY_B,
            "upper_boundary_a": SPRT_UPPER_BOUNDARY_A,
        }
    return {
        "llr_before_reset": round(raw_llr, 4),
        "llr": round(raw_llr, 4),
        "delta_llr": round(delta, 4),
        "status": "CONTINUING",
        "healthy_reset_triggered": False,
        "sprt_alarm": False,
        "alarm_triggered": False,
        "recommended_action": "NORMAL_OPERATION",
        "lot_scale_mult": 1.0,
        "lower_boundary_b": SPRT_LOWER_BOUNDARY_B,
        "upper_boundary_a": SPRT_UPPER_BOUNDARY_A,
    }


def compute_pages_lower_cusum_step(
    prev_s_minus: float,
    net_pnl_jpy: Optional[float] = None,
    r_multiple: Optional[float] = None,
    base_1r_jpy: float = CUSUM_BASE_1R_JPY,
) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装3】: Page's Lower CUSUM (mu0=+0.15, sigma_R=1.05, k=0.5, h=4.0)
    - 実現R倍数 r_i = NetPnL_i / 300 に対し:
        S_i^- = max(0, S_{i-1}^- - (r_i - 0.15) / 1.05 - 0.5)
    - S_i^- > 4.0 到達でアラーム発火（銘柄別で S_i^- > 4.0 到達した銘柄は動的ブラックリストへ自動隔離）
    """
    if r_multiple is not None and net_pnl_jpy is None:
        r_i = float(r_multiple)
    else:
        denom = max(1.0, float(base_1r_jpy or CUSUM_BASE_1R_JPY))
        r_i = float(net_pnl_jpy or 0.0) / denom

    s_prev = max(0.0, float(prev_s_minus or 0.0))
    s_next = max(0.0, s_prev - ((r_i - CUSUM_MU_0) / CUSUM_SIGMA_R) - CUSUM_SLACK_K)
    breached = bool(s_next > CUSUM_THRESHOLD_H)

    return {
        "r_multiple": round(r_i, 4),
        "prev_s_minus": round(s_prev, 4),
        "s_minus": round(s_next, 4),
        "threshold_h": CUSUM_THRESHOLD_H,
        "alarm_triggered": breached,
    }

# Windows SetThreadExecutionState フラグ
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_AWAYMODE_REQUIRED = 0x00000040
ES_KEEP_AWAKE_FLAGS = ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED  # 0x80000041


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def get_gmo_trading_day_start(now_dt: Optional[datetime] = None) -> datetime:
    """GMOコインの日次区切り（毎朝 06:00 JST）の当日開始時刻を返す"""
    now_jst = _now_jst(now_dt)
    base = now_jst.replace(hour=6, minute=0, second=0, microsecond=0)
    if now_jst.hour < 6:
        base -= timedelta(days=1)
    return base


def get_next_gmo_trading_day_start(now_dt: Optional[datetime] = None) -> datetime:
    """翌営業日の 06:00 JST を返す"""
    return get_gmo_trading_day_start(now_dt) + timedelta(days=1)


def prevent_windows_sleep() -> bool:
    """
    Windows API `SetThreadExecutionState(0x80000041)` を呼び出し、
    24時間常駐デーモン稼働中のOSスリープ・休止状態への移行を抑止する。
    """
    try:
        if os.name == "nt" and hasattr(ctypes, "windll"):
            res = ctypes.windll.kernel32.SetThreadExecutionState(ES_KEEP_AWAKE_FLAGS)
            return bool(res != 0)
    except Exception as exc:
        logger.debug("SetThreadExecutionState 呼び出しスキップ: %s", exc)
    return False


def is_in_saturday_pre_maintenance_clear_window(now_dt: Optional[datetime] = None) -> bool:
    """毎週土曜日 08:50 〜 08:55 JST（定期メンテ直前の未約定BUY指値クリア枠）かどうかを判定する"""
    now_jst = _now_jst(now_dt)
    if now_jst.weekday() != 5:
        return False
    minutes = now_jst.hour * 60 + now_jst.minute
    return (8 * 60 + 50) <= minutes < (8 * 60 + 55)


def check_memory_rss_and_should_restart(
    max_rss_mb: float = 512.0,
    has_open_positions_or_orders: bool = False,
    simulated_rss_mb: Optional[float] = None,
) -> Dict[str, Any]:
    """
    プロセスの RSS メモリ使用量を検査し、閾値 (`max_rss_mb`) を超え、かつ
    保有ポジションおよび待機注文がゼロ（フラット状態）のときのみ計画再起動を推奨する。
    """
    rss_mb = 0.0
    if simulated_rss_mb is not None:
        rss_mb = float(simulated_rss_mb)
    else:
        try:
            import psutil

            proc = psutil.Process(os.getpid())
            rss_mb = proc.memory_info().rss / (1024.0 * 1024.0)
        except Exception:
            rss_mb = 50.0

    exceeded = rss_mb >= float(max_rss_mb)
    should_restart = bool(exceeded and not has_open_positions_or_orders)
    return {
        "rss_mb": round(rss_mb, 2),
        "max_rss_mb": float(max_rss_mb),
        "exceeded": exceeded,
        "has_open_positions_or_orders": has_open_positions_or_orders,
        "should_graceful_restart": should_restart,
    }


class SingleInstanceLock:
    """
    排他ファイルロック (`v2_daemon.lock`) により AutoTraderDaemon の二重起動を防止するクラス。
    """

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = Path(lock_path)
        self._fh: Optional[Any] = None
        self.acquired: bool = False

    def acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fh = open(self.lock_path, "a+b")
            fh.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            fh.seek(0)
            fh.truncate()
            fh.write(f"{os.getpid()}\n".encode("ascii"))
            fh.flush()
            self._fh = fh
            self.acquired = True
            return True
        except Exception:
            try:
                fh.close()  # type: ignore[has-type]
            except Exception:
                pass
            self.acquired = False
            return False

    def release(self) -> None:
        if self._fh is not None:
            try:
                self._fh.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None
        self.acquired = False

    def is_locked_by_another_process(self) -> bool:
        """別プロセスまたは別インスタンスによって排他ロックが保持中かどうかを非破壊で検査する"""
        if self.acquired:
            return False
        if not self.lock_path.exists():
            return False
        fh = None
        try:
            fh = open(self.lock_path, "a+b")
            fh.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            return False
        except Exception:
            return True
        finally:
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass


class MultiTierCircuitBreaker:
    """
    L1〜L5 の5段階サーキットブレーカー統合クラス
    """

    def __init__(
        self,
        l1_max_spread_pct: float = 0.50,
        l1_max_latency_sec: float = 5.0,
        l2_max_consecutive_api_errors: int = 5,
        l2_pause_minutes: int = 15,
        l2_max_data_age_sec: float = 45.0,
        l3_daily_loss_limit_jpy: float = -300.0,
        l4_max_consecutive_losses: int = 4,
        l4_streak_cooldown_hours: float = 12.0,
        l4_weekly_loss_limit_jpy: float = -600.0,
        l4_weekly_cooldown_hours: float = 24.0,
        l5_kill_switch_equity_jpy: float = 17000.0,
        l5_required_consecutive_breaches: int = 3,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self.l1_max_spread_pct = float(l1_max_spread_pct)
        self.l1_max_latency_sec = float(l1_max_latency_sec)
        self.l2_max_consecutive_api_errors = int(l2_max_consecutive_api_errors)
        self.l2_pause_minutes = int(l2_pause_minutes)
        self.l2_max_data_age_sec = float(l2_max_data_age_sec)
        self.l3_daily_loss_limit_jpy = Decimal(str(l3_daily_loss_limit_jpy))
        self.l4_max_consecutive_losses = int(l4_max_consecutive_losses)
        self.l4_streak_cooldown_hours = float(l4_streak_cooldown_hours)
        self.l4_weekly_loss_limit_jpy = Decimal(str(l4_weekly_loss_limit_jpy))
        self.l4_weekly_cooldown_hours = float(l4_weekly_cooldown_hours)
        self.l5_kill_switch_equity_jpy = Decimal(str(l5_kill_switch_equity_jpy))
        self.l5_required_consecutive_breaches = int(l5_required_consecutive_breaches)
        self.symbol_rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}

        # 状態変数
        self.consecutive_api_errors: int = 0
        self.l2_paused_until: Optional[datetime] = None
        self.l3_paused_until: Optional[datetime] = None
        self.l4_paused_until: Optional[datetime] = None
        self.consecutive_losses: int = 0
        self.l5_consecutive_breach_count: int = 0
        self.l5_kill_switch_latched: bool = False
        self.l5_last_conservative_equity_jpy: Decimal = Decimal("20000")
        self.hwm_candidate_readings: List[Decimal] = []

        # Deep Research #2 【追加実装3】: Wald's SPRT & Page's Lower CUSUM 状態変数
        self.sprt_llr: float = 0.0
        self.sprt_wins_since_reset: int = 0
        self.sprt_losses_since_reset: int = 0
        self.sprt_alarm_active: bool = False
        self.portfolio_cusum_s_minus: float = 0.0
        self.symbol_cusum_s_minus: Dict[str, float] = {}
        self.dynamic_blacklisted_symbols: set[str] = set()

    @property
    def _dynamic_blacklisted_symbols(self) -> set[str]:
        return self.dynamic_blacklisted_symbols

    @property
    def sprt_alarm_latched(self) -> bool:
        return self.sprt_alarm_active

    @property
    def _sprt_llr(self) -> float:
        return self.sprt_llr

    # ------------------------------------------------------------------
    # L1: 個別銘柄一時除外（スプレッド急拡大・データ遅延）
    # ------------------------------------------------------------------
    def check_l1_symbol_filter(
        self,
        symbol: str,
        spread_pct: float,
        data_latency_sec: float = 0.0,
    ) -> Dict[str, Any]:
        reasons: List[str] = []
        if spread_pct > self.l1_max_spread_pct:
            reasons.append(f"L1: {symbol} スプレッド拡大 ({spread_pct:.3f}% > {self.l1_max_spread_pct:.2f}%)")
        if data_latency_sec > self.l1_max_latency_sec:
            reasons.append(f"L1: {symbol} データ遅延 ({data_latency_sec:.1f}s > {self.l1_max_latency_sec:.1f}s)")
        return {
            "symbol": symbol,
            "excluded": len(reasons) > 0,
            "reasons": reasons,
        }

    # ------------------------------------------------------------------
    # L2: API連続エラー・データ鮮度異常
    # ------------------------------------------------------------------
    def record_api_success(self) -> None:
        self.consecutive_api_errors = 0
        self.l2_paused_until = None

    def record_api_error(self, exc: Exception, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        self.consecutive_api_errors += 1
        triggered = False
        if self.consecutive_api_errors >= self.l2_max_consecutive_api_errors:
            triggered = True
            self.l2_paused_until = now_jst + timedelta(minutes=self.l2_pause_minutes)
        return {
            "level": "L2",
            "consecutive_api_errors": self.consecutive_api_errors,
            "triggered": triggered,
            "paused_until": self.l2_paused_until.isoformat(timespec="seconds") if self.l2_paused_until else None,
            "error": str(exc),
        }

    def check_l2_data_freshness(
        self,
        data_age_sec: float,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        in_error_pause = bool(self.l2_paused_until and now_jst < self.l2_paused_until)
        stale = data_age_sec > self.l2_max_data_age_sec
        blocked = in_error_pause or stale
        reasons: List[str] = []
        if in_error_pause and self.l2_paused_until:
            reasons.append(f"L2: API連続エラー一時停止中 (〜{self.l2_paused_until.isoformat(timespec='seconds')})")
        if stale:
            reasons.append(f"L2: 市場データ鮮度劣化 ({data_age_sec:.1f}s > {self.l2_max_data_age_sec:.1f}s)")
        return {
            "level": "L2",
            "blocked": blocked,
            "allow_sl_monitoring": True,
            "reasons": reasons,
        }

    # ------------------------------------------------------------------
    # L3: 日次損失制限（06:00 JST 区切りで -300円 / -3.0% / 2R 到達時）
    # ------------------------------------------------------------------
    def check_l3_daily_loss(
        self,
        portfolio: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        if self.l3_paused_until and now_jst >= self.l3_paused_until:
            self.l3_paused_until = None

        day_start = get_gmo_trading_day_start(now_jst)
        next_day_start = get_next_gmo_trading_day_start(now_jst)

        # 当日 06:00 JST 以降の確定損益を集計
        realized_today = Decimal("0")
        for trade in portfolio.get("closed_trades", []) or []:
            closed_at_str = str(trade.get("closed_at", "")).strip()
            pnl_val = _to_decimal(trade.get("realized_pnl_jpy", trade.get("pnl_jpy", 0)))
            if closed_at_str:
                try:
                    c_dt = _now_jst(datetime.fromisoformat(closed_at_str))
                    if c_dt >= day_start:
                        realized_today += pnl_val
                except Exception:
                    pass
            else:
                realized_today += pnl_val

        # 現在保有ポジションの含み損益（保守的 Best Bid 評価）
        unrealized = Decimal("0")
        for pos in portfolio.get("positions", []) or []:
            if str(pos.get("state", "PROTECTED")).upper() == "DUST_QUARANTINED":
                continue
            sym = str(pos.get("symbol", "")).upper()
            size_dec = _to_decimal(pos.get("size", 0))
            entry_dec = _to_decimal(pos.get("entry_price", 0))
            if size_dec <= 0 or entry_dec <= 0:
                continue
            t_info = (tickers or {}).get(sym, {})
            bid_dec = _to_decimal(t_info.get("bid", t_info.get("best_bid", pos.get("current_price", entry_dec))))
            if bid_dec <= 0:
                bid_dec = entry_dec
            unrealized += (bid_dec - entry_dec) * size_dec

        daily_total_pnl = realized_today + unrealized
        current_eq, _ = self.calculate_conservative_equity_jpy(portfolio, tickers)
        explicit_daily_start = _to_decimal(
            portfolio.get("day_start_equity_jpy", portfolio.get("daily_starting_equity_jpy", 0))
        )
        inferred_daily_start = current_eq - daily_total_pnl
        if explicit_daily_start > Decimal("0"):
            daily_starting_eq = explicit_daily_start
        elif "initial_capital_jpy" in portfolio or "initial_equity_jpy" in portfolio:
            daily_starting_eq = max(
                _to_decimal(portfolio.get("initial_capital_jpy", portfolio.get("initial_equity_jpy", 20000))),
                inferred_daily_start,
            )
        else:
            daily_starting_eq = inferred_daily_start if inferred_daily_start > Decimal("0") else Decimal("20000")
        base_l3_abs = abs(self.l3_daily_loss_limit_jpy)
        dynamic_l3_abs = max(base_l3_abs, (daily_starting_eq * Decimal("0.03")).quantize(Decimal("0.01")))
        effective_l3_limit = -dynamic_l3_abs

        if daily_total_pnl <= effective_l3_limit:
            self.l3_paused_until = next_day_start

        blocked = bool(self.l3_paused_until and now_jst < self.l3_paused_until)
        return {
            "level": "L3",
            "blocked": blocked,
            "day_start_equity_jpy": float(daily_starting_eq),
            "daily_starting_equity_jpy": float(daily_starting_eq),
            "daily_loss_limit_jpy": float(effective_l3_limit),
            "daily_realized_pnl_jpy": float(realized_today),
            "daily_unrealized_pnl_jpy": float(unrealized),
            "daily_total_pnl_jpy": float(daily_total_pnl),
            "paused_until": self.l3_paused_until.isoformat(timespec="seconds") if self.l3_paused_until else None,
        }

    # ------------------------------------------------------------------
    # L4: 週間損失制限 (-600円 -> 24h停止) ＆ 4連敗クールダウン (12h停止)
    # ------------------------------------------------------------------
    def check_l4_weekly_and_streak(
        self,
        portfolio: Dict[str, Any],
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        if self.l4_paused_until and now_jst >= self.l4_paused_until:
            self.l4_paused_until = None

        closed_trades: List[Dict[str, Any]] = portfolio.get("closed_trades", []) or []
        # 直近の連敗数をカウント
        streak = 0
        for trade in reversed(closed_trades):
            pnl = _to_decimal(trade.get("realized_pnl_jpy", trade.get("pnl_jpy", 0)))
            if pnl < Decimal("0"):
                streak += 1
            else:
                break
        self.consecutive_losses = streak

        # 直近7日間の確定損益合計
        week_cutoff = now_jst - timedelta(days=7)
        weekly_pnl = Decimal("0")
        for trade in closed_trades:
            pnl = _to_decimal(trade.get("realized_pnl_jpy", trade.get("pnl_jpy", 0)))
            closed_at_str = str(trade.get("closed_at", "")).strip()
            if closed_at_str:
                try:
                    c_dt = _now_jst(datetime.fromisoformat(closed_at_str))
                    if c_dt >= week_cutoff:
                        weekly_pnl += pnl
                except Exception:
                    weekly_pnl += pnl
            else:
                weekly_pnl += pnl

        current_eq, _ = self.calculate_conservative_equity_jpy(portfolio)
        explicit_weekly_start = _to_decimal(
            portfolio.get("week_start_equity_jpy", portfolio.get("weekly_starting_equity_jpy", 0))
        )
        weekly_starting_eq = (
            explicit_weekly_start
            if explicit_weekly_start > Decimal("0")
            else max(
                _to_decimal(portfolio.get("initial_capital_jpy", 20000)),
                current_eq - weekly_pnl,
            )
        )
        base_l4_abs = abs(self.l4_weekly_loss_limit_jpy)
        dynamic_l4_abs = max(base_l4_abs, (weekly_starting_eq * Decimal("0.06")).quantize(Decimal("0.01")))
        effective_l4_limit = -dynamic_l4_abs

        reasons: List[str] = []
        if weekly_pnl <= effective_l4_limit:
            target_until = now_jst + timedelta(hours=self.l4_weekly_cooldown_hours)
            if self.l4_paused_until is None or target_until > self.l4_paused_until:
                self.l4_paused_until = target_until
            reasons.append(f"L4: 直近7日間損失上限到達 ({float(weekly_pnl):+.0f}円 <= {float(effective_l4_limit):.0f}円 -> 24h停止)")

        if streak >= self.l4_max_consecutive_losses:
            target_until = now_jst + timedelta(hours=self.l4_streak_cooldown_hours)
            if self.l4_paused_until is None or target_until > self.l4_paused_until:
                self.l4_paused_until = target_until
            reasons.append(f"L4: {streak}連敗到達 (>= {self.l4_max_consecutive_losses}連敗 -> 12hクールダウン)")

        ladder_info = evaluate_losing_streak_ladder(streak)
        if not ladder_info["allow_live_buy"]:
            reasons.append(
                f"連敗段階ガード ({ladder_info['ladder_stage']}): {streak}連敗により実発注停止・Shadow/Halt保護"
            )

        blocked = bool((self.l4_paused_until and now_jst < self.l4_paused_until) or not ladder_info["allow_live_buy"])
        return {
            "level": "L4",
            "blocked": blocked,
            "week_start_equity_jpy": float(weekly_starting_eq),
            "weekly_starting_equity_jpy": float(weekly_starting_eq),
            "weekly_loss_limit_jpy": float(effective_l4_limit),
            "consecutive_losses": streak,
            "losing_streak_ladder": ladder_info,
            "weekly_pnl_jpy": float(weekly_pnl),
            "paused_until": self.l4_paused_until.isoformat(timespec="seconds") if self.l4_paused_until else None,
            "reasons": reasons,
        }

    # ------------------------------------------------------------------
    # L5: ハード・キルスイッチ (Best Bid * (1 - Taker Fee) で3回連続 < 17,000円 / 初期元本85%)
    # ------------------------------------------------------------------
    def calculate_conservative_equity_jpy(
        self,
        portfolio: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> tuple[Decimal, bool]:
        """
        `last`（最終約定値）ではなく、保守的な即時清算価値 `Best Bid * (1 - Taker Fee)` を用いて総資産を計算する。
        保有銘柄の Best Bid が欠損（0以下）している場合は `(equity, False)` を返し、API欠損による誤判定を防ぐ。
        """
        if "jpy_balance" in portfolio and portfolio.get("jpy_balance") is not None:
            jpy_balance = _to_decimal(portfolio.get("jpy_balance", 20000))
        elif "jpy_amount" in portfolio and portfolio.get("jpy_amount") is not None:
            jpy_balance = _to_decimal(portfolio.get("jpy_amount", 20000))
        else:
            avail_jpy = _to_decimal(portfolio.get("jpy_available", 20000))
            locked_pending_jpy = Decimal("0")
            for p_ord in portfolio.get("pending_orders", []) or []:
                if str(p_ord.get("side", "BUY")).upper() == "BUY" and str(
                    p_ord.get("state", "PENDING_BUY")
                ).upper() in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED"):
                    p_price = _to_decimal(p_ord.get("limit_price", p_ord.get("price", 0)))
                    p_size = _to_decimal(p_ord.get("size", 0)) - _to_decimal(p_ord.get("executed_size", 0))
                    if p_price > Decimal("0") and p_size > Decimal("0"):
                        locked_pending_jpy += p_price * p_size
            jpy_balance = avail_jpy + locked_pending_jpy
        positions: List[Dict[str, Any]] = portfolio.get("positions", []) or []
        tickers_map = tickers or {}

        liquidation_value = Decimal("0")
        all_bids_valid = True

        for pos in positions:
            if str(pos.get("state", "PROTECTED")).upper() == "DUST_QUARANTINED":
                continue
            sym = str(pos.get("symbol", "")).upper()
            size_dec = _to_decimal(pos.get("size", 0))
            if size_dec <= Decimal("0"):
                continue

            t_info = tickers_map.get(sym, {})
            best_bid_raw = t_info.get("bid", t_info.get("best_bid"))
            best_ask_raw = t_info.get("ask", t_info.get("best_ask"))
            best_bid_dec = _to_decimal(best_bid_raw)
            best_ask_dec = _to_decimal(best_ask_raw)
            ref_price_dec = _to_decimal(pos.get("current_price") or pos.get("entry_price", 0))

            is_quote_corrupt = False
            if best_bid_dec <= Decimal("0"):
                is_quote_corrupt = True
            elif best_ask_dec > Decimal("0") and (
                best_bid_dec > best_ask_dec
                or ((best_ask_dec - best_bid_dec) / best_bid_dec) > Decimal("0.10")
            ):
                # Crossed book (bid > ask) または 10% 超の異常スプレッド欠損
                is_quote_corrupt = True
            elif ref_price_dec > Decimal("0") and best_bid_dec > (ref_price_dec * Decimal("1.50")):
                # 単発 +50% 超の異常ティッカースパイク（通信ノイズ・板バグ）
                is_quote_corrupt = True

            if is_quote_corrupt:
                all_bids_valid = False
                best_bid_dec = ref_price_dec if ref_price_dec > Decimal("0") else max(best_bid_dec, Decimal("0"))

            rule = self.symbol_rules.get(sym, {})
            taker_fee_dec = max(_to_decimal(rule.get("taker_fee", "0.0009")), Decimal("0"))
            net_bid_price = best_bid_dec * (Decimal("1") - taker_fee_dec)
            liquidation_value += net_bid_price * size_dec

        # 未約定の買い指値で拘束されている予約資金が jpy_balance から既に差し引かれている場合の補正は不要だが、
        # PaperBroker のように約定時に引き落とす設計では jpy_balance + liquidation_value が即時清算総資産となる。
        total_eq = jpy_balance + liquidation_value
        return total_eq, all_bids_valid

    def evaluate_l5_hard_kill_switch(
        self,
        portfolio: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        L5 ハード・キルスイッチ判定 (パターンA High-Water Mark 連動):
        - 過去最高総資産 `peak_equity_jpy` を追跡し、`threshold = max(init_cap * 0.85 [既定17,000円], peak_equity_jpy * 0.85)` を動的算出する。
        - `Best Bid * (1 - Taker Fee)` による保守的総資産が `< threshold` となった回数をカウントし、
          有効な Best Bid のもとで **3回連続** 下回ったときに `l5_kill_switch_latched = True` を発動する。
        - 単発のヒゲ（1〜2回で回復）や Best Bid 欠損時はカウンタをリセットまたはスキップし、誤爆を防ぐ。
        """
        conservative_eq, bids_valid = self.calculate_conservative_equity_jpy(portfolio, tickers)
        self.l5_last_conservative_equity_jpy = conservative_eq

        raw_init_cap = portfolio.get("initial_capital_jpy", portfolio.get("initial_equity_jpy"))
        raw_ks = portfolio.get("drawdown_kill_switch_jpy")
        if raw_init_cap is not None:
            init_cap = _to_decimal(raw_init_cap)
        elif raw_ks is not None and _to_decimal(raw_ks) > Decimal("0"):
            init_cap = (_to_decimal(raw_ks) / Decimal("0.85")).quantize(Decimal("1"))
        else:
            init_cap = Decimal("20000")
        if init_cap <= Decimal("0"):
            init_cap = Decimal("20000")
        prev_peak = max(_to_decimal(portfolio.get("peak_equity_jpy", 0)), init_cap)
        has_active_positions = any(
            str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
            and _to_decimal(p.get("size", 0)) > Decimal("0")
            for p in (portfolio.get("positions", []) or [])
        )

        peak_updated = False
        if not has_active_positions:
            # ポジションフラット時（確定現金100%）はティッカーノイズの影響を受けないため即時HWM反映
            self.hwm_candidate_readings.clear()
            if bids_valid and conservative_eq > prev_peak:
                peak_equity = conservative_eq
                peak_updated = True
            else:
                peak_equity = prev_peak
        else:
            # 保有建玉あり（含み益評価込み）の場合は、単発価格スパイクによるHWM汚染を防ぐため
            # 正常なBest Bidのもとで 3回連続 prev_peak を上回った場合の最小値でHWMを更新する
            if bids_valid and conservative_eq > prev_peak:
                self.hwm_candidate_readings.append(conservative_eq)
                if len(self.hwm_candidate_readings) > 3:
                    self.hwm_candidate_readings = self.hwm_candidate_readings[-3:]
                if len(self.hwm_candidate_readings) >= 3:
                    confirmed_peak = min(self.hwm_candidate_readings)
                    if confirmed_peak > prev_peak:
                        peak_equity = confirmed_peak
                        peak_updated = True
                    else:
                        peak_equity = prev_peak
                else:
                    peak_equity = prev_peak
            else:
                self.hwm_candidate_readings.clear()
                peak_equity = prev_peak

        peak_val_num = int(peak_equity) if peak_equity == peak_equity.to_integral() else float(round(peak_equity, 2))
        if portfolio.get("peak_equity_jpy") != peak_val_num:
            portfolio["peak_equity_jpy"] = peak_val_num
            peak_updated = True

        init_cap_floor = (init_cap * Decimal("0.85")).quantize(Decimal("0.01"))
        hwm_threshold = (peak_equity * Decimal("0.85")).quantize(Decimal("0.01"))
        if init_cap >= Decimal("20000"):
            base_l5_floor = max(self.l5_kill_switch_equity_jpy, init_cap_floor)
        else:
            base_l5_floor = max(Decimal("8500"), init_cap_floor)
        threshold = max(base_l5_floor, hwm_threshold)
        thresh_val_num = int(threshold) if threshold == threshold.to_integral() else float(round(threshold, 2))
        if portfolio.get("drawdown_kill_switch_jpy") != thresh_val_num:
            portfolio["drawdown_kill_switch_jpy"] = thresh_val_num
            peak_updated = True

        if not bids_valid:
            # API欠損（Best Bid <= 0）時はカウントを進めない
            return {
                "level": "L5",
                "latched": self.l5_kill_switch_latched,
                "newly_triggered": False,
                "conservative_equity_jpy": float(conservative_eq),
                "peak_equity_jpy": float(peak_equity),
                "peak_updated": peak_updated,
                "threshold_jpy": float(threshold),
                "consecutive_breaches": self.l5_consecutive_breach_count,
                "bids_valid": False,
            }

        newly_triggered = False
        if conservative_eq < threshold:
            self.l5_consecutive_breach_count += 1
            if (
                self.l5_consecutive_breach_count >= self.l5_required_consecutive_breaches
                and not self.l5_kill_switch_latched
            ):
                self.l5_kill_switch_latched = True
                newly_triggered = True
        else:
            self.l5_consecutive_breach_count = 0

        return {
            "level": "L5",
            "latched": self.l5_kill_switch_latched,
            "newly_triggered": newly_triggered,
            "conservative_equity_jpy": float(conservative_eq),
            "peak_equity_jpy": float(peak_equity),
            "peak_updated": peak_updated,
            "threshold_jpy": float(threshold),
            "consecutive_breaches": self.l5_consecutive_breach_count,
            "bids_valid": True,
        }

    def evaluate_anti_martingale_risk(
        self,
        portfolio: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        テーマ5-(2): HWMからのドローダウン率 DD_t に応じた連続的アンチ・マーチンゲール 1R リスク率
        および月間利益 +6.0% 超 & DD_t < 2.0% 時の非対称利益クッション拡大 (最大 1.85%) を算出する。
        """
        conservative_eq, _ = self.calculate_conservative_equity_jpy(portfolio, tickers)
        return compute_anti_martingale_risk_params(portfolio=portfolio, total_equity=conservative_eq)

    def evaluate_streak_ladder(
        self,
        portfolio_or_streak: Any,
        base_slot_cap_jpy: float = 9_000.0,
    ) -> Dict[str, Any]:
        """連敗段階ガード (Losing Streak Ladder: 5連敗Yellow, 6連敗Orange 4500円, 7連敗Red Shadow, 8連敗Halt) を判定する"""
        if isinstance(portfolio_or_streak, dict):
            explicit_st = portfolio_or_streak.get("consecutive_losses")
            if explicit_st is not None:
                streak = int(explicit_st)
            else:
                streak = 0
                for tr in reversed(portfolio_or_streak.get("closed_trades", []) or []):
                    pnl = _to_decimal(tr.get("realized_pnl_jpy", tr.get("pnl_jpy", 0)))
                    if pnl < Decimal("0"):
                        streak += 1
                    else:
                        break
        else:
            streak = int(portfolio_or_streak or 0)
        self.consecutive_losses = streak
        return evaluate_losing_streak_ladder(streak, base_slot_cap_jpy=base_slot_cap_jpy)

    def record_closed_trade_sprt_and_cusum(
        self,
        symbol: str,
        net_pnl_jpy: float,
        r_multiple: Optional[float] = None,
        weight: float = 1.0,
        is_shadow: bool = False,
        db_wal: Optional[Any] = None,
        portfolio: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        トレード決済ごとに Wald's SPRT 対数尤度比 LLR_n と Page's Lower CUSUM (全体・銘柄別 S_i^-) を更新し、
        銘柄別 S_i^- > 4.0 到達時は当該銘柄を自動的に動的ブラックリストへ隔離する。
        """
        if is_shadow and weight == 1.0:
            weight = SHADOW_DISCOUNT_WEIGHT
        sym_up = str(symbol or "NONE").strip().upper()
        pnl_val = float(net_pnl_jpy or 0.0)
        is_win = pnl_val > 0.0

        if is_win:
            self.sprt_wins_since_reset += 1
        else:
            self.sprt_losses_since_reset += 1

        sprt_res = compute_wald_sprt_step(prev_llr=self.sprt_llr, is_win=is_win, weight=weight)
        self.sprt_llr = float(sprt_res["llr"])
        if sprt_res["healthy_reset_triggered"]:
            self.sprt_wins_since_reset = 0
            self.sprt_losses_since_reset = 0
            self.sprt_alarm_active = False
        elif sprt_res["sprt_alarm"]:
            self.sprt_alarm_active = True

        pf_cusum_res = compute_pages_lower_cusum_step(
            prev_s_minus=self.portfolio_cusum_s_minus,
            net_pnl_jpy=pnl_val,
            r_multiple=r_multiple,
        )
        self.portfolio_cusum_s_minus = float(pf_cusum_res["s_minus"])

        sym_prev_s = float(self.symbol_cusum_s_minus.get(sym_up, 0.0) or 0.0)
        sym_cusum_res = compute_pages_lower_cusum_step(
            prev_s_minus=sym_prev_s,
            net_pnl_jpy=pnl_val,
            r_multiple=r_multiple,
        )
        self.symbol_cusum_s_minus[sym_up] = float(sym_cusum_res["s_minus"])

        newly_quarantined = False
        if sym_cusum_res["alarm_triggered"] and sym_up not in ("", "NONE"):
            if sym_up not in self.dynamic_blacklisted_symbols:
                newly_quarantined = True
            self.dynamic_blacklisted_symbols.add(sym_up)

        state_snapshot = {
            "sprt_llr": round(self.sprt_llr, 4),
            "sprt_wins_since_reset": self.sprt_wins_since_reset,
            "sprt_losses_since_reset": self.sprt_losses_since_reset,
            "sprt_status": sprt_res["status"],
            "sprt_alarm": self.sprt_alarm_active,
            "sprt_recommended_action": sprt_res["recommended_action"],
            "portfolio_cusum_s_minus": round(self.portfolio_cusum_s_minus, 4),
            "portfolio_cusum_alarm": bool(pf_cusum_res["alarm_triggered"]),
            "symbol": sym_up,
            "symbol_r_multiple": sym_cusum_res["r_multiple"],
            "symbol_cusum_s_minus": round(self.symbol_cusum_s_minus[sym_up], 4),
            "symbol_cusum_alarm": bool(sym_cusum_res["alarm_triggered"]),
            "symbol_cusum": sym_cusum_res,
            "portfolio_cusum": pf_cusum_res,
            "sprt": sprt_res,
            "symbol_newly_quarantined": newly_quarantined,
            "symbol_cusum_map": {k: round(v, 4) for k, v in sorted(self.symbol_cusum_s_minus.items())},
            "dynamic_blacklisted_symbols": sorted(self.dynamic_blacklisted_symbols),
        }

        if portfolio is not None and isinstance(portfolio, dict):
            existing_dyn = {
                str(s).strip().upper()
                for s in (portfolio.get("dynamic_blacklisted_symbols", []) or [])
                if s
            }
            portfolio["dynamic_blacklisted_symbols"] = sorted(existing_dyn | self.dynamic_blacklisted_symbols)
            portfolio["sprt_cusum_state"] = state_snapshot

        if db_wal is not None and hasattr(db_wal, "save_sprt_cusum_state"):
            try:
                db_wal.save_sprt_cusum_state(state_snapshot)
            except Exception as exc:
                logger.debug("save_sprt_cusum_state スキップ: %s", exc)

        return state_snapshot

    def load_sprt_cusum_from_wal(self, db_wal: Optional[Any]) -> Dict[str, Any]:
        """DatabaseWAL から永続化済みの SPRT & CUSUM 状態を復元する"""
        if db_wal is None or not hasattr(db_wal, "load_sprt_cusum_state"):
            return {}
        try:
            st = db_wal.load_sprt_cusum_state()
        except Exception:
            st = {}
        if not isinstance(st, dict) or not st:
            return {}
        self.sprt_llr = float(st.get("sprt_llr", 0.0) or 0.0)
        self.sprt_wins_since_reset = int(st.get("sprt_wins_since_reset", 0) or 0)
        self.sprt_losses_since_reset = int(st.get("sprt_losses_since_reset", 0) or 0)
        self.sprt_alarm_active = bool(st.get("sprt_alarm", False))
        self.portfolio_cusum_s_minus = float(st.get("portfolio_cusum_s_minus", 0.0) or 0.0)
        raw_map = st.get("symbol_cusum_map") or {}
        if isinstance(raw_map, dict):
            self.symbol_cusum_s_minus = {str(k).upper(): float(v or 0.0) for k, v in raw_map.items()}
        raw_bl = st.get("dynamic_blacklisted_symbols") or []
        if isinstance(raw_bl, list):
            self.dynamic_blacklisted_symbols = {str(x).strip().upper() for x in raw_bl if x}
        return st
