"""
マーケットレジーム（市場地合い）分類フィルタ＆2時間ヒステリシス冷却モジュール (regime_filter.py)

1. MarketRegimeFilter:
   - BTC の 1h / 4h / 1d トレンド・EMA配列・騰落率・RSI・ADX(14)・ボラティリティパーセンタイル、
     およびスクリーニング対象銘柄の Breadth（上昇銘柄比率：1h または 4h/日足移動平均上回り比率）から
     以下のレジームに分類する：
     * `TREND_UP`: 上昇トレンド（順張り押し目買い推奨）
     * `RANGE`: レンジ・中立相場（深い押し目のみ厳選）
     * `RISK_OFF_CRASH`: 急落・全面安リスクオフ（新規 `BUY` 完全凍結 ＆ 未約定 `PENDING_BUY` 即時全キャンセル）

2. `RISK_OFF_CRASH` 発動条件と 2時間ヒステリシス（冷却期間）:
   - 発動条件（いずれか1つ以上に該当）:
     a) BTC 直近1時間騰落率 `<= -2.5%`
     b) BTC 直近4時間騰落率 `<= -4.0%`（または BTC 4h RSI `<= 22.0`）
     c) 市場 Breadth（スクリーニング銘柄のうち上昇トレンド/移動平均線上の銘柄比率）`< 25%`（対象4銘柄以上のとき）
   - ヒステリシス（チャタリング防止）:
     一度 `RISK_OFF_CRASH` に入った場合、トリガー数値が一時的に回復しても
     最低 **2時間（120分）** は `RISK_OFF_CRASH`（新規買い凍結）を維持する。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Deep Research #2: 全17銘柄の3ティア分類・5銘柄永久ブラックリスト定義
TIER1_SYMBOLS = {"BTC", "ETH", "XRP", "SOL"}
TIER2_SYMBOLS = {"DOGE", "SUI", "LTC", "BCH", "LINK", "ADA", "XLM", "DOT"}
BLACKLISTED_SYMBOLS = {"FCR", "NAC", "WILD", "ASTR", "ATOM"}

SOL_PRIORITY_SCORE_BONUS = 8.0
BCH_MAX_SPREAD_PCT = 0.22
TIER2_MAX_SELL_SLIPPAGE_PCT = 0.15
TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY = 45000.0


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def classify_symbol_tier(symbol: str) -> Dict[str, Any]:
    """
    銘柄を Tier 1 (主力4銘柄) / Tier 2 (条件付き8銘柄) / Tier 3 (永久除外ブラックリスト5銘柄) に分類する。
    `SOL` には Tier 1 板厚と Maker リベート (-0.03%) 兼備による優先スコアボーナス (+8.0pt) を付与する。
    """
    sym_up = str(symbol or "").strip().upper()
    if sym_up in BLACKLISTED_SYMBOLS:
        return {
            "symbol": sym_up,
            "tier": "TIER3_BLACKLISTED",
            "is_blacklisted": True,
            "is_tier1": False,
            "is_tier2": False,
            "priority_score_bonus": -999.0,
            "maker_rebate_pct": -0.03,
            "max_spread_pct": 0.0,
        }
    if sym_up in TIER1_SYMBOLS:
        is_sol = sym_up == "SOL"
        return {
            "symbol": sym_up,
            "tier": "TIER1",
            "is_blacklisted": False,
            "is_tier1": True,
            "is_tier2": False,
            "priority_score_bonus": SOL_PRIORITY_SCORE_BONUS if is_sol else 3.0,
            "maker_rebate_pct": -0.03 if is_sol else -0.01,
            "max_spread_pct": 0.35,
        }
    if sym_up in TIER2_SYMBOLS:
        return {
            "symbol": sym_up,
            "tier": "TIER2",
            "is_blacklisted": False,
            "is_tier1": False,
            "is_tier2": True,
            "priority_score_bonus": 0.0,
            "maker_rebate_pct": -0.03,
            "max_spread_pct": BCH_MAX_SPREAD_PCT if sym_up == "BCH" else 0.35,
        }
    return {
        "symbol": sym_up,
        "tier": "OTHER",
        "is_blacklisted": sym_up.endswith("_JPY"),
        "is_tier1": False,
        "is_tier2": False,
        "priority_score_bonus": 0.0,
        "maker_rebate_pct": -0.01,
        "max_spread_pct": 0.35,
    }


def evaluate_cme_monday_gap(
    market_summaries: Optional[Dict[str, Dict[str, Any]]] = None,
    symbol: str = "BTC",
    current_price: Optional[float] = None,
    cme_friday_close: Optional[float] = None,
    atr: Optional[float] = None,
) -> Dict[str, Any]:
    """
    月曜朝 06:30〜09:30 JST（CME先物オープン窓埋め時間帯）のギャップ評価:
    - 週末乖離幅 `gap_abs < 1.0 * ATR`（小ギャップ）: 窓埋め回帰のみ許可（上窓開け `current_price > friday_close` の高値追いロング禁止）
    - `gap_abs > 1.2 * ATR`（大幅ギャップ）: トレンドフォロー（順張りブレイク）許可
    """
    sym_up = str(symbol or "BTC").strip().upper()
    sym_sum = ((market_summaries or {}).get(sym_up) or (market_summaries or {}).get("BTC") or {})
    price_info = sym_sum.get("price", {}) or {}
    h1_info = sym_sum.get("h1_indicators", {}) or {}
    h4_info = sym_sum.get("h4_indicators", {}) or {}
    daily_info = sym_sum.get("daily_indicators", {}) or {}

    cur_p = float(
        current_price
        if current_price is not None
        else (price_info.get("last") or price_info.get("best_bid") or h1_info.get("close") or 0.0)
    )
    fri_c = float(
        cme_friday_close
        if cme_friday_close is not None
        else (
            sym_sum.get("cme_friday_close")
            or price_info.get("cme_friday_close")
            or price_info.get("friday_close")
            or h4_info.get("cme_friday_close")
            or daily_info.get("cme_friday_close")
            or daily_info.get("friday_close")
            or 0.0
        )
    )
    atr_val = float(
        atr
        if atr is not None
        else (
            sym_sum.get("cme_reference_atr")
            or h1_info.get("atr_14")
            or h4_info.get("atr_14")
            or daily_info.get("atr_14")
            or (cur_p * 0.015 if cur_p > 0 else 1.0)
        )
    )
    if atr_val <= 0:
        atr_val = max(1.0, cur_p * 0.015)

    if cur_p <= 0 or fri_c <= 0:
        return {
            "has_cme_gap_data": False,
            "current_price": cur_p,
            "cme_friday_close": fri_c,
            "atr": atr_val,
            "gap_signed": 0.0,
            "gap_atr_ratio": 0.0,
            "cme_gap_regime": "NO_GAP_DATA_DEFAULT_TIER1",
            "gap_regime": "NO_GAP_DATA_DEFAULT_TIER1",
            "allow_mean_reversion_only": False,
            "allow_trend_follow": True,
            "block_gap_up_long": False,
        }

    gap_signed = cur_p - fri_c
    gap_abs = abs(gap_signed)
    gap_atr_ratio = gap_abs / atr_val

    if gap_atr_ratio < 1.0:
        regime = "SMALL_GAP_MEAN_REVERSION_ONLY"
        allow_mr_only = True
        allow_tf = False
        # 小ギャップで上窓開け (cur_p > fri_c) の場合、下方向への窓埋め回帰が優位なため高値追いロングを禁止
        block_gap_up_long = bool(gap_signed > 0)
    elif gap_atr_ratio > 1.2:
        regime = "LARGE_GAP_TREND_FOLLOW_ALLOWED"
        allow_mr_only = False
        allow_tf = True
        block_gap_up_long = False
    else:
        regime = "MODERATE_GAP_SELECTIVE"
        allow_mr_only = True
        allow_tf = False
        block_gap_up_long = bool(gap_signed > 0)

    return {
        "has_cme_gap_data": True,
        "current_price": round(cur_p, 4),
        "cme_friday_close": round(fri_c, 4),
        "atr": round(atr_val, 4),
        "gap_signed": round(gap_signed, 4),
        "gap_atr_ratio": round(gap_atr_ratio, 4),
        "cme_gap_regime": regime,
        "gap_regime": regime,
        "allow_mean_reversion_only": allow_mr_only,
        "allow_trend_follow": allow_tf,
        "block_gap_up_long": block_gap_up_long,
    }


def evaluate_jst_session_and_schedule_matrix(
    now_dt: Optional[datetime] = None,
    market_summaries: Optional[Dict[str, Dict[str, Any]]] = None,
    exchange_status: Optional[str] = None,
    maintenance_open_confirmed_at: Optional[datetime] = None,
    post_maint_stabilization_minutes: int = 15,
) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装2】: JST時間帯別・曜日別パラメータ補正マトリクス ＆ 土曜メンテ・月曜朝CME窓開け制御
    1. 毎日 05:45〜06:15 JST (DAILY_ROLLOVER_BLACKOUT): 新規発注・指値変更完全停止、待機指値を 05:45 JST にクリア
    2. 平日 03:00〜05:45 JST (EARLY_MORNING_ILLIQUID): Tier 1のみ、spread<=0.20%、conf+0.08、offset 1.30x ATR、size 0.70x(最大1枠)、Mode 1禁止
    3. 平日 12:00〜15:00 JST (TOKYO_LUNCH_LULL): Tier 1のみ、spread<=0.22%、conf+0.05、offset 1.15x ATR、size 0.80x(最大1枠)、TTL 50%短縮
    4. 平日 15:00〜21:00 JST (EUROPE_ACTIVE) & 21:00〜03:00 JST (EU_US_OVERLAP_GOLDEN): Tier 1 + Tier 2 計12銘柄フル稼働 (21:00〜03:00は conf-0.02, offset 0.90x ATR)
    5. 土曜メンテナンス完全防護 (土曜 08:00〜11:25 JST / 動的延長対応):
       - 土曜 08:00 JST: 新規発注停止＆未約定指値クリア
       - 土曜 08:45 JST: 保有ポジション事前クローズ保護 (Pre-Maintenance Exit)
       - 土曜 09:00〜11:10 JST (および延長時): メンテナンススリープ
       - 再開 (OPEN) 確認後 15分間 (通常 11:25 JST まで / 延長時は open_confirmed_at + 15分): 板安定まで新規エントリー待機
    6. 週末 (土曜 11:25 〜 月曜 06:30 JST: WEEKEND_CME_CLOSED): Tier 1のみ、spread<=0.22%、conf+0.06、offset 1.40x ATR、size 0.75x(最大1枠)
    7. 月曜朝 06:30〜09:30 JST (MONDAY_MORNING_CME_OPEN): Tier 1のみ、size 0.80x(最大1枠)、CMEギャップ判定 (<1.0 ATR 窓埋めのみ / >1.2 ATR トレンドフォロー可)
    """
    now_jst = _now_jst(now_dt)
    wd = now_jst.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
    mins = now_jst.hour * 60 + now_jst.minute
    ex_stat = str(exchange_status or "OPEN").strip().upper()

    all_12_symbols = sorted(TIER1_SYMBOLS | TIER2_SYMBOLS)
    tier1_4_symbols = sorted(TIER1_SYMBOLS)

    # ------------------------------------------------------------------
    # ① 毎日 05:45〜06:15 JST: ロールオーバー・端数精算ブラックアウト
    # ------------------------------------------------------------------
    if (5 * 60 + 45) <= mins < (6 * 60 + 15):
        return {
            "session_name": "DAILY_ROLLOVER_BLACKOUT",
            "allow_new_buy": False,
            "allow_limit_modification": False,
            "should_clear_pending_buys": True,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": [],
            "allowed_symbols": [],
            "max_spread_pct": 0.0,
            "confidence_threshold_delta": 1.0,
            "limit_offset_atr_mult": 1.0,
            "size_mult": 0.0,
            "max_concurrent_slots": 0,
            "order_ttl_mult": 1.0,
            "allow_mode1_breakout_peg": False,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ⑤ 土曜メンテナンス完全防護 (土曜 08:00〜11:25 JST + 動的延長対応)
    # ------------------------------------------------------------------
    open_conf_jst = _now_jst(maintenance_open_confirmed_at) if maintenance_open_confirmed_at is not None else None
    in_dynamic_post_maint_wait = False
    stabilization_until_jst: Optional[datetime] = None
    if open_conf_jst is not None:
        stabilization_until_jst = open_conf_jst + timedelta(minutes=post_maint_stabilization_minutes)
        if open_conf_jst <= now_jst < stabilization_until_jst:
            in_dynamic_post_maint_wait = True

    is_sat_maint_window = (wd == 5 and (8 * 60) <= mins < (11 * 60 + 25))
    is_dynamic_maint_active = ex_stat in ("MAINTENANCE", "PREOPEN")
    if is_sat_maint_window or is_dynamic_maint_active or in_dynamic_post_maint_wait:
        if is_dynamic_maint_active:
            sub_phase = f"EXCHANGE_{ex_stat}_EXTENDED"
            should_clear = False
            should_exit = False
            in_maint = True
            post_wait = False
        elif in_dynamic_post_maint_wait:
            sub_phase = "POST_MAINTENANCE_STABILIZATION_WAIT"
            should_clear = False
            should_exit = False
            in_maint = False
            post_wait = True
        elif mins < (8 * 60 + 45):
            sub_phase = "PRE_MAINT_0800_ENTRY_STOP_AND_CLEAR"
            should_clear = True
            should_exit = False
            in_maint = False
            post_wait = False
        elif mins < (9 * 60):
            sub_phase = "PRE_MAINT_0845_POSITION_EXIT"
            should_clear = True
            should_exit = True
            in_maint = mins >= (8 * 60 + 55)
            post_wait = False
        elif mins < (11 * 60 + 10):
            sub_phase = "SATURDAY_SCHEDULED_MAINTENANCE"
            should_clear = False
            should_exit = False
            in_maint = True
            post_wait = False
        else:
            # 11:10 〜 11:25 JST: 再開確認後の15分間板安定待機
            sub_phase = "POST_MAINTENANCE_STABILIZATION_WAIT"
            should_clear = False
            should_exit = False
            in_maint = False
            post_wait = True
            if stabilization_until_jst is None:
                stabilization_until_jst = now_jst.replace(hour=11, minute=25, second=0, microsecond=0)

        return {
            "session_name": "SATURDAY_MAINTENANCE_GUARD",
            "sub_phase": sub_phase,
            "block_reason": sub_phase,
            "allow_new_buy": False,
            "allow_limit_modification": not in_maint,
            "should_clear_pending_buys": should_clear,
            "should_pre_maintenance_exit": should_exit,
            "should_exit_positions_pre_maint": should_exit,
            "in_exchange_maintenance": in_maint,
            "post_maintenance_stabilization_wait": post_wait,
            "stabilization_until": (
                stabilization_until_jst.isoformat(timespec="seconds") if stabilization_until_jst else None
            ),
            "allowed_tiers": [],
            "allowed_symbols": [],
            "max_spread_pct": 0.0,
            "confidence_threshold_delta": 1.0,
            "limit_offset_atr_mult": 1.40,
            "size_mult": 0.0,
            "max_concurrent_slots": 0,
            "order_ttl_mult": 1.0,
            "allow_mode1_breakout_peg": False,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ⑥ 週末 (土曜 11:25 JST 〜 月曜 06:30 JST: WEEKEND_CME_CLOSED)
    #    ※ただし月曜〜金曜の 03:00〜05:45 JST は早朝流動性空白 (②) として扱う
    # ------------------------------------------------------------------
    is_weekend_cme_closed = (
        (wd == 5 and mins >= (11 * 60 + 25))
        or (wd == 6)
        or (wd == 0 and (mins < (3 * 60) or (6 * 60 + 15) <= mins < (6 * 60 + 30)))
    )
    if is_weekend_cme_closed:
        return {
            "session_name": "WEEKEND_CME_CLOSED",
            "allow_new_buy": True,
            "allow_limit_modification": True,
            "should_clear_pending_buys": False,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": tier1_4_symbols,
            "max_spread_pct": 0.22,
            "confidence_threshold_delta": 0.06,
            "limit_offset_atr_mult": 1.40,
            "size_mult": 0.75,
            "max_concurrent_slots": 1,
            "order_ttl_mult": 1.0,
            "allow_mode1_breakout_peg": False,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ⑦ 月曜朝 06:30〜09:30 JST: CME先物オープン窓埋め時間帯
    # ------------------------------------------------------------------
    if wd == 0 and (6 * 60 + 30) <= mins < (9 * 60 + 30):
        cme_info = evaluate_cme_monday_gap(market_summaries=market_summaries)
        return {
            "session_name": "MONDAY_MORNING_CME_OPEN",
            "allow_new_buy": not bool(cme_info.get("block_gap_up_long", False)),
            "allow_limit_modification": True,
            "should_clear_pending_buys": False,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": tier1_4_symbols,
            "max_spread_pct": 0.25,
            "confidence_threshold_delta": 0.03,
            "limit_offset_atr_mult": 1.15 if cme_info.get("allow_mean_reversion_only") else 1.00,
            "size_mult": 0.80,
            "max_concurrent_slots": 1,
            "order_ttl_mult": 1.0,
            "allow_mode1_breakout_peg": bool(cme_info.get("allow_trend_follow", False)),
            "cme_gap_info": cme_info,
        }

    # ------------------------------------------------------------------
    # ② 平日 03:00〜05:45 JST: 早朝流動性空白 (EARLY_MORNING_ILLIQUID)
    # ------------------------------------------------------------------
    if (3 * 60) <= mins < (5 * 60 + 45):
        return {
            "session_name": "EARLY_MORNING_ILLIQUID",
            "allow_new_buy": True,
            "allow_limit_modification": True,
            "should_clear_pending_buys": False,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": tier1_4_symbols,
            "max_spread_pct": 0.20,
            "confidence_threshold_delta": 0.08,
            "limit_offset_atr_mult": 1.30,
            "size_mult": 0.70,
            "max_concurrent_slots": 1,
            "order_ttl_mult": 1.0,
            "allow_mode1_breakout_peg": False,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ③ 平日 12:00〜15:00 JST: 東京昼休み閑散 (TOKYO_LUNCH_LULL)
    # ------------------------------------------------------------------
    if (12 * 60) <= mins < (15 * 60):
        return {
            "session_name": "TOKYO_LUNCH_LULL",
            "allow_new_buy": True,
            "allow_limit_modification": True,
            "should_clear_pending_buys": False,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": ["TIER1"],
            "allowed_symbols": tier1_4_symbols,
            "max_spread_pct": 0.22,
            "confidence_threshold_delta": 0.05,
            "limit_offset_atr_mult": 1.15,
            "size_mult": 0.80,
            "max_concurrent_slots": 1,
            "order_ttl_mult": 0.50,
            "allow_mode1_breakout_peg": False,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ④-B 平日 21:00〜03:00 JST: 欧米重複ゴールデンタイム (EU_US_OVERLAP_GOLDEN)
    # ------------------------------------------------------------------
    if mins >= (21 * 60) or mins < (3 * 60):
        return {
            "session_name": "EU_US_OVERLAP_GOLDEN",
            "allow_new_buy": True,
            "allow_limit_modification": True,
            "should_clear_pending_buys": False,
            "should_pre_maintenance_exit": False,
            "in_exchange_maintenance": False,
            "post_maintenance_stabilization_wait": False,
            "allowed_tiers": ["TIER1", "TIER2"],
            "allowed_symbols": all_12_symbols,
            "max_spread_pct": 0.35,
            "confidence_threshold_delta": -0.02,
            "limit_offset_atr_mult": 0.90,
            "size_mult": 1.00,
            "max_concurrent_slots": 2,
            "order_ttl_mult": 1.00,
            "allow_mode1_breakout_peg": True,
            "cme_gap_info": None,
        }

    # ------------------------------------------------------------------
    # ④-A 平日 15:00〜21:00 JST (欧州参入 EUROPE_ACTIVE) および 06:15〜12:00 JST (東京午前 TOKYO_MORNING_ACTIVE)
    # ------------------------------------------------------------------
    session_label = "EUROPE_ACTIVE" if mins >= (15 * 60) else "TOKYO_MORNING_ACTIVE"
    return {
        "session_name": session_label,
        "allow_new_buy": True,
        "allow_limit_modification": True,
        "should_clear_pending_buys": False,
        "should_pre_maintenance_exit": False,
        "in_exchange_maintenance": False,
        "post_maintenance_stabilization_wait": False,
        "allowed_tiers": ["TIER1", "TIER2"],
        "allowed_symbols": all_12_symbols,
        "max_spread_pct": 0.35,
        "confidence_threshold_delta": 0.00,
        "limit_offset_atr_mult": 1.00,
        "size_mult": 1.00,
        "max_concurrent_slots": 2,
        "order_ttl_mult": 1.00,
        "allow_mode1_breakout_peg": True,
        "cme_gap_info": None,
    }


class RegimeName(str):
    """
    4状態レジーム (`BROAD_RISK_ON`, `BTC_DOMINANCE_DRAIN`, `NEUTRAL_SELECTIVE`, `RISK_OFF_CRASH`) と
    旧3分類 (`TREND_UP`, `RANGE`, `RISK_OFF_CRASH`) の完全後方互換性を持つ文字列クラス。
    """

    _ALIASES = {
        "BROAD_RISK_ON": {"BROAD_RISK_ON", "TREND_UP"},
        "BTC_DOMINANCE_DRAIN": {"BTC_DOMINANCE_DRAIN", "RANGE", "TREND_UP"},
        "NEUTRAL_SELECTIVE": {"NEUTRAL_SELECTIVE", "RANGE"},
        "RISK_OFF_CRASH": {"RISK_OFF_CRASH"},
    }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            s_self = super().__str__()
            s_other = str.__str__(other) if isinstance(other, RegimeName) else other
            if s_self == s_other:
                return True
            return s_other in self._ALIASES.get(s_self, {s_self})
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()


class MarketRegimeFilter:
    """
    市場全体の地合いを4状態マトリクス (`BROAD_RISK_ON` / `BTC_DOMINANCE_DRAIN` / `NEUTRAL_SELECTIVE` / `RISK_OFF_CRASH`)
    （および後方互換 `TREND_UP` / `RANGE` / `RISK_OFF_CRASH`）で判定し、
    `RISK_OFF_CRASH` 時は2時間のヒステリシス冷却期間・未約定買い指値の一括キャンセル・保有建玉Stage 4タイトニングを提供する。
    """

    MAJOR_REGIME_SYMBOLS = {"BTC", "ETH", "XRP", "SOL"}

    def __init__(
        self,
        btc_1h_crash_pct: float = -2.5,
        btc_4h_crash_pct: float = -4.0,
        btc_4h_crash_rsi: float = 22.0,
        min_breadth_ratio: float = 0.25,
        alt_drain_breadth_threshold: float = 0.40,
        hysteresis_hours: float = 2.0,
    ) -> None:
        self.btc_1h_crash_pct = float(btc_1h_crash_pct)
        self.btc_4h_crash_pct = float(btc_4h_crash_pct)
        self.btc_4h_crash_rsi = float(btc_4h_crash_rsi)
        self.min_breadth_ratio = float(min_breadth_ratio)
        self.alt_drain_breadth_threshold = float(alt_drain_breadth_threshold)
        self.hysteresis_hours = float(hysteresis_hours)

        self.current_regime: str = RegimeName("NEUTRAL_SELECTIVE")
        self.risk_off_triggered_at: Optional[datetime] = None
        self.risk_off_cooldown_until: Optional[datetime] = None
        self.last_reason: str = ""

    @property
    def is_risk_off(self) -> bool:
        return str(self.current_regime) == "RISK_OFF_CRASH"

    @property
    def is_broad_risk_on(self) -> bool:
        return str(self.current_regime) == "BROAD_RISK_ON"

    @property
    def is_btc_dominance_drain(self) -> bool:
        return str(self.current_regime) == "BTC_DOMINANCE_DRAIN"

    @property
    def is_neutral_selective(self) -> bool:
        return str(self.current_regime) == "NEUTRAL_SELECTIVE"

    def evaluate(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        市場サマリから現在のマーケットレジームを判定する。
        戻り値:
        ```python
        {
            "regime": "TREND_UP" | "RANGE" | "RISK_OFF_CRASH",
            "allow_new_buy": bool,
            "should_cancel_pending_buys": bool,
            "in_hysteresis_cooldown": bool,
            "cooldown_until": Optional[str],
            "btc_change_1h_pct": float,
            "btc_change_4h_pct": float,
            "btc_rsi_4h": float,
            "breadth_ratio": float,
            "reasons": List[str],
        }
        ```
        """
        now_jst = _now_jst(now_dt)
        reasons: List[str] = []
        jst_schedule = evaluate_jst_session_and_schedule_matrix(
            now_dt=now_jst,
            market_summaries=market_summaries,
        )

        btc_summary = (market_summaries or {}).get("BTC") or {}
        btc_daily = btc_summary.get("daily_indicators") or {}
        btc_h4 = btc_summary.get("h4_indicators") or {}
        btc_h1 = btc_summary.get("h1_indicators") or {}
        btc_price = btc_summary.get("price") or {}

        # BTC 1h 騰落率
        btc_1h_pct = float(
            btc_h1.get("change_1h_pct")
            or btc_h1.get("change_pct")
            or btc_price.get("change_1h_pct")
            or 0.0
        )
        if btc_1h_pct == 0.0 and btc_h1.get("open") and btc_h1.get("close"):
            try:
                o1 = float(btc_h1["open"])
                c1 = float(btc_h1["close"])
                if o1 > 0:
                    btc_1h_pct = (c1 - o1) / o1 * 100.0
            except (TypeError, ValueError):
                pass

        # BTC 4h 騰落率 & RSI
        btc_4h_pct = float(
            btc_h4.get("change_4h_pct")
            or btc_h4.get("confirmed_change_4h_pct")
            or btc_price.get("change_4h_pct")
            or 0.0
        )
        if btc_4h_pct == 0.0 and btc_h4.get("open") and btc_h4.get("close"):
            try:
                o4 = float(btc_h4["open"])
                c4 = float(btc_h4["close"])
                if o4 > 0:
                    btc_4h_pct = (c4 - o4) / o4 * 100.0
            except (TypeError, ValueError):
                pass

        btc_4h_rsi = float(btc_h4.get("rsi_14") or btc_h4.get("confirmed_rsi_14") or 50.0)

        # Breadth（スクリーニング銘柄のうち 1h EMA50/SMA25 または日足/4hトレンドが上向きの比率、およびBTC除くアルトBreadth）
        total_syms = 0
        bullish_syms = 0
        alt_total_syms = 0
        alt_bullish_syms = 0
        explicit_bearish_breadth_count = 0
        for sym, summary in (market_summaries or {}).items():
            if not isinstance(summary, dict):
                continue
            sym_up = str(sym).strip().upper()
            total_syms += 1
            is_alt = sym_up != "BTC"
            if is_alt:
                alt_total_syms += 1
            h1 = summary.get("h1_indicators") or {}
            h4 = summary.get("h4_indicators") or {}
            daily = summary.get("daily_indicators") or {}

            h1_close = float(h1.get("close", 0) or 0)
            h1_ma = float(h1.get("ema_50") or h1.get("sma_25") or 0)
            h4_close = float(h4.get("close", 0) or 0)
            h4_ma = float(h4.get("sma_25", 0) or 0)
            d_trend = str(daily.get("trend", ""))

            has_ma_or_trend = bool(
                (h1_close > 0 and h1_ma > 0)
                or (h4_close > 0 and h4_ma > 0)
                or d_trend
                or ("above_ema50" in summary)
            )
            if not has_ma_or_trend:
                # 情報未指定時は中立としてカウント
                is_above = True
            else:
                is_above = bool(
                    (h1_close > 0 and h1_ma > 0 and h1_close >= h1_ma)
                    or (h4_close > 0 and h4_ma > 0 and h4_close >= h4_ma)
                    or (d_trend in ("STRONG_UPTREND", "UPTREND"))
                    or (summary.get("above_ema50") is True)
                )
                if summary.get("above_ema50") is False:
                    explicit_bearish_breadth_count += 1

            if is_above:
                bullish_syms += 1
                if is_alt:
                    alt_bullish_syms += 1

        breadth_ratio = (bullish_syms / total_syms) if total_syms > 0 else 1.0
        alt_breadth_ratio = (alt_bullish_syms / alt_total_syms) if alt_total_syms > 0 else breadth_ratio

        # RISK_OFF_CRASH トリガー判定
        crash_triggered = False
        if btc_1h_pct <= self.btc_1h_crash_pct:
            crash_triggered = True
            reasons.append(f"BTC 1h急落 ({btc_1h_pct:+.2f}% <= {self.btc_1h_crash_pct:+.2f}%)")
        if btc_4h_pct <= self.btc_4h_crash_pct:
            crash_triggered = True
            reasons.append(f"BTC 4h急落 ({btc_4h_pct:+.2f}% <= {self.btc_4h_crash_pct:+.2f}%)")
        if btc_4h_rsi <= self.btc_4h_crash_rsi:
            crash_triggered = True
            reasons.append(f"BTC 4h RSIパニック圏 ({btc_4h_rsi:.1f} <= {self.btc_4h_crash_rsi:.1f})")
        btc_under_pressure = (
            btc_1h_pct <= -1.0
            or btc_4h_pct <= -1.5
            or btc_4h_rsi <= 38.0
            or explicit_bearish_breadth_count >= 3
        )
        if total_syms >= 4 and breadth_ratio < self.min_breadth_ratio and btc_under_pressure:
            crash_triggered = True
            reasons.append(
                f"市場Breadth悪化 ({bullish_syms}/{total_syms} = {breadth_ratio * 100:.1f}% < {self.min_breadth_ratio * 100:.0f}%)"
            )

        if crash_triggered:
            self.current_regime = RegimeName("RISK_OFF_CRASH")
            self.risk_off_triggered_at = now_jst
            self.risk_off_cooldown_until = now_jst + timedelta(hours=self.hysteresis_hours)
            self.last_reason = " / ".join(reasons)
            return {
                "regime": self.current_regime,
                "regime_4state": "RISK_OFF_CRASH",
                "legacy_regime": "RISK_OFF_CRASH",
                "is_risk_off": True,
                "allow_new_buy": False,
                "should_cancel_pending_buys": True,
                "tighten_trailing_to_stage4": True,
                "allowed_execution_modes": [],
                "base_1r_pct": 0.0,
                "allowed_symbol_filter": "NONE",
                "in_hysteresis_cooldown": False,
                "cooldown_until": self.risk_off_cooldown_until.isoformat(timespec="seconds"),
                "btc_change_1h_pct": round(btc_1h_pct, 2),
                "btc_change_4h_pct": round(btc_4h_pct, 2),
                "btc_rsi_4h": round(btc_4h_rsi, 2),
                "breadth_ratio": round(breadth_ratio, 4),
                "alt_breadth_ratio": round(alt_breadth_ratio, 4),
                "jst_schedule_matrix": jst_schedule,
                "session_name": jst_schedule["session_name"],
                "reasons": reasons,
            }

        # 2時間ヒステリシス（冷却期間）中かどうかチェック
        if self.risk_off_cooldown_until is not None:
            cd_until_jst = _now_jst(self.risk_off_cooldown_until)
            self.risk_off_cooldown_until = cd_until_jst
            if now_jst < cd_until_jst:
                remaining_min = (cd_until_jst - now_jst).total_seconds() / 60.0
                hyst_reason = (
                    f"RISK_OFF_CRASH 冷却ヒステリシス継続中 "
                    f"(残り {remaining_min:.0f}分 / 解除予定 {cd_until_jst.isoformat(timespec='seconds')})"
                )
                self.current_regime = RegimeName("RISK_OFF_CRASH")
                return {
                    "regime": self.current_regime,
                    "regime_4state": "RISK_OFF_CRASH",
                    "legacy_regime": "RISK_OFF_CRASH",
                    "is_risk_off": True,
                    "allow_new_buy": False,
                    "should_cancel_pending_buys": True,
                    "tighten_trailing_to_stage4": True,
                    "allowed_execution_modes": [],
                    "base_1r_pct": 0.0,
                    "allowed_symbol_filter": "NONE",
                    "in_hysteresis_cooldown": True,
                    "cooldown_until": cd_until_jst.isoformat(timespec="seconds"),
                    "btc_change_1h_pct": round(btc_1h_pct, 2),
                    "btc_change_4h_pct": round(btc_4h_pct, 2),
                    "btc_rsi_4h": round(btc_4h_rsi, 2),
                    "breadth_ratio": round(breadth_ratio, 4),
                    "alt_breadth_ratio": round(alt_breadth_ratio, 4),
                    "jst_schedule_matrix": jst_schedule,
                    "session_name": jst_schedule["session_name"],
                    "reasons": [hyst_reason],
                }

        # 冷却期間経過後は解除 -> 残り3状態 (BROAD_RISK_ON / BTC_DOMINANCE_DRAIN / NEUTRAL_SELECTIVE) を判定
        self.risk_off_cooldown_until = None
        btc_trend = str(btc_daily.get("trend", ""))
        btc_bullish = (
            btc_trend in ("STRONG_UPTREND", "UPTREND")
            or btc_1h_pct >= 0.50
            or btc_4h_pct >= 1.00
        )

        if btc_bullish and alt_total_syms >= 1 and alt_breadth_ratio < self.alt_drain_breadth_threshold:
            # BTC_DOMINANCE_DRAIN: BTCは強いがアルトBreadth < 40% の独歩高・アルト停滞局面
            self.current_regime = RegimeName("BTC_DOMINANCE_DRAIN")
            legacy_regime = "RANGE"
            allowed_modes = ["MOMENTUM_PULLBACK_DYNAMIC"]
            base_1r = 0.0135
            sym_filter = "MAJORS_OR_HIGH_TFI"
            reasons.append(
                f"BTC独歩高・アルト停滞 (BTC={btc_trend or 'BULL'}, アルトBreadth={alt_breadth_ratio * 100:.0f}% < {self.alt_drain_breadth_threshold * 100:.0f}%)"
            )
        elif btc_trend in ("STRONG_UPTREND", "UPTREND") and breadth_ratio >= 0.50:
            # BROAD_RISK_ON: 全面強気・アルト循環
            self.current_regime = RegimeName("BROAD_RISK_ON")
            legacy_regime = "TREND_UP"
            allowed_modes = ["AGGRESSIVE_BREAKOUT_PEG", "MOMENTUM_PULLBACK_DYNAMIC"]
            base_1r = 0.015
            sym_filter = "ALL_17"
            reasons.append(f"全面強気・アルト循環 ({btc_trend}) ＆ Breadth良好 ({breadth_ratio * 100:.0f}%)")
        else:
            # NEUTRAL_SELECTIVE: 中立・個別材料相場 (BKSRスクイーズ解放やCVD吸収が確認された厳選銘柄のみ)
            self.current_regime = RegimeName("NEUTRAL_SELECTIVE")
            legacy_regime = "RANGE"
            allowed_modes = ["MOMENTUM_PULLBACK_DYNAMIC", "DEEP_LIQUIDITY_DIP"]
            base_1r = 0.012
            sym_filter = "SQUEEZE_OR_ABSORPTION_ONLY"
            reasons.append(f"中立・個別材料選別相場 (Breadth={breadth_ratio * 100:.0f}%)")

        if not jst_schedule.get("allow_mode1_breakout_peg", True):
            allowed_modes = [m for m in allowed_modes if m != "AGGRESSIVE_BREAKOUT_PEG"]
            if not allowed_modes:
                allowed_modes = ["MOMENTUM_PULLBACK_DYNAMIC"]

        allow_buy_final = bool(jst_schedule.get("allow_new_buy", True))
        cancel_pending_final = bool(jst_schedule.get("should_clear_pending_buys", False))
        if not allow_buy_final:
            reasons.append(f"JST時間帯制御 ({jst_schedule['session_name']}): 新規BUY停止")

        return {
            "regime": self.current_regime,
            "regime_4state": str(self.current_regime),
            "legacy_regime": legacy_regime,
            "is_risk_off": False,
            "allow_new_buy": allow_buy_final,
            "should_cancel_pending_buys": cancel_pending_final,
            "tighten_trailing_to_stage4": False,
            "allowed_execution_modes": allowed_modes,
            "base_1r_pct": base_1r,
            "allowed_symbol_filter": sym_filter,
            "in_hysteresis_cooldown": False,
            "cooldown_until": None,
            "btc_change_1h_pct": round(btc_1h_pct, 2),
            "btc_change_4h_pct": round(btc_4h_pct, 2),
            "btc_rsi_4h": round(btc_4h_rsi, 2),
            "breadth_ratio": round(breadth_ratio, 4),
            "alt_breadth_ratio": round(alt_breadth_ratio, 4),
            "jst_schedule_matrix": jst_schedule,
            "session_name": jst_schedule["session_name"],
            "reasons": reasons,
        }
