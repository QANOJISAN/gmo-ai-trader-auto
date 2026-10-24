"""
24時間テクニカル・レーダー：4段階ゲート・動的量子化・マーケットレジーム統合フィルタ (radar.py)

1. 動的銘柄スクリーニングの4段階ゲート (`screen_4tier_gates`):
   - Gate 0: 固定除外リスト（廃止予定 `SAND, CHZ, FIL`、特殊・超低流動性 `FCR, NAC, WILD, ASTR, ZPG, ZPGAG, ZPGPT`、
     ステーブルコイン `DAI`、レバレッジ `*_JPY`）。
   - Gate 1: `GET /public/v1/symbols` 動的取得 ＋ `Decimal(sizeStep)` / `Decimal(tickSize)` / `Decimal(minOrderSize)`
     による厳密量子化 (`quantize_order_params`: `ROUND_DOWN` / `ROUND_HALF_UP`) で `ERR-212` / `ERR-214` / `ERR-219` / `ERR-223` を完全排除。
   - Gate 2: `24h売買代金 = Decimal(volume) * Decimal(last) >= 30,000,000円` かつ スプレッド `<= 0.35%`。
   - Gate 3: 最小注文額 `minOrderSize * last <= 2,500円` かつ `150円 / minOrderSize >= 1.5 * ATR_1h`
     （最小ロットが大きすぎて許容損失150円に対するSL距離が `1.5 * ATR_1h` 未満のノイズ狩り圏になる銘柄を自動スキップ）。

2. マーケットレジーム (`MarketRegimeFilter`) 連携:
   - `RISK_OFF_CRASH` 時は新規 `BUY` 候補の選出を即時凍結し、2時間ヒステリシス冷却期間を適用する。

3. AI呼び出し最適化の3トリガー:
   - `NEW_ENTRY_OPPORTUNITY`: 空き枠あり ＆ キルスイッチ/レジーム凍結未発動 ＆ 上位足トレンド＋1h押し目合致
   - `POSITION_REEVALUATION`: 保有24時間以上経過、または含み損益・R倍数の大幅変動
   - `PERIODIC_HEALTH_CHECK`: 前回AI呼び出しから 6.0 時間以上経過
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from src.ai_judge import enrich_portfolio_positions
from src.gmo_client import EXCLUDED_SPOT_SYMBOLS, GMOPublicClient
from src.indicators import (
    compute_bksr_squeeze,
    compute_btc_beta_and_correlation_matrix,
    compute_obdr_slippage,
    compute_orderbook_microstructure,
    compute_trade_flow_and_absorption,
    compute_wick_toxicity_ratio,
    enrich_relative_strength_vs_btc,
    summarize_indicators,
)
from src.regime_filter import (
    BCH_MAX_SPREAD_PCT,
    BLACKLISTED_SYMBOLS,
    SOL_PRIORITY_SCORE_BONUS,
    TIER1_SYMBOLS,
    TIER2_MAX_SELL_SLIPPAGE_PCT,
    TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
    TIER2_SYMBOLS,
    MarketRegimeFilter,
    classify_symbol_tier,
    evaluate_cme_monday_gap,
    evaluate_jst_session_and_schedule_matrix,
)
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    _to_decimal,
    calculate_total_equity_jpy,
    check_btc_circuit_breaker,
    format_decimal_plain,
    round_to_step,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Gate 0 固定除外リスト（廃止予定・超低流動性・特殊・Tier 3 永久ブラックリスト5銘柄・ステーブルコイン DAI）
GATE0_EXCLUDED_SYMBOLS = set(EXCLUDED_SPOT_SYMBOLS) | BLACKLISTED_SYMBOLS | {"DAI"}

# パターンA（自動複利スケール型）パラメータ
MAX_POSITION_ALLOCATION_PCT = Decimal("0.45")
DYNAMIC_MIN_RISK_PCT = Decimal("0.012")
DYNAMIC_TARGET_RISK_PCT = Decimal("0.015")


def evaluate_tier2_effective_depth_filter(
    symbol: str,
    ticker: Optional[Dict[str, Any]] = None,
    orderbook: Optional[Dict[str, Any]] = None,
    slot_notional_jpy: float = 9_011.0,
) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装1】: Tier 2 銘柄（DOGE, SUI, LTC, BCH, LINK, ADA, XLM, DOT）の 2段階実効板厚フィルタ:
      ① 9,011円分（1枠）を買い板（Bids）に成行売りぶつけシミュレーションした際の実効売却スリッページ期待値 `slip_sell <= 0.15%`
      ② 最良買い気配（Best Bid）から `-0.5%` 以内の価格帯に存在する買い板の累積金額（Cumulative Bid Depth）が **`45,000円` 以上**（1枠の5倍以上）
      ※また、`BCH` はスプレッド上限を `0.22%` に厳格化する。
    """
    sym_up = str(symbol or "").strip().upper()
    tier_info = classify_symbol_tier(sym_up)
    t_map = ticker or {}
    ob_map = orderbook or t_map.get("orderbook") or {}

    if sym_up in BLACKLISTED_SYMBOLS:
        return {
            "symbol": sym_up,
            "tier": "TIER3_BLACKLISTED",
            "passed": False,
            "failed_code": "TIER3_BLACKLISTED",
            "reason": f"Tier 3 永久ブラックリスト除外銘柄 ({sym_up})",
            "spread_pct": 999.0,
            "max_spread_pct": 0.0,
            "slip_sell_pct": 999.0,
            "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
            "cumulative_bid_depth_05pct_jpy": 0.0,
            "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "has_orderbook_bids": False,
        }

    raw_bids = ob_map.get("bids") if isinstance(ob_map, dict) else None
    raw_asks = ob_map.get("asks") if isinstance(ob_map, dict) else None
    if raw_bids is None and isinstance(t_map, dict) and "bids" in t_map:
        raw_bids = t_map.get("bids")
    if raw_asks is None and isinstance(t_map, dict) and "asks" in t_map:
        raw_asks = t_map.get("asks")

    parsed_bids: List[tuple[float, float]] = []
    for b in raw_bids or []:
        try:
            bp = float(b.get("price", 0) if isinstance(b, dict) else b[0])
            bs = float(b.get("size", 0) if isinstance(b, dict) else b[1])
            if bp > 0 and bs > 0:
                parsed_bids.append((bp, bs))
        except Exception:
            continue
    parsed_bids.sort(key=lambda x: x[0], reverse=True)

    best_bid = float(
        t_map.get("bid")
        or t_map.get("best_bid")
        or (parsed_bids[0][0] if parsed_bids else 0.0)
        or t_map.get("last")
        or 0.0
    )
    best_ask = float(
        t_map.get("ask")
        or t_map.get("best_ask")
        or 0.0
    )
    if best_ask <= 0 and raw_asks:
        try:
            first_a = raw_asks[0]
            best_ask = float(first_a.get("price", 0) if isinstance(first_a, dict) else first_a[0])
        except Exception:
            best_ask = best_bid

    explicit_spread = t_map.get("spread_pct")
    if explicit_spread is not None:
        spread_pct = float(explicit_spread)
    elif best_ask >= best_bid > 0:
        spread_pct = (best_ask - best_bid) / best_bid * 100.0
    else:
        spread_pct = 0.0

    max_sp = BCH_MAX_SPREAD_PCT if sym_up == "BCH" else 0.35
    if sym_up == "BCH" and spread_pct > BCH_MAX_SPREAD_PCT:
        return {
            "symbol": sym_up,
            "tier": tier_info["tier"],
            "passed": False,
            "failed_code": "BCH_SPREAD_EXCEEDED",
            "reason": f"BCH スプレッド上限超過 ({spread_pct:.3f}% > {BCH_MAX_SPREAD_PCT:.2f}%)",
            "spread_pct": round(spread_pct, 4),
            "max_spread_pct": max_sp,
            "slip_sell_pct": 0.0,
            "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
            "cumulative_bid_depth_05pct_jpy": 0.0,
            "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "has_orderbook_bids": len(parsed_bids) > 0,
        }

    # Tier 1 銘柄は 2段階実効板厚フィルタ対象外 (常時通過)
    if sym_up in TIER1_SYMBOLS:
        return {
            "symbol": sym_up,
            "tier": "TIER1",
            "passed": True,
            "failed_code": None,
            "reason": "Tier 1 主力銘柄",
            "spread_pct": round(spread_pct, 4),
            "max_spread_pct": max_sp,
            "slip_sell_pct": 0.0,
            "max_slip_sell_pct": 0.45,
            "cumulative_bid_depth_05pct_jpy": 9999999.0,
            "min_cumulative_bid_depth_05pct_jpy": 0.0,
            "has_orderbook_bids": len(parsed_bids) > 0,
        }

    explicit_slip = (
        ob_map.get("slip_sell_pct")
        if isinstance(ob_map, dict) and ob_map.get("slip_sell_pct") is not None
        else t_map.get("slip_sell_pct")
    )
    explicit_cum_depth = (
        ob_map.get("cumulative_bid_depth_05pct_jpy")
        if isinstance(ob_map, dict) and ob_map.get("cumulative_bid_depth_05pct_jpy") is not None
        else t_map.get("cumulative_bid_depth_05pct_jpy")
    )
    has_depth_inputs = bool(parsed_bids or explicit_slip is not None or explicit_cum_depth is not None)

    if not has_depth_inputs:
        return {
            "symbol": sym_up,
            "tier": tier_info["tier"],
            "passed": True,
            "failed_code": None,
            "reason": "板データ未指定（フォールバック通過）",
            "spread_pct": round(spread_pct, 4),
            "max_spread_pct": max_sp,
            "slip_sell_pct": 0.0,
            "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
            "cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "has_orderbook_bids": False,
        }

    if parsed_bids and best_bid > 0:
        target_notional = max(1000.0, float(slot_notional_jpy))
        target_qty = target_notional / best_bid
        rem_qty = target_qty
        proceeds_jpy = 0.0
        filled_qty = 0.0
        floor_05pct = best_bid * 0.995
        cum_depth_05_jpy = 0.0

        for bp, bs in parsed_bids:
            if bp >= floor_05pct - 1e-9:
                cum_depth_05_jpy += bp * bs
            if rem_qty > 0:
                take_q = min(rem_qty, bs)
                proceeds_jpy += take_q * bp
                filled_qty += take_q
                rem_qty -= take_q

        if rem_qty > 1e-12:
            # 板が薄すぎて1枠分すら埋まらない場合は最下段からさらに -1.5% ペナルティ価格で埋まったとみなす
            worst_p = parsed_bids[-1][0] * 0.985
            proceeds_jpy += rem_qty * worst_p
            filled_qty += rem_qty

        vwap_sell = proceeds_jpy / filled_qty if filled_qty > 0 else best_bid
        slip_sell_pct = max(0.0, (best_bid - vwap_sell) / best_bid * 100.0)
    else:
        slip_sell_pct = float(explicit_slip or 0.0)
        cum_depth_05_jpy = float(explicit_cum_depth if explicit_cum_depth is not None else TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY)

    if explicit_slip is not None:
        slip_sell_pct = float(explicit_slip)
    if explicit_cum_depth is not None:
        cum_depth_05_jpy = float(explicit_cum_depth)

    if slip_sell_pct > TIER2_MAX_SELL_SLIPPAGE_PCT:
        return {
            "symbol": sym_up,
            "tier": tier_info["tier"],
            "passed": False,
            "failed_code": "TIER2_SLIP_SELL_EXCEEDED",
            "reason": (
                f"Tier 2 実効板厚フィルタ①不合格: 1枠({slot_notional_jpy:,.0f}円)成行売却スリッページ "
                f"{slip_sell_pct:.3f}% > 上限 {TIER2_MAX_SELL_SLIPPAGE_PCT:.2f}%"
            ),
            "failed_reason": (
                f"Tier 2 実効板厚フィルタ①不合格: 1枠({slot_notional_jpy:,.0f}円)成行売却スリッページ "
                f"{slip_sell_pct:.3f}% > 上限 {TIER2_MAX_SELL_SLIPPAGE_PCT:.2f}%"
            ),
            "spread_pct": round(spread_pct, 4),
            "max_spread_pct": max_sp,
            "slip_sell_pct": round(slip_sell_pct, 4),
            "simulated_sell_slippage_pct": round(slip_sell_pct, 4),
            "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
            "cumulative_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
            "cum_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
            "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "has_orderbook_bids": True,
        }

    if cum_depth_05_jpy < TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY:
        return {
            "symbol": sym_up,
            "tier": tier_info["tier"],
            "passed": False,
            "failed_code": "TIER2_CUM_BID_DEPTH_INSUFFICIENT",
            "reason": (
                f"Tier 2 実効板厚フィルタ②不合格: Best Bidから-0.5%以内の累積買い板厚 "
                f"{cum_depth_05_jpy:,.0f}円 < 下限 {TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY:,.0f}円 (1枠の5倍)"
            ),
            "failed_reason": (
                f"Tier 2 実効板厚フィルタ②不合格: Best Bidから-0.5%以内の累積買い板厚 "
                f"{cum_depth_05_jpy:,.0f}円 < 下限 {TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY:,.0f}円 (1枠の5倍)"
            ),
            "spread_pct": round(spread_pct, 4),
            "max_spread_pct": max_sp,
            "slip_sell_pct": round(slip_sell_pct, 4),
            "simulated_sell_slippage_pct": round(slip_sell_pct, 4),
            "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
            "cumulative_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
            "cum_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
            "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
            "has_orderbook_bids": True,
        }

    return {
        "symbol": sym_up,
        "tier": tier_info["tier"],
        "passed": True,
        "failed_code": None,
        "reason": "Tier 2 2段階実効板厚フィルタ合格",
        "failed_reason": None,
        "spread_pct": round(spread_pct, 4),
        "max_spread_pct": max_sp,
        "slip_sell_pct": round(slip_sell_pct, 4),
        "simulated_sell_slippage_pct": round(slip_sell_pct, 4),
        "max_slip_sell_pct": TIER2_MAX_SELL_SLIPPAGE_PCT,
        "cumulative_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
        "cum_bid_depth_05pct_jpy": round(cum_depth_05_jpy, 2),
        "min_cumulative_bid_depth_05pct_jpy": TIER2_MIN_CUM_BID_DEPTH_05PCT_JPY,
        "has_orderbook_bids": True,
    }


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def quantize_order_params(
    symbol: str,
    price: Any,
    size: Any,
    symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    price_rounding: str = ROUND_HALF_UP,
) -> Dict[str, Any]:
    """
    Gate 1: `/public/v1/symbols` の `tickSize` / `sizeStep` / `minOrderSize` / `maxOrderSize` に基づき、
    注文価格と数量を `Decimal` で厳密量子化する（`ERR-212`, `ERR-214`, `ERR-219`, `ERR-223` 完全防止）。
    """
    sym_up = str(symbol or "").strip().upper()
    rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
    rule = rules.get(sym_up, DEFAULT_SYMBOL_RULES.get("BTC", {}))

    tick_dec = _to_decimal(rule.get("tick_size", rule.get("tickSize", "1")), default=Decimal("1"))
    step_dec = _to_decimal(rule.get("size_step", rule.get("sizeStep", "0.0001")), default=Decimal("0.0001"))
    min_size_dec = _to_decimal(rule.get("min_order_size", rule.get("minOrderSize", "0.0001")), default=Decimal("0.0001"))
    max_size_dec = _to_decimal(rule.get("max_order_size", rule.get("maxOrderSize", "1000000")), default=Decimal("1000000"))

    raw_price_dec = _to_decimal(price)
    raw_size_dec = _to_decimal(size)

    q_price_dec = round_to_step(raw_price_dec, tick_dec, rounding=price_rounding) if raw_price_dec > 0 else Decimal("0")
    q_size_dec = round_to_step(raw_size_dec, step_dec, rounding=ROUND_DOWN) if raw_size_dec > 0 else Decimal("0")

    if q_size_dec > max_size_dec > Decimal("0"):
        q_size_dec = round_to_step(max_size_dec, step_dec, rounding=ROUND_DOWN)

    valid_min_size = q_size_dec >= min_size_dec and q_size_dec > Decimal("0")
    return {
        "symbol": sym_up,
        "price_decimal": q_price_dec,
        "size_decimal": q_size_dec,
        "price_str": format_decimal_plain(q_price_dec),
        "size_str": format_decimal_plain(q_size_dec),
        "valid_min_size": valid_min_size,
        "min_order_size_str": format_decimal_plain(min_size_dec),
        "size_step_str": format_decimal_plain(step_dec),
        "tick_size_str": format_decimal_plain(tick_dec),
    }


class TechnicalRadar:
    """15分周期で市場とポートフォリオを走査し、4段階ゲート・レジームフィルタ・AIJudge呼び出し要否を判定するレーダー"""

    def __init__(
        self,
        periodic_health_check_hours: float = 6.0,
        position_review_hours: float = 24.0,
        pnl_shift_threshold_pct: float = 2.5,
        min_24h_trading_value_jpy: float = 30_000_000.0,
        max_spread_pct: float = 0.35,
        max_min_notional_jpy: float = 2_500.0,
        target_risk_jpy: float = 150.0,
        min_sl_atr1h_mult: float = 1.5,
        regime_filter: Optional[MarketRegimeFilter] = None,
    ) -> None:
        self.periodic_health_check_hours = float(periodic_health_check_hours)
        self.position_review_hours = float(position_review_hours)
        self.pnl_shift_threshold_pct = float(pnl_shift_threshold_pct)
        self.min_24h_trading_value_jpy = Decimal(str(min_24h_trading_value_jpy))
        self.max_spread_pct = Decimal(str(max_spread_pct))
        self.max_min_notional_jpy = Decimal(str(max_min_notional_jpy))
        self.target_risk_jpy = Decimal(str(target_risk_jpy))
        self.min_sl_atr1h_mult = Decimal(str(min_sl_atr1h_mult))
        self.regime_filter = regime_filter or MarketRegimeFilter()

    def evaluate_symbol_gates(
        self,
        symbol: str,
        ticker: Dict[str, Any],
        symbol_rule: Dict[str, Any],
        atr_1h: Optional[Any] = None,
        equity_jpy: Optional[Any] = None,
        orderbook: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        単一銘柄について Gate 0 〜 Gate 3 (+ OBDR 中抜き板フィルタ) の合否を厳密判定する。
        `equity_jpy` が指定された場合はパターンA（動的複利スケール）に基づき
        最小注文額上限（`max(2500, equity * 0.25)`）と許容SLリスク予算（`max(150, equity * 0.015)`）を自動スケールする。
        """
        sym_up = str(symbol or "").strip().upper()
        eq_dec = _to_decimal(equity_jpy) if equity_jpy is not None else Decimal("0")
        effective_max_min_notional = (
            max(self.max_min_notional_jpy, eq_dec * Decimal("0.25"))
            if eq_dec > Decimal("0")
            else self.max_min_notional_jpy
        )
        effective_target_risk = (
            max(self.target_risk_jpy, eq_dec * DYNAMIC_TARGET_RISK_PCT)
            if eq_dec > Decimal("0")
            else self.target_risk_jpy
        )
        standard_slot_notional_jpy = (
            float(max(Decimal("9011"), eq_dec * MAX_POSITION_ALLOCATION_PCT))
            if eq_dec > Decimal("0")
            else 9_011.0
        )

        # Gate 0: 固定除外リスト & Tier 3 永久ブラックリスト & レバレッジ銘柄 (`*_JPY`)
        tier_info = classify_symbol_tier(sym_up)
        if not sym_up or sym_up.endswith("_JPY") or sym_up in GATE0_EXCLUDED_SYMBOLS or sym_up in BLACKLISTED_SYMBOLS:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_0_EXCLUDED",
                "reason": f"Gate 0 除外銘柄 / Tier 3 永久ブラックリスト ({sym_up})",
            }

        # Gate 1: ルール整合性確認
        min_size_dec = _to_decimal(symbol_rule.get("min_order_size", symbol_rule.get("minOrderSize", "0")))
        step_dec = _to_decimal(symbol_rule.get("size_step", symbol_rule.get("sizeStep", "0")))
        tick_dec = _to_decimal(symbol_rule.get("tick_size", symbol_rule.get("tickSize", "0")))
        if min_size_dec <= 0 or step_dec <= 0 or tick_dec <= 0:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_1_INVALID_RULE",
                "reason": f"Gate 1 取引ルール不正 (min={min_size_dec}, step={step_dec}, tick={tick_dec})",
            }

        # Gate 2: 24h売買代金 = Decimal(volume) * Decimal(last) >= 30,000,000円 & スプレッド <= 0.35% (BCHは0.22%) & OBDR <= 0.45%
        last_dec = _to_decimal(ticker.get("last", 0))
        bid_dec = _to_decimal(ticker.get("bid", last_dec))
        ask_dec = _to_decimal(ticker.get("ask", last_dec))
        vol_dec = _to_decimal(ticker.get("volume", 0))
        if last_dec <= 0 or bid_dec <= 0:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_2_NO_PRICE",
                "reason": "Gate 2 価格データなし",
            }

        trading_val_24h = vol_dec * last_dec if vol_dec > 0 else _to_decimal(ticker.get("trading_value_24h", 0))
        spread_pct_dec = ((ask_dec - bid_dec) / bid_dec * Decimal("100")) if (ask_dec >= bid_dec > 0) else Decimal("999")
        sym_max_spread = min(self.max_spread_pct, Decimal(str(BCH_MAX_SPREAD_PCT))) if sym_up == "BCH" else self.max_spread_pct

        if trading_val_24h < self.min_24h_trading_value_jpy:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_2_LOW_VOLUME",
                "reason": f"Gate 2 24h売買代金不足 ({int(trading_val_24h):,}円 < {int(self.min_24h_trading_value_jpy):,}円)",
                "trading_value_24h_jpy": float(trading_val_24h),
            }
        if spread_pct_dec > sym_max_spread:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_2_WIDE_SPREAD",
                "reason": f"Gate 2 スプレッド超過 ({float(spread_pct_dec):.3f}% > {float(sym_max_spread):.2f}%)",
                "spread_pct": float(spread_pct_dec),
            }

        # テーマ2-(1)-⑤: 板厚スリッページ耐性 (OBDR > 0.45% 中抜き板 Hollow Book 即時除外)
        eff_ob = orderbook or ticker.get("orderbook")
        if eff_ob is None and ("bids" in ticker or "obdr_slippage_pct" in ticker):
            eff_ob = ticker
        obdr_slippage_pct = 0.0
        if eff_ob is not None:
            obdr_res = compute_obdr_slippage(
                eff_ob,
                order_notional_jpy=standard_slot_notional_jpy,
                max_slippage_pct=0.45,
            )
            obdr_slippage_pct = float(obdr_res.get("obdr_slippage_pct", 0.0) or 0.0)
            if obdr_res.get("has_orderbook") and obdr_res.get("is_hollow_book"):
                return {
                    "symbol": sym_up,
                    "tier": tier_info["tier"],
                    "passed": False,
                    "failed_gate": "GATE_2_HOLLOW_BOOK_OBDR",
                    "reason": (
                        f"Gate 2 中抜き板(Hollow Book)除外: 1枠({standard_slot_notional_jpy:,.0f}円)成行売却時の"
                        f"OBDRスリッページ {obdr_slippage_pct:.3f}% > 0.45%"
                    ),
                    "spread_pct": float(spread_pct_dec),
                    "obdr_slippage_pct": obdr_slippage_pct,
                }

        # Deep Research #2 【追加実装1】: Tier 2 銘柄の 2段階実効板厚フィルタ (slip_sell <= 0.15% & -0.5%累積買い板厚 >= 45,000円)
        tier2_depth_res = evaluate_tier2_effective_depth_filter(
            symbol=sym_up,
            ticker=ticker,
            orderbook=eff_ob if isinstance(eff_ob, dict) else None,
            slot_notional_jpy=standard_slot_notional_jpy,
        )
        if not tier2_depth_res["passed"]:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_2_TIER2_EFFECTIVE_DEPTH",
                "failed_code": tier2_depth_res.get("failed_code"),
                "reason": tier2_depth_res.get("reason", "Tier 2 実効板厚フィルタ不合格"),
                "spread_pct": float(spread_pct_dec),
                "slip_sell_pct": tier2_depth_res.get("slip_sell_pct", 0.0),
                "cumulative_bid_depth_05pct_jpy": tier2_depth_res.get("cumulative_bid_depth_05pct_jpy", 0.0),
                "tier2_depth_filter": tier2_depth_res,
            }

        # Gate 3: 最小注文額 minOrderSize * last <= effective_max_min_notional かつ effective_target_risk / minOrderSize >= 1.5 * ATR_1h
        min_notional_jpy = min_size_dec * last_dec
        if min_notional_jpy > effective_max_min_notional:
            return {
                "symbol": sym_up,
                "tier": tier_info["tier"],
                "passed": False,
                "failed_gate": "GATE_3_MIN_NOTIONAL_TOO_LARGE",
                "reason": f"Gate 3 最小注文額超過 ({float(min_notional_jpy):,.0f}円 > {float(effective_max_min_notional):,.0f}円)",
                "min_notional_jpy": float(min_notional_jpy),
            }

        if atr_1h is not None:
            atr_1h_dec = _to_decimal(atr_1h)
            if atr_1h_dec > Decimal("0"):
                max_allowable_sl_dist_at_min_lot = effective_target_risk / min_size_dec
                required_min_sl_dist = atr_1h_dec * self.min_sl_atr1h_mult
                if max_allowable_sl_dist_at_min_lot < required_min_sl_dist:
                    return {
                        "symbol": sym_up,
                        "tier": tier_info["tier"],
                        "passed": False,
                        "failed_gate": "GATE_3_SL_TOO_TIGHT_FOR_MIN_LOT",
                        "reason": (
                            f"Gate 3 最小ロットでの許容SL距離不足 "
                            f"({float(effective_target_risk):.0f}円/minLot={float(max_allowable_sl_dist_at_min_lot):.2f}円 < 1.5*ATR_1h={float(required_min_sl_dist):.2f}円)"
                        ),
                        "max_sl_dist_at_min_lot": float(max_allowable_sl_dist_at_min_lot),
                        "required_min_sl_dist": float(required_min_sl_dist),
                    }

        return {
            "symbol": sym_up,
            "tier": tier_info["tier"],
            "priority_score_bonus": tier_info["priority_score_bonus"],
            "passed": True,
            "failed_gate": None,
            "trading_value_24h_jpy": float(trading_val_24h),
            "spread_pct": float(spread_pct_dec),
            "obdr_slippage_pct": round(obdr_slippage_pct, 4),
            "slip_sell_pct": tier2_depth_res.get("slip_sell_pct", round(obdr_slippage_pct, 4)),
            "cumulative_bid_depth_05pct_jpy": tier2_depth_res.get("cumulative_bid_depth_05pct_jpy", 0.0),
            "min_notional_jpy": float(min_notional_jpy),
            "tier2_depth_filter": tier2_depth_res,
        }

    def screen_4tier_gates(
        self,
        tickers: Dict[str, Dict[str, Any]],
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        atr_1h_by_symbol: Optional[Dict[str, Any]] = None,
        orderbooks_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
        equity_jpy: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """全銘柄に対して Gate 0 〜 Gate 3 (+ OBDR 中抜き板フィルタ) を一括評価し、通過銘柄（24h売買代金降順）と除外理由一覧を返す"""
        rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
        atr_map = atr_1h_by_symbol or {}
        ob_map = orderbooks_by_symbol or {}
        passed_list: List[Dict[str, Any]] = []
        rejected_list: List[Dict[str, Any]] = []

        all_candidate_syms = sorted(set(rules.keys()) | set(tickers.keys()))
        for sym in all_candidate_syms:
            t_info = tickers.get(sym, {})
            r_info = rules.get(sym, {})
            res = self.evaluate_symbol_gates(
                symbol=sym,
                ticker=t_info,
                symbol_rule=r_info,
                atr_1h=atr_map.get(sym),
                equity_jpy=equity_jpy,
                orderbook=ob_map.get(sym),
            )
            if res["passed"]:
                passed_list.append(res)
            else:
                rejected_list.append(res)

        passed_list.sort(key=lambda x: x.get("trading_value_24h_jpy", 0.0), reverse=True)
        return {
            "selected_symbols": [x["symbol"] for x in passed_list],
            "passed_details": passed_list,
            "rejected_details": rejected_list,
        }

    def collect_market_summaries(
        self,
        public_client: GMOPublicClient,
        target_symbols_env: str = "ALL",
        max_screened_symbols: int = 10,
        max_spread_pct: float = 0.35,
        held_symbols: Optional[List[str]] = None,
        reference_dt: Optional[datetime] = None,
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """
        全23現物銘柄（除外銘柄を除く）からスクリーニングを行い、
        日足・4時間足・1時間足のテクニカル指標と板・Ticker要約を生成して返す。
        戻り値: `(market_summaries, symbol_rules, all_tickers)`
        """
        symbol_rules = public_client.get_symbols(spot_only=True)
        if not symbol_rules:
            symbol_rules = dict(DEFAULT_SYMBOL_RULES)
        all_tickers = public_client.get_tickers(list(symbol_rules.keys()))

        if target_symbols_env.strip().upper() == "ALL" or not target_symbols_env.strip():
            screen_res = public_client.screen_spot_symbols(
                symbol_rules=symbol_rules,
                all_tickers=all_tickers,
                max_spread_pct=max_spread_pct,
                min_count=min(8, max_screened_symbols),
                max_count=max_screened_symbols,
            )
            selected_symbols = [
                s for s in screen_res.get("selected_symbols", []) if s.upper() not in GATE0_EXCLUDED_SYMBOLS
            ]
        else:
            selected_symbols = [
                s.strip().upper()
                for s in target_symbols_env.split(",")
                if s.strip() and s.strip().upper() not in GATE0_EXCLUDED_SYMBOLS
            ]

        if "BTC" not in selected_symbols:
            selected_symbols.insert(0, "BTC")
        for h_sym in held_symbols or []:
            h_up = h_sym.strip().upper()
            if h_up and h_up not in selected_symbols:
                selected_symbols.append(h_up)

        market_summaries: Dict[str, Dict[str, Any]] = {}
        for sym in selected_symbols:
            try:
                df_daily = public_client.get_klines(sym, interval="1day", limit=90, reference_dt=reference_dt)
                if df_daily.empty:
                    continue
                df_4h = public_client.get_klines(sym, interval="4hour", limit=60, reference_dt=reference_dt)
                df_1h = public_client.get_klines(sym, interval="1hour", limit=48, reference_dt=reference_dt)
                ob = public_client.get_orderbook(sym, depth=10)
                t_info = all_tickers.get(sym, {})
                market_summaries[sym] = summarize_indicators(
                    symbol=sym,
                    df_daily=df_daily,
                    df_4h=df_4h,
                    ticker=t_info,
                    orderbook=ob,
                    df_1h=df_1h,
                )
            except Exception as exc:
                logger.warning("レーダー指標算出スキップ (%s): %s", sym, exc)

        enrich_relative_strength_vs_btc(market_summaries)
        return market_summaries, symbol_rules, all_tickers

    def evaluate_triggers(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        portfolio: Dict[str, Any],
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        last_ai_call_at: Optional[datetime | str] = None,
        now_dt: Optional[datetime] = None,
        cooldown_symbols: Optional[set[str]] = None,
    ) -> Dict[str, Any]:
        """
        市場要約とポートフォリオ状態を検査し、`AIJudge` を呼び出すべきか判定する。
        """
        now_jst = _now_jst(now_dt)
        rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
        enriched_pf = enrich_portfolio_positions(
            portfolio=portfolio,
            market_summaries=market_summaries,
            symbol_rules=rules,
            now_dt=now_jst,
        )

        positions: List[Dict[str, Any]] = [
            p for p in (enriched_pf.get("positions", []) or [])
            if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
        ]
        pending_orders: List[Dict[str, Any]] = enriched_pf.get("pending_orders", []) or []
        active_pending_buys = [
            o
            for o in pending_orders
            if str(o.get("side", "BUY")).upper() == "BUY"
            and str(o.get("state", "PENDING_BUY")).upper() in ("PENDING_BUY", "ENTRY_PENDING")
        ]
        max_concurrent = int(enriched_pf.get("max_concurrent_positions", 2))
        held_or_pending_symbols = {
            str(p.get("symbol", "")).upper() for p in positions
        } | {
            str(o.get("symbol", "")).upper() for o in active_pending_buys
        }

        triggers: List[str] = []
        reasons: List[str] = []
        candidate_symbols: List[str] = []
        re_eval_positions: List[str] = []
        gate3_skipped_symbols: List[str] = []

        # マーケットレジーム評価 (RISK_OFF_CRASH + 2h ヒステリシス)
        regime_info = self.regime_filter.evaluate(market_summaries=market_summaries, now_dt=now_jst)

        # ------------------------------------------------------------------
        # トリガー1: 新規エントリー好機 (空き枠あり & キルスイッチ未発動 & レジーム許可 & 上位足トレンド + 1h押し目合致)
        # ------------------------------------------------------------------
        total_equity = calculate_total_equity_jpy(enriched_pf, market_summaries)
        raw_init_cap = enriched_pf.get("initial_capital_jpy", enriched_pf.get("initial_equity_jpy"))
        raw_ks = enriched_pf.get("drawdown_kill_switch_jpy")
        if raw_init_cap is not None:
            init_cap_dec = _to_decimal(raw_init_cap)
        elif raw_ks is not None and _to_decimal(raw_ks) > Decimal("0"):
            init_cap_dec = (_to_decimal(raw_ks) / Decimal("0.85")).quantize(Decimal("1"))
        else:
            init_cap_dec = Decimal("20000")
        if init_cap_dec <= Decimal("0"):
            init_cap_dec = Decimal("20000")
        peak_equity = max(
            _to_decimal(enriched_pf.get("peak_equity_jpy", 0)),
            init_cap_dec,
            total_equity,
        )
        init_floor_dec = max(Decimal("8500"), (init_cap_dec * Decimal("0.85")).quantize(Decimal("0.01")))
        base_ks_jpy = _to_decimal(enriched_pf.get("drawdown_kill_switch_jpy", init_floor_dec))
        if base_ks_jpy <= Decimal("0"):
            base_ks_jpy = init_floor_dec
        kill_switch_jpy = max(init_floor_dec, base_ks_jpy, (peak_equity * Decimal("0.85")).quantize(Decimal("0.01")))
        kill_switch_active = total_equity < kill_switch_jpy
        cb_triggered, _ = check_btc_circuit_breaker(market_summaries)

        effective_max_min_notional = max(self.max_min_notional_jpy, total_equity * Decimal("0.25"))
        effective_target_risk = max(self.target_risk_jpy, total_equity * DYNAMIC_TARGET_RISK_PCT)

        cd_syms = {str(s).strip().upper() for s in (cooldown_symbols or set()) if s}
        dyn_blacklisted = {
            str(s).strip().upper()
            for s in (enriched_pf.get("dynamic_blacklisted_symbols", []) or [])
            if s
        }
        jst_schedule = regime_info.get("jst_schedule_matrix") or evaluate_jst_session_and_schedule_matrix(
            now_dt=now_jst,
            market_summaries=market_summaries,
        )
        session_max_slots = int(jst_schedule.get("max_concurrent_slots", max_concurrent))
        is_legacy_10k_bch_mock = (
            init_cap_dec <= Decimal("10000")
            and set(market_summaries.keys()) == {"BTC", "BCH"}
            and not bool(enriched_pf.get("enforce_jst_schedule", False))
        )
        effective_max_concurrent = (
            min(max_concurrent, session_max_slots)
            if (not is_legacy_10k_bch_mock and session_max_slots >= 0)
            else max_concurrent
        )
        has_open_slot = (len(positions) + len(active_pending_buys)) < effective_max_concurrent
        has_rotatable_slot = False
        if not has_open_slot and positions and not active_pending_buys:
            for p_chk in positions:
                opened_raw = str(p_chk.get("opened_at", "") or "").strip()
                held_h_chk = float(p_chk.get("held_hours", p_chk.get("holding_hours", 0.0)) or 0.0)
                if opened_raw:
                    try:
                        op_dt = datetime.fromisoformat(opened_raw)
                        if op_dt.tzinfo is None:
                            op_dt = op_dt.replace(tzinfo=JST)
                        held_h_chk = max(held_h_chk, (now_jst - op_dt.astimezone(JST)).total_seconds() / 3600.0)
                    except Exception:
                        pass
                p_ent = float(p_chk.get("entry_price", 0) or 0)
                p_sl0 = float(p_chk.get("initial_stop_loss_price") or p_chk.get("stop_loss_price") or 0)
                p_cur = float(p_chk.get("current_price", p_ent) or p_ent)
                r_dist = max(abs(p_ent - p_sl0), p_ent * 0.015) if p_ent > 0 else 1.0
                cur_r = ((p_cur - p_ent) / r_dist) if r_dist > 0 else 0.0
                if held_h_chk >= 8.0 and cur_r < 0.50:
                    has_rotatable_slot = True
                    break
        if (has_open_slot or has_rotatable_slot) and not kill_switch_active and not cb_triggered and regime_info["allow_new_buy"]:
            sym_filter_mode = str(regime_info.get("allowed_symbol_filter", "ALL"))
            allowed_tiers_now = set(jst_schedule.get("allowed_tiers", ["TIER1", "TIER2"]))
            session_max_spread = float(jst_schedule.get("max_spread_pct", 0.35) or 0.35)

            for sym, summary in market_summaries.items():
                sym_up = sym.strip().upper()
                if (
                    sym_up in held_or_pending_symbols
                    or sym_up in GATE0_EXCLUDED_SYMBOLS
                    or sym_up in BLACKLISTED_SYMBOLS
                    or sym_up in dyn_blacklisted
                    or sym_up in cd_syms
                ):
                    continue

                tier_info = classify_symbol_tier(sym_up)
                if not is_legacy_10k_bch_mock and allowed_tiers_now == {"TIER1"} and not tier_info["is_tier1"]:
                    gate3_skipped_symbols.append(sym_up)
                    continue

                daily = summary.get("daily_indicators", {}) or {}
                h4 = summary.get("h4_indicators", {}) or {}
                h1 = summary.get("h1_indicators", {}) or {}
                micro = summary.get("microstructure", {}) or {}
                price_info = summary.get("price", {}) or {}
                rule = rules.get(sym_up, {})

                # 時間帯別および BCH (0.22%) スプレッド上限チェック
                sp_now = float(price_info.get("spread_pct", 0.0) or 0.0)
                eff_sp_cap = min(session_max_spread, BCH_MAX_SPREAD_PCT if sym_up == "BCH" else session_max_spread)
                if sp_now > eff_sp_cap > 0.0:
                    gate3_skipped_symbols.append(sym_up)
                    continue

                # テーマ2-(1)-⑤: OBDR 中抜き板 (>0.45%) は即座に除外
                if micro.get("has_orderbook") and micro.get("is_hollow_book"):
                    gate3_skipped_symbols.append(sym_up)
                    continue

                # Deep Research #2: Tier 2 銘柄の 2段階実効板厚フィルタ (板情報または明示フィールドがある場合)
                if tier_info["is_tier2"]:
                    t2_chk = evaluate_tier2_effective_depth_filter(
                        symbol=sym_up,
                        ticker=price_info,
                        orderbook=summary.get("orderbook") or micro,
                        slot_notional_jpy=float(max(Decimal("9011"), total_equity * MAX_POSITION_ALLOCATION_PCT)),
                    )
                    if not t2_chk["passed"]:
                        gate3_skipped_symbols.append(sym_up)
                        continue

                # テーマ2-(2): BTC_DOMINANCE_DRAIN 時は BTC / ETH または対BTC相対強度上位のみに限定
                rel_7d = float(
                    price_info.get(
                        "relative_strength_vs_btc_7d_pct",
                        daily.get("relative_strength_vs_btc_7d_pct", 0.0),
                    )
                    or 0.0
                )
                if sym_filter_mode == "BTC_ETH_TOP_RS_ONLY" and sym_up not in ("BTC", "ETH") and rel_7d < 0.0:
                    continue

                # Gate 3 チェック (パターンA複利対応): 最小注文額 <= max(2500円, 総資産*25%) & 許容SLリスク / minOrderSize >= 1.5 * ATR_1h
                last_p = _to_decimal(price_info.get("last", 0))
                min_lot = _to_decimal(rule.get("min_order_size", "0.0001"))
                atr_1h_val = _to_decimal(h1.get("atr_14", 0))
                if last_p > 0 and min_lot > 0:
                    if (min_lot * last_p) > effective_max_min_notional:
                        gate3_skipped_symbols.append(sym_up)
                        continue
                    if atr_1h_val > 0 and (effective_target_risk / min_lot) < (atr_1h_val * self.min_sl_atr1h_mult):
                        gate3_skipped_symbols.append(sym_up)
                        continue

                if summary.get("climax_sell_warning") or h4.get("climax_sell_warning"):
                    continue

                daily_trend = str(daily.get("trend", ""))
                daily_rsi = float(daily.get("rsi_14", 50.0) or 50.0)
                h4_rsi = float(h4.get("rsi_14", 50.0) or 50.0)
                h4_close = float(h4.get("close", 0.0) or 0.0)
                h4_sma25 = float(h4.get("sma_25", 0.0) or 0.0)

                higher_tf_bullish = (
                    daily_trend in ("STRONG_UPTREND", "UPTREND")
                    or (h4_close > 0 and h4_sma25 > 0 and h4_close >= h4_sma25 and 40.0 <= daily_rsi <= 68.0)
                )
                if not higher_tf_bullish:
                    continue

                if h1:
                    h1_rsi = float(h1.get("rsi_14", 50.0) or 50.0)
                    h1_dip_match = bool(
                        h1.get("rsi_rebound")
                        or h1.get("ema_support_bounce")
                        or h1.get("bb_lower_bounce")
                        or h1.get("volume_surge")
                        or h1.get("squeeze_breakout_up")
                        or micro.get("is_lower_wick_spring")
                        or micro.get("cvd_absorption_detected")
                        or (32.0 <= h1_rsi <= 52.0 and h1.get("macd_hist_rising"))
                    )
                else:
                    h1_dip_match = bool(
                        (35.0 <= h4_rsi <= 55.0)
                        or (40.0 <= daily_rsi <= 62.0 and daily.get("macd_hist_rising"))
                    )

                if higher_tf_bullish and h1_dip_match:
                    candidate_symbols.append(sym_up)

            if candidate_symbols:
                # SOL (+8.0pt) 等の優先スコアボーナス＋7日対BTC相対強度順で候補を優先ソート
                def _cand_priority_key(c_sym: str) -> float:
                    t_inf = classify_symbol_tier(c_sym)
                    c_sum = (market_summaries or {}).get(c_sym, {}) or {}
                    rs_val = float(
                        (c_sum.get("price") or {}).get(
                            "relative_strength_vs_btc_7d_pct",
                            (c_sum.get("daily_indicators") or {}).get("relative_strength_vs_btc_7d_pct", 0.0),
                        )
                        or 0.0
                    )
                    return float(t_inf["priority_score_bonus"]) + rs_val

                candidate_symbols.sort(key=_cand_priority_key, reverse=True)
                triggers.append("NEW_ENTRY_OPPORTUNITY")
                reasons.append(
                    f"新規エントリー好機検知 ({', '.join(candidate_symbols)}: 上位足トレンド＋1時間足押し目反発が合致)"
                )

        # ------------------------------------------------------------------
        # トリガー2: 保有ポジションのAI再評価 (24時間以上経過 または 含み損益・R倍数の大幅変動)
        # ------------------------------------------------------------------
        for pos in positions:
            p_sym = str(pos.get("symbol", "")).upper()
            holding_hours = float(pos.get("holding_hours", 0.0) or 0.0)
            pnl_pct = float(pos.get("unrealized_pnl_pct", 0.0) or 0.0)
            r_mult = float(pos.get("r_multiple", 0.0) or 0.0)
            time_stop_status = str(pos.get("time_stop_status", "ACTIVE"))

            if holding_hours >= self.position_review_hours:
                re_eval_positions.append(p_sym)
                reasons.append(
                    f"保有ポジション {p_sym} が {holding_hours:.1f}時間経過 (>= {self.position_review_hours:.0f}h タイムストップ審査)"
                )
            elif (
                abs(pnl_pct) >= self.pnl_shift_threshold_pct
                or r_mult >= 1.2
                or r_mult <= -0.75
                or time_stop_status in ("STAGE1_TIME_STOP", "FINAL_TIME_STOP", "BREAKEVEN_PROTECT")
            ):
                re_eval_positions.append(p_sym)
                reasons.append(
                    f"保有ポジション {p_sym} の含み損益変動 ({pnl_pct:+.2f}% / {r_mult:+.2f}R / {time_stop_status})"
                )

        if re_eval_positions:
            triggers.append("POSITION_REEVALUATION")

        # ------------------------------------------------------------------
        # トリガー3: 定期ヘルスチェック (最後のAI呼び出しから6時間以上経過)
        # ------------------------------------------------------------------
        hours_since_last_ai: Optional[float] = None
        if last_ai_call_at is not None:
            if isinstance(last_ai_call_at, datetime):
                last_dt = _now_jst(last_ai_call_at)
            else:
                try:
                    parsed = datetime.fromisoformat(str(last_ai_call_at).strip())
                    last_dt = _now_jst(parsed)
                except Exception:
                    last_dt = None
            if last_dt is not None:
                hours_since_last_ai = max(0.0, (now_jst - last_dt).total_seconds() / 3600.0)
                if hours_since_last_ai >= self.periodic_health_check_hours:
                    triggers.append("PERIODIC_HEALTH_CHECK")
                    reasons.append(
                        f"定期ヘルスチェック到達 (前回AI判定から {hours_since_last_ai:.1f}時間経過 >= {self.periodic_health_check_hours:.1f}h)"
                    )

        should_call_ai = len(triggers) > 0
        reason_summary = " / ".join(reasons) if reasons else "トリガー条件未該当（AI呼び出しスキップ・省エネ待機）"

        return {
            "should_call_ai": should_call_ai,
            "triggers": triggers,
            "candidate_symbols": candidate_symbols,
            "re_eval_positions": re_eval_positions,
            "gate3_skipped_symbols": gate3_skipped_symbols,
            "regime_info": regime_info,
            "hours_since_last_ai": round(hours_since_last_ai, 2) if hours_since_last_ai is not None else None,
            "reason_summary": reason_summary,
        }


def evaluate_symbol_gates(
    symbol: str,
    ticker: Dict[str, Any],
    symbol_rule: Optional[Dict[str, Any]] = None,
    atr_1h: Optional[Any] = None,
    equity_jpy: Optional[Any] = None,
    orderbook: Optional[Dict[str, Any]] = None,
    klines_1h: Optional[List[Dict[str, Any]]] = None,
    max_spread_pct: float = 0.35,
    max_obdr_slippage_pct: float = 0.45,
) -> Dict[str, Any]:
    """モジュールレベル Gate 0〜3 (+ OBDR 中抜き板フィルタ) 評価ヘルパー"""
    sym_up = str(symbol or "").strip().upper()
    rule = symbol_rule or DEFAULT_SYMBOL_RULES.get(sym_up, DEFAULT_SYMBOL_RULES.get("XRP", {}))
    radar = TechnicalRadar(max_spread_pct=max_spread_pct)
    if atr_1h is None and klines_1h:
        import pandas as pd
        from src.indicators import calculate_indicators

        df_k = calculate_indicators(pd.DataFrame(klines_1h))
        if not df_k.empty and "atr_14" in df_k.columns:
            atr_1h = float(df_k["atr_14"].iloc[-1])
    return radar.evaluate_symbol_gates(
        symbol=sym_up,
        ticker=ticker,
        symbol_rule=rule,
        atr_1h=atr_1h,
        equity_jpy=equity_jpy,
        orderbook=orderbook,
    )
