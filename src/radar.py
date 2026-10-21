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
from src.indicators import enrich_relative_strength_vs_btc, summarize_indicators
from src.regime_filter import MarketRegimeFilter
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

# Gate 0 固定除外リスト（廃止予定・超低流動性・特殊・ステーブルコイン DAI）
GATE0_EXCLUDED_SYMBOLS = set(EXCLUDED_SPOT_SYMBOLS) | {"DAI"}


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
    ) -> Dict[str, Any]:
        """
        単一銘柄について Gate 0 〜 Gate 3 の合否を厳密判定する。
        """
        sym_up = str(symbol or "").strip().upper()

        # Gate 0: 固定除外リスト & レバレッジ銘柄 (`*_JPY`)
        if not sym_up or sym_up.endswith("_JPY") or sym_up in GATE0_EXCLUDED_SYMBOLS:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_0_EXCLUDED",
                "reason": f"Gate 0 除外銘柄 ({sym_up})",
            }

        # Gate 1: ルール整合性確認
        min_size_dec = _to_decimal(symbol_rule.get("min_order_size", symbol_rule.get("minOrderSize", "0")))
        step_dec = _to_decimal(symbol_rule.get("size_step", symbol_rule.get("sizeStep", "0")))
        tick_dec = _to_decimal(symbol_rule.get("tick_size", symbol_rule.get("tickSize", "0")))
        if min_size_dec <= 0 or step_dec <= 0 or tick_dec <= 0:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_1_INVALID_RULE",
                "reason": f"Gate 1 取引ルール不正 (min={min_size_dec}, step={step_dec}, tick={tick_dec})",
            }

        # Gate 2: 24h売買代金 = Decimal(volume) * Decimal(last) >= 30,000,000円 & スプレッド <= 0.35%
        last_dec = _to_decimal(ticker.get("last", 0))
        bid_dec = _to_decimal(ticker.get("bid", last_dec))
        ask_dec = _to_decimal(ticker.get("ask", last_dec))
        vol_dec = _to_decimal(ticker.get("volume", 0))
        if last_dec <= 0 or bid_dec <= 0:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_2_NO_PRICE",
                "reason": "Gate 2 価格データなし",
            }

        trading_val_24h = vol_dec * last_dec if vol_dec > 0 else _to_decimal(ticker.get("trading_value_24h", 0))
        spread_pct_dec = ((ask_dec - bid_dec) / bid_dec * Decimal("100")) if (ask_dec >= bid_dec > 0) else Decimal("999")

        if trading_val_24h < self.min_24h_trading_value_jpy:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_2_LOW_VOLUME",
                "reason": f"Gate 2 24h売買代金不足 ({int(trading_val_24h):,}円 < {int(self.min_24h_trading_value_jpy):,}円)",
                "trading_value_24h_jpy": float(trading_val_24h),
            }
        if spread_pct_dec > self.max_spread_pct:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_2_WIDE_SPREAD",
                "reason": f"Gate 2 スプレッド超過 ({float(spread_pct_dec):.3f}% > {float(self.max_spread_pct):.2f}%)",
                "spread_pct": float(spread_pct_dec),
            }

        # Gate 3: 最小注文額 minOrderSize * last <= 2,500円 かつ 150円 / minOrderSize >= 1.5 * ATR_1h
        min_notional_jpy = min_size_dec * last_dec
        if min_notional_jpy > self.max_min_notional_jpy:
            return {
                "symbol": sym_up,
                "passed": False,
                "failed_gate": "GATE_3_MIN_NOTIONAL_TOO_LARGE",
                "reason": f"Gate 3 最小注文額超過 ({float(min_notional_jpy):,.0f}円 > {float(self.max_min_notional_jpy):,.0f}円)",
                "min_notional_jpy": float(min_notional_jpy),
            }

        if atr_1h is not None:
            atr_1h_dec = _to_decimal(atr_1h)
            if atr_1h_dec > Decimal("0"):
                max_allowable_sl_dist_at_min_lot = self.target_risk_jpy / min_size_dec
                required_min_sl_dist = atr_1h_dec * self.min_sl_atr1h_mult
                if max_allowable_sl_dist_at_min_lot < required_min_sl_dist:
                    return {
                        "symbol": sym_up,
                        "passed": False,
                        "failed_gate": "GATE_3_SL_TOO_TIGHT_FOR_MIN_LOT",
                        "reason": (
                            f"Gate 3 最小ロットでの許容SL距離不足 "
                            f"(150円/minLot={float(max_allowable_sl_dist_at_min_lot):.2f}円 < 1.5*ATR_1h={float(required_min_sl_dist):.2f}円)"
                        ),
                        "max_sl_dist_at_min_lot": float(max_allowable_sl_dist_at_min_lot),
                        "required_min_sl_dist": float(required_min_sl_dist),
                    }

        return {
            "symbol": sym_up,
            "passed": True,
            "failed_gate": None,
            "trading_value_24h_jpy": float(trading_val_24h),
            "spread_pct": float(spread_pct_dec),
            "min_notional_jpy": float(min_notional_jpy),
        }

    def screen_4tier_gates(
        self,
        tickers: Dict[str, Dict[str, Any]],
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        atr_1h_by_symbol: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """全銘柄に対して Gate 0 〜 Gate 3 を一括評価し、通過銘柄（24h売買代金降順）と除外理由一覧を返す"""
        rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
        atr_map = atr_1h_by_symbol or {}
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
                ob = public_client.get_orderbook(sym, depth=5)
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
        kill_switch_jpy = _to_decimal(enriched_pf.get("drawdown_kill_switch_jpy", 8500))
        if kill_switch_jpy <= Decimal("0"):
            kill_switch_jpy = Decimal("8500")
        kill_switch_active = total_equity < kill_switch_jpy
        cb_triggered, _ = check_btc_circuit_breaker(market_summaries)

        has_open_slot = (len(positions) + len(active_pending_buys)) < max_concurrent
        if has_open_slot and not kill_switch_active and not cb_triggered and regime_info["allow_new_buy"]:
            for sym, summary in market_summaries.items():
                sym_up = sym.strip().upper()
                if sym_up in held_or_pending_symbols or sym_up in GATE0_EXCLUDED_SYMBOLS:
                    continue

                daily = summary.get("daily_indicators", {}) or {}
                h4 = summary.get("h4_indicators", {}) or {}
                h1 = summary.get("h1_indicators", {}) or {}
                price_info = summary.get("price", {}) or {}
                rule = rules.get(sym_up, {})

                # Gate 3 チェック: 最小注文額 <= 2,500円 & 150円 / minOrderSize >= 1.5 * ATR_1h
                last_p = _to_decimal(price_info.get("last", 0))
                min_lot = _to_decimal(rule.get("min_order_size", "0.0001"))
                atr_1h_val = _to_decimal(h1.get("atr_14", 0))
                if last_p > 0 and min_lot > 0:
                    if (min_lot * last_p) > self.max_min_notional_jpy:
                        gate3_skipped_symbols.append(sym_up)
                        continue
                    if atr_1h_val > 0 and (self.target_risk_jpy / min_lot) < (atr_1h_val * self.min_sl_atr1h_mult):
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
