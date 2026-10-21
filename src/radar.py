"""
24時間テクニカル・レーダー：AI呼び出し最適化フィルタ (radar.py)

15分おきに全対象現物銘柄（23銘柄のうち取扱廃止・超低流動性銘柄を除いたスクリーニング銘柄）の
日足・4時間足・1時間足および板情報をスキャンし、無条件に毎回 LLM (Gemini) を呼び出すのではなく、
以下の3つのトリガー条件のいずれかを満たしたときだけ `AIJudge` を起動する省エネ・高精度フィルタ：

1. 新規エントリー好機 (`NEW_ENTRY_OPPORTUNITY`):
   - 空き枠（保有ポジション数＋待機中 `PENDING_BUY` 注文数 < `max_concurrent_positions` [2枠]）があり、
   - ドローダウン・キルスイッチ（総資産 < 8,500円）および BTC 暴落サーキットブレーカーが未発動で、
   - 上位足トレンド（日足または4時間足）と 1時間足の押し目シグナル（RSI反発・EMA/SMA反発・BB下限反発・出来高急増）が合致した銘柄が1つ以上あるとき。

2. 保有ポジションのAI再評価 (`POSITION_REEVALUATION`):
   - 保有ポジションが 24時間以上経過（タイムストップ審査）、または
   - 含み損益が大きく変動（含み損益率 ±2.5%以上、R倍数 +1.2R以上のトレーリング審査、または -0.75R以下の早期撤退審査）し、
     AIによる `MODIFY` / `SELL` 判定が必要なとき。

3. 定期ヘルスチェック (`PERIODIC_HEALTH_CHECK`):
   - 最後のAI呼び出し時刻 (`last_ai_call_at`) から 6.0 時間以上経過したとき。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from src.ai_judge import enrich_portfolio_positions
from src.gmo_client import EXCLUDED_SPOT_SYMBOLS, GMOPublicClient
from src.indicators import enrich_relative_strength_vs_btc, summarize_indicators
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    _to_decimal,
    calculate_total_equity_jpy,
    check_btc_circuit_breaker,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


class TechnicalRadar:
    """15分周期で市場とポートフォリオを走査し、AIJudge 呼び出し要否を判定するテクニカル・レーダー"""

    def __init__(
        self,
        periodic_health_check_hours: float = 6.0,
        position_review_hours: float = 24.0,
        pnl_shift_threshold_pct: float = 2.5,
    ) -> None:
        self.periodic_health_check_hours = float(periodic_health_check_hours)
        self.position_review_hours = float(position_review_hours)
        self.pnl_shift_threshold_pct = float(pnl_shift_threshold_pct)

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
            selected_symbols = list(screen_res.get("selected_symbols", []))
        else:
            selected_symbols = [
                s.strip().upper()
                for s in target_symbols_env.split(",")
                if s.strip() and s.strip().upper() not in EXCLUDED_SPOT_SYMBOLS
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
        戻り値:
        ```python
        {
            "should_call_ai": bool,
            "triggers": List[str],
            "candidate_symbols": List[str],
            "re_eval_positions": List[str],
            "hours_since_last_ai": Optional[float],
            "reason_summary": str,
        }
        ```
        """
        now_jst = _now_jst(now_dt)
        rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
        enriched_pf = enrich_portfolio_positions(
            portfolio=portfolio,
            market_summaries=market_summaries,
            symbol_rules=rules,
            now_dt=now_jst,
        )

        positions: List[Dict[str, Any]] = enriched_pf.get("positions", []) or []
        pending_orders: List[Dict[str, Any]] = enriched_pf.get("pending_orders", []) or []
        active_pending_buys = [
            o
            for o in pending_orders
            if str(o.get("side", "BUY")).upper() == "BUY"
            and str(o.get("state", "PENDING_BUY")).upper() == "PENDING_BUY"
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

        # ------------------------------------------------------------------
        # トリガー1: 新規エントリー好機 (空き枠あり & キルスイッチ未発動 & 上位足トレンド + 1h押し目合致)
        # ------------------------------------------------------------------
        total_equity = calculate_total_equity_jpy(enriched_pf, market_summaries)
        kill_switch_jpy = _to_decimal(enriched_pf.get("drawdown_kill_switch_jpy", 8500))
        if kill_switch_jpy <= Decimal("0"):
            kill_switch_jpy = Decimal("8500")
        kill_switch_active = total_equity < kill_switch_jpy
        cb_triggered, _ = check_btc_circuit_breaker(market_summaries)

        has_open_slot = (len(positions) + len(active_pending_buys)) < max_concurrent
        if has_open_slot and not kill_switch_active and not cb_triggered:
            for sym, summary in market_summaries.items():
                sym_up = sym.strip().upper()
                if sym_up in held_or_pending_symbols or sym_up in EXCLUDED_SPOT_SYMBOLS:
                    continue

                daily = summary.get("daily_indicators", {}) or {}
                h4 = summary.get("h4_indicators", {}) or {}
                h1 = summary.get("h1_indicators", {}) or {}

                if summary.get("climax_sell_warning") or h4.get("climax_sell_warning"):
                    continue

                daily_trend = str(daily.get("trend", ""))
                daily_rsi = float(daily.get("rsi_14", 50.0) or 50.0)
                h4_rsi = float(h4.get("rsi_14", 50.0) or 50.0)
                h4_close = float(h4.get("close", 0.0) or 0.0)
                h4_sma25 = float(h4.get("sma_25", 0.0) or 0.0)

                # 上位足トレンド条件: 日足が STRONG_UPTREND / UPTREND、または 4h終値 >= 4h SMA25 かつ RSI 適正
                higher_tf_bullish = (
                    daily_trend in ("STRONG_UPTREND", "UPTREND")
                    or (h4_close > 0 and h4_sma25 > 0 and h4_close >= h4_sma25 and 40.0 <= daily_rsi <= 68.0)
                )
                if not higher_tf_bullish:
                    continue

                # 1時間足の押し目・反発条件（1hデータがない場合は日足/4h足の押し目条件で代替評価）
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
            "hours_since_last_ai": round(hours_since_last_ai, 2) if hours_since_last_ai is not None else None,
            "reason_summary": reason_summary,
        }
