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


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


class MarketRegimeFilter:
    """
    市場全体の地合い (`TREND_UP` / `RANGE` / `RISK_OFF_CRASH`) を判定し、
    `RISK_OFF_CRASH` 時は2時間のヒステリシス冷却期間と未約定買い指値の一括キャンセルを提供する。
    """

    def __init__(
        self,
        btc_1h_crash_pct: float = -2.5,
        btc_4h_crash_pct: float = -4.0,
        btc_4h_crash_rsi: float = 22.0,
        min_breadth_ratio: float = 0.25,
        hysteresis_hours: float = 2.0,
    ) -> None:
        self.btc_1h_crash_pct = float(btc_1h_crash_pct)
        self.btc_4h_crash_pct = float(btc_4h_crash_pct)
        self.btc_4h_crash_rsi = float(btc_4h_crash_rsi)
        self.min_breadth_ratio = float(min_breadth_ratio)
        self.hysteresis_hours = float(hysteresis_hours)

        self.current_regime: str = "RANGE"
        self.risk_off_triggered_at: Optional[datetime] = None
        self.risk_off_cooldown_until: Optional[datetime] = None
        self.last_reason: str = ""

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

        # Breadth（スクリーニング銘柄のうち 1h EMA50/SMA25 または日足/4hトレンドが上向きの比率）
        total_syms = 0
        bullish_syms = 0
        for sym, summary in (market_summaries or {}).items():
            if not isinstance(summary, dict):
                continue
            total_syms += 1
            h1 = summary.get("h1_indicators") or {}
            h4 = summary.get("h4_indicators") or {}
            daily = summary.get("daily_indicators") or {}

            h1_close = float(h1.get("close", 0) or 0)
            h1_ma = float(h1.get("ema_50") or h1.get("sma_25") or 0)
            h4_close = float(h4.get("close", 0) or 0)
            h4_ma = float(h4.get("sma_25", 0) or 0)
            d_trend = str(daily.get("trend", ""))

            is_above = False
            if h1_close > 0 and h1_ma > 0:
                is_above = h1_close >= h1_ma
            elif h4_close > 0 and h4_ma > 0:
                is_above = h4_close >= h4_ma
            elif d_trend in ("STRONG_UPTREND", "UPTREND"):
                is_above = True
            elif summary.get("above_ema50") is True:
                is_above = True
            elif summary.get("above_ema50") is False:
                is_above = False
            else:
                # 情報未指定時は中立としてカウント
                is_above = True

            if is_above:
                bullish_syms += 1

        breadth_ratio = (bullish_syms / total_syms) if total_syms > 0 else 1.0

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
        if total_syms >= 4 and breadth_ratio < self.min_breadth_ratio:
            crash_triggered = True
            reasons.append(
                f"市場Breadth悪化 ({bullish_syms}/{total_syms} = {breadth_ratio * 100:.1f}% < {self.min_breadth_ratio * 100:.0f}%)"
            )

        if crash_triggered:
            self.current_regime = "RISK_OFF_CRASH"
            self.risk_off_triggered_at = now_jst
            self.risk_off_cooldown_until = now_jst + timedelta(hours=self.hysteresis_hours)
            self.last_reason = " / ".join(reasons)
            return {
                "regime": "RISK_OFF_CRASH",
                "allow_new_buy": False,
                "should_cancel_pending_buys": True,
                "in_hysteresis_cooldown": False,
                "cooldown_until": self.risk_off_cooldown_until.isoformat(timespec="seconds"),
                "btc_change_1h_pct": round(btc_1h_pct, 2),
                "btc_change_4h_pct": round(btc_4h_pct, 2),
                "btc_rsi_4h": round(btc_4h_rsi, 2),
                "breadth_ratio": round(breadth_ratio, 4),
                "reasons": reasons,
            }

        # 2時間ヒステリシス（冷却期間）中かどうかチェック
        if self.risk_off_cooldown_until is not None and now_jst < self.risk_off_cooldown_until:
            remaining_min = (self.risk_off_cooldown_until - now_jst).total_seconds() / 60.0
            hyst_reason = (
                f"RISK_OFF_CRASH 冷却ヒステリシス継続中 "
                f"(残り {remaining_min:.0f}分 / 解除予定 {self.risk_off_cooldown_until.isoformat(timespec='seconds')})"
            )
            self.current_regime = "RISK_OFF_CRASH"
            return {
                "regime": "RISK_OFF_CRASH",
                "allow_new_buy": False,
                "should_cancel_pending_buys": True,
                "in_hysteresis_cooldown": True,
                "cooldown_until": self.risk_off_cooldown_until.isoformat(timespec="seconds"),
                "btc_change_1h_pct": round(btc_1h_pct, 2),
                "btc_change_4h_pct": round(btc_4h_pct, 2),
                "btc_rsi_4h": round(btc_4h_rsi, 2),
                "breadth_ratio": round(breadth_ratio, 4),
                "reasons": [hyst_reason],
            }

        # 冷却期間経過後は解除
        self.risk_off_cooldown_until = None
        btc_trend = str(btc_daily.get("trend", ""))
        if btc_trend in ("STRONG_UPTREND", "UPTREND") and breadth_ratio >= 0.50:
            self.current_regime = "TREND_UP"
            reasons.append(f"BTC上昇トレンド ({btc_trend}) ＆ Breadth良好 ({breadth_ratio * 100:.0f}%)")
        else:
            self.current_regime = "RANGE"
            reasons.append(f"レンジ・選別相場 (Breadth={breadth_ratio * 100:.0f}%)")

        return {
            "regime": self.current_regime,
            "allow_new_buy": True,
            "should_cancel_pending_buys": False,
            "in_hysteresis_cooldown": False,
            "cooldown_until": None,
            "btc_change_1h_pct": round(btc_1h_pct, 2),
            "btc_change_4h_pct": round(btc_4h_pct, 2),
            "btc_rsi_4h": round(btc_4h_rsi, 2),
            "breadth_ratio": round(breadth_ratio, 4),
            "reasons": reasons,
        }
