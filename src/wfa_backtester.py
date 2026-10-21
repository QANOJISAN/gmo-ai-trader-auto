"""
保守的約定モデル＆ローリング Walk-Forward Analysis (WFA) バックテストエンジン (wfa_backtester.py)

1. ConservativeSpotBacktester:
   - Maker 指値 (`SOK`: Post-Only) の約定判定を3段階 (`fill_model`) で切り替え可能：
     ① `"STRICT_PENETRATION"` (デフォルト・保守的貫通必須):
        ローソク足の安値が指値価格を 1 tick 以上下抜けた場合 (`Low <= P_limit - 1 * tickSize`) のみ約定と判定する。
        ヒゲ先がちょうど `Low == P_limit` にタッチしただけの足では、板の先頭キューに割り込めないものとして「未約定」とする。
     ② `"QUEUE_PRIORITY"` (キュー優先度モデル):
        `Low < P_limit`、または `Low == P_limit` かつ出来高が想定板厚の 2.5 倍以上ある場合のみ約定と判定する。
     ③ `"OPTIMISTIC_TOUCH"` (楽観的タッチ):
        `Low <= P_limit` で約定と判定する（比較ベンチマーク用）。

2. 逆指値 (`STOP` / `FAK`) の非線形スリッページモデル:
   - `STOP` 発動時の実効約定価格 (`effective_sl_fill_price`) は、単に `SL` 価格ちょうどではなく、
     急落時の板薄・貫通幅に応じた非線形スリッページを課す：
     `slippage_price = base_slippage + 0.15 * max(0.0, sl_price - bar_low)`
     `fill_price = sl_price - slippage_price`
     さらに Taker 手数料（0.05%〜0.09%）を控除して実現損益を算出する。

3. ローリング Walk-Forward Analysis (WFA):
   - In-Sample (IS) = 60日、Out-of-Sample (OOS) = 20日、ステップ = 10日のローリングウィンドウで検証し、
     * Profit Factor (PF = 総利益 / 総損失)
     * 最大ドローダウン (MaxDD 円 / %)
     * 1トレードあたり期待値 (Expectancy 円/トレード)
     * Walk-Forward Efficiency (`WFE = (OOS 年率換算リターン) / (IS 年率換算リターン)`)
     を算出する。
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional

logger = logging.getLogger(__name__)

FillModelType = Literal["STRICT_PENETRATION", "QUEUE_PRIORITY", "OPTIMISTIC_TOUCH"]


class ConservativeSpotBacktester:
    """
    3段階 SOK 指値約定モデル・非線形 STOP スリッページ・ローリング WFA 評価エンジン
    """

    def __init__(
        self,
        fill_model: FillModelType = "STRICT_PENETRATION",
        tick_size: float = 1.0,
        maker_fee_rate: float = -0.0001,
        taker_fee_rate: float = 0.0005,
        base_slippage_pct: float = 0.0005,
        overshoot_slippage_coeff: float = 0.15,
        initial_capital_jpy: float = 10000.0,
    ) -> None:
        self.fill_model: FillModelType = fill_model
        self.tick_size = float(tick_size)
        self.maker_fee_rate = float(maker_fee_rate)
        self.taker_fee_rate = float(taker_fee_rate)
        self.base_slippage_pct = float(base_slippage_pct)
        self.overshoot_slippage_coeff = float(overshoot_slippage_coeff)
        self.initial_capital_jpy = float(initial_capital_jpy)

    def check_sok_limit_buy_filled(
        self,
        limit_price: float,
        bar_low: float,
        bar_volume: float = 0.0,
        estimated_queue_size: float = 10.0,
        tick_size: Optional[float] = None,
    ) -> bool:
        """
        Maker 指値買い (`SOK`) が当該バーで約定したかを 3段階モデルで判定する。
        """
        tick = float(tick_size if tick_size is not None else self.tick_size)
        if self.fill_model == "STRICT_PENETRATION":
            # 1 tick 以上の下抜け貫通が必須 (Low == limit_price のヒゲタッチのみは未約定)
            return bar_low <= (limit_price - tick + 1e-9)
        if self.fill_model == "QUEUE_PRIORITY":
            if bar_low < (limit_price - 1e-9):
                return True
            if abs(bar_low - limit_price) <= 1e-9:
                return bar_volume >= (estimated_queue_size * 2.5)
            return False
        # OPTIMISTIC_TOUCH
        return bar_low <= (limit_price + 1e-9)

    def calculate_stop_sell_fill(
        self,
        sl_price: float,
        bar_low: float,
        size: float,
        entry_price: float,
        taker_fee_rate: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        逆指値 (`STOP` / `FAK`) 発動時の非線形スリッページ込み実効約定価格と実現損益（円）を算出する。
        `slippage = base_slippage + 0.15 * max(0, SL - Low)`
        """
        t_fee = float(taker_fee_rate if taker_fee_rate is not None else self.taker_fee_rate)
        base_slip = sl_price * self.base_slippage_pct
        overshoot = max(0.0, sl_price - bar_low)
        nonlinear_slip = base_slip + (self.overshoot_slippage_coeff * overshoot)
        effective_fill_price = max(self.tick_size, sl_price - nonlinear_slip)

        gross_pnl = (effective_fill_price - entry_price) * size
        entry_maker_fee = entry_price * size * self.maker_fee_rate
        exit_taker_fee = effective_fill_price * size * t_fee
        net_pnl_jpy = gross_pnl - entry_maker_fee - exit_taker_fee

        return {
            "sl_price": sl_price,
            "bar_low": bar_low,
            "overshoot": round(overshoot, 6),
            "slippage_price": round(nonlinear_slip, 6),
            "effective_fill_price": round(effective_fill_price, 6),
            "exit_taker_fee_jpy": round(exit_taker_fee, 4),
            "net_pnl_jpy": round(net_pnl_jpy, 2),
        }

    def compute_performance_metrics(
        self,
        trade_pnls_jpy: List[float],
        initial_capital_jpy: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        トレード損益系列から Profit Factor (PF)、最大ドローダウン (MaxDD)、勝率、1トレード期待値（円）を算出する。
        """
        init_cap = float(initial_capital_jpy if initial_capital_jpy is not None else self.initial_capital_jpy)
        if not trade_pnls_jpy:
            return {
                "trade_count": 0,
                "win_rate": 0.0,
                "gross_profit_jpy": 0.0,
                "gross_loss_jpy": 0.0,
                "net_profit_jpy": 0.0,
                "profit_factor": 0.0,
                "expectancy_jpy": 0.0,
                "max_drawdown_jpy": 0.0,
                "max_drawdown_pct": 0.0,
            }

        wins = [p for p in trade_pnls_jpy if p > 0]
        losses = [p for p in trade_pnls_jpy if p < 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        net_profit = sum(trade_pnls_jpy)
        pf = (gross_profit / gross_loss) if gross_loss > 0 else (99.9 if gross_profit > 0 else 0.0)
        expectancy = net_profit / len(trade_pnls_jpy)

        # エクイティカーブから最大ドローダウン (MaxDD) を算出
        equity = init_cap
        peak = init_cap
        max_dd_jpy = 0.0
        max_dd_pct = 0.0
        for p in trade_pnls_jpy:
            equity += p
            if equity > peak:
                peak = equity
            dd_jpy = peak - equity
            dd_pct = (dd_jpy / peak * 100.0) if peak > 0 else 0.0
            if dd_jpy > max_dd_jpy:
                max_dd_jpy = dd_jpy
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

        return {
            "trade_count": len(trade_pnls_jpy),
            "win_rate": round(len(wins) / len(trade_pnls_jpy), 4),
            "gross_profit_jpy": round(gross_profit, 2),
            "gross_loss_jpy": round(gross_loss, 2),
            "net_profit_jpy": round(net_profit, 2),
            "profit_factor": round(pf, 4),
            "expectancy_jpy": round(expectancy, 2),
            "max_drawdown_jpy": round(max_dd_jpy, 2),
            "max_drawdown_pct": round(max_dd_pct, 4),
        }

    def run_rolling_wfa(
        self,
        daily_pnls_jpy: List[float],
        is_days: int = 60,
        oos_days: int = 20,
        step_days: int = 10,
    ) -> Dict[str, Any]:
        """
        IS=60日 / OOS=20日（ステップ10日）のローリング Walk-Forward Analysis (WFA) を実行し、
        各ウィンドウの IS/OOS 成績および Walk-Forward Efficiency (`WFE`) を算出する。
        """
        n = len(daily_pnls_jpy)
        windows: List[Dict[str, Any]] = []
        oos_all_pnls: List[float] = []
        is_daily_means: List[float] = []
        oos_daily_means: List[float] = []

        start = 0
        while start + is_days + oos_days <= n:
            is_slice = daily_pnls_jpy[start : start + is_days]
            oos_slice = daily_pnls_jpy[start + is_days : start + is_days + oos_days]

            is_metrics = self.compute_performance_metrics([p for p in is_slice if p != 0.0])
            oos_metrics = self.compute_performance_metrics([p for p in oos_slice if p != 0.0])

            is_mean_per_day = sum(is_slice) / max(1, is_days)
            oos_mean_per_day = sum(oos_slice) / max(1, oos_days)
            win_wfe = (oos_mean_per_day / is_mean_per_day) if abs(is_mean_per_day) > 1e-9 else 0.0

            windows.append(
                {
                    "window_index": len(windows) + 1,
                    "is_range": (start, start + is_days),
                    "oos_range": (start + is_days, start + is_days + oos_days),
                    "is_net_profit_jpy": round(sum(is_slice), 2),
                    "oos_net_profit_jpy": round(sum(oos_slice), 2),
                    "is_profit_factor": is_metrics["profit_factor"],
                    "oos_profit_factor": oos_metrics["profit_factor"],
                    "window_wfe": round(win_wfe, 4),
                }
            )
            oos_all_pnls.extend([p for p in oos_slice if p != 0.0])
            is_daily_means.append(is_mean_per_day)
            oos_daily_means.append(oos_mean_per_day)
            start += step_days

        overall_oos_metrics = self.compute_performance_metrics(oos_all_pnls)
        avg_is_daily = sum(is_daily_means) / len(is_daily_means) if is_daily_means else 0.0
        avg_oos_daily = sum(oos_daily_means) / len(oos_daily_means) if oos_daily_means else 0.0
        overall_wfe = (avg_oos_daily / avg_is_daily) if abs(avg_is_daily) > 1e-9 else 0.0

        return {
            "is_days": is_days,
            "oos_days": oos_days,
            "step_days": step_days,
            "window_count": len(windows),
            "windows": windows,
            "overall_oos_metrics": overall_oos_metrics,
            "walk_forward_efficiency": round(overall_wfe, 4),
        }
