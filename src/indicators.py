"""
テクニカル指標算出モジュール (indicators.py)

ローソク足 DataFrame から以下の指標を算出し、AI判定用の要約辞書を生成する:
- 単純移動平均線 (SMA 7, 25, 75)
- 指数平滑移動平均線 (EMA 12, 26)
- RSI (14)
- MACD (12, 26, 9)
- ボリンジャーバンド (20, ±2σ)
- ATR (14) — 押し目買い指値深さ・利確(TP)・損切り(SL)幅の動的算出に利用
- 出来高比率 (volume_ratio_20) — 17:00時点の未確定日足を除外し、前日までの確定日足および確定済み直近4時間足を基準に算出
- 直近7日間リターン (%) および 対BTC 7日間相対強度 (銘柄7d騰落率 - BTC7d騰落率)
- クライマックス売り警告 (climax_sell_warning) — 直近4時間足が陰線かつ出来高比率2.0倍以上のナイフ掴み警告
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from src.json_utils import sanitize_json_value

JST = timezone(timedelta(hours=9))


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    ローソク足 DataFrame に各種テクニカル指標カラムを追加して返す。
    入力カラム: ['open_time', 'open', 'high', 'low', 'close', 'volume']
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    close = out["close"].astype(float).ffill().bfill().fillna(0.0)
    high = out["high"].astype(float).ffill().bfill().fillna(close)
    low = out["low"].astype(float).ffill().bfill().fillna(close)
    volume = out["volume"].astype(float).fillna(0.0)

    # 1. 移動平均線 (SMA / EMA)
    out["sma_7"] = close.rolling(window=7, min_periods=1).mean()
    out["sma_25"] = close.rolling(window=25, min_periods=1).mean()
    out["sma_75"] = close.rolling(window=75, min_periods=1).mean()
    out["ema_12"] = close.ewm(span=12, adjust=False).mean()
    out["ema_26"] = close.ewm(span=26, adjust=False).mean()
    out["ema_50"] = close.ewm(span=50, adjust=False).mean()

    # 2. MACD (12, 26, 9)
    out["macd"] = out["ema_12"] - out["ema_26"]
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    # 3. RSI (Wilder方式 14期間)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi_raw = 100.0 - (100.0 / (1.0 + rs))
    rsi_filled = np.where(
        avg_loss.isna() | avg_gain.isna(),
        50.0,
        np.where(
            (avg_loss == 0.0) & (avg_gain > 0.0),
            100.0,
            np.where(
                (avg_gain == 0.0) & (avg_loss > 0.0),
                0.0,
                rsi_raw.fillna(50.0),
            ),
        ),
    )
    out["rsi_14"] = pd.Series(rsi_filled, index=out.index)

    # 4. ボリンジャーバンド (20期間, ±2σ / 少数行でもNaNを出さないよう min_periods=1)
    bb_mid = close.rolling(window=20, min_periods=1).mean()
    bb_std = close.rolling(window=20, min_periods=1).std(ddof=0).fillna(0.0)
    out["bb_mid"] = bb_mid
    out["bb_upper"] = bb_mid + 2.0 * bb_std
    out["bb_lower"] = bb_mid - 2.0 * bb_std
    out["bb_width_pct"] = np.where(
        bb_mid > 0,
        (out["bb_upper"] - out["bb_lower"]) / bb_mid * 100.0,
        0.0,
    )

    # 5. ATR (Average True Range, 14期間 & 20期間) & ADX(14) & BKSR
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["atr_14"] = true_range.ewm(alpha=1.0 / 14.0, min_periods=1, adjust=False).mean()
    out["atr_20"] = true_range.ewm(alpha=1.0 / 20.0, min_periods=1, adjust=False).mean()
    out["bksr"] = np.where(
        out["atr_20"] > 0,
        (4.0 * bb_std) / (3.0 * out["atr_20"]),
        1.0,
    )

    up_move = high.diff().fillna(0.0)
    down_move = (-low.diff()).fillna(0.0)
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0),
        index=out.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=out.index,
    )
    atr_safe = out["atr_14"].replace(0.0, np.nan)
    plus_di = 100.0 * (plus_dm.ewm(alpha=1.0 / 14.0, min_periods=1, adjust=False).mean() / atr_safe).fillna(0.0)
    minus_di = 100.0 * (minus_dm.ewm(alpha=1.0 / 14.0, min_periods=1, adjust=False).mean() / atr_safe).fillna(0.0)
    di_sum = (plus_di + minus_di).replace(0.0, np.nan)
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum).fillna(20.0)
    out["adx_14"] = dx.ewm(alpha=1.0 / 14.0, min_periods=1, adjust=False).mean()

    # 6. 出来高移動平均・変化率（直前までの確定足20本平均を基準とする）
    prev_vol_sma_20 = volume.shift(1).rolling(window=20, min_periods=1).mean()
    fallback_vol_sma_20 = volume.rolling(window=20, min_periods=1).mean()
    vol_sma_20 = prev_vol_sma_20.fillna(fallback_vol_sma_20)
    out["volume_ratio_20"] = np.where(vol_sma_20 > 0, volume / vol_sma_20, 1.0)

    return out


def compute_orderbook_microstructure(
    orderbook: Optional[Dict[str, Any]],
    last_price: float = 0.0,
    tick_size: float = 0.0,
) -> Dict[str, Any]:
    """
    テーマ2-(1)-①: 5段階指数減衰加重板不均衡 (OBI_5 = sum_{i=1}^5 e^{-0.5(i-1)} Q_bid,i vs ask in [-1.0, +1.0])
    および Stoikov Micro-Price (P_micro = I_t * P_a + (1 - I_t) * P_b, I_t = Q_b / (Q_b + Q_a))、
    上位5〜10気配の最大買い壁 (support_wall_price, support_wall_front_run_price) を算出する。
    """
    if not orderbook or not isinstance(orderbook, dict):
        return {
            "obi_5": 0.0,
            "best_imbalance_it": 0.5,
            "micro_price": float(last_price or 0.0),
            "mid_price": float(last_price or 0.0),
            "micro_price_divergence_bps": 0.0,
            "support_wall_price": float(last_price or 0.0),
            "support_wall_size": 0.0,
            "support_wall_front_run_price": float(last_price or 0.0),
            "resistance_wall_price": float(last_price or 0.0),
            "resistance_wall_size": 0.0,
        }

    raw_bids = orderbook.get("bids") or []
    raw_asks = orderbook.get("asks") or []

    parsed_bids: List[tuple[float, float]] = []
    for b in raw_bids[:10]:
        try:
            p = float(b.get("price", 0.0) if isinstance(b, dict) else b[0])
            s = float(b.get("size", 0.0) if isinstance(b, dict) else b[1])
            if p > 0 and s > 0:
                parsed_bids.append((p, s))
        except Exception:
            continue

    parsed_asks: List[tuple[float, float]] = []
    for a in raw_asks[:10]:
        try:
            p = float(a.get("price", 0.0) if isinstance(a, dict) else a[0])
            s = float(a.get("size", 0.0) if isinstance(a, dict) else a[1])
            if p > 0 and s > 0:
                parsed_asks.append((p, s))
        except Exception:
            continue

    parsed_bids.sort(key=lambda x: x[0], reverse=True)
    parsed_asks.sort(key=lambda x: x[0])

    if not parsed_bids and not parsed_asks:
        fallback_ob_imb = float(orderbook.get("orderbook_imbalance", 0.0) or 0.0)
        it_fallback = max(0.0, min(1.0, 0.5 * (fallback_ob_imb + 1.0)))
        return {
            "obi_5": round(max(-1.0, min(1.0, fallback_ob_imb)), 4),
            "best_imbalance_it": round(it_fallback, 4),
            "micro_price": float(last_price or 0.0),
            "mid_price": float(last_price or 0.0),
            "micro_price_divergence_bps": 0.0,
            "support_wall_price": float(last_price or 0.0),
            "support_wall_size": 0.0,
            "support_wall_front_run_price": float(last_price or 0.0),
            "resistance_wall_price": float(last_price or 0.0),
            "resistance_wall_size": 0.0,
        }

    best_bid_p, best_bid_q = parsed_bids[0] if parsed_bids else (float(last_price or 0.0), 0.0)
    best_ask_p, best_ask_q = parsed_asks[0] if parsed_asks else (float(last_price or 0.0), 0.0)
    mid_price = (
        0.5 * (best_bid_p + best_ask_p)
        if (best_bid_p > 0 and best_ask_p > 0)
        else (best_bid_p or best_ask_p or float(last_price or 0.0))
    )

    # 最良気配インバランス I_t = Q_b / (Q_b + Q_a) と Stoikov Micro-Price P_micro
    denom_top = best_bid_q + best_ask_q
    best_it = (best_bid_q / denom_top) if denom_top > 0 else 0.5
    micro_price = (
        best_it * best_ask_p + (1.0 - best_it) * best_bid_p
        if (best_bid_p > 0 and best_ask_p > 0)
        else mid_price
    )
    micro_div_bps = ((micro_price - mid_price) / mid_price * 10000.0) if mid_price > 0 else 0.0

    # 5段階指数減衰加重板不均衡 OBI_5 = (W_bid - W_ask) / (W_bid + W_ask)
    w_bid = 0.0
    for idx, (_, q_b) in enumerate(parsed_bids[:5]):
        w_bid += math.exp(-0.5 * idx) * q_b
    w_ask = 0.0
    for idx, (_, q_a) in enumerate(parsed_asks[:5]):
        w_ask += math.exp(-0.5 * idx) * q_a
    obi_5 = ((w_bid - w_ask) / (w_bid + w_ask)) if (w_bid + w_ask) > 0 else 0.0
    obi_5 = max(-1.0, min(1.0, obi_5))

    # 上位5〜10気配で最大の買い板厚を持つサポート壁と、その1ティック上 (Front-running tick)
    inferred_tick = tick_size if tick_size > 0 else (1.0 if mid_price >= 50.0 else 0.001)
    if parsed_bids:
        wall_p, wall_q = max(parsed_bids[:10], key=lambda item: item[1])
        front_run_p = wall_p + inferred_tick
        if best_ask_p > 0 and front_run_p >= best_ask_p:
            front_run_p = max(wall_p, best_ask_p - inferred_tick)
    else:
        wall_p, wall_q, front_run_p = mid_price, 0.0, mid_price

    if parsed_asks:
        res_p, res_q = max(parsed_asks[:10], key=lambda item: item[1])
    else:
        res_p, res_q = mid_price, 0.0

    return {
        "obi_5": round(obi_5, 4),
        "best_imbalance_it": round(best_it, 4),
        "micro_price": round(micro_price, 6),
        "mid_price": round(mid_price, 6),
        "micro_price_divergence_bps": round(micro_div_bps, 3),
        "support_wall_price": round(wall_p, 6),
        "support_wall_size": round(wall_q, 6),
        "support_wall_front_run_price": round(front_run_p, 6),
        "resistance_wall_price": round(res_p, 6),
        "resistance_wall_size": round(res_q, 6),
    }


def compute_trade_flow_and_absorption(
    trades: Optional[List[Dict[str, Any]]] = None,
    df_1h: Optional[Any] = None,
    now_ms: Optional[float] = None,
    klines_15m: Optional[Any] = None,
    atr_15m: Optional[float] = None,
    now_ts_ms: Optional[float] = None,
) -> Dict[str, Any]:
    """
    テーマ2-(1)-②: 正規化トレード・フロー不均衡 (TFI_15m, TFI_1h in [-1.0, +1.0])
    および CVD吸収スコア (成行売りが優勢でも価格下落幅が圧縮されている「売り吸収 = 底打ちサイン」の検出)。
    約定履歴リスト `trades` が無い場合は 1時間足ローソク足のクローズ位置・出来高から近似推定する。
    """
    if now_ms is None and now_ts_ms is not None:
        now_ms = float(now_ts_ms)
    if df_1h is None and klines_15m is not None:
        df_1h = klines_15m
    if isinstance(df_1h, list):
        df_1h = pd.DataFrame(df_1h)
    tfi_15m = 0.0
    tfi_1h = 0.0
    buy_notional_1h = 0.0
    sell_notional_1h = 0.0
    price_change_15m_pct = 0.0

    if trades and isinstance(trades, list) and len(trades) > 0:
        parsed_rows: List[tuple[float, str, float, float]] = []
        for idx, t in enumerate(trades):
            if not isinstance(t, dict):
                continue
            side = str(t.get("side", "")).upper()
            try:
                price = float(t.get("price", 0.0) or 0.0)
                size = float(t.get("size", 0.0) or 0.0)
            except Exception:
                continue
            if price <= 0 or size <= 0:
                continue
            ts_ms = 0.0
            raw_ts = t.get("timestamp") or t.get("time") or t.get("executed_at")
            if isinstance(raw_ts, (int, float)):
                ts_ms = float(raw_ts) if raw_ts > 1e11 else float(raw_ts) * 1000.0
            elif isinstance(raw_ts, str) and raw_ts:
                try:
                    ts_ms = pd.Timestamp(raw_ts).timestamp() * 1000.0
                except Exception:
                    ts_ms = 0.0
            parsed_rows.append((ts_ms, side, price, size))

        if parsed_rows:
            max_ts = max(r[0] for r in parsed_rows)
            ref_now = float(now_ms) if (now_ms and now_ms > 0) else max_ts
            buy_15m = 0.0
            sell_15m = 0.0
            prices_15m: List[float] = []
            total_n = len(parsed_rows)
            for i, (ts_ms, side, price, size) in enumerate(parsed_rows):
                notional = price * size
                in_1h = (ref_now - ts_ms <= 3600_000.0) if ref_now > 0 and ts_ms > 0 else True
                in_15m = (
                    (ref_now - ts_ms <= 900_000.0)
                    if (ref_now > 0 and ts_ms > 0)
                    else (i >= max(0, total_n - max(1, total_n // 3)))
                )
                if in_1h:
                    if side == "BUY":
                        buy_notional_1h += notional
                    elif side == "SELL":
                        sell_notional_1h += notional
                if in_15m:
                    prices_15m.append(price)
                    if side == "BUY":
                        buy_15m += notional
                    elif side == "SELL":
                        sell_15m += notional

            if (buy_15m + sell_15m) > 0:
                tfi_15m = (buy_15m - sell_15m) / (buy_15m + sell_15m)
            if (buy_notional_1h + sell_notional_1h) > 0:
                tfi_1h = (buy_notional_1h - sell_notional_1h) / (buy_notional_1h + sell_notional_1h)
            if len(prices_15m) >= 2 and prices_15m[0] > 0:
                price_change_15m_pct = (prices_15m[-1] - prices_15m[0]) / prices_15m[0] * 100.0

    # ローソク足からのフォールバックまたは補完
    price_change_1h_pct = 0.0
    lower_wick_cur = 0.0
    atr_pct_1h = 0.8
    if df_1h is not None and not df_1h.empty:
        row = df_1h.iloc[-1]
        prev_row = df_1h.iloc[-2] if len(df_1h) >= 2 else row
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        prev_c = float(prev_row["close"])
        rng = max(1e-9, h - l)
        clv = ((c - l) - (h - c)) / rng  # [-1.0, +1.0] Close Location Value
        lower_wick_cur = (min(o, c) - l) / rng
        price_change_1h_pct = ((c - prev_c) / prev_c * 100.0) if prev_c > 0 else 0.0
        atr_pct_1h = max(0.15, (rng / c * 100.0) if c > 0 else 0.8)
        if not trades:
            tfi_15m = max(-1.0, min(1.0, clv))
            if len(df_1h) >= 2:
                prev_rng = max(1e-9, float(prev_row["high"]) - float(prev_row["low"]))
                prev_clv = (
                    (float(prev_row["close"]) - float(prev_row["low"]))
                    - (float(prev_row["high"]) - float(prev_row["close"]))
                ) / prev_rng
                tfi_1h = max(-1.0, min(1.0, 0.65 * clv + 0.35 * prev_clv))
            else:
                tfi_1h = tfi_15m
            price_change_15m_pct = price_change_1h_pct * 0.5

    # CVD吸収スコア: 成行売りが優勢 (TFI < -0.12) なのに価格下落幅が圧縮されている (または下ヒゲ吸収)
    sell_pressure = max(0.0, -min(tfi_15m, tfi_1h))
    effective_drop_pct = max(0.0, -min(price_change_15m_pct, price_change_1h_pct))
    drop_compression_ratio = max(0.0, 1.0 - (effective_drop_pct / max(0.25, atr_pct_1h)))
    absorption_score = sell_pressure * drop_compression_ratio
    if lower_wick_cur >= 0.45 and sell_pressure > 0.10:
        absorption_score = min(1.0, absorption_score + 0.25 * lower_wick_cur)
    cvd_absorption_detected = bool(
        (sell_pressure >= 0.15 and effective_drop_pct <= max(0.25, 0.35 * atr_pct_1h))
        or absorption_score >= 0.22
    )

    return {
        "tfi_15m": round(max(-1.0, min(1.0, tfi_15m)), 4),
        "tfi_1h": round(max(-1.0, min(1.0, tfi_1h)), 4),
        "buy_notional_1h_jpy": round(buy_notional_1h, 2),
        "sell_notional_1h_jpy": round(sell_notional_1h, 2),
        "absorption_score": round(min(1.0, absorption_score), 4),
        "cvd_absorption_detected": cvd_absorption_detected,
    }


def compute_bksr_squeeze(df: Optional[Any]) -> Dict[str, Any]:
    """
    テーマ2-(1)-③: ボリンジャー・ケルトナー圧縮比 (BKSR = 4.0 * sigma_20 / (3.0 * ATR_20))
    BKSR < 1.0 でエネルギー充填（スクイーズ状態）、1.0を上抜けかつ close > BB_upper または直近スクイーズからの上放れを検知。
    """
    if isinstance(df, list):
        df = pd.DataFrame(df)
    if df is None or df.empty:
        return {
            "bksr": 1.0,
            "is_squeeze": False,
            "recent_squeeze": False,
            "squeeze_breakout_up": False,
        }
    ind = calculate_indicators(df) if "bksr" not in df.columns else df
    latest = ind.iloc[-1]
    bksr_val = float(latest.get("bksr", 1.0) or 1.0)
    is_squeeze = bool(bksr_val < 1.0)
    tail_bksr = ind["bksr"].tail(5).astype(float)
    recent_squeeze = bool((tail_bksr < 1.05).any())
    close_p = float(latest["close"])
    bb_upper = float(latest["bb_upper"])
    bb_mid = float(latest["bb_mid"])
    prev_close = float(ind.iloc[-2]["close"]) if len(ind) >= 2 else close_p
    squeeze_breakout_up = bool(
        recent_squeeze
        and close_p > prev_close
        and (close_p >= bb_upper * 0.998 or (bksr_val >= 1.0 and close_p > bb_mid))
    )
    return {
        "bksr": round(bksr_val, 4),
        "is_squeeze": is_squeeze,
        "recent_squeeze": recent_squeeze,
        "squeeze_breakout_up": squeeze_breakout_up,
    }


def compute_wick_toxicity_ratio(
    df: Optional[Any],
    tfi_15m: float = 0.0,
    lookback: int = 3,
) -> Dict[str, Any]:
    """
    テーマ2-(1)-④: ストップ狩り・ヒゲ毒性比率 (WTR: Wick Toxicity Ratio)
    直近3本において:
    - UpperWickRatio = (High - max(Open, Close)) / (High - Low) > 0.55 かつ出来高増を「上値追い罠（減点）」
    - LowerWickRatio = (min(Open, Close) - Low) / (High - Low) > 0.60 かつ TFI > +0.15 を「ストップ狩り吸収完了のSpringシグナル（加点）」
    """
    if isinstance(df, list):
        df = pd.DataFrame(df)
    if df is None or df.empty:
        return {
            "upper_wick_ratio": 0.0,
            "lower_wick_ratio": 0.0,
            "upper_wick_trap_penalty": 0.0,
            "lower_wick_spring_bonus": 0.0,
            "wtr_net_score": 0.0,
            "is_upper_wick_trap": False,
            "is_lower_wick_spring": False,
        }
    ind = calculate_indicators(df) if "volume_ratio_20" not in df.columns else df
    window = ind.tail(max(1, int(lookback)))
    max_upper_ratio = 0.0
    max_lower_ratio = 0.0
    upper_trap = False
    lower_spring = False

    for _, row in window.iterrows():
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        v_ratio_raw = row.get("volume_ratio_20", 1.0)
        v_ratio = 1.2 if (v_ratio_raw is None or pd.isna(v_ratio_raw) or float(v_ratio_raw) == 0.0) else float(v_ratio_raw)
        rng = h - l
        if rng <= 0:
            continue
        u_ratio = (h - max(o, c)) / rng
        l_ratio = (min(o, c) - l) / rng
        max_upper_ratio = max(max_upper_ratio, u_ratio)
        max_lower_ratio = max(max_lower_ratio, l_ratio)
        if u_ratio > 0.55 and v_ratio >= 1.0:
            upper_trap = True
        if l_ratio > 0.60 and (tfi_15m >= 0.0 or c >= o):
            lower_spring = True

    penalty = -0.15 if upper_trap else (-0.05 if max_upper_ratio > 0.55 else 0.0)
    bonus = 0.15 if (max_lower_ratio > 0.60 and tfi_15m > 0.15) else (0.08 if lower_spring else 0.0)
    return {
        "upper_wick_ratio": round(max_upper_ratio, 4),
        "lower_wick_ratio": round(max_lower_ratio, 4),
        "upper_wick_trap_penalty": round(penalty, 4),
        "lower_wick_spring_bonus": round(bonus, 4),
        "wtr_net_score": round(bonus + penalty, 4),
        "is_upper_wick_trap": bool(upper_trap),
        "is_lower_wick_spring": bool(lower_spring or (max_lower_ratio > 0.60 and tfi_15m > 0.15)),
    }


def compute_obdr_slippage(
    orderbook: Optional[Dict[str, Any]],
    order_notional_jpy: float = 9_011.0,
    max_slippage_pct: float = 0.45,
    target_notional_jpy: Optional[float] = None,
) -> Dict[str, Any]:
    """
    テーマ2-(1)-⑤: 板厚スリッページ耐性 (OBDR: Order Book Depth Resilience)
    1枠の標準投下資金 (9,011円) を現在板の bids に対して成行売却シミュレーションした際の
    加重平均約定価格と仲値の乖離率を算出し、0.45% を超える「中抜き板（Hollow Book）」を検知する。
    """
    if target_notional_jpy is not None:
        order_notional_jpy = float(target_notional_jpy)
    if not orderbook or not isinstance(orderbook, dict):
        return {
            "order_notional_jpy": round(float(order_notional_jpy), 2),
            "vwap_fill_price": 0.0,
            "mid_price": 0.0,
            "obdr_slippage_pct": 0.0,
            "filled_ratio": 1.0,
            "is_hollow_book": False,
            "has_orderbook": False,
        }

    if "obdr_slippage_pct" in orderbook and not orderbook.get("bids"):
        slip = float(orderbook.get("obdr_slippage_pct", 0.0) or 0.0)
        return {
            "order_notional_jpy": round(float(order_notional_jpy), 2),
            "vwap_fill_price": 0.0,
            "mid_price": 0.0,
            "obdr_slippage_pct": round(slip, 4),
            "filled_ratio": 1.0,
            "is_hollow_book": bool(slip > max_slippage_pct),
            "has_orderbook": True,
        }

    raw_bids = orderbook.get("bids") or []
    raw_asks = orderbook.get("asks") or []
    if not raw_bids:
        return {
            "order_notional_jpy": round(float(order_notional_jpy), 2),
            "vwap_fill_price": 0.0,
            "mid_price": 0.0,
            "obdr_slippage_pct": 0.0,
            "filled_ratio": 1.0,
            "is_hollow_book": False,
            "has_orderbook": False,
        }

    bids: List[tuple[float, float]] = []
    for b in raw_bids:
        try:
            p = float(b.get("price", 0.0) if isinstance(b, dict) else b[0])
            s = float(b.get("size", 0.0) if isinstance(b, dict) else b[1])
            if p > 0 and s > 0:
                bids.append((p, s))
        except Exception:
            continue
    bids.sort(key=lambda x: x[0], reverse=True)

    asks: List[tuple[float, float]] = []
    for a in raw_asks:
        try:
            p = float(a.get("price", 0.0) if isinstance(a, dict) else a[0])
            s = float(a.get("size", 0.0) if isinstance(a, dict) else a[1])
            if p > 0 and s > 0:
                asks.append((p, s))
        except Exception:
            continue
    asks.sort(key=lambda x: x[0])

    if not bids:
        return {
            "order_notional_jpy": round(float(order_notional_jpy), 2),
            "vwap_fill_price": 0.0,
            "mid_price": 0.0,
            "obdr_slippage_pct": 99.9,
            "filled_ratio": 0.0,
            "is_hollow_book": True,
            "has_orderbook": True,
        }

    best_bid = bids[0][0]
    best_ask = asks[0][0] if asks else best_bid
    mid_price = 0.5 * (best_bid + best_ask) if best_ask > 0 else best_bid

    remaining_jpy = max(1.0, float(order_notional_jpy))
    total_qty = 0.0
    total_proceeds_jpy = 0.0

    for bid_p, bid_sz in bids:
        level_jpy = bid_p * bid_sz
        take_jpy = min(remaining_jpy, level_jpy)
        take_qty = take_jpy / bid_p
        total_qty += take_qty
        total_proceeds_jpy += take_jpy
        remaining_jpy -= take_jpy
        if remaining_jpy <= 1e-6:
            break

    # 板をすべて食い尽くしてもまだ約定しきれない場合は最下段のさらに1.0%下で残り約定とみなすペナルティ
    if remaining_jpy > 1e-6:
        worst_p = bids[-1][0] * 0.99
        if worst_p > 0:
            total_qty += remaining_jpy / worst_p
            total_proceeds_jpy += remaining_jpy

    vwap_price = (float(order_notional_jpy) / total_qty) if total_qty > 0 else 0.0
    slippage_pct = ((mid_price - vwap_price) / mid_price * 100.0) if (mid_price > 0 and vwap_price > 0) else 99.9
    slippage_pct = max(0.0, slippage_pct)
    filled_ratio = min(1.0, (float(order_notional_jpy) - max(0.0, remaining_jpy)) / max(1.0, float(order_notional_jpy)))

    return {
        "order_notional_jpy": round(float(order_notional_jpy), 2),
        "vwap_fill_price": round(vwap_price, 6),
        "mid_price": round(mid_price, 6),
        "obdr_slippage_pct": round(slippage_pct, 4),
        "filled_ratio": round(filled_ratio, 4),
        "is_hollow_book": bool(slippage_pct > float(max_slippage_pct)),
        "has_orderbook": True,
    }


def compute_btc_beta_and_correlation_matrix(
    market_summaries: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    テーマ5-(1): 1時間足リターン系列 (`h1_indicators.returns_1h`) から
    対BTCベータ値 beta_i = Cov(r_i, r_BTC) / Var(r_BTC) および
    銘柄間ペアワイズ相関係数 rho_{A, B} を算出する。
    """
    returns_map: Dict[str, List[float]] = {}
    for sym, summary in (market_summaries or {}).items():
        if not isinstance(summary, dict):
            continue
        h1 = summary.get("h1_indicators") or {}
        rets = h1.get("returns_1h") or summary.get("returns_1h") or []
        if isinstance(rets, list) and len(rets) >= 3:
            returns_map[sym.upper()] = [float(x) for x in rets]

    btc_rets = returns_map.get("BTC", [])
    betas: Dict[str, float] = {}
    corr_vs_btc: Dict[str, float] = {}
    pairwise_corr: Dict[str, Dict[str, float]] = {}

    for sym_u in (market_summaries or {}).keys():
        su = sym_u.upper()
        pairwise_corr[su] = {su: 1.0}
        if su == "BTC":
            betas[su] = 1.0
            corr_vs_btc[su] = 1.0
            continue
        r_i = returns_map.get(su, [])
        n = min(len(r_i), len(btc_rets))
        if n >= 3:
            a = np.array(r_i[-n:], dtype=float)
            b = np.array(btc_rets[-n:], dtype=float)
            var_b = float(np.var(b))
            std_a = float(np.std(a))
            std_b = float(np.std(b))
            if var_b > 1e-12:
                cov_ab = float(np.mean((a - np.mean(a)) * (b - np.mean(b))))
                betas[su] = round(max(-3.0, min(5.0, cov_ab / var_b)), 4)
            else:
                betas[su] = 1.0
            if std_a > 1e-12 and std_b > 1e-12:
                rho = float(np.corrcoef(a, b)[0, 1])
                corr_vs_btc[su] = round(max(-1.0, min(1.0, rho)), 4)
            else:
                corr_vs_btc[su] = 0.5
        else:
            betas[su] = float((market_summaries.get(sym_u) or {}).get("beta_vs_btc", 1.1) or 1.1)
            corr_vs_btc[su] = float((market_summaries.get(sym_u) or {}).get("corr_vs_btc", 0.6) or 0.6)

    syms = [k.upper() for k in (market_summaries or {}).keys()]
    for i, s1 in enumerate(syms):
        for s2 in syms[i + 1 :]:
            r1 = returns_map.get(s1, [])
            r2 = returns_map.get(s2, [])
            n = min(len(r1), len(r2))
            if n >= 3:
                a1 = np.array(r1[-n:], dtype=float)
                a2 = np.array(r2[-n:], dtype=float)
                if float(np.std(a1)) > 1e-12 and float(np.std(a2)) > 1e-12:
                    rho_12 = float(np.corrcoef(a1, a2)[0, 1])
                    rho_val = round(max(-1.0, min(1.0, rho_12)), 4)
                else:
                    rho_val = 0.5
            else:
                rho_val = round(min(0.95, max(0.0, corr_vs_btc.get(s1, 0.6) * corr_vs_btc.get(s2, 0.6))), 4)
            pairwise_corr.setdefault(s1, {})[s2] = rho_val
            pairwise_corr.setdefault(s2, {})[s1] = rho_val

    return {
        "betas_vs_btc": betas,
        "corr_vs_btc": corr_vs_btc,
        "pairwise_corr": pairwise_corr,
    }


def summarize_1h_indicators(df_1h: pd.DataFrame | None) -> Dict[str, Any]:
    """
    1時間足 DataFrame から24時間常時監視・押し目検知レーダー用の指標辞書を算出する。
    """
    if df_1h is None or df_1h.empty:
        return {}
    h1_ind = calculate_indicators(df_1h)
    if h1_ind.empty:
        return {}

    latest = h1_ind.iloc[-1]
    prev = h1_ind.iloc[-2] if len(h1_ind) >= 2 else latest
    confirmed = prev if len(h1_ind) >= 2 else latest

    close_p = float(latest["close"])
    open_p = float(latest["open"])
    high_p = float(latest["high"])
    low_p = float(latest["low"])
    prev_close = float(prev["close"])
    change_1h_pct = ((close_p - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

    rsi_14 = float(latest["rsi_14"])
    prev_rsi_14 = float(prev["rsi_14"])
    ema_12 = float(latest["ema_12"])
    ema_26 = float(latest["ema_26"])
    sma_25 = float(latest["sma_25"])
    bb_upper = float(latest["bb_upper"])
    bb_mid = float(latest["bb_mid"])
    bb_lower = float(latest["bb_lower"])
    macd_hist = float(latest["macd_hist"])
    prev_macd_hist = float(prev["macd_hist"])
    atr_14 = max(float(confirmed["atr_14"]), float(latest["atr_14"]))
    atr_20 = max(float(confirmed.get("atr_20", atr_14)), float(latest.get("atr_20", atr_14)))
    adx_14 = float(latest.get("adx_14", 20.0))
    bksr = float(latest.get("bksr", 1.0))
    ema_50 = float(latest.get("ema_50", sma_25))
    vol_ratio = max(float(confirmed["volume_ratio_20"]), float(latest["volume_ratio_20"]))

    bksr_info = compute_bksr_squeeze(df_1h)
    wtr_info = compute_wick_toxicity_ratio(df_1h)
    returns_1h = (
        h1_ind["close"].astype(float).pct_change().dropna().tail(24).round(6).tolist()
        if len(h1_ind) >= 2
        else []
    )

    window_24 = h1_ind.iloc[-24:]
    high_24h = float(window_24["high"].max())
    low_24h = float(window_24["low"].min())
    pullback_from_high_pct = (
        ((high_24h - close_p) / high_24h * 100.0) if high_24h > 0 else 0.0
    )

    # 1時間足の押し目・反発シグナル判定
    rsi_rebound = bool((prev_rsi_14 <= 45.0 and rsi_14 > prev_rsi_14) or (30.0 <= rsi_14 <= 46.0))
    ema_support_bounce = bool(
        sma_25 > 0
        and (low_p <= sma_25 * 1.006 or low_p <= ema_26 * 1.006)
        and (close_p >= sma_25 * 0.995 or close_p > open_p)
    )
    bb_lower_bounce = bool(
        bb_lower > 0
        and (low_p <= bb_lower * 1.008 or float(prev["low"]) <= float(prev["bb_lower"]) * 1.008)
        and close_p >= bb_lower
    )
    volume_surge = bool(vol_ratio >= 1.25 and close_p >= open_p)

    return sanitize_json_value(
        {
            "open": round(open_p, 6),
            "high": round(high_p, 6),
            "low": round(low_p, 6),
            "close": round(close_p, 6),
            "change_1h_pct": round(change_1h_pct, 2),
            "rsi_14": round(rsi_14, 2),
            "prev_rsi_14": round(prev_rsi_14, 2),
            "ema_12": round(ema_12, 6),
            "ema_26": round(ema_26, 6),
            "ema_50": round(ema_50, 6),
            "sma_25": round(sma_25, 6),
            "bb_upper": round(bb_upper, 6),
            "bb_mid": round(bb_mid, 6),
            "bb_lower": round(bb_lower, 6),
            "macd_hist": round(macd_hist, 6),
            "macd_hist_rising": bool(macd_hist > prev_macd_hist),
            "atr_14": round(atr_14, 6),
            "atr_20": round(atr_20, 6),
            "adx_14": round(adx_14, 2),
            "bksr": round(bksr, 4),
            "is_squeeze": bool(bksr_info["is_squeeze"]),
            "squeeze_breakout_up": bool(bksr_info["squeeze_breakout_up"]),
            "upper_wick_ratio": wtr_info["upper_wick_ratio"],
            "lower_wick_ratio": wtr_info["lower_wick_ratio"],
            "wtr_net_score": wtr_info["wtr_net_score"],
            "returns_1h": returns_1h,
            "volume_ratio_20": round(vol_ratio, 2),
            "pullback_from_24h_high_pct": round(pullback_from_high_pct, 2),
            "rsi_rebound": rsi_rebound,
            "ema_support_bounce": ema_support_bounce,
            "bb_lower_bounce": bb_lower_bounce,
            "volume_surge": volume_surge,
        }
    )


def summarize_indicators(
    symbol: str,
    df_daily: pd.DataFrame,
    df_4h: pd.DataFrame | None = None,
    ticker: Dict[str, Any] | None = None,
    orderbook: Dict[str, Any] | None = None,
    btc_return_7d_pct: float | None = None,
    df_1h: pd.DataFrame | None = None,
) -> Dict[str, Any]:
    """
    日足・4時間足のテクニカル指標と板・Ticker情報を統合し、
    AI判定プロンプトおよびルールベース判定で使いやすい辞書形式にまとめる。
    - 17:00時点の未確定日足による出来高過小評価バグを防ぐため、
      `volume_ratio_20` は「前日までの確定日足」および「確定済みの直近4時間足」を基準に算出する。
    - 各銘柄の「直近7日間リターン（%）」と「対BTCの7日間相対強度（銘柄7d騰落率 - BTC7d騰落率）」を算出する。
    - 直近4時間足が陰線（下落）かつ出来高比率2.0倍以上の場合は `climax_sell_warning: True` を付与する。
    """
    daily_ind = calculate_indicators(df_daily)
    if daily_ind.empty:
        raise ValueError(f"銘柄 {symbol} の日足データが空のため指標を算出できません。")

    latest = daily_ind.iloc[-1]
    prev = daily_ind.iloc[-2] if len(daily_ind) >= 2 else latest

    close_price = float(latest["close"])
    prev_close = float(prev["close"])
    change_1d_pct = ((close_price - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

    last_price = float(ticker["last"]) if ticker and float(ticker.get("last", 0) or 0) > 0 else close_price

    # 直近7日間リターン (%) と 対BTC 7日間相対強度 (%)
    if len(daily_ind) >= 8:
        close_7d_ago = float(daily_ind.iloc[-8]["close"])
    elif len(daily_ind) >= 2:
        close_7d_ago = float(daily_ind.iloc[0]["close"])
    else:
        close_7d_ago = close_price
    return_7d_pct = ((last_price - close_7d_ago) / close_7d_ago * 100.0) if close_7d_ago > 0 else 0.0

    if symbol.upper() == "BTC":
        relative_strength_vs_btc_7d_pct = 0.0
    elif btc_return_7d_pct is not None:
        relative_strength_vs_btc_7d_pct = return_7d_pct - float(btc_return_7d_pct)
    else:
        relative_strength_vs_btc_7d_pct = 0.0

    sma_7 = float(latest["sma_7"])
    sma_25 = float(latest["sma_25"])
    sma_75 = float(latest["sma_75"])
    rsi_14 = float(latest["rsi_14"])
    macd = float(latest["macd"])
    macd_signal = float(latest["macd_signal"])
    macd_hist = float(latest["macd_hist"])
    prev_macd_hist = float(prev["macd_hist"])
    bb_upper = float(latest["bb_upper"])
    bb_mid = float(latest["bb_mid"])
    bb_lower = float(latest["bb_lower"])

    # ATR(14) と 出来高比率(volume_ratio_20) は未確定日足（17:00時点の当日足）のノイズを排除し、
    # 前日までの確定日足 (daily_ind.iloc[:-1]) を基準に算出する
    if len(daily_ind) >= 2:
        confirmed_daily = daily_ind.iloc[:-1]
        confirmed_last_row = confirmed_daily.iloc[-1]
        confirmed_prev_vols = confirmed_daily["volume"].iloc[:-1].tail(20)
        if len(confirmed_prev_vols) == 0:
            confirmed_prev_vols = confirmed_daily["volume"].tail(20)
        confirmed_daily_avg_vol = float(confirmed_prev_vols.mean())
        confirmed_daily_vol = float(confirmed_last_row["volume"])
        confirmed_daily_vol_ratio = (
            confirmed_daily_vol / confirmed_daily_avg_vol if confirmed_daily_avg_vol > 0 else 1.0
        )
        # 日足ATRも確定日足ベースと最新足ベースの大きい方を採用し過小評価を防ぐ
        atr_14 = max(float(confirmed_last_row["atr_14"]), float(latest["atr_14"]))
    else:
        confirmed_daily_vol_ratio = float(latest["volume_ratio_20"])
        atr_14 = float(latest["atr_14"])

    vol_ratio = confirmed_daily_vol_ratio

    # 日足トレンド判定
    if close_price > sma_25 > sma_75:
        daily_trend = "STRONG_UPTREND"
    elif close_price > sma_25:
        daily_trend = "UPTREND"
    elif close_price < sma_25 < sma_75:
        daily_trend = "STRONG_DOWNTREND"
    else:
        daily_trend = "DOWNTREND_OR_RANGE"

    # 4時間足の補助指標（確定済み直近4時間足の出来高比率・クライマックス売り警告・24h高値/安値を含む）
    h4_summary: Dict[str, Any] = {}
    high_24h_klines = float(latest["high"])
    low_24h_klines = float(latest["low"])
    climax_sell_warning = False
    confirmed_h4_vol_ratio = confirmed_daily_vol_ratio

    if df_4h is not None and not df_4h.empty:
        h4_ind = calculate_indicators(df_4h)
        h4_latest = h4_ind.iloc[-1]
        h4_prev = h4_ind.iloc[-2] if len(h4_ind) >= 2 else h4_latest

        # 最新4h足が形成途中の未確定足（開始から4時間未満）かどうかを判定
        is_latest_h4_unclosed = False
        h4_elapsed_ms = 0.0
        if len(h4_ind) >= 2:
            now_ms = datetime.now(JST).timestamp() * 1000.0
            last_open_ms: float | None = None
            if "open_time" in h4_ind.columns:
                try:
                    val_ot = float(h4_latest["open_time"])
                    if not np.isnan(val_ot) and val_ot > 0:
                        last_open_ms = val_ot
                except Exception:
                    last_open_ms = None
            if last_open_ms is None and "timestamp" in h4_ind.columns:
                try:
                    ts_val = pd.Timestamp(h4_latest["timestamp"])
                    if ts_val.tzinfo is None:
                        ts_val = ts_val.tz_localize(JST)
                    last_open_ms = ts_val.timestamp() * 1000.0
                except Exception:
                    last_open_ms = None

            if last_open_ms is not None and 0.0 <= (now_ms - last_open_ms) < (4 * 3600 * 1000):
                is_latest_h4_unclosed = True
                h4_elapsed_ms = now_ms - last_open_ms
            else:
                # タイムスタンプが過去日のモック/テストデータであっても、最新足が開始直後（前足比出来高25%未満かつ値動き極小）なら未確定足として扱う
                prev_v = float(h4_prev["volume"])
                curr_v = float(h4_latest["volume"])
                prev_c = max(1.0, float(h4_prev["close"]))
                curr_c = float(h4_latest["close"])
                if prev_v > 0 and curr_v < prev_v * 0.25 and abs(curr_c - prev_c) / prev_c < 0.002:
                    is_latest_h4_unclosed = True
                    h4_elapsed_ms = 5000.0

        # 確定済みの直近4時間足と過去20本（確定足）平均出来高の算出
        if is_latest_h4_unclosed and len(h4_ind) >= 2:
            h4_confirmed_df = h4_ind.iloc[:-1]
            h4_confirmed_bar = h4_confirmed_df.iloc[-1]
            h4_base_vols = h4_confirmed_df["volume"].iloc[:-1].tail(20)
            if len(h4_base_vols) == 0:
                h4_base_vols = h4_confirmed_df["volume"].tail(20)
        else:
            h4_confirmed_df = h4_ind
            h4_confirmed_bar = h4_latest
            h4_base_vols = h4_ind["volume"].iloc[:-1].tail(20)
            if len(h4_base_vols) == 0:
                h4_base_vols = h4_ind["volume"].tail(20)

        h4_avg_vol_20 = float(h4_base_vols.mean()) if len(h4_base_vols) > 0 else 0.0
        confirmed_bar_vol = float(h4_confirmed_bar["volume"])
        latest_bar_vol = float(h4_latest["volume"])

        confirmed_h4_vol_ratio = (confirmed_bar_vol / h4_avg_vol_20) if h4_avg_vol_20 > 0 else 1.0
        latest_h4_vol_ratio = (latest_bar_vol / h4_avg_vol_20) if h4_avg_vol_20 > 0 else 1.0

        # 直近4時間足が陰線（close < open）かつ出来高比率が2.0倍以上の場合は climax_sell_warning: True
        confirmed_is_bearish = float(h4_confirmed_bar["close"]) < float(h4_confirmed_bar["open"])
        latest_is_bearish = float(h4_latest["close"]) < float(h4_latest["open"])
        if (confirmed_is_bearish and confirmed_h4_vol_ratio >= 2.0) or (
            latest_is_bearish and latest_h4_vol_ratio >= 2.0
        ):
            climax_sell_warning = True

        # 表示用の4h出来高比率は確定済み直近4時間足を基本とし、現在進行中の足が既に上回っている場合はその値を反映
        effective_h4_vol_ratio = (
            max(confirmed_h4_vol_ratio, latest_h4_vol_ratio)
            if is_latest_h4_unclosed
            else confirmed_h4_vol_ratio
        )

        # 17:00定時実行時に新しい4時間足（17:00足）が開始直後（数秒経過）の場合でも、
        # 直前の確定4時間足（13:00〜17:00足）および4時間前比の騰落率がゼロ化されないよう算出
        h4_close = float(h4_latest["close"])
        h4_prev_close = float(h4_prev["close"])
        h4_confirmed_close = float(h4_confirmed_bar["close"])
        h4_confirmed_open = float(h4_confirmed_bar["open"])
        if len(h4_confirmed_df) >= 2:
            confirmed_prev_close = float(h4_confirmed_df.iloc[-2]["close"])
        else:
            confirmed_prev_close = h4_confirmed_open

        confirmed_change_from_prev_pct = (
            ((h4_confirmed_close - confirmed_prev_close) / confirmed_prev_close * 100.0)
            if confirmed_prev_close > 0
            else 0.0
        )
        confirmed_intrabar_change_pct = (
            ((h4_confirmed_close - h4_confirmed_open) / h4_confirmed_open * 100.0)
            if h4_confirmed_open > 0
            else 0.0
        )
        confirmed_change_4h_pct = (
            confirmed_intrabar_change_pct
            if abs(confirmed_intrabar_change_pct) > abs(confirmed_change_from_prev_pct)
            else confirmed_change_from_prev_pct
        )
        rolling_4h_change_pct = (
            ((h4_close - confirmed_prev_close) / confirmed_prev_close * 100.0)
            if confirmed_prev_close > 0
            else confirmed_change_4h_pct
        )
        unclosed_bar_change_pct = (
            ((h4_close - h4_prev_close) / h4_prev_close * 100.0) if h4_prev_close > 0 else 0.0
        )

        if is_latest_h4_unclosed:
            if h4_elapsed_ms < (60 * 60 * 1000):
                # 17:00定時実行など新4h足の開始1時間以内は、直前4時間（13:00〜17:00）の実変動率を採用
                change_4h_pct = (
                    confirmed_change_4h_pct
                    if abs(confirmed_change_4h_pct) > abs(rolling_4h_change_pct)
                    else rolling_4h_change_pct
                )
                h4_open_display = h4_confirmed_open
            else:
                change_4h_pct = (
                    unclosed_bar_change_pct
                    if abs(unclosed_bar_change_pct) >= abs(rolling_4h_change_pct)
                    else rolling_4h_change_pct
                )
                h4_open_display = float(h4_latest["open"])
        else:
            change_4h_pct = confirmed_change_4h_pct
            h4_open_display = h4_confirmed_open

        confirmed_rsi_14 = float(h4_confirmed_bar["rsi_14"])
        latest_rsi_14 = float(h4_latest["rsi_14"])
        effective_h4_rsi = (
            confirmed_rsi_14
            if (is_latest_h4_unclosed and h4_elapsed_ms < (30 * 60 * 1000))
            else latest_rsi_14
        )
        effective_h4_atr = max(float(h4_confirmed_bar["atr_14"]), float(h4_latest["atr_14"]))

        window_bars = 7 if is_latest_h4_unclosed else 6
        h4_window_24h = h4_ind.iloc[-window_bars:]
        high_24h_klines = float(h4_window_24h["high"].max())
        low_24h_klines = float(h4_window_24h["low"].min())

        h4_summary = {
            "open": round(h4_open_display, 6),
            "close": round(h4_close, 6),
            "change_4h_pct": round(change_4h_pct, 2),
            "confirmed_change_4h_pct": round(confirmed_change_4h_pct, 2),
            "rolling_4h_change_pct": round(rolling_4h_change_pct, 2),
            "rsi_14": round(effective_h4_rsi, 2),
            "confirmed_rsi_14": round(confirmed_rsi_14, 2),
            "macd_hist": round(float(h4_confirmed_bar["macd_hist"] if (is_latest_h4_unclosed and h4_elapsed_ms < 1800000) else h4_latest["macd_hist"]), 6),
            "atr_14": round(effective_h4_atr, 6),
            "sma_25": round(float(h4_latest["sma_25"]), 6),
            "volume_ratio_20": round(effective_h4_vol_ratio, 2),
            "confirmed_h4_volume_ratio_20": round(confirmed_h4_vol_ratio, 2),
            "climax_sell_warning": bool(climax_sell_warning),
            "high_24h": round(high_24h_klines, 6),
            "low_24h": round(low_24h_klines, 6),
        }

    # 直近20日間の高値・安値（サポート・レジスタンス目安）
    recent_window = daily_ind.iloc[-20:]
    recent_high_20d = float(recent_window["high"].max())
    recent_low_20d = float(recent_window["low"].min())

    best_ask = float(ticker["ask"]) if ticker and ticker.get("ask") else close_price
    best_bid = float(ticker["bid"]) if ticker and ticker.get("bid") else close_price
    ticker_high = float(ticker.get("high", 0) or 0) if ticker else 0.0
    ticker_low = float(ticker.get("low", 0) or 0) if ticker else 0.0
    high_24h = max(high_24h_klines, ticker_high) if ticker_high > 0 else high_24h_klines
    low_24h = min(low_24h_klines, ticker_low) if ticker_low > 0 else low_24h_klines
    spread_pct = float(ticker.get("spread_pct", 0.0)) if ticker else 0.0
    ob_imbalance = float(orderbook.get("orderbook_imbalance", 0.0)) if orderbook else 0.0
    bid_depth_1pct_jpy = float(orderbook.get("bid_depth_1pct_jpy", 0.0)) if orderbook else 0.0
    ask_depth_1pct_jpy = float(orderbook.get("ask_depth_1pct_jpy", 0.0)) if orderbook else 0.0

    h1_summary = summarize_1h_indicators(df_1h)
    ob_micro = compute_orderbook_microstructure(orderbook, last_price=last_price)
    trades_list = (orderbook or {}).get("trades") if isinstance(orderbook, dict) else None
    tfi_info = compute_trade_flow_and_absorption(trades=trades_list, df_1h=df_1h)
    bksr_source_df = df_1h if (df_1h is not None and not df_1h.empty) else (df_4h if (df_4h is not None and not df_4h.empty) else df_daily)
    bksr_info = compute_bksr_squeeze(bksr_source_df)
    wtr_info = compute_wick_toxicity_ratio(bksr_source_df, tfi_15m=float(tfi_info["tfi_15m"]))
    obdr_info = compute_obdr_slippage(orderbook, order_notional_jpy=9_011.0)
    microstructure = {
        **ob_micro,
        **tfi_info,
        **bksr_info,
        **wtr_info,
        **obdr_info,
    }

    # M-7: NaN / Infinity が latest_decision.json へ伝播してブラウザの JSON.parse が壊れないよう再帰サニタイズ
    return sanitize_json_value({
        "symbol": symbol,
        "latest_candle_jst": str(latest.get("datetime_jst", "")),
        "climax_sell_warning": bool(climax_sell_warning),
        "price": {
            "last": last_price,
            "best_ask": best_ask,
            "best_bid": best_bid,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "spread_pct": spread_pct,
            "change_1d_pct": round(change_1d_pct, 2),
            "return_7d_pct": round(return_7d_pct, 2),
            "change_7d_pct": round(return_7d_pct, 2),
            "relative_strength_vs_btc_7d_pct": round(relative_strength_vs_btc_7d_pct, 2),
            "recent_high_20d": recent_high_20d,
            "recent_low_20d": recent_low_20d,
        },
        "daily_indicators": {
            "trend": daily_trend,
            "sma_7": round(sma_7, 6),
            "sma_25": round(sma_25, 6),
            "sma_75": round(sma_75, 6),
            "rsi_14": round(rsi_14, 2),
            "macd": round(macd, 6),
            "macd_signal": round(macd_signal, 6),
            "macd_hist": round(macd_hist, 6),
            "macd_hist_rising": bool(macd_hist > prev_macd_hist),
            "bb_upper": round(bb_upper, 6),
            "bb_mid": round(bb_mid, 6),
            "bb_lower": round(bb_lower, 6),
            "atr_14": round(atr_14, 6),
            "atr_pct": round((atr_14 / last_price * 100.0) if last_price > 0 else 0.0, 2),
            "volume_ratio_20": round(vol_ratio, 2),
            "confirmed_daily_volume_ratio_20": round(confirmed_daily_vol_ratio, 2),
            "confirmed_h4_volume_ratio_20": round(confirmed_h4_vol_ratio, 2),
            "return_7d_pct": round(return_7d_pct, 2),
            "relative_strength_vs_btc_7d_pct": round(relative_strength_vs_btc_7d_pct, 2),
            "climax_sell_warning": bool(climax_sell_warning),
        },
        "h4_indicators": h4_summary,
        "h1_indicators": h1_summary,
        "microstructure": microstructure,
        "orderbook": {
            "asks": list((orderbook or {}).get("asks", [])[:10]),
            "bids": list((orderbook or {}).get("bids", [])[:10]),
        },
        "orderbook_liquidity": {
            "bid_depth_1pct_jpy": round(bid_depth_1pct_jpy, 0),
            "ask_depth_1pct_jpy": round(ask_depth_1pct_jpy, 0),
            "imbalance_snapshot": ob_imbalance,
            "obi_5": microstructure["obi_5"],
            "obdr_slippage_pct": microstructure["obdr_slippage_pct"],
            "is_hollow_book": microstructure["is_hollow_book"],
            "support_wall_price": microstructure["support_wall_price"],
            "support_wall_front_run_price": microstructure["support_wall_front_run_price"],
            "note": (
                "板インバランスは5段階指数減衰加重(OBI_5)およびOBDRスリッページ耐性(9,011円成行売却時の仲値乖離率)と"
                "併せて評価し、0.45%超の中抜き板(Hollow Book)は排除すること。"
            ),
        },
        "orderbook_imbalance": ob_imbalance,
    })


def enrich_relative_strength_vs_btc(market_summaries: Dict[str, Dict[str, Any]]) -> None:
    """
    全銘柄の要約データに対し、BTCの7日間リターンを基準とした
    「対BTCの7日間相対強度（銘柄7d騰落率 - BTC7d騰落率）」および
    1時間足リターンに基づく「対BTCベータ値・相関係数マトリクス」を一括計算・反映する。
    """
    if not market_summaries or "BTC" not in market_summaries:
        return
    btc_summary = market_summaries["BTC"]
    btc_7d = float(
        btc_summary.get("price", {}).get(
            "return_7d_pct",
            btc_summary.get("daily_indicators", {}).get("return_7d_pct", 0.0),
        )
        or 0.0
    )
    beta_corr_info = compute_btc_beta_and_correlation_matrix(market_summaries)
    betas_map = beta_corr_info.get("betas_vs_btc", {})
    corr_btc_map = beta_corr_info.get("corr_vs_btc", {})
    pairwise_map = beta_corr_info.get("pairwise_corr", {})

    for sym, summary in market_summaries.items():
        if not isinstance(summary, dict):
            continue
        su = sym.upper()
        p_info = summary.get("price") or {}
        d_info = summary.get("daily_indicators") or {}
        sym_7d = float(p_info.get("return_7d_pct", d_info.get("return_7d_pct", 0.0)) or 0.0)
        rel_val = 0.0 if su == "BTC" else round(sym_7d - btc_7d, 2)
        if "price" in summary and isinstance(summary["price"], dict):
            summary["price"]["relative_strength_vs_btc_7d_pct"] = rel_val
        if "daily_indicators" in summary and isinstance(summary["daily_indicators"], dict):
            summary["daily_indicators"]["relative_strength_vs_btc_7d_pct"] = rel_val
        summary["relative_strength_vs_btc_7d_pct"] = rel_val
        summary["beta_vs_btc"] = float(betas_map.get(su, 1.0))
        summary["corr_vs_btc"] = float(corr_btc_map.get(su, 1.0 if su == "BTC" else 0.6))
        summary["pairwise_corr"] = dict(pairwise_map.get(su, {su: 1.0}))
