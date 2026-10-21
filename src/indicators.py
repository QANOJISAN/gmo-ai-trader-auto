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

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

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

    # 5. ATR (Average True Range, 14期間)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["atr_14"] = true_range.ewm(alpha=1.0 / 14.0, min_periods=1, adjust=False).mean()

    # 6. 出来高移動平均・変化率（直前までの確定足20本平均を基準とする）
    prev_vol_sma_20 = volume.shift(1).rolling(window=20, min_periods=1).mean()
    fallback_vol_sma_20 = volume.rolling(window=20, min_periods=1).mean()
    vol_sma_20 = prev_vol_sma_20.fillna(fallback_vol_sma_20)
    out["volume_ratio_20"] = np.where(vol_sma_20 > 0, volume / vol_sma_20, 1.0)

    return out


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
    vol_ratio = max(float(confirmed["volume_ratio_20"]), float(latest["volume_ratio_20"]))

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
            "sma_25": round(sma_25, 6),
            "bb_upper": round(bb_upper, 6),
            "bb_mid": round(bb_mid, 6),
            "bb_lower": round(bb_lower, 6),
            "macd_hist": round(macd_hist, 6),
            "macd_hist_rising": bool(macd_hist > prev_macd_hist),
            "atr_14": round(atr_14, 6),
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
        "h1_indicators": summarize_1h_indicators(df_1h),
        "orderbook_liquidity": {
            "bid_depth_1pct_jpy": round(bid_depth_1pct_jpy, 0),
            "ask_depth_1pct_jpy": round(ask_depth_1pct_jpy, 0),
            "imbalance_snapshot": ob_imbalance,
            "note": (
                "板インバランスは17時時点の一瞬のスナップショットであり見せ板ノイズを含むため、"
                "方向予測の主根拠にせず「±1%圏内の板の厚み（流動性・スリッページ確認）」として扱うこと。"
            ),
        },
        "orderbook_imbalance": ob_imbalance,
    })


def enrich_relative_strength_vs_btc(market_summaries: Dict[str, Dict[str, Any]]) -> None:
    """
    全銘柄の要約データに対し、BTCの7日間リターンを基準とした
    「対BTCの7日間相対強度（銘柄7d騰落率 - BTC7d騰落率）」を一括計算・反映する。
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
    for sym, summary in market_summaries.items():
        p_info = summary.get("price") or {}
        d_info = summary.get("daily_indicators") or {}
        sym_7d = float(p_info.get("return_7d_pct", d_info.get("return_7d_pct", 0.0)) or 0.0)
        rel_val = 0.0 if sym.upper() == "BTC" else round(sym_7d - btc_7d, 2)
        if "price" in summary and isinstance(summary["price"], dict):
            summary["price"]["relative_strength_vs_btc_7d_pct"] = rel_val
        if "daily_indicators" in summary and isinstance(summary["daily_indicators"], dict):
            summary["daily_indicators"]["relative_strength_vs_btc_7d_pct"] = rel_val
        summary["relative_strength_vs_btc_7d_pct"] = rel_val
