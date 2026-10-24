"""
最小注文数量・呼値丸め・資金上限・最大損失補正モジュール (validator.py)

GMOコイン「取引所（現物）」の注文ルール（最小注文数量 minOrderSize、数量刻み sizeStep、
呼値 tickSize）、資金配分上限（デフォルト1銘柄あたり50%＝5,000円、最大同時2銘柄）、
および厳格化されたリスク管理ルールに適合するようAI判定結果を検証・丸め補正する:
1. 相場急変時の暴落サーキットブレーカー（BTC 4h騰落率 <= -4.0% または 4h RSI <= 22）
2. ドローダウン・キルスイッチ（総資産 < 8,500円［初期資金比-15%］で新規BUYを強制HOLD）
3. 取引コスト負け排除（「(スプレッド + Taker手数料) ÷ SL距離」が 10% を超える場合はHOLD）
4. 1トレードの名目許容損失上限「min(150円, 総資産の1.5%)」からの数量逆算 ＆
   Taker手数料（0.05%〜0.09%）＋スリッページ予備込みの実損失 200円以内保証
5. 2枠目新規BUY時のポートフォリオ合計リスク上限（1枠目と合わせて総資産の3.0%＝初期300円以内）
浮動小数点誤差を防ぐため `decimal.Decimal` による演算を使用する。
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# GMOコイン 取引所（現物）標準ルールのフォールバック定義（M-1: 動的スクリーニング対象銘柄を網羅）
DEFAULT_SYMBOL_RULES: Dict[str, Dict[str, str]] = {
    "BTC": {
        "symbol": "BTC",
        "min_order_size": "0.00001",
        "max_order_size": "5",
        "size_step": "0.00001",
        "tick_size": "1",
        "taker_fee": "0.0005",
        "maker_fee": "-0.0001",
    },
    "ETH": {
        "symbol": "ETH",
        "min_order_size": "0.001",
        "max_order_size": "100",
        "size_step": "0.0001",
        "tick_size": "1",
        "taker_fee": "0.0005",
        "maker_fee": "-0.0001",
    },
    "XRP": {
        "symbol": "XRP",
        "min_order_size": "1",
        "max_order_size": "100000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0005",
        "maker_fee": "-0.0001",
    },
    "SOL": {
        "symbol": "SOL",
        "min_order_size": "0.01",
        "max_order_size": "500",
        "size_step": "0.01",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "DOGE": {
        "symbol": "DOGE",
        "min_order_size": "10",
        "max_order_size": "200000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "SUI": {
        "symbol": "SUI",
        "min_order_size": "0.1",
        "max_order_size": "30000",
        "size_step": "0.1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "XLM": {
        "symbol": "XLM",
        "min_order_size": "1",
        "max_order_size": "100000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "ADA": {
        "symbol": "ADA",
        "min_order_size": "1",
        "max_order_size": "100000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "DOT": {
        "symbol": "DOT",
        "min_order_size": "0.1",
        "max_order_size": "10000",
        "size_step": "0.1",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "ATOM": {
        "symbol": "ATOM",
        "min_order_size": "0.01",
        "max_order_size": "10000",
        "size_step": "0.01",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "BCH": {
        "symbol": "BCH",
        "min_order_size": "0.01",
        "max_order_size": "200",
        "size_step": "0.001",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "LTC": {
        "symbol": "LTC",
        "min_order_size": "0.1",
        "max_order_size": "1000",
        "size_step": "0.01",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "LINK": {
        "symbol": "LINK",
        "min_order_size": "0.1",
        "max_order_size": "10000",
        "size_step": "0.1",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "AVAX": {
        "symbol": "AVAX",
        "min_order_size": "0.1",
        "max_order_size": "5000",
        "size_step": "0.1",
        "tick_size": "1",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "RENDER": {
        "symbol": "RENDER",
        "min_order_size": "0.1",
        "max_order_size": "10000",
        "size_step": "0.1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "MONA": {
        "symbol": "MONA",
        "min_order_size": "1",
        "max_order_size": "50000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "XYM": {
        "symbol": "XYM",
        "min_order_size": "1",
        "max_order_size": "500000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "QTUM": {
        "symbol": "QTUM",
        "min_order_size": "0.1",
        "max_order_size": "20000",
        "size_step": "0.1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "DAI": {
        "symbol": "DAI",
        "min_order_size": "1",
        "max_order_size": "100000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0005",
        "maker_fee": "-0.0001",
    },
    "ASTR": {
        "symbol": "ASTR",
        "min_order_size": "10",
        "max_order_size": "500000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "FCR": {
        "symbol": "FCR",
        "min_order_size": "10",
        "max_order_size": "1000000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "NAC": {
        "symbol": "NAC",
        "min_order_size": "0.1",
        "max_order_size": "50000",
        "size_step": "0.1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
    "WILD": {
        "symbol": "WILD",
        "min_order_size": "1",
        "max_order_size": "100000",
        "size_step": "1",
        "tick_size": "0.001",
        "taker_fee": "0.0009",
        "maker_fee": "-0.0003",
    },
}


def _to_decimal(
    val: Any,
    default: Decimal = Decimal("0"),
) -> Decimal:
    """数値を安全に有限の Decimal へ変換する（None・空文字・bool・カンマ・通貨記号・NaN・Infinity を完全防御）"""
    if val is None or isinstance(val, bool):
        return default
    if isinstance(val, Decimal):
        return val if val.is_finite() else default
    if isinstance(val, int):
        return Decimal(val)
    if isinstance(val, float):
        if not math.isfinite(val):
            return default
        return Decimal(str(val))
    s = str(val).strip()
    if not s:
        return default
    for ch in (",", " ", "\u3000", "円", "¥", "￥", "$"):
        s = s.replace(ch, "")
    if not s:
        return default
    try:
        d = Decimal(s)
        return d if d.is_finite() else default
    except Exception:
        return default


# AI判定アクションのホワイトリスト（M-1: 未知のアクションは必ず HOLD へ正規化）
VALID_ACTIONS = frozenset({"BUY", "SELL", "MODIFY", "HOLD"})


def normalize_action(val: Any) -> str:
    """アクション文字列を大文字化し、互換の MODIFY_OCO を MODIFY へ、未知の値を HOLD へ正規化する"""
    act = str(val if val is not None else "HOLD").strip().upper()
    if act == "MODIFY_OCO":
        act = "MODIFY"
    return act if act in VALID_ACTIONS else "HOLD"


def normalize_confidence(val: Any) -> float:
    """
    確信度を [0.0, 1.0] に正規化する（M-2）。
    `85` や `85.0` のような 0〜100 の百分率表記は 100 で割って小数へ変換し、最終的に必ずクランプする。
    """
    f = float(_to_decimal(val))
    if f <= 0:
        return 0.0
    if f > 1.0:
        f = f / 100.0
    return round(min(1.0, max(0.0, f)), 4)


def _format_number(val: Decimal) -> Union[int, float]:
    """Decimal を JSON シリアライズしやすい int または float に変換する"""
    normalized = val.normalize()
    if normalized == normalized.to_integral():
        return int(normalized)
    return float(val)


def format_decimal_plain(val: Union[str, int, float, Decimal]) -> str:
    """
    指数表記 ('1e-05' や '1E-5') を一切使わず、
    通常の小数または整数文字列 ('0.00001' や '13000000') に変換する。
    """
    if val is None or val == "":
        return "0"
    d = _to_decimal(val).normalize()
    if d == d.to_integral():
        return str(int(d))
    return format(d, "f")


def format_price_jpy(val: Union[str, int, float, Decimal]) -> str:
    """
    価格（円）を表示用文字列へ変換する。
    - 整数価格の銘柄（BTC, ETH, SOL等）はカンマ区切り整数（例: '13,079,360'）
    - 小数価格の銘柄（XRP, DOGE, ADA, XLM, SUI等）は小数点以下を保持（例: '12.605', '1,250.999'）
    """
    if val is None or val == "":
        return "0"
    d = _to_decimal(val).normalize()
    if d == d.to_integral():
        return f"{int(d):,}"
    plain = format(d, "f")
    if "." in plain:
        int_part, frac_part = plain.split(".", 1)
        sign = ""
        if int_part.startswith("-"):
            sign = "-"
            int_part = int_part[1:]
        return f"{sign}{int(int_part):,}.{frac_part}"
    return f"{int(d):,}"


def round_to_step(
    value: Union[str, int, float, Decimal],
    step: Union[str, int, float, Decimal],
    rounding: str = ROUND_DOWN,
) -> Decimal:
    """指定された刻み幅 (step) の整数倍に丸める"""
    d_val = _to_decimal(value)
    d_step = _to_decimal(step)
    if d_step <= 0:
        return d_val
    units = (d_val / d_step).to_integral_value(rounding=rounding)
    return units * d_step


def round_price_to_tick(
    price: Union[str, int, float, Decimal],
    tick_size: Union[str, int, float, Decimal],
    rounding: str = ROUND_HALF_UP,
) -> Union[int, float]:
    """価格を呼値 (tick_size) 単位に丸める"""
    rounded = round_to_step(price, tick_size, rounding=rounding)
    return _format_number(rounded)


def round_size_to_step(
    size: Union[str, int, float, Decimal],
    size_step: Union[str, int, float, Decimal],
) -> Union[int, float]:
    """注文数量を数量刻み (size_step) 単位で切り捨て丸め (ROUND_DOWN) する"""
    rounded = round_to_step(size, size_step, rounding=ROUND_DOWN)
    return _format_number(rounded)


def _hold_decision(adjusted: Dict[str, Any], symbol: str = "NONE", reason_prefix: str = "") -> Dict[str, Any]:
    """判定を HOLD（見送り）へ正規化して返すヘルパー（limit_price等もすべて0へリセット）"""
    if reason_prefix:
        adjusted["reasoning"] = f"{reason_prefix} " + str(adjusted.get("reasoning", ""))
    adjusted.update(
        {
            "action": "HOLD",
            "symbol": symbol,
            "order_type": "NONE",
            "limit_price": 0,
            "size": 0,
            "take_profit_price": 0,
            "stop_loss_price": 0,
            "expected_cost_jpy": 0,
            "expected_loss_jpy": 0,
            "risk_reward_ratio": 0.0,
        }
    )
    return adjusted


def calculate_total_equity_jpy(
    portfolio: Dict[str, Any],
    market_summaries: Dict[str, Dict[str, Any]] | None = None,
) -> Decimal:
    """
    総資産（日本円残高 ＋ 保有ポジションの時価評価額）を算出する。
    market_summaries に現在値 (price.last) がある場合は時価評価し、ない場合は current_price または entry_price を用いる。
    """
    jpy_balance = _to_decimal(portfolio.get("jpy_balance", 20000))
    positions: List[Dict[str, Any]] = portfolio.get("positions", []) or []
    summaries = market_summaries or {}

    holdings_value = Decimal("0")
    for pos in positions:
        p_sym = str(pos.get("symbol", "")).upper()
        p_size = _to_decimal(pos.get("size", 0) or 0)
        if p_size <= 0:
            continue
        mark_price_raw = (
            summaries.get(p_sym, {}).get("price", {}).get("last")
            or pos.get("current_price")
            or pos.get("entry_price", 0)
        )
        p_mark = _to_decimal(mark_price_raw or 0)
        holdings_value += p_mark * p_size

    return jpy_balance + holdings_value


def calculate_existing_portfolio_risk_jpy(
    positions: List[Dict[str, Any]],
    merged_rules: Dict[str, Dict[str, Any]],
    pending_orders: List[Dict[str, Any]] | None = None,
) -> Decimal:
    """
    現在保有中のポジション（および発注中の待機指値 `PENDING_BUY`）が抱えている想定SLリスク合計額（円）を算出する。
    SLが建値（entry_price）以上に引き上げられている建玉、および `DUST_QUARANTINED` 端数はダウンサイドリスク 0 円とみなす。
    """
    total_risk = Decimal("0")
    for pos in positions:
        if str(pos.get("state", "PROTECTED")).upper() == "DUST_QUARANTINED":
            continue
        p_sym = str(pos.get("symbol", "")).upper()
        p_size = _to_decimal(pos.get("size", 0) or 0)
        p_entry = _to_decimal(pos.get("entry_price", 0) or 0)
        p_sl = _to_decimal(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
        explicit_risk = _to_decimal(pos.get("expected_loss_jpy", 0) or pos.get("initial_risk_jpy", 0) or 0)

        if p_entry > 0 and p_sl >= p_entry:
            # 建値保護済みポジション
            continue
        if p_entry > 0 and Decimal("0") < p_sl < p_entry and p_size > 0:
            p_rule = merged_rules.get(p_sym, {})
            p_taker = max(_to_decimal(p_rule.get("taker_fee", "0.0005")), Decimal("0"))
            calc_risk = ((p_entry - p_sl) + (p_sl * p_taker)) * p_size
            total_risk += max(calc_risk, explicit_risk)
        elif explicit_risk > 0:
            total_risk += explicit_risk
        elif p_entry > 0 and p_size > 0:
            # SL未設定時のフォールバック（取得額の3%または150円）
            total_risk += min(p_entry * p_size * Decimal("0.03"), Decimal("150"))

    for ord_item in pending_orders or []:
        if str(ord_item.get("side", "BUY")).upper() != "BUY":
            continue
        if str(ord_item.get("state", "PENDING_BUY")).upper() not in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED"):
            continue
        o_sym = str(ord_item.get("symbol", "")).upper()
        o_size = _to_decimal(ord_item.get("size", 0) or 0)
        o_entry = _to_decimal(ord_item.get("limit_price", ord_item.get("price", 0)) or 0)
        o_sl = _to_decimal(ord_item.get("stop_loss_price") or ord_item.get("sl_price") or 0)
        o_explicit = _to_decimal(ord_item.get("initial_risk_jpy", ord_item.get("expected_loss_jpy", 0)) or 0)
        if o_entry > 0 and Decimal("0") < o_sl < o_entry and o_size > 0:
            o_rule = merged_rules.get(o_sym, {})
            o_taker = max(_to_decimal(o_rule.get("taker_fee", "0.0005")), Decimal("0"))
            o_calc_risk = ((o_entry - o_sl) + (o_sl * o_taker)) * o_size
            total_risk += max(o_calc_risk, o_explicit)
        elif o_explicit > 0:
            total_risk += o_explicit
        elif o_entry > 0 and o_size > 0:
            total_risk += min(o_entry * o_size * Decimal("0.03"), Decimal("150"))

    return total_risk


def check_btc_circuit_breaker(market_summaries: Dict[str, Dict[str, Any]]) -> tuple[bool, str]:
    """
    市場全体の主導銘柄である BTC の急落・パニック売り状態を検知するサーキットブレーカー:
    - BTC の直近4時間騰落率 (change_4h_pct / confirmed_change_4h_pct / rolling_4h_change_pct) が -4.0% 以下
    - または BTC の4時間足 RSI (rsi_14 / confirmed_rsi_14) が 22.0 以下
    のいずれかに該当する場合、(True, 理由メッセージ) を返す。
    """
    if not market_summaries or "BTC" not in market_summaries:
        return False, ""

    btc_summary = market_summaries.get("BTC") or {}
    btc_h4 = btc_summary.get("h4_indicators") or {}
    btc_price = btc_summary.get("price") or {}

    change_candidates: List[float] = []
    for k in ("change_4h_pct", "confirmed_change_4h_pct", "rolling_4h_change_pct"):
        v = btc_h4.get(k)
        if v is not None:
            try:
                change_candidates.append(float(v))
            except (TypeError, ValueError):
                pass
    if btc_price.get("change_4h_pct") is not None:
        try:
            change_candidates.append(float(btc_price["change_4h_pct"]))
        except (TypeError, ValueError):
            pass

    if not change_candidates and btc_h4.get("open") and btc_h4.get("close"):
        try:
            h4_open = float(btc_h4["open"])
            h4_close = float(btc_h4["close"])
            if h4_open > 0:
                change_candidates.append((h4_close - h4_open) / h4_open * 100.0)
        except (TypeError, ValueError):
            pass

    rsi_candidates: List[float] = []
    for k in ("rsi_14", "confirmed_rsi_14"):
        v = btc_h4.get(k)
        if v is not None:
            try:
                rsi_candidates.append(float(v))
            except (TypeError, ValueError):
                pass

    change_triggered = False
    rsi_triggered = False
    change_val = 0.0
    rsi_val = 50.0

    if change_candidates:
        change_val = min(change_candidates)
        if change_val <= -4.0:
            change_triggered = True

    if rsi_candidates:
        rsi_val = min(rsi_candidates)
        if rsi_val <= 22.0:
            rsi_triggered = True

    if change_triggered or rsi_triggered:
        reasons = []
        if change_triggered:
            reasons.append(f"BTC直近4時間騰落率 {change_val:+.2f}% (閾値 -4.0%以下)")
        if rsi_triggered:
            reasons.append(f"BTC 4時間足RSI {rsi_val:.1f} (閾値 22.0以下)")
        detail_str = " / ".join(reasons)
        msg = (
            f"[BTC急落による市場サーキットブレーカー発動] "
            f"市場主導銘柄BTCが急落・パニック売り状態（{detail_str}）にあるため、"
            "新規BUYエントリーを強制見送り（HOLD）へオーバーライドしました。"
        )
        return True, msg

    return False, ""


def validate_and_adjust_decision(
    decision: Dict[str, Any],
    portfolio: Dict[str, Any],
    symbol_rules: Dict[str, Dict[str, Any]],
    market_summaries: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    AIの判定結果に対して以下の検証・補正を実行する:
    0. 相場急変時の暴落サーキットブレーカー（BTC 4h騰落率 <= -4.0% または 4h RSI <= 22 で新規BUYをHOLDへ変更）
    0b. ドローダウン・キルスイッチ（総資産 < 8,500円 で新規BUYをHOLDへ強制変更）
    1. 銘柄ルール（tick_size, size_step, min_order_size）に基づく価格・数量の丸め処理 ＆ Post-Only best_bid 上限ガード
    2. 同時保有ポジション数上限（デフォルト最大2銘柄）および既保有銘柄の重複BUY防止チェック
    3. 取引コスト比率チェック：「(スプレッド + Taker手数料) ÷ SL距離」> 10% の場合は期待値マイナスとして HOLD へ変更
    4. 2枠目新規BUY時のポートフォリオ合計リスク上限（総資産の3.0%＝初期300円以内）の検証
    5. 1トレードの名目許容損失上限「min(150円, 総資産の1.5%)」での数量逆算と、
       Taker手数料＋スリッページ予備込みの実損失 200円以内保証
    """
    adjusted = dict(decision)
    action = str(adjusted.get("action", "HOLD")).strip().upper()
    if action == "MODIFY_OCO":
        action = "MODIFY"
    symbol = str(adjusted.get("symbol", "NONE")).strip().upper()

    merged_rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
    positions: List[Dict[str, Any]] = portfolio.get("positions", []) or []
    jpy_balance = _to_decimal(portfolio.get("jpy_balance", 20000))
    total_equity = calculate_total_equity_jpy(portfolio, market_summaries)
    raw_init_cap = portfolio.get("initial_capital_jpy", portfolio.get("initial_equity_jpy"))
    raw_ks = portfolio.get("drawdown_kill_switch_jpy")
    if raw_init_cap is not None:
        init_cap_val = _to_decimal(raw_init_cap)
    elif raw_ks is not None and _to_decimal(raw_ks) > Decimal("0"):
        init_cap_val = (_to_decimal(raw_ks) / Decimal("0.85")).quantize(Decimal("1"))
    else:
        init_cap_val = Decimal("20000")
    if init_cap_val <= Decimal("0"):
        init_cap_val = Decimal("20000")
    peak_equity_val = max(
        _to_decimal(portfolio.get("peak_equity_jpy", 0)),
        init_cap_val,
    )
    init_floor_jpy = max(Decimal("8500"), (init_cap_val * Decimal("0.85")).quantize(Decimal("0.01")))
    base_ks_jpy = _to_decimal(portfolio.get("drawdown_kill_switch_jpy", init_floor_jpy))
    if base_ks_jpy <= 0:
        # M-4: null・0・不正値でキルスイッチが無効化（0円）されないよう、初期資金の85%（既定17,000円）へフォールバック
        base_ks_jpy = init_floor_jpy
    kill_switch_jpy = max(init_floor_jpy, base_ks_jpy, (peak_equity_val * Decimal("0.85")).quantize(Decimal("0.01")))

    # M-2: 確信度を [0.0, 1.0] に正規化（85 等の百分率表記による「8500%」表示を防止）
    adjusted["confidence"] = normalize_confidence(adjusted.get("confidence", 0))

    # M-1: 未知のアクション（WAIT / CLOSE 等）は SELL 分岐へ落とさず、最初に HOLD へ正規化
    if action not in VALID_ACTIONS:
        unknown_action = action
        logger.warning("未知のアクション '%s' を HOLD へ正規化しました", unknown_action)
        return _hold_decision(
            adjusted,
            symbol=symbol if symbol in merged_rules else "NONE",
            reason_prefix=f"[未知のアクション '{unknown_action}' のためHOLDへ正規化]",
        )

    # 0a. ドローダウン・キルスイッチ（総資産 < max(8500, peak_equity * 0.85) で新規BUYを停止）
    if action == "BUY" and total_equity < kill_switch_jpy:
        msg = (
            f"[ドローダウン・キルスイッチ発動によりHOLDへ強制変更] "
            f"現在の総資産 ({int(total_equity):,} 円) がキルスイッチ基準値 ({int(kill_switch_jpy):,} 円 / ピーク資産 {int(peak_equity_val):,} 円比 -15%) "
            "を下回っているため、新規BUYを停止します。"
        )
        logger.warning("🛑 キルスイッチ発動: %s", msg)
        adjusted["kill_switch_triggered"] = True
        return _hold_decision(adjusted, symbol=symbol if symbol in merged_rules else "NONE", reason_prefix=msg)

    # 0b. 相場急変時の暴落サーキットブレーカー（新規BUY時のみ発動）
    if action == "BUY":
        cb_triggered, cb_reason = check_btc_circuit_breaker(market_summaries)
        if cb_triggered:
            logger.warning("🚨 サーキットブレーカー発動: %s", cb_reason)
            adjusted["circuit_breaker_triggered"] = True
            return _hold_decision(adjusted, symbol=symbol if symbol in merged_rules else "NONE", reason_prefix=cb_reason)

    if action == "HOLD" or symbol == "NONE" or symbol not in merged_rules:
        prefix = ""
        if action != "HOLD" and symbol not in merged_rules:
            prefix = f"[補正: HOLDへ変更] 指定銘柄 '{symbol}' の取引ルールが存在しないため見送ります。"
        return _hold_decision(adjusted, symbol=symbol if symbol in merged_rules else "NONE", reason_prefix=prefix)

    max_concurrent = int(portfolio.get("max_concurrent_positions", 2))
    active_positions: List[Dict[str, Any]] = [
        p for p in positions if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
    ]
    pending_orders: List[Dict[str, Any]] = portfolio.get("pending_orders", []) or []
    active_pending_buys = [
        o for o in pending_orders
        if str(o.get("side", "BUY")).upper() == "BUY"
        and str(o.get("state", "PENDING_BUY")).upper() in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED")
    ]
    locked_pending_buy_jpy = Decimal("0")
    for o in active_pending_buys:
        o_price = _to_decimal(o.get("limit_price", o.get("price", 0)) or 0)
        o_size = max(Decimal("0"), _to_decimal(o.get("size", 0) or 0) - _to_decimal(o.get("executed_size", 0) or 0))
        if o_price > Decimal("0") and o_size > Decimal("0"):
            locked_pending_buy_jpy += o_price * o_size
    available_jpy_for_new_buy = max(Decimal("0"), jpy_balance - locked_pending_buy_jpy)

    # 新規買い (BUY) 時の同時保有銘柄数＋発注中指値数上限チェック
    # （ただし 2スロット満杯でも confidence >= 0.85 かつ 8.0時間以上経過＆含み益 < +0.5R の停滞ポジションがあれば入れ替えを許可）
    effective_active_positions = list(active_positions)
    conf_for_rot = _to_decimal(adjusted.get("confidence", 0.0))
    if action == "BUY" and (len(active_positions) + len(active_pending_buys)) >= max_concurrent:
        rotatable_pos: Optional[Dict[str, Any]] = None
        if conf_for_rot >= Decimal("0.85"):
            now_ref_str = str(portfolio.get("last_updated", "") or adjusted.get("timestamp", "")).strip()
            now_ref_dt: Optional[datetime] = None
            if now_ref_str:
                try:
                    now_ref_dt = datetime.fromisoformat(now_ref_str.replace("Z", "+00:00"))
                except Exception:
                    now_ref_dt = None
            for p_cand in active_positions:
                p_sym = str(p_cand.get("symbol", "")).strip().upper()
                if not p_sym or p_sym == symbol:
                    continue
                held_h = _to_decimal(p_cand.get("held_hours", p_cand.get("holding_hours", -1)))
                if p_cand.get("opened_at"):
                    try:
                        op_dt = datetime.fromisoformat(str(p_cand["opened_at"]).replace("Z", "+00:00"))
                        ref_dt = now_ref_dt or datetime.now(op_dt.tzinfo)
                        calc_h = _to_decimal((ref_dt - op_dt).total_seconds() / 3600.0)
                        if calc_h > held_h:
                            held_h = calc_h
                    except Exception:
                        if held_h < Decimal("0"):
                            held_h = Decimal("0")
                elif held_h < Decimal("0"):
                    held_h = Decimal("0")
                if held_h < Decimal("8.0"):
                    continue
                p_entry = _to_decimal(p_cand.get("entry_price", 0))
                p_sl = _to_decimal(p_cand.get("initial_sl_price", p_cand.get("stop_loss_price", 0)))
                p_one_r = p_entry - p_sl if (p_entry > p_sl > Decimal("0")) else (p_entry * Decimal("0.035"))
                p_sum = (market_summaries or {}).get(p_sym, {}) or {}
                p_cur = _to_decimal(
                    p_sum.get("price", {}).get("best_bid")
                    or p_sum.get("price", {}).get("last")
                    or p_cand.get("current_price")
                    or p_entry
                )
                cur_r = ((p_cur - p_entry) / p_one_r) if p_one_r > Decimal("0") else Decimal("0")
                if cur_r < Decimal("0.50"):
                    rotatable_pos = p_cand
                    break

        if rotatable_pos is not None:
            rot_sym = str(rotatable_pos.get("symbol", "")).strip().upper()
            effective_active_positions = [p for p in active_positions if p is not rotatable_pos]
            rot_size = _to_decimal(rotatable_pos.get("size", 0))
            rot_sum = (market_summaries or {}).get(rot_sym, {}) or {}
            rot_px = _to_decimal(
                rot_sum.get("price", {}).get("best_bid")
                or rot_sum.get("price", {}).get("last")
                or rotatable_pos.get("current_price")
                or rotatable_pos.get("entry_price", 0)
            )
            rot_proceeds = rot_size * rot_px * Decimal("0.9995")
            available_jpy_for_new_buy += max(Decimal("0"), rot_proceeds)
            adjusted["slot_rotation_required"] = True
            adjusted["slot_rotation_target_symbol"] = rot_sym
            adjusted["slot_rotation_reason"] = f"TIME_STOP_SLOT_ROTATION (保有8.0h以上・含み益<+0.5Rの停滞枠 {rot_sym} を高確信 {symbol} へ入れ替え)"
        else:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[同時保有・発注上限によりHOLDへ自動補正] 既に最大同時枠 ({max_concurrent}枠: "
                    f"保有{len(active_positions)}件＋待機指値{len(active_pending_buys)}件) に達しているため、新規買いエントリーを見送ります。"
                ),
            )

    # 新規買い (BUY) 時の既保有・待機中銘柄重複エントリー防止ガード
    held_symbols_active = {
        str(p.get("symbol", "")).strip().upper()
        for p in active_positions
        if _to_decimal(p.get("size", 0) or 0) > 0
    } | {
        str(o.get("symbol", "")).strip().upper()
        for o in active_pending_buys
    }
    if action == "BUY" and symbol in held_symbols_active:
        return _hold_decision(
            adjusted,
            symbol=symbol,
            reason_prefix=(
                f"[既保有銘柄の重複購入防止によりHOLDへ自動補正] 既に {symbol} のポジションを保有中のため、"
                "1銘柄50%資金配分上限および注文管理の整合性維持に基づき同一銘柄の買い増しを見送ります。"
            ),
        )

    rule = merged_rules[symbol]
    tick_size = _to_decimal(rule.get("tick_size", "1"))
    size_step = _to_decimal(rule.get("size_step", "0.0001"))
    min_order_size = _to_decimal(rule.get("min_order_size", "0.0001"))
    max_order_size = _to_decimal(rule.get("max_order_size", "1000000"))
    taker_fee = max(_to_decimal(rule.get("taker_fee", "0.0005")), Decimal("0"))
    min_order_size_str = format_decimal_plain(min_order_size)

    summary = (market_summaries or {}).get(symbol, {}) or {}
    price_info = summary.get("price", {}) or {}
    raw_limit_candidate = (
        adjusted.get("limit_price")
        or adjusted.get("entry_price")
        or adjusted.get("price")
        or 0
    )
    last_price = _to_decimal(price_info.get("last", 0) or raw_limit_candidate or 0)
    best_ask = _to_decimal(price_info.get("best_ask", 0) or 0)
    best_bid = _to_decimal(price_info.get("best_bid", 0) or 0)
    atr_val = _to_decimal(summary.get("daily_indicators", {}).get("atr_14", 0) or (last_price * Decimal("0.02")))
    if atr_val <= 0:
        atr_val = last_price * Decimal("0.02")

    # 1. 指値価格・TP・SL の丸め処理 (suggested_entry_offset_atr およびキー別名対応)
    raw_limit = _to_decimal(raw_limit_candidate)
    if (
        action == "BUY"
        and raw_limit <= Decimal("0")
        and adjusted.get("suggested_entry_offset_atr") is not None
    ):
        offset_atr_d = max(Decimal("0"), _to_decimal(adjusted.get("suggested_entry_offset_atr")))
        anchor_p = best_bid if best_bid > Decimal("0") else last_price
        if anchor_p > Decimal("0"):
            raw_limit = max(tick_size, anchor_p - (offset_atr_d * atr_val))
    if raw_limit <= 0:
        raw_limit = last_price
    limit_dec = round_to_step(raw_limit, tick_size, rounding=ROUND_DOWN)
    if limit_dec <= 0:
        limit_dec = tick_size

    raw_tp = _to_decimal(
        adjusted.get("take_profit_price")
        or adjusted.get("tp_price")
        or adjusted.get("take_profit")
        or 0
    )
    raw_sl = _to_decimal(
        adjusted.get("stop_loss_price")
        or adjusted.get("sl_price")
        or adjusted.get("stop_loss")
        or 0
    )

    if action == "BUY":
        # Post-Only（Maker専用）指値注文が即時失効しないよう best_bid 以下（または best_ask 未満）へ上限丸め
        if best_bid > 0 and limit_dec > best_bid:
            limit_dec = max(tick_size, round_to_step(best_bid, tick_size, rounding=ROUND_DOWN))
        elif best_ask > 0 and limit_dec >= best_ask:
            limit_dec = max(tick_size, round_to_step(best_ask - tick_size, tick_size, rounding=ROUND_DOWN))

        # BUY時は SL < limit_price < TP を保証
        if raw_sl <= 0 or raw_sl >= limit_dec:
            default_sl_dist = min(atr_val * Decimal("1.2"), limit_dec * Decimal("0.035"))
            raw_sl = limit_dec - default_sl_dist
        if raw_tp <= limit_dec:
            sl_dist_est = max(limit_dec - raw_sl, tick_size)
            raw_tp = limit_dec + max(atr_val * Decimal("2.0"), sl_dist_est * Decimal("1.8"))

        tp_dec = round_to_step(raw_tp, tick_size, rounding=ROUND_DOWN)
        sl_dec = round_to_step(raw_sl, tick_size, rounding=ROUND_DOWN)

        if tp_dec <= limit_dec:
            tp_dec = limit_dec + tick_size
        if sl_dec >= limit_dec:
            sl_dec = max(tick_size, limit_dec - tick_size)
    elif action == "MODIFY":
        # MODIFY時は新規指値ではなく保有建玉のTP/SL変更のため limit_price は 0 とする
        # H-3: 現在価格はティッカー（price.last → best_bid）からのみ取得し、limit_dec（=tick_size 1円）へは絶対にフォールバックしない
        ref_curr = _to_decimal(price_info.get("last", 0) or 0)
        if ref_curr <= 0 and best_bid > 0:
            ref_curr = best_bid
        price_unknown = ref_curr <= 0
        limit_dec = Decimal("0")
        existing_tp = Decimal("0")
        existing_sl = Decimal("0")
        for pos in active_positions:
            if str(pos.get("symbol", "")).strip().upper() == symbol:
                existing_tp = _to_decimal(pos.get("take_profit_price") or pos.get("tp_price") or 0)
                existing_sl = _to_decimal(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
                break

        existing_tp_rounded = (
            round_to_step(existing_tp, tick_size, rounding=ROUND_DOWN) if existing_tp > 0 else Decimal("0")
        )
        existing_sl_rounded = (
            round_to_step(existing_sl, tick_size, rounding=ROUND_DOWN) if existing_sl > 0 else Decimal("0")
        )

        # TPの引き継ぎ・補正
        if raw_tp <= 0 and existing_tp > 0:
            raw_tp = existing_tp
        if price_unknown:
            # 現在価格不明時は現在値基準の再計算を行わず、AI提案TP（なければ既存TP）をそのまま採用
            if raw_tp <= 0:
                raw_tp = existing_tp
        elif raw_tp <= 0 or raw_tp <= ref_curr:
            if existing_tp > ref_curr:
                raw_tp = existing_tp
            else:
                raw_tp = ref_curr + max(
                    tick_size * Decimal("3"),
                    atr_val * Decimal("1.5") if atr_val > 0 else ref_curr * Decimal("0.015"),
                )

        # SLの引き継ぎ・トレーリング引き下げ防止（引き上げのみ許容）
        safe_max_sl = Decimal("0")
        if price_unknown:
            # H-3: 現在価格が不明な場合はSLを一切書き換えず既存値を維持（1円SL等の異常指示を防止）
            raw_sl = existing_sl
        else:
            # 現在価格からの最低限の安全距離: max(tick_size * 3, 0.5 * ATR_4h または現在値の0.5%)
            atr_4h = _to_decimal(summary.get("h4_indicators", {}).get("atr_14", 0) or 0)
            min_sl_safety_dist = max(
                tick_size * Decimal("3"),
                (atr_4h * Decimal("0.5")) if atr_4h > 0 else (ref_curr * Decimal("0.005")),
            )
            safe_max_sl = max(tick_size, ref_curr - min_sl_safety_dist)

            if raw_sl <= 0:
                raw_sl = existing_sl if existing_sl > 0 else safe_max_sl
            if existing_sl > 0 and raw_sl < existing_sl:
                raw_sl = existing_sl
            # H-2: 安全上限 (safe_max_sl) を超える提案SLはすべてクランプ（現在値以上の場合に限らず適用）。
            #      既存SLが既に安全上限より上にある場合は引き下げず既存SLを維持する。
            if raw_sl > safe_max_sl:
                raw_sl = max(existing_sl, safe_max_sl) if existing_sl > 0 else safe_max_sl

        tp_dec = round_to_step(raw_tp, tick_size, rounding=ROUND_DOWN) if raw_tp > 0 else Decimal("0")
        sl_dec = round_to_step(raw_sl, tick_size, rounding=ROUND_DOWN) if raw_sl > 0 else Decimal("0")

        if tp_dec <= 0 and sl_dec <= 0:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[有効なTP/SLを算出できないためHOLDへ自動補正] {symbol} の現在価格または既存TP/SLが不明なため、"
                    "TP/SL変更を見送ります。"
                ),
            )
        # Phase 20 H-3: 現在価格が既に既存SL以下（または安全距離上限 safe_max_sl 未満）に下落しており、
        #               売りの逆指値 (SL) を現在価格より安全距離だけ下に置けない場合は MODIFY を出さず HOLD へ変換
        if not price_unknown and safe_max_sl > 0 and sl_dec > safe_max_sl:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[現在価格が既存SL水準に接近・到達しているためHOLDへ自動補正] {symbol} の現在価格 "
                    f"({format_price_jpy(ref_curr)} 円) が既存SL ({format_price_jpy(sl_dec)} 円) 水準以下または"
                    f"安全上限 ({format_price_jpy(safe_max_sl)} 円) に接近しており、売りの逆指値 (SL) を安全に再発注できないため HOLD とします。"
                ),
            )
        if tp_dec > 0 and sl_dec > 0 and tp_dec <= sl_dec:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[TP/SL不整合によりHOLDへ自動補正] {symbol} の補正後TP ({format_price_jpy(tp_dec)} 円) が "
                    f"SL ({format_price_jpy(sl_dec)} 円) 以下となるため、TP/SL変更を見送ります。"
                ),
            )

        # 補正後の tp_dec == existing_tp かつ sl_dec == existing_sl（実質変更なし）の場合は無駄な手動変更通知を出さないよう HOLD へ変換
        if (
            (existing_tp_rounded > 0 or existing_sl_rounded > 0)
            and tp_dec == existing_tp_rounded
            and sl_dec == existing_sl_rounded
        ):
            if price_unknown:
                noop_reason = (
                    f"[TP/SL変更なしによりHOLDへ自動補正] {symbol} の現在価格を取得できないためSLは既存値 "
                    f"({format_price_jpy(sl_dec)} 円) を維持し、TPも既存設定と同一のため HOLD とします。"
                )
            else:
                noop_reason = (
                    f"[TP/SL変更なしによりHOLDへ自動補正] {symbol} の補正後TP ({format_price_jpy(tp_dec)} 円) / "
                    f"SL ({format_price_jpy(sl_dec)} 円) が既存のTP/SL設定と同一（または安全距離確保のため引き上げ見送り）"
                    "のため、既存設定を維持し HOLD とします。"
                )
            return _hold_decision(adjusted, symbol=symbol, reason_prefix=noop_reason)

        if price_unknown:
            adjusted["reasoning"] = (
                f"[現在価格取得不可のためSLは既存値を維持] " + str(adjusted.get("reasoning", ""))
            )
    else:
        # SELL（決済売り）時は TP/SL は 0
        tp_dec = Decimal("0")
        sl_dec = Decimal("0")

    # パターンA（自動複利スケール型）: 1銘柄あたり最大投入比率 45% (MAX_POSITION_ALLOCATION_PCT = 0.45)
    # ただし保有0件（2スロット空）かつ confidence >= 0.80、ストップ距離率 d_SL < 2.2%、主要銘柄 (BTC/ETH/XRP/SOL) の場合は
    # 第1スロット配分上限を最大 65% まで動的拡張し、第2スロット上限を残り 25%（合計上限 90%・現金予備 10% 死守）に制御する。
    MAX_POSITION_ALLOCATION_PCT = Decimal("0.45")
    DYNAMIC_EXPANDED_SLOT1_ALLOCATION_PCT = Decimal("0.65")
    MAX_TOTAL_EXPOSURE_PCT = Decimal("0.90")
    DYNAMIC_SLOT_MAJOR_SYMBOLS = {"BTC", "ETH", "XRP", "SOL"}

    raw_alloc_pct = _to_decimal(portfolio.get("max_allocation_per_trade_pct", MAX_POSITION_ALLOCATION_PCT))
    if raw_alloc_pct <= Decimal("0") or raw_alloc_pct > Decimal("0.50") or total_equity > Decimal("15000"):
        max_alloc_pct = MAX_POSITION_ALLOCATION_PCT
    else:
        max_alloc_pct = raw_alloc_pct

    # パターンA: 1トレードあたりの許容損失額（SLリスク予算）を現在の総資産の 1.2%〜1.5%（実損失上限 2.0%）で動的算出
    conf_val = _to_decimal(adjusted.get("confidence", 0.80))
    # 確信度に応じて [1.2%, 1.5%] の範囲でスケーリング（通常の BUY 承認時 >=0.75 は 1.5% を上限として適用）
    dynamic_risk_pct = Decimal("0.015") if conf_val >= Decimal("0.75") else Decimal("0.012")
    dynamic_min_risk_jpy = total_equity * Decimal("0.012")
    dynamic_target_risk_jpy = max(dynamic_min_risk_jpy, total_equity * dynamic_risk_pct)
    dynamic_hard_max_risk_jpy = total_equity * Decimal("0.020")

    target_risk_cfg = _to_decimal(portfolio.get("target_risk_per_trade_jpy", 0))
    max_risk_cfg = _to_decimal(portfolio.get("max_risk_per_trade_jpy", 0))
    if target_risk_cfg <= Decimal("0") or total_equity > Decimal("10000") or target_risk_cfg in (Decimal("150"), Decimal("300")):
        effective_target_risk_jpy = dynamic_target_risk_jpy
    else:
        effective_target_risk_jpy = min(target_risk_cfg, dynamic_target_risk_jpy)

    if max_risk_cfg <= Decimal("0") or total_equity > Decimal("10000") or max_risk_cfg in (Decimal("200"), Decimal("400")):
        effective_max_risk_jpy = dynamic_hard_max_risk_jpy
    else:
        effective_max_risk_jpy = min(max_risk_cfg, dynamic_hard_max_risk_jpy)

    fee_buffer = Decimal("1") + taker_fee
    init_sl_diff = limit_dec - sl_dec if (action == "BUY" and limit_dec > sl_dec > Decimal("0")) else Decimal("0")
    init_sl_dist_rate = (init_sl_diff / limit_dec) if limit_dec > Decimal("0") else Decimal("0")
    occupied_slots_count = len(effective_active_positions) + len(active_pending_buys)

    dynamic_slot_expanded = False
    if (
        action == "BUY"
        and occupied_slots_count == 0
        and conf_val >= Decimal("0.80")
        and Decimal("0") < init_sl_dist_rate < Decimal("0.022")
        and symbol in DYNAMIC_SLOT_MAJOR_SYMBOLS
    ):
        max_alloc_pct = DYNAMIC_EXPANDED_SLOT1_ALLOCATION_PCT
        dynamic_slot_expanded = True

    existing_invested_jpy = Decimal("0")
    for p_act in effective_active_positions:
        p_cost = _to_decimal(p_act.get("cost_jpy", 0) or 0)
        if p_cost <= Decimal("0"):
            p_cost = _to_decimal(p_act.get("entry_price", 0) or 0) * _to_decimal(p_act.get("size", 0) or 0)
        existing_invested_jpy += max(Decimal("0"), p_cost)
    for o_act in active_pending_buys:
        o_px = _to_decimal(o_act.get("limit_price", o_act.get("price", 0)) or 0)
        o_sz = max(Decimal("0"), _to_decimal(o_act.get("size", 0) or 0) - _to_decimal(o_act.get("executed_size", 0) or 0))
        existing_invested_jpy += max(Decimal("0"), o_px * o_sz)

    max_total_exposure_jpy = total_equity * MAX_TOTAL_EXPOSURE_PCT
    remaining_exposure_cap_jpy = max(Decimal("0"), max_total_exposure_jpy - existing_invested_jpy)
    cash_reserve_floor_jpy = total_equity * (Decimal("1") - MAX_TOTAL_EXPOSURE_PCT)
    usable_jpy_after_reserve = max(Decimal("0"), available_jpy_for_new_buy - max(Decimal("0"), cash_reserve_floor_jpy - max(Decimal("0"), jpy_balance - available_jpy_for_new_buy)))
    if occupied_slots_count >= 1:
        per_trade_cap_jpy = min(available_jpy_for_new_buy, total_equity * max_alloc_pct, remaining_exposure_cap_jpy)
    else:
        per_trade_cap_jpy = min(available_jpy_for_new_buy, total_equity * max_alloc_pct, max_total_exposure_jpy)
    available_budget = per_trade_cap_jpy / fee_buffer

    raw_size = _to_decimal(adjusted.get("size", 0) or 0)

    if action == "BUY":
        # 2. 「(スプレッド + Taker手数料) ÷ SL距離」が 10% を超える場合はコスト負け（期待値マイナス）として HOLD へ変更
        spread_pct_raw = price_info.get("spread_pct")
        if spread_pct_raw is not None and _to_decimal(spread_pct_raw) > 0:
            spread_rate = _to_decimal(spread_pct_raw) / Decimal("100")
        elif best_ask > best_bid > 0:
            spread_rate = (best_ask - best_bid) / best_bid
        else:
            spread_rate = Decimal("0")

        sl_diff = limit_dec - sl_dec
        sl_distance_rate = (sl_diff / limit_dec) if limit_dec > 0 else Decimal("0")
        cost_to_sl_ratio = (
            ((spread_rate + taker_fee) / sl_distance_rate) if sl_distance_rate > 0 else Decimal("999")
        )
        if cost_to_sl_ratio > Decimal("0.10"):
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[取引コスト比率超過によりHOLDへ自動補正] {symbol} の (スプレッド {float(spread_rate * 100):.3f}% + "
                    f"Taker手数料 {float(taker_fee * 100):.3f}%) ÷ SL距離 {float(sl_distance_rate * 100):.2f}% = "
                    f"{float(cost_to_sl_ratio * 100):.1f}% が上限 10.0% を超えるため、コスト負け（期待値マイナス）として見送ります。"
                ),
            )

        # 3. 2枠目を新規BUYする際の「ポートフォリオ合計リスク」上限（総資産の3.0%＝初期300円）チェック
        max_portfolio_risk_jpy = total_equity * Decimal("0.03")
        existing_risk_jpy = calculate_existing_portfolio_risk_jpy(effective_active_positions, merged_rules, active_pending_buys)
        remaining_portfolio_risk_jpy = max_portfolio_risk_jpy - existing_risk_jpy
        if remaining_portfolio_risk_jpy <= Decimal("0"):
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[ポートフォリオ合計リスク上限によりHOLDへ自動補正] 既存ポジションの想定リスク "
                    f"(約 {int(existing_risk_jpy):,} 円) が総資産の3.0%上限 ({int(max_portfolio_risk_jpy):,} 円) に"
                    "達しているため、新規BUYを見送ります。"
                ),
            )

        # 4. 最小注文数量に必要な日本円を計算
        min_required_jpy = min_order_size * limit_dec * fee_buffer
        if min_required_jpy > available_jpy_for_new_buy:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[資金不足によりHOLDへ自動補正] {symbol} の最小注文数量 ({min_order_size_str} {symbol}) の購入に "
                    f"約 {int(min_required_jpy):,} 円が必要ですが、現在利用可能余力 ({int(available_jpy_for_new_buy):,} 円) を超過しています。"
                ),
            )

        max_affordable_size = round_to_step(available_budget / limit_dec, size_step, rounding=ROUND_DOWN)
        if max_affordable_size < min_order_size:
            if min_required_jpy <= per_trade_cap_jpy * Decimal("1.05") and min_required_jpy <= available_jpy_for_new_buy:
                max_affordable_size = min_order_size
            else:
                return _hold_decision(
                    adjusted,
                    symbol=symbol,
                    reason_prefix=(
                        f"[1銘柄あたり資金配分上限によりHOLDへ自動補正] {symbol} の最小注文数量 ({min_order_size_str} {symbol} ≈ "
                        f"{int(min_required_jpy):,} 円) が1ポジション上限 ({int(per_trade_cap_jpy):,} 円) を超過しています。"
                    ),
                )

        # 5. パターンA 動的複利リスク予算「総資産の1.2%〜1.5%（および残余ポートフォリオリスク枠）」から注文数量を逆算し、
        #    さらに Taker手数料 + スリッページ予備を見込んだ実損失でも絶対に 総資産の2.0%（および残余リスク枠）を超えないよう制御
        nominal_target_risk_jpy = min(
            effective_target_risk_jpy,
            remaining_portfolio_risk_jpy,
        )
        hard_max_risk_jpy = min(effective_max_risk_jpy, remaining_portfolio_risk_jpy)

        # 1単位あたりの名目損失（SL幅＋SL発動時Taker手数料）およびスリッページ予備込み実損失
        sl_taker_fee_per_unit = sl_dec * taker_fee
        slippage_reserve_rate = max(spread_rate, Decimal("0.0020"))
        slippage_per_unit = sl_dec * slippage_reserve_rate

        nominal_loss_per_unit = sl_diff + sl_taker_fee_per_unit
        stressed_loss_per_unit = nominal_loss_per_unit + slippage_per_unit

        if nominal_loss_per_unit <= 0:
            nominal_loss_per_unit = tick_size
        if stressed_loss_per_unit <= 0:
            stressed_loss_per_unit = nominal_loss_per_unit

        raw_risk_size = nominal_target_risk_jpy / nominal_loss_per_unit
        size_by_nominal_target = round_to_step(
            raw_risk_size,
            size_step,
            rounding=ROUND_DOWN,
        )
        size_by_stressed_cap = round_to_step(
            hard_max_risk_jpy / stressed_loss_per_unit,
            size_step,
            rounding=ROUND_DOWN,
        )
        risk_capped_size = min(size_by_nominal_target, size_by_stressed_cap)

        if raw_size <= 0:
            candidate_size = min(max_affordable_size, risk_capped_size, max_order_size)
        else:
            rounded_raw = round_to_step(raw_size, size_step, rounding=ROUND_DOWN)
            candidate_size = min(rounded_raw, max_affordable_size, risk_capped_size, max_order_size)

        if candidate_size >= min_order_size:
            size_dec = candidate_size
        else:
            # 最小注文数量 (min_order_size) での損失が上限内に収まるか検証
            min_lot_nominal_loss = nominal_loss_per_unit * min_order_size
            min_lot_stressed_loss = stressed_loss_per_unit * min_order_size

            if (
                min_lot_nominal_loss <= nominal_target_risk_jpy
                and min_lot_stressed_loss <= hard_max_risk_jpy
            ):
                size_dec = min_order_size
            else:
                if occupied_slots_count >= 1 and (
                    existing_risk_jpy + min_lot_nominal_loss > max_portfolio_risk_jpy
                    or existing_risk_jpy + min_lot_stressed_loss > max_portfolio_risk_jpy
                ):
                    return _hold_decision(
                        adjusted,
                        symbol=symbol,
                        reason_prefix=(
                            f"[ポートフォリオ合計リスク上限によりHOLDへ自動補正] 1枠目の想定リスク ({int(existing_risk_jpy):,} 円) と "
                            f"2枠目 {symbol} の最小ロット想定リスク ({int(min_lot_nominal_loss):,} 円) の合計が "
                            f"総資産の3.0%上限 ({int(max_portfolio_risk_jpy):,} 円) を超過するため見送ります。"
                        ),
                    )
                return _hold_decision(
                    adjusted,
                    symbol=symbol,
                    reason_prefix=(
                        f"[想定最大損失超過によりHOLDへ自動補正] {symbol} の最小ロット ({min_order_size_str}) での想定SL損失 "
                        f"(名目 {int(min_lot_nominal_loss):,} 円 / スリッページ込 {int(min_lot_stressed_loss):,} 円) が "
                        f"許容上限 (名目 {int(nominal_target_risk_jpy):,} 円 / 実損失上限 {int(hard_max_risk_jpy):,} 円) を超過するため見送ります。"
                    ),
                )

        # 6. 最小ロット切り捨て (Floor) で浮いたリスク許容額を初期ストップ距離へ逆算還元 (Breathing Room Adjustment)
        original_sl_dec = sl_dec
        breathing_ticks = 0
        base_nominal_loss = nominal_loss_per_unit * size_dec
        base_stressed_loss = stressed_loss_per_unit * size_dec
        if size_dec > Decimal("0") and base_nominal_loss < nominal_target_risk_jpy and base_stressed_loss < hard_max_risk_jpy:
            leftover_nominal_jpy = max(Decimal("0"), nominal_target_risk_jpy - base_nominal_loss)
            leftover_stressed_jpy = max(Decimal("0"), hard_max_risk_jpy - base_stressed_loss)
            # ロット切り捨て1ステップ分相当（または初期SL距離の15%）を上限として安全に数ティック引き下げる
            step_risk_cap_jpy = size_step * nominal_loss_per_unit
            effective_leftover_jpy = min(leftover_nominal_jpy, leftover_stressed_jpy, step_risk_cap_jpy)
            max_extra_dist = min(effective_leftover_jpy / size_dec, sl_diff * Decimal("0.15"))
            candidate_ticks = int((max_extra_dist / tick_size).to_integral_value(rounding=ROUND_DOWN))
            if candidate_ticks > 0:
                for t_try in range(candidate_ticks, max(0, candidate_ticks - 5), -1):
                    cand_sl = original_sl_dec - (Decimal(t_try) * tick_size)
                    if cand_sl <= Decimal("0"):
                        continue
                    cand_diff = limit_dec - cand_sl
                    cand_nom_unit = cand_diff + (cand_sl * taker_fee)
                    cand_str_unit = cand_nom_unit + (cand_sl * slippage_reserve_rate)
                    if (
                        (cand_nom_unit * size_dec) <= nominal_target_risk_jpy
                        and (cand_str_unit * size_dec) <= hard_max_risk_jpy
                    ):
                        sl_dec = cand_sl
                        sl_diff = cand_diff
                        nominal_loss_per_unit = cand_nom_unit
                        stressed_loss_per_unit = cand_str_unit
                        breathing_ticks = t_try
                        break

        expected_cost = int((limit_dec * size_dec).to_integral_value(rounding=ROUND_HALF_UP))
        reward = tp_dec - limit_dec
        risk = limit_dec - sl_dec
        expected_loss_jpy = int((nominal_loss_per_unit * size_dec).to_integral_value(rounding=ROUND_HALF_UP))
        stressed_loss_jpy = int((stressed_loss_per_unit * size_dec).to_integral_value(rounding=ROUND_UP))
        rr_ratio = float(round(reward / risk, 2)) if risk > 0 else 0.0

        adjusted["order_type"] = "LIMIT_POST_ONLY"
        adjusted["expected_loss_with_slippage_jpy"] = stressed_loss_jpy
        adjusted["portfolio_total_risk_jpy"] = int(
            (existing_risk_jpy + (nominal_loss_per_unit * size_dec)).to_integral_value(rounding=ROUND_HALF_UP)
        )
        adjusted["original_stop_loss_price"] = _format_number(original_sl_dec)
        adjusted["breathing_room_ticks"] = breathing_ticks
        adjusted["breathing_room_applied"] = bool(breathing_ticks > 0)
        adjusted["dynamic_slot_expanded"] = dynamic_slot_expanded
        adjusted["dynamic_slot_expansion_applied"] = dynamic_slot_expanded
        adjusted["effective_slot_alloc_pct"] = float(max_alloc_pct)
        adjusted["effective_slot_allocation_pct"] = (
            float(round(per_trade_cap_jpy / total_equity, 4)) if total_equity > Decimal("0") else float(max_alloc_pct)
        )
        adjusted["slot2_remaining_allocation_pct"] = (
            float(round(max(Decimal("0"), MAX_TOTAL_EXPOSURE_PCT - ( Decimal(expected_cost) / total_equity )), 4))
            if (occupied_slots_count == 0 and total_equity > Decimal("0"))
            else float(round(remaining_exposure_cap_jpy / total_equity, 4)) if total_equity > Decimal("0") else 0.25
        )
        adjusted["cash_reserve_floor_jpy"] = int(cash_reserve_floor_jpy.to_integral_value(rounding=ROUND_DOWN))

    else:
        # SELL または MODIFY の場合は保有ポジション数量を確認
        held_size = Decimal("0")
        held_entry = last_price if last_price > 0 else limit_dec
        held_initial_risk_jpy = Decimal("0")
        for pos in active_positions:
            if str(pos.get("symbol", "")).upper() == symbol:
                held_size += _to_decimal(pos.get("size", 0) or 0)
                if _to_decimal(pos.get("entry_price", 0) or 0) > 0:
                    held_entry = _to_decimal(pos.get("entry_price", 0))
                if _to_decimal(pos.get("initial_risk_jpy", 0) or 0) > 0:
                    held_initial_risk_jpy = _to_decimal(pos.get("initial_risk_jpy", 0))

        if held_size < min_order_size:
            return _hold_decision(
                adjusted,
                symbol=symbol,
                reason_prefix=(
                    f"[保有数量不足によりHOLDへ自動補正] {symbol} の保有数量 ({format_decimal_plain(held_size)}) が最小注文数量 "
                    f"({min_order_size_str}) 未満のため {action} 注文を発注できません。"
                ),
            )

        target_size = held_size if (raw_size <= 0 or raw_size > held_size) else raw_size
        size_dec = round_to_step(target_size, size_step, rounding=ROUND_DOWN)
        if size_dec < min_order_size:
            size_dec = min_order_size

        if action == "MODIFY":
            adjusted["order_type"] = "STOP_OR_LIMIT"
            expected_cost = 0
            reward = max(Decimal("0"), tp_dec - held_entry)
            if sl_dec < held_entry:
                risk = held_entry - sl_dec
                expected_loss_jpy = int(
                    ((risk + sl_dec * taker_fee) * size_dec).to_integral_value(rounding=ROUND_HALF_UP)
                )
                rr_ratio = float(round(reward / risk, 2)) if risk > 0 else 0.0
            else:
                # 建値保護済み（SL >= 取得単価）で損失リスク0円の場合、初期リスク幅を基準にRR比を安全に算出
                expected_loss_jpy = 0
                if held_initial_risk_jpy > 0 and size_dec > 0:
                    init_risk_per_unit = held_initial_risk_jpy / size_dec
                    rr_ratio = float(round(reward / init_risk_per_unit, 2)) if init_risk_per_unit > 0 else 0.0
                elif held_entry > 0:
                    fallback_risk = held_entry * Decimal("0.035")
                    rr_ratio = float(round(reward / fallback_risk, 2)) if fallback_risk > 0 else 0.0
                else:
                    rr_ratio = 0.0
        else:
            # SELL（決済売り）
            adjusted["order_type"] = "LIMIT_OR_MARKET"
            expected_cost = 0
            expected_loss_jpy = 0
            rr_ratio = 0.0

    adjusted.update(
        {
            "action": action,
            "symbol": symbol,
            "limit_price": _format_number(limit_dec),
            "size": _format_number(size_dec),
            "take_profit_price": _format_number(tp_dec),
            "stop_loss_price": _format_number(sl_dec),
            "expected_cost_jpy": expected_cost,
            "expected_loss_jpy": expected_loss_jpy,
            "risk_reward_ratio": rr_ratio,
        }
    )
    return adjusted
