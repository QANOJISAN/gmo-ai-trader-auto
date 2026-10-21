"""
逆選択（Adverse Selection / ナイフ掴み・板ぶつけ被弾）回避・3層ミクロ構造フィルタ (adverse_selection_guard.py)

Public WebSocket の `orderbooks` と `trades` (`TAKER_ONLY`) から以下の3指標をリアルタイム算出し、
「新規買い指値エントリーの許可/拒否」「軽度売り圧力時のパッシブ・オフセット指値計算」
および「発注済み待機買い指値 (`PENDING_BUY`) の即時緊急キャンセル判定」を行う：

① 第1層：5段階指数減衰重み付き板インバランス ($OBI_5$) と Micro-Price 乖離
   - Entry許可: $OBI_5 \\ge -0.10$ かつ 第1買板比率 $r_{\\text{bid},1} \\ge 0.35$
   - 即時取消: $OBI_5 \\le -0.45$ または 自注文価格以上の買板残量 $< 2.0 \\times \\text{size}$

② 第2層：半減期60秒の Taker 約定フロー毒性 ($TFI_{60s}$) と 売り急増倍率 ($SSI_{60s}$)
   - Entry許可: $TFI_{60s} > -0.25$ かつ $SSI_{60s} < 1.8$
   - 即時取消: $TFI_{60s} \\le -0.50$ または $SSI_{60s} \\ge 3.0$ または 5秒内5連続価格切下げ SELL スイープ検知
   - 軽度売り圧力時 ($-0.25 < TFI_{60s} < -0.05$): 最良買気配の先頭に並ばず、パッシブ・オフセット価格に SOK 指値を配置

③ 第3層：60秒急落幅 ($\\Delta P_{60s}$) 対 $ATR_{5m}(14)$ 比・相対スプレッド比・データ鮮度
   - Entry許可: $R_{60s} < 0.70 \\cdot ATR_{5m}$ かつ 相対スプレッド比 $\\le 2.0$倍 かつ データ遅延 $\\le 5.0$秒
   - 即時取消: $\\Delta P_{60s} \\le -0.80 \\cdot ATR_{5m}$ または 相対スプレッド比 $\\ge 3.0$倍 または データ遅延 $> 5.0$秒
"""

from __future__ import annotations

import collections
import math
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Deque, Dict, List, Optional, Tuple, Union

from src.validator import DEFAULT_SYMBOL_RULES, _to_decimal, round_to_step


class AdverseSelectionGuard:
    """3層ミクロ構造フィルタ ＆ 待機買い指値の逆選択即時キャンセルガーディアン"""

    LAMBDA_DECAY = 0.50
    TFI_HALF_LIFE_SEC = 60.0

    def __init__(self, baseline_sell_vol_60s: float = 10.0) -> None:
        self.default_baseline_sell_vol_60s = max(1e-6, float(baseline_sell_vol_60s))
        self._trades_by_symbol: Dict[str, Deque[Dict[str, Any]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=2000)
        )
        self._price_history_60s: Dict[str, Deque[Tuple[float, float]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=1000)
        )
        self._latest_orderbook: Dict[str, Dict[str, Any]] = {}
        self._baseline_sell_vol: Dict[str, float] = {}
        self._median_spread: Dict[str, float] = {}

    def record_orderbook(
        self,
        symbol: str,
        asks: List[Dict[str, Any]],
        bids: List[Dict[str, Any]],
        timestamp_sec: Optional[float] = None,
        median_spread: Optional[float] = None,
        is_rest_snapshot: bool = False,
    ) -> None:
        sym = symbol.strip().upper()
        ts = float(timestamp_sec if timestamp_sec is not None else time.time())
        norm_asks = sorted(
            [{"price": float(a["price"]), "size": float(a["size"])} for a in asks if float(a.get("size", 0)) > 0],
            key=lambda x: x["price"],
        )
        norm_bids = sorted(
            [{"price": float(b["price"]), "size": float(b["size"])} for b in bids if float(b.get("size", 0)) > 0],
            key=lambda x: x["price"],
            reverse=True,
        )
        self._latest_orderbook[sym] = {
            "asks": norm_asks,
            "bids": norm_bids,
            "timestamp_sec": ts,
            "is_rest_snapshot": bool(is_rest_snapshot),
        }
        if norm_asks and norm_bids:
            mid = (norm_asks[0]["price"] + norm_bids[0]["price"]) / 2.0
            self._price_history_60s[sym].append((ts, mid))
            spread = max(0.0, norm_asks[0]["price"] - norm_bids[0]["price"])
            if median_spread is not None and median_spread > 0:
                self._median_spread[sym] = float(median_spread)
            elif sym not in self._median_spread and spread > 0:
                self._median_spread[sym] = spread

    def record_taker_trade(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        timestamp_sec: Optional[float] = None,
    ) -> None:
        sym = symbol.strip().upper()
        ts = float(timestamp_sec if timestamp_sec is not None else time.time())
        self._trades_by_symbol[sym].append(
            {
                "side": side.strip().upper(),
                "price": float(price),
                "size": float(size),
                "timestamp_sec": ts,
            }
        )
        self._price_history_60s[sym].append((ts, float(price)))

    def set_baseline_metrics(
        self,
        symbol: str,
        baseline_sell_vol_60s: Optional[float] = None,
        median_spread: Optional[float] = None,
    ) -> None:
        sym = symbol.strip().upper()
        if baseline_sell_vol_60s is not None and baseline_sell_vol_60s > 0:
            self._baseline_sell_vol[sym] = float(baseline_sell_vol_60s)
        if median_spread is not None and median_spread > 0:
            self._median_spread[sym] = float(median_spread)

    # ------------------------------------------------------------------
    # 第1層: 5段階指数減衰重み付き板インバランス OBI_5 & Micro-Price
    # ------------------------------------------------------------------
    def compute_layer1_obi(
        self,
        symbol: str,
        order_price: Optional[float] = None,
    ) -> Dict[str, float]:
        sym = symbol.strip().upper()
        ob = self._latest_orderbook.get(sym, {})
        asks = ob.get("asks", [])[:5]
        bids = ob.get("bids", [])[:5]

        if not asks or not bids:
            return {
                "obi_5": 0.0,
                "level1_bid_ratio": 0.5,
                "micro_price": 0.0,
                "mid_price": 0.0,
                "bid_queue_at_or_above_order": 999999.0,
                "spread": 0.0,
            }

        v_bid = 0.0
        v_ask = 0.0
        for i in range(5):
            w = math.exp(-self.LAMBDA_DECAY * i)
            if i < len(bids):
                v_bid += w * float(bids[i]["size"])
            if i < len(asks):
                v_ask += w * float(asks[i]["size"])

        obi_5 = (v_bid - v_ask) / (v_bid + v_ask) if (v_bid + v_ask) > 0 else 0.0

        q_bid1 = float(bids[0]["size"])
        q_ask1 = float(asks[0]["size"])
        p_bid1 = float(bids[0]["price"])
        p_ask1 = float(asks[0]["price"])
        l1_total = q_bid1 + q_ask1
        l1_bid_ratio = (q_bid1 / l1_total) if l1_total > 0 else 0.5
        mid_price = (p_ask1 + p_bid1) / 2.0
        micro_price = ((q_bid1 * p_ask1 + q_ask1 * p_bid1) / l1_total) if l1_total > 0 else mid_price

        bid_queue = 0.0
        all_bids = ob.get("bids", [])
        if order_price is not None and order_price > 0:
            for b in all_bids:
                if float(b["price"]) >= float(order_price) - 1e-9:
                    bid_queue += float(b["size"])
        else:
            bid_queue = q_bid1

        return {
            "obi_5": round(obi_5, 4),
            "level1_bid_ratio": round(l1_bid_ratio, 4),
            "micro_price": round(micro_price, 6),
            "mid_price": round(mid_price, 6),
            "bid_queue_at_or_above_order": round(bid_queue, 8),
            "spread": max(0.0, p_ask1 - p_bid1),
            "best_bid": p_bid1,
            "best_ask": p_ask1,
        }

    # ------------------------------------------------------------------
    # 第2層: 半減期60秒 TFI_60s & 売り急増倍率 SSI_60s & 5秒5連続SELLスイープ検知
    # ------------------------------------------------------------------
    def compute_layer2_tfi(
        self,
        symbol: str,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        sym = symbol.strip().upper()
        now = float(now_sec if now_sec is not None else time.time())
        trades = self._trades_by_symbol.get(sym, collections.deque())

        v_buy_decay = 0.0
        v_sell_decay = 0.0
        v_sell_raw_60s = 0.0
        recent_5s_sells: List[float] = []

        for tr in trades:
            age = now - float(tr["timestamp_sec"])
            if age < -1.0 or age > 300.0:
                continue
            age_clamped = max(0.0, age)
            w = math.pow(2.0, -age_clamped / self.TFI_HALF_LIFE_SEC)
            sz = float(tr["size"])
            side = str(tr["side"]).upper()
            if side == "BUY":
                v_buy_decay += w * sz
            elif side == "SELL":
                v_sell_decay += w * sz
                if age_clamped <= 60.0:
                    v_sell_raw_60s += sz
                if age_clamped <= 5.0:
                    recent_5s_sells.append(float(tr["price"]))

        total_decay = v_buy_decay + v_sell_decay
        tfi_60s = ((v_buy_decay - v_sell_decay) / total_decay) if total_decay > 0 else 0.0
        baseline_sell = self._baseline_sell_vol.get(sym, self.default_baseline_sell_vol_60s)
        ssi_60s = (v_sell_raw_60s / baseline_sell) if baseline_sell > 0 else 0.0

        # 5秒以内に5連続で価格を切り下げる Taker SELL スイープが発生したか判定
        sell_sweep_detected = False
        if len(recent_5s_sells) >= 5:
            last5 = recent_5s_sells[-5:]
            if all(last5[i] > last5[i + 1] for i in range(4)):
                sell_sweep_detected = True

        return {
            "tfi_60s": round(tfi_60s, 4),
            "ssi_60s": round(ssi_60s, 4),
            "sell_sweep_detected": sell_sweep_detected,
            "v_buy_decay": round(v_buy_decay, 6),
            "v_sell_decay": round(v_sell_decay, 6),
        }

    # ------------------------------------------------------------------
    # 第3層: 60秒急落幅 ΔP_60s 対 ATR_5m(14) 比 & 相対スプレッド比 & データ遅延
    # ------------------------------------------------------------------
    def compute_layer3_velocity_and_latency(
        self,
        symbol: str,
        atr_5m: float,
        now_sec: Optional[float] = None,
    ) -> Dict[str, float]:
        sym = symbol.strip().upper()
        now = float(now_sec if now_sec is not None else time.time())
        ob = self._latest_orderbook.get(sym, {})
        last_ob_ts = float(ob.get("timestamp_sec", now))
        data_latency_sec = max(0.0, now - last_ob_ts)

        hist = self._price_history_60s.get(sym, collections.deque())
        prices_in_60s = [p for ts, p in hist if 0.0 <= (now - ts) <= 60.0]
        if prices_in_60s:
            current_p = prices_in_60s[-1]
            peak_p_60s = max(prices_in_60s)
            delta_p_60s = current_p - peak_p_60s
        else:
            delta_p_60s = 0.0

        safe_atr = max(1e-6, float(atr_5m))
        drop_atr_ratio = max(0.0, -delta_p_60s) / safe_atr

        l1 = self.compute_layer1_obi(sym)
        curr_spread = float(l1.get("spread", 0.0))
        med_spread = max(1e-9, float(self._median_spread.get(sym, curr_spread if curr_spread > 0 else 1.0)))
        rel_spread_ratio = (curr_spread / med_spread) if curr_spread > 0 else 1.0

        return {
            "delta_p_60s": round(delta_p_60s, 6),
            "drop_atr_ratio_60s": round(drop_atr_ratio, 4),
            "relative_spread_ratio": round(rel_spread_ratio, 4),
            "data_latency_sec": round(data_latency_sec, 3),
        }

    # ------------------------------------------------------------------
    # 統合判定: 新規エントリー許可 ＆ パッシブ・オフセット価格計算
    # ------------------------------------------------------------------
    def evaluate_entry_guard(
        self,
        symbol: str,
        proposed_limit_price: Union[str, int, float, Decimal],
        atr_5m: float,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        新規買い指値の発注前に3層ミクロ構造フィルタを評価する。
        - 軽度売り圧力 (`-0.25 < TFI_60s < -0.05`) の場合は最良買気配の先頭に並ばず、
          パッシブ・オフセット価格 (`adjusted_limit_price`) に引き下げた SOK 指値を返す。
        """
        sym = symbol.strip().upper()
        l1 = self.compute_layer1_obi(sym)
        l2 = self.compute_layer2_tfi(sym, now_sec=now_sec)
        l3 = self.compute_layer3_velocity_and_latency(sym, atr_5m=atr_5m, now_sec=now_sec)

        reject_reasons: List[str] = []
        if l1["obi_5"] < -0.10:
            reject_reasons.append(f"OBI_5({l1['obi_5']:.2f}) < -0.10")
        if l1["level1_bid_ratio"] < 0.35:
            reject_reasons.append(f"L1買板比率({l1['level1_bid_ratio']:.2f}) < 0.35")
        if l2["tfi_60s"] <= -0.25:
            reject_reasons.append(f"TFI_60s({l2['tfi_60s']:.2f}) <= -0.25")
        if l2["ssi_60s"] >= 1.8:
            reject_reasons.append(f"SSI_60s({l2['ssi_60s']:.2f}) >= 1.8")
        if l2["sell_sweep_detected"]:
            reject_reasons.append("5秒内5連続SELLスイープ検知")
        if l3["drop_atr_ratio_60s"] >= 0.70:
            reject_reasons.append(f"60秒急落比({l3['drop_atr_ratio_60s']:.2f}×ATR_5m) >= 0.70")
        if l3["relative_spread_ratio"] > 2.0:
            reject_reasons.append(f"相対スプレッド比({l3['relative_spread_ratio']:.2f}倍) > 2.0")
        if l3["data_latency_sec"] > 5.0:
            reject_reasons.append(f"板データ遅延({l3['data_latency_sec']:.1f}s) > 5.0s")

        rule = DEFAULT_SYMBOL_RULES.get(sym, {})
        tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
        prop_dec = round_to_step(_to_decimal(proposed_limit_price), tick_size, rounding=ROUND_DOWN)
        passive_offset_applied = False

        # 軽度売り圧力時 (-0.25 < TFI_60s < -0.05) は最良買気配 (Best Bid) を基準に
        # Bid - max(1_tick, quantize_tick(0.15 * atr_5m * abs(tfi_60s))) のパッシブ・オフセットを適用
        if not reject_reasons and (-0.25 < l2["tfi_60s"] < -0.05):
            best_bid_d = _to_decimal(l1.get("best_bid", prop_dec) or prop_dec)
            best_ask_d = _to_decimal(l1.get("best_ask", 0) or 0)
            anchor_bid_d = min(prop_dec, best_bid_d) if best_bid_d > Decimal("0") else prop_dec
            if best_ask_d > tick_size:
                anchor_bid_d = min(anchor_bid_d, best_ask_d - tick_size)
            raw_dyn_offset = Decimal("0.15") * _to_decimal(atr_5m) * _to_decimal(abs(l2["tfi_60s"]))
            quantized_dyn_offset = round_to_step(raw_dyn_offset, tick_size, rounding=ROUND_DOWN)
            offset_d = max(tick_size, quantized_dyn_offset)
            passive_target = round_to_step(anchor_bid_d - offset_d, tick_size, rounding=ROUND_DOWN)
            if passive_target > Decimal("0") and passive_target < prop_dec:
                prop_dec = passive_target
                passive_offset_applied = True

        return {
            "allow_entry": len(reject_reasons) == 0,
            "reject_reasons": reject_reasons,
            "passive_offset_applied": passive_offset_applied,
            "adjusted_limit_price": float(prop_dec),
            "layer1": l1,
            "layer2": l2,
            "layer3": l3,
        }

    # ------------------------------------------------------------------
    # 統合判定: 発注済み待機買い指値 (PENDING_BUY) の即時緊急キャンセル判定
    # ------------------------------------------------------------------
    def should_cancel_resting_buy_order(
        self,
        symbol: str,
        order_price: float,
        order_size: float,
        atr_5m: float,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        発注済みの待機買い指値 (`PENDING_BUY`) について、逆選択（大口売り浴びせによる貫通約定）を
        回避するため即座に `cancel_order` すべきかを判定する。
        """
        sym = symbol.strip().upper()
        l1 = self.compute_layer1_obi(sym, order_price=order_price)
        l2 = self.compute_layer2_tfi(sym, now_sec=now_sec)
        l3 = self.compute_layer3_velocity_and_latency(sym, atr_5m=atr_5m, now_sec=now_sec)

        cancel_reasons: List[str] = []
        if l1["obi_5"] <= -0.45:
            cancel_reasons.append(f"OBI_5急悪化({l1['obi_5']:.2f} <= -0.45)")
        if l1["bid_queue_at_or_above_order"] < 2.0 * float(order_size):
            cancel_reasons.append(
                f"自注文価格の買板クッション枯渇({l1['bid_queue_at_or_above_order']:.4f} < {2.0 * float(order_size):.4f})"
            )
        if l2["tfi_60s"] <= -0.50:
            cancel_reasons.append(f"Taker売り毒性急増(TFI_60s={l2['tfi_60s']:.2f} <= -0.50)")
        if l2["ssi_60s"] >= 3.0:
            cancel_reasons.append(f"売り出来高スパイク(SSI_60s={l2['ssi_60s']:.2f} >= 3.0)")
        if l2["sell_sweep_detected"]:
            cancel_reasons.append("5秒内5連続価格切下げSELLスイープ検知")
        if l3["drop_atr_ratio_60s"] >= 0.80:
            cancel_reasons.append(f"60秒急落(-{l3['drop_atr_ratio_60s']:.2f}×ATR_5m <= -0.80×ATR_5m)")
        if l3["relative_spread_ratio"] >= 3.0:
            cancel_reasons.append(f"スプレッド異常拡大({l3['relative_spread_ratio']:.2f}倍 >= 3.0倍)")
        if l3["data_latency_sec"] > 5.0:
            cancel_reasons.append(f"板データ遅延({l3['data_latency_sec']:.1f}s > 5.0s)")

        return {
            "should_cancel": len(cancel_reasons) > 0,
            "cancel_reasons": cancel_reasons,
            "layer1": l1,
            "layer2": l2,
            "layer3": l3,
        }
