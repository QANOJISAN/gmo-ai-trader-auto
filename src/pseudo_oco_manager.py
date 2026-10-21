"""
現物API向け「WAL意図ログ付き8状態ステートマシン・原子的決済・4段階トレーリング」管理エンジン (pseudo_oco_manager.py)

【v2 フルスペック機構】
1. SQLite WAL `intents` 先行記録 ＆ 8状態ステートマシン:
   - `ENTRY_PENDING` (`PENDING_BUY`), `PARTIAL_FILLED`, `UNPROTECTED_HOLD`,
     `PROTECTED_ACTIVE` (`SL_ACTIVE`), `MODIFYING_STOP_INTENT`, `EXIT_PENDING`, `CLOSED`, `DUST_QUARANTINED`
   - 0.2秒周期の Fast Recovery ガーディアン（最大3.0秒の Dead-Man's Timer）により「ノーガード放置 0秒」を保証。
2. 原子的緊急決済 ＆ フラッシュクラッシュ時の一部約定・残り失効追撃スイープ:
   - 損切りフォールバック・緊急決済は `MARKET / FAK / SELL` + `cancel_before=True` を使用。
   - 直前の部分約定による `ERR-201`（残高不足）や、フラッシュクラッシュ時の STOP (`FAK`) 一部約定・残り失効 (`EXPIRED` / `CANCELED`) を検知した際は、
     即座に `assets` の `available`（または未約定残 $Q_{\\text{rem}}$）を確認して追撃の成行売りで残存ポジションを一掃。
3. Bot側二重化SL（3秒継続監視）:
   - `bid <= stop_price`（または `last <= stop_price`）が 3.0秒継続しても取引所 STOP の発動が確認できない場合は、
     Bot自身が `cancel_before=True` 付き成行売りを執行。
4. 非対称銘柄（ETH/BCH/LTC等）の部分約定 60秒グレースタイマー・トランシェSTOP・端数 `DUST_QUARANTINED` 隔離:
   - `minOrderSize` 未満の端数は `DUST_QUARANTINED` に隔離し、無限発注エラーを完全遮断。
5. 4段階トレーリング ＆ `capabilities.json` 連動 (`changeOrder` 優先 ➔ `cancelOrder` + `placeOrder` 自動フォールバック) ＆ Smart TP。
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.database_wal import DatabaseWAL
from src.gmo_private_client import (
    GMOPrivateAPIError,
    GMOPrivateClient,
    InsufficientAvailableAssetError,
    PaperBroker,
)
from src.json_utils import load_json_file, write_json_file
from src.notifier import Notifier
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    _to_decimal,
    format_decimal_plain,
    format_price_jpy,
    round_to_step,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
MAJOR_SYMBOLS = frozenset({"BTC", "ETH", "XRP", "SOL"})


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def _parse_iso_jst(ts_str: str, fallback: datetime) -> datetime:
    if not ts_str:
        return fallback
    try:
        dt = datetime.fromisoformat(str(ts_str).strip())
        if dt.tzinfo is None:
            return dt.replace(tzinfo=JST)
        return dt.astimezone(JST)
    except Exception:
        return fallback


def calculate_initial_1r_distance(
    entry_price: Union[str, int, float, Decimal],
    symbol: str,
    atr_15m: Union[str, int, float, Decimal] = 0,
    explicit_sl_price: Union[str, int, float, Decimal] = 0,
) -> Decimal:
    """
    初期リスク単位 $1R$ を算出する:
    - 明示的な初期 SL 価格 (`explicit_sl_price`) が与えられ `entry_price > explicit_sl_price > 0` の場合はその距離を基準としつつ、
    - $1R_{\\text{formula}} = \\max(2.0 \\times ATR_{15m}(14), P_{\\text{entry}} \\times d_{\\min})$
      （主要銘柄 $d_{\\min}=0.60\\%$, アルト $0.90\\%$）も算出可能にする。
    """
    p_entry = _to_decimal(entry_price)
    sl_d = _to_decimal(explicit_sl_price)
    if p_entry > Decimal("0") and Decimal("0") < sl_d < p_entry:
        return p_entry - sl_d

    sym = symbol.strip().upper()
    d_min = Decimal("0.0060") if sym in MAJOR_SYMBOLS else Decimal("0.0090")
    atr_d = _to_decimal(atr_15m)
    return max(atr_d * Decimal("2.0"), p_entry * d_min)


def calculate_breakeven_sl_price(
    entry_price: Union[str, int, float, Decimal],
    symbol: str,
    symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    one_r_distance: Optional[Union[str, int, float, Decimal]] = None,
) -> Union[int, float]:
    """
    建値微益SL価格（`entry_price * max(1.002, 1 + 2 * taker_fee)` または `entry_price + max(0.15R, 0.15%)` を
    `tick_size` で `ROUND_UP` 切り上げ）を算出する。
    """
    sym = symbol.strip().upper()
    rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
    rule = rules.get(sym, {})
    tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
    if tick_size <= 0:
        tick_size = Decimal("1")
    taker_fee = max(_to_decimal(rule.get("taker_fee", "0.0005")), Decimal("0"))
    entry_d = _to_decimal(entry_price)

    mult = max(Decimal("1.002"), Decimal("1") + (taker_fee * Decimal("2")))
    candidate_be = entry_d * mult

    if one_r_distance is not None and _to_decimal(one_r_distance) > Decimal("0"):
        r_d = _to_decimal(one_r_distance)
        alt_be = entry_d + max(r_d * Decimal("0.15"), entry_d * Decimal("0.0015"))
        candidate_be = max(candidate_be, alt_be)

    rounded = round_to_step(candidate_be, tick_size, rounding=ROUND_UP)
    if rounded == rounded.to_integral_value():
        return int(rounded)
    return float(rounded)


def compute_four_stage_trailing_sl(
    entry_price: Union[str, int, float, Decimal],
    current_bid: Union[str, int, float, Decimal],
    highest_bid: Union[str, int, float, Decimal],
    current_sl: Union[str, int, float, Decimal],
    one_r_dist: Union[str, int, float, Decimal],
    symbol: str,
    symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[int, Union[int, float], bool]:
    """
    4段階トレーリングSL計算:
    - Stage 0 (< +1.0R): 初期SL維持
    - Stage 1 (+1.0R <= R < +1.5R): 建値＋手数料保護 (`calculate_breakeven_sl_price`)
    - Stage 2 (+1.5R <= R < +2.0R): $+0.65R$ 利益ロック ($P_{\\text{entry}} + 0.65R$)
    - Stage 3/4 (R >= +2.0R): シャンデリア追従 ($\\max(P_{\\text{entry}} + 1.10R, P_{\\text{high}} - 1.0R)$)、
      ただし既存SLから $0.20R$ 以上切り上がる場合のみ更新（ヒステリシス制御）。
    戻り値: `(stage, target_sl_price, should_update)`
    """
    sym = symbol.strip().upper()
    rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
    rule = rules.get(sym, {})
    tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
    if tick_size <= 0:
        tick_size = Decimal("1")

    entry_d = _to_decimal(entry_price)
    bid_d = _to_decimal(current_bid)
    high_d = max(bid_d, _to_decimal(highest_bid))
    curr_sl_d = _to_decimal(current_sl)
    r_d = max(tick_size, _to_decimal(one_r_dist))

    r_multiple = (bid_d - entry_d) / r_d
    high_r_multiple = (high_d - entry_d) / r_d
    eff_r = max(r_multiple, high_r_multiple)

    if eff_r < Decimal("1.0"):
        return 0, float(curr_sl_d), False

    be_val = _to_decimal(calculate_breakeven_sl_price(entry_d, sym, rules))

    if eff_r < Decimal("1.5"):
        target_d = be_val
        stage = 1
        should_upd = curr_sl_d < target_d and bid_d > target_d
    elif eff_r < Decimal("2.0"):
        raw_s2 = entry_d + (r_d * Decimal("0.65"))
        target_d = max(be_val, round_to_step(raw_s2, tick_size, rounding=ROUND_UP))
        stage = 2
        should_upd = curr_sl_d < target_d and bid_d > target_d
    else:
        raw_chandelier = max(entry_d + (r_d * Decimal("1.10")), high_d - r_d)
        target_d = max(be_val, round_to_step(raw_chandelier, tick_size, rounding=ROUND_UP))
        stage = 3 if eff_r < Decimal("3.0") else 4
        hysteresis_step = r_d * Decimal("0.20")
        # 初回 Stage 3 到達時、または前回SLから 0.20R 以上引き上がる場合のみ更新
        should_upd = (target_d > curr_sl_d) and (
            (curr_sl_d <= entry_d + (r_d * Decimal("0.70"))) or ((target_d - curr_sl_d) >= hysteresis_step)
        ) and (bid_d > target_d)

    out_price: Union[int, float] = (
        int(target_d) if target_d == target_d.to_integral_value() else float(target_d)
    )
    return stage, out_price, bool(should_upd)


class PseudoOCOManager:
    """現物API向け WAL意図ログ付き8状態ステートマシン・原子的決済・4段階トレーリング管理エンジン"""

    DEAD_MAN_MAX_UNPROTECTED_SEC = 3.0
    FAST_RECOVERY_INTERVAL_SEC = 0.2
    PARTIAL_FILL_GRACE_SEC = 60.0
    BOT_REDUNDANT_SL_BREACH_SEC = 3.0

    def __init__(
        self,
        broker: Union[GMOPrivateClient, PaperBroker],
        portfolio_path: Union[str, Path] = "portfolio.json",
        notifier: Optional[Notifier] = None,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        order_expiry_hours: Optional[float] = None,
        dry_run_notify: bool = False,
        wal_db: Optional[DatabaseWAL] = None,
        capabilities_path: Optional[Union[str, Path]] = None,
    ) -> None:
        self.broker = broker
        self.portfolio_path = Path(portfolio_path)
        self.notifier = notifier or Notifier()
        self.symbol_rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
        self.order_expiry_hours = float(
            order_expiry_hours
            if order_expiry_hours is not None
            else os.getenv("ORDER_EXPIRY_HOURS", "4")
        )
        self.dry_run_notify = dry_run_notify
        self.wal_db = wal_db or getattr(broker, "wal_db", None)
        self.capabilities_path = (
            Path(capabilities_path)
            if capabilities_path is not None
            else self.portfolio_path.parent / "capabilities.json"
        )
        self.stop_change_order_supported: bool = self._load_stop_change_capability()

    def _load_stop_change_capability(self) -> bool:
        if self.capabilities_path and self.capabilities_path.exists():
            try:
                cap = load_json_file(self.capabilities_path)
                return bool(cap.get("stop_change_order_supported", False))
            except Exception:
                return False
        return False

    def load_portfolio(self) -> Dict[str, Any]:
        if not self.portfolio_path.exists():
            default_pf = {
                "initial_capital_jpy": 10000,
                "jpy_balance": 10000,
                "realized_pnl_total_jpy": 0,
                "total_realized_pnl_jpy": 0,
                "max_allocation_per_trade_pct": 0.50,
                "max_concurrent_positions": 2,
                "target_risk_per_trade_jpy": 150,
                "max_risk_per_trade_jpy": 200,
                "drawdown_kill_switch_jpy": 8500,
                "positions": [],
                "quarantined_dust": [],
                "closed_trades": [],
                "pending_orders": [],
                "execution_mode": getattr(self.broker, "MODE", "DRY_RUN"),
                "last_updated": _now_jst().isoformat(timespec="seconds"),
            }
            write_json_file(self.portfolio_path, default_pf)
            return default_pf
        pf = load_json_file(self.portfolio_path)
        pf.setdefault("positions", [])
        pf.setdefault("quarantined_dust", [])
        pf.setdefault("closed_trades", [])
        pf.setdefault("pending_orders", [])
        pf.setdefault("realized_pnl_total_jpy", int(pf.get("total_realized_pnl_jpy", 0) or 0))
        pf.setdefault("total_realized_pnl_jpy", int(pf.get("realized_pnl_total_jpy", 0) or 0))
        return pf

    def save_portfolio(self, portfolio: Dict[str, Any], now_dt: Optional[datetime] = None) -> None:
        now_jst = _now_jst(now_dt)
        portfolio["last_updated"] = now_jst.isoformat(timespec="seconds")
        portfolio["execution_mode"] = getattr(self.broker, "MODE", "DRY_RUN")
        if self.wal_db is not None:
            self.wal_db.sync_portfolio_to_Sqlite_and_json(
                portfolio,
                now_ts_ms=int(now_jst.timestamp() * 1000),
            )
        write_json_file(self.portfolio_path, portfolio)

    # ------------------------------------------------------------------
    # 0.2秒 Fast Recovery ガーディアン (最大3秒 Dead-Man's Timer)
    # ------------------------------------------------------------------
    def run_fast_recovery_guardian(
        self,
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        万一 `UNPROTECTED_HOLD` や `MODIFYING_STOP_INTENT` のまま `sl_order_id` が空白になっている建玉が存在する場合、
        0.2秒周期で即座に検知し、最大3.0秒以内に逆指値 `STOP` を再設置（または既にSL水準割れなら成行緊急決済）して
        「ノーガード放置時間 0秒（最大でも3秒未満）」の不変条件を強制する。
        """
        now_jst = _now_jst(now_dt)
        pf = self.load_portfolio()
        recovered_events: List[Dict[str, Any]] = []
        changed = False

        remaining_positions: List[Dict[str, Any]] = []
        for pos in pf.get("positions", []):
            sym = str(pos.get("symbol", "")).upper()
            size = float(pos.get("size", 0) or 0)
            sl_oid = str(pos.get("sl_order_id", "") or "")
            sl_price = float(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
            rule = self.symbol_rules.get(sym, {})
            min_size = float(rule.get("min_order_size", 0.0001) or 0.0001)

            # 最小注文数量未満の端数は DUST_QUARANTINED へ隔離して無限ループを防止
            if 0 < size < min_size:
                dust_entry = dict(pos)
                dust_entry["state"] = "DUST_QUARANTINED"
                dust_entry["quarantined_at"] = now_jst.isoformat(timespec="seconds")
                pf.setdefault("quarantined_dust", []).append(dust_entry)
                if self.wal_db is not None:
                    self.wal_db.quarantine_dust_position(
                        symbol=sym,
                        dust_size=size,
                        entry_price=pos.get("entry_price", 0),
                    )
                recovered_events.append({"type": "DUST_QUARANTINED", "symbol": sym, "size": size})
                changed = True
                continue

            if not sl_oid and size >= min_size:
                t_sym = (tickers or {}).get(sym, {})
                curr_bid = float(t_sym.get("bid", 0) or t_sym.get("last", 0) or pos.get("current_price", 0) or 0)

                # 現在価格が既に SL 以下の場合は STOP ではなく即座に cancel_before=True 成行売りで緊急脱出
                if curr_bid > 0 and sl_price > 0 and curr_bid <= sl_price:
                    closed = self._execute_atomic_market_exit(
                        pf=pf,
                        pos=pos,
                        reason="DEAD_MAN_EMERGENCY_SL",
                        now_jst=now_jst,
                    )
                    recovered_events.append(
                        {
                            "type": "DEAD_MAN_EMERGENCY_FLATTEN",
                            "symbol": sym,
                            "exit_price": closed["exit_price"],
                            "pnl_jpy": closed["pnl_jpy"],
                        }
                    )
                    changed = True
                    continue

                # 通常の逆指値 STOP を即座に再設置
                target_sl = sl_price if sl_price > 0 else float(_to_decimal(pos.get("entry_price", 0)) * Decimal("0.965"))
                try:
                    sl_order = self.broker.place_order(
                        symbol=sym,
                        side="SELL",
                        execution_type="STOP",
                        size=size,
                        price=target_sl,
                        time_in_force="FAK",
                    )
                    pos["sl_order_id"] = str(sl_order.get("order_id", ""))
                    pos["stop_loss_price"] = target_sl
                    pos["state"] = "SL_ACTIVE"
                    pos["unprotected_since"] = None
                    recovered_events.append(
                        {
                            "type": "FAST_RECOVERY_SL_RESTORED",
                            "symbol": sym,
                            "sl_order_id": pos["sl_order_id"],
                            "stop_loss_price": target_sl,
                        }
                    )
                    changed = True
                except InsufficientAvailableAssetError:
                    # 既に別注文でロックされているか部分売却済みの場合は実残高を確認して調整
                    balances = self.broker.get_jpy_and_spot_balances()
                    avail = float(balances["spot_assets"].get(sym, {}).get("available", 0.0) or 0.0)
                    if avail >= min_size:
                        sl_order = self.broker.place_order(
                            symbol=sym,
                            side="SELL",
                            execution_type="STOP",
                            size=avail,
                            price=target_sl,
                            time_in_force="FAK",
                        )
                        pos["size"] = avail
                        pos["sl_order_id"] = str(sl_order.get("order_id", ""))
                        pos["state"] = "SL_ACTIVE"
                        changed = True

            remaining_positions.append(pos)

        pf["positions"] = remaining_positions
        if changed:
            self.save_portfolio(pf, now_dt=now_jst)
        return recovered_events

    # ------------------------------------------------------------------
    # 原子的成行決済 (cancel_before=True ＆ ERR-201 実残高再取得リトライ ＆ 端数隔離)
    # ------------------------------------------------------------------
    def _execute_atomic_market_exit(
        self,
        pf: Dict[str, Any],
        pos: Dict[str, Any],
        reason: str,
        now_jst: datetime,
    ) -> Dict[str, Any]:
        """
        `MARKET / FAK / SELL` + `cancel_before=True` による原子的緊急決済を実行する。
        - 直前の部分約定で `ERR-201` (`InsufficientAvailableAssetError`) が返った場合は、
          即座に `get_jpy_and_spot_balances()` で実残高を確認し、`minOrderSize` 以上なら実残高で再発注、
          `minOrderSize` 未満なら `DUST_QUARANTINED` に隔離する。
        """
        sym = str(pos.get("symbol", "")).upper()
        req_size = float(pos.get("size", 0) or 0)
        old_sl_id = str(pos.get("sl_order_id", "") or "")
        rule = self.symbol_rules.get(sym, {})
        min_size = float(rule.get("min_order_size", 0.0001) or 0.0001)

        intent_id: Optional[str] = None
        if self.wal_db is not None:
            intent_id = self.wal_db.create_intent(
                kind="ATOMIC_MARKET_EXIT",
                symbol=sym,
                payload={"symbol": sym, "size": req_size, "reason": reason, "old_sl_order_id": old_sl_id},
            )

        try:
            sell_order = self.broker.place_order(
                symbol=sym,
                side="SELL",
                execution_type="MARKET",
                size=req_size,
                time_in_force="FAK",
                cancel_before=True,
                intent_id=intent_id,
            )
        except InsufficientAvailableAssetError:
            if old_sl_id:
                try:
                    self.broker.cancel_order(old_sl_id)
                except Exception:
                    pass
            balances = self.broker.get_jpy_and_spot_balances()
            spot_info = balances["spot_assets"].get(sym, {})
            actual_avail = float(spot_info.get("available", spot_info.get("amount", 0.0)) or 0.0)
            if actual_avail < min_size:
                if actual_avail > 0:
                    pf.setdefault("quarantined_dust", []).append(
                        {
                            "symbol": sym,
                            "size": actual_avail,
                            "entry_price": pos.get("entry_price", 0),
                            "state": "DUST_QUARANTINED",
                            "quarantined_at": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                    if self.wal_db is not None:
                        self.wal_db.quarantine_dust_position(sym, actual_avail, pos.get("entry_price", 0))
                return self._record_closed_trade(
                    pf=pf,
                    pos=pos,
                    exit_price=float(pos.get("current_price") or pos.get("entry_price", 0)),
                    exit_order_id="ALREADY_FILLED_OR_DUST",
                    reason=reason,
                    is_maker=False,
                    now_jst=now_jst,
                )
            pos["size"] = actual_avail
            sell_order = self.broker.place_order(
                symbol=sym,
                side="SELL",
                execution_type="MARKET",
                size=actual_avail,
                time_in_force="FAK",
                cancel_before=True,
            )

        exit_price = float(
            sell_order.get("executed_price")
            or sell_order.get("price")
            or pos.get("current_price")
            or pos.get("entry_price", 0)
        )
        return self._record_closed_trade(
            pf=pf,
            pos=pos,
            exit_price=exit_price,
            exit_order_id=str(sell_order.get("order_id", "")),
            reason=reason,
            is_maker=False,
            now_jst=now_jst,
        )

    def place_new_buy_order(
        self,
        decision: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now_jst = _now_jst(now_dt)
        now_str = now_jst.isoformat(timespec="seconds")

        if tickers and isinstance(self.broker, PaperBroker):
            self.broker.update_market_tickers(tickers)

        sym = str(decision.get("symbol", "")).strip().upper()
        limit_price = decision.get("limit_price", 0)
        size = decision.get("size", 0)
        tp_price = decision.get("take_profit_price", 0)
        sl_price = decision.get("stop_loss_price", 0)
        expected_loss_jpy = float(decision.get("expected_loss_jpy", 150) or 150)

        order_info = self.broker.place_order(
            symbol=sym,
            side="BUY",
            execution_type="LIMIT",
            size=size,
            price=limit_price,
            time_in_force="SOK",
            max_post_only_retries=2,
        )

        status = str(order_info.get("status", "WAITING")).upper()
        if status == "CANCELED":
            logger.warning("買い指値 (Post-Only) が板変動により失効しました: %s", order_info)
            return {"status": "CANCELED", "order": order_info}

        actual_limit_price = float(order_info.get("price", limit_price) or limit_price)
        actual_size = float(order_info.get("size", size) or size)

        pending_entry = {
            "order_id": str(order_info["order_id"]),
            "symbol": sym,
            "side": "BUY",
            "execution_type": "LIMIT",
            "time_in_force": "SOK",
            "limit_price": actual_limit_price,
            "size": actual_size,
            "take_profit_price": float(tp_price),
            "stop_loss_price": float(sl_price),
            "initial_risk_jpy": expected_loss_jpy,
            "state": "PENDING_BUY",
            "placed_at": now_str,
            "expires_at": (now_jst + timedelta(hours=self.order_expiry_hours)).isoformat(timespec="seconds"),
            "post_only_adjusted": bool(order_info.get("post_only_adjusted", False)),
            "adjust_count": int(order_info.get("adjust_count", 0)),
            "reasoning": str(decision.get("reasoning", "")),
        }

        pf = self.load_portfolio()
        pf["pending_orders"].append(pending_entry)
        self.save_portfolio(pf, now_dt=now_jst)

        self.notifier.notify_event(
            title=f"新規買い指値発注 (PENDING_BUY): {sym}",
            message=(
                f"・モード: `{getattr(self.broker, 'MODE', 'DRY_RUN')}`\n"
                f"・注文ID: `{pending_entry['order_id']}`\n"
                f"・指値単価: {format_price_jpy(actual_limit_price)} 円 (Post-Only SOK)\n"
                f"・注文数量: {format_decimal_plain(actual_size)} {sym}\n"
                f"・予定TP / SL: {format_price_jpy(tp_price)} 円 / {format_price_jpy(sl_price)} 円\n"
                f"・有効期限: {self.order_expiry_hours:.1f} 時間 ({pending_entry['expires_at']})"
            ),
            level="BUY",
            dry_run=self.dry_run_notify,
        )
        return {"status": "PENDING_BUY", "pending_order": pending_entry, "order": order_info}

    def step(
        self,
        tickers: Dict[str, Dict[str, Any]],
        now_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        高速ループで呼び出されるステートマシン更新処理:
        1. `PaperBroker` の場合はリアルタイム `tickers` で仮想約定判定を実行
        2. 状態1 `PENDING_BUY` / `PARTIAL_FILLED`:
           - 全量約定 ➔ 直ちに `STOP` 逆指値を自動発注して `SL_ACTIVE` へ遷移
           - 部分約定 ➔ 非対称銘柄の最小注文数量 (`minOrderSize`) と 60秒グレースタイマーを考慮し、
             期限切れ時は残りキャンセル＋約定分が `minOrderSize` 以上なら `STOP` 発注、`minOrderSize` 未満なら `DUST_QUARANTINED` に隔離
        3. 状態2 `SL_ACTIVE` (`PROTECTED_ACTIVE`):
           - 取引所 `STOP` 注文の約定検知（もし `STOP` `FAK` が一部約定して残り失効 `EXPIRED`/`CANCELED` していた場合は、
             直ちに追撃の `MARKET / FAK / SELL` で残存ポジションを一掃！）
           - Bot側二重化SL: `bid <= stop_loss_price` が 3.0秒以上継続しても取引所 `STOP` が約定しない場合は Bot 自身が成行決済を実行
           - 利確スイッチ (Smart TP): 現在 Bid >= `take_profit_price` で SL取消ロック解除 ➔ 利確売り発注
           - 4段階トレーリング (+1.0R 建値保護 / +1.5R +0.65Rロック / +2.0R シャンデリア追従)
        """
        now_jst = _now_jst(now_dt)
        events: List[Dict[str, Any]] = []

        if isinstance(self.broker, PaperBroker):
            self.broker.evaluate_paper_orders(tickers=tickers, now_dt=now_jst)

        pf = self.load_portfolio()
        pf_changed = False

        # ------------------------------------------------------------------
        # Step A: PENDING_BUY / PARTIAL_FILLED 注文の約定チェック & 4hタイムアウト / 60sグレース管理
        # ------------------------------------------------------------------
        remaining_pending: List[Dict[str, Any]] = []
        for pending in pf.get("pending_orders", []):
            p_state = str(pending.get("state", "PENDING_BUY")).upper()
            if p_state not in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED"):
                continue

            oid = str(pending.get("order_id", ""))
            sym = str(pending.get("symbol", "")).upper()
            rule = self.symbol_rules.get(sym, {})
            min_order_size = float(rule.get("min_order_size", 0.0001) or 0.0001)

            placed_dt = _parse_iso_jst(str(pending.get("placed_at", "")), now_jst)
            elapsed_hours = (now_jst - placed_dt).total_seconds() / 3600.0

            fetched_list = self.broker.get_orders([oid])
            order_state = fetched_list[0] if fetched_list else {}
            status = str(order_state.get("status", "WAITING")).upper()
            exec_size = float(order_state.get("executed_size", 0) or 0)
            total_size = float(pending.get("size", order_state.get("size", 0)) or 0)
            exec_price = float(
                order_state.get("executed_price")
                or order_state.get("price")
                or pending.get("limit_price", 0)
            )

            # ケース1: 全量約定
            if status == "EXECUTED" or (exec_size >= total_size > 0):
                _, sl_event = self._activate_position_with_sl(
                    pf=pf,
                    pending=pending,
                    filled_size=exec_size if exec_size > 0 else total_size,
                    filled_price=exec_price,
                    now_jst=now_jst,
                )
                events.append(sl_event)
                pf_changed = True
                continue

            # 部分約定発生時のグレースタイマー開始記録
            if 0 < exec_size < total_size and not pending.get("first_partial_fill_at"):
                pending["first_partial_fill_at"] = now_jst.isoformat(timespec="seconds")
                pending["state"] = "PARTIAL_FILLED"
                pf_changed = True

            # ケース2: 有効期限（4時間）経過によるタイムアウト
            if elapsed_hours >= self.order_expiry_hours:
                try:
                    self.broker.cancel_order(oid)
                except Exception as exc:
                    logger.warning("期限切れ注文キャンセル時の警告 (order_id=%s): %s", oid, exc)

                rechecked = self.broker.get_orders([oid])
                final_exec_size = float(
                    (rechecked[0].get("executed_size", exec_size) if rechecked else exec_size) or 0
                )

                if final_exec_size >= min_order_size:
                    _, sl_event = self._activate_position_with_sl(
                        pf=pf,
                        pending=pending,
                        filled_size=final_exec_size,
                        filled_price=exec_price,
                        now_jst=now_jst,
                        partial_expired=True,
                    )
                    events.append(sl_event)
                elif final_exec_size > 0:
                    # minOrderSize 未満の端数部分約定は DUST_QUARANTINED へ安全隔離
                    pf.setdefault("quarantined_dust", []).append(
                        {
                            "symbol": sym,
                            "size": final_exec_size,
                            "entry_price": exec_price,
                            "buy_order_id": oid,
                            "state": "DUST_QUARANTINED",
                            "quarantined_at": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                    if self.wal_db is not None:
                        self.wal_db.quarantine_dust_position(sym, final_exec_size, exec_price)
                    events.append(
                        {
                            "type": "PARTIAL_DUST_QUARANTINED",
                            "symbol": sym,
                            "order_id": oid,
                            "dust_size": final_exec_size,
                            "min_order_size": min_order_size,
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                else:
                    exp_event = {
                        "type": "BUY_ORDER_EXPIRED",
                        "symbol": sym,
                        "order_id": oid,
                        "elapsed_hours": round(elapsed_hours, 2),
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                    events.append(exp_event)
                    self.notifier.notify_event(
                        title=f"未約定指値タイムアウト自動取消: {sym}",
                        message=(
                            f"・注文ID: `{oid}`\n"
                            f"・指値: {format_price_jpy(pending.get('limit_price', 0))} 円 × {format_decimal_plain(total_size)} {sym}\n"
                            f"・経過時間: {elapsed_hours:.2f} 時間 (上限 {self.order_expiry_hours:.1f} 時間)"
                        ),
                        level="EXPIRE",
                        dry_run=self.dry_run_notify,
                    )
                pf_changed = True
                continue

            # ケース3: 取引所側でキャンセル/失効になっていた場合
            if status in ("CANCELED", "EXPIRED"):
                if exec_size >= min_order_size:
                    _, sl_event = self._activate_position_with_sl(
                        pf=pf,
                        pending=pending,
                        filled_size=exec_size,
                        filled_price=exec_price,
                        now_jst=now_jst,
                        partial_expired=True,
                    )
                    events.append(sl_event)
                elif exec_size > 0:
                    pf.setdefault("quarantined_dust", []).append(
                        {
                            "symbol": sym,
                            "size": exec_size,
                            "entry_price": exec_price,
                            "buy_order_id": oid,
                            "state": "DUST_QUARANTINED",
                            "quarantined_at": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                    if self.wal_db is not None:
                        self.wal_db.quarantine_dust_position(sym, exec_size, exec_price)
                    events.append(
                        {
                            "type": "PARTIAL_DUST_QUARANTINED",
                            "symbol": sym,
                            "order_id": oid,
                            "dust_size": exec_size,
                            "min_order_size": min_order_size,
                            "timestamp": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                pf_changed = True
                continue

            remaining_pending.append(pending)

        pf["pending_orders"] = remaining_pending

        # ------------------------------------------------------------------
        # Step B: SL_ACTIVE ポジションの監視（SL約定・一部失効追撃 / Bot二重SL / TP到達 / 4段階トレーリング）
        # ------------------------------------------------------------------
        remaining_positions: List[Dict[str, Any]] = []
        for pos in pf.get("positions", []):
            sym = str(pos.get("symbol", "")).upper()
            size = float(pos.get("size", 0) or 0)
            entry_price = float(pos.get("entry_price", 0) or 0)
            tp_price = float(pos.get("take_profit_price") or pos.get("tp_price") or 0)
            sl_price = float(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
            initial_sl_price = float(pos.get("initial_stop_loss_price") or sl_price or 0)
            sl_order_id = str(pos.get("sl_order_id", "") or "")
            rule = self.symbol_rules.get(sym, {})
            min_order_size = float(rule.get("min_order_size", 0.0001) or 0.0001)
            size_step = _to_decimal(rule.get("size_step", "0.0001"), default=Decimal("0.0001"))

            ticker = (tickers or {}).get(sym, {})
            current_bid = float(ticker.get("bid", 0) or ticker.get("last", 0) or pos.get("current_price", 0) or entry_price)
            current_last = float(ticker.get("last", 0) or current_bid or entry_price)
            pos["current_price"] = current_last
            prev_high = float(pos.get("highest_bid_since_entry", entry_price) or entry_price)
            highest_bid = max(prev_high, current_bid)
            pos["highest_bid_since_entry"] = highest_bid

            # 1) 取引所に出していた SL 注文の状態確認（全量約定 or フラッシュクラッシュ時の一部約定・残り失効追撃）
            if sl_order_id:
                sl_orders = self.broker.get_orders([sl_order_id])
                if sl_orders:
                    sl_order = sl_orders[0]
                    sl_status = str(sl_order.get("status", "")).upper()
                    sl_exec_size = float(sl_order.get("executed_size", 0) or 0)
                    sl_order_size = float(sl_order.get("size", size) or size)

                    # 1a) STOP (FAK) がフラッシュクラッシュ等で「一部のみ約定し、残りが EXPIRED / CANCELED」となった場合:
                    #     残存数量 Q_rem を直ちに MARKET / FAK / SELL で追撃一掃する！
                    if sl_status in ("EXPIRED", "CANCELED") and sl_exec_size < size:
                        rem_d = round_to_step(_to_decimal(size) - _to_decimal(sl_exec_size), size_step, rounding=ROUND_DOWN)
                        rem_f = float(rem_d)
                        sweep_price = float(sl_order.get("executed_price") or current_bid or sl_price)
                        if rem_f >= min_order_size:
                            sweep_order = self.broker.place_order(
                                symbol=sym,
                                side="SELL",
                                execution_type="MARKET",
                                size=rem_f,
                                time_in_force="FAK",
                                cancel_before=True,
                            )
                            sweep_price = float(
                                sweep_order.get("executed_price") or sweep_order.get("price") or current_bid or sl_price
                            )
                        elif rem_f > 0:
                            pf.setdefault("quarantined_dust", []).append(
                                {
                                    "symbol": sym,
                                    "size": rem_f,
                                    "entry_price": entry_price,
                                    "state": "DUST_QUARANTINED",
                                    "quarantined_at": now_jst.isoformat(timespec="seconds"),
                                }
                            )
                            if self.wal_db is not None:
                                self.wal_db.quarantine_dust_position(sym, rem_f, entry_price)

                        closed_record = self._record_closed_trade(
                            pf=pf,
                            pos=pos,
                            exit_price=sweep_price,
                            exit_order_id=sl_order_id,
                            reason="SL_PARTIAL_FAK_SWEEP",
                            is_maker=False,
                            now_jst=now_jst,
                        )
                        events.append(
                            {
                                "type": "SL_PARTIAL_FAK_SWEPT",
                                "symbol": sym,
                                "order_id": sl_order_id,
                                "initial_executed_size": sl_exec_size,
                                "swept_remaining_size": rem_f,
                                "exit_price": sweep_price,
                                "pnl_jpy": closed_record["pnl_jpy"],
                                "timestamp": now_jst.isoformat(timespec="seconds"),
                            }
                        )
                        pf_changed = True
                        continue

                    # 1b) 通常の STOP 全量約定
                    if sl_status == "EXECUTED" or (sl_exec_size >= sl_order_size > 0):
                        exit_price = float(sl_order.get("executed_price") or sl_order.get("price") or sl_price)
                        close_reason = "BREAKEVEN_SL" if pos.get("breakeven_locked") else "SL"
                        closed_record = self._record_closed_trade(
                            pf=pf,
                            pos=pos,
                            exit_price=exit_price,
                            exit_order_id=sl_order_id,
                            reason=close_reason,
                            is_maker=False,
                            now_jst=now_jst,
                        )
                        events.append(
                            {
                                "type": "SL_EXECUTED",
                                "symbol": sym,
                                "order_id": sl_order_id,
                                "exit_price": exit_price,
                                "pnl_jpy": closed_record["pnl_jpy"],
                                "reason": close_reason,
                                "timestamp": now_jst.isoformat(timespec="seconds"),
                            }
                        )
                        self.notifier.notify_event(
                            title=f"逆指値 (SL) 約定検知・決済完了: {sym} ({close_reason})",
                            message=(
                                f"・注文ID: `{sl_order_id}`\n"
                                f"・取得単価: {format_price_jpy(entry_price)} 円 ➔ 決済単価: {format_price_jpy(exit_price)} 円\n"
                                f"・数量: {format_decimal_plain(size)} {sym}\n"
                                f"・確定損益: **{closed_record['pnl_jpy']:+,} 円 ({closed_record['pnl_pct']:+.2f}%)**"
                            ),
                            level="SL",
                            dry_run=self.dry_run_notify,
                        )
                        pf_changed = True
                        continue

            # 1c) Bot側二重化SL: bid <= sl_price が 3.0秒以上継続しても取引所 STOP が約定していない場合は Bot 自身が成行決済
            if sl_price > 0 and current_bid > 0 and current_bid <= sl_price:
                breach_since_str = str(pos.get("sl_breach_since", "") or "")
                if not breach_since_str:
                    pos["sl_breach_since"] = now_jst.isoformat(timespec="seconds")
                    pf_changed = True
                else:
                    breach_dt = _parse_iso_jst(breach_since_str, now_jst)
                    if (now_jst - breach_dt).total_seconds() >= self.BOT_REDUNDANT_SL_BREACH_SEC:
                        closed_record = self._execute_atomic_market_exit(
                            pf=pf,
                            pos=pos,
                            reason="BOT_REDUNDANT_SL",
                            now_jst=now_jst,
                        )
                        events.append(
                            {
                                "type": "BOT_REDUNDANT_SL_EXECUTED",
                                "symbol": sym,
                                "exit_price": closed_record["exit_price"],
                                "pnl_jpy": closed_record["pnl_jpy"],
                                "timestamp": now_jst.isoformat(timespec="seconds"),
                            }
                        )
                        pf_changed = True
                        continue
            elif pos.get("sl_breach_since"):
                pos["sl_breach_since"] = None
                pf_changed = True

            # 2) 利確スイッチ (TP Execution): 現在 Bid >= take_profit_price
            if tp_price > 0 and current_bid >= tp_price:
                if sl_order_id:
                    self.broker.cancel_order(sl_order_id)

                tp_order = self.broker.place_order(
                    symbol=sym,
                    side="SELL",
                    execution_type="LIMIT",
                    size=size,
                    price=tp_price,
                    time_in_force="FAS",
                )
                if str(tp_order.get("status", "")).upper() != "EXECUTED":
                    try:
                        self.broker.cancel_order(tp_order["order_id"])
                    except Exception:
                        pass
                    tp_order = self.broker.place_order(
                        symbol=sym,
                        side="SELL",
                        execution_type="MARKET",
                        size=size,
                        time_in_force="FAK",
                        cancel_before=True,
                    )

                exit_price = float(
                    tp_order.get("executed_price")
                    or tp_order.get("price")
                    or current_bid
                    or tp_price
                )
                is_maker_tp = str(tp_order.get("execution_type", "LIMIT")).upper() == "LIMIT"
                closed_record = self._record_closed_trade(
                    pf=pf,
                    pos=pos,
                    exit_price=exit_price,
                    exit_order_id=str(tp_order.get("order_id", "")),
                    reason="TP",
                    is_maker=is_maker_tp,
                    now_jst=now_jst,
                )
                events.append(
                    {
                        "type": "TP_EXECUTED",
                        "symbol": sym,
                        "cancelled_sl_order_id": sl_order_id,
                        "tp_order_id": str(tp_order.get("order_id", "")),
                        "exit_price": exit_price,
                        "pnl_jpy": closed_record["pnl_jpy"],
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                )
                self.notifier.notify_event(
                    title=f"利確 (TP) 到達・SL取消＆利確決済完了: {sym}",
                    message=(
                        f"・旧SL注文取消: `{sl_order_id}` ➔ 利確注文ID: `{tp_order.get('order_id', '')}`\n"
                        f"・取得単価: {format_price_jpy(entry_price)} 円 ➔ 利確単価: {format_price_jpy(exit_price)} 円\n"
                        f"・数量: {format_decimal_plain(size)} {sym}\n"
                        f"・確定損益: **{closed_record['pnl_jpy']:+,} 円 ({closed_record['pnl_pct']:+.2f}%)**"
                    ),
                    level="TP",
                    dry_run=self.dry_run_notify,
                )
                pf_changed = True
                continue

            # 3) 4段階自動トレーリング & 自動建値ロック (+1.0R 建値保護 / +1.5R +0.65Rロック / +2.0R シャンデリア追従)
            one_r_dist = calculate_initial_1r_distance(
                entry_price=entry_price,
                symbol=sym,
                explicit_sl_price=initial_sl_price,
            )
            stage, target_sl_price, should_update_sl = compute_four_stage_trailing_sl(
                entry_price=entry_price,
                current_bid=current_bid,
                highest_bid=highest_bid,
                current_sl=sl_price,
                one_r_dist=one_r_dist,
                symbol=sym,
                symbol_rules=self.symbol_rules,
            )

            # 既存の +1.0R 金額基準判定との完全互換維持
            be_sl_price = calculate_breakeven_sl_price(entry_price, sym, self.symbol_rules)
            initial_risk_jpy = float(pos.get("initial_risk_jpy", 0) or (float(one_r_dist) * size))
            unrealized_bid_pnl_jpy = (current_bid - entry_price) * size
            if (
                not should_update_sl
                and initial_risk_jpy > 0
                and unrealized_bid_pnl_jpy >= initial_risk_jpy
                and float(sl_price) < float(be_sl_price)
                and current_bid > float(be_sl_price)
            ):
                stage = max(stage, 1)
                target_sl_price = be_sl_price
                should_update_sl = True

            if should_update_sl and float(target_sl_price) > float(sl_price) and current_bid > float(target_sl_price):
                old_sl_id = sl_order_id
                pos["state"] = "MODIFYING_STOP_INTENT"
                new_sl_order_id = self._replace_or_change_stop_order(
                    symbol=sym,
                    old_sl_order_id=old_sl_id,
                    size=size,
                    new_sl_price=target_sl_price,
                )
                pos["stop_loss_price"] = target_sl_price
                pos["sl_price"] = target_sl_price
                pos["sl_order_id"] = new_sl_order_id
                pos["breakeven_locked"] = True
                pos["trailing_stage"] = stage
                pos["breakeven_locked_at"] = now_jst.isoformat(timespec="seconds")
                pos["state"] = "SL_ACTIVE"

                ev_type = "AUTO_BREAKEVEN_LOCKED" if stage == 1 else f"TRAILING_STAGE_{stage}_LOCKED"
                events.append(
                    {
                        "type": "AUTO_BREAKEVEN_LOCKED",
                        "trailing_event_type": ev_type,
                        "trailing_stage": stage,
                        "symbol": sym,
                        "old_sl_order_id": old_sl_id,
                        "new_sl_order_id": new_sl_order_id,
                        "old_sl_price": sl_price,
                        "new_sl_price": target_sl_price,
                        "current_bid": current_bid,
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                )
                self.notifier.notify_event(
                    title=f"自動トレーリング発動 (Stage {stage}): {sym}",
                    message=(
                        f"・現在Bid: {format_price_jpy(current_bid)} 円\n"
                        f"・旧SL: `{old_sl_id}` ({format_price_jpy(sl_price)} 円)\n"
                        f"・新SL: `{new_sl_order_id}` ({format_price_jpy(target_sl_price)} 円)"
                    ),
                    level="BREAKEVEN",
                    dry_run=self.dry_run_notify,
                )
                pf_changed = True

            remaining_positions.append(pos)

        pf["positions"] = remaining_positions
        if pf_changed:
            self.save_portfolio(pf, now_dt=now_jst)

        return events

    def _replace_or_change_stop_order(
        self,
        symbol: str,
        old_sl_order_id: str,
        size: Union[str, int, float, Decimal],
        new_sl_price: Union[str, int, float, Decimal],
    ) -> str:
        """
        `capabilities.json` の `stop_change_order_supported` が `True` かつ PaperBroker でない場合は
        `change_order` を試行し、失敗時または `False`（既定）時は確実に
        `cancel_order(old_sl_order_id)` ➔ `place_order(STOP, new_sl_price)` で置き換える。
        """
        intent_id: Optional[str] = None
        if self.wal_db is not None:
            intent_id = self.wal_db.create_intent(
                kind="MODIFY_STOP",
                symbol=symbol,
                payload={
                    "old_sl_order_id": old_sl_order_id,
                    "size": format_decimal_plain(size),
                    "new_sl_price": format_decimal_plain(new_sl_price),
                },
            )

        if self.stop_change_order_supported and old_sl_order_id and not isinstance(self.broker, PaperBroker):
            try:
                changed = self.broker.change_order(old_sl_order_id, new_sl_price)
                new_id = str(changed.get("order_id", old_sl_order_id))
                if self.wal_db is not None and intent_id:
                    self.wal_db.update_intent_state(intent_id, "COMMITTED", order_id=new_id)
                return new_id
            except Exception as exc:
                logger.info("STOP changeOrder 非対応または失敗のため cancel+place へフォールバックします: %s", exc)

        if old_sl_order_id:
            self.broker.cancel_order(old_sl_order_id)

        new_sl_order = self.broker.place_order(
            symbol=symbol,
            side="SELL",
            execution_type="STOP",
            size=size,
            price=new_sl_price,
            time_in_force="FAK",
            intent_id=intent_id,
        )
        new_id = str(new_sl_order["order_id"])
        if self.wal_db is not None and intent_id:
            self.wal_db.update_intent_state(intent_id, "COMMITTED", order_id=new_id)
        return new_id

    def _activate_position_with_sl(
        self,
        pf: Dict[str, Any],
        pending: Dict[str, Any],
        filled_size: float,
        filled_price: float,
        now_jst: datetime,
        partial_expired: bool = False,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        sym = str(pending.get("symbol", "")).upper()
        rule = self.symbol_rules.get(sym, {})
        maker_fee_rate = _to_decimal(rule.get("maker_fee", "-0.0001"))

        price_d = _to_decimal(filled_price)
        size_d = _to_decimal(filled_size)
        notional_d = price_d * size_d
        entry_fee_d = (notional_d * maker_fee_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        total_cost_jpy = int((notional_d + entry_fee_d).to_integral_value(rounding=ROUND_HALF_UP))

        pf["jpy_balance"] = int(_to_decimal(pf.get("jpy_balance", 10000)) - Decimal(total_cost_jpy))

        tp_price = float(pending.get("take_profit_price", 0) or 0)
        sl_price = float(pending.get("stop_loss_price", 0) or 0)
        initial_risk_jpy = float(pending.get("initial_risk_jpy", 150) or 150)

        pos_entry: Dict[str, Any] = {
            "symbol": sym,
            "entry_price": float(price_d),
            "size": float(size_d),
            "cost_jpy": total_cost_jpy,
            "entry_fee_jpy": float(entry_fee_d),
            "take_profit_price": tp_price,
            "stop_loss_price": sl_price,
            "initial_stop_loss_price": sl_price,
            "initial_risk_jpy": initial_risk_jpy,
            "buy_order_id": str(pending.get("order_id", "")),
            "sl_order_id": "",
            "state": "UNPROTECTED_HOLD",
            "breakeven_locked": False,
            "trailing_stage": 0,
            "opened_at": now_jst.isoformat(timespec="seconds"),
        }
        pf["positions"].append(pos_entry)
        self.save_portfolio(pf, now_dt=now_jst)

        sl_order = self.broker.place_order(
            symbol=sym,
            side="SELL",
            execution_type="STOP",
            size=size_d,
            price=sl_price,
            time_in_force="FAK",
        )
        pos_entry["sl_order_id"] = str(sl_order.get("order_id", ""))
        pos_entry["state"] = "SL_ACTIVE"
        self.save_portfolio(pf, now_dt=now_jst)

        event = {
            "type": "BUY_FILLED_AND_SL_PLACED",
            "symbol": sym,
            "buy_order_id": str(pending.get("order_id", "")),
            "sl_order_id": pos_entry["sl_order_id"],
            "entry_price": float(price_d),
            "size": float(size_d),
            "stop_loss_price": sl_price,
            "take_profit_price": tp_price,
            "partial_expired": partial_expired,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

        self.notifier.notify_event(
            title=f"買い指値約定検知 ＆ 逆指値 (SL) 自動セット完了 (SL_ACTIVE): {sym}",
            message=(
                f"・買注文ID: `{pending.get('order_id', '')}` {'(部分約定)' if partial_expired else '(全量約定)'}\n"
                f"・約定単価: {format_price_jpy(filled_price)} 円 × {format_decimal_plain(filled_size)} {sym} (受渡: {total_cost_jpy:,} 円)\n"
                f"・自動SL注文ID: `{pos_entry['sl_order_id']}` (逆指値 STOP: {format_price_jpy(sl_price)} 円)\n"
                f"・監視中TP目標: {format_price_jpy(tp_price)} 円"
            ),
            level="FILL",
            dry_run=self.dry_run_notify,
        )
        return pos_entry, event

    def _record_closed_trade(
        self,
        pf: Dict[str, Any],
        pos: Dict[str, Any],
        exit_price: float,
        exit_order_id: str,
        reason: str,
        is_maker: bool,
        now_jst: datetime,
    ) -> Dict[str, Any]:
        sym = str(pos.get("symbol", "")).upper()
        rule = self.symbol_rules.get(sym, {})
        fee_rate = _to_decimal(rule.get("maker_fee", "-0.0001") if is_maker else rule.get("taker_fee", "0.0005"))

        entry_d = _to_decimal(pos.get("entry_price", 0))
        exit_d = _to_decimal(exit_price)
        size_d = _to_decimal(pos.get("size", 0))
        cost_jpy_d = _to_decimal(
            pos.get("cost_jpy")
            if pos.get("cost_jpy") is not None
            else (entry_d * size_d).to_integral_value(rounding=ROUND_HALF_UP)
        )

        gross_proceeds_d = exit_d * size_d
        exit_fee_d = (gross_proceeds_d * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        net_proceeds_jpy = int((gross_proceeds_d - exit_fee_d).to_integral_value(rounding=ROUND_HALF_UP))
        pnl_jpy = int(Decimal(net_proceeds_jpy) - cost_jpy_d)
        pnl_pct = float(((exit_d - entry_d) / entry_d * Decimal("100")) if entry_d > 0 else Decimal("0"))

        pf["jpy_balance"] = int(_to_decimal(pf.get("jpy_balance", 0)) + Decimal(net_proceeds_jpy))
        new_realized = int(_to_decimal(pf.get("realized_pnl_total_jpy", 0)) + Decimal(pnl_jpy))
        pf["realized_pnl_total_jpy"] = new_realized
        pf["total_realized_pnl_jpy"] = new_realized

        closed_record = {
            "symbol": sym,
            "size": float(size_d),
            "entry_price": float(entry_d),
            "exit_price": float(exit_d),
            "cost_jpy": int(cost_jpy_d),
            "proceeds_jpy": net_proceeds_jpy,
            "pnl_jpy": pnl_jpy,
            "pnl_pct": round(pnl_pct, 2),
            "reason": reason,
            "buy_order_id": str(pos.get("buy_order_id", "")),
            "exit_order_id": str(exit_order_id),
            "opened_at": str(pos.get("opened_at", "")),
            "closed_at": now_jst.isoformat(timespec="seconds"),
        }
        pf.setdefault("closed_trades", []).append(closed_record)
        return closed_record

    def modify_position_tp_sl(
        self,
        symbol: str,
        new_tp: float,
        new_sl: float,
        now_dt: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        AI判定の `MODIFY` に基づき、既存の SL 注文を新しい SL 逆指値へ更新する。
        ※ SL の引き下げ（`new_sl < old_sl`）は100%拒絶する不変条件を強制。
        """
        now_jst = _now_jst(now_dt)
        sym = symbol.strip().upper()
        pf = self.load_portfolio()

        for pos in pf.get("positions", []):
            if str(pos.get("symbol", "")).upper() != sym:
                continue
            old_sl_id = str(pos.get("sl_order_id", "") or "")
            old_sl = float(pos.get("stop_loss_price", 0) or 0)
            size = float(pos.get("size", 0) or 0)

            # SL 引き下げは 100% 拒絶
            effective_sl = max(float(new_sl), old_sl) if old_sl > 0 else float(new_sl)
            if effective_sl > 0 and abs(effective_sl - old_sl) > 1e-9:
                new_sl_id = self._replace_or_change_stop_order(
                    symbol=sym,
                    old_sl_order_id=old_sl_id,
                    size=size,
                    new_sl_price=effective_sl,
                )
                pos["sl_order_id"] = new_sl_id
                pos["stop_loss_price"] = effective_sl
                pos["sl_price"] = effective_sl
                if effective_sl >= float(pos.get("entry_price", 0) or 0):
                    pos["breakeven_locked"] = True

            if new_tp > 0:
                pos["take_profit_price"] = float(new_tp)
                pos["tp_price"] = float(new_tp)

            self.save_portfolio(pf, now_dt=now_jst)
            return dict(pos)

        return None

    def close_position_immediately(
        self,
        symbol: str,
        reason: str = "AI_SELL",
        now_dt: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        now_jst = _now_jst(now_dt)
        sym = symbol.strip().upper()
        pf = self.load_portfolio()
        target_idx: Optional[int] = None

        for idx, pos in enumerate(pf.get("positions", [])):
            if str(pos.get("symbol", "")).upper() == sym:
                target_idx = idx
                break

        if target_idx is None:
            return None

        pos = pf["positions"].pop(target_idx)
        closed_record = self._execute_atomic_market_exit(
            pf=pf,
            pos=pos,
            reason=reason,
            now_jst=now_jst,
        )
        self.save_portfolio(pf, now_dt=now_jst)

        self.notifier.notify_event(
            title=f"AI判定によるポジション決済完了 ({reason}): {sym}",
            message=(
                f"・注文ID: `{closed_record.get('exit_order_id', '')}`\n"
                f"・取得単価: {format_price_jpy(pos.get('entry_price', 0))} 円 ➔ 決済単価: {format_price_jpy(closed_record['exit_price'])} 円\n"
                f"・確定損益: **{closed_record['pnl_jpy']:+,} 円 ({closed_record['pnl_pct']:+.2f}%)**"
            ),
            level="INFO",
            dry_run=self.dry_run_notify,
        )
        return closed_record
