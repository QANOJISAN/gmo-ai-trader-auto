"""
現物API向け「擬似OCO＆自動トレーリング」管理エンジン (pseudo_oco_manager.py)

GMOコインの現物 Private API は単発注文のみ（IFD-OCO 非対応、かつ逆指値 SL 注文を発注すると
現物保有数量がロックされて別注文を出せない仕様）であるため、本モジュールが以下のステートマシンを管理する：

- 状態1 `PENDING_BUY`:
  買い指値（Post-Only `SOK`）を発注中。
  * 全量約定（または部分約定）を検知したら `portfolio.json` にポジションを記録し、
    直ちに保有数量分の損切り逆指値（`STOP` / `FAK`）を自動発注して `SL_ACTIVE` へ遷移。
  * `ORDER_EXPIRY_HOURS`（デフォルト4時間）経過しても未約定なら自動キャンセル。
    （部分約定していた場合は未約定残数量をキャンセルし、約定済み数量分のみ SL を発注して `SL_ACTIVE` へ遷移）

- 状態2 `SL_ACTIVE`:
  取引所サーバー側に `STOP` 逆指値注文が置かれており、PCやデーモンが停止しても下値が保護されている状態。
  * 自動建値ロック（Auto-Breakeven）:
    現在価格（`bid`）が `+1.0R` 以上に到達し、かつ現在の SL が建値微益 SL（`entry_price * 1.002` の `tick_size` 切り上げ）未満の場合、
    AI判定を待たずに自動で「旧 SL 注文を `cancel_order` ➔ 新しい建値微益 SL 注文を `place_order`」して利益をロックし、Discord へ通知。
  * 利確スイッチ（TP Execution）:
    現在価格（`bid`）が `take_profit_price` 以上に到達したら、即座に「発注中の SL 注文を `cancel_order` してロック解除 ➔
    直ちに利確売り注文を `place_order`」して決済を完了させ、`closed_trades` に記録して Discord へ通知。
  * 損切り約定検知（SL Execution Detection）:
    取引所に出していた SL 注文が `EXECUTED`（約定済み）になっていたら、自動で `closed_trades` に損切り決済を記録して Discord へ通知。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.gmo_private_client import GMOPrivateClient, PaperBroker
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


def calculate_breakeven_sl_price(
    entry_price: Union[str, int, float, Decimal],
    symbol: str,
    symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Union[int, float]:
    """
    建値微益SL価格（`entry_price * max(1.002, 1 + 2 * taker_fee)` を `tick_size` で `ROUND_UP` 切り上げ）を算出する。
    """
    sym = symbol.strip().upper()
    rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or {})}
    rule = rules.get(sym, {})
    tick_size = _to_decimal(rule.get("tick_size", "1"), default=Decimal("1"))
    if tick_size <= 0:
        tick_size = Decimal("1")
    taker_fee = max(_to_decimal(rule.get("taker_fee", "0.0005")), Decimal("0"))
    mult = max(Decimal("1.002"), Decimal("1") + (taker_fee * Decimal("2")))
    raw_be = _to_decimal(entry_price) * mult
    rounded = round_to_step(raw_be, tick_size, rounding=ROUND_UP)
    if rounded == rounded.to_integral_value():
        return int(rounded)
    return float(rounded)


class PseudoOCOManager:
    """現物API向け 擬似OCO＆自動トレーリング管理エンジン"""

    def __init__(
        self,
        broker: Union[GMOPrivateClient, PaperBroker],
        portfolio_path: Union[str, Path] = "portfolio.json",
        notifier: Optional[Notifier] = None,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        order_expiry_hours: Optional[float] = None,
        dry_run_notify: bool = False,
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
                "closed_trades": [],
                "pending_orders": [],
                "execution_mode": getattr(self.broker, "MODE", "DRY_RUN"),
                "last_updated": _now_jst().isoformat(timespec="seconds"),
            }
            write_json_file(self.portfolio_path, default_pf)
            return default_pf
        pf = load_json_file(self.portfolio_path)
        pf.setdefault("positions", [])
        pf.setdefault("closed_trades", [])
        pf.setdefault("pending_orders", [])
        pf.setdefault("realized_pnl_total_jpy", int(pf.get("total_realized_pnl_jpy", 0) or 0))
        pf.setdefault("total_realized_pnl_jpy", int(pf.get("realized_pnl_total_jpy", 0) or 0))
        return pf

    def save_portfolio(self, portfolio: Dict[str, Any], now_dt: Optional[datetime] = None) -> None:
        portfolio["last_updated"] = _now_jst(now_dt).isoformat(timespec="seconds")
        portfolio["execution_mode"] = getattr(self.broker, "MODE", "DRY_RUN")
        write_json_file(self.portfolio_path, portfolio)

    def place_new_buy_order(
        self,
        decision: Dict[str, Any],
        tickers: Optional[Dict[str, Dict[str, Any]]] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        AI判定の `BUY` シグナルに基づき、Post-Only (`LIMIT` / `SOK`) の買い指値注文を発注し、
        状態1 `PENDING_BUY` として `portfolio["pending_orders"]` に記録する。
        """
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
        高速ループ（15秒間隔）で呼び出されるステートマシン更新処理:
        1. `PaperBroker` の場合はリアルタイム `tickers` で仮想約定判定を実行
        2. 状態1 `PENDING_BUY` の約定確認 ➔ 約定時は直ちに `STOP` 逆指値を発注して `SL_ACTIVE` へ遷移、
           または `ORDER_EXPIRY_HOURS`（4時間）経過で未約定タイムアウト取消
        3. 状態2 `SL_ACTIVE` の監視:
           - サーバー側 `STOP` 注文が `EXECUTED` なら損切り決済を `closed_trades` に記録
           - 現在 Bid が `take_profit_price` 以上なら `STOP` 注文を `cancel_order` してロック解除 ➔ 利確売りを発注・決済記録
           - 現在 Bid が `+1.0R` 以上かつ現在 SL < `breakeven_sl_price` なら、旧 SL を `cancel_order` ➔ 建値微益 SL を再発注（自動建値ロック）
        """
        now_jst = _now_jst(now_dt)
        events: List[Dict[str, Any]] = []

        if isinstance(self.broker, PaperBroker):
            self.broker.evaluate_paper_orders(tickers=tickers, now_dt=now_jst)

        pf = self.load_portfolio()
        pf_changed = False

        # ------------------------------------------------------------------
        # Step A: PENDING_BUY 注文の約定チェック & 4時間タイムアウト管理
        # ------------------------------------------------------------------
        remaining_pending: List[Dict[str, Any]] = []
        for pending in pf.get("pending_orders", []):
            if str(pending.get("state", "PENDING_BUY")).upper() != "PENDING_BUY":
                continue

            oid = str(pending.get("order_id", ""))
            sym = str(pending.get("symbol", "")).upper()
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
                pos_entry, sl_event = self._activate_position_with_sl(
                    pf=pf,
                    pending=pending,
                    filled_size=exec_size if exec_size > 0 else total_size,
                    filled_price=exec_price,
                    now_jst=now_jst,
                )
                events.append(sl_event)
                pf_changed = True
                continue

            # ケース2: 有効期限（デフォルト4時間）経過によるタイムアウト
            if elapsed_hours >= self.order_expiry_hours:
                try:
                    self.broker.cancel_order(oid)
                except Exception as exc:
                    logger.warning("期限切れ注文キャンセル時の警告 (order_id=%s): %s", oid, exc)

                # 再取得して部分約定があるか確認
                rechecked = self.broker.get_orders([oid])
                final_exec_size = float(
                    (rechecked[0].get("executed_size", exec_size) if rechecked else exec_size) or 0
                )

                if final_exec_size > 0:
                    # 部分約定済みなら約定分のみ SL 発注してポジション化
                    pos_entry, sl_event = self._activate_position_with_sl(
                        pf=pf,
                        pending=pending,
                        filled_size=final_exec_size,
                        filled_price=exec_price,
                        now_jst=now_jst,
                        partial_expired=True,
                    )
                    events.append(sl_event)
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

            # ケース3: 取引所側で既にキャンセルされていた場合
            if status == "CANCELED":
                if exec_size > 0:
                    _, sl_event = self._activate_position_with_sl(
                        pf=pf,
                        pending=pending,
                        filled_size=exec_size,
                        filled_price=exec_price,
                        now_jst=now_jst,
                        partial_expired=True,
                    )
                    events.append(sl_event)
                pf_changed = True
                continue

            remaining_pending.append(pending)

        pf["pending_orders"] = remaining_pending

        # ------------------------------------------------------------------
        # Step B: SL_ACTIVE ポジションの監視（SL約定検知 / TP到達スイッチ / +1.0R建値移動）
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

            ticker = (tickers or {}).get(sym, {})
            current_bid = float(ticker.get("bid", 0) or ticker.get("last", 0) or pos.get("current_price", 0) or entry_price)
            current_last = float(ticker.get("last", 0) or current_bid or entry_price)
            pos["current_price"] = current_last

            # 1) 取引所に出していた SL 注文が約定 (EXECUTED) していないか確認
            if sl_order_id:
                sl_orders = self.broker.get_orders([sl_order_id])
                if sl_orders and str(sl_orders[0].get("status", "")).upper() == "EXECUTED":
                    sl_order = sl_orders[0]
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
                # 万一 LIMIT FAS が即時約定しなかった場合（LIVE環境で板が薄かった場合等）は MARKET で確実決済
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

            # 3) 自動建値ロック (Auto-Breakeven): 現在 Bid が +1.0R 以上に到達 & 現在 SL < 建値微益 SL
            be_sl_price = calculate_breakeven_sl_price(entry_price, sym, self.symbol_rules)
            risk_per_unit = (
                (entry_price - initial_sl_price)
                if (initial_sl_price > 0 and entry_price > initial_sl_price)
                else (entry_price * 0.03)
            )
            one_r_target_bid = entry_price + risk_per_unit
            initial_risk_jpy = float(pos.get("initial_risk_jpy", 0) or (risk_per_unit * size))
            unrealized_bid_pnl_jpy = (current_bid - entry_price) * size

            reached_one_r = (current_bid >= one_r_target_bid) or (
                initial_risk_jpy > 0 and unrealized_bid_pnl_jpy >= initial_risk_jpy
            )

            if (
                reached_one_r
                and float(sl_price) < float(be_sl_price)
                and current_bid > float(be_sl_price)
            ):
                old_sl_id = sl_order_id
                if old_sl_id:
                    self.broker.cancel_order(old_sl_id)

                new_sl_order = self.broker.place_order(
                    symbol=sym,
                    side="SELL",
                    execution_type="STOP",
                    size=size,
                    price=be_sl_price,
                    time_in_force="FAK",
                )
                pos["stop_loss_price"] = be_sl_price
                pos["sl_price"] = be_sl_price
                pos["sl_order_id"] = str(new_sl_order["order_id"])
                pos["breakeven_locked"] = True
                pos["breakeven_locked_at"] = now_jst.isoformat(timespec="seconds")
                pos["state"] = "SL_ACTIVE"

                events.append(
                    {
                        "type": "AUTO_BREAKEVEN_LOCKED",
                        "symbol": sym,
                        "old_sl_order_id": old_sl_id,
                        "new_sl_order_id": str(new_sl_order["order_id"]),
                        "old_sl_price": sl_price,
                        "new_sl_price": be_sl_price,
                        "current_bid": current_bid,
                        "timestamp": now_jst.isoformat(timespec="seconds"),
                    }
                )
                self.notifier.notify_event(
                    title=f"+1.0R 到達・自動建値ロック発動 (Auto-Breakeven): {sym}",
                    message=(
                        f"・現在Bid: {format_price_jpy(current_bid)} 円 (+1.0R 目標 {format_price_jpy(one_r_target_bid)} 円到達)\n"
                        f"・旧SL取消: `{old_sl_id}` ({format_price_jpy(sl_price)} 円)\n"
                        f"・新SL発注: `{new_sl_order['order_id']}` ({format_price_jpy(be_sl_price)} 円 / 買値+0.2%切り上げ)"
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

    def _activate_position_with_sl(
        self,
        pf: Dict[str, Any],
        pending: Dict[str, Any],
        filled_size: float,
        filled_price: float,
        now_jst: datetime,
        partial_expired: bool = False,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        買い指値が約定した際に `portfolio.json` の残高・ポジションを更新し、
        直ちに保有数量分の損切り逆指値 (`STOP`) を発注して `SL_ACTIVE` に遷移させる。
        """
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
            "state": "SL_ACTIVE",
            "breakeven_locked": False,
            "opened_at": now_jst.isoformat(timespec="seconds"),
        }
        pf["positions"].append(pos_entry)
        # PaperBroker が get_assets() でポジション保有数量を参照できるよう一旦保存してから STOP 注文を発注
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
        """AI判定の `MODIFY` に基づき、既存の SL 注文を取り消して新しい SL 逆指値を発注し、TP/SL を更新する"""
        now_jst = _now_jst(now_dt)
        sym = symbol.strip().upper()
        pf = self.load_portfolio()

        for pos in pf.get("positions", []):
            if str(pos.get("symbol", "")).upper() != sym:
                continue
            old_sl_id = str(pos.get("sl_order_id", "") or "")
            old_sl = float(pos.get("stop_loss_price", 0) or 0)
            size = float(pos.get("size", 0) or 0)

            if new_sl > 0 and abs(new_sl - old_sl) > 1e-9:
                if old_sl_id:
                    self.broker.cancel_order(old_sl_id)
                sl_order = self.broker.place_order(
                    symbol=sym,
                    side="SELL",
                    execution_type="STOP",
                    size=size,
                    price=new_sl,
                    time_in_force="FAK",
                )
                pos["sl_order_id"] = str(sl_order.get("order_id", ""))
                pos["stop_loss_price"] = float(new_sl)
                pos["sl_price"] = float(new_sl)
                if float(new_sl) >= float(pos.get("entry_price", 0) or 0):
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
        """AI判定の `SELL`（タイムストップ撤退等）に基づき、旧SLをキャンセルして成行/即時決済する"""
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
        old_sl_id = str(pos.get("sl_order_id", "") or "")
        if old_sl_id:
            try:
                self.broker.cancel_order(old_sl_id)
            except Exception as exc:
                logger.warning("AI_SELL 時の旧SL取消警告 (order_id=%s): %s", old_sl_id, exc)

        sell_order = self.broker.place_order(
            symbol=sym,
            side="SELL",
            execution_type="MARKET",
            size=pos.get("size", 0),
            time_in_force="FAK",
        )
        exit_price = float(
            sell_order.get("executed_price")
            or sell_order.get("price")
            or pos.get("current_price")
            or pos.get("entry_price", 0)
        )
        closed_record = self._record_closed_trade(
            pf=pf,
            pos=pos,
            exit_price=exit_price,
            exit_order_id=str(sell_order.get("order_id", "")),
            reason=reason,
            is_maker=False,
            now_jst=now_jst,
        )
        self.save_portfolio(pf, now_dt=now_jst)

        self.notifier.notify_event(
            title=f"AI判定によるポジション決済完了 ({reason}): {sym}",
            message=(
                f"・注文ID: `{sell_order.get('order_id', '')}`\n"
                f"・取得単価: {format_price_jpy(pos.get('entry_price', 0))} 円 ➔ 決済単価: {format_price_jpy(exit_price)} 円\n"
                f"・確定損益: **{closed_record['pnl_jpy']:+,} 円 ({closed_record['pnl_pct']:+.2f}%)**"
            ),
            level="INFO",
            dry_run=self.dry_run_notify,
        )
        return closed_record
