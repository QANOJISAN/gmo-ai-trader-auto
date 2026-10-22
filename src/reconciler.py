"""
3-Way Reconciliation（3者照合・自己修復）＆ 06:00 JST 手数料端数調整吸収モジュール (reconciler.py)

1. ThreeWayReconciler:
   - ① ローカル SQLite WAL 台帳 (`DatabaseWAL`) ＋ `portfolio.json`
   - ② `GET /private/v1/account/assets`（実口座 / PaperBroker の日本円・現物残高）
   - ③ `GET /private/v1/activeOrders` ＋ `GET /private/v1/latestExecutions`（有効注文・直近約定履歴）
   の3者を起動時・5分周期・土曜11:05 JSTメンテナンス明けに照合する。

2. 自己修復 (Self-Healing):
   - 孤児注文 (Orphan Orders): 取引所に存在するがローカル台帳に存在しない有効注文を検知し、
     安全のためキャンセル（または台帳取り込み）を実行する。
   - 幽霊注文 (Ghost Orders): ローカルが `PENDING_BUY` / `ORDERED` だが取引所の有効注文一覧にない注文を
     `GET /private/v1/orders` および `GET /private/v1/latestExecutions` で照会し、約定または失効・取消を確定させる。
   - 裸の現物ポジション (Naked Position): 取引所に `minOrderSize` 以上の現物残高があるにもかかわらず
     有効な `STOP` 逆指値注文が存在しない状態を検知した瞬間、0.5秒以内に `STOP` 注文を自動再発注して保護する。
     `minOrderSize` 未満の端数残高は `DUST_QUARANTINED` として隔離する。

3. 毎朝 06:00〜06:15 JST 手数料端数返金・調整の自動吸収:
   - `06:00〜06:15 JST` のウィンドウ内で検知された `+1〜+10円`（または `±1〜10円`）以内の日本円残高差分は
     GMOコインの日次手数料端数調整と判定し、異常アラートを出さずに `FEE_REFUND_ADJUSTMENT` として
     SQLite 台帳 (`fee_adjustments` テーブル) と `portfolio.json` の `jpy_balance` に自動同期する。
   - 閾値（日本円 50円超、または現物数量 `sizeStep` 以上）を超える原因不明の不一致を検知した場合は
     `DEGRADED_RECONCILING` モードへ移行し、新規発注をロックして `[CRITICAL]` 通知を発報する。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.database_wal import DatabaseWAL
from src.validator import DEFAULT_SYMBOL_RULES, _to_decimal, format_decimal_plain, round_to_step

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def is_in_fee_adjustment_window(now_dt: Optional[datetime] = None) -> bool:
    """毎朝 06:00〜06:15 JST の GMOコイン日次手数料端数調整ウィンドウかどうかを判定する"""
    now_jst = _now_jst(now_dt)
    return now_jst.hour == 6 and 0 <= now_jst.minute <= 15


class ThreeWayReconciler:
    """
    ローカル台帳 (SQLite WAL + portfolio.json) × 取引所資産残高 × 有効注文・約定履歴 の3者照合エンジン
    """

    def __init__(
        self,
        broker: Any,
        oco_manager: Any,
        db_wal: Optional[DatabaseWAL] = None,
        notifier: Optional[Any] = None,
        symbol_rules: Optional[Dict[str, Dict[str, Any]]] = None,
        jpy_discrepancy_threshold: float = 50.0,
        fee_adjustment_max_jpy: float = 50.0,
        cancel_orphan_orders: bool = True,
        dry_run_notify: bool = False,
    ) -> None:
        self.broker = broker
        self.oco_manager = oco_manager
        self.db_wal = db_wal or getattr(oco_manager, "db_wal", None) or getattr(oco_manager, "wal_db", None)
        self.notifier = notifier or getattr(oco_manager, "notifier", None)
        self.symbol_rules = {**DEFAULT_SYMBOL_RULES, **(symbol_rules or getattr(oco_manager, "symbol_rules", {}) or {})}
        self.jpy_discrepancy_threshold = float(jpy_discrepancy_threshold)
        self.fee_adjustment_max_jpy = float(fee_adjustment_max_jpy)
        self.cancel_orphan_orders = bool(cancel_orphan_orders)
        self.dry_run_notify = bool(dry_run_notify)

        self.degraded_reconciling: bool = False
        self.last_reconciled_at: Optional[datetime] = None
        self.last_discrepancies: List[str] = []

    def reconcile(
        self,
        now_dt: Optional[datetime] = None,
        default_sl_pct: float = 0.035,
    ) -> Dict[str, Any]:
        """
        3者照合を実行し、孤児注文・幽霊注文・裸ポジション・06:00端数調整を処理する。
        """
        lock = getattr(self.oco_manager, "_lock", None)
        if lock is not None:
            with lock:
                return self._reconcile_unlocked(now_dt=now_dt, default_sl_pct=default_sl_pct)
        return self._reconcile_unlocked(now_dt=now_dt, default_sl_pct=default_sl_pct)

    def _reconcile_unlocked(
        self,
        now_dt: Optional[datetime] = None,
        default_sl_pct: float = 0.035,
    ) -> Dict[str, Any]:
        t_start = time.monotonic()
        now_jst = _now_jst(now_dt)
        pf = self.oco_manager.load_portfolio()

        assets_summary = self.broker.get_jpy_and_spot_balances()
        exchange_jpy = _to_decimal(
            assets_summary.get(
                "jpy_amount",
                assets_summary.get("jpy_balance", assets_summary.get("jpy_available", 0)),
            )
        )
        spot_assets: Dict[str, Dict[str, Any]] = assets_summary.get("spot_assets", {}) or {}

        active_orders = self.broker.get_active_orders()
        active_by_id: Dict[str, Dict[str, Any]] = {
            str(o.get("order_id", "")): o for o in active_orders if str(o.get("order_id", ""))
        }

        actions_taken: List[str] = []
        discrepancies: List[str] = []
        orphan_orders_handled: List[str] = []
        ghost_orders_resolved: List[str] = []
        naked_positions_protected: List[str] = []
        dust_quarantined: List[str] = []
        fee_adjustment_applied: Optional[Dict[str, Any]] = None

        # ローカルが認知している注文IDセット
        local_pending_ids = {
            str(o.get("order_id", ""))
            for o in (pf.get("pending_orders", []) or [])
            if str(o.get("order_id", ""))
        }
        local_sl_ids = {
            str(p.get("sl_order_id", ""))
            for p in (pf.get("positions", []) or [])
            if str(p.get("sl_order_id", ""))
        }
        local_tp_ids = {
            str(p.get("tp_order_id", ""))
            for p in (pf.get("positions", []) or [])
            if str(p.get("tp_order_id", ""))
        }
        known_local_order_ids = local_pending_ids | local_sl_ids | local_tp_ids

        # ------------------------------------------------------------------
        # 1. 孤児注文 (Orphan Orders): 取引所に存在するがローカル台帳にない有効注文
        # ------------------------------------------------------------------
        for oid, ex_ord in list(active_by_id.items()):
            if oid not in known_local_order_ids:
                sym = str(ex_ord.get("symbol", "")).upper()
                side = str(ex_ord.get("side", "")).upper()
                exec_type = str(ex_ord.get("execution_type", "")).upper()
                if self.cancel_orphan_orders:
                    try:
                        self.broker.cancel_order(oid)
                        orphan_orders_handled.append(oid)
                        actions_taken.append(f"孤児注文キャンセル: order_id={oid} ({sym} {side} {exec_type})")
                        active_by_id.pop(oid, None)
                    except Exception as exc:
                        logger.warning("孤児注文キャンセル失敗 (order_id=%s): %s", oid, exc)
                else:
                    if self.db_wal is not None:
                        self.db_wal.upsert_order(
                            order_id=oid,
                            symbol=sym,
                            side=side,
                            execution_type=exec_type,
                            state="ORDERED",
                            price=str(ex_ord.get("price", "0")),
                            size=str(ex_ord.get("size", "0")),
                        )
                    orphan_orders_handled.append(oid)
                    actions_taken.append(f"孤児注文台帳取込: order_id={oid} ({sym} {side})")

        # ------------------------------------------------------------------
        # 2. 幽霊注文 (Ghost Orders): ローカルが PENDING_BUY だが取引所の有効注文にない
        # ------------------------------------------------------------------
        remaining_pending: List[Dict[str, Any]] = []
        for p_ord in pf.get("pending_orders", []) or []:
            oid = str(p_ord.get("order_id", ""))
            sym = str(p_ord.get("symbol", "")).upper()
            if not oid or oid in active_by_id:
                remaining_pending.append(p_ord)
                continue

            # 取引所の有効注文に存在しない -> get_orders & get_latest_executions で確認
            orders_info = self.broker.get_orders([oid])
            status = str(orders_info[0].get("status", "")).upper() if orders_info else "UNKNOWN"
            execs = self.broker.get_latest_executions(symbol=sym, count=50)
            matched_execs = [e for e in execs if str(e.get("order_id", "")) == oid]

            if status == "EXECUTED" or matched_execs:
                # 既に約定していた -> ポジション化して直ちに SL を発注
                filled_size_dec = sum((_to_decimal(e.get("size", 0)) for e in matched_execs), Decimal("0"))
                if filled_size_dec <= Decimal("0"):
                    filled_size_dec = _to_decimal(p_ord.get("size", 0))
                fill_price = float(matched_execs[0].get("price", p_ord.get("limit_price", 0))) if matched_execs else float(p_ord.get("limit_price", 0))
                sl_price = float(p_ord.get("stop_loss_price", 0) or round(fill_price * (1.0 - default_sl_pct)))
                tp_price = float(p_ord.get("take_profit_price", 0) or round(fill_price * 1.05))

                sl_order_id = ""
                rule_for_sym = self.symbol_rules.get(sym, {})
                min_size_for_sym = _to_decimal(rule_for_sym.get("min_order_size", "0.0001"))
                is_dust_fill = Decimal("0") < filled_size_dec < min_size_for_sym
                if sl_price > 0 and filled_size_dec >= min_size_for_sym:
                    try:
                        sl_res = self.broker.place_order(
                            symbol=sym,
                            side="SELL",
                            execution_type="STOP",
                            size=format_decimal_plain(filled_size_dec),
                            price=sl_price,
                            time_in_force="FAK",
                        )
                        sl_order_id = str(sl_res.get("order_id", ""))
                    except Exception as exc:
                        logger.error("幽霊注文約定後のSL発注エラー (%s): %s", sym, exc)

                maker_fee_rate = _to_decimal(rule_for_sym.get("maker_fee", "-0.0001"))
                notional_d = _to_decimal(fill_price) * filled_size_dec
                if matched_execs and any("fee" in e for e in matched_execs):
                    entry_fee_d = sum((_to_decimal(e.get("fee", 0)) for e in matched_execs), Decimal("0"))
                else:
                    entry_fee_d = (notional_d * maker_fee_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                total_cost_jpy = int((notional_d + entry_fee_d).to_integral_value(rounding=ROUND_HALF_UP))
                pf["jpy_balance"] = int(_to_decimal(pf.get("jpy_balance", 10000)) - Decimal(total_cost_jpy))

                if is_dust_fill:
                    pf.setdefault("quarantined_dust", []).append(
                        {
                            "symbol": sym,
                            "size": float(filled_size_dec),
                            "entry_price": fill_price,
                            "cost_jpy": total_cost_jpy,
                            "buy_order_id": oid,
                            "state": "DUST_QUARANTINED",
                            "quarantined_at": now_jst.isoformat(timespec="seconds"),
                        }
                    )
                    if self.db_wal is not None:
                        self.db_wal.quarantine_dust_position(sym, float(filled_size_dec), fill_price)
                    dust_quarantined.append(sym)
                    ghost_orders_resolved.append(oid)
                    actions_taken.append(f"幽霊注文の端数約定をDUST隔離: {sym} order_id={oid} size={format_decimal_plain(filled_size_dec)}")
                    continue

                new_pos = {
                    "symbol": sym,
                    "side": "BUY",
                    "size": float(filled_size_dec),
                    "entry_price": fill_price,
                    "current_price": fill_price,
                    "cost_jpy": total_cost_jpy,
                    "entry_fee_jpy": float(entry_fee_d),
                    "take_profit_price": tp_price,
                    "stop_loss_price": sl_price,
                    "initial_sl_price": sl_price,
                    "initial_stop_loss_price": sl_price,
                    "sl_order_id": sl_order_id,
                    "buy_order_id": oid,
                    "entry_order_id": oid,
                    "state": "PROTECTED" if sl_order_id else "STOP_REARMING",
                    "opened_at": now_jst.isoformat(timespec="seconds"),
                }
                pf.setdefault("positions", []).append(new_pos)
                if self.db_wal is not None:
                    if matched_execs:
                        for ex in matched_execs:
                            self.db_wal.record_execution_idempotent(
                                execution_id=str(ex.get("execution_id", f"rec_{oid}")),
                                order_id=oid,
                                symbol=sym,
                                side="BUY",
                                price_str=str(ex.get("price", fill_price)),
                                size_str=str(ex.get("size", filled_size_dec)),
                                fee_jpy_str=str(ex.get("fee", str(entry_fee_d))),
                                timestamp_str=str(ex.get("timestamp", now_jst.isoformat(timespec="seconds"))),
                            )
                    recorded_sz = self.db_wal.get_executed_size_for_order(oid) if hasattr(self.db_wal, "get_executed_size_for_order") else (filled_size_dec if self.db_wal.has_execution_for_order(oid) else Decimal("0"))
                    residual_sz = filled_size_dec - recorded_sz
                    residual_sat = (
                        int((residual_sz * Decimal("100000000")).to_integral_value(rounding=ROUND_HALF_UP))
                        if residual_sz > Decimal("0")
                        else 0
                    )
                    if residual_sz > Decimal("0") and residual_sat > 0:
                        residual_fee = (entry_fee_d * residual_sz / filled_size_dec).quantize(Decimal("0.01")) if filled_size_dec > Decimal("0") else entry_fee_d
                        self.db_wal.record_execution_idempotent(
                            execution_id=f"rec_{oid}_{residual_sat}",
                            order_id=oid,
                            symbol=sym,
                            side="BUY",
                            price_str=str(fill_price),
                            size_str=format_decimal_plain(residual_sz),
                            fee_jpy_str=str(residual_fee),
                            timestamp_str=now_jst.isoformat(timespec="seconds"),
                        )
                    self.db_wal.upsert_order(
                        order_id=oid,
                        symbol=sym,
                        side="BUY",
                        execution_type="LIMIT",
                        state="EXECUTED",
                        price=str(fill_price),
                        size=format_decimal_plain(filled_size_dec),
                        executed_size=format_decimal_plain(filled_size_dec),
                    )
                    self.db_wal.upsert_position(
                        symbol=sym,
                        state=new_pos["state"],
                        size=format_decimal_plain(filled_size_dec),
                        entry_price=str(fill_price),
                        take_profit_price=str(tp_price),
                        stop_loss_price=str(sl_price),
                        buy_order_id=oid,
                        sl_order_id=sl_order_id,
                        opened_at=new_pos["opened_at"],
                    )
                ghost_orders_resolved.append(oid)
                actions_taken.append(f"幽霊注文の約定復元＆SL保護完了: {sym} order_id={oid} -> sl_order_id={sl_order_id}")
            else:
                # CANCELED / EXPIRED 等で消失していたためローカルの待機リストから除去
                if self.db_wal is not None:
                    self.db_wal.upsert_order(
                        order_id=oid,
                        symbol=sym,
                        side="BUY",
                        execution_type="LIMIT",
                        state="CANCELED",
                        price=str(p_ord.get("limit_price", "0")),
                        size=str(p_ord.get("size", "0")),
                    )
                ghost_orders_resolved.append(oid)
                actions_taken.append(f"幽霊注文の失効・取消同期: {sym} order_id={oid} (status={status})")

        pf["pending_orders"] = remaining_pending

        # ------------------------------------------------------------------
        # 3. 保有ポジションの SL 生存確認 ＆ 取引所現物残高の裸ポジション (Naked Position) 保護
        # ------------------------------------------------------------------
        raw_positions: List[Dict[str, Any]] = list(pf.get("positions", []) or [])
        quarantined_dust_by_sym: Dict[str, Decimal] = {}
        for d_entry in (pf.get("quarantined_dust", []) or []):
            d_sym = str(d_entry.get("symbol", "")).strip().upper()
            if d_sym:
                quarantined_dust_by_sym[d_sym] = (
                    quarantined_dust_by_sym.get(d_sym, Decimal("0"))
                    + _to_decimal(d_entry.get("size", 0))
                )

        # 3a. 既存ローカルポジションの SL 確認（同一銘柄で DUST_QUARANTINED と PROTECTED が共存しても上書きしない）
        remaining_positions: List[Dict[str, Any]] = []
        for pos in raw_positions:
            sym = str(pos.get("symbol", "")).strip().upper()
            pos_state = str(pos.get("state", "")).strip().upper()
            size_dec = _to_decimal(pos.get("size", 0))
            rule = self.symbol_rules.get(sym, {})
            min_size_dec = _to_decimal(rule.get("min_order_size", "0.0001"))

            if pos_state == "DUST_QUARANTINED":
                if sym not in quarantined_dust_by_sym:
                    quarantined_dust_by_sym[sym] = size_dec
                remaining_positions.append(pos)
                continue

            sl_oid = str(pos.get("sl_order_id", "") or "")
            sl_price = float(pos.get("stop_loss_price") or pos.get("sl_price") or 0)
            entry_price = float(pos.get("entry_price", 0) or 0)

            if size_dec < min_size_dec:
                pos["state"] = "DUST_QUARANTINED"
                dust_quarantined.append(sym)
                quarantined_dust_by_sym[sym] = (
                    quarantined_dust_by_sym.get(sym, Decimal("0")) + size_dec
                )
                remaining_positions.append(pos)
                continue

            if sl_oid and sl_oid not in active_by_id:
                orders_info = self.broker.get_orders([sl_oid])
                if orders_info and str(orders_info[0].get("status", "")).upper() == "EXECUTED":
                    actions_taken.append(f"{sym}: 照合中にSL約定済みを検知 (order_id={sl_oid})")
                    continue

                # SLが消失していた場合は 0.5秒以内に再発注（隔離済み端数分は差し引いて保護数量を算出）
                spot_info = spot_assets.get(sym, {})
                actual_amt = _to_decimal(spot_info.get("amount", spot_info.get("available", size_dec)))
                net_amt = max(Decimal("0"), actual_amt - quarantined_dust_by_sym.get(sym, Decimal("0")))
                protect_size = min(size_dec, net_amt) if net_amt > Decimal("0") else size_dec
                if protect_size >= min_size_dec:
                    if sl_price <= 0 and entry_price > 0:
                        sl_price = float(round_to_step(Decimal(str(entry_price)) * Decimal(str(1.0 - default_sl_pct)), _to_decimal(rule.get("tick_size", "1"))))
                    if sl_price > 0:
                        new_sl = self.broker.place_order(
                            symbol=sym,
                            side="SELL",
                            execution_type="STOP",
                            size=format_decimal_plain(protect_size),
                            price=sl_price,
                            time_in_force="FAK",
                        )
                        pos["sl_order_id"] = str(new_sl.get("order_id", ""))
                        pos["state"] = "PROTECTED"
                        naked_positions_protected.append(sym)
                        actions_taken.append(
                            f"{sym}: SL未設定を検知し即時再発注完了 (new_sl_order_id={pos['sl_order_id']})"
                        )
            remaining_positions.append(pos)

        # 3b. 取引所に現物残高があるのにローカル有効 `positions` に存在しない「完全な裸ポジション」の検知と0.5秒以内保護
        active_local_syms = {
            str(p.get("symbol", "")).strip().upper()
            for p in remaining_positions
            if str(p.get("state", "")).strip().upper() != "DUST_QUARANTINED"
        }
        quarantined_syms = set(quarantined_dust_by_sym.keys())
        for sym, asset_info in spot_assets.items():
            sym_up = sym.strip().upper()
            if sym_up == "JPY" or sym_up in active_local_syms:
                continue
            total_amt_dec = _to_decimal(asset_info.get("amount", asset_info.get("available", 0)))
            amt_dec = max(Decimal("0"), total_amt_dec - quarantined_dust_by_sym.get(sym_up, Decimal("0")))
            if amt_dec <= Decimal("0"):
                continue

            rule = self.symbol_rules.get(sym_up, {})
            min_size_dec = _to_decimal(rule.get("min_order_size", "0.0001"))
            size_step_dec = _to_decimal(rule.get("size_step", "0.0001"))
            tick_dec = _to_decimal(rule.get("tick_size", "1"))

            if amt_dec < min_size_dec:
                if sym_up in quarantined_syms:
                    continue
                # 最小注文数量未満は DUST_QUARANTINED として隔離
                dust_pos = {
                    "symbol": sym_up,
                    "side": "BUY",
                    "size": float(amt_dec),
                    "entry_price": 0.0,
                    "current_price": 0.0,
                    "take_profit_price": 0.0,
                    "stop_loss_price": 0.0,
                    "sl_order_id": "",
                    "state": "DUST_QUARANTINED",
                    "opened_at": now_jst.isoformat(timespec="seconds"),
                }
                remaining_positions.append(dust_pos)
                dust_quarantined.append(sym_up)
                quarantined_dust_by_sym[sym_up] = amt_dec
                quarantined_syms.add(sym_up)
                actions_taken.append(f"{sym_up}: 最小ロット未満の端数残高 ({format_decimal_plain(amt_dec)}) を DUST_QUARANTINED に隔離")
                continue

            # 既に取引所上に有効な SELL STOP 注文が存在するかチェック
            existing_stop = next(
                (
                    o for o in active_by_id.values()
                    if str(o.get("symbol", "")).upper() == sym_up
                    and str(o.get("side", "")).upper() == "SELL"
                    and str(o.get("execution_type", "")).upper() == "STOP"
                ),
                None,
            )
            order_size_dec = round_to_step(amt_dec, size_step_dec)
            ref_price = float(asset_info.get("reference_price", 0) or 0)
            if ref_price <= 0:
                # 直近約定から推定
                recent_execs = self.broker.get_latest_executions(symbol=sym_up, count=5)
                if recent_execs:
                    ref_price = float(recent_execs[0].get("price", 0) or 0)
            if ref_price <= 0:
                ref_price = 1000.0

            sl_dec = round_to_step(Decimal(str(ref_price)) * Decimal(str(1.0 - default_sl_pct)), tick_dec)
            tp_dec = round_to_step(Decimal(str(ref_price)) * Decimal("1.06"), tick_dec)

            if existing_stop is not None:
                sl_oid = str(existing_stop.get("order_id", ""))
                sl_val = float(existing_stop.get("price", sl_dec) or sl_dec)
            else:
                new_sl = self.broker.place_order(
                    symbol=sym_up,
                    side="SELL",
                    execution_type="STOP",
                    size=format_decimal_plain(order_size_dec),
                    price=float(sl_dec),
                    time_in_force="FAK",
                )
                sl_oid = str(new_sl.get("order_id", ""))
                sl_val = float(sl_dec)
                naked_positions_protected.append(sym_up)
                actions_taken.append(
                    f"{sym_up}: 裸の現物ポジション ({format_decimal_plain(order_size_dec)}) を検知し STOP 逆指値 (sl_order_id={sl_oid}) を即時発注して保護完了"
                )

            recovered_pos = {
                "symbol": sym_up,
                "side": "BUY",
                "size": float(order_size_dec),
                "entry_price": ref_price,
                "current_price": ref_price,
                "take_profit_price": float(tp_dec),
                "stop_loss_price": sl_val,
                "initial_sl_price": sl_val,
                "sl_order_id": sl_oid,
                "state": "PROTECTED" if sl_oid else "STOP_REARMING",
                "opened_at": now_jst.isoformat(timespec="seconds"),
            }
            remaining_positions.append(recovered_pos)
            active_local_syms.add(sym_up)
            if self.db_wal is not None:
                self.db_wal.upsert_position(
                    symbol=sym_up,
                    state=recovered_pos["state"],
                    size=format_decimal_plain(order_size_dec),
                    entry_price=str(ref_price),
                    take_profit_price=str(tp_dec),
                    stop_loss_price=str(sl_val),
                    sl_order_id=sl_oid,
                    opened_at=recovered_pos["opened_at"],
                )

        pf["positions"] = remaining_positions

        # ------------------------------------------------------------------
        # 4. 日本円残高の照合 ＆ 毎朝 06:00〜06:15 JST 手数料端数調整の自動吸収
        # ------------------------------------------------------------------
        local_jpy = _to_decimal(pf.get("jpy_balance", exchange_jpy))
        jpy_diff = exchange_jpy - local_jpy

        if jpy_diff != Decimal("0"):
            abs_diff = abs(jpy_diff)
            if is_in_fee_adjustment_window(now_jst) and Decimal("0") < abs_diff <= Decimal(str(self.fee_adjustment_max_jpy)):
                # 06:00〜06:15 JST の ±1〜10円以内の端数調整 -> FEE_REFUND_ADJUSTMENT として自動吸収
                pf["jpy_balance"] = int(exchange_jpy) if exchange_jpy == exchange_jpy.to_integral() else float(exchange_jpy)
                if self.db_wal is not None:
                    fee_adjustment_applied = self.db_wal.record_fee_refund_adjustment(
                        adjustment_jpy=int(jpy_diff),
                        balance_before=int(local_jpy),
                        balance_after=int(exchange_jpy),
                        reason="FEE_REFUND_ADJUSTMENT",
                        detected_at=now_jst,
                    )
                else:
                    fee_adjustment_applied = {
                        "reason": "FEE_REFUND_ADJUSTMENT",
                        "adjustment_jpy": int(jpy_diff),
                        "balance_before_jpy": int(local_jpy),
                        "balance_after_jpy": int(exchange_jpy),
                    }
                actions_taken.append(
                    f"06:00 JST 日次手数料端数調整を自動吸収 (FEE_REFUND_ADJUSTMENT: {int(jpy_diff):+d}円, {int(local_jpy)} -> {int(exchange_jpy)}円)"
                )
            elif abs_diff > Decimal(str(self.jpy_discrepancy_threshold)):
                discrepancies.append(
                    f"日本円残高の重大不一致検知: local={local_jpy}円 vs exchange={exchange_jpy}円 (diff={jpy_diff:+}円 > {self.jpy_discrepancy_threshold}円)"
                )
            else:
                # 軽微な端数差は実口座残高へ同期
                pf["jpy_balance"] = int(exchange_jpy) if exchange_jpy == exchange_jpy.to_integral() else float(exchange_jpy)
                actions_taken.append(f"日本円残高同期: {local_jpy}円 -> {exchange_jpy}円")

        self.oco_manager.save_portfolio(pf, now_dt=now_jst)
        elapsed_ms = round((time.monotonic() - t_start) * 1000.0, 2)
        self.last_reconciled_at = now_jst
        self.last_discrepancies = discrepancies

        if discrepancies:
            self.degraded_reconciling = True
            if self.notifier is not None:
                self.notifier.notify_event(
                    title="[CRITICAL] 3-Way Reconciliation 重大不一致検知 (DEGRADED_RECONCILING)",
                    message="\n".join(f"・{d}" for d in discrepancies),
                    level="CRITICAL",
                    dry_run=self.dry_run_notify,
                )
        else:
            self.degraded_reconciling = False

        return {
            "status": "DEGRADED_RECONCILING" if self.degraded_reconciling else "RECONCILED",
            "degraded_reconciling": self.degraded_reconciling,
            "elapsed_ms": elapsed_ms,
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "exchange_jpy": float(exchange_jpy),
            "jpy_diff": float(jpy_diff),
            "actions_taken": actions_taken,
            "discrepancies": discrepancies,
            "orphan_orders_handled": orphan_orders_handled,
            "ghost_orders_resolved": ghost_orders_resolved,
            "naked_positions_protected": naked_positions_protected,
            "dust_quarantined": dust_quarantined,
            "fee_adjustment_applied": fee_adjustment_applied,
        }
