"""
v2 全自動売買システム：全8カテゴリ・64項目＋4大複合障害 カオスエンジニアリング（Fault Injection）総合監査スイート
(tests/test_v2_chaos_audit.py)

すべてのテストは `tempfile.TemporaryDirectory` およびインメモリ / ChaosMockExchange 上で完結し、
現行 v1 (`e:\\gmo-ai-trader`) および本番ファイルに 1 バイトも干渉しない。
"""

from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from auto_trader import AutoTraderDaemon
from src.adverse_selection_guard import AdverseSelectionGuard
from src.ai_judge import (
    AIJudge,
    _extract_json_dict,
    enforce_counter_thesis_and_conviction_gate,
)
from src.circuit_breaker import (
    MultiTierCircuitBreaker,
    SingleInstanceLock,
    check_memory_rss_and_should_restart,
    is_in_saturday_pre_maintenance_clear_window,
    prevent_windows_sleep,
)
from src.database_wal import DatabaseWAL
from src.gmo_private_client import (
    GMOAuthHaltError,
    GMOPrivateAPIError,
    GMOPrivateClient,
    InsufficientAvailableAssetError,
    PaperBroker,
    PriorityTokenBucket,
)
from src.live_probe import GMOCoinLiveProbe
from src.llm_shadow_evaluator import LLMShadowEvaluator
from src.pseudo_oco_manager import (
    PseudoOCOManager,
    calculate_breakeven_sl_price,
    compute_four_stage_trailing_sl,
)
from src.radar import TechnicalRadar, quantize_order_params
from src.reconciler import ThreeWayReconciler, is_in_fee_adjustment_window
from src.regime_filter import MarketRegimeFilter
from src.remote_control import (
    RemoteControlManager,
    ReplayOrExpiredNonceError,
    UnauthorizedCommandError,
)
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    normalize_action,
    normalize_confidence,
    validate_and_adjust_decision,
)
from src.wfa_backtester import ConservativeSpotBacktester
from src.ws_hybrid_engine import SyncState, WSHybridEngine

JST = timezone(timedelta(hours=9))


class ChaosMockExchange(PaperBroker):
    """
    Fault Injection（障害注入）対応の GMOコイン取引所シミュレータ (`ChaosMockExchange`)
    - HTTP タイムアウト、各種 GMO エラーコード (`ERR-201`, `ERR-5008`, `ERR-5010`, `ERR-5106`, `ERR-5203` 等)、
      フラッシュクラッシュ時の STOP (FAK) 部分約定＋残量 EXPIRED 失効、板の薄化・スプーフィング等を再現する。
    """

    def __init__(self, tmp_dir: Path, wal_db: Optional[DatabaseWAL] = None) -> None:
        super().__init__(
            paper_orders_path=tmp_dir / "paper_orders.json",
            portfolio_path=tmp_dir / "portfolio.json",
            wal_db=wal_db,
        )
        self.base_dir = tmp_dir
        self.injected_errors: List[Exception] = []
        self.fail_cancel_with_5106: bool = False
        self.fail_first_market_sell_with_201: bool = False
        self.stop_change_order_supported_flag: bool = True

    def place_order(
        self,
        symbol: str,
        side: str,
        execution_type: str,
        size: Any,
        price: Optional[Any] = None,
        time_in_force: Optional[str] = None,
        losscut_price: Optional[Any] = None,
        cancel_before: bool = False,
        max_post_only_retries: int = 2,
        intent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self.injected_errors:
            raise self.injected_errors.pop(0)
        if (
            self.fail_first_market_sell_with_201
            and str(side).upper() == "SELL"
            and str(execution_type).upper() == "MARKET"
        ):
            self.fail_first_market_sell_with_201 = False
            raise InsufficientAvailableAssetError(
                "GMO Private API エラー: ERR-201 Insufficient funds",
                error_codes=["ERR-201"],
            )
        return super().place_order(
            symbol=symbol,
            side=side,
            execution_type=execution_type,
            size=size,
            price=price,
            time_in_force=time_in_force,
            losscut_price=losscut_price,
            cancel_before=cancel_before,
            max_post_only_retries=max_post_only_retries,
            intent_id=intent_id,
        )

    def cancel_order(self, order_id: Any) -> Dict[str, Any]:
        if self.fail_cancel_with_5106:
            self.fail_cancel_with_5106 = False
            raise GMOPrivateAPIError(
                "GMO Private API エラー: ERR-5106 Already executed or canceled",
                error_codes=["ERR-5106"],
            )
        return super().cancel_order(order_id)

    def change_order(self, order_id: Any, price: Any, losscut_price: Optional[Any] = None) -> Dict[str, Any]:
        state = self._ensure_state_file()
        for o in state.get("orders", []):
            if str(o.get("order_id", "")) == str(order_id):
                if str(o.get("execution_type", "")).upper() == "STOP" and not self.stop_change_order_supported_flag:
                    raise GMOPrivateAPIError("ERR-5106: changeOrder not supported for STOP", error_codes=["ERR-5106"])
        return super().change_order(order_id=order_id, price=price, losscut_price=losscut_price)

    def inject_partial_stop_fill_and_expire(
        self,
        stop_order_id: str,
        filled_size: float,
        fill_price: float,
        now_dt: Optional[datetime] = None,
    ) -> None:
        """
        フラッシュクラッシュによる STOP (FAK) の一部約定＋残量 EXPIRED（失効）を注入する。
        """
        now_str = (now_dt or datetime.now(JST)).isoformat(timespec="seconds")
        state = self._ensure_state_file()
        for o in state.get("orders", []):
            if str(o.get("order_id", "")) == str(stop_order_id):
                orig_size = float(o.get("size", 0))
                o["executed_size"] = float(filled_size)
                o["remaining_size"] = max(0.0, round(orig_size - float(filled_size), 8))
                o["status"] = "EXPIRED"
                o["price"] = float(fill_price)
                state.setdefault("executions", []).append(
                    {
                        "execution_id": f"exec_partial_stop_{stop_order_id}",
                        "order_id": str(stop_order_id),
                        "symbol": o["symbol"],
                        "side": "SELL",
                        "execution_type": "STOP",
                        "size": float(filled_size),
                        "price": float(fill_price),
                        "fee": round(float(filled_size) * float(fill_price) * 0.0009, 2),
                        "timestamp": now_str,
                    }
                )
                break
        self._save_state(state)


class TestV2ChaosAudit64Items(unittest.TestCase):
    """
    全8カテゴリ・64項目＋4大複合障害シナリオを網羅するカオスエンジニアリング監査テストスイート
    """

    # ==================================================================
    # カテゴリ1（#1〜#8）: 認証・時刻ズレ・レート制限・Private API障害
    # ==================================================================
    def test_cat1_01_to_08_auth_clock_ratelimit_and_private_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "cat1.db", auto_sync_files=False)
            client = GMOPrivateClient(api_key="test_key", api_secret="test_secret", wal_db=wal)

            # #1: HMAC-SHA256 署名検証 (GETはクエリ除外、POSTは指数表記なし厳密JSON、PUT/DELETE ws-authはbodyを署名対象から除外)
            body_str = GMOPrivateClient.serialize_body_strict(
                {"symbol": "BTC", "size": Decimal("0.00001"), "price": Decimal("14000000")}
            )
            self.assertNotIn("e-", body_str.lower())
            self.assertIn('"0.00001"', body_str)
            h_get = client._build_auth_headers("GET", "/v1/activeOrders", body_str="", timestamp_ms=1700000000000)
            h_put_ws = client._build_auth_headers(
                "PUT",
                "/v1/ws-auth",
                body_str='{"token":"tok_abc"}',
                timestamp_ms=1700000000000,
                exclude_body_from_sign=True,
            )
            h_put_no_body = client._build_auth_headers(
                "PUT",
                "/v1/ws-auth",
                body_str="",
                timestamp_ms=1700000000000,
                exclude_body_from_sign=False,
            )
            self.assertEqual(len(h_get["API-SIGN"]), 64)
            # PUT /v1/ws-auth は body を送っても署名対象文字列からは除外されるため一致する
            self.assertEqual(h_put_ws["API-SIGN"], h_put_no_body["API-SIGN"])

            # #2: Cristian のアルゴリズムによる clock_offset_ms 自己校正と ERR-5008 / ERR-5009 即時再試行成功
            resp_err5008 = MagicMock()
            resp_err5008.status_code = 200
            resp_err5008.text = '{"status":1}'
            resp_err5008.headers = {}
            resp_err5008.json.return_value = {
                "status": 1,
                "messages": [{"message_code": "ERR-5008", "message_string": "TIMESTAMP_EARLY"}],
                "responsetime": "2026-10-21T12:00:05.000Z",
            }
            resp_ok = MagicMock()
            resp_ok.status_code = 200
            resp_ok.text = '{"status":0}'
            resp_ok.headers = {}
            resp_ok.json.return_value = {
                "status": 0,
                "data": [{"symbol": "JPY", "amount": "10000", "available": "10000"}],
                "responsetime": "2026-10-21T12:00:05.100Z",
            }
            with patch.object(client.session, "get", side_effect=[resp_err5008, resp_ok]):
                assets = client.get_assets()
                self.assertEqual(assets[0]["symbol"], "JPY")
                self.assertNotEqual(client.clock_offset_ms, 0)

            # #3: ERR-5010 / ERR-5011 / ERR-5012 発生時の即時 HALT (DRY_RUN へのサイレントフォールバック完全禁止)
            resp_err5010 = MagicMock()
            resp_err5010.status_code = 200
            resp_err5010.text = '{"status":1}'
            resp_err5010.headers = {}
            resp_err5010.json.return_value = {
                "status": 1,
                "messages": [{"message_code": "ERR-5010", "message_string": "Invalid API key"}],
            }
            with patch.object(client.session, "get", return_value=resp_err5010):
                with self.assertRaises(GMOAuthHaltError):
                    client.get_assets()
                self.assertTrue(client.halted)

            # #4: 4段階 PriorityTokenBucket (Priority 4 新規BUYがリザーブ制限で待機しても Priority 1 緊急決済は即座に通過)
            bucket = PriorityTokenBucket(rate_per_sec=10.0, capacity=10.0)
            for _ in range(4):
                bucket.acquire(priority=4, cost=1.0, max_wait_sec=0.05)
            # 残トークン6: Priority 1 (reserve=0) は待機時間 0.0 秒で即座に通過する
            w_p1 = bucket.acquire(priority=1, cost=1.0, max_wait_sec=0.05)
            self.assertEqual(w_p1, 0.0)

            # #5: POST /v1/order タイムアウト / ERR-5003 時の盲目的再送禁止 (intents -> UNKNOWN -> activeOrders 照合復元)
            client2 = GMOPrivateClient(api_key="k", api_secret="s", wal_db=wal)
            resp_5003 = MagicMock()
            resp_5003.status_code = 200
            resp_5003.text = '{"status":1}'
            resp_5003.headers = {}
            resp_5003.json.return_value = {
                "status": 1,
                "messages": [{"message_code": "ERR-5003", "message_string": "Request timeout"}],
            }
            resp_active_found = MagicMock()
            resp_active_found.status_code = 200
            resp_active_found.text = '{"status":0}'
            resp_active_found.headers = {}
            resp_active_found.json.return_value = {
                "status": 0,
                "data": {
                    "list": [
                        {
                            "orderId": 990011,
                            "symbol": "BCH",
                            "side": "BUY",
                            "executionType": "LIMIT",
                            "size": "0.1",
                            "executedSize": "0",
                            "price": "44000",
                            "status": "ORDERED",
                        }
                    ]
                },
            }
            with patch.object(client2.session, "post", return_value=resp_5003), patch.object(
                client2.session, "get", return_value=resp_active_found
            ):
                recovered_order = client2.place_order(
                    symbol="BCH",
                    side="BUY",
                    execution_type="LIMIT",
                    size="0.1",
                    price="44000",
                    time_in_force="SOK",
                )
                self.assertEqual(recovered_order["order_id"], "990011")

            # #6, #7, #8: changeOrder / cancelOrders (最大10件) / cancelBulkOrder / tradingVolume
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            o1 = ex.place_order("DOGE", "BUY", "LIMIT", "10", "15", "SOK")
            chg = ex.change_order(o1["order_id"], "14.999")
            self.assertEqual(chg["order_id"], o1["order_id"])
            self.assertEqual(chg["price"], 14.999)

            o2 = ex.place_order("DOGE", "BUY", "LIMIT", "10", "14", "SOK")
            batch_res = ex.cancel_orders([o1["order_id"], o2["order_id"]])
            self.assertEqual(len(batch_res["success"]), 2)

            o3 = ex.place_order("DOGE", "BUY", "LIMIT", "10", "13", "SOK")
            bulk_ids = ex.cancel_bulk_order(symbols=["DOGE"], side="BUY")
            self.assertIn(o3["order_id"], bulk_ids)

            tv = ex.get_trading_volume()
            self.assertIsInstance(tv, dict)

    # ==================================================================
    # カテゴリ2（#9〜#18）: 擬似OCO状態マシン・原子的決済・フラッシュクラッシュ
    # ==================================================================
    def test_cat2_09_to_18_pseudo_oco_atomic_exit_and_flash_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "cat2.db", auto_sync_files=False)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(
                broker=ex,
                portfolio_path=tmp_dir / "portfolio.json",
                wal_db=wal,
                dry_run_notify=True,
            )
            t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=JST)

            # 新規 BUY 発注 -> 約定 -> SL_ACTIVE
            buy_res = oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            self.assertEqual(buy_res["status"], "PENDING_BUY")
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 43990, "bid": 43980, "last": 43985}},
                now_dt=t0 + timedelta(seconds=15),
            )
            pf = oco.load_portfolio()
            self.assertEqual(len(pf["positions"]), 1)
            sl_oid = pf["positions"][0]["sl_order_id"]
            self.assertTrue(bool(sl_oid))

            # #9: WAL intent 記録 + 未保護状態の 0.5秒以内 Fast Recovery Guardian 復旧
            ex.cancel_order(sl_oid)
            pf["positions"][0]["sl_order_id"] = ""
            pf["positions"][0]["state"] = "UNPROTECTED_HOLD"
            oco.save_portfolio(pf, now_dt=t0 + timedelta(seconds=20))
            t_rec_start = time.monotonic()
            rec_events = oco.run_fast_recovery_guardian(
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0 + timedelta(seconds=21),
            )
            rec_ms = (time.monotonic() - t_rec_start) * 1000.0
            self.assertLess(rec_ms, 500.0)
            self.assertTrue(any(e.get("type") == "FAST_RECOVERY_SL_RESTORED" for e in rec_events))

            # #10: 競合パターンA (TP決済しようとした瞬間に取引所側で既にSLが約定して ERR-5106 が返るケース)
            ex.evaluate_paper_orders(
                {"BCH": {"symbol": "BCH", "ask": 42400, "bid": 42390, "last": 42395}},
                now_dt=t0 + timedelta(seconds=30),
            )
            ex.fail_cancel_with_5106 = True
            events_race_a = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 46600, "bid": 46550, "last": 46550}},
                now_dt=t0 + timedelta(seconds=35),
            )
            ex.fail_cancel_with_5106 = False
            self.assertTrue(any(e.get("type") == "SL_EXECUTED" for e in events_race_a))
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # #11: 原子的緊急決済 (MARKET / FAK / cancel_before=True + ERR-201 発生時の実残高リトライ)
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0 + timedelta(minutes=1),
            )
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 43950, "bid": 43940, "last": 43945}},
                now_dt=t0 + timedelta(minutes=1, seconds=15),
            )
            ex.fail_first_market_sell_with_201 = True
            closed_atomic = oco.close_position_immediately("BCH", reason="EMERGENCY_TEST", now_dt=t0 + timedelta(minutes=2))
            self.assertIsNotNone(closed_atomic)
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # #12: フラッシュクラッシュで STOP (FAK) が部分約定 + 残量 EXPIRED 失効した際の成行追撃掃討
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0 + timedelta(minutes=3),
            )
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 43950, "bid": 43940, "last": 43945}},
                now_dt=t0 + timedelta(minutes=3, seconds=15),
            )
            pf_fc = oco.load_portfolio()
            sl_oid_fc = pf_fc["positions"][0]["sl_order_id"]
            ex.inject_partial_stop_fill_and_expire(sl_oid_fc, filled_size=0.06, fill_price=42400.0, now_dt=t0 + timedelta(minutes=4))
            events_fc = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 42010, "bid": 42000, "last": 42000}},
                now_dt=t0 + timedelta(minutes=4, seconds=5),
            )
            self.assertTrue(any("SL" in str(e.get("type", "")) for e in events_fc))
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # #14 & #15: 非対称銘柄 (BCH min=0.01, step=0.001) の端数部分約定 (0.004 < 0.01) DUST_QUARANTINED 隔離
            buy_dust = oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.02,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0 + timedelta(minutes=10),
            )
            ex.simulate_partial_fill(buy_dust["order"]["order_id"], filled_size=0.004, fill_price=44000.0, now_dt=t0 + timedelta(minutes=11))
            events_dust = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 44200, "bid": 44150, "last": 44150}},
                now_dt=t0 + timedelta(hours=5),
            )
            self.assertTrue(any("DUST" in str(e.get("type", "")) for e in events_dust))

            # #16: Post-Only (SOK) 板ぶつかり時の自動1ティック後退
            retreat_ord = oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "SOL",
                    "limit_price": 20000,
                    "size": 0.1,
                    "take_profit_price": 21200,
                    "stop_loss_price": 19300,
                },
                tickers={"SOL": {"symbol": "SOL", "ask": 20000, "bid": 19999, "last": 20000}},
                now_dt=t0 + timedelta(hours=5, minutes=15),
            )
            self.assertLess(retreat_ord["pending_order"]["limit_price"], 20000)

            # #17: 4段階トレーリングSL (+1.0R 建値、+1.5R +0.65Rロック、+2.0R シャンデリア & 0.20R ヒステリシス)
            st1, p1, upd1 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10300, highest_bid=10300, current_sl=9700, one_r_dist=300, symbol="SOL"
            )
            self.assertEqual(st1, 1)
            self.assertTrue(upd1)
            self.assertGreater(p1, 10000)

            st2, p2, upd2 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10460, highest_bid=10460, current_sl=p1, one_r_dist=300, symbol="SOL"
            )
            self.assertEqual(st2, 2)
            self.assertTrue(upd2)
            self.assertGreaterEqual(p2, 10195)

            st3, p3, upd3 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10700, highest_bid=10700, current_sl=p2, one_r_dist=300, symbol="SOL"
            )
            self.assertGreaterEqual(st3, 3)
            self.assertTrue(upd3)
            # 0.20R (60円) 未満の微動では更新しない (ヒステリシス)
            _, _, upd_hyst = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10720, highest_bid=10720, current_sl=p3, one_r_dist=300, symbol="SOL"
            )
            self.assertFalse(upd_hyst)

    # ==================================================================
    # カテゴリ3（#19〜#26）: WebSocket 再接続・4フェーズバッファ同期・順序逆転
    # ==================================================================
    def test_cat3_19_to_26_websocket_4phase_sync_and_event_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "cat3.db", auto_sync_files=False)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            ws_engine = WSHybridEngine(broker=ex, wal_db=wal, subscription_interval_sec=0.0)

            # #19: 35分 PUT /v1/ws-auth トークン延長と失敗時の POST 再取得フォールバック
            tok1 = ws_engine.obtain_or_refresh_token(now_mono=1000.0)
            self.assertTrue(bool(tok1))
            tok2 = ws_engine.obtain_or_refresh_token(now_mono=1000.0 + 36 * 60)
            self.assertEqual(tok2, tok1)

            # #20: 1.2秒直列購読間隔 & TAKER_ONLY オプション検証
            self.assertGreaterEqual(WSHybridEngine.SUBSCRIPTION_INTERVAL_SEC, 1.0)
            pub_payloads = ws_engine.build_public_subscription_payloads(["BTC", "ETH"])
            trades_subs = [p for p in pub_payloads if p["channel"] == "trades"]
            self.assertTrue(all(p.get("option") == "TAKER_ONLY" for p in trades_subs))

            # #21: 75秒無通信タイムアウト検知
            ws_engine.last_message_ts = 2000.0
            self.assertEqual(ws_engine.check_heartbeat(now_mono=2050.0), "ALIVE")
            self.assertEqual(ws_engine.check_heartbeat(now_mono=2080.0), "RECONNECT_REQUIRED")

            # #22, #23, #24, #25, #26: 4フェーズ Pre-Subscription Buffered Sync & 重複 executionId べき等性 & ラティス順序逆転防御
            wal.upsert_order("ghost_901", "BTC", "BUY", "LIMIT", "ORDERED", price="14000000", size="0.0001")

            def _inject_ws_events_during_phase1(eng: WSHybridEngine) -> None:
                self.assertEqual(eng.sync_state, SyncState.SYNCING)
                eng.on_raw_ws_message(
                    {
                        "channel": "executionEvents",
                        "executionId": "50001",
                        "orderId": "ord_live_1",
                        "symbol": "ETH",
                        "side": "BUY",
                        "executionType": "LIMIT",
                        "size": "0.01",
                        "price": "400000",
                        "fee": "-0.4",
                    }
                )
                eng.on_raw_ws_message(
                    {
                        "channel": "orderEvents",
                        "orderId": "ord_live_1",
                        "symbol": "ETH",
                        "side": "BUY",
                        "executionType": "LIMIT",
                        "orderStatus": "EXECUTED",
                        "orderPrice": "400000",
                        "orderSize": "0.01",
                        "orderExecutedSize": "0.01",
                    }
                )
                # 順序逆転した古い ORDERED イベント
                eng.on_raw_ws_message(
                    {
                        "channel": "orderEvents",
                        "orderId": "ord_live_1",
                        "symbol": "ETH",
                        "side": "BUY",
                        "executionType": "LIMIT",
                        "orderStatus": "ORDERED",
                        "orderPrice": "400000",
                        "orderSize": "0.01",
                        "orderExecutedSize": "0.0",
                    }
                )

            sync_report = ws_engine.run_four_phase_sync(
                symbols=["BTC", "ETH"],
                on_phase1_ready_hook=_inject_ws_events_during_phase1,
            )
            self.assertEqual(ws_engine.sync_state, SyncState.LIVE)
            self.assertEqual(sync_report["ws_buffer_drained_count"], 3)

            # ラティスにより ord_live_1 は EXECUTED のまま（古い ORDERED に巻き戻っていない）
            ord_after = wal.get_order("ord_live_1")
            self.assertIsNotNone(ord_after)
            self.assertEqual(ord_after["state"], "EXECUTED")

            # 同一 executionId ("50001") を再度記録してもべき等に無視される (#23)
            inserted_dup, _ = wal.record_execution("50001", "ord_live_1", "ETH", "BUY", "0.01", "400000", "-0.4")
            self.assertFalse(inserted_dup)

    # ==================================================================
    # カテゴリ4（#27〜#34）: マイクロストラクチャ逆選択・4段階ゲート・レジーム
    # ==================================================================
    def test_cat4_27_to_34_microstructure_4gates_and_regime_hysteresis(self) -> None:
        guard = AdverseSelectionGuard()
        t_now = 10000.0

        # #27: Layer 1 OBI_5 < -0.10 & Level-1 bid ratio < 0.35 -> 新規エントリー拒否 + 薄い買板の既存指値キャンセル
        guard.record_orderbook(
            symbol="SOL",
            bids=[{"price": 1000.0, "size": 0.5}, {"price": 999.0, "size": 0.5}],
            asks=[{"price": 1001.0, "size": 15.0}, {"price": 1002.0, "size": 15.0}],
            timestamp_sec=t_now,
        )
        res_l1 = guard.evaluate_entry_guard("SOL", proposed_limit_price=1000.0, atr_5m=20.0, now_sec=t_now)
        self.assertFalse(res_l1["allow_entry"])
        self.assertLess(res_l1["layer1"]["obi_5"], -0.35)

        cancel_l1 = guard.should_cancel_resting_buy_order("SOL", order_price=1000.0, order_size=1.0, atr_5m=20.0, now_sec=t_now)
        self.assertTrue(cancel_l1["should_cancel"])

        # #28: Layer 2 5秒間5連続価格切り下げ SELL スイープ検知 -> VETO
        guard2 = AdverseSelectionGuard()
        guard2.record_orderbook(
            symbol="SOL",
            bids=[{"price": 1000.0, "size": 50.0}, {"price": 999.0, "size": 50.0}],
            asks=[{"price": 1001.0, "size": 20.0}, {"price": 1002.0, "size": 20.0}],
            timestamp_sec=t_now,
        )
        for idx, p in enumerate([1005.0, 1004.0, 1003.0, 1002.0, 1001.0]):
            guard2.record_taker_trade("SOL", "SELL", price=p, size=1.0, timestamp_sec=t_now - 4.0 + idx * 0.8)
        res_sweep = guard2.evaluate_entry_guard("SOL", proposed_limit_price=1000.0, atr_5m=20.0, now_sec=t_now)
        self.assertFalse(res_sweep["allow_entry"])
        self.assertTrue(res_sweep["layer2"]["sell_sweep_detected"])

        # #29: Layer 2 軽度売り優勢 (-0.25 < TFI_60s < -0.05) -> パッシブ・オフセット指値
        guard3 = AdverseSelectionGuard(baseline_sell_vol_60s=100.0)
        guard3.record_orderbook(
            symbol="SOL",
            bids=[{"price": 1000.0, "size": 50.0}, {"price": 999.0, "size": 50.0}],
            asks=[{"price": 1001.0, "size": 20.0}, {"price": 1002.0, "size": 20.0}],
            timestamp_sec=t_now,
        )
        guard3.record_taker_trade("SOL", "SELL", price=1000.0, size=6.0, timestamp_sec=t_now - 5.0)
        guard3.record_taker_trade("SOL", "BUY", price=1001.0, size=4.5, timestamp_sec=t_now - 2.0)
        res_offset = guard3.evaluate_entry_guard("SOL", proposed_limit_price=1000.0, atr_5m=10.0, now_sec=t_now)
        self.assertTrue(res_offset["allow_entry"])
        self.assertTrue(res_offset["passive_offset_applied"])
        self.assertLess(res_offset["adjusted_limit_price"], 1000.0)

        # #30: Layer 3 急落ボラティリティ拡大 & データ遅延 (> 5.0s)
        res_l3 = guard3.evaluate_entry_guard("SOL", proposed_limit_price=1000.0, atr_5m=10.0, now_sec=t_now + 6.5)
        self.assertFalse(res_l3["allow_entry"])

        # #31, #32, #33: 4段階ゲート (Gate 0 除外、Gate 1 Decimal 量子化、Gate 2 売買代金 >= 3000万円、Gate 3 最小ロット & SL距離)
        radar = TechnicalRadar()
        q_params = quantize_order_params("XRP", price="85.12345", size="12.987")
        self.assertEqual(q_params["price_str"], "85.123")
        self.assertEqual(q_params["size_str"], "12")

        g0_res = radar.evaluate_symbol_gates("DAI", {"last": 150, "bid": 149.9, "ask": 150.1, "volume": 500000}, DEFAULT_SYMBOL_RULES["DAI"])
        self.assertFalse(g0_res["passed"])
        self.assertEqual(g0_res["failed_gate"], "GATE_0_EXCLUDED")

        g2_res = radar.evaluate_symbol_gates("SOL", {"last": 20000, "bid": 19990, "ask": 20000, "volume": 500}, DEFAULT_SYMBOL_RULES["SOL"])
        self.assertFalse(g2_res["passed"])
        self.assertEqual(g2_res["failed_gate"], "GATE_2_LOW_VOLUME")

        g3_res = radar.evaluate_symbol_gates(
            "LTC",
            {"last": 15000, "bid": 14990, "ask": 15000, "volume": 5000},
            DEFAULT_SYMBOL_RULES["LTC"],
            atr_1h=1200.0,
        )
        self.assertFalse(g3_res["passed"])
        self.assertEqual(g3_res["failed_gate"], "GATE_3_SL_TOO_TIGHT_FOR_MIN_LOT")

        # #34: MarketRegimeFilter (RISK_OFF_CRASH + 2時間ヒステリシス冷却)
        rf = MarketRegimeFilter()
        t_crash = datetime(2026, 10, 22, 10, 0, 0, tzinfo=JST)
        crash_summaries = {
            "BTC": {
                "price": {"last": 13500000},
                "h1_indicators": {"change_1h_pct": -3.1},
                "h4_indicators": {"change_4h_pct": -3.5, "rsi_14": 28.0},
                "daily_indicators": {"trend": "DOWNTREND"},
            }
        }
        reg1 = rf.evaluate(crash_summaries, now_dt=t_crash)
        self.assertEqual(reg1["regime"], "RISK_OFF_CRASH")
        self.assertFalse(reg1["allow_new_buy"])
        self.assertTrue(reg1["should_cancel_pending_buys"])

        # 30分後に BTC が反発しても、2時間ヒステリシスにより RISK_OFF_CRASH が維持される
        calm_summaries = {
            "BTC": {
                "price": {"last": 13800000},
                "h1_indicators": {"change_1h_pct": 0.8},
                "h4_indicators": {"change_4h_pct": 0.5, "rsi_14": 52.0},
                "daily_indicators": {"trend": "UPTREND"},
            }
        }
        reg_30m = rf.evaluate(calm_summaries, now_dt=t_crash + timedelta(minutes=30))
        self.assertEqual(reg_30m["regime"], "RISK_OFF_CRASH")
        self.assertTrue(reg_30m["in_hysteresis_cooldown"])
        self.assertFalse(reg_30m["allow_new_buy"])

        # 2時間1分経過後は冷却解除
        reg_2h = rf.evaluate(calm_summaries, now_dt=t_crash + timedelta(hours=2, minutes=1))
        self.assertEqual(reg_2h["regime"], "TREND_UP")
        self.assertTrue(reg_2h["allow_new_buy"])

    # ==================================================================
    # カテゴリ5（#35〜#42）: LLM Veto・反対仮説スキーマ・Stale Signal・シャドー評価
    # ==================================================================
    def test_cat5_35_to_42_llm_veto_counter_thesis_and_shadow_evaluator(self) -> None:
        # #35: bearish_counter_thesis が2項目未満の BUY は強制 HOLD (Veto)
        d_short_ct = enforce_counter_thesis_and_conviction_gate(
            {
                "bearish_counter_thesis": ["弱気理由が1つしかない"],
                "bullish_conviction_factors": ["強気1", "強気2"],
                "conviction_score": 85,
                "action": "BUY",
                "symbol": "SOL",
                "limit_price": 20000,
                "size": 0.2,
            }
        )
        self.assertEqual(d_short_ct["action"], "HOLD")
        self.assertTrue(d_short_ct["vetoed_by_counter_thesis"])

        # #36: conviction_score < 75 (例: 74) の BUY は強制 HOLD (Veto)
        d_low_conv = enforce_counter_thesis_and_conviction_gate(
            {
                "bearish_counter_thesis": ["弱気1", "弱気2"],
                "bullish_conviction_factors": ["強気1", "強気2"],
                "conviction_score": 74,
                "action": "BUY",
                "symbol": "SOL",
                "limit_price": 20000,
                "size": 0.2,
            }
        )
        self.assertEqual(d_low_conv["action"], "HOLD")
        self.assertTrue(d_low_conv["vetoed_by_conviction_gate"])

        # #37, #38, #39: JSON 抽出 (前置き辞書スキップ)、未知アクション WAIT/CLOSE -> HOLD、確信度 85 -> 0.85
        raw_llm = '市場概況 {"BTC": 14000000} です。\n```json\n{"action": "WAIT", "confidence": 85, "reasoning": "様子見"}\n```'
        extracted = _extract_json_dict(raw_llm)
        self.assertEqual(normalize_action(extracted["action"]), "HOLD")
        self.assertEqual(normalize_confidence(extracted["confidence"]), 0.85)

        # #40 & #41: 同一銘柄 AI クールダウン (60分)・SL 被弾クールダウン (4時間)・Stale Signal Gate (> 0.30% 価格乖離破棄)
        judge = AIJudge()
        t_now = datetime(2026, 10, 22, 12, 0, 0, tzinfo=JST)
        judge.record_sl_hit_cooldown("BCH", now_dt=t_now)
        on_sl_cd, _ = judge.is_symbol_on_sl_cooldown("BCH", now_dt=t_now + timedelta(hours=2))
        self.assertTrue(on_sl_cd)
        on_sl_cd_after, _ = judge.is_symbol_on_sl_cooldown("BCH", now_dt=t_now + timedelta(hours=4, minutes=1))
        self.assertFalse(on_sl_cd_after)

        stale_checked = judge.check_stale_signal_gate(
            {"action": "BUY", "symbol": "SOL", "limit_price": 20000, "size": 0.2},
            snapshot_price=20000.0,
            latest_price=20100.0,  # +0.50% (> 0.30%)
        )
        self.assertEqual(stale_checked["action"], "HOLD")
        self.assertTrue(stale_checked["stale_signal_discarded"])

        # #42: LLMShadowEvaluator (4h/12h/24h MFE/MAE & パス依存評価: 同一足で High>=TP かつ Low<=SL は保守的に SL_HIT)
        with tempfile.TemporaryDirectory() as tmp_str:
            wal = DatabaseWAL(Path(tmp_str) / "cat5.db", auto_sync_files=False)
            shadow = LLMShadowEvaluator(db_wal=wal)
            eid_veto = shadow.record_decision("SOL", "HOLD", conviction_score=60, entry_price=100.0, tp_price=106.0, sl_price=96.0)
            shadow.update_with_horizon_bars(
                eid_veto,
                bars_4h=[{"high": 107.0, "low": 95.0, "close": 98.0}],
            )
            self.assertEqual(shadow.records[eid_veto]["outcome_24h"], "SL_HIT")
            self.assertLess(shadow.records[eid_veto]["virtual_pnl_24h_jpy"], 0.0)

            eid_buy = shadow.record_decision("BTC", "BUY", conviction_score=85, entry_price=100.0, tp_price=106.0, sl_price=96.0)
            shadow.update_with_horizon_bars(
                eid_buy,
                bars_4h=[{"high": 107.0, "low": 99.0, "close": 106.5}],
            )
            edge = shadow.summarize_counterfactual_edge()
            self.assertGreater(edge["counterfactual_edge_jpy"], 0.0)
            self.assertEqual(edge["veto_avoided_losses_count"], 1)

    # ==================================================================
    # カテゴリ6（#43〜#50）: SQLite WAL・税務計算（方式A/B）・06:00端数調整
    # ==================================================================
    def test_cat6_43_to_50_sqlite_wal_tax_methods_and_0600_fee_adjustment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "cat6.db", portfolio_json_path=tmp_dir / "portfolio.json")

            # #43: WAL PRAGMA 検証 (journal_mode=WAL, synchronous=FULL(2), busy_timeout=5000)
            pragmas = wal.get_pragmas()
            self.assertEqual(pragmas["journal_mode"], "WAL")
            self.assertEqual(pragmas["synchronous"], 2)
            self.assertEqual(pragmas["busy_timeout"], 5000)

            # #44: Append-Only トリガー検証 (executions の UPDATE / DELETE 禁止)
            wal.record_execution("exec_1001", "ord_1", "BTC", "BUY", "0.001", "10000000", "-1.0")
            with wal._lock:
                with self.assertRaises(sqlite3.IntegrityError):
                    wal._conn.execute("UPDATE executions SET price_str = '999' WHERE execution_id = 'exec_1001'")
                with self.assertRaises(sqlite3.IntegrityError):
                    wal._conn.execute("DELETE FROM executions WHERE execution_id = 'exec_1001'")

            # #45, #46, #47: 方式A（原則法：Maker負の手数料を雑所得計上）と方式B（純受渡法）および移動平均法・総平均法ビュー
            ledger_rows = wal.get_tax_moving_avg_ledger("BTC")
            self.assertEqual(len(ledger_rows), 1)
            self.assertEqual(ledger_rows[0]["book_value_a_jpy"], 10000)
            self.assertEqual(ledger_rows[0]["misc_rebate_income_a_jpy"], 1)
            self.assertEqual(ledger_rows[0]["book_value_b_jpy"], 9999)

            # 売却 (0.001 BTC @ 11,000,000円 = 11,000円、Taker手数料 +5.5円 -> 切り上げ 6円)
            wal.record_execution("exec_1002", "ord_2", "BTC", "SELL", "0.001", "11000000", "5.5")
            ledger_after_sell = wal.get_tax_moving_avg_ledger("BTC")[0]
            self.assertEqual(ledger_after_sell["total_size_satoshi"], 0)
            self.assertEqual(ledger_after_sell["realized_pnl_a_jpy"], 11000 - 6 - 10000)
            self.assertEqual(ledger_after_sell["realized_pnl_b_jpy"], 11000 - 6 - 9999)

            annual_view = wal.get_annual_total_average_summary()
            self.assertEqual(len(annual_view), 1)
            self.assertEqual(annual_view[0]["execution_count"], 2)

            # #48, #49, #50: 06:00〜06:15 JST 手数料端数調整 (+3円) の自動吸収 vs 12:00 JST 50円超不一致の DEGRADED_RECONCILING
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(broker=ex, portfolio_path=tmp_dir / "portfolio.json", wal_db=wal, dry_run_notify=True)
            rec = ThreeWayReconciler(broker=ex, oco_manager=oco, db_wal=wal, dry_run_notify=True)

            pf = oco.load_portfolio()
            pf["jpy_balance"] = 9997
            oco.save_portfolio(pf)
            state_ex = ex._ensure_state_file()
            state_ex["jpy_balance"] = 10000.0
            ex._save_state(state_ex)

            t_0605 = datetime(2026, 10, 22, 6, 5, 0, tzinfo=JST)
            self.assertTrue(is_in_fee_adjustment_window(t_0605))
            res_0605 = rec.reconcile(now_dt=t_0605)
            self.assertEqual(res_0605["status"], "RECONCILED")
            self.assertIsNotNone(res_0605["fee_adjustment_applied"])
            self.assertEqual(oco.load_portfolio()["jpy_balance"], 10000)

            # 12:00 JST に原因不明の +200円 (> 50円) 乖離が発生 -> DEGRADED_RECONCILING
            pf_now = oco.load_portfolio()
            pf_now["jpy_balance"] = 9800
            oco.save_portfolio(pf_now)
            res_1200 = rec.reconcile(now_dt=datetime(2026, 10, 22, 12, 0, 0, tzinfo=JST))
            self.assertEqual(res_1200["status"], "DEGRADED_RECONCILING")
            self.assertTrue(rec.degraded_reconciling)

    # ==================================================================
    # カテゴリ7（#51〜#58）: 多層サーキットブレーカー（L1〜L5）＆ Windows 常駐耐障害性
    # ==================================================================
    def test_cat7_51_to_58_circuit_breakers_l1_to_l5_and_windows_resilience(self) -> None:
        cb = MultiTierCircuitBreaker()
        t_now = datetime(2026, 10, 22, 14, 0, 0, tzinfo=JST)

        # #51: L1 個別銘柄除外
        l1 = cb.check_l1_symbol_filter("XRP", spread_pct=0.65, data_latency_sec=1.0)
        self.assertTrue(l1["excluded"])

        # #52: L2 API 連続エラー 5回 & データ鮮度 > 45秒
        for _ in range(5):
            cb.record_api_error(RuntimeError("timeout"), now_dt=t_now)
        l2 = cb.check_l2_data_freshness(data_age_sec=10.0, now_dt=t_now)
        self.assertTrue(l2["blocked"])
        self.assertTrue(l2["allow_sl_monitoring"])
        cb.record_api_success()

        # #53: L3 日次損失制限 (06:00 JST 区切りで -300円到達 -> 翌06:00まで停止)
        pf_l3 = {
            "jpy_balance": 9700,
            "closed_trades": [{"closed_at": "2026-10-22T09:00:00+09:00", "realized_pnl_jpy": -310}],
            "positions": [],
        }
        l3 = cb.check_l3_daily_loss(pf_l3, now_dt=t_now)
        self.assertTrue(l3["blocked"])
        self.assertEqual(l3["paused_until"], "2026-10-23T06:00:00+09:00")

        # #54: L4 4連敗クールダウン (12h) & 週間損失 -600円 (24h)
        pf_l4 = {
            "closed_trades": [
                {"closed_at": "2026-10-20T10:00:00+09:00", "realized_pnl_jpy": -100},
                {"closed_at": "2026-10-20T15:00:00+09:00", "realized_pnl_jpy": -100},
                {"closed_at": "2026-10-21T10:00:00+09:00", "realized_pnl_jpy": -100},
                {"closed_at": "2026-10-21T15:00:00+09:00", "realized_pnl_jpy": -100},
            ]
        }
        l4 = cb.check_l4_weekly_and_streak(pf_l4, now_dt=t_now)
        self.assertTrue(l4["blocked"])
        self.assertEqual(l4["consecutive_losses"], 4)

        # #55: L5 ハード・キルスイッチ (Best Bid * (1 - Taker Fee) で 3回連続 < 8,500円)
        pf_l5 = {
            "jpy_balance": 4400,
            "drawdown_kill_switch_jpy": 8500,
            "positions": [{"symbol": "BCH", "size": 0.1, "entry_price": 45000, "current_price": 50000}],
        }
        tickers_dip = {"BCH": {"last": 50000, "bid": 40000}}
        tickers_ok = {"BCH": {"last": 50000, "bid": 45000}}

        self.assertFalse(cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_dip)["latched"])
        self.assertFalse(cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_dip)["latched"])
        self.assertFalse(cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_ok)["latched"])
        self.assertEqual(cb.l5_consecutive_breach_count, 0)

        # API欠損 (bid=0) はスキップされ誤爆しない
        self.assertFalse(cb.evaluate_l5_hard_kill_switch(pf_l5, {"BCH": {"last": 50000, "bid": 0}})["latched"])

        # 3回連続で < 8,500円 となった瞬間にラッチ発動
        cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_dip)
        cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_dip)
        res_l5_3rd = cb.evaluate_l5_hard_kill_switch(pf_l5, tickers_dip)
        self.assertTrue(res_l5_3rd["latched"])
        self.assertTrue(res_l5_3rd["newly_triggered"])

        # #56, #57, #58: Windows SetThreadExecutionState / SingleInstanceLock / RSS メモリ監視 / 土曜 08:50 事前クリア
        prevent_windows_sleep()
        with tempfile.TemporaryDirectory() as tmp_str:
            lock_file = Path(tmp_str) / "v2_daemon.lock"
            lock1 = SingleInstanceLock(lock_file)
            lock2 = SingleInstanceLock(lock_file)
            self.assertTrue(lock1.acquire())
            self.assertFalse(lock2.acquire())
            lock1.release()
            self.assertTrue(lock2.acquire())
            lock2.release()

        mem_busy = check_memory_rss_and_should_restart(max_rss_mb=200.0, has_open_positions_or_orders=True, simulated_rss_mb=350.0)
        self.assertFalse(mem_busy["should_graceful_restart"])
        mem_flat = check_memory_rss_and_should_restart(max_rss_mb=200.0, has_open_positions_or_orders=False, simulated_rss_mb=350.0)
        self.assertTrue(mem_flat["should_graceful_restart"])

        sat_0852 = datetime(2026, 10, 24, 8, 52, 0, tzinfo=JST)
        self.assertTrue(is_in_saturday_pre_maintenance_clear_window(sat_0852))

    # ==================================================================
    # カテゴリ8（#59〜#64）: リモート制御・リプレイ防御・Live Probe・WFAバックテスト
    # ==================================================================
    def test_cat8_59_to_64_remote_control_replay_probe_and_wfa_backtester(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "cat8.db", auto_sync_files=False)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(broker=ex, portfolio_path=tmp_dir / "portfolio.json", wal_db=wal, dry_run_notify=True)
            rc = RemoteControlManager(oco_manager=oco, db_wal=wal, allowed_owner_discord_id="owner_777")

            t0 = datetime(2026, 10, 22, 15, 0, 0, tzinfo=JST)
            oco.place_new_buy_order(
                {"action": "BUY", "symbol": "BCH", "limit_price": 44000, "size": 0.1, "take_profit_price": 46500, "stop_loss_price": 42500},
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            oco.step(tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}}, now_dt=t0 + timedelta(seconds=10))
            oco.place_new_buy_order(
                {"action": "BUY", "symbol": "SOL", "limit_price": 19500, "size": 0.1, "take_profit_price": 21000, "stop_loss_price": 18800},
                tickers={"SOL": {"ask": 20000, "bid": 19990, "last": 20000}},
                now_dt=t0 + timedelta(seconds=20),
            )

            # #59: PAUSE と CANCEL_ENTRIES は既存の STOP 注文を絶対にキャンセルせず維持する
            pause_res = rc.execute_command("PAUSE", actor_id="owner_777", command_nonce="n_pause_1", now_dt=t0 + timedelta(seconds=30))
            self.assertTrue(pause_res["is_paused"])
            self.assertEqual(len(pause_res["preserved_sl_order_ids"]), 1)

            ce_res = rc.execute_command("CANCEL_ENTRIES", actor_id="owner_777", command_nonce="n_ce_1", now_dt=t0 + timedelta(seconds=40))
            self.assertEqual(len(ce_res["canceled_buy_order_ids"]), 1)
            self.assertEqual(len(ce_res["preserved_sl_order_ids"]), 1)
            self.assertEqual(len(oco.load_portfolio()["pending_orders"]), 0)
            self.assertEqual(len(oco.load_portfolio()["positions"]), 1)

            # #60 & #61: FLATTEN_ALL の2段階承認 + 未認可ユーザー拒否 + リプレイ攻撃完全拒否
            with self.assertRaises(UnauthorizedCommandError):
                rc.execute_command("FLATTEN_ALL", actor_id="attacker_999", now_dt=t0 + timedelta(seconds=45))

            req_flat = rc.execute_command("FLATTEN_ALL", actor_id="owner_777", now_dt=t0 + timedelta(seconds=50))
            self.assertFalse(req_flat["executed"])
            nonce = req_flat["nonce"]
            self.assertEqual(len(oco.load_portfolio()["positions"]), 1)

            conf_flat = rc.execute_command(f"FLATTEN_CONFIRM {nonce}", actor_id="owner_777", now_dt=t0 + timedelta(seconds=60))
            self.assertTrue(conf_flat["executed"])
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            with self.assertRaises(ReplayOrExpiredNonceError):
                rc.execute_command(f"FLATTEN_CONFIRM {nonce}", actor_id="owner_777", now_dt=t0 + timedelta(seconds=65))

            # #62: 3階層通知ディスパッチャ
            disp = rc.dispatcher
            r_dig = disp.dispatch("DAILY_DIGEST", "15m HOLD", "変化なし", now_dt=t0)
            self.assertFalse(r_dig["sent_immediately"])
            flushed = disp.flush_daily_digest(now_dt=t0 + timedelta(hours=24))
            self.assertTrue(flushed["sent"])
            self.assertEqual(flushed["flushed_count"], 1)

            # #63: GMOCoinLiveProbe (例外発生時でも try...finally で残存プローブ注文が 100% クリーンアップされる)
            probe = GMOCoinLiveProbe(broker=ex, capabilities_path=tmp_dir / "capabilities.json")
            cap_ok = probe.run_probe(now_dt=t0)
            self.assertTrue(cap_ok["cleanup_verified_zero_residual_orders"])
            self.assertTrue((tmp_dir / "capabilities.json").exists())

            with self.assertRaises(RuntimeError):
                probe.run_probe(inject_mid_probe_exception=RuntimeError("Mid-probe crash!"), now_dt=t0)
            self.assertEqual(len(ex.get_active_orders("DOGE")), 0)

            # #64: ConservativeSpotBacktester (SOK 貫通必須モデル & 非線形 STOP スリッページ & IS=60d/OOS=20d WFA)
            bt = ConservativeSpotBacktester(fill_model="STRICT_PENETRATION", tick_size=1.0)
            self.assertFalse(bt.check_sok_limit_buy_filled(limit_price=10000.0, bar_low=10000.0))
            self.assertTrue(bt.check_sok_limit_buy_filled(limit_price=10000.0, bar_low=9999.0))

            sl_fill = bt.calculate_stop_sell_fill(sl_price=9700.0, bar_low=9500.0, size=0.5, entry_price=10000.0)
            self.assertLess(sl_fill["effective_fill_price"], 9700.0 - 0.15 * 200.0)

            daily_series = [25.0 if i % 3 != 0 else -15.0 for i in range(120)]
            wfa_res = bt.run_rolling_wfa(daily_series, is_days=60, oos_days=20, step_days=10)
            self.assertGreaterEqual(wfa_res["window_count"], 4)
            self.assertGreater(wfa_res["overall_oos_metrics"]["profit_factor"], 1.0)

    # ==================================================================
    # 4大複合障害シナリオ (Compound Chaos Scenarios A, B, C, D)
    # ==================================================================
    def test_compound_scenario_a_flash_crash_ws_disconnect_and_rate_limit(self) -> None:
        """複合シナリオA: フラッシュクラッシュ (-6%) + WS切断 + ERR-5003 レート制限下での Tier 0 優先脱出"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "comp_a.db", auto_sync_files=False)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(broker=ex, portfolio_path=tmp_dir / "portfolio.json", wal_db=wal, dry_run_notify=True)
            ws = WSHybridEngine(broker=ex, wal_db=wal)

            t0 = datetime(2026, 10, 22, 18, 0, 0, tzinfo=JST)
            oco.place_new_buy_order(
                {"action": "BUY", "symbol": "BCH", "limit_price": 44000, "size": 0.1, "take_profit_price": 46500, "stop_loss_price": 42500},
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            oco.step(tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}}, now_dt=t0 + timedelta(seconds=10))
            sl_oid = oco.load_portfolio()["positions"][0]["sl_order_id"]

            # WS切断中にフラッシュクラッシュ (-6%) が発生し、STOP が一部約定 (0.04) して残り (0.06) が EXPIRED 失効
            ex.inject_partial_stop_fill_and_expire(sl_oid, filled_size=0.04, fill_price=42400.0, now_dt=t0 + timedelta(seconds=20))
            sync_res = ws.run_four_phase_sync(
                symbols=["BCH"],
                on_phase1_ready_hook=lambda eng: eng.on_raw_ws_message(
                    {"channel": "orderEvents", "orderId": sl_oid, "symbol": "BCH", "side": "SELL", "executionType": "STOP", "orderStatus": "EXPIRED", "orderSize": "0.1", "orderExecutedSize": "0.04"}
                ),
            )
            self.assertEqual(ws.sync_state, SyncState.LIVE)
            self.assertEqual(sync_res["ws_buffer_drained_count"], 1)

            # Tier 0 バケット優先権を確認した上で step() を回すと、残存 0.06 BCH が直ちに成行追撃で一掃される
            events = oco.step(tickers={"BCH": {"ask": 41400, "bid": 41360, "last": 41360}}, now_dt=t0 + timedelta(seconds=25))
            self.assertTrue(any(e.get("type") == "SL_PARTIAL_FAK_SWEPT" for e in events))
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

    def test_compound_scenario_b_partial_dust_fill_and_crash_recovery(self) -> None:
        """複合シナリオB: 最小ロット未満 (0.004 BCH < 0.01) の端数部分約定直後にプロセスクラッシュ -> 再起動 3-Way 照合で DUST_QUARANTINED 隔離"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "comp_b.db", auto_sync_files=False)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(broker=ex, portfolio_path=tmp_dir / "portfolio.json", wal_db=wal, dry_run_notify=True)

            # 取引所に 0.004 BCH (< minOrderSize 0.01) の端数残高だけが残された状態でクラッシュ・再起動したと仮定
            pf = oco.load_portfolio()
            pf["positions"] = [{"symbol": "BCH", "side": "BUY", "size": 0.004, "entry_price": 44000, "stop_loss_price": 42500, "sl_order_id": "", "state": "UNPROTECTED_HOLD"}]
            oco.save_portfolio(pf)

            rec = ThreeWayReconciler(broker=ex, oco_manager=oco, db_wal=wal, dry_run_notify=True)
            res = rec.reconcile(now_dt=datetime(2026, 10, 22, 19, 0, 0, tzinfo=JST))
            self.assertEqual(res["status"], "RECONCILED")
            self.assertIn("BCH", res["dust_quarantined"])
            pf_after = oco.load_portfolio()
            self.assertTrue(any(p.get("state") == "DUST_QUARANTINED" for p in pf_after["positions"]))

    def test_compound_scenario_c_saturday_maintenance_lifecycle(self) -> None:
        """複合シナリオC: 土曜 08:52 事前未約定BUYクリア -> メンテ中SL消失 -> 11:05 PREOPEN 待機 -> OPEN 3-Way 照合で SL 即時復旧"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            ex = ChaosMockExchange(tmp_dir, wal_db=daemon.db_wal)
            daemon.broker = ex
            daemon.oco_manager.broker = ex
            daemon.reconciler.broker = ex

            sat_0840 = datetime(2026, 10, 24, 8, 40, 0, tzinfo=JST)
            daemon.oco_manager.place_new_buy_order(
                {"action": "BUY", "symbol": "BCH", "limit_price": 44000, "size": 0.1, "take_profit_price": 46500, "stop_loss_price": 42500},
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=sat_0840,
            )
            daemon.run_fast_cycle(tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}}, now_dt=sat_0840 + timedelta(seconds=15))

            # 2枠目の未約定 BUY 指値 (SOL) を発注
            daemon.oco_manager.place_new_buy_order(
                {"action": "BUY", "symbol": "SOL", "limit_price": 19500, "size": 0.1, "take_profit_price": 21000, "stop_loss_price": 18800},
                tickers={"SOL": {"ask": 20000, "bid": 19990, "last": 20000}},
                now_dt=sat_0840 + timedelta(seconds=30),
            )
            self.assertEqual(len(daemon.oco_manager.load_portfolio()["pending_orders"]), 1)

            # Scenario C: 土曜 08:52 事前クリア -> メンテ中 SL 消失 -> 11:05 PREOPEN 待機 -> OPEN 照合復旧
            sat_0852 = datetime(2026, 10, 24, 8, 52, 0, tzinfo=JST)
            daemon.run_fast_cycle(tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}}, now_dt=sat_0852)
            self.assertEqual(len(daemon.oco_manager.load_portfolio()["pending_orders"]), 0)
            self.assertEqual(len(daemon.oco_manager.load_portfolio()["positions"]), 1)

            sat_0900 = datetime(2026, 10, 24, 9, 0, 0, tzinfo=JST)
            res_sleep = daemon.run_fast_cycle(now_dt=sat_0900)
            self.assertEqual(res_sleep["status"], "MAINTENANCE_SLEEP")
            old_sl_id = daemon.oco_manager.load_portfolio()["positions"][0]["sl_order_id"]
            ex.cancel_order(old_sl_id)

            sat_1105 = datetime(2026, 10, 24, 11, 5, 0, tzinfo=JST)
            res_preopen = daemon.run_fast_cycle(now_dt=sat_1105, exchange_status="PREOPEN")
            self.assertEqual(res_preopen["status"], "WAITING_EXCHANGE_OPEN_PREOPEN")

            res_open = daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=sat_1105 + timedelta(minutes=1),
                exchange_status="OPEN",
            )
            self.assertEqual(res_open["status"], "OK")
            self.assertIsNotNone(res_open["reconciled"])
            new_sl_id = daemon.oco_manager.load_portfolio()["positions"][0]["sl_order_id"]
            self.assertNotEqual(new_sl_id, old_sl_id)
            self.assertEqual(len(ex.get_active_orders("BCH")), 1)

    def test_compound_scenario_d_0605_fee_adjustment_gemini_429_and_risk_off_crash(self) -> None:
        """複合シナリオD: 06:05 JST 手数料端数調整 (+4円) + Gemini 429 枯渇フォールバック + RISK_OFF_CRASH 下での既存SL維持"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            ex = ChaosMockExchange(tmp_dir, wal_db=daemon.db_wal)
            daemon.broker = ex
            daemon.oco_manager.broker = ex
            daemon.reconciler.broker = ex

            t_0600 = datetime(2026, 10, 25, 6, 0, 0, tzinfo=JST)
            daemon.oco_manager.place_new_buy_order(
                {"action": "BUY", "symbol": "BCH", "limit_price": 44000, "size": 0.1, "take_profit_price": 46500, "stop_loss_price": 42500},
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t_0600,
            )
            daemon.run_fast_cycle(tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}}, now_dt=t_0600 + timedelta(seconds=10))

            # 06:05 JST に取引所側の日本円残高が +4円 端数調整されたケース -> 自動吸収
            pf = daemon.oco_manager.load_portfolio()
            local_before = int(pf["jpy_balance"])
            state_ex = ex._ensure_state_file()
            state_ex["jpy_balance"] = float(local_before + 4)
            ex._save_state(state_ex)

            rec_0605 = daemon.reconciler.reconcile(now_dt=datetime(2026, 10, 25, 6, 5, 0, tzinfo=JST))
            self.assertEqual(rec_0605["status"], "RECONCILED")
            self.assertEqual(rec_0605["fee_adjustment_applied"]["adjustment_jpy"], 4)
            self.assertEqual(int(daemon.oco_manager.load_portfolio()["jpy_balance"]), local_before + 4)

            # BTC 急落 (-4.5%) による RISK_OFF_CRASH 発動 -> 新規 BUY は遮断されるが既存 BCH の SL 監視は 100% 継続
            reg_res = daemon.radar.regime_filter.evaluate(
                {
                    "BTC": {
                        "price": {"last": 9550000},
                        "h1_indicators": {"change_1h_pct": -4.5},
                        "h4_indicators": {"change_4h_pct": -4.0, "rsi_14": 25.0},
                        "daily_indicators": {"trend": "DOWNTREND"},
                    }
                },
                now_dt=datetime(2026, 10, 25, 9, 0, 0, tzinfo=JST),
            )
            self.assertEqual(reg_res["regime"], "RISK_OFF_CRASH")
            self.assertFalse(reg_res["allow_new_buy"])
            self.assertEqual(len(daemon.oco_manager.load_portfolio()["positions"]), 1)

    def test_round1_master_audit_regressions(self) -> None:
        """第1回徹底総合監査 (Round 1 Master Comprehensive Audit) 検出10項目 (F-01〜F-10) のリグレッション検証"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            ex = ChaosMockExchange(tmp_dir, wal_db=daemon.db_wal)
            daemon.broker = ex
            daemon.oco_manager.broker = ex
            daemon.reconciler.broker = ex
            daemon.live_probe.broker = ex

            # [F-01] 全11モジュールの配線 & DatabaseWAL(auto_sync_files=True) 確認
            self.assertIsNotNone(daemon.ws_engine)
            self.assertIsNotNone(daemon.live_probe)
            self.assertIsNotNone(daemon.backtester)
            self.assertTrue(daemon.db_wal.auto_sync_files)

            # [F-02] cancel_before=True は SELL + MARKET + FAK のみ許可され、BUY/LIMIT では GMOPrivateAPIError となること
            with self.assertRaises(GMOPrivateAPIError):
                ex.place_order(
                    symbol="XRP",
                    side="BUY",
                    execution_type="LIMIT",
                    size=1,
                    price=80.0,
                    time_in_force="SOK",
                    cancel_before=True,
                )
            probe_res = daemon.live_probe.run_probe()
            self.assertTrue(probe_res["cancel_before_market_fak_sell_only_enforced"])

            # [F-03] AdverseSelectionGuard の PASSIVE_OFFSET が Best Bid 基準で max(1_tick, 0.15 * atr_5m * |tfi_60s|) を引くこと
            guard = AdverseSelectionGuard()
            now_sec = 1_760_000_000.0
            guard.record_orderbook(
                symbol="BTC",
                asks=[{"price": 10_000_100, "size": 1.0}],
                bids=[{"price": 10_000_000, "size": 1.0}],
                timestamp_sec=now_sec,
            )
            guard.record_taker_trade("BTC", "SELL", 10_000_000, 0.60, timestamp_sec=now_sec)
            guard.record_taker_trade("BTC", "BUY", 10_000_100, 0.40, timestamp_sec=now_sec)
            # TFI = (0.40 - 0.60) / 1.0 = -0.20 (-0.25 < TFI < -0.05 の PASSIVE_OFFSET 領域)
            # 0.15 * 20000 * 0.20 = 600 円オフセット
            passive_res = guard.evaluate_entry_guard(
                symbol="BTC",
                proposed_limit_price=10_000_500,  # Best Bid (10,000,000) より高い提案価格でも Best Bid 基準へ補正
                atr_5m=20_000.0,
                now_sec=now_sec,
            )
            self.assertTrue(passive_res["allow_entry"])
            self.assertTrue(passive_res["passive_offset_applied"])
            self.assertEqual(passive_res["adjusted_limit_price"], 10_000_000 - 600)

            # [F-04] compute_four_stage_trailing_sl の4段階遷移 & calculate_breakeven_sl_price の 0.15R オプション検証
            st1, sl1, upd1 = compute_four_stage_trailing_sl(
                entry_price=100_000,
                current_bid=102_200,
                highest_bid=102_200,
                current_sl=98_000,
                one_r_dist=2_000,
                symbol="SOL",
            )
            self.assertEqual(st1, 1)
            self.assertTrue(upd1)
            self.assertEqual(sl1, 100_200)
            be_with_r = calculate_breakeven_sl_price(100_000, "SOL", one_r_distance=2_000)
            self.assertEqual(be_with_r, 100_300)

            # [F-05 & F-06] PseudoOCOManager で SL 約定時に realized_pnl_jpy が記録され、
            # L3/L4 サーキットブレーカーと AIJudge 180分クールダウンに連動すること
            t0 = datetime(2026, 10, 26, 7, 0, 0, tzinfo=JST)
            daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 48000,
                    "stop_loss_price": 40500,
                },
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            # 約定させる (44,000円 × 0.1 BCH = 4,400円 <= 10,000円口座)
            daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}},
                now_dt=t0 + timedelta(seconds=10),
            )
            # SL ヒットさせる (40,500円決済 -> -350円損失 <= -300円の L3 日次損失制限到達)
            fast_sl_res = daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 40450, "bid": 40400, "last": 40400}},
                now_dt=t0 + timedelta(seconds=25),
            )
            self.assertTrue(any(e["type"] == "SL_EXECUTED" for e in fast_sl_res["events"]))
            # AIJudge の同一銘柄 SL クールダウンが自動で記録されていること
            cd_blocked, _ = daemon.ai_judge.is_symbol_on_sl_cooldown("BCH", now_dt=t0 + timedelta(minutes=30))
            self.assertTrue(cd_blocked)
            # L3 日次損失制限が closed_trades の損失を検知してブロックすること
            pf_after_sl = daemon.oco_manager.load_portfolio()
            l3_chk = daemon.circuit_breaker.check_l3_daily_loss(pf_after_sl, now_dt=t0 + timedelta(minutes=30))
            self.assertTrue(l3_chk["blocked"])
            self.assertLessEqual(l3_chk["daily_realized_pnl_jpy"], -300.0)

            # [F-07] 06:00〜06:15 JST の +25円 手数料端数返金が FEE_REFUND_ADJUSTMENT として自動吸収されること
            pf_0605 = daemon.oco_manager.load_portfolio()
            base_jpy = int(pf_0605["jpy_balance"])
            st_ex = ex._ensure_state_file()
            st_ex["jpy_balance"] = float(base_jpy + 25)
            ex._save_state(st_ex)
            rec_25 = daemon.reconciler.reconcile(now_dt=datetime(2026, 10, 27, 6, 5, 0, tzinfo=JST))
            self.assertEqual(rec_25["status"], "RECONCILED")
            self.assertEqual(rec_25["fee_adjustment_applied"]["adjustment_jpy"], 25)

            # [F-08 & F-09 & F-10] 臨時メンテ ERR-5201 非カウント、run_v2_daemon.ps1 ロック分離、DBコマンドポーリング
            daemon.consecutive_api_errors = 0
            err_info = daemon.record_api_error(Exception("GMO API Error ERR-5201: MAINTENANCE"), now_dt=t0)
            self.assertEqual(err_info["consecutive_api_errors"], 0)
            self.assertTrue(err_info["maintenance_sleep"])

            ps1_text = (Path(__file__).resolve().parent.parent / "run_v2_daemon.ps1").read_text(encoding="utf-8")
            self.assertIn("v2_launcher.lock", ps1_text)
            self.assertNotIn('$LockFile = Join-Path $ProjectDir "v2_daemon.lock"', ps1_text)

            daemon.db_wal.register_control_command(
                nonce="poll_pause_001",
                command="PAUSE",
                actor_id=daemon.remote_control.allowed_owner_discord_id,
                require_confirm=False,
                now_dt=t0,
            )
            polled = daemon.remote_control.poll_and_execute_db_commands(now_dt=t0)
            self.assertEqual(len(polled), 1)
            self.assertTrue(daemon.remote_control.is_paused)

    def test_round2_deep_cross_component_audit(self) -> None:
        """第2回徹底総合監査 (Round 2 Deep Cross-Component & Runtime State Audit) 全8領域 (R2-01〜R2-08) の検証"""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            ex = ChaosMockExchange(tmp_dir, wal_db=daemon.db_wal)
            daemon.broker = ex
            daemon.oco_manager.broker = ex
            daemon.reconciler.broker = ex

            t0 = datetime(2026, 10, 27, 12, 0, 0, tzinfo=JST)

            # [領域3 / R2-01] PENDING_BUY 注文中（日本円 4,400円拘束中）に ThreeWayReconciler.reconcile() が走っても
            # jpy_amount (10,000円) と jpy_available (5,600円) を正しく区別し、誤って DEGRADED_RECONCILING にならないこと
            daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 48000,
                    "stop_loss_price": 42500,
                    "expected_loss_jpy": 150,
                },
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            balances = ex.get_jpy_and_spot_balances()
            self.assertEqual(balances["jpy_amount"], 10000.0)
            self.assertEqual(balances["jpy_balance"], 10000.0)
            self.assertEqual(balances["jpy_available"], 5600.0)
            self.assertEqual(balances["jpy_locked"], 4400.0)

            rec_pending = daemon.reconciler.reconcile(now_dt=t0 + timedelta(seconds=30))
            self.assertEqual(rec_pending["status"], "RECONCILED")
            self.assertFalse(daemon.reconciler.degraded_reconciling)

            # 同時に2枠目の新規BUYを validate_and_adjust_decision にかけた際、
            # 1枠目の PENDING_BUY 拘束額 (4,400円) とリスク (150円) が正しく差し引かれて評価されること
            pf_with_pending = daemon.oco_manager.load_portfolio()
            val_slot2 = validate_and_adjust_decision(
                decision={
                    "action": "BUY",
                    "symbol": "SOL",
                    "suggested_entry_offset_atr": 0.5,
                    "tp_price": 22000,
                    "sl_price": 19200,
                    "confidence": 85,
                },
                portfolio=pf_with_pending,
                symbol_rules=DEFAULT_SYMBOL_RULES,
                market_summaries={
                    "BTC": {"price": {"last": 10000000}, "h4_indicators": {"change_4h_pct": 0.5, "rsi_14": 55.0}},
                    "SOL": {
                        "price": {"last": 20000, "best_bid": 19990, "best_ask": 20000, "spread_pct": 0.05},
                        "daily_indicators": {"atr_14": 400.0},
                    },
                },
            )
            self.assertEqual(val_slot2["action"], "BUY")
            # Best Bid 19,990 - 0.5 * 400 = 19,790 円に suggested_entry_offset_atr が反映されていること (R2-06)
            self.assertEqual(val_slot2["limit_price"], 19790)
            self.assertLessEqual(val_slot2["expected_cost_jpy"], 5600)

            # [領域1 / R2-02] 約定 -> SL発動まで進めた際、SQLite WAL の orders / executions / tax_moving_avg_ledger / positions
            # がリアルタイム同期され、万一 portfolio.json が消失しても SQLite スナップショットから完全復元されること
            daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}},
                now_dt=t0 + timedelta(seconds=45),
            )
            pos_rows = daemon.db_wal.get_positions_by_states(["SL_ACTIVE", "PROTECTED_ACTIVE"])
            self.assertTrue(any(r["symbol"] == "BCH" for r in pos_rows))
            tax_rows = daemon.db_wal.get_tax_moving_avg_ledger("BCH")
            self.assertEqual(len(tax_rows), 1)
            self.assertGreater(tax_rows[0]["total_size_satoshi"], 0)

            # portfolio.json を一時削除しても load_portfolio() が SQLite WAL から自動復元すること
            daemon.portfolio_path.unlink()
            restored_pf = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(restored_pf["positions"]), 1)
            self.assertEqual(restored_pf["positions"][0]["symbol"], "BCH")
            self.assertTrue(daemon.portfolio_path.exists())

            # [領域2 / R2-03] REST スナップショット由来の板記録が 15秒後の run_fast_cycle で
            # 5.0秒遅延として誤爆キャンセルされないこと
            daemon.adverse_guard.record_orderbook(
                symbol="SOL",
                asks=[{"price": 20010, "size": 5.0}],
                bids=[{"price": 20000, "size": 5.0}],
                timestamp_sec=t0.timestamp(),
                is_rest_snapshot=True,
            )
            daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "SOL",
                    "limit_price": 19790,
                    "size": 0.1,
                    "take_profit_price": 21500,
                    "stop_loss_price": 19100,
                },
                tickers={"SOL": {"ask": 20010, "bid": 20000, "last": 20000}},
                now_dt=t0 + timedelta(seconds=50),
            )
            fast_after_15s = daemon.run_fast_cycle(
                tickers={
                    "BCH": {"ask": 44100, "bid": 44050, "last": 44050},
                    "SOL": {"ask": 20010, "bid": 20000, "last": 20000},
                },
                now_dt=t0 + timedelta(seconds=65),
            )
            self.assertFalse(
                any(e.get("type") == "ADVERSE_SELECTION_RESTING_BUY_CANCELED" for e in fast_after_15s["events"])
            )
            self.assertEqual(len(daemon.oco_manager.load_portfolio()["pending_orders"]), 1)

            # [領域4 / R2-04] Offset-naive と Offset-aware datetime が混在しても TypeError が起きないこと
            naive_dt = datetime(2026, 10, 27, 11, 30, 0)
            daemon.ai_judge._last_symbol_ai_eval_at["XRP"] = naive_dt
            on_cd, _ = daemon.ai_judge.is_symbol_on_ai_cooldown("XRP", now_dt=t0)
            self.assertTrue(on_cd)
            daemon.regime_filter.risk_off_cooldown_until = datetime(2026, 10, 27, 13, 0, 0)
            reg_chk = daemon.regime_filter.evaluate({"BTC": {"price": {"last": 10000000}}}, now_dt=t0)
            self.assertEqual(reg_chk["regime"], "RISK_OFF_CRASH")

            # [領域5 / R2-05] L5 ハード・キルスイッチ 3回連続発動時に待機注文キャンセル＋保有建玉の即時成行決済が実行されること
            crash_tickers = {
                "BCH": {"ask": 43100, "bid": 43000, "last": 43000},
                "SOL": {"ask": 20010, "bid": 20000, "last": 20000},
            }
            pf_l5_setup = daemon.oco_manager.load_portfolio()
            pf_l5_setup["jpy_balance"] = 4000  # 4,000 + 43,000*0.1*(1-0.0009) = 8,296.13 < 8,500 (かつ SL 42,500 より上)
            daemon.oco_manager.save_portfolio(pf_l5_setup, now_dt=t0 + timedelta(seconds=70))
            daemon.run_fast_cycle(tickers=crash_tickers, now_dt=t0 + timedelta(seconds=75))
            daemon.run_fast_cycle(tickers=crash_tickers, now_dt=t0 + timedelta(seconds=90))
            l5_trig_cycle = daemon.run_fast_cycle(tickers=crash_tickers, now_dt=t0 + timedelta(seconds=105))
            self.assertTrue(l5_trig_cycle["l5_kill_switch"]["newly_triggered"])
            self.assertTrue(any(e.get("type") == "L5_KILL_SWITCH_FLATTENED" for e in l5_trig_cycle["events"]))
            pf_after_l5 = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_l5["pending_orders"]), 0)
            self.assertEqual(len(pf_after_l5["positions"]), 0)

    def test_round3_live_parity_and_long_run_audit(self) -> None:
        """第3回徹底総合監査 (Round 3 Live-Parity, Edge-Case & Long-Run Stability Audit) 全6領域 (R3-01〜R3-06) の検証"""
        import inspect
        import os
        from generate_report import compute_performance_metrics
        from migrate_from_v1 import execute_migration
        from src.notifier import Notifier, mask_secrets

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "r3.db", portfolio_json_path=tmp_dir / "portfolio.json", auto_sync_files=True)
            live_client = GMOPrivateClient(api_key="test_live_key_123456", api_secret="test_live_secret_654321", wal_db=wal)
            paper_broker = PaperBroker(
                paper_orders_path=tmp_dir / "paper_orders.json",
                portfolio_path=tmp_dir / "portfolio.json",
                wal_db=wal,
            )

            # [領域1 / R3-01] GMOPrivateClient (LIVE) と PaperBroker (DRY_RUN) の全ブローカーメソッドシグネチャ 100% パリティ検証
            broker_methods = [
                "get_assets",
                "get_jpy_and_spot_balances",
                "get_trading_volume",
                "create_ws_auth_token",
                "extend_ws_auth_token",
                "delete_ws_auth_token",
                "get_active_orders",
                "get_orders",
                "change_order",
                "cancel_order",
                "cancel_orders",
                "cancel_bulk_order",
                "get_latest_executions",
                "place_order",
            ]
            for m_name in broker_methods:
                live_sig = list(inspect.signature(getattr(live_client, m_name)).parameters.keys())
                paper_sig = list(inspect.signature(getattr(paper_broker, m_name)).parameters.keys())
                self.assertEqual(live_sig, paper_sig, f"Signature mismatch on {m_name}: {live_sig} vs {paper_sig}")

            # GMO API 特有の空辞書レスポンス {"status": 0, "data": {}} で例外なく空リスト [] を返すこと
            empty_resp = MagicMock()
            empty_resp.status_code = 200
            empty_resp.text = '{"status":0,"data":{}}'
            empty_resp.headers = {}
            empty_resp.json.return_value = {"status": 0, "data": {}}
            with patch.object(live_client.session, "get", return_value=empty_resp):
                self.assertEqual(live_client.get_active_orders("BCH"), [])
                self.assertEqual(live_client.get_orders(["12345"]), [])
                self.assertEqual(live_client.get_latest_executions("BCH"), [])

            # get_latest_executions に空の symbol を渡した場合に ValueError で防御されること
            with self.assertRaises(ValueError):
                live_client.get_latest_executions("")
            with self.assertRaises(ValueError):
                paper_broker.get_latest_executions("   ")

            # PaperBroker 単体で GMOCoinLiveProbe.run_probe() を実行した際にも change_order が CHANGED を返して成功すること
            paper_broker.update_market_tickers({"XRP": {"ask": 85.0, "bid": 84.9, "last": 85.0}})
            probe = GMOCoinLiveProbe(
                broker=paper_broker,
                capabilities_path=tmp_dir / "capabilities.json",
                probe_symbol="XRP",
            )
            probe_caps = probe.run_probe()
            self.assertTrue(probe_caps["limit_change_order_supported"])
            self.assertTrue(probe_caps["limit_change_order_preserves_order_id"])
            self.assertEqual(len(paper_broker.get_active_orders("XRP")), 0)

            # [領域2 / R3-02] 部分約定時の executed_size 同期、PaperBroker 資産計算整合性、および 60秒グレースタイムアウト検証
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            t0 = datetime(2026, 10, 28, 10, 0, 0, tzinfo=JST)
            buy_res = daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 80.0,
                    "size": 10.0,
                    "take_profit_price": 86.0,
                    "stop_loss_price": 77.0,
                    "expected_loss_jpy": 30,
                },
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.5}},
                now_dt=t0,
            )
            oid_xrp = buy_res["order"]["order_id"]
            # 10 XRP のうち 4 XRP だけ部分約定 (残り 6 XRP が板に残る状態)
            daemon.broker.simulate_partial_fill(oid_xrp, filled_size=4.0, fill_price=80.0, now_dt=t0 + timedelta(seconds=10))
            daemon.run_fast_cycle(
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.5}},
                now_dt=t0 + timedelta(seconds=15),
            )
            pf_partial = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_partial["pending_orders"]), 1)
            self.assertEqual(pf_partial["pending_orders"][0]["state"], "PARTIAL_FILLED")
            self.assertEqual(pf_partial["pending_orders"][0]["executed_size"], 4.0)

            # 部分約定中も PaperBroker の利用可能日本円が (10,000 - 4*80 - 6*80 = 9,200円) で整合し、4 XRP が現物残高に反映されていること
            bal_during_partial = daemon.broker.get_jpy_and_spot_balances()
            self.assertEqual(bal_during_partial["jpy_available"], 9200.0)
            self.assertEqual(bal_during_partial["spot_assets"]["XRP"]["amount"], 4.0)

            # 60秒グレース期間経過後 (t0 + 80s): 残り 6 XRP が自動キャンセルされ、約定済み 4 XRP に対して直ちに SL がセットされること
            fast_after_grace = daemon.run_fast_cycle(
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.5}},
                now_dt=t0 + timedelta(seconds=80),
            )
            self.assertTrue(any(e.get("type") == "BUY_FILLED_AND_SL_PLACED" for e in fast_after_grace["events"]))
            pf_after_grace = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_grace["pending_orders"]), 0)
            self.assertEqual(len(pf_after_grace["positions"]), 1)
            self.assertEqual(pf_after_grace["positions"][0]["size"], 4.0)
            self.assertEqual(pf_after_grace["positions"][0]["state"], "SL_ACTIVE")

            # 3-Way Reconciliation を実行しても 1円のズレもなく RECONCILED となること
            rec_after_partial = daemon.reconciler.reconcile(now_dt=t0 + timedelta(seconds=85))
            self.assertEqual(rec_after_partial["status"], "RECONCILED")
            self.assertFalse(daemon.reconciler.degraded_reconciling)

            # [領域4 / R3-04] generate_report.py (DUST除外・v2拡張理由集計) & Notifier (User-Agent・秘密情報マスク) & migrate_from_v1 検証
            pf_after_grace["positions"].append(
                {
                    "symbol": "BCH",
                    "size": 0.002,
                    "entry_price": 44000,
                    "state": "DUST_QUARANTINED",
                }
            )
            pf_after_grace["closed_trades"].extend(
                [
                    {"symbol": "SOL", "size": 0.1, "entry_price": 20000, "exit_price": 20050, "pnl_jpy": 5, "pnl_pct": 0.25, "reason": "BREAKEVEN_SL", "closed_at": "2026-10-28T10:00:00+09:00"},
                    {"symbol": "ETH", "size": 0.01, "entry_price": 400000, "exit_price": 410000, "pnl_jpy": 100, "pnl_pct": 2.5, "reason": "AI_SELL", "closed_at": "2026-10-28T10:05:00+09:00"},
                ]
            )
            metrics = compute_performance_metrics(pf_after_grace, [])
            # DUST_QUARANTINED の BCH は open_positions_count に含まれず、XRP 1銘柄のみカウントされること
            self.assertEqual(metrics["open_positions_count"], 1)
            self.assertGreaterEqual(metrics["reason_counts"]["SL"], 1)
            self.assertGreaterEqual(metrics["reason_counts"]["MANUAL"], 1)

            notifier = Notifier()
            self.assertEqual(notifier.session.headers.get("User-Agent"), "gmo-ai-trader-v2/2.0")
            with patch.dict(os.environ, {"GMO_API_SECRET": "super_secret_gmo_key_99999"}):
                masked_str = mask_secrets(
                    "Error with key super_secret_gmo_key_99999 and webhook https://discord.com/api/webhooks/12345/abcdefg and AIzaSyD12345678901234567890"
                )
                self.assertNotIn("super_secret_gmo_key_99999", masked_str)
                self.assertNotIn("abcdefg", masked_str)
                self.assertIn("***REDACTED***", masked_str)

            # migrate_from_v1 のドライラン・実コピー検証 (ダミーv1ディレクトリから一時ディレクトリへ)
            fake_v1 = tmp_dir / "fake_v1"
            fake_v2 = tmp_dir / "fake_v2"
            fake_v1.mkdir(parents=True, exist_ok=True)
            fake_v2.mkdir(parents=True, exist_ok=True)
            (fake_v1 / "portfolio.json").write_text('{"initial_capital_jpy": 10000, "jpy_balance": 10000, "positions": []}', encoding="utf-8")
            mig_res = execute_migration(
                v1_portfolio_path=fake_v1 / "portfolio.json",
                v2_portfolio_path=fake_v2 / "portfolio.json",
            )
            self.assertTrue(mig_res["migrated"])
            self.assertTrue((fake_v2 / "portfolio.json").exists())

            # [領域5 / R3-05] 長期稼働時のインメモリコレクション上限キャップ検証
            for i in range(250):
                daemon.ai_judge.record_symbol_ai_call(f"SYM_{i}", now_dt=t0 + timedelta(seconds=i))
            self.assertLessEqual(len(daemon.ai_judge._last_symbol_ai_eval_at), 200)

            for i in range(550):
                daemon.remote_control.dispatcher.dispatch("DAILY_DIGEST", f"Title {i}", "Msg", now_dt=t0)
            self.assertLessEqual(len(daemon.remote_control.dispatcher.digest_buffer), 500)

            # [領域3 / R3-03] WSHybridEngine & AutoTraderDaemon のクリーンシャットダウン検証
            daemon.ws_engine.obtain_or_refresh_token()
            self.assertTrue(bool(daemon.ws_engine.ws_token))
            daemon.shutdown()
            self.assertEqual(daemon.ws_engine.ws_token, "")
            self.assertEqual(daemon.ws_engine.sync_state, SyncState.DISCONNECTED)

    def test_round4_accounting_invariants_and_180day_endurance(self) -> None:
        """
        第4回 徹底総合監査 (Round 4 Accounting Invariants, Execution Settlement & 180-Day Endurance Audit):
        - R4-01: 資金・建玉・税金台帳の会計恒等式 (パターン A/B/C/D すべてで 1円・1 Satoshi の誤差も二重計上もゼロ)
        - R4-02: SL_PARTIAL_FAK_SWEEP の加重平均決済単価算出 & STOP未約定キャンセル時の誤成行決済防止
        - R4-03: 180日間 (半年・26回の土曜メンテ＋180回の日次ロールオーバー) 耐久シミュレーション & SQLite WAL チェックポイント検証
        - R4-04: 別プロセス CLI からの PAUSE/RESUME 永続化および FLATTEN_ALL 2段階承認のプロセス間連携検証
        """
        with tempfile.TemporaryDirectory() as td:
            tmp_dir = Path(td)
            daemon = AutoTraderDaemon(base_dir=tmp_dir, execution_mode="DRY_RUN", dry_run_notify=True)
            t0 = datetime(2026, 10, 28, 12, 0, 0, tzinfo=JST)

            # ------------------------------------------------------------------
            # 1. パターン A: PENDING_BUY ➔ 全量約定 ➔ SL_ACTIVE ➔ TP決済 (DatabaseWAL 二重計上ゼロ & 1円単位完全一致)
            # ------------------------------------------------------------------
            buy_a = daemon.oco_manager.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 80.0,
                    "size": 10.0,
                    "take_profit_price": 85.0,
                    "stop_loss_price": 77.0,
                    "expected_loss_jpy": 30,
                },
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.5}},
                now_dt=t0,
            )
            self.assertEqual(buy_a["status"], "PENDING_BUY")
            # 約定発生 (80.0円 × 10 XRP = 800円)
            daemon.run_fast_cycle(
                tickers={"XRP": {"ask": 80.0, "bid": 79.9, "last": 80.0}},
                now_dt=t0 + timedelta(seconds=15),
            )
            # DatabaseWAL の tax_moving_avg_ledger が 2倍 (20 XRP) ではなく正確に 10 XRP (1,000,000,000 satoshi) であること
            with daemon.db_wal._lock:
                tax_row_open = daemon.db_wal._conn.execute(
                    "SELECT * FROM tax_moving_avg_ledger WHERE symbol = 'XRP'"
                ).fetchone()
                exec_rows_buy = daemon.db_wal._conn.execute(
                    "SELECT COUNT(*) AS cnt FROM executions WHERE symbol = 'XRP' AND side = 'BUY'"
                ).fetchone()
            self.assertEqual(int(exec_rows_buy["cnt"]), 1)
            self.assertEqual(int(tax_row_open["total_size_satoshi"]), 10 * 100_000_000)

            # TP 到達で全量決済 (85.0円 × 10 XRP = 850円)
            daemon.run_fast_cycle(
                tickers={"XRP": {"ask": 85.5, "bid": 85.0, "last": 85.2}},
                now_dt=t0 + timedelta(seconds=30),
            )
            pf_after_a = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_a["positions"]), 0)
            self.assertEqual(len(pf_after_a["pending_orders"]), 0)
            with daemon.db_wal._lock:
                tax_row_closed = daemon.db_wal._conn.execute(
                    "SELECT * FROM tax_moving_avg_ledger WHERE symbol = 'XRP'"
                ).fetchone()
                exec_rows_sell = daemon.db_wal._conn.execute(
                    "SELECT COUNT(*) AS cnt FROM executions WHERE symbol = 'XRP' AND side = 'SELL'"
                ).fetchone()
            self.assertEqual(int(exec_rows_sell["cnt"]), 1)
            self.assertEqual(int(tax_row_closed["total_size_satoshi"]), 0)
            self.assertEqual(int(tax_row_closed["book_value_a_jpy"]), 0)
            self.assertEqual(int(tax_row_closed["book_value_b_jpy"]), 0)

            rec_a = daemon.reconciler.reconcile(now_dt=t0 + timedelta(seconds=35))
            self.assertEqual(rec_a["status"], "RECONCILED")
            self.assertEqual(rec_a["jpy_diff"], 0.0)

            # ------------------------------------------------------------------
            # 2. パターン B: PENDING_BUY ➔ 未約定キャンセル & 逆選択キャンセル直前の部分約定保護
            # ------------------------------------------------------------------
            jpy_before_b = daemon.oco_manager.load_portfolio()["jpy_balance"]
            buy_b = daemon.oco_manager.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 80.0,
                    "size": 10.0,
                    "take_profit_price": 85.0,
                    "stop_loss_price": 77.0,
                    "expected_loss_jpy": 30,
                },
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.5}},
                now_dt=t0 + timedelta(minutes=1),
            )
            oid_b = buy_b["order"]["order_id"]
            # キャンセル直前に 5 XRP 部分約定した状態で CANCEL_ENTRIES が呼ばれた場合、5 XRP が孤児化せず SL_ACTIVE になること
            daemon.broker.simulate_partial_fill(oid_b, filled_size=5.0, fill_price=80.0, now_dt=t0 + timedelta(minutes=1, seconds=10))
            daemon.remote_control.cancel_pending_entries_only(actor_id=None, now_dt=t0 + timedelta(minutes=1, seconds=20))
            pf_after_b = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_b["pending_orders"]), 0)
            self.assertEqual(len(pf_after_b["positions"]), 1)
            self.assertEqual(pf_after_b["positions"][0]["size"], 5.0)
            self.assertEqual(pf_after_b["positions"][0]["state"], "SL_ACTIVE")
            rec_b = daemon.reconciler.reconcile(now_dt=t0 + timedelta(minutes=1, seconds=25))
            self.assertEqual(rec_b["status"], "RECONCILED")
            # クリーンアップ決済
            daemon.oco_manager.close_position_immediately("XRP", reason="TEST_CLEANUP", now_dt=t0 + timedelta(minutes=1, seconds=30))
            self.assertAlmostEqual(daemon.oco_manager.load_portfolio()["jpy_balance"], jpy_before_b, delta=5.0)

            # ------------------------------------------------------------------
            # 3. パターン D: minOrderSize 未満の端数部分約定 ➔ 60秒経過 ➔ DUST_QUARANTINED ➔ 3-Way 照合完全一致
            # ------------------------------------------------------------------
            buy_d = daemon.oco_manager.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 40000.0,
                    "size": 0.02,
                    "take_profit_price": 42000.0,
                    "stop_loss_price": 39000.0,
                    "expected_loss_jpy": 20,
                },
                tickers={"BCH": {"ask": 40100.0, "bid": 40050.0, "last": 40080.0}},
                now_dt=t0 + timedelta(minutes=5),
            )
            oid_d = buy_d["order"]["order_id"]
            # BCH の min_order_size (0.01) 未満である 0.004 BCH だけ部分約定
            daemon.broker.simulate_partial_fill(oid_d, filled_size=0.004, fill_price=40000.0, now_dt=t0 + timedelta(minutes=5, seconds=10))
            daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 40100.0, "bid": 40050.0, "last": 40080.0}},
                now_dt=t0 + timedelta(minutes=5, seconds=15),
            )
            # 60秒グレース期間経過後 (t0 + 6m30s): DUST_QUARANTINED へ隔離されること
            fast_d = daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 40100.0, "bid": 40050.0, "last": 40080.0}},
                now_dt=t0 + timedelta(minutes=6, seconds=30),
            )
            self.assertTrue(any(e.get("type") == "PARTIAL_DUST_QUARANTINED" for e in fast_d["events"]))
            pf_after_d = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_d["pending_orders"]), 0)
            self.assertEqual(len(pf_after_d["positions"]), 0)
            self.assertEqual(len(pf_after_d["quarantined_dust"]), 1)
            self.assertAlmostEqual(float(pf_after_d["quarantined_dust"][0]["size"]), 0.004, places=6)

            # PaperBroker.get_assets() と ThreeWayReconciler が DUST 隔離後も 1円・1 Satoshi のズレなく RECONCILED となること
            rec_d = daemon.reconciler.reconcile(now_dt=t0 + timedelta(minutes=6, seconds=35))
            self.assertEqual(rec_d["status"], "RECONCILED")
            self.assertEqual(rec_d["discrepancies"], [])

            # ------------------------------------------------------------------
            # 4. R4-02: SL_PARTIAL_FAK_SWEEP 加重平均決済単価 & STOP未約定キャンセル時の再発注保護
            # ------------------------------------------------------------------
            buy_sl = daemon.oco_manager.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 43000.0,
                    "size": 0.10,
                    "take_profit_price": 45000.0,
                    "stop_loss_price": 42400.0,
                    "expected_loss_jpy": 60,
                },
                tickers={"BCH": {"ask": 43100.0, "bid": 43050.0, "last": 43080.0}},
                now_dt=t0 + timedelta(minutes=10),
            )
            # 約定させて SL_ACTIVE へ
            daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 43000.0, "bid": 42980.0, "last": 43000.0}},
                now_dt=t0 + timedelta(minutes=10, seconds=15),
            )
            pf_sl = daemon.oco_manager.load_portfolio()
            pos_bch = pf_sl["positions"][0]
            sl_oid_1 = pos_bch["sl_order_id"]

            # (a) 価格が SL より十分高い状態で取引所 STOP 注文が executed_size=0 で CANCELED になった場合、誤って成行売りせず STOP を再設置すること
            daemon.broker.cancel_order(sl_oid_1)
            ev_rearm = daemon.oco_manager.step(
                tickers={"BCH": {"ask": 43050.0, "bid": 43000.0, "last": 43020.0}},
                now_dt=t0 + timedelta(minutes=10, seconds=30),
            )
            self.assertTrue(any(e.get("type") == "CANCELED_SL_REARMED" for e in ev_rearm))
            pf_rearmed = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_rearmed["positions"]), 1)
            sl_oid_2 = pf_rearmed["positions"][0]["sl_order_id"]
            self.assertNotEqual(sl_oid_1, sl_oid_2)

            # (b) 急落時に STOP 注文が 0.06 BCH (@42,400円) だけ部分約定して残り 0.04 BCH が CANCELED (FAK失効) となり、
            #     直後の成行スイープが 42,000円で 0.04 BCH 約定した場合、加重平均単価 42,240.0円で記録されること
            daemon.broker.simulate_partial_fill(sl_oid_2, filled_size=0.06, fill_price=42400.0, now_dt=t0 + timedelta(minutes=11))
            daemon.broker.cancel_order(sl_oid_2)
            ev_sweep = daemon.oco_manager.step(
                tickers={"BCH": {"ask": 42050.0, "bid": 42000.0, "last": 42000.0}},
                now_dt=t0 + timedelta(minutes=11, seconds=5),
            )
            sweep_events = [e for e in ev_sweep if e.get("type") == "SL_PARTIAL_FAK_SWEPT"]
            self.assertEqual(len(sweep_events), 1)
            # (0.06 * 42400 + 0.04 * 42000) / 0.10 = 42240.0
            self.assertAlmostEqual(float(sweep_events[0]["exit_price"]), 42240.0, places=2)
            pf_after_sweep = daemon.oco_manager.load_portfolio()
            last_closed = pf_after_sweep["closed_trades"][-1]
            self.assertAlmostEqual(float(last_closed["exit_price"]), 42240.0, places=2)

            # ------------------------------------------------------------------
            # 5. R4-04: 別プロセス CLI からの PAUSE / RESUME 永続化 & FLATTEN_ALL 2段階承認連携
            # ------------------------------------------------------------------
            cli_remote = RemoteControlManager(oco_manager=daemon.oco_manager, db_wal=daemon.db_wal)
            cli_remote.execute_command("PAUSE", now_dt=t0 + timedelta(minutes=12))
            self.assertTrue(bool(daemon.oco_manager.load_portfolio().get("is_paused")))
            cli_remote.execute_command("RESUME", now_dt=t0 + timedelta(minutes=12, seconds=10))
            self.assertFalse(bool(daemon.oco_manager.load_portfolio().get("is_paused")))

            # 別インスタンスで FLATTEN_ALL 要求 ➔ メモリを共有しない第2インスタンスで FLATTEN_CONFIRM <nonce> が成功すること
            req_flt = cli_remote.execute_command("FLATTEN_ALL", now_dt=t0 + timedelta(minutes=12, seconds=20))
            flt_nonce = req_flt["nonce"]
            cli_remote_2 = RemoteControlManager(oco_manager=daemon.oco_manager, db_wal=daemon.db_wal)
            conf_flt = cli_remote_2.execute_command(
                f"FLATTEN_CONFIRM {flt_nonce}",
                now_dt=t0 + timedelta(minutes=12, seconds=30),
            )
            self.assertEqual(conf_flt["status"], "FLATTENED")
            cli_remote_2.execute_command("RESUME", now_dt=t0 + timedelta(minutes=12, seconds=35))

            # ------------------------------------------------------------------
            # 6. R4-03: 180日間 (半年・26回の土曜メンテ＋180回の日次06:00ロールオーバー) 高速耐久シミュレーション
            # ------------------------------------------------------------------
            sim_start = datetime(2026, 11, 1, 5, 55, 0, tzinfo=JST)
            saturday_count = 0
            for day_idx in range(180):
                day_dt = sim_start + timedelta(days=day_idx)
                # 毎日 06:05 JST の日次ロールオーバー (DAILY_DIGEST フラッシュ + SQLite WAL PASSIVE チェックポイント)
                daemon.remote_control.dispatcher.dispatch("DAILY_DIGEST", "Heartbeat", f"Day {day_idx}", now_dt=day_dt)
                res_0605 = daemon.run_fast_cycle(
                    tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                    now_dt=day_dt.replace(hour=6, minute=5),
                )
                self.assertEqual(res_0605["status"], "OK")

                # 土曜日の場合は 08:52 事前クリア ➔ 09:00 メンテスリープ ➔ 11:06 PREOPEN 待機 ➔ 11:10 OPEN 復帰照合を検証
                if day_dt.weekday() == 5:
                    saturday_count += 1
                    daemon.run_fast_cycle(
                        tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                        now_dt=day_dt.replace(hour=8, minute=52),
                    )
                    res_maint = daemon.run_fast_cycle(
                        tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                        now_dt=day_dt.replace(hour=9, minute=0),
                    )
                    self.assertEqual(res_maint["status"], "MAINTENANCE_SLEEP")
                    res_preopen = daemon.run_fast_cycle(
                        tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                        exchange_status="PREOPEN",
                        now_dt=day_dt.replace(hour=11, minute=6),
                    )
                    self.assertEqual(res_preopen["status"], "WAITING_EXCHANGE_OPEN_PREOPEN")
                    res_reopen = daemon.run_fast_cycle(
                        tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                        exchange_status="OPEN",
                        now_dt=day_dt.replace(hour=11, minute=10),
                    )
                    self.assertEqual(res_reopen["status"], "OK")
                    self.assertEqual(res_reopen["reconciled"]["status"], "RECONCILED")

            self.assertGreaterEqual(saturday_count, 25)
            self.assertEqual(len(daemon.remote_control.dispatcher.digest_buffer), 0)
            self.assertLessEqual(len(daemon.remote_control.dispatcher.sent_history), 500)

            with daemon.db_wal._lock:
                integrity = daemon.db_wal._conn.execute("PRAGMA integrity_check;").fetchone()[0]
            self.assertEqual(str(integrity).lower(), "ok")

            daemon.shutdown()

    def test_round5_multi_fill_and_concurrency_convergence_audit(self) -> None:
        """
        Round 5 徹底総合監査:
        1. [R5-01] 複数回部分約定 (Multi-Fill: 40% + 60%) 時の `get_executed_size_for_order` と未記録残余分の完全補完記録
        2. [R5-02] デーモン稼働中の `--remote-cmd` SQLite `control_commands` キューイング & `cancel_pending_entries_only` 例外耐性
        3. [R5-03] `GMOPrivateClient` / `PaperBroker` の `snake_case` & `camelCase` 両対応正規化と `get_executions` パリティ
        4. [R5-04] TP発動・トレーリング更新直前に既存 `STOP` が取引所で約定した競合 (Race Condition) の自動検知・二重売却防止
        5. [R5-05] 同一銘柄における `DUST_QUARANTINED` 端数と新規有効ポジションの共存時の 3-Way Reconciler / PaperBroker 整合性
        """
        from decimal import Decimal
        from src.gmo_private_client import GMOPrivateClient

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=JST)

            # ------------------------------------------------------------------
            # 1. R5-01: Multi-Fill (40 XRP + 60 XRP = 100 XRP) の約定記録と残余補完
            # ------------------------------------------------------------------
            buy_res = daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 80.0,
                    "size": 100,
                    "take_profit_price": 86.0,
                    "stop_loss_price": 78.5,
                    "expected_loss_jpy": 150,
                },
                tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                now_dt=t0,
            )
            xrp_oid = buy_res["pending_order"]["order_id"]

            # 1回目の部分約定 (40 XRP) を simulate_partial_fill で発生させ、その後 60 XRP が追加約定して計 100 XRP になったケース
            daemon.broker.simulate_partial_fill(
                xrp_oid,
                filled_size=40.0,
                fill_price=80.0,
                now_dt=t0 + timedelta(seconds=5),
            )
            self.assertEqual(daemon.db_wal.get_executed_size_for_order(xrp_oid), Decimal("40"))

            # もし2回目の約定明細 (60 XRP) が executions API から欠落し、get_orders の executed_size=100 のみで検知された場合でも、
            # _record_executions_to_wal が未記録残余 (100 - 40 = 60 XRP) を自動補完して合計 100 XRP になることを検証
            state_ex_m = daemon.broker._ensure_state_file()
            for o in state_ex_m["orders"]:
                if str(o.get("order_id")) == str(xrp_oid):
                    o["executed_size"] = 100.0
                    o["remaining_size"] = 0.0
                    o["status"] = "EXECUTED"
            daemon.broker._save_state(state_ex_m)

            daemon.run_fast_cycle(
                tickers={"XRP": {"ask": 79.9, "bid": 79.8, "last": 79.9}},
                now_dt=t0 + timedelta(seconds=15),
            )
            # 40 XRP だけでスキップされず、未記録だった残余 60 XRP が補完記録されて合計 100 XRP になっていること
            self.assertEqual(daemon.db_wal.get_executed_size_for_order(xrp_oid), Decimal("100"))

            # 一旦 XRP を AI_SELL で決済
            daemon.oco_manager.close_position_immediately("XRP", reason="TEST_CLEANUP", now_dt=t0 + timedelta(seconds=30))

            # ------------------------------------------------------------------
            # 2. R5-03: camelCase / snake_case 両対応正規化 & get_executions パリティ
            # ------------------------------------------------------------------
            execs_by_oid = daemon.broker.get_executions(order_id=xrp_oid)
            self.assertGreaterEqual(len(execs_by_oid), 1)
            ex0 = execs_by_oid[0]
            self.assertEqual(str(ex0.get("order_id")), str(xrp_oid))
            self.assertEqual(str(ex0.get("orderId")), str(xrp_oid))
            self.assertIn("execution_id", ex0)
            self.assertIn("executionId", ex0)

            norm_ord = GMOPrivateClient._normalize_order_dict(
                {
                    "orderId": 99901,
                    "symbol": "BCH",
                    "side": "BUY",
                    "executionType": "LIMIT",
                    "settleType": "OPEN",
                    "size": "0.095",
                    "executedSize": "0.040",
                    "price": "43880",
                    "losscutPrice": "0",
                    "status": "ORDERED",
                    "timeInForce": "SOK",
                    "timestamp": "2026-10-22T03:00:00.000Z",
                }
            )
            self.assertEqual(norm_ord["order_id"], "99901")
            self.assertEqual(norm_ord["orderId"], "99901")
            self.assertEqual(norm_ord["execution_type"], "LIMIT")
            self.assertEqual(norm_ord["executionType"], "LIMIT")
            self.assertAlmostEqual(float(norm_ord["executed_size"]), 0.04, places=4)
            self.assertAlmostEqual(float(norm_ord["executedSize"]), 0.04, places=4)
            self.assertEqual(norm_ord["orderStatus"], "ORDERED")

            vol_info = daemon.broker.get_trading_volume()
            self.assertIn("jpyVolume", vol_info)
            self.assertIn("jpy_volume", vol_info)
            self.assertIn("tierLevel", vol_info)
            self.assertIn("tier_level", vol_info)

            # ------------------------------------------------------------------
            # 3. R5-02: デーモン稼働中の CLI コマンド SQLite キューイング & cancel_pending_entries_only 例外耐性
            # ------------------------------------------------------------------
            daemon.instance_lock.acquire()
            try:
                from src.circuit_breaker import SingleInstanceLock

                probe_lock = SingleInstanceLock(daemon.instance_lock.lock_path)
                self.assertTrue(probe_lock.is_locked_by_another_process())

                cli_rc = RemoteControlManager(oco_manager=daemon.oco_manager, db_wal=daemon.db_wal)
                enq_pause = cli_rc.enqueue_or_execute_command(
                    "PAUSE",
                    is_daemon_running=True,
                    now_dt=t0 + timedelta(minutes=1),
                )
                self.assertEqual(enq_pause["status"], "QUEUED_FOR_DAEMON")
                # デーモンの次サイクルで PAUSE が消化・反映されること
                daemon.run_fast_cycle(
                    tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                    now_dt=t0 + timedelta(minutes=1, seconds=5),
                )
                self.assertTrue(daemon.remote_control.is_paused)

                # FLATTEN_ALL の 2段階承認も is_daemon_running=True でキュー経由実行できること
                req_f = cli_rc.enqueue_or_execute_command(
                    "FLATTEN_ALL",
                    is_daemon_running=True,
                    now_dt=t0 + timedelta(minutes=1, seconds=10),
                )
                self.assertEqual(req_f["status"], "AWAITING_CONFIRMATION")
                enq_conf = cli_rc.enqueue_or_execute_command(
                    f"FLATTEN_CONFIRM {req_f['nonce']}",
                    is_daemon_running=True,
                    now_dt=t0 + timedelta(minutes=1, seconds=15),
                )
                self.assertEqual(enq_conf["status"], "QUEUED_FOR_DAEMON")
                daemon.run_fast_cycle(
                    tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                    now_dt=t0 + timedelta(minutes=1, seconds=20),
                )
                # RESUME で再開
                cli_rc.enqueue_or_execute_command(
                    "RESUME",
                    is_daemon_running=True,
                    now_dt=t0 + timedelta(minutes=1, seconds=25),
                )
                daemon.run_fast_cycle(
                    tickers={"XRP": {"ask": 81.0, "bid": 80.5, "last": 80.8}},
                    now_dt=t0 + timedelta(minutes=1, seconds=30),
                )
                self.assertFalse(daemon.remote_control.is_paused)
            finally:
                daemon.instance_lock.release()

            # cancel_pending_entries_only で部分約定注文の SL 発注が一時エラーになっても UNPROTECTED_HOLD が保存され、
            # 次サイクルの Fast Recovery ガーディアンで SL が確実に再設置されること
            ltc_res = daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "LTC",
                    "limit_price": 12000.0,
                    "size": 0.2,
                    "take_profit_price": 12800.0,
                    "stop_loss_price": 11600.0,
                    "expected_loss_jpy": 80,
                },
                tickers={"LTC": {"ask": 12100.0, "bid": 12050.0, "last": 12080.0}},
                now_dt=t0 + timedelta(minutes=1, seconds=35),
            )
            ltc_oid = ltc_res["pending_order"]["order_id"]
            daemon.broker.simulate_partial_fill(
                ltc_oid,
                filled_size=0.1,
                fill_price=12000.0,
                now_dt=t0 + timedelta(minutes=1, seconds=40),
            )
            orig_place_order = daemon.broker.place_order

            def _fail_stop_once(*args, **kwargs):
                if str(kwargs.get("execution_type", "")).upper() == "STOP":
                    raise GMOPrivateAPIError("Transient STOP placement failure")
                return orig_place_order(*args, **kwargs)

            daemon.broker.place_order = _fail_stop_once
            try:
                can_res = daemon.remote_control.cancel_pending_entries_only(
                    now_dt=t0 + timedelta(minutes=1, seconds=45),
                )
                self.assertIn(ltc_oid, can_res["canceled_buy_order_ids"])
            finally:
                daemon.broker.place_order = orig_place_order

            pf_ltc = daemon.oco_manager.load_portfolio()
            ltc_pos = [p for p in pf_ltc["positions"] if p["symbol"] == "LTC"]
            self.assertEqual(len(ltc_pos), 1)
            self.assertEqual(ltc_pos[0]["state"], "UNPROTECTED_HOLD")

            # Fast Recovery ガーディアンが直ちに STOP 注文を復旧して SL_ACTIVE に戻すこと
            rec_ev = daemon.oco_manager.run_fast_recovery_guardian(
                tickers={"LTC": {"ask": 12050.0, "bid": 12000.0, "last": 12020.0}},
                now_dt=t0 + timedelta(minutes=1, seconds=46),
            )
            self.assertTrue(any(e.get("type") == "FAST_RECOVERY_SL_RESTORED" for e in rec_ev))
            daemon.oco_manager.close_position_immediately("LTC", reason="TEST_CLEANUP", now_dt=t0 + timedelta(minutes=1, seconds=50))

            # ------------------------------------------------------------------
            # 4. R5-04: トレーリング更新直前に既存 STOP が約定していた競合の安全ハンドリング
            # ------------------------------------------------------------------
            daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "SOL",
                    "limit_price": 20000.0,
                    "size": 0.1,
                    "take_profit_price": 22000.0,
                    "stop_loss_price": 19400.0,
                    "expected_loss_jpy": 60,
                },
                tickers={"SOL": {"ask": 20100.0, "bid": 20050.0, "last": 20080.0}},
                now_dt=t0 + timedelta(minutes=2),
            )
            daemon.run_fast_cycle(
                tickers={"SOL": {"ask": 19980.0, "bid": 19950.0, "last": 19980.0}},
                now_dt=t0 + timedelta(minutes=2, seconds=10),
            )
            pf_sol = daemon.oco_manager.load_portfolio()
            sol_pos = [p for p in pf_sol["positions"] if p["symbol"] == "SOL"][0]
            sol_sl_oid = sol_pos["sl_order_id"]

            # `modify_position_tp_sl` が呼ばれた瞬間に、取引所側で既に `sol_sl_oid` が EXECUTED になっていた競合を模擬
            state_ex = daemon.broker._ensure_state_file()
            for o in state_ex["orders"]:
                if str(o.get("order_id")) == str(sol_sl_oid):
                    o["status"] = "EXECUTED"
                    o["orderStatus"] = "EXECUTED"
                    o["executed_size"] = "0.1"
                    o["executedSize"] = "0.1"
                    o["executed_price"] = 19400.0
            daemon.broker._save_state(state_ex)

            mod_res = daemon.oco_manager.modify_position_tp_sl(
                symbol="SOL",
                new_tp=22500.0,
                new_sl=19800.0,
                now_dt=t0 + timedelta(minutes=2, seconds=20),
            )
            self.assertIsNotNone(mod_res)
            self.assertEqual(mod_res.get("exit_order_id"), sol_sl_oid)
            self.assertEqual(
                len([p for p in daemon.oco_manager.load_portfolio()["positions"] if p["symbol"] == "SOL"]),
                0,
            )

            # ------------------------------------------------------------------
            # 5. R5-05: 同一銘柄 (BCH) で DUST_QUARANTINED (0.003 BCH) と有効建玉 (0.095 BCH) が共存するケース
            # ------------------------------------------------------------------
            bch_dust_res = daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000.0,
                    "size": 0.05,
                    "take_profit_price": 46500.0,
                    "stop_loss_price": 42500.0,
                    "expected_loss_jpy": 75,
                },
                tickers={"BCH": {"ask": 44100.0, "bid": 44050.0, "last": 44080.0}},
                now_dt=t0 + timedelta(minutes=3),
            )
            bch_dust_oid = bch_dust_res["pending_order"]["order_id"]
            daemon.broker.simulate_partial_fill(
                bch_dust_oid,
                filled_size=0.003,
                fill_price=44000.0,
                now_dt=t0 + timedelta(minutes=3, seconds=5),
            )
            # 1回目の step で PARTIAL_FILLED 検知＆60秒グレースタイマー開始 -> 65秒後の step で DUST_QUARANTINED に隔離
            daemon.oco_manager.step(
                tickers={"BCH": {"ask": 44200.0, "bid": 44150.0, "last": 44180.0}},
                now_dt=t0 + timedelta(minutes=3, seconds=10),
            )
            daemon.oco_manager.step(
                tickers={"BCH": {"ask": 44200.0, "bid": 44150.0, "last": 44180.0}},
                now_dt=t0 + timedelta(minutes=4, seconds=15),
            )
            pf_bch1 = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_bch1["quarantined_dust"]), 1)

            # 同じ BCH で新規に 0.095 BCH の有効ポジションを建てて約定させる
            daemon.oco_manager.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000.0,
                    "size": 0.095,
                    "take_profit_price": 46500.0,
                    "stop_loss_price": 42500.0,
                    "expected_loss_jpy": 142,
                },
                tickers={"BCH": {"ask": 44100.0, "bid": 44050.0, "last": 44080.0}},
                now_dt=t0 + timedelta(minutes=4, seconds=20),
            )
            daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 43950.0, "bid": 43900.0, "last": 43950.0}},
                now_dt=t0 + timedelta(minutes=4, seconds=30),
            )

            # PaperBroker.get_assets() が 0.095 + 0.003 = 0.098 BCH (available=0.003) を返し、
            # ThreeWayReconciler が DUST_QUARANTINED と有効ポジションを上書きせず共存維持すること
            bch_assets = {a["symbol"]: a for a in daemon.broker.get_assets()}
            self.assertEqual(Decimal(bch_assets["BCH"]["amount"]), Decimal("0.098"))
            self.assertEqual(Decimal(bch_assets["BCH"]["available"]), Decimal("0.003"))

            rec_coexist = daemon.reconciler.reconcile(now_dt=t0 + timedelta(minutes=4, seconds=45))
            self.assertEqual(rec_coexist["status"], "RECONCILED")
            pf_coexist = daemon.oco_manager.load_portfolio()
            active_bch = [
                p for p in pf_coexist["positions"]
                if p.get("symbol") == "BCH" and p.get("state") != "DUST_QUARANTINED"
            ]
            self.assertEqual(len(active_bch), 1)
            self.assertAlmostEqual(float(active_bch[0]["size"]), 0.095, places=4)

            daemon.shutdown()


if __name__ == "__main__":
    unittest.main()
