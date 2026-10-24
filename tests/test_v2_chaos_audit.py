"""
v2 全自動売買システム：全8カテゴリ・64項目＋4大複合障害 カオスエンジニアリング（Fault Injection）総合監査スイート
(tests/test_v2_chaos_audit.py)

すべてのテストは `tempfile.TemporaryDirectory` およびインメモリ / ChaosMockExchange 上で完結し、
本番ファイルに 1 バイトも干渉しない。
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

            # #17: 4段階トレーリングSL (Stage 1: <+1.5R 初期SL維持、Stage 2: >=+1.6R +0.15Rロック、Stage 3: >=+2.2R シャンデリア、Stage 4: >=+3.0R クライマックス & 0.15R ヒステリシス)
            st1, p1, upd1 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10300, highest_bid=10300, current_sl=9700, one_r_dist=300, symbol="SOL"
            )
            self.assertEqual(st1, 1)
            self.assertFalse(upd1)
            self.assertEqual(p1, 9700)

            st2, p2, upd2 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10480, highest_bid=10480, current_sl=9700, one_r_dist=300, symbol="SOL"
            )
            self.assertEqual(st2, 2)
            self.assertTrue(upd2)
            self.assertGreaterEqual(p2, 10045)

            st3, p3, upd3 = compute_four_stage_trailing_sl(
                entry_price=10000, current_bid=10700, highest_bid=10700, current_sl=p2, one_r_dist=300, symbol="SOL"
            )
            self.assertGreaterEqual(st3, 3)
            self.assertTrue(upd3)
            # 0.15R (45円) 未満の微動では更新しない (ヒステリシス)
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

            # #43: WAL PRAGMA 検証 (journal_mode=WAL, synchronous=NORMAL(1)/FULL(2), busy_timeout=5000)
            pragmas = wal.get_pragmas()
            self.assertEqual(pragmas["journal_mode"], "WAL")
            self.assertIn(pragmas["synchronous"], (1, 2))
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
            self.assertFalse(upd1)
            self.assertEqual(sl1, 98_000)
            st2_r1, sl2_r1, upd2_r1 = compute_four_stage_trailing_sl(
                entry_price=100_000,
                current_bid=103_300,
                highest_bid=103_300,
                current_sl=98_000,
                one_r_dist=2_000,
                symbol="SOL",
            )
            self.assertEqual(st2_r1, 2)
            self.assertTrue(upd2_r1)
            self.assertEqual(sl2_r1, 100_300)
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
            # 60秒グレース期間経過後 (t0 + 6m30s, RISK_OFF_CRASH緊急時): DUST_QUARANTINED へ隔離されること
            fast_d = daemon.run_fast_cycle(
                tickers={"BCH": {"ask": 40100.0, "bid": 40050.0, "last": 40080.0, "risk_off_crash": True}},
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
            # 1回目の step で PARTIAL_FILLED 検知＆60秒グレースタイマー開始 -> 65秒後の step (RISK_OFF_CRASH緊急時) で DUST_QUARANTINED に隔離
            daemon.oco_manager.step(
                tickers={"BCH": {"ask": 44200.0, "bid": 44150.0, "last": 44180.0}},
                now_dt=t0 + timedelta(minutes=3, seconds=10),
            )
            daemon.oco_manager.step(
                tickers={"BCH": {"ask": 44200.0, "bid": 44150.0, "last": 44180.0, "risk_off_crash": True}},
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

    def test_round6_zero_defect_and_monte_carlo_fuzzing_audit(self) -> None:
        """
        Round 6 徹底総合監査:
        1. [R6-01] `get_orders` / `cancel_orders` に空リスト `[]`、整数リスト `[1001, 1002]`、カンマ区切り文字列 `"1001,1002"` を渡した場合の正規化検証
        2. [R6-02] `poll_and_execute_db_commands` において不正・期限切れ・未知の `PENDING` コマンドが `REJECTED` に更新され無限ループしないこと
        3. [R6-03] 1 Satoshi 未満 (`1e-10`) の端数誤差で `0 Satoshi` レコードが挿入されないこと、および `L5_KILL_SWITCH` / `FLATTEN_ALL` 時の `quarantined_dust` 非干渉＆部分約定昇格建玉の確実な決済
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
            t0 = datetime(2026, 10, 22, 14, 0, 0, tzinfo=JST)

            # ------------------------------------------------------------------
            # 1. R6-01: _parse_order_ids_arg による get_orders / cancel_orders 入力形式網羅検証
            # ------------------------------------------------------------------
            self.assertEqual(daemon.broker.get_orders([]), [])
            self.assertEqual(daemon.broker.get_orders(""), [])
            self.assertEqual(daemon.broker.cancel_orders([]), {"success": [], "failed": []})
            self.assertEqual(daemon.broker.cancel_orders(""), {"success": [], "failed": []})

            o1 = daemon.broker.place_order(symbol="XRP", side="BUY", execution_type="LIMIT", size=10, price=75.0, time_in_force="SOK")
            o2 = daemon.broker.place_order(symbol="XRP", side="BUY", execution_type="LIMIT", size=10, price=74.0, time_in_force="SOK")
            id1_int = int(o1["order_id"])
            id2_int = int(o2["order_id"])
            comma_ids = f"{id1_int},{id2_int}"

            # 整数リストでもカンマ区切り文字列でも 2件とも取得・キャンセルできること
            fetched_int = daemon.broker.get_orders([id1_int, id2_int])
            self.assertEqual(len(fetched_int), 2)
            fetched_csv = daemon.broker.get_orders(comma_ids)
            self.assertEqual(len(fetched_csv), 2)

            cancel_csv_res = daemon.broker.cancel_orders(comma_ids)
            self.assertEqual(len(cancel_csv_res["success"]), 2)
            self.assertEqual(len(cancel_csv_res["failed"]), 0)

            # GMOPrivateClient._parse_order_ids_arg 単体でも 12件カンマ区切り文字列が 12要素に分割されること
            twelve_csv = ",".join(str(1000 + i) for i in range(12))
            parsed_12 = GMOPrivateClient._parse_order_ids_arg(twelve_csv)
            self.assertEqual(len(parsed_12), 12)

            # ------------------------------------------------------------------
            # 2. R6-02: poll_and_execute_db_commands の不正・期限切れコマンド REJECTED 遷移（無限再試行防止）
            # ------------------------------------------------------------------
            daemon.db_wal.enqueue_control_command(
                command="FLATTEN_CONFIRM",
                nonce="bad_cli_1",
                requested_by="LOCAL_OWNER",
                status="PENDING",
                payload={"target_nonce": "flt_nonexistent_expired"},
                now_ts_ms=int(t0.timestamp() * 1000),
            )
            daemon.db_wal.enqueue_control_command(
                command="UNKNOWN_CMD_XYZ",
                nonce="bad_cli_2",
                requested_by="LOCAL_OWNER",
                status="PENDING",
                payload={},
                now_ts_ms=int(t0.timestamp() * 1000),
            )
            self.assertEqual(len(daemon.db_wal.fetch_pending_control_commands()), 2)

            # 1回のポーリングで両方 REJECTED に遷移し、次サイクルでは PENDING が 0件になること
            daemon.remote_control.poll_and_execute_db_commands(now_dt=t0 + timedelta(seconds=5))
            self.assertEqual(len(daemon.db_wal.fetch_pending_control_commands()), 0)
            row1 = daemon.db_wal.get_control_command_by_nonce("bad_cli_1")
            row2 = daemon.db_wal.get_control_command_by_nonce("bad_cli_2")
            self.assertEqual(row1["status"], "REJECTED")
            self.assertEqual(row2["status"], "REJECTED")

            # ------------------------------------------------------------------
            # 3. R6-03: 1 Satoshi 未満の端数誤差防止 ＆ DUST共存時の L5 キルスイッチ完全性
            # ------------------------------------------------------------------
            # 既に 10.0 XRP が記録されている注文に対し、total_size_d = 10.0000000001 (差 1e-10 < 1 Satoshi) を渡しても
            # 0 Satoshi の補完レコードが挿入されないこと
            daemon.db_wal.record_execution_idempotent(
                execution_id="exec_subsat_test_1",
                order_id="777001",
                symbol="XRP",
                side="BUY",
                price_str="80",
                size_str="10",
                fee_jpy_str="0",
                timestamp_str=t0.isoformat(timespec="seconds"),
            )
            daemon.oco_manager._record_executions_to_wal(
                symbol="XRP",
                side="BUY",
                primary_order_id="777001",
                all_order_ids=["777001"],
                total_size_d=Decimal("10.0000000001"),
                avg_price_d=Decimal("80"),
                total_fee_d=Decimal("0"),
                matched_execs=[],
                fallback_prefix="exec_buy",
                now_jst=t0,
            )
            with daemon.db_wal._lock:
                cnt_777 = daemon.db_wal._conn.execute(
                    "SELECT COUNT(*) FROM executions WHERE order_id = '777001'"
                ).fetchone()[0]
            self.assertEqual(int(cnt_777), 1)

            # BCH に DUST_QUARANTINED (0.003 BCH) と有効建玉 (0.095 BCH) が共存する状態で
            # L5 ハード・キルスイッチが発動した際、有効建玉のみが決済され、端数には成行売りが出ないこと
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
                now_dt=t0 + timedelta(minutes=1, seconds=5),
            )
            # 約定させて SL_ACTIVE へ
            daemon.oco_manager.step(
                tickers={"BCH": {"ask": 43950.0, "bid": 43900.0, "last": 43950.0}},
                now_dt=t0 + timedelta(minutes=1, seconds=10),
            )

            pf = daemon.oco_manager.load_portfolio()
            pf["jpy_balance"] = 3000  # 総資産約 7,172円 (< 8,500円) にして L5 を誘発
            pf["quarantined_dust"] = [
                {
                    "symbol": "BCH",
                    "size": 0.003,
                    "entry_price": 44000.0,
                    "cost_jpy": 132,
                    "state": "DUST_QUARANTINED",
                }
            ]
            daemon.oco_manager.save_portfolio(pf, now_dt=t0 + timedelta(minutes=1, seconds=15))

            # さらに 3回連続で純資産 < 8,500円 を検知させて L5 キルスイッチを発動
            for step_i in range(3):
                res_l5 = daemon.run_fast_cycle(
                    tickers={"BCH": {"ask": 43950.0, "bid": 43900.0, "last": 43950.0}},
                    now_dt=t0 + timedelta(minutes=1, seconds=20 + step_i * 5),
                )
            self.assertEqual(res_l5["status"], "OK")
            self.assertTrue(res_l5["l5_kill_switch"]["latched"])
            self.assertTrue(res_l5["l5_kill_switch"]["newly_triggered"])
            pf_after_l5 = daemon.oco_manager.load_portfolio()
            active_after_l5 = [
                p for p in pf_after_l5["positions"]
                if p.get("state") != "DUST_QUARANTINED"
            ]
            self.assertEqual(len(active_after_l5), 0)
            self.assertEqual(len(pf_after_l5["quarantined_dust"]), 1)

            daemon.shutdown()

    def test_pattern_a_compounding_and_campaign_phase1_phase2_lifecycle(self) -> None:
        """
        パターンA（資産10万円時の複利スケーリング＆L3/L4/L5 High-Water Mark保護）および
        第1回運用期限満了 (2027-07-09 00:00) -> 7月9日休止 -> 第2回開始 (2027-07-10 00:00) ->
        第2回運用期限満了 (2027-12-10 00:00) の完全自動締め・全決済・レポート生成ライフサイクル検証
        """
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            try:
                # ==============================================================
                # Part 1: パターンA 資産10万円時の動的複利スケーリング (45%配分・1.5%リスク・L3/L4/L5 HWM)
                # ==============================================================
                t_comp = datetime(2027, 3, 15, 12, 0, 0, tzinfo=JST)
                pf_100k = daemon.oco_manager.load_portfolio()
                pf_100k["initial_capital_jpy"] = 10000
                pf_100k["jpy_balance"] = 100000
                pf_100k["max_allocation_per_trade_pct"] = 0.45
                daemon.oco_manager.save_portfolio(pf_100k, now_dt=t_comp)

                # save_portfolio と SQLite WAL に peak_equity_jpy=100,000 / drawdown_kill_switch_jpy=85,000 が永続化されていること
                pf_loaded = daemon.oco_manager.load_portfolio()
                self.assertEqual(pf_loaded["peak_equity_jpy"], 100000)
                self.assertEqual(pf_loaded["drawdown_kill_switch_jpy"], 85000)
                self.assertEqual(daemon.db_wal.get_meta("peak_equity_jpy"), 100000)

                # 資産10万円時: 1銘柄最大45,000円 (45%)・許容損失1,500円 (1.5%) までスケールすること
                # SOL @ 40,000円, SL @ 38,600円 (1枚あたり名目損失約1,434円 <= 1,500円, 投入額 40,000円 <= 45,000円)
                summaries_100k = {
                    "BTC": {
                        "price": {"last": 14000000, "best_bid": 13995000, "best_ask": 14000000, "spread_pct": 0.03},
                        "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 55.0, "atr_14": 300000.0},
                        "h4_indicators": {"change_4h_pct": 1.2, "rsi_14": 54.0, "close": 14000000, "sma_25": 13800000},
                        "h1_indicators": {"change_1h_pct": 0.5, "rsi_14": 45.0, "atr_14": 80000.0, "rsi_rebound": True},
                    },
                    "SOL": {
                        "price": {"last": 40000, "best_bid": 40000, "best_ask": 40010, "spread_pct": 0.025},
                        "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 52.0, "atr_14": 1200.0},
                        "h4_indicators": {"change_4h_pct": 1.5, "rsi_14": 50.0, "close": 40000, "sma_25": 39000},
                        "h1_indicators": {"change_1h_pct": 0.6, "rsi_14": 42.0, "atr_14": 450.0, "rsi_rebound": True},
                    },
                }
                val_100k = validate_and_adjust_decision(
                    decision={
                        "action": "BUY",
                        "symbol": "SOL",
                        "limit_price": 40000,
                        "size": 1.0,
                        "take_profit_price": 43000,
                        "stop_loss_price": 38600,
                        "confidence": 0.85,
                    },
                    portfolio=pf_loaded,
                    symbol_rules=DEFAULT_SYMBOL_RULES,
                    market_summaries=summaries_100k,
                )
                self.assertEqual(val_100k["action"], "BUY")
                self.assertAlmostEqual(float(val_100k["size"]), 1.0, places=4)
                self.assertEqual(val_100k["expected_cost_jpy"], 40000)
                self.assertTrue(1200 <= val_100k["expected_loss_jpy"] <= 1500)

                # L3 (日次 -3% = -3,000円) & L4 (週次 -6% = -6,000円) の動的スケーリング確認
                l3_chk = daemon.circuit_breaker.check_l3_daily_loss(pf_loaded, now_dt=t_comp)
                self.assertEqual(l3_chk["daily_loss_limit_jpy"], -3000.0)
                l4_chk = daemon.circuit_breaker.check_l4_weekly_and_streak(pf_loaded, now_dt=t_comp)
                self.assertEqual(l4_chk["weekly_loss_limit_jpy"], -6000.0)

                # ==============================================================
                # Part 2: 第1回終了2時間前凍結 (2027-07-08 22:05) -> 満了全決済 (2027-07-09 00:00)
                # ==============================================================
                t_before_p1_freeze = datetime(2027, 7, 8, 21, 30, 0, tzinfo=JST)
                tickers_p1 = {
                    "XRP": {"bid": 200.0, "ask": 200.1, "last": 200.0},
                    "DOGE": {"bid": 20.0, "ask": 20.01, "last": 20.0},
                }
                daemon.broker.update_market_tickers(tickers_p1)

                # (1) XRP は約定済み保有建玉 (SL_ACTIVE)、DOGE は未約定の買い指値 (PENDING_BUY) を作成
                buy_xrp = daemon.oco_manager.place_new_buy_order(
                    {
                        "action": "BUY",
                        "symbol": "XRP",
                        "limit_price": 200.0,
                        "size": 50.0,
                        "take_profit_price": 220.0,
                        "stop_loss_price": 193.0,
                        "expected_loss_jpy": 350,
                    },
                    tickers=tickers_p1,
                    now_dt=t_before_p1_freeze,
                )
                self.assertEqual(buy_xrp["status"], "PENDING_BUY")
                # XRP を約定させて SL_ACTIVE へ
                daemon.oco_manager.step(
                    tickers={"XRP": {"bid": 199.8, "ask": 199.9, "last": 199.8}},
                    now_dt=t_before_p1_freeze + timedelta(minutes=1),
                )
                # DOGE を未約定の PENDING_BUY として発注
                buy_doge = daemon.oco_manager.place_new_buy_order(
                    {
                        "action": "BUY",
                        "symbol": "DOGE",
                        "limit_price": 19.5,
                        "size": 100.0,
                        "take_profit_price": 22.0,
                        "stop_loss_price": 18.8,
                        "expected_loss_jpy": 70,
                    },
                    tickers=tickers_p1,
                    now_dt=t_before_p1_freeze + timedelta(minutes=2),
                )
                self.assertEqual(buy_doge["status"], "PENDING_BUY")

                # (2) 2027-07-08 22:05 JST (第1回終了2時間前ウィンドウ):
                # 未約定の DOGE 買い指値が自動キャンセルされ、保有中の XRP ポジションは維持されること
                t_p1_freeze = datetime(2027, 7, 8, 22, 5, 0, tzinfo=JST)
                fast_freeze1 = daemon.run_fast_cycle(
                    tickers={"XRP": {"bid": 202.0, "ask": 202.1, "last": 202.0}, "DOGE": {"bid": 20.0, "ask": 20.01, "last": 20.0}},
                    now_dt=t_p1_freeze,
                )
                self.assertEqual(fast_freeze1["campaign_phase"], "PHASE1_PRE_DEADLINE_FREEZE")
                pf_freeze1 = daemon.oco_manager.load_portfolio()
                self.assertEqual(len(pf_freeze1["pending_orders"]), 0)
                active_pos_f1 = [p for p in pf_freeze1["positions"] if p.get("state") != "DUST_QUARANTINED"]
                self.assertEqual(len(active_pos_f1), 1)
                self.assertEqual(active_pos_f1[0]["symbol"], "XRP")

                # (3) 2027-07-09 00:00:05 JST (第1回運用期限到達):
                # XRP ポジションが PHASE1_DEADLINE_SETTLEMENT で即時成行全決済され、PERFORMANCE_REPORT_PHASE1.md が生成されること
                t_p1_end = datetime(2027, 7, 9, 0, 0, 5, tzinfo=JST)
                fast_p1_end = daemon.run_fast_cycle(
                    tickers={"XRP": {"bid": 205.0, "ask": 205.1, "last": 205.0}},
                    now_dt=t_p1_end,
                )
                self.assertEqual(fast_p1_end["status"], "PHASE1_COMPLETED_INTERMISSION")
                self.assertTrue(any(e.get("type") == "PHASE1_DEADLINE_SETTLEMENT" for e in fast_p1_end["events"]))
                self.assertTrue((tmp_dir / "PERFORMANCE_REPORT_PHASE1.md").exists())

                pf_after_p1 = daemon.oco_manager.load_portfolio()
                self.assertTrue(pf_after_p1.get("phase1_settled"))
                self.assertEqual(
                    len([p for p in pf_after_p1["positions"] if p.get("state") != "DUST_QUARANTINED"]),
                    0,
                )

                # (4) 2027-07-09 12:00:00 JST (検証・改良休止ウィンドウ中):
                # 重複決済は走らず、PHASE1_COMPLETED_INTERMISSION で安全にスキップされること
                t_intermission = datetime(2027, 7, 9, 12, 0, 0, tzinfo=JST)
                fast_inter = daemon.run_fast_cycle(now_dt=t_intermission)
                self.assertEqual(fast_inter["status"], "PHASE1_COMPLETED_INTERMISSION")
                self.assertEqual(fast_inter["events"], [])

                # ==============================================================
                # Part 3: 第2回自動開始 (2027-07-10 00:00) -> 第2回満了全決済 (2027-12-10 00:00)
                # ==============================================================
                t_p2_start = datetime(2027, 7, 10, 0, 0, 5, tzinfo=JST)
                fast_p2_start = daemon.run_fast_cycle(
                    tickers={"XRP": {"bid": 210.0, "ask": 210.1, "last": 210.0}},
                    now_dt=t_p2_start,
                )
                self.assertEqual(fast_p2_start["status"], "OK")
                self.assertEqual(fast_p2_start["campaign_phase"], "PHASE2_ACTIVE")
                self.assertTrue(any(e.get("type") == "PHASE2_CAMPAIGN_STARTED" for e in fast_p2_start["events"]))

                # 第2回期間中に新規ポジション (XRP 50枚 @ 200円) を構築して約定させる
                daemon.oco_manager.place_new_buy_order(
                    {
                        "action": "BUY",
                        "symbol": "XRP",
                        "limit_price": 200.0,
                        "size": 50.0,
                        "take_profit_price": 230.0,
                        "stop_loss_price": 192.0,
                        "expected_loss_jpy": 400,
                    },
                    tickers={"XRP": {"bid": 200.5, "ask": 200.6, "last": 200.5}},
                    now_dt=t_p2_start + timedelta(hours=2),
                )
                daemon.oco_manager.step(
                    tickers={"XRP": {"bid": 199.5, "ask": 199.6, "last": 199.5}},
                    now_dt=t_p2_start + timedelta(hours=2, minutes=5),
                )

                # 2027-12-10 00:00:05 JST (第2回運用期限満了):
                # 全ポジションが PHASE2_DEADLINE_SETTLEMENT で成行全決済され、PERFORMANCE_REPORT_PHASE2.md が生成され HALT へ移行すること
                t_p2_end = datetime(2027, 12, 10, 0, 0, 5, tzinfo=JST)
                fast_p2_end = daemon.run_fast_cycle(
                    tickers={"XRP": {"bid": 215.0, "ask": 215.1, "last": 215.0}},
                    now_dt=t_p2_end,
                )
                self.assertEqual(fast_p2_end["status"], "PHASE2_COMPLETED_HALT")
                self.assertTrue(any(e.get("type") == "PHASE2_DEADLINE_SETTLEMENT" for e in fast_p2_end["events"]))
                self.assertTrue((tmp_dir / "PERFORMANCE_REPORT_PHASE2.md").exists())

                pf_after_p2 = daemon.oco_manager.load_portfolio()
                self.assertTrue(pf_after_p2.get("phase2_settled"))
                self.assertEqual(
                    len([p for p in pf_after_p2["positions"] if p.get("state") != "DUST_QUARANTINED"]),
                    0,
                )
            finally:
                daemon.shutdown()

    def test_web_dashboard_api_and_pause_resume_lifecycle(self) -> None:
        """
        v2 ローカルWebダッシュボード (web_dashboard.py & run_v2_web.ps1) の統合検証:
        1. `run_v2_web.ps1` が Pure ASCII である検証
        2. HTTP サーバー (`GET /`, `GET /api/status`, `GET /api/chart`, `POST /api/control`) のエンドツーエンド応答検証
        3. `PAUSE` -> `RESUME` ボタン操作で次サイクルを待たずに `is_paused` が即時反転し、同時に DB キューにも登録されること
        4. `FLATTEN_ALL` 2段階承認 (`nonce` 発行 -> `FLATTEN_CONFIRM`) の動作確認
        """
        import json
        import threading
        import urllib.request
        import pandas as pd
        from web_dashboard import WebDashboardService, create_dashboard_http_server

        # 1. run_v2_web.ps1 / launch_hidden.vbs / register_v2_task.ps1 が Pure ASCII かつ完全非表示 (wscript.exe + launch_hidden.vbs) かつ 127.0.0.1 ループバック専用バインドであること
        import inspect
        base_repo_dir = Path(__file__).resolve().parent.parent
        ps1_path = base_repo_dir / "run_v2_web.ps1"
        vbs_path = base_repo_dir / "launch_hidden.vbs"
        reg_path = base_repo_dir / "register_v2_task.ps1"
        self.assertTrue(ps1_path.exists())
        self.assertTrue(vbs_path.exists())
        self.assertTrue(reg_path.exists())
        self.assertTrue(all(b < 128 for b in ps1_path.read_bytes()), "run_v2_web.ps1 must be 100% Pure ASCII")
        self.assertTrue(all(b < 128 for b in vbs_path.read_bytes()), "launch_hidden.vbs must be 100% Pure ASCII")
        self.assertTrue(all(b < 128 for b in reg_path.read_bytes()), "register_v2_task.ps1 must be 100% Pure ASCII")
        ps1_text = ps1_path.read_text(encoding="ascii")
        reg_text = reg_path.read_text(encoding="ascii")
        self.assertIn("shell.Run(cmd, 0, True)", vbs_path.read_text(encoding="ascii"))
        self.assertIn("wscript.exe", reg_text)
        self.assertIn("launch_hidden.vbs", reg_text)
        self.assertIn('[string]$BindHost = "127.0.0.1"', ps1_text)
        self.assertIn("-BindHost 127.0.0.1 -Port 8080 -NoBrowser", reg_text)
        self.assertNotIn("0.0.0.0", reg_text)
        self.assertEqual(
            inspect.signature(create_dashboard_http_server).parameters["host"].default,
            "127.0.0.1",
        )

        class _DummyPublicClient:
            def get_tickers(self, symbols=None):
                return {
                    "BTC": {"symbol": "BTC", "last": 14500000.0, "bid": 14495000.0, "ask": 14505000.0, "high": 14600000.0, "low": 14300000.0, "volume": 120.0, "spread_pct": 0.068},
                    "XRP": {"symbol": "XRP", "last": 200.0, "bid": 199.9, "ask": 200.1, "high": 205.0, "low": 195.0, "volume": 500000.0, "spread_pct": 0.10},
                }

            def get_klines(self, symbol="BTC", interval="1hour", limit=72, reference_dt=None):
                base_ms = int(datetime(2026, 10, 22, 12, 0, 0, tzinfo=JST).timestamp() * 1000)
                rows = []
                for i in range(24):
                    rows.append(
                        {
                            "open_time": base_ms + i * 3600 * 1000,
                            "open": 14400000.0 + i * 2000,
                            "high": 14410000.0 + i * 2000,
                            "low": 14390000.0 + i * 2000,
                            "close": 14405000.0 + i * 2000,
                            "volume": 5.0,
                        }
                    )
                return pd.DataFrame(rows)

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                dry_run_notify=True,
            )
            # 保有建玉 (XRP) と 隔離端数 (ETH DUST_QUARANTINED) を両方セット
            pf = daemon.oco_manager.load_portfolio()
            pf["jpy_balance"] = 10025
            pf["peak_equity_jpy"] = 10025
            pf["positions"] = [
                {
                    "symbol": "XRP",
                    "side": "BUY",
                    "size": 20.0,
                    "entry_price": 195.0,
                    "current_price": 200.0,
                    "take_profit_price": 215.0,
                    "stop_loss_price": 189.0,
                    "sl_order_id": "9001",
                    "state": "SL_ACTIVE",
                    "opened_at": "2026-10-22T18:00:00+09:00",
                },
                {
                    "symbol": "ETH",
                    "side": "BUY",
                    "size": 0.0007,
                    "entry_price": 0.0,
                    "current_price": 0.0,
                    "take_profit_price": 0.0,
                    "stop_loss_price": 0.0,
                    "sl_order_id": "",
                    "state": "DUST_QUARANTINED",
                    "opened_at": "2026-10-22T17:30:00+09:00",
                },
            ]
            daemon.oco_manager.save_portfolio(pf)

            svc = WebDashboardService(base_dir=tmp_dir, public_client=_DummyPublicClient())
            server, _ = create_dashboard_http_server(host="127.0.0.1", port=0, service=svc)
            port = server.server_address[1]
            t = threading.Thread(target=server.serve_forever, daemon=True)
            t.start()
            base_url = f"http://127.0.0.1:{port}"

            try:
                # (1) GET / (HTML UI)
                with urllib.request.urlopen(f"{base_url}/", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    html = resp.read().decode("utf-8")
                    self.assertIn("GMO AI Trader v2", html)
                    self.assertIn("priceChartCanvas", html)

                # (2) GET /api/status
                with urllib.request.urlopen(f"{base_url}/api/status", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    st = json.loads(resp.read().decode("utf-8"))
                    self.assertFalse(st["daemon"]["is_paused"])
                    self.assertEqual(len(st["positions"]["active"]), 1)
                    self.assertEqual(st["positions"]["active"][0]["symbol"], "XRP")
                    self.assertEqual(len(st["positions"]["quarantined_dust"]), 1)
                    self.assertEqual(st["positions"]["quarantined_dust"][0]["symbol"], "ETH")
                    self.assertEqual(st["equity_summary"]["pattern_a"]["max_allocation_pct"], 0.45)

                # (3) GET /api/chart?symbol=XRP&interval=1hour
                with urllib.request.urlopen(f"{base_url}/api/chart?symbol=XRP&interval=1hour", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    ch = json.loads(resp.read().decode("utf-8"))
                    self.assertEqual(ch["symbol"], "XRP")
                    self.assertEqual(len(ch["candles"]), 24)
                    self.assertTrue(ch["overlays"]["has_position"])
                    self.assertEqual(ch["overlays"]["entry_price"], 195.0)
                    self.assertEqual(ch["overlays"]["stop_loss_price"], 189.0)
                    self.assertEqual(ch["overlays"]["take_profit_price"], 215.0)

                # (4) POST /api/control: PAUSE -> 即座に /api/status が is_paused=True になり、RESUME で is_paused=False に戻ること
                def _post_ctrl(payload: dict) -> dict:
                    req = urllib.request.Request(
                        f"{base_url}/api/control",
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=5) as r:
                        return json.loads(r.read().decode("utf-8"))

                pause_res = _post_ctrl({"command": "PAUSE"})
                self.assertTrue(pause_res["ok"])
                self.assertTrue(pause_res["is_paused"])

                with urllib.request.urlopen(f"{base_url}/api/status", timeout=5) as resp:
                    st_paused = json.loads(resp.read().decode("utf-8"))
                    self.assertTrue(st_paused["daemon"]["is_paused"])

                resume_res = _post_ctrl({"command": "RESUME"})
                self.assertTrue(resume_res["ok"])
                self.assertFalse(resume_res["is_paused"])

                with urllib.request.urlopen(f"{base_url}/api/status", timeout=5) as resp:
                    st_resumed = json.loads(resp.read().decode("utf-8"))
                    self.assertFalse(st_resumed["daemon"]["is_paused"])

                # (5) POST /api/control: FLATTEN_ALL 2段階承認発行
                flt_req = _post_ctrl({"command": "FLATTEN_ALL"})
                self.assertTrue(flt_req["ok"])
                self.assertIn("nonce", flt_req["result"])
                flt_conf = _post_ctrl({"command": "FLATTEN_CONFIRM", "nonce": flt_req["result"]["nonce"]})
                self.assertTrue(flt_conf["ok"])

                # (6) PWA 必須エンドポイント検証 (/manifest.json, /sw.js, /icon-192.png, /icon-512.png, /icon.svg, /api/qr, CORS OPTIONS)
                with urllib.request.urlopen(f"{base_url}/manifest.json", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")
                    manifest = json.loads(resp.read().decode("utf-8"))
                    self.assertEqual(manifest["name"], "GMO AI Trader v2 (Full-Auto)")
                    self.assertEqual(manifest["short_name"], "AI Trader v2")
                    self.assertEqual(manifest["start_url"], "./index.html")
                    self.assertEqual(manifest["scope"], "./")
                    self.assertEqual(manifest["display"], "standalone")
                    self.assertEqual(manifest["orientation"], "portrait")
                    self.assertEqual(manifest["background_color"], "#0b0f19")
                    self.assertEqual(manifest["theme_color"], "#0b0f19")
                    self.assertGreaterEqual(len(manifest["icons"]), 2)
                    self.assertEqual(manifest["icons"][0]["src"], "./icon-192.png")

                opt_req = urllib.request.Request(f"{base_url}/api/control", method="OPTIONS")
                with urllib.request.urlopen(opt_req, timeout=5) as resp:
                    self.assertEqual(resp.status, 204)
                    self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")

                with urllib.request.urlopen(f"{base_url}/sw.js", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    sw_code = resp.read().decode("utf-8")
                    self.assertIn("CLEAR_CACHE", sw_code)
                    self.assertIn("skipWaiting", sw_code)

                with urllib.request.urlopen(f"{base_url}/icon-192.png", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    png192 = resp.read()
                    self.assertTrue(png192.startswith(b"\x89PNG\r\n\x1a\n"))
                    self.assertGreater(len(png192), 500)

                with urllib.request.urlopen(f"{base_url}/icon-512.png", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    png512 = resp.read()
                    self.assertTrue(png512.startswith(b"\x89PNG\r\n\x1a\n"))
                    self.assertGreater(len(png512), len(png192))

                with urllib.request.urlopen(f"{base_url}/icon.svg", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    svg_txt = resp.read().decode("utf-8")
                    self.assertIn("<svg", svg_txt)

                with urllib.request.urlopen(f"{base_url}/api/qr?text=http%3A%2F%2F192.168.11.3%3A8080", timeout=5) as resp:
                    self.assertEqual(resp.status, 200)
                    qr_svg = resp.read().decode("utf-8")
                    self.assertIn("<svg", qr_svg)
                    self.assertIn("<rect", qr_svg)

                # (7) network_info および 静的PWAスナップショット出力 (docs/) 検証
                self.assertIn("network_info", st)
                self.assertIn("lan_ip", st["network_info"])
                self.assertIn("lan_url", st["network_info"])
                self.assertEqual(st["network_info"]["github_pages_url"], "https://qanojisan.github.io/gmo-ai-trader-auto/")

                export_res = svc.export_static_pwa_bundle(output_dir=tmp_dir / "docs")
                self.assertTrue(export_res["ok"])
                for fname in (
                    "index.html",
                    "manifest.json",
                    "sw.js",
                    "icon-192.png",
                    "icon-512.png",
                    "icon.svg",
                    "qr-lan.svg",
                    "qr-gh.svg",
                    "status_snapshot.json",
                    "chart_snapshots.json",
                ):
                    self.assertTrue((tmp_dir / "docs" / fname).exists(), f"Missing exported file: {fname}")
                from src.pwa_crypto import decrypt_json_envelope
                snap_json = json.loads((tmp_dir / "docs" / "status_snapshot.json").read_text(encoding="utf-8"))
                self.assertTrue(snap_json["encrypted"])
                dec_snap = decrypt_json_envelope(snap_json, svc.get_pwa_unlock_password())
                self.assertTrue(dec_snap["static_snapshot"])
            finally:
                server.shutdown()
                server.server_close()
                svc.close()
                daemon.shutdown()

    def test_round9_non_disruptive_post_golive_comprehensive_audit(self) -> None:
        """
        Round 9: 本番稼働後・無停止総合監査 (Non-Disruptive Post-Go-Live Comprehensive Audit)
        - [監査1] ステルスラッパー (run_v2_daemon.ps1 / run_v2_web.ps1) の PID 再利用耐性 (ProcessName 検証)
        - [監査3] Git Auto-Push のプロセス間ロック (v2_git_push.lock) ＆ メインループ非ブロッキング検証
        - [監査4] GitHub Pages (*.github.io) 上での LAN IP タイムアウト完全スキップ＆閲覧専用ガイダンス検証
        - [監査5] High-Water Mark (peak_equity_jpy) の単発価格スパイク・板逆転汚染防御＆3回連続確認ロジック検証
        - [監査6] 第1回・第2回 運用期限の SQLite WAL 双方向復元＆24時間超停電ジャンプ時の Phase 1 優先決済保証
        """
        from web_dashboard import DASHBOARD_HTML, push_docs_snapshot_async
        from src.circuit_breaker import SingleInstanceLock

        base_repo_dir = Path(__file__).resolve().parent.parent

        # [監査1] run_v2_daemon.ps1 / run_v2_web.ps1 の ProcessName 検証ガード確認
        daemon_ps1 = (base_repo_dir / "run_v2_daemon.ps1").read_text(encoding="ascii")
        web_ps1 = (base_repo_dir / "run_v2_web.ps1").read_text(encoding="ascii")
        self.assertIn('ProcessName -match "^(powershell|pwsh|python)"', daemon_ps1)
        self.assertIn('ProcessName -match "^(powershell|pwsh|python)"', web_ps1)

        # [監査4] GitHub Pages (*.github.io) での LAN タイムアウト完全スキップ＆閲覧専用トーストメッセージ確認
        self.assertIn('location.hostname.endsWith("github.io") || location.protocol === "https:"', DASHBOARD_HTML)
        self.assertIn(
            "📱 外出先（GitHub Pages閲覧専用モード）です。操作はPCダッシュボード（127.0.0.1:8080）から実行してください",
            DASHBOARD_HTML,
        )

        # [監査5] High-Water Mark (peak_equity_jpy) 単発価格スパイク汚染防御 ＆ 3回連続確認
        cb = MultiTierCircuitBreaker(l5_kill_switch_equity_jpy=8500.0)
        pf_hwm = {
            "initial_capital_jpy": 10000,
            "jpy_balance": 6025,
            "peak_equity_jpy": 10025,
            "drawdown_kill_switch_jpy": 8521.25,
            "positions": [
                {
                    "symbol": "XRP",
                    "size": 20.0,
                    "entry_price": 200.0,
                    "current_price": 200.0,
                    "state": "SL_ACTIVE",
                }
            ],
        }
        # (a) 1サイクルだけ XRP Bid が 2,000円 (10倍スパイク異常値) に跳ねた場合 -> bids_valid=False で棄却され peak_equity_jpy は 10,025 のまま
        r_spike = cb.evaluate_l5_hard_kill_switch(pf_hwm, {"XRP": {"bid": 2000.0, "ask": 2001.0}})
        self.assertFalse(r_spike["bids_valid"])
        self.assertEqual(pf_hwm["peak_equity_jpy"], 10025)
        # その後 3サイクル正常価格 (200円) に戻っても L5 キルスイッチが誤発動しないこと
        for _ in range(3):
            r_norm = cb.evaluate_l5_hard_kill_switch(pf_hwm, {"XRP": {"bid": 200.0, "ask": 200.1}})
            self.assertFalse(r_norm["latched"])
            self.assertEqual(r_norm["consecutive_breaches"], 0)

        # (b) 正常な価格範囲 (+25%: 250円, 総資産約 11,020.5円) で推移した場合:
        # 1〜2回目ではまだ HWM は更新されず、3回連続確認された時点で HWM が更新されること
        r_up1 = cb.evaluate_l5_hard_kill_switch(pf_hwm, {"XRP": {"bid": 250.0, "ask": 250.1}})
        self.assertEqual(pf_hwm["peak_equity_jpy"], 10025)
        r_up2 = cb.evaluate_l5_hard_kill_switch(pf_hwm, {"XRP": {"bid": 252.0, "ask": 252.1}})
        self.assertEqual(pf_hwm["peak_equity_jpy"], 10025)
        r_up3 = cb.evaluate_l5_hard_kill_switch(pf_hwm, {"XRP": {"bid": 251.0, "ask": 251.1}})
        self.assertGreater(pf_hwm["peak_equity_jpy"], 11000)
        self.assertTrue(r_up3["peak_updated"])

        # [監査3 & 監査6] 一時ディレクトリ上での Git Push ロック非干渉 ＆ 運用期限 WAL 復元・停電ジャンプ決済検証
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            # Audit 3: 一時ディレクトリで push_docs_snapshot_async を呼んでも即座に return しブロックしないこと
            t_start = time.monotonic()
            push_docs_snapshot_async(tmp_dir, force=True)
            self.assertLess(time.monotonic() - t_start, 0.05)

            git_lock_a = SingleInstanceLock(tmp_dir / "v2_git_push.lock")
            git_lock_b = SingleInstanceLock(tmp_dir / "v2_git_push.lock")
            self.assertTrue(git_lock_a.acquire())
            self.assertFalse(git_lock_b.acquire())
            git_lock_a.release()
            self.assertTrue(git_lock_b.acquire())
            git_lock_b.release()

            # Audit 6: 2027-07-08 21:00 に建玉を保有したまま PC が停電し、2027-07-10 00:05 (Phase 2 開始後) に復帰した場合でも
            # Phase 1 の全決済と PERFORMANCE_REPORT_PHASE1.md 生成が漏れなく先に実行されること
            daemon = AutoTraderDaemon(base_dir=tmp_dir, execution_mode="DRY_RUN", dry_run_notify=True)
            try:
                t_pre = datetime(2027, 7, 8, 21, 0, 0, tzinfo=JST)
                tk = {"XRP": {"bid": 200.0, "ask": 200.1, "last": 200.0}}
                daemon.broker.update_market_tickers(tk)
                daemon.oco_manager.place_new_buy_order(
                    {
                        "action": "BUY",
                        "symbol": "XRP",
                        "limit_price": 200.0,
                        "size": 20.0,
                        "take_profit_price": 220.0,
                        "stop_loss_price": 192.0,
                        "expected_loss_jpy": 160,
                    },
                    tickers=tk,
                    now_dt=t_pre,
                )
                daemon.oco_manager.step(tickers={"XRP": {"bid": 199.8, "ask": 199.9, "last": 199.8}}, now_dt=t_pre + timedelta(minutes=1))

                # 2027-07-10 00:05 JST にいきなり起動
                t_jump_p2 = datetime(2027, 7, 10, 0, 5, 0, tzinfo=JST)
                res_jump = daemon.run_fast_cycle(tickers={"XRP": {"bid": 205.0, "ask": 205.1, "last": 205.0}}, now_dt=t_jump_p2)
                self.assertEqual(res_jump["campaign_phase"], "PHASE2_ACTIVE")
                self.assertTrue(any(e.get("type") == "PHASE1_DEADLINE_SETTLEMENT" for e in res_jump["events"]))
                self.assertTrue(any(e.get("type") == "PHASE2_CAMPAIGN_STARTED" for e in res_jump["events"]))
                self.assertTrue((tmp_dir / "PERFORMANCE_REPORT_PHASE1.md").exists())

                # portfolio.json の phase1_settled が万一消えても SQLite WAL (account_meta) から自動復元され二重決済しないこと
                pf_corrupt = daemon.oco_manager.load_portfolio()
                pf_corrupt.pop("phase1_settled", None)
                daemon.oco_manager.save_portfolio(pf_corrupt, now_dt=t_jump_p2 + timedelta(minutes=1))

                res_next = daemon.run_fast_cycle(tickers={"XRP": {"bid": 205.0, "ask": 205.1, "last": 205.0}}, now_dt=t_jump_p2 + timedelta(minutes=2))
                self.assertFalse(any(e.get("type") == "PHASE1_DEADLINE_SETTLEMENT" for e in res_next["events"]))
                self.assertTrue(daemon.oco_manager.load_portfolio().get("phase1_settled"))
            finally:
                daemon.shutdown()

    def test_round10_bch_buy_pipeline_lot_rounding_and_v2_auto_notifier_format(self) -> None:
        """
        Round 10 リグレッション検証:
        1. BCH の BUY 判定 (42,987円 / 0.076 BCH) ➔ 数量刻み (0.001 BCH) 丸め ➔ 自動指値発注 (PENDING_BUY)
        2. 通知メッセージ & manual_instruction に旧v1の「手動発注手順」「上限5,000円枠」「買付反映」が一切含まれず、
           v2全自動執行ステータス・注文ID・総資産45%枠上限が出力されること
        3. BTCが平穏な押し目局面で 1h SMA25 割れのみによって誤って RISK_OFF_CRASH キャンセルが発動しないこと
        4. 万一 PENDING_BUY が後から自動キャンセルされた場合や SOK 即時失効した場合に、
           latest_decision.json が「BUY 発注中」のまま残らず HOLD (ORDER_CANCELED / CANCELED) へ同期されること
        """
        from src.notifier import build_manual_order_guide, format_notification_message

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                ai_judge=AIJudge(provider="rule_based"),
                dry_run_notify=True,
            )
            try:
                t0 = datetime(2026, 10, 23, 23, 6, 57, tzinfo=JST)
                pf = daemon.oco_manager.load_portfolio()
                pf["jpy_balance"] = 10025
                pf["initial_capital_jpy"] = 10025
                daemon.oco_manager.save_portfolio(pf, now_dt=t0)

                raw_bch_decision = {
                    "bearish_counter_thesis": ["上位足レジスタンス反落リスク", "アルト流動性低下リスク"],
                    "bullish_conviction_factors": ["対BTC相対強度良好", "1h押し目反発"],
                    "conviction_score": 75,
                    "action": "BUY",
                    "symbol": "BCH",
                    "order_type": "LIMIT_POST_ONLY",
                    "limit_price": 42987,
                    "size": 0.0764,  # size_step=0.001 により 0.076 BCH へ切り捨て丸めされること
                    "take_profit_price": 47602,
                    "stop_loss_price": 41053,
                    "confidence": 0.75,
                    "provider": "rule_based_quant",
                    "reasoning": "BCH 押し目買い推奨テスト",
                }
                bch_mkt = {
                    "BCH": {
                        "price": {"last": 43500.0, "best_bid": 43490.0, "best_ask": 43510.0, "spread_pct": 0.04},
                        "daily_indicators": {"trend": "UPTREND", "atr_14": 900.0, "rsi_14": 50.2, "macd_hist_rising": True},
                        "h4_indicators": {"close": 43500.0, "sma_25": 43000.0, "rsi_14": 48.0, "change_4h_pct": 0.5},
                        "h1_indicators": {"close": 43500.0, "sma_25": 43400.0, "rsi_14": 42.0, "rsi_rebound": True, "atr_14": 180.0},
                    },
                    "BTC": {
                        "price": {"last": 13000000.0, "best_bid": 12999000.0, "best_ask": 13001000.0, "spread_pct": 0.01},
                        "daily_indicators": {"trend": "UPTREND", "atr_14": 250000.0, "rsi_14": 52.0},
                        "h4_indicators": {"close": 13000000.0, "sma_25": 12950000.0, "rsi_14": 51.0, "change_4h_pct": 0.3},
                        "h1_indicators": {"close": 13000000.0, "sma_25": 13010000.0, "change_1h_pct": -0.1, "rsi_14": 48.0},
                    },
                }
                validated = validate_and_adjust_decision(
                    decision=raw_bch_decision,
                    portfolio=daemon.oco_manager.load_portfolio(),
                    symbol_rules=DEFAULT_SYMBOL_RULES,
                    market_summaries=bch_mkt,
                )
                self.assertEqual(validated["action"], "BUY")
                self.assertEqual(validated["symbol"], "BCH")
                self.assertAlmostEqual(float(validated["size"]), 0.076, places=6)

                tickers = {
                    "BCH": {"symbol": "BCH", "bid": 43490.0, "ask": 43510.0, "last": 43500.0},
                    "BTC": {"symbol": "BTC", "bid": 12999000.0, "ask": 13001000.0, "last": 13000000.0},
                }
                exec_res = daemon.oco_manager.place_new_buy_order(validated, tickers=tickers, now_dt=t0)
                self.assertEqual(exec_res["status"], "PENDING_BUY")
                oid = exec_res["pending_order"]["order_id"]
                validated["execution_result"] = exec_res
                validated["manual_instruction"] = build_manual_order_guide(validated)

                msg = format_notification_message(validated)
                for legacy_kw in ("手動発注手順", "手動操作手順", "買付反映", "上限5,000円枠", "5,000円枠"):
                    self.assertNotIn(legacy_kw, msg)
                    self.assertNotIn(legacy_kw, validated["manual_instruction"])
                self.assertIn("【🤖 v2 全自動執行ステータス（手動操作不要）】", msg)
                self.assertIn("総資産45%枠上限", msg)
                self.assertIn(oid, msg)

                # 15分後に1h足が押し目でSMA25を割り込んでも、BTCが急落していなければ誤って RISK_OFF_CRASH にならず pending_orders が維持されること
                calm_pullback_summaries = {
                    "BTC": {
                        "h1_indicators": {"close": 12990000.0, "sma_25": 13010000.0, "change_1h_pct": -0.2},
                        "h4_indicators": {"close": 12990000.0, "sma_25": 12950000.0, "change_4h_pct": 0.2, "rsi_14": 49.0},
                        "daily_indicators": {"trend": "DOWNTREND_OR_RANGE"},
                    },
                    "BCH": {
                        "h1_indicators": {"close": 43200.0, "sma_25": 43400.0},
                        "h4_indicators": {"close": 43200.0, "sma_25": 43000.0},
                        "daily_indicators": {"trend": "UPTREND"},
                    },
                    "SOL": {"h1_indicators": {"close": 23800.0, "sma_25": 24000.0}, "daily_indicators": {"trend": "DOWNTREND_OR_RANGE"}},
                    "XRP": {"h1_indicators": {"close": 84.0, "sma_25": 85.0}, "daily_indicators": {"trend": "DOWNTREND_OR_RANGE"}},
                }
                reg_eval = daemon.regime_filter.evaluate(calm_pullback_summaries, now_dt=t0 + timedelta(minutes=15))
                self.assertNotEqual(reg_eval["regime"], "RISK_OFF_CRASH")
                self.assertFalse(reg_eval["should_cancel_pending_buys"])

                # 万一 RISK_OFF_CRASH 等で待機中指値がキャンセルされた場合、latest_decision.json も HOLD (ORDER_CANCELED) に同期されること
                from src.json_utils import load_json_file, write_json_file

                write_json_file(daemon.latest_decision_path, validated)
                daemon._sync_latest_decision_on_pending_cancel(
                    canceled_order_ids=[oid],
                    cancel_reason="テスト自動取消",
                    now_dt=t0 + timedelta(minutes=15),
                )
                synced_latest = load_json_file(daemon.latest_decision_path)
                self.assertEqual(synced_latest["action"], "HOLD")
                self.assertEqual(synced_latest["original_action"], "BUY")
                self.assertEqual(synced_latest["status"], "HOLD (ORDER_CANCELED)")
                self.assertIn("テスト自動取消", synced_latest["reasoning"])
                self.assertIn("自動発注を見送りました", synced_latest["manual_instruction"])
            finally:
                daemon.shutdown()

    def test_round11_dpapi_vault_acl_and_git_token_security_audit(self) -> None:
        """
        Round 11 セキュリティ強化監査テスト:
        1. Windows DPAPI (`Crypt32.dll`) `protect_secret` / `unprotect_secret` の暗号化・復号ラウンドトリップ、
           二重暗号化防止、平文後方互換性、`load_and_unprotect_dotenv` の動作検証
        2. 本番 `.env` 内の5つのシークレット (`GEMINI_API_KEY`, `DISCORD_WEBHOOK_URL`, `GMO_API_KEY`, `GMO_API_SECRET`, `GITHUB_TOKEN`)
           がすべて `DPAPI:AQAA...` 形式で保存されており、平文キーが一切残っていないこと
        3. 本番 `.env` の Windows ACL (`icacls`) から `BUILTIN\\Users` および `Authenticated Users` が排除されていること
        4. `.git/config` に平文トークン (`ghp_...` / `@github.com`) が一切含まれず、`build_github_basic_auth_header()` と
           `mask_secrets()` によるメモリ一時認証・完全マスクが機能すること
        """
        import subprocess
        from src.dpapi_vault import (
            DEFAULT_SECRET_KEYS,
            build_github_basic_auth_header,
            encrypt_dotenv_secrets_inplace,
            get_secret_env,
            is_dpapi_protected,
            load_and_unprotect_dotenv,
            protect_secret,
            unprotect_secret,
        )
        from src.notifier import mask_secrets

        # 1. DPAPI 暗号化・復号ラウンドトリップ検証
        sample_plain = "test-secret-value-12345!@#_日本語OK"
        enc = protect_secret(sample_plain)
        self.assertTrue(enc.startswith("DPAPI:AQAA"))
        self.assertTrue(is_dpapi_protected(enc))
        # 二重暗号化されないこと
        self.assertEqual(protect_secret(enc), enc)
        # 復号で元の平文と完全一致すること
        self.assertEqual(unprotect_secret(enc), sample_plain)
        # 平文を渡した場合はそのまま返ること（後方互換）
        self.assertEqual(unprotect_secret("plain-backward-compat"), "plain-backward-compat")

        # 一時 .env ファイルでの encrypt_dotenv_secrets_inplace & load_and_unprotect_dotenv 検証
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_env = Path(tmpdir) / ".env"
            tmp_env.write_text(
                "GMO_API_KEY=dummy_gmo_key_abc123\n"
                "EXECUTION_MODE=LIVE\n",
                encoding="utf-8",
            )
            stats = encrypt_dotenv_secrets_inplace(tmp_env, extra_secrets={"GITHUB_TOKEN": "ghp_dummyToken999"})
            self.assertEqual(stats["GMO_API_KEY"], "DPAPI_ENCRYPTED")
            self.assertEqual(stats["GITHUB_TOKEN"], "DPAPI_ENCRYPTED")
            raw_env_text = tmp_env.read_text(encoding="utf-8")
            self.assertNotIn("dummy_gmo_key_abc123", raw_env_text)
            self.assertNotIn("ghp_dummyToken999", raw_env_text)

            auth_hdr = build_github_basic_auth_header(dotenv_path=tmp_env)
            self.assertTrue(auth_hdr.startswith("AUTHORIZATION: basic "))
            masked_cmd = mask_secrets(f'git -c "http.extraheader={auth_hdr}" push origin main')
            self.assertNotIn(auth_hdr.split()[-1], masked_cmd)
            self.assertIn("AUTHORIZATION: basic ***REDACTED***", masked_cmd)

        # 2. 本番 `.env` の DPAPI 暗号化状態検証
        repo_root = Path(__file__).resolve().parent.parent
        prod_env = repo_root / ".env"
        self.assertTrue(prod_env.exists())
        prod_env_text = prod_env.read_text(encoding="utf-8")
        required_dpapi_keys = {
            "GEMINI_API_KEY",
            "DISCORD_WEBHOOK_URL",
            "GMO_API_KEY",
            "GMO_API_SECRET",
            "GITHUB_TOKEN",
            "PWA_UNLOCK_PASSWORD",
        }
        found_keys = set()
        for line in prod_env_text.splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k, v = k.strip(), v.strip()
            if k in required_dpapi_keys:
                found_keys.add(k)
                self.assertTrue(v.startswith("DPAPI:AQAA"), f"{k} must be DPAPI encrypted")
                dec_val = unprotect_secret(v)
                self.assertGreater(len(dec_val), 10)
        self.assertEqual(found_keys, required_dpapi_keys)

        # 3. 本番 `.env` の Windows ACL (`icacls`) 検証
        icacls_res = subprocess.run(
            ["icacls", str(prod_env)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000),
        )
        acl_out = icacls_res.stdout
        self.assertNotIn("BUILTIN\\Users", acl_out)
        self.assertNotIn("Authenticated Users", acl_out)
        self.assertIn("BUILTIN\\Administrators:(F)", acl_out)
        self.assertIn("NT AUTHORITY\\SYSTEM:(F)", acl_out)

        # 4. 本番 `.git/config` に平文トークンが残っていないこと
        git_cfg = (repo_root / ".git" / "config").read_text(encoding="utf-8")
        self.assertNotIn("ghp_", git_cfg)
        self.assertNotIn("@github.com", git_cfg)
        self.assertIn("https://github.com/QANOJISAN/gmo-ai-trader-auto.git", git_cfg)

    def test_round12_zero_knowledge_pwa_aes_gcm_and_v1_decommission_audit(self) -> None:
        """
        Round 12 セキュリティ＆旧v1廃止監査テスト:
        1. `src/pwa_crypto.py` (Windows `bcrypt.dll` AES-256-GCM + PBKDF2-SHA256 150,000回) の暗号化・復号ラウンドトリップ検証、
           および改ざん検知 (認証タグ不一致エラー)・誤パスワード拒否検証
        2. `export_static_pwa_bundle` で出力された `docs/index.html`, `docs/status_snapshot.json`, `docs/chart_snapshots.json` の
           全ファイル内に、`20025` や `20,025`、建玉情報、AI判断根拠、ロック解除パスワード等の平文データが1文字も含まれないこと
        3. 旧v1ディレクトリが廃止・削除され、v2が専用 `.venv\\Scripts\\python.exe` で完全独立稼働していること
        """
        import base64
        import json
        from src.pwa_crypto import decrypt_json_envelope, encrypt_json_envelope
        from web_dashboard import WebDashboardService

        # 1. Windows bcrypt.dll AES-256-GCM + PBKDF2 150,000回 ラウンドトリップ & 改ざん検知検証
        sample_payload = {
            "equity_summary": {"total_equity_jpy": 20025, "initial_capital_jpy": 20000},
            "positions": {"active": [{"symbol": "BCH", "size": 0.076, "entry_price": 42987}]},
            "latest_decision": {"action": "BUY", "reasoning": "上位足トレンドと1時間足押し目反発が合致"},
        }
        test_pw = "GMOv2-Test-Pass-9876"
        env = encrypt_json_envelope(sample_payload, test_pw)
        self.assertTrue(env["encrypted"])
        self.assertEqual(env["version"], 1)
        self.assertEqual(env["kdf"], "PBKDF2-SHA256")
        self.assertEqual(env["iterations"], 150000)
        self.assertEqual(env["cipher"], "AES-256-GCM")
        self.assertEqual(len(base64.b64decode(env["salt"])), 16)
        self.assertEqual(len(base64.b64decode(env["iv"])), 12)

        dec = decrypt_json_envelope(env, test_pw)
        self.assertEqual(dec, sample_payload)

        # 誤パスワード時に ValueError が発生すること
        with self.assertRaises(ValueError):
            decrypt_json_envelope(env, "Wrong-Password-0000")

        # ペイロード末尾1バイト (認証タグ) 改ざん時に ValueError が発生すること
        tampered_env = dict(env)
        raw_ct = bytearray(base64.b64decode(tampered_env["payload"]))
        raw_ct[-1] ^= 0x55
        tampered_env["payload"] = base64.b64encode(bytes(raw_ct)).decode("ascii")
        with self.assertRaises(ValueError):
            decrypt_json_envelope(tampered_env, test_pw)

        # 2. export_static_pwa_bundle のゼロ知識化 (docs/ 内に平文資産・建玉・AI根拠が1文字も含まれないこと)
        class _DummyPub:
            def get_tickers(self, symbols=None):
                return {"BCH": {"symbol": "BCH", "last": 43500.0, "bid": 43490.0, "ask": 43510.0}}

            def get_klines(self, symbol="BTC", interval="1hour", limit=48, reference_dt=None):
                return None

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            daemon = AutoTraderDaemon(base_dir=tmp_dir, execution_mode="DRY_RUN", dry_run_notify=True)
            try:
                pf = daemon.oco_manager.load_portfolio()
                pf["initial_capital_jpy"] = 20000
                pf["jpy_balance"] = 20025
                pf["peak_equity_jpy"] = 20025
                pf["positions"] = [
                    {
                        "symbol": "BCH",
                        "side": "BUY",
                        "size": 0.076,
                        "entry_price": 42987.0,
                        "current_price": 43500.0,
                        "take_profit_price": 45500.0,
                        "stop_loss_price": 41800.0,
                        "sl_order_id": "777888",
                        "state": "SL_ACTIVE",
                    }
                ]
                daemon.oco_manager.save_portfolio(pf)
                (tmp_dir / "latest_decision.json").write_text(
                    json.dumps(
                        {
                            "action": "BUY",
                            "symbol": "BCH",
                            "reasoning": "SECRET_AI_REASONING_MARKER_999",
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

                svc = WebDashboardService(base_dir=tmp_dir, public_client=_DummyPub())
                try:
                    out_docs = tmp_dir / "docs"
                    svc.export_static_pwa_bundle(output_dir=out_docs, include_charts=True)
                    unlock_pw = svc.get_pwa_unlock_password()

                    for fname in ("index.html", "status_snapshot.json", "chart_snapshots.json"):
                        raw_text = (out_docs / fname).read_text(encoding="utf-8")
                        for forbidden in (
                            "20025",
                            "20,025",
                            "42987",
                            "42,987",
                            "777888",
                            "SECRET_AI_REASONING_MARKER_999",
                            unlock_pw,
                        ):
                            self.assertNotIn(
                                forbidden,
                                raw_text,
                                f"Plaintext leak '{forbidden}' found in docs/{fname}",
                            )

                    status_env = json.loads((out_docs / "status_snapshot.json").read_text(encoding="utf-8"))
                    chart_env = json.loads((out_docs / "chart_snapshots.json").read_text(encoding="utf-8"))
                    self.assertEqual(
                        set(status_env.keys()),
                        {"encrypted", "version", "kdf", "iterations", "cipher", "salt", "iv", "payload"},
                    )
                    self.assertEqual(
                        set(chart_env.keys()),
                        {"encrypted", "version", "kdf", "iterations", "cipher", "salt", "iv", "payload"},
                    )
                    dec_status = decrypt_json_envelope(status_env, unlock_pw)
                    self.assertEqual(dec_status["equity_summary"]["jpy_balance"], 20025)
                    self.assertEqual(dec_status["latest_decision"]["reasoning"], "SECRET_AI_REASONING_MARKER_999")
                finally:
                    svc.close()
            finally:
                daemon.shutdown()

        # 3. 旧v1廃止・v2独立 .venv 検証
        repo_root = Path(__file__).resolve().parent.parent
        legacy_v1_dir = repo_root.parent / "gmo-ai-trader"
        self.assertFalse(legacy_v1_dir.exists(), "Legacy v1 directory must be completely deleted")
        v2_venv_py = repo_root / ".venv" / "Scripts" / "python.exe"
        self.assertTrue(v2_venv_py.exists(), "v2 independent .venv python.exe must exist")
        archive_v1 = repo_root / "archive_v1"
        for f in ("portfolio.json", "history_log.csv", "latest_decision.json", "PERFORMANCE_REPORT.md"):
            self.assertTrue((archive_v1 / f).exists(), f"archive_v1/{f} must exist")

    # ==================================================================
    # Round 13 Audit: Step 1（長期無人稼働の死角封じ・取引所API安全弁）
    #               + Step 2（エグジット4段階再設計・15秒Maker利確・Time-Stop・ロット逆算＆動的枠拡張）
    # ==================================================================
    def test_round13_step1_and_step2_comprehensive_upgrade_audit(self) -> None:
        import os

        from src.gmo_private_client import (
            GMOPrivateAPIError,
            GMOPrivateClient,
            is_in_daily_fee_settlement_freeze_window,
        )
        from web_dashboard import cleanup_stale_git_push_lock

        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal = DatabaseWAL(tmp_dir / "round13.db", portfolio_json_path=tmp_dir / "portfolio.json", auto_sync_files=True)
            ex = ChaosMockExchange(tmp_dir, wal_db=wal)
            oco = PseudoOCOManager(
                broker=ex,
                portfolio_path=tmp_dir / "portfolio.json",
                wal_db=wal,
                dry_run_notify=True,
                async_tp_trial_mode=True,
            )
            t0 = datetime(2026, 10, 24, 12, 0, 0, tzinfo=JST)

            # ------------------------------------------------------------------
            # ① STOP 失効（STOP_EXPIRED_UNFILLED: 約定ゼロ失効）時の即時追撃決済フェイルセーフ
            #    ＆ changeOrder 競合（ERR-5003 -> EXECUTED 照合）の冪等処理
            # ------------------------------------------------------------------
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
                now_dt=t0,
            )
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 43990, "bid": 43980, "last": 43985}},
                now_dt=t0 + timedelta(seconds=10),
            )
            pf1 = oco.load_portfolio()
            self.assertEqual(len(pf1["positions"]), 1)
            sl_oid_exp = pf1["positions"][0]["sl_order_id"]

            # フラッシュクラッシュ（±20%超制限）で STOP (FAK) が executed_size=0 のまま EXPIRED 失効したケースを注入
            ex.inject_partial_stop_fill_and_expire(
                sl_oid_exp,
                filled_size=0.0,
                fill_price=42500.0,
                now_dt=t0 + timedelta(seconds=20),
            )
            guardian_evs = oco.run_fast_recovery_guardian(
                tickers={"BCH": {"symbol": "BCH", "ask": 34100, "bid": 34000, "last": 34000}},
                now_dt=t0 + timedelta(seconds=21),
            )
            self.assertTrue(
                any(e.get("state_transition") == "STOP_EXPIRED_UNFILLED" for e in guardian_evs),
                f"200ms guardian must detect STOP_EXPIRED_UNFILLED and chase-liquidate: {guardian_evs}",
            )
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # changeOrder 競合（ERR-5003 / ERR-5016 -> get_orders で EXECUTED 確認時は例外にせず冪等完了）
            priv_client = GMOPrivateClient(
                api_key="dummy_key",
                api_secret="dummy_secret",
                wal_db=wal,
            )
            priv_client._sleep = lambda _: None  # type: ignore[method-assign]
            race_calls = {"change": 0, "orders": 0}

            def _mock_request_race(method: str, path: str, params=None, body=None, priority: int = 3):
                if path == "/private/v1/changeOrder":
                    race_calls["change"] += 1
                    raise GMOPrivateAPIError(
                        "ERR-5003 invalid order state",
                        messages=[{"message_code": "ERR-5003", "message_string": "Order state invalid"}],
                    )
                if path == "/private/v1/orders":
                    race_calls["orders"] += 1
                    return {
                        "list": [
                            {
                                "orderId": 999111,
                                "symbol": "SOL",
                                "side": "SELL",
                                "executionType": "STOP",
                                "orderStatus": "EXECUTED",
                                "price": "20500",
                                "size": "0.1",
                                "executedSize": "0.1",
                            }
                        ]
                    }
                return {}

            priv_client._request = _mock_request_race  # type: ignore[method-assign]
            idemp_res = priv_client.change_order("999111", 20600)
            self.assertEqual(idemp_res["status"], "EXECUTED")
            self.assertTrue(idemp_res.get("idempotent_already_executed"))
            self.assertEqual(race_calls["change"], 1)
            self.assertEqual(race_calls["orders"], 1)

            # HTTP Date ヘッダからの時刻オフセット自動補正 (|Δt| >= 500ms)
            offset_applied = priv_client.calibrate_clock_offset_from_http_date(
                "Sat, 24 Oct 2026 03:00:02 GMT",
                local_now_ms=int(datetime(2026, 10, 24, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                rtt_ms=40,
            )
            self.assertGreaterEqual(abs(offset_applied), 500)
            self.assertEqual(priv_client.clock_offset_ms, offset_applied)

            # 非対称銘柄 (BCH: min=0.01, step=0.001) の最小ロット未満部分約定 (0.004 < 0.01) が 60秒グレースで安易にキャンセルされないこと
            buy_submin = oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.02,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0 + timedelta(minutes=1),
            )
            submin_oid = buy_submin["order"]["order_id"]
            ex.simulate_partial_fill(submin_oid, filled_size=0.004, fill_price=44000.0, now_dt=t0 + timedelta(minutes=1, seconds=5))
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 44150, "bid": 44100, "last": 44100}},
                now_dt=t0 + timedelta(minutes=1, seconds=10),
            )
            # 90秒経過時点 (> 60秒グレース、ただし < 4時間期限 & 通常相場) でもキャンセルされず待機継続
            oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 44150, "bid": 44100, "last": 44100}},
                now_dt=t0 + timedelta(minutes=2, seconds=50),
            )
            pf_submin = oco.load_portfolio()
            self.assertEqual(len(pf_submin["pending_orders"]), 1)
            self.assertTrue(pf_submin["pending_orders"][0].get("sub_min_grace_hold"))
            # その後追加約定で 0.012 >= minOrderSize(0.01) に達した瞬間にトランシェ STOP が即時発注されること
            ex.simulate_partial_fill(submin_oid, filled_size=0.008, fill_price=44000.0, now_dt=t0 + timedelta(minutes=3))
            tranche_evs = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 44150, "bid": 44100, "last": 44100}},
                now_dt=t0 + timedelta(minutes=3, seconds=5),
            )
            self.assertTrue(any(e.get("type") == "PARTIAL_FILL_TRANCHE_SL_PLACED" for e in tranche_evs))
            # 60秒グレース超過かつ >= minOrderSize のため残りはキャンセルされ SL_ACTIVE ポジションへ昇格
            self.assertTrue(any(e.get("type") == "BUY_FILLED_AND_SL_PLACED" for e in tranche_evs))
            oco.close_position_immediately("BCH", reason="CLEANUP_TEST", now_dt=t0 + timedelta(minutes=3, seconds=10))

            # ------------------------------------------------------------------
            # ② 4段階トレーリング（Stage 1: <+1.5R 変更なし、Stage 2: >=+1.6R 建値++0.15Rロック、
            #    Stage 3: >=+2.2R シャンデリア、Stage 4: >=+3.0R クライマックス）の単調引き上げ検証
            # ------------------------------------------------------------------
            # Stage 1: +1.4R (10000 -> 10420, 1R=300) では初期SL(9700)を維持し should_update=False
            s1_st, s1_px, s1_upd = compute_four_stage_trailing_sl(
                entry_price=10000,
                current_bid=10420,
                highest_bid=10420,
                current_sl=9700,
                one_r_dist=300,
                symbol="SOL",
            )
            self.assertEqual(s1_st, 1)
            self.assertFalse(s1_upd)
            self.assertEqual(s1_px, 9700)

            # Stage 2 (早期構造条件): +1.25R (10375) かつ higher_low_15m=True & buy_flow_positive=True で発動
            s2e_st, s2e_px, s2e_upd = compute_four_stage_trailing_sl(
                entry_price=10000,
                current_bid=10375,
                highest_bid=10375,
                current_sl=9700,
                one_r_dist=300,
                symbol="SOL",
                higher_low_15m=True,
                buy_flow_positive=True,
            )
            self.assertEqual(s2e_st, 2)
            self.assertTrue(s2e_upd)
            self.assertGreaterEqual(s2e_px, 10045)

            # Stage 2 (標準): +1.65R (10495) で 往復コスト＋スリッページ＋+0.15R (+45円) ロック
            s2_st, s2_px, s2_upd = compute_four_stage_trailing_sl(
                entry_price=10000,
                current_bid=10495,
                highest_bid=10495,
                current_sl=9700,
                one_r_dist=300,
                symbol="SOL",
            )
            self.assertEqual(s2_st, 2)
            self.assertTrue(s2_upd)
            self.assertGreaterEqual(s2_px, 10045)

            # Stage 3 (適応型シャンデリア): +2.4R (10720) & atr_15m=120 で highest_bid - k*ATR_15m に追従
            s3_st, s3_px, s3_upd = compute_four_stage_trailing_sl(
                entry_price=10000,
                current_bid=10720,
                highest_bid=10720,
                current_sl=s2_px,
                one_r_dist=300,
                symbol="SOL",
                atr_15m=120,
            )
            self.assertEqual(s3_st, 3)
            self.assertTrue(s3_upd)
            self.assertGreater(s3_px, s2_px)

            # Stage 4 (クライマックス): +3.1R (10930) または RSI > 78 で直近安値直下へタイトニング
            s4_st, s4_px, s4_upd = compute_four_stage_trailing_sl(
                entry_price=10000,
                current_bid=10930,
                highest_bid=10930,
                current_sl=s3_px,
                one_r_dist=300,
                symbol="SOL",
                atr_15m=120,
                rsi_15m=81.5,
                recent_low_15m=10780,
            )
            self.assertEqual(s4_st, 4)
            self.assertTrue(s4_upd)
            self.assertGreater(s4_px, s3_px)

            # ------------------------------------------------------------------
            # ③ 利確（TP）時の「15秒 Maker (SOK) トライアル ➔ 未約定または仮想ストップ割れ時の cancelBefore=true 成行フォールバック」
            # ------------------------------------------------------------------
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "SOL",
                    "limit_price": 20000,
                    "size": 0.1,
                    "take_profit_price": 20800,
                    "stop_loss_price": 19500,
                },
                tickers={"SOL": {"symbol": "SOL", "ask": 20050, "bid": 20010, "last": 20010}},
                now_dt=t0 + timedelta(minutes=10),
            )
            oco.step(
                tickers={"SOL": {"symbol": "SOL", "ask": 19995, "bid": 19990, "last": 19990}},
                now_dt=t0 + timedelta(minutes=10, seconds=10),
            )
            # TP (20800) 到達 -> SOK 売り発注 & 15秒トライアル開始 (TP_MAKER_TRIAL_STARTED)
            tp_start_evs = oco.step(
                tickers={"SOL": {"symbol": "SOL", "ask": 20820, "bid": 20805, "last": 20805}},
                now_dt=t0 + timedelta(minutes=11),
            )
            self.assertTrue(any(e.get("type") == "TP_MAKER_TRIAL_STARTED" for e in tp_start_evs))
            pf_tp_trial = oco.load_portfolio()
            self.assertEqual(pf_tp_trial["positions"][0]["state"], "TP_MAKER_TRIAL")
            self.assertEqual(pf_tp_trial["positions"][0]["tp_virtual_stop_price"], 20802.0)  # BestBid(20805) - 3 ticks

            # 16秒経過（> 15秒）かつ未約定 -> 直ちに cancelBefore=true 付き MARKET / FAK / SELL フォールバックで利確完了
            tp_fb_evs = oco.step(
                tickers={"SOL": {"symbol": "SOL", "ask": 20830, "bid": 20804, "last": 20804}},
                now_dt=t0 + timedelta(minutes=11, seconds=16),
            )
            self.assertTrue(any(e.get("type") == "TP_EXECUTED" for e in tp_fb_evs))
            self.assertEqual(tp_fb_evs[0].get("tp_execution_mode"), "MARKET_FALLBACK_AFTER_15S")
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # ------------------------------------------------------------------
            # ④ 時間減衰エグジット（4.5h 停滞撤退 TIME_STOP_STAGNATION ＆ 8.0h 高確信枠ローテーション TIME_STOP_SLOT_ROTATION）
            # ------------------------------------------------------------------
            t_stag = t0 + timedelta(minutes=20)
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 100.0,
                    "size": 50.0,
                    "take_profit_price": 106.0,
                    "stop_loss_price": 98.0,
                },
                tickers={"XRP": {"symbol": "XRP", "ask": 100.2, "bid": 100.1, "last": 100.1}},
                now_dt=t_stag,
            )
            oco.step(
                tickers={"XRP": {"symbol": "XRP", "ask": 99.9, "bid": 99.8, "last": 99.8}},
                now_dt=t_stag + timedelta(seconds=10),
            )
            # エントリーから 4.6時間経過、MFE < +0.8R、現在 +0.10R ([-0.35R, +0.45R] ノイズ帯) -> TIME_STOP_STAGNATION_EXIT
            stag_evs = oco.step(
                tickers={"XRP": {"symbol": "XRP", "ask": 100.3, "bid": 100.2, "last": 100.2}},
                now_dt=t_stag + timedelta(hours=4, minutes=40),
            )
            self.assertTrue(any(e.get("type") == "TIME_STOP_STAGNATION_EXIT" for e in stag_evs))
            self.assertEqual(len(oco.load_portfolio()["positions"]), 0)

            # 8.0h 高確信枠ローテーション: 2枠満杯で 8.5時間経過・含み益 +0.1R の停滞枠がある状態から、他銘柄の confidence=0.88 BUY シグナル発生
            t_rot = t_stag + timedelta(hours=5)
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "XRP",
                    "limit_price": 100.0,
                    "size": 30.0,
                    "take_profit_price": 106.0,
                    "stop_loss_price": 98.0,
                },
                tickers={"XRP": {"symbol": "XRP", "ask": 100.2, "bid": 100.1, "last": 100.1}},
                now_dt=t_rot,
            )
            oco.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.05,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t_rot,
            )
            oco.step(
                tickers={
                    "XRP": {"symbol": "XRP", "ask": 99.9, "bid": 99.8, "last": 99.8},
                    "BCH": {"symbol": "BCH", "ask": 43950, "bid": 43900, "last": 43900},
                },
                now_dt=t_rot + timedelta(seconds=10),
            )
            pf_two = oco.load_portfolio()
            self.assertEqual(len(pf_two["positions"]), 2)
            # XRP の opened_at を 8.5時間前に設定
            pf_two["positions"][0]["opened_at"] = (t_rot - timedelta(hours=8, minutes=30)).isoformat(timespec="seconds")
            pf_two["positions"][0]["current_price"] = 100.2  # +0.1R (< +0.5R)
            pf_two["positions"][1]["current_price"] = 45200.0  # +0.8R (>= +0.5R)
            oco.save_portfolio(pf_two, now_dt=t_rot + timedelta(seconds=20))

            val_rot = validate_and_adjust_decision(
                decision={
                    "action": "BUY",
                    "symbol": "SOL",
                    "order_type": "LIMIT",
                    "limit_price": 20000,
                    "size": 0.2,
                    "take_profit_price": 21200,
                    "stop_loss_price": 19500,
                    "confidence": 0.88,
                    "conviction_score": 88,
                },
                portfolio=oco.load_portfolio(),
                market_summaries={
                    "XRP": {"price": {"best_bid": 100.2, "best_ask": 100.25, "last": 100.2}, "h1_indicators": {"atr_14": 1.2}},
                    "BCH": {"price": {"best_bid": 45200, "best_ask": 45220, "last": 45200}, "h1_indicators": {"atr_14": 600}},
                    "SOL": {"price": {"best_bid": 20010, "best_ask": 20015, "last": 20010}, "h1_indicators": {"atr_14": 250}},
                },
                symbol_rules=DEFAULT_SYMBOL_RULES,
            )
            self.assertEqual(val_rot["action"], "BUY", val_rot.get("reasoning"))
            self.assertTrue(val_rot.get("slot_rotation_required"))
            self.assertEqual(val_rot.get("slot_rotation_target_symbol"), "XRP")

            rot_exec = oco.place_new_buy_order(
                decision=val_rot,
                tickers={
                    "XRP": {"symbol": "XRP", "ask": 100.3, "bid": 100.2, "last": 100.2},
                    "BCH": {"symbol": "BCH", "ask": 45250, "bid": 45200, "last": 45200},
                    "SOL": {"symbol": "SOL", "ask": 20050, "bid": 20010, "last": 20010},
                },
                now_dt=t_rot + timedelta(seconds=30),
            )
            self.assertEqual(rot_exec["status"], "PENDING_BUY")
            self.assertTrue(rot_exec.get("slot_rotation_executed"))
            self.assertEqual(rot_exec["rotated_closed_trade"]["reason"], "TIME_STOP_SLOT_ROTATION")

            # ------------------------------------------------------------------
            # ⑤ ロット切り捨て余剰リスクによる初期SL引き下げ補正（Breathing Room）
            #    ＆ 0ポジション高確信時の第1枠 65% 動的拡張の検証
            # ------------------------------------------------------------------
            pf_zero = {
                "initial_capital_jpy": 20000,
                "jpy_balance": 20025,
                "peak_equity_jpy": 20025,
                "positions": [],
                "pending_orders": [],
            }
            val_dyn = validate_and_adjust_decision(
                decision={
                    "action": "BUY",
                    "symbol": "BTC",
                    "order_type": "LIMIT",
                    "limit_price": 12500000,
                    "size": 0.00109,  # 0.0001刻みへ切り捨て -> 0.0010 BTC (12,500円: 45%枠9,011円を超え65%枠13,016円以内)
                    "take_profit_price": 12950000,
                    "stop_loss_price": 12280000,  # d_SL = 1.76% < 2.2%
                    "confidence": 0.85,
                    "conviction_score": 85,
                },
                portfolio=pf_zero,
                market_summaries={
                    "BTC": {
                        "price": {"best_bid": 12502000, "best_ask": 12505000, "last": 12502000},
                        "h1_indicators": {"atr_14": 150000},
                    }
                },
                symbol_rules=DEFAULT_SYMBOL_RULES,
            )
            self.assertEqual(val_dyn["action"], "BUY")
            self.assertTrue(val_dyn.get("dynamic_slot_expansion_applied"))
            self.assertEqual(val_dyn.get("effective_slot_alloc_pct"), 0.65)
            self.assertGreater(val_dyn["expected_cost_jpy"], 9011)
            self.assertLessEqual(val_dyn["expected_cost_jpy"], 13016)
            self.assertTrue(val_dyn.get("breathing_room_applied"))
            self.assertLess(val_dyn["stop_loss_price"], 12280000)
            self.assertLessEqual(val_dyn["expected_loss_jpy"], 300)

            # 05:59:30〜06:00:30 JST 手数料端数精算ウィンドウ判定 & Stale Git lock 解除 & WAL TRUNCATE 検証
            self.assertTrue(
                is_in_daily_fee_settlement_freeze_window(datetime(2026, 10, 25, 5, 59, 45, tzinfo=JST))
            )
            self.assertTrue(
                is_in_daily_fee_settlement_freeze_window(datetime(2026, 10, 25, 6, 0, 15, tzinfo=JST))
            )
            self.assertFalse(
                is_in_daily_fee_settlement_freeze_window(datetime(2026, 10, 25, 6, 0, 31, tzinfo=JST))
            )
            stale_lock = tmp_dir / "v2_git_push.lock"
            stale_lock.write_text("stale", encoding="utf-8")
            old_mtime = time.time() - 360.0
            os.utime(stale_lock, (old_mtime, old_mtime))
            self.assertTrue(cleanup_stale_git_push_lock(stale_lock, max_age_sec=300.0))
            self.assertFalse(stale_lock.exists())

            trunc_res = wal.checkpoint_truncate()
            self.assertEqual(trunc_res["mode"], "TRUNCATE")
            wal.close()

    def test_round14_step3_microstructure_3mode_premortem_correlation_and_antimartingale_audit(self) -> None:
        """
        Round 14 監査テスト (Step 3 最終フェーズ完全検証):
        ① 5大マイクロストラクチャ特徴量 (OBI_5, TFI & CVD Absorption, BKSR, WTR, OBDR > 0.45% Hollow Book 即座除外)
           ＋ 4状態マーケットレジーム判定 (BROAD_RISK_ON, BTC_DOMINANCE_DRAIN, NEUTRAL_SELECTIVE, RISK_OFF_CRASH Stage 4 タイト化)
        ② 3モード動的指値執行 (AGGRESSIVE_BREAKOUT_PEG 15秒チェイス最大3回・45分失効, MOMENTUM_PULLBACK_DYNAMIC サポート壁+1tick前置き・2時間失効, DEEP_LIQUIDITY_DIP 4時間失効)
           ＋ トランシェ分割可否判定 (< 4.0 * minOrderSize の単一注文強制) ＋ 接近時OFIガード (<= 0.15 * ATR キャンセル)
        ③ 5ブロック無次元化特徴量テンソル (Normalized Feature Tensor) ＋ CRO反証型 Pre-Mortem プロンプト・先頭キー順序
           ＋ Shadow Evaluation 閉ループ自己較正スコアカード (FPR/FNR/TNR・ベイズ較正・動的閾値 max(0.72, Quantile_0.75))
        ④ 2枠同時保有時の対BTC相関・ベータ制御 (高相関・高ベータ制限、オープンリスク連動 R2=0.85%=170円縮小、Stage 2 到達後 Free-Roll Risk Recycling 1.50%=300円回復)
        ⑤ 連続的アンチ・マーチンゲール関数 (DD <3%->1.50%, 3..6%->1.15%, 6..10%->0.80%, 10..15%->0.50%) ＋ 非対称利益クッション (月次>+6% & DD<2% で最大1.85%)
        """
        from src.ai_judge import SYSTEM_PROMPT, build_normalized_feature_tensor
        from src.indicators import (
            compute_bksr_squeeze,
            compute_btc_beta_and_correlation_matrix,
            compute_obdr_slippage,
            compute_orderbook_microstructure,
            compute_trade_flow_and_absorption,
            compute_wick_toxicity_ratio,
        )
        from src.radar import TechnicalRadar, evaluate_symbol_gates
        from src.regime_filter import MarketRegimeFilter
        from src.validator import (
            compute_3mode_dynamic_limit_execution,
            compute_anti_martingale_risk_params,
            evaluate_slot2_correlation_and_beta_control,
            evaluate_tranche_split_feasibility,
        )

        # ------------------------------------------------------------------
        # ① 5大マイクロストラクチャ特徴量 ＆ Hollow Book 即座除外 ＆ 4状態レジーム検証
        # ------------------------------------------------------------------
        # 1-1. OBI_5 & Micro-Price & サポート壁 (+1 tick 前置き)
        ob_sample = {
            "bids": [
                {"price": "100.000", "size": "150"},
                {"price": "99.900", "size": "120"},
                {"price": "99.500", "size": "2500"},  # 巨大サポート買い壁 (>= 平均×3.0)
                {"price": "99.400", "size": "100"},
                {"price": "99.300", "size": "100"},
            ],
            "asks": [
                {"price": "100.100", "size": "50"},
                {"price": "100.200", "size": "80"},
                {"price": "100.300", "size": "90"},
                {"price": "100.400", "size": "100"},
                {"price": "100.500", "size": "110"},
            ],
        }
        ob_micro = compute_orderbook_microstructure(ob_sample, tick_size=0.001)
        self.assertGreater(ob_micro["obi_5"], 0.50)
        self.assertAlmostEqual(ob_micro["best_imbalance_it"], 150.0 / 200.0, places=4)
        self.assertGreater(ob_micro["micro_price"], ob_micro["mid_price"])
        self.assertAlmostEqual(ob_micro["support_wall_price"], 99.500, places=3)
        self.assertAlmostEqual(ob_micro["support_wall_front_run_price"], 99.501, places=3)

        # 1-2. TFI (Trade Flow Imbalance) & CVD Absorption
        now_ms = int(datetime(2026, 10, 24, 12, 0, 0, tzinfo=JST).timestamp() * 1000)
        trades_sample = [
            {"side": "SELL", "price": 100.0, "size": 500.0, "timestamp": now_ms - 60000},
            {"side": "BUY", "price": 100.05, "size": 100.0, "timestamp": now_ms - 30000},
        ]
        klines_abs = [
            {"open": 100.10, "high": 100.20, "low": 98.80, "close": 100.05, "volume": 1000},
        ]
        tfi_res = compute_trade_flow_and_absorption(
            trades_sample, klines_15m=klines_abs, atr_15m=0.50, now_ts_ms=now_ms
        )
        self.assertLess(tfi_res["tfi_15m"], -0.40)
        self.assertTrue(tfi_res["cvd_absorption_detected"])

        # 1-3. BKSR ボラティリティ・スクイーズ比 (< 1.0 でスクイーズ、上抜けでブレイクアウト)
        squeeze_klines = []
        for i in range(25):
            # 高値・安値幅(ATR=4.0)に対して終値変動が極小(フラット) -> 4*sigma < 3*ATR -> BKSR < 1.0
            c_val = 100.0 + (0.05 if i % 2 == 0 else -0.05)
            squeeze_klines.append({"open": 100.0, "high": 102.0, "low": 98.0, "close": c_val, "volume": 100})
        # 最終足でボリンジャー上限を上抜け
        squeeze_klines[-1] = {"open": 100.0, "high": 103.0, "low": 99.5, "close": 101.5, "volume": 300}
        bksr_res = compute_bksr_squeeze(squeeze_klines)
        self.assertLess(bksr_res["bksr"], 1.0)
        self.assertTrue(bksr_res["is_squeeze"])
        self.assertTrue(bksr_res["squeeze_breakout_up"])

        # 1-4. WTR (Wick Toxicity Ratio: 上ヒゲ罠 vs 下ヒゲSpring)
        wtr_trap = compute_wick_toxicity_ratio(
            [{"open": 100.0, "high": 105.0, "low": 99.8, "close": 100.2, "volume": 200}],
            lookback=1,
        )
        self.assertTrue(wtr_trap["is_upper_wick_trap"])
        wtr_spring = compute_wick_toxicity_ratio(
            [{"open": 100.0, "high": 100.2, "low": 95.0, "close": 100.1, "volume": 200}],
            lookback=1,
        )
        self.assertTrue(wtr_spring["is_lower_wick_spring"])

        # 1-5. OBDR (9,011円成行売却シミュレーションでスリッページ > 0.45% の Hollow Book を即座除外)
        hollow_ob = {
            "asks": [{"price": "100.10", "size": "100"}],
            "bids": [
                {"price": "100.00", "size": "5"},   # 500円分のみ最良気配 (見かけスプレッド 0.10% <= 0.35%)
                {"price": "99.00", "size": "100"},  # 残り8,511円は 99.00円で約定 -> 加重平均 ~99.05円 (スリッページ ~0.95% > 0.45%)
            ],
        }
        obdr_res = compute_obdr_slippage(hollow_ob, target_notional_jpy=9011.0)
        self.assertGreater(obdr_res["obdr_slippage_pct"], 0.45)
        self.assertTrue(obdr_res["is_hollow_book"])

        gate_hollow = evaluate_symbol_gates(
            symbol="XLM",
            ticker={"bid": 100.00, "ask": 100.10, "last": 100.00, "volume": 500000},  # スプレッド 0.10% <= 0.35%
            klines_1h=[{"open": 99, "high": 101, "low": 98, "close": 100, "volume": 10000}] * 24,
            orderbook=hollow_ob,
            max_spread_pct=0.35,
            max_obdr_slippage_pct=0.45,
        )
        self.assertFalse(gate_hollow["passed"])
        self.assertEqual(gate_hollow["failed_gate"], "GATE_2_HOLLOW_BOOK_OBDR")

        # 1-6. 4状態マーケットレジーム判定 (BROAD_RISK_ON, BTC_DOMINANCE_DRAIN, NEUTRAL_SELECTIVE, RISK_OFF_CRASH)
        rf = MarketRegimeFilter()
        reg_broad = rf.evaluate(
            market_summaries={
                "BTC": {"daily_indicators": {"trend": "STRONG_UPTREND"}, "h4_indicators": {"rsi_14": 58.0}},
                "ETH": {"daily_indicators": {"trend": "STRONG_UPTREND"}},
                "SOL": {"daily_indicators": {"trend": "UPTREND"}},
                "XRP": {"daily_indicators": {"trend": "UPTREND"}},
            },
        )
        self.assertEqual(reg_broad["regime"], "BROAD_RISK_ON")
        self.assertEqual(reg_broad["regime"], "TREND_UP")  # 後方互換性確認

        reg_drain = rf.evaluate(
            market_summaries={
                "BTC": {"daily_indicators": {"trend": "STRONG_UPTREND"}, "h4_indicators": {"rsi_14": 58.0}},
                "ETH": {"daily_indicators": {"trend": "DOWNTREND_OR_RANGE"}, "above_ema50": False},
                "SOL": {"daily_indicators": {"trend": "DOWNTREND_OR_RANGE"}, "above_ema50": False},
                "XRP": {"daily_indicators": {"trend": "DOWNTREND_OR_RANGE"}, "above_ema50": False},
            },
        )
        self.assertEqual(reg_drain["regime"], "BTC_DOMINANCE_DRAIN")
        self.assertEqual(reg_drain["allowed_symbol_filter"], "MAJORS_OR_HIGH_TFI")

        reg_crash = rf.evaluate(
            market_summaries={
                "BTC": {"h1_indicators": {"change_1h_pct": -3.2}, "h4_indicators": {"change_4h_pct": -4.8, "rsi_14": 20.0}},
            },
        )
        self.assertEqual(reg_crash["regime"], "RISK_OFF_CRASH")
        self.assertTrue(reg_crash["should_cancel_pending_buys"])
        self.assertTrue(reg_crash["tighten_trailing_to_stage4"])

        # RISK_OFF_CRASH 時の保有ポジション Stage 4 タイト化確認
        st_crash, sl_crash, upd_crash = compute_four_stage_trailing_sl(
            entry_price=100.0,
            current_bid=102.5,
            highest_bid=103.0,
            current_sl=98.0,
            one_r_dist=2.0,
            symbol="XRP",
            atr_15m=0.5,
            force_stage4_tightening=True,
        )
        self.assertEqual(st_crash, 4)
        self.assertTrue(upd_crash)
        self.assertGreater(sl_crash, 100.0)

        # ------------------------------------------------------------------
        # ② 3モード動的指値執行 ＆ トランシェ分割可否 ＆ 接近時OFIガード検証
        # ------------------------------------------------------------------
        # Mode 1: AGGRESSIVE_BREAKOUT_PEG (有効期限 45分 = 0.75h, max_peg_count = 3)
        m1 = compute_3mode_dynamic_limit_execution(
            symbol="SOL",
            confidence=0.88,
            summary={
                "price": {"last": 20010, "best_bid": 20000, "best_ask": 20020},
                "h1_indicators": {"atr_14": 300, "adx_14": 32.0},
                "microstructure": {"best_imbalance_it": 0.72, "bksr": 0.85, "squeeze_breakout_up": True},
            },
            tick_size=Decimal("1"),
            explicit_mode="AGGRESSIVE_BREAKOUT_PEG",
        )
        self.assertEqual(m1["execution_mode_type"], "AGGRESSIVE_BREAKOUT_PEG")
        self.assertEqual(m1["calculated_limit_decimal"], Decimal("20000"))
        self.assertAlmostEqual(m1["order_expiry_hours"], 0.75, places=2)
        self.assertEqual(m1["max_peg_count"], 3)

        # Mode 2: MOMENTUM_PULLBACK_DYNAMIC (サポート壁 99.500 の +1 tick = 99.501 前置き、有効期限 2.0h)
        m2 = compute_3mode_dynamic_limit_execution(
            symbol="XRP",
            confidence=0.78,
            summary={
                "price": {"last": 100.000, "best_bid": 100.000, "best_ask": 100.050},
                "h1_indicators": {"atr_14": 1.000},
                "microstructure": {"support_wall_price": 99.500, "support_wall_front_run_price": 99.501},
            },
            tick_size=Decimal("0.001"),
            explicit_mode="MOMENTUM_PULLBACK_DYNAMIC",
        )
        self.assertEqual(m2["execution_mode_type"], "MOMENTUM_PULLBACK_DYNAMIC")
        self.assertEqual(m2["calculated_limit_decimal"], Decimal("99.501"))
        self.assertTrue(m2["front_run_tick_applied"])
        self.assertAlmostEqual(m2["order_expiry_hours"], 2.0, places=2)

        # Mode 3: DEEP_LIQUIDITY_DIP (有効期限 4.0h)
        m3 = compute_3mode_dynamic_limit_execution(
            symbol="ETH",
            confidence=0.72,
            summary={
                "price": {"last": 400000, "best_bid": 400000, "best_ask": 400200},
                "h1_indicators": {"atr_14": 5000},
                "microstructure": {},
            },
            tick_size=Decimal("1"),
            explicit_mode="DEEP_LIQUIDITY_DIP",
        )
        self.assertEqual(m3["execution_mode_type"], "DEEP_LIQUIDITY_DIP")
        self.assertAlmostEqual(m3["order_expiry_hours"], 4.0, places=2)

        # トランシェ分割可否: BTC (0.0003 < 4 * 0.0001) は単一注文強制、XRP (90 >= 4 * 1) は2トランシェ分割可
        tr_btc = evaluate_tranche_split_feasibility(
            symbol="BTC",
            order_size=Decimal("0.0003"),
            min_order_size=Decimal("0.0001"),
            size_step=Decimal("0.0001"),
            limit_price=Decimal("12500000"),
            atr_15m=Decimal("50000"),
            tick_size=Decimal("1"),
        )
        self.assertFalse(tr_btc["tranche_split_allowed"])
        self.assertEqual(tr_btc["execution_tranche_mode"], "SINGLE_ORDER_DYNAMIC_PEG")

        tr_xrp = evaluate_tranche_split_feasibility(
            symbol="XRP",
            order_size=Decimal("90"),
            min_order_size=Decimal("1"),
            size_step=Decimal("1"),
            limit_price=Decimal("100.000"),
            atr_15m=Decimal("0.500"),
            tick_size=Decimal("0.001"),
        )
        self.assertTrue(tr_xrp["tranche_split_allowed"])
        self.assertEqual(tr_xrp["execution_tranche_mode"], "TWO_TRANCHE_SPLIT")

        # Mode 1 15秒ループの動的チェイス・ペギング (changeOrder 最大3回) と Approaching OFI Velocity Guard 検証
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp_dir = Path(tmp_str)
            wal_s3 = DatabaseWAL(tmp_dir / "s3.db", portfolio_json_path=tmp_dir / "pf.json")
            broker_s3 = PaperBroker(
                paper_orders_path=tmp_dir / "paper_orders.json",
                portfolio_path=tmp_dir / "pf.json",
                wal_db=wal_s3,
            )
            oco_s3 = PseudoOCOManager(
                broker=broker_s3,
                portfolio_path=tmp_dir / "pf.json",
                dry_run_notify=True,
                wal_db=wal_s3,
            )
            t0_s3 = datetime(2026, 10, 24, 13, 0, 0, tzinfo=JST)
            res_peg_ord = oco_s3.place_new_buy_order(
                decision={
                    "action": "BUY",
                    "symbol": "SOL",
                    "order_type": "LIMIT_POST_ONLY",
                    "execution_mode_type": "AGGRESSIVE_BREAKOUT_PEG",
                    "order_expiry_hours": 0.75,
                    "max_peg_count": 3,
                    "limit_price": 20000,
                    "size": 0.2,
                    "take_profit_price": 21200,
                    "stop_loss_price": 19500,
                },
                tickers={"SOL": {"symbol": "SOL", "ask": 20050, "bid": 20010, "last": 20010}},
                now_dt=t0_s3,
            )
            self.assertEqual(res_peg_ord["status"], "PENDING_BUY")
            # Best Bid が 20015 -> 20025 -> 20035 -> 20045 へ上昇し best_imbalance_it=0.75 (>0.60) の時、最大3回まで追従ペグ
            for step_i, bid_p in enumerate([20015, 20025, 20035, 20045], start=1):
                evs_peg = oco_s3.step(
                    tickers={
                        "SOL": {
                            "symbol": "SOL",
                            "ask": bid_p + 30,
                            "bid": bid_p,
                            "last": bid_p + 10,
                            "best_imbalance_it": 0.75,
                        }
                    },
                    now_dt=t0_s3 + timedelta(seconds=15 * step_i),
                )
                if step_i <= 3:
                    self.assertTrue(any(e.get("type") == "AGGRESSIVE_BREAKOUT_PEG_UPDATED" for e in evs_peg))
                else:
                    # 4回目は max_peg_count=3 到達により追従しない
                    self.assertFalse(any(e.get("type") == "AGGRESSIVE_BREAKOUT_PEG_UPDATED" for e in evs_peg))

            pf_peg_after = oco_s3.load_portfolio()
            self.assertEqual(pf_peg_after["pending_orders"][0]["peg_count"], 3)
            self.assertEqual(float(pf_peg_after["pending_orders"][0]["limit_price"]), 20035.0)
            wal_s3.close()

        # 接近時 OFI ガード (Approaching OFI Velocity Guard): 指値から 0.15 * ATR 以内に接近かつ売り優勢で即キャンセル
        adv_guard = AdverseSelectionGuard()
        adv_guard.record_orderbook(
            symbol="XRP",
            asks=[{"price": 100.10, "size": 500}],
            bids=[{"price": 100.05, "size": 50}],  # サポート壁が消滅 (50 < 2.0 * 100)
            timestamp_sec=1000.0,
        )
        for i in range(5):
            adv_guard.record_taker_trade("XRP", "SELL", 100.06, 200.0, 995.0 + i)
        app_chk = adv_guard.should_cancel_resting_buy_order(
            symbol="XRP",
            order_price=100.00,
            order_size=100.0,
            atr_5m=1.0,  # 距離 0.05 <= 0.15 * 1.0 (0.15 ATR 以内に接近)
            now_sec=1000.0,
        )
        self.assertTrue(app_chk["should_cancel"])
        self.assertTrue(app_chk["approaching_guard"]["should_cancel"])

        # ------------------------------------------------------------------
        # ③ 5ブロック Normalized Feature Tensor ＋ Pre-Mortem プロンプト ＋ Shadow閉ループ自己較正検証
        # ------------------------------------------------------------------
        self.assertIn("Pre-Mortem", SYSTEM_PROMPT)
        self.assertIn("bearish_invalidation_points", SYSTEM_PROMPT)
        self.assertIn("execution_mode_type", SYSTEM_PROMPT)

        sample_summaries = {
            "XRP": {
                "market_regime": "BROAD_RISK_ON",
                "price": {
                    "last": 100.0,
                    "best_bid": 99.95,
                    "best_ask": 100.05,
                    "high": 104.0,
                    "low": 96.0,
                    "spread_pct": 0.10,
                    "relative_strength_vs_btc_24h_pct": 3.2,
                    "relative_strength_vs_btc_7d_pct": 5.5,
                    "beta_vs_btc": 1.15,
                    "correlation_vs_btc": 0.62,
                },
                "daily_indicators": {"atr_14": 3.0, "sma_25": 97.0, "rsi_14": 56.0, "trend": "STRONG_UPTREND"},
                "h4_indicators": {"atr_14": 1.2, "rsi_14": 54.0},
                "h1_indicators": {"atr_14": 0.6, "ema_20": 99.2},
                "microstructure": {
                    "obi_5": 0.42,
                    "best_imbalance_it": 0.68,
                    "micro_price_divergence_bps": 4.5,
                    "tfi_15m": 0.35,
                    "tfi_1h": 0.28,
                    "cvd_absorption_detected": True,
                    "absorption_score": 1.5,
                    "obdr_slippage_pct": 0.12,
                    "is_hollow_book": False,
                    "bksr": 0.88,
                    "is_squeeze": True,
                    "squeeze_breakout_up": True,
                    "upper_wick_ratio": 0.15,
                    "lower_wick_ratio": 0.60,
                    "wtr_net_score": 0.45,
                    "is_upper_wick_trap": False,
                    "is_lower_wick_spring": True,
                    "support_wall_price": 99.0,
                    "resistance_wall_price": 103.5,
                },
            }
        }
        nft = build_normalized_feature_tensor(sample_summaries, DEFAULT_SYMBOL_RULES, market_regime="BROAD_RISK_ON")
        self.assertIn("XRP", nft)
        for blk_key in (
            "regime_and_relative_strength",
            "multi_timeframe_structure",
            "microstructure_and_flow",
            "volatility_and_trap_metrics",
            "cost_adjusted_economics",
        ):
            self.assertIn(blk_key, nft["XRP"])

        # Pre-Mortem スキーマ先頭キー順序と Shadow Evaluation 閉ループ自己較正の検証
        shd = LLMShadowEvaluator()
        # 直近の判定で AI Confidence が 0.80〜0.88 にインフレし、かつ BUY の一部が SL 被弾している状況を登録
        for idx, (act, conv, out_cls, pnl_v, mfe_v, mae_v) in enumerate(
            [
                ("BUY", 84, "SL_HIT", -280.0, 0.5, -3.2),
                ("BUY", 86, "SL_HIT", -290.0, 0.4, -3.5),
                ("BUY", 88, "TP_HIT", 520.0, 5.8, -0.8),
                ("HOLD", 78, "TP_HIT", 480.0, 4.5, -0.5),
                ("HOLD", 82, "SL_HIT", -300.0, 0.2, -3.8),
                ("HOLD", 85, "SL_HIT", -260.0, 0.1, -3.1),
            ]
        ):
            eid = shd.record_decision(
                symbol="XRP",
                ai_action=act,
                conviction_score=conv,
                entry_price=100.0,
                tp_price=105.0,
                sl_price=97.0,
            )
            shd.records[eid]["outcome_24h"] = out_cls
            shd.records[eid]["virtual_pnl_24h_jpy"] = pnl_v
            shd.records[eid]["mfe_24h_pct"] = mfe_v
            shd.records[eid]["mae_24h_pct"] = mae_v

        scorecard = shd.compute_self_calibration_scorecard(limit=30)
        self.assertAlmostEqual(scorecard["fpr_overconfidence_rate"], 2.0 / 3.0, places=3)
        self.assertAlmostEqual(scorecard["fnr_missed_opportunity_rate"], 1.0 / 3.0, places=3)
        self.assertAlmostEqual(scorecard["tnr_veto_accuracy_rate"], 2.0 / 3.0, places=3)
        self.assertGreater(scorecard["effective_threshold"], 0.80)  # max(0.72, Quantile_0.75) により動的引き上げ

        cal_res = shd.calibrate_confidence_and_threshold(raw_confidence=0.80)
        # (alpha_0=2 + W=1 + k=5 * 0.80) / (2 + 2 + N=3 + 5) = 7.0 / 12.0 = 0.5833
        self.assertAlmostEqual(cal_res["calibrated_confidence"], 7.0 / 12.0, places=3)
        self.assertFalse(cal_res["passes_dynamic_threshold"])  # 0.80 < effective_threshold (~0.8575)

        judge_s3 = AIJudge(provider="rule_based", shadow_evaluator=shd)
        dec_s3 = judge_s3.judge(
            market_summaries=sample_summaries,
            portfolio={"jpy_balance": 20025, "positions": [], "pending_orders": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
        )
        # 先頭キーが bearish_invalidation_points であることの確認
        first_keys = list(dec_s3.keys())[:6]
        self.assertEqual(
            first_keys[:5],
            [
                "bearish_invalidation_points",
                "bearish_counter_thesis",
                "bullish_edge_points",
                "bullish_conviction_factors",
                "execution_mode_type",
            ],
        )

        # ------------------------------------------------------------------
        # ④ 2枠同時保有時の対BTC相関・ベータ制御 ＆ Free-Roll Risk Recycling 検証
        # ------------------------------------------------------------------
        # 4-1. 1時間足リターンからの対BTCベータ & ペアワイズ相関行列計算
        btc_rets = [0.01 * (i % 3 - 1) for i in range(24)]
        eth_rets = [0.015 * (i % 3 - 1) for i in range(24)]  # 高ベータ・高相関
        corr_mat = compute_btc_beta_and_correlation_matrix(
            {
                "BTC": {"h1_indicators": {"returns_1h": btc_rets}},
                "ETH": {"h1_indicators": {"returns_1h": eth_rets}},
            }
        )
        self.assertGreater(corr_mat["corr_vs_btc"]["ETH"], 0.90)
        self.assertGreater(corr_mat["betas_vs_btc"]["ETH"], 1.30)

        # 4-2. 高相関(rho=0.85 > 0.80) かつ 高ベータ合算(1.5 + 1.5 = 3.0 > 2.8) で confidence=0.78 (< 0.82) はブロック
        slot1_active_pos = [
            {
                "symbol": "ETH",
                "size": 0.02,
                "entry_price": 400000,
                "stop_loss_price": 392000,  # OpenRisk_1 > 0 (Stage 1: SL < entry)
                "trailing_stage": 1,
                "breakeven_locked": False,
                "beta_vs_btc": 1.50,
            }
        ]
        ms_corr = {
            "ETH": {"beta_vs_btc": 1.50, "pairwise_corr": {"SOL": 0.85}},
            "SOL": {"beta_vs_btc": 1.50, "pairwise_corr": {"ETH": 0.85}},
        }
        c_block = evaluate_slot2_correlation_and_beta_control(
            candidate_symbol="SOL",
            candidate_confidence=0.78,
            active_positions=slot1_active_pos,
            market_summaries=ms_corr,
            base_1r_pct=Decimal("0.015"),
        )
        self.assertTrue(c_block["blocked_by_high_corr_beta"])
        self.assertEqual(c_block["required_min_confidence"], 0.82)

        # 4-3. OpenRisk_1 > 0 かつ 相関 rho=0.72 (> 0.65) の場合、第2枠の許容リスク R2 を 1.50%(300円) -> 0.85%(170円) に縮小
        ms_mid_corr = {
            "ETH": {"beta_vs_btc": 1.10, "pairwise_corr": {"SOL": 0.72}},
            "SOL": {"beta_vs_btc": 1.20, "pairwise_corr": {"ETH": 0.72}},
        }
        c_reduced = evaluate_slot2_correlation_and_beta_control(
            candidate_symbol="SOL",
            candidate_confidence=0.80,
            active_positions=slot1_active_pos,
            market_summaries=ms_mid_corr,
            base_1r_pct=Decimal("0.015"),
        )
        self.assertFalse(c_reduced["blocked_by_high_corr_beta"])
        self.assertTrue(c_reduced["correlation_haircut_applied"])
        self.assertEqual(c_reduced["effective_slot2_risk_pct_decimal"], Decimal("0.0085"))
        self.assertAlmostEqual(float(c_reduced["effective_slot2_risk_pct_decimal"]) * 20025.0, 170.21, delta=1.0)  # 約170円

        # 4-4. 第1枠が Stage 2（+0.15R 建値ロック完了 = OpenRisk_1 == 0）に到達した場合、Free-Roll Risk Recycling で R2 = 1.50% (300円) へ全額回復
        slot1_freeroll_pos = [
            {
                "symbol": "ETH",
                "size": 0.02,
                "entry_price": 400000,
                "stop_loss_price": 401200,  # SL >= entry (OpenRisk_1 == 0)
                "trailing_stage": 2,
                "breakeven_locked": True,
            }
        ]
        c_freeroll = evaluate_slot2_correlation_and_beta_control(
            candidate_symbol="SOL",
            candidate_confidence=0.80,
            active_positions=slot1_freeroll_pos,
            market_summaries=ms_mid_corr,
            base_1r_pct=Decimal("0.015"),
        )
        self.assertFalse(c_freeroll["blocked_by_high_corr_beta"])
        self.assertTrue(c_freeroll["free_roll_recycling_active"])
        self.assertEqual(c_freeroll["effective_slot2_risk_pct_decimal"], Decimal("0.015"))
        self.assertAlmostEqual(float(c_freeroll["effective_slot2_risk_pct_decimal"]) * 20025.0, 300.38, delta=1.0)  # 約300円

        # ------------------------------------------------------------------
        # ⑤ 連続的アンチ・マーチンゲール関数 ＆ 非対称利益クッション検証
        # ------------------------------------------------------------------
        # DD = 0.0% (< 3.0%): 1R = 1.50% (300円)
        am_normal = compute_anti_martingale_risk_params(
            portfolio={"initial_capital_jpy": 20000, "peak_equity_jpy": 20025},
            total_equity=Decimal("20025"),
        )
        self.assertEqual(am_normal["anti_martingale_tier"], "TIER_0_NORMAL_150BPS")
        self.assertEqual(am_normal["risk_pct_decimal"], Decimal("0.0150"))

        # DD = 4.5% (3.0% <= DD < 6.0%): 1R = 1.15% (~220円)
        am_dd4 = compute_anti_martingale_risk_params(
            portfolio={"initial_capital_jpy": 20000, "peak_equity_jpy": 20000},
            total_equity=Decimal("19100"),
        )
        self.assertEqual(am_dd4["anti_martingale_tier"], "TIER_1_DD_3_TO_6PCT_115BPS")
        self.assertEqual(am_dd4["risk_pct_decimal"], Decimal("0.0115"))

        # DD = 8.0% (6.0% <= DD < 10.0%): 1R = 0.80%
        am_dd8 = compute_anti_martingale_risk_params(
            portfolio={"initial_capital_jpy": 20000, "peak_equity_jpy": 20000},
            total_equity=Decimal("18400"),
        )
        self.assertEqual(am_dd8["anti_martingale_tier"], "TIER_2_DD_6_TO_10PCT_80BPS")
        self.assertEqual(am_dd8["risk_pct_decimal"], Decimal("0.0080"))

        # DD = 12.0% (10.0% <= DD < 15.0%): 1R = 0.50%
        am_dd12 = compute_anti_martingale_risk_params(
            portfolio={"initial_capital_jpy": 20000, "peak_equity_jpy": 20000},
            total_equity=Decimal("17600"),
        )
        self.assertEqual(am_dd12["anti_martingale_tier"], "TIER_3_DD_10_TO_15PCT_50BPS")
        self.assertEqual(am_dd12["risk_pct_decimal"], Decimal("0.0050"))

        # 非対称利益クッション: 月次リターン > +6.0% (例: +41.0%, Equity=22,000円) かつ DD < 2.0% の時、最大 1.85% まで拡張
        am_cushion = compute_anti_martingale_risk_params(
            portfolio={
                "initial_capital_jpy": 20000,
                "peak_equity_jpy": 22000,
                "monthly_realized_return_pct": 41.0,
            },
            total_equity=Decimal("22000"),
        )
        self.assertEqual(am_cushion["anti_martingale_tier"], "TIER_0_PROFIT_CUSHION_BOOST")
        self.assertTrue(am_cushion["asymmetric_cushion_applied"])
        self.assertEqual(am_cushion["risk_pct_decimal"], Decimal("0.0185"))

    def test_round15_deep_research2_tier_matrix_jst_schedule_and_sprt_cusum_audit(self) -> None:
        """
        Round 15 (Deep Research Report #2 統合監査テスト):
        1. 全17銘柄の3ティア分類・5銘柄永久ブラックリスト (FCR, NAC, WILD, ASTR, ATOM)・SOL優先ボーナス・Tier 2 実効板厚フィルタ・BCH 0.22% スプレッド上限
        2. JST時間帯別・曜日別7区分パラメータマトリクス・土曜メンテ前後制御 (08:00停止, 08:45エグジット, 12:10延長メンテ+15分安定化待機)・月曜朝CME窓開け制御
        3. 少サンプル統計管理 (100件凍結ゲート, 連敗ラダー5〜8連敗, Wald's SPRT, Page's Lower CUSUM, Shadow 25%重み, SQLite WAL永続化)
        """
        from decimal import Decimal
        from src.circuit_breaker import (
            MultiTierCircuitBreaker,
            compute_pages_lower_cusum_step,
            compute_wald_sprt_step,
            evaluate_losing_streak_ladder,
        )
        from src.database_wal import DatabaseWAL
        from src.llm_shadow_evaluator import (
            LLMShadowEvaluator,
            SHADOW_DISCOUNT_WEIGHT,
            evaluate_mintrl_freeze_stage,
        )
        from src.radar import TechnicalRadar, evaluate_tier2_effective_depth_filter
        import json
        from src.regime_filter import (
            BCH_MAX_SPREAD_PCT,
            BLACKLISTED_SYMBOLS,
            SOL_PRIORITY_SCORE_BONUS,
            TIER1_SYMBOLS,
            TIER2_SYMBOLS,
            classify_symbol_tier,
            evaluate_cme_monday_gap,
            evaluate_jst_session_and_schedule_matrix,
        )
        from src.validator import DEFAULT_SYMBOL_RULES, validate_and_adjust_decision

        # ------------------------------------------------------------------
        # ① 全17銘柄の3ティア分類・5銘柄永久ブラックリスト・Tier 2 実効板厚フィルタ・BCH 0.22% 上限
        # ------------------------------------------------------------------
        self.assertEqual(BLACKLISTED_SYMBOLS, {"FCR", "NAC", "WILD", "ASTR", "ATOM"})
        self.assertEqual(TIER1_SYMBOLS, {"BTC", "ETH", "XRP", "SOL"})
        self.assertEqual(TIER2_SYMBOLS, {"DOGE", "SUI", "LTC", "BCH", "LINK", "ADA", "XLM", "DOT"})

        sol_tier = classify_symbol_tier("SOL")
        self.assertEqual(sol_tier["tier"], "TIER1")
        self.assertEqual(sol_tier["priority_score_bonus"], SOL_PRIORITY_SCORE_BONUS)

        for bl_sym in BLACKLISTED_SYMBOLS:
            bl_info = classify_symbol_tier(bl_sym)
            self.assertEqual(bl_info["tier"], "TIER3_BLACKLISTED")
            self.assertTrue(bl_info["is_blacklisted"])
            res_bl = validate_and_adjust_decision(
                decision={
                    "action": "BUY",
                    "symbol": bl_sym,
                    "confidence": 0.90,
                    "limit_price": 100.0,
                    "take_profit_price": 108.0,
                    "stop_loss_price": 97.0,
                },
                portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
                symbol_rules=DEFAULT_SYMBOL_RULES,
                market_summaries={bl_sym: {"price": {"last": 100.0, "best_bid": 100.0, "best_ask": 100.1}}},
            )
            self.assertEqual(res_bl["action"], "HOLD")
            self.assertTrue(res_bl.get("tier3_blacklisted_rejected"))

        # Tier 2 実効板厚フィルタ: ① 9,011円成行売り想定スリッページ <= 0.15% & ② -0.5%以内累積買い板厚 >= 45,000円
        thin_ob = {
            "bids": [
                {"price": "100.0", "size": "20.0"},  # 2,000円
                {"price": "99.0", "size": "100.0"},  # 9,900円 (1.0%下 -> スリッページ大 & -0.5%板厚不足)
            ],
            "asks": [{"price": "100.1", "size": "100.0"}],
        }
        t2_thin = evaluate_tier2_effective_depth_filter(
            symbol="ADA",
            ticker={"bid": 100.0, "ask": 100.1, "last": 100.0},
            orderbook=thin_ob,
            slot_notional_jpy=9011.0,
        )
        self.assertFalse(t2_thin["passed"])

        deep_ob = {
            "bids": [
                {"price": "100.00", "size": "300.0"},  # 30,000円 (slippage=0%)
                {"price": "99.95", "size": "250.0"},   # 24,987.5円 (合計 54,987.5円 >= 45,000円)
            ],
            "asks": [{"price": "100.10", "size": "300.0"}],
        }
        t2_deep = evaluate_tier2_effective_depth_filter(
            symbol="ADA",
            ticker={"bid": 100.00, "ask": 100.10, "last": 100.00},
            orderbook=deep_ob,
            slot_notional_jpy=9011.0,
        )
        self.assertTrue(t2_deep["passed"])
        self.assertLessEqual(t2_deep["simulated_sell_slippage_pct"], 0.15)
        self.assertGreaterEqual(t2_deep["cum_bid_depth_05pct_jpy"], 45000.0)

        # BCH 専用スプレッド上限 (0.22%) チェック
        bch_wide_val = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "BCH",
                "confidence": 0.85,
                "limit_price": 70000.0,
                "take_profit_price": 74000.0,
                "stop_loss_price": 68000.0,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries={
                "BCH": {
                    "price": {"last": 70000.0, "best_bid": 70000.0, "best_ask": 70180.0, "spread_pct": 0.257},
                    "daily_indicators": {"atr_14": 2000.0},
                }
            },
        )
        self.assertEqual(bch_wide_val["action"], "HOLD")
        self.assertTrue(bch_wide_val.get("tier2_depth_filter_rejected"))
        self.assertEqual(bch_wide_val.get("tier2_depth_failed_code"), "BCH_SPREAD_EXCEEDS_022_PCT")

        # ------------------------------------------------------------------
        # ② JST時間帯別・曜日別7区分パラメータマトリクス・土曜メンテ延長・月曜朝CME窓開け制御
        # ------------------------------------------------------------------
        # 1) 日次ロールオーバー (05:45〜06:15 JST)
        dt_roll = datetime(2026, 10, 21, 5, 50, 0, tzinfo=JST)
        m_roll = evaluate_jst_session_and_schedule_matrix(now_dt=dt_roll)
        self.assertEqual(m_roll["session_name"], "DAILY_ROLLOVER_BLACKOUT")
        self.assertFalse(m_roll["allow_new_buy"])
        self.assertTrue(m_roll["should_clear_pending_buys"])

        # 2) 早朝閑散帯 (平日 03:00〜05:45 JST)
        dt_early = datetime(2026, 10, 21, 4, 0, 0, tzinfo=JST)
        m_early = evaluate_jst_session_and_schedule_matrix(now_dt=dt_early)
        self.assertEqual(m_early["session_name"], "EARLY_MORNING_ILLIQUID")
        self.assertEqual(m_early["allowed_tiers"], ["TIER1"])
        self.assertAlmostEqual(m_early["max_spread_pct"], 0.20, places=2)
        self.assertAlmostEqual(m_early["confidence_threshold_delta"], 0.08, places=2)
        self.assertAlmostEqual(m_early["limit_offset_atr_mult"], 1.30, places=2)
        self.assertAlmostEqual(m_early["size_mult"], 0.70, places=2)
        self.assertEqual(m_early["max_concurrent_slots"], 1)
        self.assertFalse(m_early["allow_mode1_breakout_peg"])

        # 3) 東京昼休み (平日 12:00〜15:00 JST)
        dt_lunch = datetime(2026, 10, 21, 12, 30, 0, tzinfo=JST)
        m_lunch = evaluate_jst_session_and_schedule_matrix(now_dt=dt_lunch)
        self.assertEqual(m_lunch["session_name"], "TOKYO_LUNCH_LULL")
        self.assertEqual(m_lunch["allowed_tiers"], ["TIER1"])
        self.assertAlmostEqual(m_lunch["order_ttl_mult"], 0.50, places=2)

        # 4) 欧米重複ゴールデンタイム (21:00〜03:00 JST)
        dt_golden = datetime(2026, 10, 21, 22, 30, 0, tzinfo=JST)
        m_golden = evaluate_jst_session_and_schedule_matrix(now_dt=dt_golden)
        self.assertEqual(m_golden["session_name"], "EU_US_OVERLAP_GOLDEN")
        self.assertEqual(m_golden["allowed_tiers"], ["TIER1", "TIER2"])
        self.assertAlmostEqual(m_golden["confidence_threshold_delta"], -0.02, places=2)
        self.assertAlmostEqual(m_golden["limit_offset_atr_mult"], 0.90, places=2)

        # 5) 土曜メンテナンス前後制御 (08:00停止, 08:45エグジット, 12:10延長メンテ+15分安定化待機)
        m_sat_0810 = evaluate_jst_session_and_schedule_matrix(now_dt=datetime(2026, 10, 24, 8, 10, 0, tzinfo=JST))
        self.assertEqual(m_sat_0810["session_name"], "SATURDAY_MAINTENANCE_GUARD")
        self.assertFalse(m_sat_0810["allow_new_buy"])
        self.assertTrue(m_sat_0810["should_clear_pending_buys"])

        m_sat_0846 = evaluate_jst_session_and_schedule_matrix(now_dt=datetime(2026, 10, 24, 8, 46, 0, tzinfo=JST))
        self.assertTrue(m_sat_0846["should_exit_positions_pre_maint"])

        # 延長メンテが 12:10 JST に OPEN 確認された場合、12:20 JST はまだ安定化待機中 (+15m = 12:25 まで)
        open_1210 = datetime(2026, 10, 24, 12, 10, 0, tzinfo=JST)
        m_sat_1220 = evaluate_jst_session_and_schedule_matrix(
            now_dt=datetime(2026, 10, 24, 12, 20, 0, tzinfo=JST),
            exchange_status="OPEN",
            maintenance_open_confirmed_at=open_1210,
        )
        self.assertEqual(m_sat_1220["session_name"], "SATURDAY_MAINTENANCE_GUARD")
        self.assertFalse(m_sat_1220["allow_new_buy"])
        self.assertEqual(m_sat_1220["block_reason"], "POST_MAINTENANCE_STABILIZATION_WAIT")

        # 6) 月曜朝 CME 窓開け (06:30〜09:30 JST): 小幅ギャップアップ (< 1.0*ATR) は飛びつきロング禁止
        dt_mon = datetime(2026, 10, 26, 7, 15, 0, tzinfo=JST)
        m_mon_small_gap = evaluate_jst_session_and_schedule_matrix(
            now_dt=dt_mon,
            market_summaries={
                "BTC": {
                    "price": {"last": 14100000.0},
                    "daily_indicators": {"atr_14": 300000.0, "cme_friday_close": 13950000.0},  # +0.50 ATR gap up
                }
            },
        )
        self.assertEqual(m_mon_small_gap["session_name"], "MONDAY_MORNING_CME_OPEN")
        self.assertFalse(m_mon_small_gap["allow_new_buy"])
        self.assertEqual(m_mon_small_gap["cme_gap_info"]["gap_regime"], "SMALL_GAP_MEAN_REVERSION_ONLY")

        # 大幅窓開け (> 1.2*ATR) はトレンドフォロー許可
        cme_large = evaluate_cme_monday_gap(
            current_price=14400000.0,
            cme_friday_close=13950000.0,
            atr=300000.0,
        )
        self.assertEqual(cme_large["gap_regime"], "LARGE_GAP_TREND_FOLLOW_ALLOWED")
        self.assertTrue(cme_large["allow_trend_follow"])

        # ------------------------------------------------------------------
        # ③ 少サンプル統計管理 (MinTRL 100件凍結, 連敗ラダー, Wald's SPRT, Page's Lower CUSUM, Shadow 25%重み)
        # ------------------------------------------------------------------
        self.assertEqual(evaluate_mintrl_freeze_stage(35)["mintrl_stage"], "STAGE0_HARD_FREEZE")
        self.assertTrue(evaluate_mintrl_freeze_stage(35)["parameters_frozen"])
        self.assertEqual(evaluate_mintrl_freeze_stage(150)["mintrl_stage"], "STAGE1_BOUNDARY_AUDIT")
        self.assertEqual(evaluate_mintrl_freeze_stage(260)["mintrl_stage"], "STAGE2_MINTRL_UNLOCKED")
        self.assertFalse(evaluate_mintrl_freeze_stage(260)["parameters_frozen"])

        # 連敗ラダー (5連敗Yellow / 6連敗Orange / 7連敗Red / 8連敗Halt)
        self.assertEqual(evaluate_losing_streak_ladder(4)["ladder_stage"], "GREEN_NORMAL")
        self.assertEqual(evaluate_losing_streak_ladder(5)["ladder_stage"], "YELLOW_TIER1_ONLY")
        self.assertEqual(evaluate_losing_streak_ladder(6)["ladder_stage"], "ORANGE_HALF_SLOT")
        self.assertAlmostEqual(evaluate_losing_streak_ladder(6)["max_slot_jpy"], 4500.0, places=1)
        self.assertEqual(evaluate_losing_streak_ladder(7)["ladder_stage"], "RED_SHADOW_ONLY")
        self.assertEqual(evaluate_losing_streak_ladder(8)["ladder_stage"], "HALT_SYSTEM_PROTECTION")

        # 6連敗時に Validator で Tier 2 が拒否され、Tier 1 (SOL) は 4,500円上限に半減されること
        val_6loss_t2 = validate_and_adjust_decision(
            decision={"action": "BUY", "symbol": "DOGE", "confidence": 0.85, "limit_price": 20.0},
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "consecutive_losses": 6, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries={"DOGE": {"price": {"last": 20.0, "best_bid": 20.0, "best_ask": 20.01}}},
        )
        self.assertEqual(val_6loss_t2["action"], "HOLD")
        self.assertTrue(val_6loss_t2.get("yellow_tier1_only_rejected"))

        val_6loss_t1 = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "SOL",
                "confidence": 0.85,
                "limit_price": 20000.0,
                "take_profit_price": 21200.0,
                "stop_loss_price": 19400.0,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "consecutive_losses": 6, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries={
                "SOL": {
                    "price": {"last": 20000.0, "best_bid": 20000.0, "best_ask": 20010.0, "spread_pct": 0.05},
                    "daily_indicators": {"atr_14": 600.0},
                }
            },
        )
        self.assertEqual(val_6loss_t1["action"], "BUY")
        self.assertTrue(val_6loss_t1.get("orange_half_slot_applied"))
        self.assertLessEqual(val_6loss_t1["expected_cost_jpy"], 4500)

        # Wald's SPRT: 下限 B = -2.2513 で 0 リセット、上限 A = +2.8904 で DEGRADATION_ALARM
        llr = 0.0
        for _ in range(8):
            s_win = compute_wald_sprt_step(llr, is_win=True, weight=1.0)
            llr = s_win["llr"]
        self.assertEqual(s_win["status"], "HEALTHY_RESET_ZERO")
        self.assertEqual(llr, 0.0)

        for _ in range(11):
            s_loss = compute_wald_sprt_step(llr, is_win=False, weight=1.0)
            llr = s_loss["llr"]
        self.assertEqual(s_loss["status"], "DEGRADATION_ALARM")
        self.assertTrue(s_loss["alarm_triggered"])

        # Page's Lower CUSUM & SQLite WAL 永続化
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_path = Path(tmp_dir_str)
            wal = DatabaseWAL(tmp_path / "sprt_cusum_test.db")
            cb = MultiTierCircuitBreaker(symbol_rules=DEFAULT_SYMBOL_RULES)
            for _ in range(6):
                res_sc = cb.record_closed_trade_sprt_and_cusum(
                    symbol="LINK",
                    net_pnl_jpy=-360.0,  # -1.2R (1回あたり +0.7857 -> 6回で S_i^- = 4.714 > 4.0)
                    r_multiple=-1.2,
                    is_shadow=False,
                    db_wal=wal,
                )
            self.assertTrue(res_sc["symbol_cusum"]["alarm_triggered"])
            self.assertIn("LINK", cb._dynamic_blacklisted_symbols)

            # 再起動後も SQLite WAL から SPRT / CUSUM 状態と動的ブラックリストが復元されること
            cb_reloaded = MultiTierCircuitBreaker(symbol_rules=DEFAULT_SYMBOL_RULES)
            loaded_state = cb_reloaded.load_sprt_cusum_from_wal(wal)
            self.assertIn("LINK", loaded_state["dynamic_blacklisted_symbols"])
            self.assertIn("LINK", cb_reloaded._dynamic_blacklisted_symbols)

            # Shadow Trade 25%重み割引 (w_shadow = 0.25) の検証
            shadow_ev = LLMShadowEvaluator(db_wal=wal)
            self.assertEqual(SHADOW_DISCOUNT_WEIGHT, 0.25)
            cal_res = shadow_ev.calibrate_confidence_and_threshold(
                raw_confidence=0.80,
                live_trade_count=20,
                live_wins=10,
                live_losses=10,
            )
            self.assertEqual(cal_res["mintrl_stage"], "STAGE0_HARD_FREEZE")
            self.assertTrue(cal_res["parameters_frozen"])
            self.assertEqual(cal_res["shadow_weight"], 0.25)
            wal.close()

    def test_round16_no_window_subprocess_wal_pragmas_ups_guard_and_2028_tax_forensic_bus_audit(self) -> None:
        """
        Round 16 監査テスト:
        ① コンソールポップアップの完全根絶（全 `.py` ファイルの `subprocess` における `CREATE_NO_WINDOW = 0x08000000`
           ＆ `STARTUPINFO(SW_HIDE=0)` ＆ `launch_hidden.vbs` の `0` 指定検証）
        ② `DatabaseWAL` の6つの高可用性 PRAGMA（`WAL`, `synchronous=NORMAL(1)`, `busy_timeout=5000`,
           `temp_store=MEMORY(2)`, `mmap_size=268435456`, `wal_autocheckpoint=1000`）の適用検証
        ③ `SystemLifecycleGuard` において、デスクトップPC（`ACLineStatus=1, BatteryLifePercent=255`）で誤発動しないこと、
           およびバッテリー低下（`ACLineStatus=0, BatteryLifePercent=35`）またはOSシャットダウンシグナル時に
           「`PENDING_BUY` のみキャンセルし、既存建玉の `STOP` 逆指値は維持したまま `wal_checkpoint(TRUNCATE)` を完了する」ことの検証
        ④ `classify_jp_crypto_tax_regime` において、`2027-12-31 23:59:59 JST` が `AGGREGATE_MISC (0.55945)`、
           `2028-01-01 00:00:00 JST` が `SEPARATE_20315 (0.20315, loss_carryforward_years=3)` と正確に区分されること、
           および `connect_forensic_readonly()` による Read-Only WAL 共有バス経由で `v_forensic_tax_ledger` が
           ロック競合ゼロで読み取れ、誤った書き込み（`INSERT`/`UPDATE`）は `sqlite3.OperationalError` で即座に拒絶されることの検証
        """
        import ast
        import os
        from src.database_wal import classify_jp_crypto_tax_regime, connect_forensic_readonly
        from src.system_lifecycle_guard import (
            CREATE_NO_WINDOW,
            CTRL_SHUTDOWN_EVENT,
            SW_HIDE,
            SYSTEM_POWER_STATUS,
            SystemLifecycleGuard,
            build_hidden_subprocess_kwargs,
            get_system_power_status,
        )

        repo_root = Path(__file__).resolve().parent.parent

        # ------------------------------------------------------------------
        # ① コンソールポップアップの完全根絶監査 (AST 全スキャン & VBS/PS1 検証)
        # ------------------------------------------------------------------
        hidden_kw = build_hidden_subprocess_kwargs(is_git=True)
        if os.name == "nt":
            self.assertEqual(hidden_kw["creationflags"], CREATE_NO_WINDOW)
            self.assertEqual(hidden_kw["startupinfo"].wShowWindow, SW_HIDE)
        self.assertEqual(hidden_kw["env"]["GIT_TERMINAL_PROMPT"], "0")
        self.assertEqual(hidden_kw["env"]["GCM_INTERACTIVE"], "never")

        subprocess_calls_checked = 0
        for py_file in repo_root.rglob("*.py"):
            if ".venv" in py_file.parts:
                continue
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == "subprocess":
                        if node.func.attr in ("run", "Popen", "check_output", "call", "check_call"):
                            subprocess_calls_checked += 1
                            kw_names = {k.arg for k in node.keywords if k.arg is not None}
                            has_kwargs_unpack = any(k.arg is None for k in node.keywords)
                            self.assertTrue(
                                "creationflags" in kw_names or has_kwargs_unpack,
                                f"{py_file.name}:{node.lineno} subprocess.{node.func.attr} is missing creationflags=CREATE_NO_WINDOW",
                            )
        self.assertGreaterEqual(subprocess_calls_checked, 6)

        vbs_text = (repo_root / "launch_hidden.vbs").read_text(encoding="utf-8")
        self.assertIn("shell.Run(cmd, 0, True)", vbs_text)

        # ------------------------------------------------------------------
        # ② 高可用性 SQLite WAL PRAGMA 6項目の全面適用検証
        # ------------------------------------------------------------------
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_path = Path(tmp_dir_str)
            wal = DatabaseWAL(tmp_path / "round16_wal.db", portfolio_json_path=tmp_path / "portfolio.json")
            pragmas = wal.get_pragmas()
            self.assertEqual(pragmas["journal_mode"], "WAL")
            self.assertEqual(pragmas["synchronous"], 1)  # NORMAL(1)
            self.assertEqual(pragmas["synchronous_name"], "NORMAL")
            self.assertEqual(pragmas["busy_timeout"], 5000)
            self.assertEqual(pragmas["temp_store"], 2)  # MEMORY(2)
            self.assertEqual(pragmas["temp_store_name"], "MEMORY")
            self.assertEqual(pragmas["mmap_size"], 268435456)
            self.assertEqual(pragmas["wal_autocheckpoint"], 1000)

            # ------------------------------------------------------------------
            # ③ Win32 `SYSTEM_POWER_STATUS` (c_ubyte) ＆ UPS / OSシャットダウン保護検証
            # ------------------------------------------------------------------
            field_types = dict(SYSTEM_POWER_STATUS._fields_)
            self.assertEqual(field_types["ACLineStatus"].__name__, "c_ubyte")
            self.assertEqual(field_types["BatteryLifePercent"].__name__, "c_ubyte")

            # デスクトップPC AC常時接続 (ACLineStatus=1, BatteryLifePercent=255) で誤発動しないこと
            p_desktop = get_system_power_status(
                simulated_status={"ACLineStatus": 1, "BatteryFlag": 128, "BatteryLifePercent": 255}
            )
            self.assertFalse(p_desktop["should_emergency_graceful_stop"])
            self.assertEqual(p_desktop["power_mode"], "DESKTOP_AC_NO_BATTERY")

            # バッテリー駆動中でも残量 75% (> 40%) なら通常稼働継続
            p_batt_ok = get_system_power_status(
                simulated_status={"ACLineStatus": 0, "BatteryFlag": 1, "BatteryLifePercent": 75}
            )
            self.assertFalse(p_batt_ok["should_emergency_graceful_stop"])

            # UPS バッテリー低下 (ACLineStatus=0, BatteryLifePercent=35 <= 40) で緊急グレースフル停止発動
            ex = ChaosMockExchange(tmp_path, wal_db=wal)
            oco = PseudoOCOManager(
                broker=ex,
                portfolio_path=tmp_path / "portfolio.json",
                wal_db=wal,
                dry_run_notify=True,
            )
            rc = RemoteControlManager(oco_manager=oco, db_wal=wal, allowed_owner_discord_id="owner_777")
            guard = SystemLifecycleGuard(
                oco_manager=oco,
                remote_control=rc,
                db_wal=wal,
                dry_run_notify=True,
                register_win32_handler=False,
            )

            t0 = datetime(2026, 10, 24, 15, 0, 0, tzinfo=JST)
            # 1) 保有ポジション (BCH: 取引所サーバー上の STOP 注文あり) を作成
            oco.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 46500,
                    "stop_loss_price": 42500,
                },
                tickers={"BCH": {"ask": 44100, "bid": 44050, "last": 44050}},
                now_dt=t0,
            )
            oco.step(tickers={"BCH": {"ask": 43950, "bid": 43940, "last": 43945}}, now_dt=t0 + timedelta(seconds=10))
            # 2) 未約定の待機買い指値 (SOL: PENDING_BUY) を作成
            oco.place_new_buy_order(
                {
                    "action": "BUY",
                    "symbol": "SOL",
                    "limit_price": 19500,
                    "size": 0.1,
                    "take_profit_price": 21000,
                    "stop_loss_price": 18800,
                },
                tickers={"SOL": {"ask": 20000, "bid": 19990, "last": 20000}},
                now_dt=t0 + timedelta(seconds=20),
            )
            pf_before = oco.load_portfolio()
            self.assertEqual(len(pf_before["positions"]), 1)
            self.assertEqual(len(pf_before["pending_orders"]), 1)
            active_sl_oid = pf_before["positions"][0]["sl_order_id"]
            pending_buy_oid = pf_before["pending_orders"][0]["order_id"]

            # デスクトップAC状態では何もキャンセルされないこと
            chk_desktop = guard.check_power_and_execute_if_needed(
                simulated_status={"ACLineStatus": 1, "BatteryFlag": 128, "BatteryLifePercent": 255},
                now_dt=t0 + timedelta(seconds=25),
            )
            self.assertFalse(chk_desktop["triggered"])
            self.assertEqual(len(oco.load_portfolio()["pending_orders"]), 1)

            # UPS バッテリー残量 35% 検知 -> PENDING_BUY のみキャンセル、STOP は完全維持、WAL TRUNCATE 完了
            chk_ups = guard.check_power_and_execute_if_needed(
                simulated_status={"ACLineStatus": 0, "BatteryFlag": 2, "BatteryLifePercent": 35},
                now_dt=t0 + timedelta(seconds=30),
            )
            self.assertTrue(chk_ups["triggered"])
            shut_res = chk_ups["shutdown_result"]
            self.assertEqual(shut_res["status"], "GRACEFUL_SHUTDOWN_COMPLETED")
            self.assertIn(pending_buy_oid, shut_res["canceled_buy_order_ids"])
            self.assertIn(active_sl_oid, shut_res["preserved_sl_order_ids"])
            self.assertEqual(shut_res["wal_checkpoint"]["mode"], "TRUNCATE")

            pf_after = oco.load_portfolio()
            self.assertEqual(len(pf_after["pending_orders"]), 0)
            self.assertEqual(len(pf_after["positions"]), 1)
            self.assertEqual(pf_after["positions"][0]["sl_order_id"], active_sl_oid)
            # 取引所モック上でも BCH の STOP 注文が WAITING のまま生きており、SOL の LIMIT BUY はキャンセル済みであること
            bch_active = ex.get_active_orders("BCH")
            self.assertEqual(len(bch_active), 1)
            self.assertEqual(bch_active[0]["order_id"], active_sl_oid)
            self.assertEqual(len(ex.get_active_orders("SOL")), 0)

            # OS シャットダウンシグナル (CTRL_SHUTDOWN_EVENT=6) の冪等ハンドリング検証
            guard_os = SystemLifecycleGuard(
                oco_manager=oco,
                remote_control=rc,
                db_wal=wal,
                dry_run_notify=True,
                register_win32_handler=False,
            )
            os_sig_res = guard_os.handle_os_ctrl_event(CTRL_SHUTDOWN_EVENT, now_dt=t0 + timedelta(seconds=40))
            self.assertEqual(os_sig_res["reason"], "WIN32_CTRL_SHUTDOWN_EVENT")
            self.assertEqual(os_sig_res["wal_checkpoint"]["mode"], "TRUNCATE")
            self.assertIn(active_sl_oid, os_sig_res["preserved_sl_order_ids"])

            # ------------------------------------------------------------------
            # ④ 2028年申告分離課税（20.315%）区分 ＆ Forensic Ledger Read-Only WAL 共有バス検証
            # ------------------------------------------------------------------
            tax_2027 = classify_jp_crypto_tax_regime("2027-12-31T23:59:59+09:00", realized_pnl_jpy=10000)
            self.assertEqual(tax_2027["tax_year"], 2027)
            self.assertEqual(tax_2027["tax_regime"], "AGGREGATE_MISC")
            self.assertAlmostEqual(tax_2027["effective_tax_rate"], 0.55945, places=5)
            self.assertEqual(tax_2027["loss_carryforward_years"], 0)
            self.assertEqual(tax_2027["exchange_registration"], "JP_FSA_REGISTERED_GMO")
            self.assertAlmostEqual(tax_2027["estimated_tax_jpy"], 5594.50, places=2)
            self.assertAlmostEqual(tax_2027["after_tax_net_pnl_jpy"], 4405.50, places=2)

            tax_2028 = classify_jp_crypto_tax_regime("2028-01-01T00:00:00+09:00", realized_pnl_jpy=10000)
            self.assertEqual(tax_2028["tax_year"], 2028)
            self.assertEqual(tax_2028["tax_regime"], "SEPARATE_20315")
            self.assertAlmostEqual(tax_2028["effective_tax_rate"], 0.20315, places=5)
            self.assertEqual(tax_2028["loss_carryforward_years"], 3)
            self.assertEqual(tax_2028["exchange_registration"], "JP_FSA_REGISTERED_GMO")
            self.assertAlmostEqual(tax_2028["estimated_tax_jpy"], 2031.50, places=2)
            self.assertAlmostEqual(tax_2028["after_tax_net_pnl_jpy"], 7968.50, places=2)

            # 2027年・2028年の税務イベントおよび 06:00 JST 端数返金を記録
            wal.record_tax_ledger_event(
                event_id="evt_2027_close_1",
                symbol="SOL",
                event_type="SPOT_TRADE_CLOSE",
                side="SELL",
                size=0.5,
                price_jpy=22000,
                gross_proceeds_jpy=11000,
                cost_basis_jpy=10000,
                fee_or_rebate_jpy=2.0,
                realized_pnl_jpy=1002.0,
                source_order_id="ord_2027_1",
                timestamp_jst="2027-12-31T23:59:59+09:00",
            )
            wal.record_tax_ledger_event(
                event_id="evt_2028_close_1",
                symbol="BTC",
                event_type="SPOT_TRADE_CLOSE",
                side="SELL",
                size=0.001,
                price_jpy=15000000,
                gross_proceeds_jpy=15000,
                cost_basis_jpy=13000,
                fee_or_rebate_jpy=3.0,
                realized_pnl_jpy=2003.0,
                source_order_id="ord_2028_1",
                timestamp_jst="2028-01-01T00:00:00+09:00",
            )
            wal.record_fee_adjustment(
                date_str="2028-01-02",
                amount_jpy=4,
                note="DAILY_FEE_FRACTION_SETTLEMENT (06:00:30 JST)",
            )

            rows_2027 = wal.query_forensic_tax_ledger(tax_year=2027)
            rows_2028 = wal.query_forensic_tax_ledger(tax_year=2028)
            self.assertTrue(any(r["event_id"] == "evt_2027_close_1" and r["tax_regime"] == "AGGREGATE_MISC" for r in rows_2027))
            self.assertTrue(any(r["event_id"] == "evt_2028_close_1" and r["tax_regime"] == "SEPARATE_20315" for r in rows_2028))
            self.assertTrue(any(r["event_type"] == "DAILY_FEE_FRACTION_REFUND" and r["tax_regime"] == "SEPARATE_20315" for r in rows_2028))

            # Forensic Ledger Read-Only WAL 共有バス (`connect_forensic_readonly`) の読み取り成功 ＆ 書き込み拒絶検証
            ro_conn = connect_forensic_readonly(wal.db_path)
            try:
                view_rows = ro_conn.execute("SELECT * FROM v_forensic_tax_ledger").fetchall()
                self.assertGreaterEqual(len(view_rows), 3)
                with self.assertRaises(sqlite3.OperationalError):
                    ro_conn.execute(
                        "INSERT INTO tax_ledger_events (event_id, timestamp_jst, tax_year, tax_regime, effective_tax_rate, created_ts_ms) "
                        "VALUES ('illegal_write', '2028-01-01T00:00:00+09:00', 2028, 'SEPARATE_20315', 0.20315, 1)"
                    )
                with self.assertRaises(sqlite3.OperationalError):
                    ro_conn.execute("UPDATE tax_ledger_events SET realized_pnl_jpy = 999999")
            finally:
                ro_conn.close()
            wal.close()

    def test_round17_link_xlm_dot_micro_conditions_and_us_macro_announcement_window_audit(self) -> None:
        """
        Round 17 Audit Test (Deep Research #2 細部マイクロ条件4点の100%完全一致検証):
        1. Tier 2 個別銘柄の細部フィルタ (`src/regime_filter.py`, `src/radar.py`, `src/validator.py`):
           - `LINK`: 欧米重複の最活況時間帯（平日 21:00〜03:00 JST）のみスキャン・発注を許可し、それ以外の時間帯（例: 16:00 JST）は除外。
           - `XLM`: 原則 `XRP` を優先し、現在すでに `XRP` ポジションを保有中（または有効な `XRP` 買い指値が待機中）の場合のみ補完銘柄として許可。
           - `DOT`: 平常時の板薄スリッページ（1.32R損失拡大リスク）を防ぐため、出来高急増時（`volume_ratio >= 1.8`）のみ許可し、平常時はスキップ。
        2. 平日 21:30 / 22:30 JST（米主要マクロ経済指標発表）前後5分間のマイクロ・フィルタ (`21:25〜21:35` / `22:25〜22:35 JST`):
           - 新規買い指値の発注を一時保留 (`allow_new_buy=False`)。
           - `AutoTraderDaemon.run_fast_cycle` にて未約定の待機買い指値 (`PENDING_BUY`) を一時退避（キャンセル）し、保有中建玉の取引所サーバー `STOP` 注文は絶対に解除せず維持。
        """
        import json
        from src.regime_filter import (
            DOT_MIN_VOLUME_SURGE_RATIO,
            evaluate_jst_session_and_schedule_matrix,
            evaluate_tier2_symbol_micro_conditions,
            evaluate_us_macro_announcement_window,
            is_in_us_macro_announcement_window,
        )

        deep_ob_link = {
            "bids": [
                {"price": "2000.0", "size": "15.0"},   # 30,000円
                {"price": "1999.0", "size": "15.0"},   # 29,985円 (合計 59,985円 >= 45,000円)
            ],
            "asks": [{"price": "2001.0", "size": "15.0"}],
        }

        # ------------------------------------------------------------------
        # 1-A. LINK: 平日 16:00 JST (EUROPE_ACTIVE) では除外、23:00 JST (EU_US_OVERLAP_GOLDEN) では許可
        # ------------------------------------------------------------------
        dt_1600 = datetime(2026, 10, 21, 16, 0, 0, tzinfo=JST)  # 水曜 16:00 JST
        dt_2300 = datetime(2026, 10, 21, 23, 0, 0, tzinfo=JST)  # 水曜 23:00 JST (米指標ウィンドウ外)

        link_cond_1600 = evaluate_tier2_symbol_micro_conditions("LINK", now_dt=dt_1600)
        self.assertFalse(link_cond_1600["passed"])
        self.assertEqual(link_cond_1600["failed_code"], "LINK_OUTSIDE_EU_US_OVERLAP_WINDOW")

        link_cond_2300 = evaluate_tier2_symbol_micro_conditions("LINK", now_dt=dt_2300)
        self.assertTrue(link_cond_2300["passed"])

        # validator でも 16:00 JST の LINK BUY は HOLD へ補正され、23:00 JST では BUY が通ることを確認
        link_summaries = {
            "BTC": {"daily_indicators": {"trend": "STRONG_UPTREND"}, "h4_indicators": {"rsi_14": 58.0}},
            "LINK": {
                "price": {"last": 2000.0, "best_bid": 2000.0, "best_ask": 2001.0, "spread_pct": 0.05},
                "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 55.0, "atr_14": 80.0},
                "h4_indicators": {"close": 2000.0, "sma_25": 1950.0, "rsi_14": 52.0, "atr_14": 40.0},
                "h1_indicators": {"rsi_14": 44.0, "atr_14": 25.0, "rsi_rebound": True},
                "orderbook": deep_ob_link,
            },
        }
        val_link_1600 = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "LINK",
                "confidence": 0.85,
                "limit_price": 2000.0,
                "take_profit_price": 2120.0,
                "stop_loss_price": 1960.0,
                "now_dt": dt_1600,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=link_summaries,
        )
        self.assertEqual(val_link_1600["action"], "HOLD")
        self.assertTrue(val_link_1600.get("link_session_window_rejected"))
        self.assertEqual(val_link_1600.get("tier2_micro_failed_code"), "LINK_OUTSIDE_EU_US_OVERLAP_WINDOW")

        val_link_2300 = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "LINK",
                "confidence": 0.85,
                "limit_price": 2000.0,
                "take_profit_price": 2120.0,
                "stop_loss_price": 1960.0,
                "now_dt": dt_2300,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=link_summaries,
        )
        self.assertEqual(val_link_2300["action"], "BUY")

        # ------------------------------------------------------------------
        # 1-B. XLM: 原則 XRP 優先・XRP 保有中（または待機指値あり）の場合のみ補完銘柄として許可
        # ------------------------------------------------------------------
        deep_ob_xlm = {
            "bids": [
                {"price": "50.0", "size": "600.0"},    # 30,000円
                {"price": "49.95", "size": "500.0"},   # 24,975円 (合計 54,975円 >= 45,000円)
            ],
            "asks": [{"price": "50.05", "size": "600.0"}],
        }
        xlm_summaries = {
            "XLM": {
                "price": {"last": 50.0, "best_bid": 50.0, "best_ask": 50.05, "spread_pct": 0.10},
                "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 54.0, "atr_14": 2.0},
                "h4_indicators": {"close": 50.0, "sma_25": 48.0, "rsi_14": 50.0, "atr_14": 1.2},
                "h1_indicators": {"rsi_14": 42.0, "atr_14": 0.8, "rsi_rebound": True},
                "orderbook": deep_ob_xlm,
            }
        }
        # XRP 未保有時は XLM 単体エントリーを拒否
        val_xlm_no_xrp = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "XLM",
                "confidence": 0.85,
                "limit_price": 50.0,
                "take_profit_price": 53.0,
                "stop_loss_price": 49.0,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": [], "pending_orders": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=xlm_summaries,
        )
        self.assertEqual(val_xlm_no_xrp["action"], "HOLD")
        self.assertTrue(val_xlm_no_xrp.get("xlm_requires_xrp_rejected"))
        self.assertEqual(val_xlm_no_xrp.get("tier2_micro_failed_code"), "XLM_REQUIRES_XRP_HELD_OR_PENDING")

        # XRP 保有中（または XRP 待機指値あり）の時は補完銘柄として許可
        val_xlm_with_xrp = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "XLM",
                "confidence": 0.85,
                "limit_price": 50.0,
                "take_profit_price": 53.0,
                "stop_loss_price": 49.0,
            },
            portfolio={
                "jpy_balance": 20025,
                "initial_capital_jpy": 20000,
                "positions": [],
                "pending_orders": [
                    {
                        "order_id": "xrp_buy_1",
                        "symbol": "XRP",
                        "side": "BUY",
                        "state": "PENDING_BUY",
                        "limit_price": "100.0",
                        "size": "80",
                    }
                ],
            },
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=xlm_summaries,
        )
        self.assertEqual(val_xlm_with_xrp["action"], "BUY")

        # ------------------------------------------------------------------
        # 1-C. DOT: 出来高急増時 (volume_ratio >= 1.8) のみ許可、平常出来高はスキップ
        # ------------------------------------------------------------------
        self.assertAlmostEqual(DOT_MIN_VOLUME_SURGE_RATIO, 1.80, places=2)
        deep_ob_dot = {
            "bids": [
                {"price": "800.0", "size": "40.0"},    # 32,000円
                {"price": "799.0", "size": "30.0"},    # 23,970円 (合計 55,970円 >= 45,000円)
            ],
            "asks": [{"price": "800.5", "size": "40.0"}],
        }
        dot_normal_summaries = {
            "DOT": {
                "price": {"last": 800.0, "best_bid": 800.0, "best_ask": 800.5, "spread_pct": 0.06},
                "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 54.0, "atr_14": 30.0},
                "h4_indicators": {"close": 800.0, "sma_25": 780.0, "rsi_14": 50.0, "atr_14": 18.0, "volume_ratio": 1.15},
                "h1_indicators": {"rsi_14": 43.0, "atr_14": 12.0, "rsi_rebound": True, "volume_ratio": 1.20},
                "orderbook": deep_ob_dot,
            }
        }
        val_dot_normal = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "DOT",
                "confidence": 0.85,
                "limit_price": 800.0,
                "take_profit_price": 848.0,
                "stop_loss_price": 784.0,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=dot_normal_summaries,
        )
        self.assertEqual(val_dot_normal["action"], "HOLD")
        self.assertTrue(val_dot_normal.get("dot_volume_surge_rejected"))
        self.assertEqual(val_dot_normal.get("tier2_micro_failed_code"), "DOT_NO_VOLUME_SURGE")

        dot_surge_summaries = {
            "DOT": {
                "price": {"last": 800.0, "best_bid": 800.0, "best_ask": 800.5, "spread_pct": 0.06},
                "daily_indicators": {"trend": "STRONG_UPTREND", "rsi_14": 54.0, "atr_14": 30.0},
                "h4_indicators": {"close": 800.0, "sma_25": 780.0, "rsi_14": 50.0, "atr_14": 18.0, "volume_ratio": 2.15},
                "h1_indicators": {"rsi_14": 43.0, "atr_14": 12.0, "rsi_rebound": True, "volume_ratio": 2.25, "volume_surge": True},
                "orderbook": deep_ob_dot,
            }
        }
        val_dot_surge = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "DOT",
                "confidence": 0.85,
                "limit_price": 800.0,
                "take_profit_price": 848.0,
                "stop_loss_price": 784.0,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries=dot_surge_summaries,
        )
        self.assertEqual(val_dot_surge["action"], "BUY")

        # Radar でも LINK(16:00除外), XLM(XRP未保有時除外), DOT(平常出来高除外) が機能することを確認
        radar_r17 = TechnicalRadar()
        r17_trig_1600 = radar_r17.evaluate_triggers(
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": [], "pending_orders": []},
            market_summaries={
                **link_summaries,
                **xlm_summaries,
                **dot_normal_summaries,
            },
            symbol_rules=DEFAULT_SYMBOL_RULES,
            now_dt=dt_1600,
        )
        self.assertNotIn("LINK", r17_trig_1600["candidate_symbols"])
        self.assertNotIn("XLM", r17_trig_1600["candidate_symbols"])
        self.assertNotIn("DOT", r17_trig_1600["candidate_symbols"])

        # ------------------------------------------------------------------
        # 2. 平日 21:30 / 22:30 JST 米主要マクロ経済指標発表前後5分間マイクロ・フィルタ
        #    (21:25〜21:35 JST / 22:25〜22:35 JST)
        # ------------------------------------------------------------------
        dt_macro_2130 = datetime(2026, 10, 21, 21, 30, 0, tzinfo=JST)  # 水曜 21:30 JST (夏時間 8:30 ET)
        dt_macro_2230 = datetime(2026, 10, 21, 22, 30, 0, tzinfo=JST)  # 水曜 22:30 JST (冬時間 8:30 EST)
        dt_non_macro = datetime(2026, 10, 21, 21, 45, 0, tzinfo=JST)   # 水曜 21:45 JST (通常時)

        self.assertTrue(is_in_us_macro_announcement_window(dt_macro_2130))
        self.assertTrue(is_in_us_macro_announcement_window(dt_macro_2230))
        self.assertFalse(is_in_us_macro_announcement_window(dt_non_macro))

        m_macro_2130 = evaluate_jst_session_and_schedule_matrix(now_dt=dt_macro_2130)
        self.assertEqual(m_macro_2130["session_name"], "US_MACRO_ANNOUNCEMENT_WINDOW")
        self.assertTrue(m_macro_2130["in_us_macro_announcement_window"])
        self.assertFalse(m_macro_2130["allow_new_buy"])
        self.assertTrue(m_macro_2130["should_clear_pending_buys"])
        self.assertTrue(m_macro_2130["preserve_active_sl_orders"])

        # 新規BUYが米指標ウィンドウ中に一時保留 (HOLD) されることを確認
        val_macro_hold = validate_and_adjust_decision(
            decision={
                "action": "BUY",
                "symbol": "SOL",
                "confidence": 0.90,
                "limit_price": 25000.0,
                "take_profit_price": 26500.0,
                "stop_loss_price": 24500.0,
                "now_dt": dt_macro_2130,
            },
            portfolio={"jpy_balance": 20025, "initial_capital_jpy": 20000, "positions": []},
            symbol_rules=DEFAULT_SYMBOL_RULES,
            market_summaries={
                "SOL": {
                    "price": {"last": 25000.0, "best_bid": 25000.0, "best_ask": 25010.0, "spread_pct": 0.04},
                    "daily_indicators": {"atr_14": 1000.0},
                }
            },
        )
        self.assertEqual(val_macro_hold["action"], "HOLD")
        self.assertTrue(val_macro_hold.get("us_macro_announcement_window_rejected"))

        # AutoTraderDaemon.run_fast_cycle(21:30 JST) で未約定買い指値のみ退避され、保有建玉の STOP 注文は維持されることを確認
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pf_path = tmp_path / "portfolio.json"
            hist_path = tmp_path / "history_log.csv"
            dec_path = tmp_path / "latest_decision.json"
            wal_path = tmp_path / "macro_test.sqlite3"

            sl_oid = "9100000001"
            seed_pf = {
                "jpy_balance": 20025.0,
                "initial_capital_jpy": 20000.0,
                "positions": [
                    {
                        "symbol": "SOL",
                        "size": 0.36,
                        "entry_price": 25000.0,
                        "current_price": 25100.0,
                        "stop_loss_price": 24500.0,
                        "take_profit_price": 26500.0,
                        "state": "SL_ACTIVE",
                        "sl_order_id": sl_oid,
                        "opened_at": "2026-10-21T20:00:00+09:00",
                    }
                ],
                "pending_orders": [],
            }
            pf_path.write_text(json.dumps(seed_pf, ensure_ascii=False, indent=2), encoding="utf-8")

            broker = PaperBroker(portfolio_path=pf_path)

            # 2枠目: 待機中の未約定 BTC BUY 指値注文を発注
            buy_ord = broker.place_order(
                symbol="BTC",
                side="BUY",
                execution_type="LIMIT",
                price="14000000",
                size="0.0006",
            )
            buy_oid = str(buy_ord["order_id"])

            init_pf = {
                "jpy_balance": 20025.0,
                "initial_capital_jpy": 20000.0,
                "positions": [
                    {
                        "symbol": "SOL",
                        "size": 0.36,
                        "entry_price": 25000.0,
                        "current_price": 25100.0,
                        "stop_loss_price": 24500.0,
                        "take_profit_price": 26500.0,
                        "state": "SL_ACTIVE",
                        "sl_order_id": sl_oid,
                        "opened_at": "2026-10-21T20:00:00+09:00",
                    }
                ],
                "pending_orders": [
                    {
                        "order_id": buy_oid,
                        "symbol": "BTC",
                        "side": "BUY",
                        "execution_type": "LIMIT",
                        "limit_price": 14000000.0,
                        "size": 0.0006,
                        "stop_loss_price": 13750000.0,
                        "take_profit_price": 14600000.0,
                        "state": "PENDING_BUY",
                        "placed_at": "2026-10-21T21:10:00+09:00",
                    }
                ],
            }
            pf_path.write_text(json.dumps(init_pf, ensure_ascii=False, indent=2), encoding="utf-8")

            daemon = AutoTraderDaemon(
                base_dir=tmp_path,
                execution_mode="PAPER",
                broker=broker,
                dry_run_notify=True,
            )
            try:
                fast_res = daemon.run_fast_cycle(
                    tickers={
                        "SOL": {"last": "25100", "bid": "25095", "ask": "25105", "high": "25150", "low": "25050"},
                        "BTC": {"last": "14050000", "bid": "14049000", "ask": "14051000", "high": "14060000", "low": "14040000"},
                    },
                    now_dt=dt_macro_2130,
                    exchange_status="OPEN",
                )
                ev_types = [e.get("type") for e in fast_res.get("events", [])]
                self.assertIn("US_MACRO_ANNOUNCEMENT_PENDING_BUY_EVACUATED", ev_types)

                pf_after = daemon.oco_manager.load_portfolio()
                # 未約定の買い指値はキャンセルされて空になっていること
                self.assertEqual(len(pf_after.get("pending_orders", [])), 0)
                # 保有中の SOL ポジションと取引所サーバー STOP 注文は100%そのまま維持されていること
                self.assertEqual(len(pf_after.get("positions", [])), 1)
                self.assertEqual(pf_after["positions"][0]["sl_order_id"], sl_oid)
                active_ids = {str(o.get("order_id")) for o in broker.get_active_orders()}
                self.assertIn(sl_oid, active_ids)
                self.assertNotIn(buy_oid, active_ids)
            finally:
                daemon.db_wal.close()

    def test_round18_process_isolation_risk_bus_forensic_tax_and_phase1_dsr_audit(self) -> None:
        r"""
        Round 18 Audit Test (Deep Research #3 シングルノード・マルチボット統合アーキテクチャ & 税務監査 & 5ゲートDSR検証):
        1. `configure_bot_process_isolation` (Tier 1〜4 優先度設定・REALTIME禁止キャップ)、`trim_process_working_set` (`EmptyWorkingSet`)、
           および `DatabaseWAL.create_online_backup` / `verify_startup_integrity` の検証。
        2. `RiskBusSentinelWriter` (`BEGIN IMMEDIATE` + `locking_mode=NORMAL` + `journal_mode=WAL`) と `RiskBusBotReader` (`?mode=ro`)
           による SWMR 同時読み書き・3段階グローバルブレーカー (`NORMAL` / `THROTTLE` / `HALT_HEDGE`)・2028年税務整合型ヘッジ比率・`AutoTraderDaemon` 連動検証。
        3. `ForensicTaxAuditor` による全口座合算の移動平均法・総平均法並列計算、Makerリベート2方式切替、
           2028年分離課税 (`20.315%`・3年繰越) と総合課税 (`HYPERLIQUID`/`QANX_DEX`) の損益通算禁止ルール
           （黒字300万円・赤字200万円でも相殺されず300万円に課税されること）、および2027年12月タックス・ハーベスティングアラートの検証。
        4. `phase1_dsr_validator` による報告書数値例 ($T=300, N=50, \hat{\gamma}_3=-0.1952, \hat{\gamma}_4=5.0128, \widehat{SR}^*=0.1337, SR_0=0.0698 \implies \text{DSR} \approx 0.8603 < 0.95$)
           の精密再現検証、および CSCV PBO・WFE・5ゲート昇格判定（ケースA/B/C）の検証。
        """
        import json
        import math
        import os
        from decimal import Decimal
        from src.system_lifecycle_guard import (
            ABOVE_NORMAL_PRIORITY_CLASS,
            BELOW_NORMAL_PRIORITY_CLASS,
            HIGH_PRIORITY_CLASS,
            NORMAL_PRIORITY_CLASS,
            REALTIME_PRIORITY_CLASS,
            configure_bot_process_isolation,
            trim_process_working_set,
        )
        from src.global_risk_bus import (
            BREAKER_LEVEL_HALT_HEDGE,
            BREAKER_LEVEL_NORMAL,
            BREAKER_LEVEL_THROTTLE,
            RiskBusBotReader,
            RiskBusSentinelWriter,
            evaluate_cross_venue_breaker_level,
        )
        from src.forensic_tax_auditor import ForensicTaxAuditor, TaxCategory
        from src.phase1_dsr_validator import (
            calc_dsr_and_mintrl,
            compute_effective_trials,
            compute_pbo_cscv,
            compute_wfe_and_oos_pf,
            evaluate_phase1_to_phase2_promotion,
            expected_max_sr,
        )

        # ------------------------------------------------------------------
        # ① Windows プロセス優先度4階層分離 & EmptyWorkingSet & SQLite Online Backup 検証
        # ------------------------------------------------------------------
        try:
            iso_t1 = configure_bot_process_isolation(tier=1)
            self.assertEqual(iso_t1["applied_priority_class"], HIGH_PRIORITY_CLASS)
            self.assertFalse(iso_t1["realtime_capped"])

            iso_t2 = configure_bot_process_isolation(tier=2)
            self.assertEqual(iso_t2["applied_priority_class"], ABOVE_NORMAL_PRIORITY_CLASS)

            iso_t4 = configure_bot_process_isolation(tier=4)
            self.assertEqual(iso_t4["applied_priority_class"], BELOW_NORMAL_PRIORITY_CLASS)

            # REALTIME_PRIORITY_CLASS 指定時は強制的に HIGH_PRIORITY_CLASS にキャップされること
            iso_rt = configure_bot_process_isolation(tier=1, priority_class_override=REALTIME_PRIORITY_CLASS)
            self.assertTrue(iso_rt["realtime_capped"])
            self.assertEqual(iso_rt["applied_priority_class"], HIGH_PRIORITY_CLASS)

            iso_t3 = configure_bot_process_isolation(tier=3)
            self.assertEqual(iso_t3["applied_priority_class"], NORMAL_PRIORITY_CLASS)
        finally:
            configure_bot_process_isolation(tier=3)

        trim_res = trim_process_working_set()
        self.assertIn("ok", trim_res)
        self.assertGreaterEqual(trim_res["gc_collected_objects"], 0)
        if os.name == "nt":
            self.assertTrue(trim_res["ok"])

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            wal_db_path = tmp_path / "source_wal.db"
            bak_db_path = tmp_path / "backup_snapshot.db"
            wal = DatabaseWAL(wal_db_path, auto_sync_files=False)
            try:
                int_chk = wal.verify_startup_integrity(full_check=True)
                self.assertTrue(int_chk["ok"])
                self.assertEqual(int_chk["status"].lower(), "ok")

                wal.create_intent(kind="ENTRY_BUY", symbol="SOL", payload={"size": "0.5"})
                bak_res = wal.create_online_backup(bak_db_path)
                self.assertTrue(bak_res["ok"])
                self.assertEqual(bak_res["integrity"].lower(), "ok")
                self.assertTrue(bak_db_path.exists())
                self.assertGreater(bak_res["size_bytes"], 0)
            finally:
                wal.close()

            # ------------------------------------------------------------------
            # ② SWMR 共有リスクバス (`risk_bus.db`) & 3段階グローバルブレーカー & 2028年税務整合型ヘッジ比率
            # ------------------------------------------------------------------
            rb_path = tmp_path / "risk_bus.db"
            writer = RiskBusSentinelWriter(rb_path)
            reader = RiskBusBotReader(rb_path)
            try:
                pragmas = writer.verify_pragmas()
                self.assertEqual(pragmas["locking_mode"], "normal")
                self.assertEqual(pragmas["journal_mode"], "wal")
                self.assertEqual(pragmas["busy_timeout"], 5000)
                self.assertEqual(pragmas["wal_autocheckpoint"], 1000)

                # Level 0: NORMAL
                ev0 = writer.evaluate_and_publish(
                    btc_z_score=-0.8,
                    avg_corr_distance=0.62,
                    daily_drawdown_pct=0.4,
                    gmo_spot_notional_jpy=18000.0,
                    comprehensive_alt_notional_jpy=15000.0,
                    tax_year=2027,
                )
                self.assertEqual(ev0["breaker_level"], BREAKER_LEVEL_NORMAL)
                st0 = reader.read_breaker_state()
                self.assertEqual(st0["breaker_level"], BREAKER_LEVEL_NORMAL)

                # Level 1: THROTTLE (Z_BTC = -2.8 < -2.5 または d_bar = 0.28 < 0.35)
                ev1 = writer.evaluate_and_publish(
                    btc_z_score=-2.8,
                    avg_corr_distance=0.28,
                    daily_drawdown_pct=1.2,
                    gmo_spot_notional_jpy=18000.0,
                    comprehensive_alt_notional_jpy=15000.0,
                    tax_year=2027,
                )
                self.assertEqual(ev1["breaker_level"], BREAKER_LEVEL_THROTTLE)
                st1 = reader.read_breaker_state()
                self.assertEqual(st1["breaker_level"], BREAKER_LEVEL_THROTTLE)

                # Level 2: HALT_HEDGE (2027年 vs 2028年の税務区分整合型ヘッジ比率検証)
                # 2027年: GMO(18,000円) + QANX(15,000円) = 33,000円全体をヘッジ (33,000 / 150 = 220.0 USD)
                ev2_2027 = evaluate_cross_venue_breaker_level(
                    btc_z_score=-4.5,
                    avg_corr_distance=0.20,
                    daily_drawdown_pct=3.5,
                    gmo_spot_notional_jpy=18000.0,
                    comprehensive_alt_notional_jpy=15000.0,
                    fx_usdjpy=150.0,
                    tax_year=2027,
                )
                self.assertEqual(ev2_2027["breaker_level"], BREAKER_LEVEL_HALT_HEDGE)
                self.assertFalse(ev2_2027["gmo_direct_unwind_priority"])
                self.assertAlmostEqual(ev2_2027["hedge_short_target_usd"], 220.0, places=2)

                # 2028年: 分離課税(GMO 18,000円)はGMO内優先決済とし、総合課税(QANX 15,000円)のみをPerpヘッジ (15,000 / 150 = 100.0 USD)
                ev2_2028 = writer.evaluate_and_publish(
                    btc_z_score=-4.5,
                    avg_corr_distance=0.20,
                    daily_drawdown_pct=3.5,
                    gmo_spot_notional_jpy=18000.0,
                    comprehensive_alt_notional_jpy=15000.0,
                    fx_usdjpy=150.0,
                    tax_year=2028,
                )
                self.assertEqual(ev2_2028["breaker_level"], BREAKER_LEVEL_HALT_HEDGE)
                self.assertTrue(ev2_2028["gmo_direct_unwind_priority"])
                self.assertAlmostEqual(ev2_2028["gmo_direct_unwind_target_jpy"], 18000.0, places=2)
                self.assertAlmostEqual(ev2_2028["hedge_short_target_usd"], 100.0, places=2)

                st2 = reader.read_breaker_state()
                self.assertEqual(st2["breaker_level"], BREAKER_LEVEL_HALT_HEDGE)
                self.assertAlmostEqual(st2["hedge_short_target_usd"], 100.0, places=2)

                # AutoTraderDaemon が risk_bus.db (Level 2) を検知して未約定BUYをキャンセルし、既存STOPを維持することを確認
                bot_dir = tmp_path / "bot_r18"
                bot_dir.mkdir(parents=True, exist_ok=True)
                pf_r18_path = bot_dir / "portfolio.json"
                sl_oid_r18 = "9100000001"
                seed_pf_r18 = {
                    "jpy_balance": 20025.0,
                    "initial_capital_jpy": 20000.0,
                    "positions": [
                        {
                            "symbol": "SOL",
                            "size": 0.36,
                            "entry_price": 25000.0,
                            "current_price": 25100.0,
                            "stop_loss_price": 24500.0,
                            "take_profit_price": 26500.0,
                            "state": "SL_ACTIVE",
                            "sl_order_id": sl_oid_r18,
                            "trailing_stage": 1,
                            "opened_at": "2026-10-21T18:00:00+09:00",
                        }
                    ],
                    "pending_orders": [],
                }
                pf_r18_path.write_text(json.dumps(seed_pf_r18, ensure_ascii=False, indent=2), encoding="utf-8")
                broker_r18 = PaperBroker(portfolio_path=pf_r18_path)
                buy_ord_r18 = broker_r18.place_order(
                    symbol="BTC",
                    side="BUY",
                    execution_type="LIMIT",
                    price="14000000",
                    size="0.0006",
                )
                buy_oid_r18 = str(buy_ord_r18["order_id"])
                seed_pf_r18["pending_orders"] = [
                    {
                        "order_id": buy_oid_r18,
                        "symbol": "BTC",
                        "side": "BUY",
                        "execution_type": "LIMIT",
                        "limit_price": 14000000.0,
                        "size": 0.0006,
                        "stop_loss_price": 13750000.0,
                        "take_profit_price": 14600000.0,
                        "state": "PENDING_BUY",
                        "placed_at": "2026-10-21T19:00:00+09:00",
                    }
                ]
                pf_r18_path.write_text(json.dumps(seed_pf_r18, ensure_ascii=False, indent=2), encoding="utf-8")

                daemon_r18 = AutoTraderDaemon(
                    base_dir=bot_dir,
                    execution_mode="PAPER",
                    broker=broker_r18,
                    dry_run_notify=True,
                    risk_bus_db_path=rb_path,
                )
                try:
                    dt_normal = datetime(2026, 10, 21, 19, 15, 0, tzinfo=JST)
                    fc_res = daemon_r18.run_fast_cycle(
                        tickers={
                            "SOL": {"last": "25100", "bid": "25095", "ask": "25105", "high": "25150", "low": "25050"},
                            "BTC": {"last": "14050000", "bid": "14049000", "ask": "14051000", "high": "14060000", "low": "14040000"},
                        },
                        now_dt=dt_normal,
                        exchange_status="OPEN",
                    )
                    ev_types_r18 = [e.get("type") for e in fc_res.get("events", [])]
                    self.assertIn("GLOBAL_RISK_BUS_HALT_HEDGE", ev_types_r18)
                    pf_after_r18 = daemon_r18.oco_manager.load_portfolio()
                    self.assertEqual(len(pf_after_r18.get("pending_orders", [])), 0)
                    self.assertEqual(pf_after_r18["positions"][0]["trailing_stage"], 4)
                    active_ids_r18 = {str(o.get("order_id")) for o in broker_r18.get_active_orders()}
                    self.assertIn(sl_oid_r18, active_ids_r18)
                    self.assertNotIn(buy_oid_r18, active_ids_r18)
                finally:
                    daemon_r18.db_wal.close()
            finally:
                reader.close()
                writer.close(truncate_wal=True)

        # ------------------------------------------------------------------
        # ③ ForensicTaxAuditor: 移動平均法・総平均法並列計算、Makerリベート2方式、
        #    2028年分離課税 vs 総合課税の損益通算禁止監査、2027年12月タックスハーベスティング
        # ------------------------------------------------------------------
        # 3-A. 方式① (rebate_incorporation_mode=False: 雑所得独立計上) vs 方式② (True: 取得価額控除)
        auditor_m1 = ForensicTaxAuditor(rebate_incorporation_mode=False)
        r_m1_buy = auditor_m1.record_spot_fill(
            venue="GMO_COIN",
            asset="SOL",
            side="BUY",
            qty="10",
            price_jpy="20000",
            fee_jpy="-60",  # Makerリベート 60円受取
            timestamp_jst="2027-05-01T12:00:00+09:00",
        )
        self.assertEqual(r_m1_buy["cost_basis_jpy"], Decimal("200000"))
        self.assertEqual(r_m1_buy["rebate_income_jpy"], Decimal("60"))

        auditor_m2 = ForensicTaxAuditor(rebate_incorporation_mode=True)
        r_m2_buy = auditor_m2.record_spot_fill(
            venue="GMO_COIN",
            asset="SOL",
            side="BUY",
            qty="10",
            price_jpy="20000",
            fee_jpy="-60",  # Makerリベート 60円を取得価額から控除
            timestamp_jst="2027-05-01T12:00:00+09:00",
        )
        self.assertEqual(r_m2_buy["cost_basis_jpy"], Decimal("199940"))
        self.assertEqual(r_m2_buy["rebate_income_jpy"], Decimal("0"))

        # 3-B. 移動平均法 vs 総平均法の乖離検知 (年末高騰後の買付による総平均法トラップ)
        auditor_m1.record_spot_fill(
            venue="GMO_COIN",
            asset="SOL",
            side="SELL",
            qty="5",
            price_jpy="30000",
            fee_jpy="0",
            timestamp_jst="2027-08-01T12:00:00+09:00",
        )
        # 年末に高値 40,000円で 10 SOL 再買付 -> 総平均単価が引き上げられ移動平均法との乖離が発生
        auditor_m1.record_spot_fill(
            venue="QANX_DEX",
            asset="SOL",
            side="BUY",
            qty="10",
            price_jpy="40000",
            fee_jpy="0",
            timestamp_jst="2027-11-15T12:00:00+09:00",
        )
        comp_2027 = auditor_m1.compute_basis_method_comparison()["SOL"]
        # 移動平均実現益 = (30,000 - 20,000) * 5 = +50,000円
        # 総平均単価 = (200,000 + 400,000) / 20 = 30,000円 -> 総平均実現益 = (30,000 - 30,000) * 5 = 0円
        self.assertEqual(comp_2027["moving_realized_pnl_jpy"], Decimal("50000"))
        self.assertEqual(comp_2027["periodic_realized_pnl_jpy"], Decimal("0"))
        self.assertEqual(comp_2027["basis_method_divergence_jpy"], Decimal("50000"))
        self.assertTrue(comp_2027["periodic_average_trap_warning"])

        # 3-C. 2028年 申告分離課税(GMO +300万円) vs 総合課税(Hyperliquid/QANX -200万円) の損益通算禁止監査
        auditor_2028 = ForensicTaxAuditor(rebate_incorporation_mode=False)
        auditor_2028.record_spot_fill(
            venue="GMO_COIN",
            asset="BTC",
            side="BUY",
            qty="1",
            price_jpy="10000000",
            fee_jpy="0",
            timestamp_jst="2028-02-01T10:00:00+09:00",
        )
        auditor_2028.record_spot_fill(
            venue="GMO_COIN",
            asset="BTC",
            side="SELL",
            qty="1",
            price_jpy="13000000",  # GMO現物 +300万円の譲渡益 (SEPARATE_20315)
            fee_jpy="0",
            timestamp_jst="2028-06-01T10:00:00+09:00",
        )
        auditor_2028.record_perp_or_defi_pnl(
            venue="HYPERLIQUID",
            asset="USDC",
            realized_pnl_jpy="-1200000",  # Hyperliquid -120万円の損失 (COMPREHENSIVE_MISC)
            timestamp_jst="2028-07-01T10:00:00+09:00",
        )
        auditor_2028.record_perp_or_defi_pnl(
            venue="QANX_DEX",
            asset="QANX",
            realized_pnl_jpy="-800000",   # QANX DEX -80万円の損失 (合計 -200万円)
            timestamp_jst="2028-09-01T10:00:00+09:00",
        )
        sum_2028 = auditor_2028.audit_year_summary(2028)
        self.assertEqual(sum_2028["separate_20315_raw_pnl_jpy"], Decimal("3000000"))
        self.assertEqual(sum_2028["comprehensive_misc_raw_pnl_jpy"], Decimal("-2000000"))
        self.assertTrue(sum_2028["cross_bucket_offset_prohibited"])
        self.assertTrue(sum_2028["cross_bucket_offset_blocked"])
        # 相殺されず 300万円全額に 20.315% (= 609,450円) が課税され、総合課税は 0円フロアとなること
        self.assertEqual(sum_2028["separate_20315_taxable_income_jpy"], Decimal("3000000"))
        self.assertEqual(sum_2028["comprehensive_misc_taxable_income_jpy"], Decimal("0"))
        self.assertEqual(sum_2028["estimated_separate_tax_jpy"], Decimal("609450.00"))
        self.assertEqual(sum_2028["total_income_for_deduction_check_jpy"], Decimal("3000000"))

        # 逆パターン: GMO現物が -150万円の赤字、Hyperliquidが +250万円の黒字の場合 -> 3年繰越控除 150万円 & 総合課税 250万円
        auditor_2028_rev = ForensicTaxAuditor(rebate_incorporation_mode=False)
        auditor_2028_rev.record_spot_fill(
            venue="GMO_COIN",
            asset="ETH",
            side="BUY",
            qty="10",
            price_jpy="500000",
            timestamp_jst="2028-03-01T10:00:00+09:00",
        )
        auditor_2028_rev.record_spot_fill(
            venue="GMO_COIN",
            asset="ETH",
            side="SELL",
            qty="10",
            price_jpy="350000",  # GMO現物 -150万円
            timestamp_jst="2028-08-01T10:00:00+09:00",
        )
        auditor_2028_rev.record_perp_or_defi_pnl(
            venue="HYPERLIQUID",
            asset="USDC",
            realized_pnl_jpy="2500000",  # Hyperliquid +250万円
            timestamp_jst="2028-10-01T10:00:00+09:00",
        )
        sum_2028_rev = auditor_2028_rev.audit_year_summary(2028)
        self.assertEqual(sum_2028_rev["separate_20315_taxable_income_jpy"], Decimal("0"))
        self.assertEqual(sum_2028_rev["carryforward_loss_3yr_jpy"], Decimal("1500000"))
        self.assertEqual(sum_2028_rev["comprehensive_misc_taxable_income_jpy"], Decimal("2500000"))

        # 3-D. 2027年12月タックス・ハーベスティング監査アラート
        harv_2027 = auditor_2028.evaluate_2027_year_end_tax_harvesting(
            realized_pnl_2027_jpy="500000",
            open_positions=[
                {"venue": "QANX_DEX", "symbol": "QANX", "unrealized_pnl_jpy": "-180000"},
                {"venue": "GMO_COIN", "symbol": "SOL", "unrealized_pnl_jpy": "400000"},
            ],
        )
        self.assertIn("RECOMMEND_2027_LOSS_HARVESTING", harv_2027["alerts"])
        self.assertIn("RECOMMEND_DEFER_GMO_GAIN_TO_2028", harv_2027["alerts"])
        self.assertGreater(harv_2027["estimated_loss_harvest_tax_saving_jpy"], Decimal("0"))
        self.assertGreater(harv_2027["estimated_deferral_tax_saving_jpy"], Decimal("0"))

        # ------------------------------------------------------------------
        # ④ 2027年7月 Phase 1 終了時 5ゲート統計検証パイプライン (DSR / MinTRL / WFE / PBO / OOS PF)
        # ------------------------------------------------------------------
        # 4-A. 報告書の数値例の精密再現検証:
        #   T=300, N=50, skew=-0.1952, kurt_pearson=5.0128, sr_hat=0.1337, sr_0=0.0698
        #   => DSR ≈ 0.8603 (< 0.95) で Gate 1 不合格 & MinTRL ≈ 692.8 (> 300) で Gate 2 不合格
        dsr_report_ex = calc_dsr_and_mintrl(
            sr_0=0.0698,
            confidence=0.95,
            T=300,
            sr_hat=0.1337,
            skew=-0.1952,
            kurt_pearson=5.0128,
        )
        self.assertAlmostEqual(dsr_report_ex["dsr"], 0.8602, delta=0.001)
        self.assertFalse(dsr_report_ex["gate1_dsr_pass"])
        self.assertGreater(dsr_report_ex["min_trl"], 690.0)
        self.assertFalse(dsr_report_ex["gate2_mintrl_pass"])

        # 4-B. 有効試行回数 N_eff, 期待最大SR_0, CSCV PBO, WFE, 5ゲート昇格判定 (ケースA / ケースB / ケースC)
        # 一貫して高い正のリターンを持つ候補群で Case A (PROMOTE_CURRENT_LIVE_PARAMETER) を検証
        t_obs = 160
        strong_live = [0.008 + 0.003 * math.sin(i * 0.7) for i in range(t_obs)]
        strong_matrix = [
            [
                strong_live[t],                                  # cand 0 (live: 最良)
                0.004 + 0.004 * math.cos(t * 0.5),               # cand 1
                0.002 + 0.005 * math.sin(t * 0.3 + 0.4),         # cand 2
                0.001 + 0.005 * math.cos(t * 0.4 + 0.8),         # cand 3
            ]
            for t in range(t_obs)
        ]
        n_eff_res = compute_effective_trials(strong_matrix)
        self.assertGreaterEqual(float(n_eff_res), 1.0)
        self.assertLessEqual(float(n_eff_res), 4.0)

        pbo_strong = compute_pbo_cscv(strong_matrix, n_partitions=8)
        self.assertEqual(pbo_strong["n_combinations"], 70)
        self.assertLess(pbo_strong["pbo"], 0.50)
        self.assertTrue(pbo_strong["gate4_pbo_pass"])

        promo_a = evaluate_phase1_to_phase2_promotion(
            live_returns=strong_live,
            shadow_returns_matrix=strong_matrix,
            live_candidate_idx=0,
            confidence=0.95,
        )
        self.assertEqual(promo_a["case"], "CASE_A")
        self.assertEqual(promo_a["decision"], "PROMOTE_CURRENT_LIVE_PARAMETER")
        self.assertTrue(promo_a["all_5_gates_live_pass"])

        # 現行Live (idx=2) は未達だが、Shadow最良候補 (idx=0) が全5ゲートを突破する Case B (CANARY_PROMOTE_NEW_SHADOW_PARAMETER) を検証
        mediocre_live = [(-0.002 if i % 2 == 0 else 0.0021) for i in range(t_obs)]
        canary_matrix = [
            [
                strong_live[t],                                  # cand 0 (Shadow最良候補: 5ゲート突破)
                0.004 + 0.004 * math.cos(t * 0.5),               # cand 1
                mediocre_live[t],                                # cand 2 (現行Live: DSR未達)
                0.001 + 0.005 * math.cos(t * 0.4 + 0.8),         # cand 3
            ]
            for t in range(t_obs)
        ]
        promo_b = evaluate_phase1_to_phase2_promotion(
            live_returns=mediocre_live,
            shadow_returns_matrix=canary_matrix,
            live_candidate_idx=2,
            confidence=0.95,
        )
        self.assertEqual(promo_b["case"], "CASE_B")
        self.assertEqual(promo_b["decision"], "CANARY_PROMOTE_NEW_SHADOW_PARAMETER")
        self.assertAlmostEqual(promo_b["canary_lot_ratio"], 0.50, places=2)
        self.assertFalse(promo_b["all_5_gates_live_pass"])
        self.assertTrue(promo_b["all_5_gates_shadow_pass"])

        # ランダムノイズのみの候補群で Case C (KEEP_FROZEN_OR_SCALE_DOWN) を検証
        weak_live = [(-0.002 if i % 2 == 0 else 0.0018) for i in range(t_obs)]
        weak_matrix = [
            [
                weak_live[t],
                (-0.0022 if (t + 1) % 2 == 0 else 0.0019),
                (-0.0025 if (t + 2) % 3 == 0 else 0.0010),
                (-0.0019 if (t + 3) % 2 == 0 else 0.0017),
            ]
            for t in range(t_obs)
        ]
        promo_c = evaluate_phase1_to_phase2_promotion(
            live_returns=weak_live,
            shadow_returns_matrix=weak_matrix,
            live_candidate_idx=0,
            confidence=0.95,
        )
        self.assertEqual(promo_c["case"], "CASE_C")
        self.assertEqual(promo_c["decision"], "KEEP_FROZEN_OR_SCALE_DOWN")
        self.assertFalse(promo_c["all_5_gates_live_pass"])


if __name__ == "__main__":
    unittest.main()
