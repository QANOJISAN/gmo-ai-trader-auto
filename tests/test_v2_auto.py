"""
v2（24時間常時監視・GMOコイン現物全自動売買システム）単体・統合テストスイート (tests/test_v2_auto.py)

すべてのテストは一時ディレクトリ (`tempfile.TemporaryDirectory`) 上で隔離実行され、
本番 v1 (`e:\\gmo-ai-trader`) や v2 ルートのデータファイルを一切書き換えない。

【検証項目】
1. テクニカル・レーダー (`TechnicalRadar`):
   - トリガー未該当時の AI 呼び出しスキップ（省エネフィルタ）
   - 上位足上昇トレンド＋1時間足押し目反発による `NEW_ENTRY_OPPORTUNITY` 検知
   - 保有24時間以上経過 / 含み損益変動による `POSITION_REEVALUATION` 検知
   - 前回AI呼び出しから6時間以上経過による `PERIODIC_HEALTH_CHECK` 検知
2. `PaperBroker` & `GMOPrivateClient`:
   - HMAC-SHA256 署名生成 (`API-KEY`, `API-TIMESTAMP`, `API-SIGN`) の正確性
   - Post-Only (`SOK`) 指値が `best_ask` 以上で発注された際の即時失効と1ティック自動調整再発注
   - 現物 SL (`STOP`) 発注中の保有数量ロック (`ERR-201` / `InsufficientAvailableAssetError`) の再現
3. `PseudoOCOManager` & `AutoTraderDaemon` フルライフサイクル (`DRY_RUN`):
   - レーダー検知 ➔ AI判定 (`BUY`) ➔ 自動指値発注 (`PENDING_BUY`)
   - 指値刺さり約定検知 ➔ 自動で取引所へ逆指値 (`STOP`) セット (`SL_ACTIVE`)
   - `+1.0R` 到達時の AI 非介在・自動建値SL移動 (`AUTO_BREAKEVEN_LOCKED`: 旧SL取消 ➔ 新建値微益SL発注)
   - TP到達時の SL 取消ロック解除 ➔ 自動利確売り決済 (`TP_EXECUTED`) ＆ `closed_trades` 記録
   - SL到達時の逆指値約定検知 (`SL_EXECUTED`) ＆ `closed_trades` 記録
   - 未約定のまま4時間 (`ORDER_EXPIRY_HOURS=4`) 経過した買い指値の自動タイムアウト取消 (`BUY_ORDER_EXPIRED`)
   - 部分約定のまま4時間経過した際の「未約定残り取消＋約定済み数量分の自動SLセット」
4. 土曜定期メンテナンス自動スリープ＆再開時 Reconciliation:
   - 土曜 `08:55〜11:04 JST` の自動スリープ (`MAINTENANCE_SLEEP`)
   - 土曜 `11:05 JST` メンテ明けの自動 `get_assets()` / `get_active_orders()` 照合 (`RECONCILED`) と欠落SLの自動再発注
5. 安全機構（1日最大取引回数制限 `MAX_DAILY_TRADES`・API連続エラー時の自動一時停止サーキットブレーカー）
6. `migrate_from_v1.py` プリフライト確認 (`--check`) と一時ファイルへの移行実行検証、および PowerShell スクリプトの Pure ASCII / AST 構文検証
"""

from __future__ import annotations

import hashlib
import hmac
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from auto_trader import AutoTraderDaemon, is_in_saturday_maintenance_window
from migrate_from_v1 import execute_migration, run_preflight_check
from src.ai_judge import AIJudge
from src.gmo_private_client import (
    GMOPrivateClient,
    InsufficientAvailableAssetError,
    PaperBroker,
)
from src.indicators import summarize_1h_indicators, summarize_indicators
from src.notifier import Notifier
from src.pseudo_oco_manager import PseudoOCOManager, calculate_breakeven_sl_price
from src.radar import TechnicalRadar
from src.validator import DEFAULT_SYMBOL_RULES

JST = timezone(timedelta(hours=9))


def _make_bullish_dip_market_summary(symbol: str = "BCH", last_price: float = 44500.0) -> Dict[str, Any]:
    """上位足が上昇トレンドかつ1時間足で押し目反発している市場要約モックを生成する"""
    return {
        "symbol": symbol,
        "climax_sell_warning": False,
        "price": {
            "last": last_price,
            "best_ask": last_price + 10.0,
            "best_bid": last_price - 10.0,
            "high_24h": last_price * 1.03,
            "low_24h": last_price * 0.97,
            "spread_pct": 0.04,
            "change_1d_pct": 2.5,
            "return_7d_pct": 8.5,
            "relative_strength_vs_btc_7d_pct": 4.2,
        },
        "daily_indicators": {
            "trend": "STRONG_UPTREND",
            "sma_7": last_price * 0.99,
            "sma_25": last_price * 0.96,
            "sma_75": last_price * 0.92,
            "rsi_14": 54.0,
            "macd_hist": 120.0,
            "macd_hist_rising": True,
            "atr_14": 900.0,
            "volume_ratio_20": 1.35,
            "return_7d_pct": 8.5,
            "relative_strength_vs_btc_7d_pct": 4.2,
            "climax_sell_warning": False,
        },
        "h4_indicators": {
            "open": last_price * 0.995,
            "close": last_price,
            "change_4h_pct": 0.8,
            "rsi_14": 48.0,
            "macd_hist": 45.0,
            "atr_14": 350.0,
            "sma_25": last_price * 0.985,
            "volume_ratio_20": 1.25,
            "climax_sell_warning": False,
        },
        "h1_indicators": {
            "open": last_price * 0.996,
            "close": last_price,
            "change_1h_pct": 0.4,
            "rsi_14": 41.5,
            "prev_rsi_14": 37.0,
            "macd_hist": 15.0,
            "macd_hist_rising": True,
            "atr_14": 180.0,
            "volume_ratio_20": 1.4,
            "rsi_rebound": True,
            "ema_support_bounce": True,
            "bb_lower_bounce": False,
            "volume_surge": True,
        },
        "orderbook_liquidity": {
            "bid_depth_1pct_jpy": 5000000.0,
            "ask_depth_1pct_jpy": 5000000.0,
            "imbalance_snapshot": 0.15,
        },
        "orderbook_imbalance": 0.15,
    }


class TestV2AutoTradingSystem(unittest.TestCase):
    """v2 24時間全自動売買システムの単体・統合テスト"""

    def test_01_hmac_sha256_private_client_signature(self) -> None:
        """GMOPrivateClient の HMAC-SHA256 署名とパス正規化を検証"""
        client = GMOPrivateClient(api_key="test_key_123", api_secret="test_secret_456")
        self.assertTrue(client.has_credentials())
        headers = client._build_auth_headers("POST", "/v1/order", '{"symbol":"BCH"}')
        self.assertEqual(headers["API-KEY"], "test_key_123")
        ts = headers["API-TIMESTAMP"]
        expected_sign = hmac.new(
            b"test_secret_456",
            f'{ts}POST/v1/order{{"symbol":"BCH"}}'.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(headers["API-SIGN"], expected_sign)

    def test_02_1h_indicators_and_technical_radar_triggers(self) -> None:
        """1時間足指標算出と TechnicalRadar の3大トリガー条件・省エネスキップを検証"""
        # 1h DataFrame の指標計算確認
        base_ms = int(datetime(2026, 10, 21, 0, 0, tzinfo=JST).timestamp() * 1000)
        rows = []
        for i in range(30):
            p = 44000.0 + np.sin(i / 3.0) * 300.0
            rows.append(
                {
                    "open_time": base_ms + i * 3600 * 1000,
                    "open": p - 20,
                    "high": p + 80,
                    "low": p - 80,
                    "close": p + 10,
                    "volume": 100.0 + i * 5.0,
                }
            )
        df_1h = pd.DataFrame(rows)
        h1_sum = summarize_1h_indicators(df_1h)
        self.assertIn("rsi_14", h1_sum)
        self.assertIn("rsi_rebound", h1_sum)
        self.assertIn("ema_support_bounce", h1_sum)

        radar = TechnicalRadar()
        now_jst = datetime(2026, 10, 21, 19, 0, tzinfo=JST)
        pf = {
            "initial_capital_jpy": 10000,
            "jpy_balance": 10000,
            "max_concurrent_positions": 2,
            "drawdown_kill_switch_jpy": 8500,
            "positions": [],
            "pending_orders": [],
        }

        # (A) 全銘柄がレンジ・下降トレンドかつ前回AI呼び出しが1時間前の場合は AI 呼び出しをスキップ
        weak_summary = _make_bullish_dip_market_summary("BTC", 13000000.0)
        weak_summary["daily_indicators"]["trend"] = "DOWNTREND_OR_RANGE"
        weak_summary["h4_indicators"]["sma_25"] = 13500000.0
        eval_skip = radar.evaluate_triggers(
            market_summaries={"BTC": weak_summary},
            portfolio=pf,
            last_ai_call_at=now_jst - timedelta(hours=1),
            now_dt=now_jst,
        )
        self.assertFalse(eval_skip["should_call_ai"])
        self.assertEqual(eval_skip["triggers"], [])

        # (B) 新規エントリー好機 (上位足上昇トレンド＋1h押し目反発)
        bull_bch = _make_bullish_dip_market_summary("BCH", 44500.0)
        eval_entry = radar.evaluate_triggers(
            market_summaries={"BTC": weak_summary, "BCH": bull_bch},
            portfolio=pf,
            last_ai_call_at=now_jst - timedelta(hours=1),
            now_dt=now_jst,
        )
        self.assertTrue(eval_entry["should_call_ai"])
        self.assertIn("NEW_ENTRY_OPPORTUNITY", eval_entry["triggers"])
        self.assertIn("BCH", eval_entry["candidate_symbols"])

        # (C) 保有ポジションが24時間以上経過した場合の POSITION_REEVALUATION トリガー
        pf_held = dict(pf)
        pf_held["positions"] = [
            {
                "symbol": "BCH",
                "size": 0.095,
                "entry_price": 43880.0,
                "take_profit_price": 46500.0,
                "stop_loss_price": 42300.0,
                "opened_at": (now_jst - timedelta(hours=25)).isoformat(timespec="seconds"),
            }
        ]
        eval_pos = radar.evaluate_triggers(
            market_summaries={"BTC": weak_summary},
            portfolio=pf_held,
            last_ai_call_at=now_jst - timedelta(hours=1),
            now_dt=now_jst,
        )
        self.assertTrue(eval_pos["should_call_ai"])
        self.assertIn("POSITION_REEVALUATION", eval_pos["triggers"])

        # (D) 最後のAI呼び出しから6時間以上経過した場合の PERIODIC_HEALTH_CHECK トリガー
        eval_periodic = radar.evaluate_triggers(
            market_summaries={"BTC": weak_summary},
            portfolio=pf,
            last_ai_call_at=now_jst - timedelta(hours=6, minutes=5),
            now_dt=now_jst,
        )
        self.assertTrue(eval_periodic["should_call_ai"])
        self.assertIn("PERIODIC_HEALTH_CHECK", eval_periodic["triggers"])

    def test_03_full_dry_run_lifecycle_buy_fill_sl_breakeven_tp(self) -> None:
        """
        DRY_RUN モードにおける完全自動売買ライフサイクル統合検証:
        レーダー検知 ➔ AI判定(ルールベース) ➔ 自動指値発注(PENDING_BUY) ➔ 約定検知 ➔
        自動SLセット(SL_ACTIVE) ➔ +1.0R自動建値SL移動(AUTO_BREAKEVEN_LOCKED) ➔
        TP到達時のSL取消＆自動利確(TP_EXECUTED)
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                ai_judge=AIJudge(provider="rule_based"),
                notifier=Notifier(),
                dry_run_notify=True,
            )

            t0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=JST)
            bch_summary = _make_bullish_dip_market_summary("BCH", 44500.0)
            btc_summary = _make_bullish_dip_market_summary("BTC", 13000000.0)
            btc_summary["daily_indicators"]["trend"] = "UPTREND"
            btc_summary["daily_indicators"]["relative_strength_vs_btc_7d_pct"] = 0.0

            tickers_t0 = {
                "BCH": {"symbol": "BCH", "ask": 44510.0, "bid": 44490.0, "last": 44500.0, "volume": 1000.0},
                "BTC": {"symbol": "BTC", "ask": 13001000.0, "bid": 12999000.0, "last": 13000000.0, "volume": 500.0},
            }

            # 1. レーダー検知 ➔ AI判定 ➔ 自動買い指値発注 (PENDING_BUY)
            res_ai = daemon.run_radar_and_ai_cycle(
                market_summaries={"BTC": btc_summary, "BCH": bch_summary},
                symbol_rules=DEFAULT_SYMBOL_RULES,
                tickers=tickers_t0,
                now_dt=t0,
            )
            self.assertEqual(res_ai["status"], "AI_EXECUTED")
            self.assertTrue(res_ai["ai_called"])
            decision = res_ai["decision"]
            self.assertEqual(decision["action"], "BUY")
            self.assertEqual(decision["symbol"], "BCH")

            pf_after_buy_order = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_after_buy_order["pending_orders"]), 1)
            pending = pf_after_buy_order["pending_orders"][0]
            self.assertEqual(pending["state"], "PENDING_BUY")
            limit_price = float(pending["limit_price"])
            tp_price = float(pending["take_profit_price"])
            sl_price = float(pending["stop_loss_price"])
            order_size = float(pending["size"])
            self.assertGreater(limit_price, 0)
            self.assertGreater(tp_price, limit_price)
            self.assertLess(sl_price, limit_price)

            # 2. 指値刺さり約定検知 (`last <= limit_price`) ➔ 自動で逆指値 STOP セット (`SL_ACTIVE`)
            t1 = t0 + timedelta(minutes=15)
            tickers_fill = {
                "BCH": {
                    "symbol": "BCH",
                    "ask": limit_price,
                    "bid": limit_price - 10.0,
                    "last": limit_price - 5.0,
                    "volume": 1100.0,
                }
            }
            fast_res_fill = daemon.run_fast_cycle(tickers=tickers_fill, now_dt=t1)
            self.assertEqual(fast_res_fill["status"], "OK")
            fill_events = fast_res_fill["events"]
            self.assertEqual(len(fill_events), 1)
            self.assertEqual(fill_events[0]["type"], "BUY_FILLED_AND_SL_PLACED")

            pf_sl_active = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_sl_active["pending_orders"]), 0)
            self.assertEqual(len(pf_sl_active["positions"]), 1)
            pos = pf_sl_active["positions"][0]
            self.assertEqual(pos["state"], "SL_ACTIVE")
            first_sl_order_id = pos["sl_order_id"]
            self.assertTrue(first_sl_order_id)

            # GMOコイン現物ロック仕様の検証: SL注文をキャンセルせずに別の売り注文を出そうとすると ERR-201 例外が発生する
            with self.assertRaises(InsufficientAvailableAssetError):
                daemon.broker.place_order(
                    symbol="BCH",
                    side="SELL",
                    execution_type="LIMIT",
                    size=order_size,
                    price=tp_price,
                )

            # 3. +1.0R 到達 ➔ AI判定を待たずに自動建値ロック (`AUTO_BREAKEVEN_LOCKED`)
            t2 = t1 + timedelta(minutes=20)
            one_r_dist = limit_price - sl_price
            bid_at_plus_1r = limit_price + one_r_dist + 20.0
            # ただし TP 未満であることを保証
            if bid_at_plus_1r >= tp_price:
                bid_at_plus_1r = (limit_price + tp_price) / 2.0

            tickers_1r = {
                "BCH": {
                    "symbol": "BCH",
                    "ask": bid_at_plus_1r + 10.0,
                    "bid": bid_at_plus_1r,
                    "last": bid_at_plus_1r,
                    "volume": 1200.0,
                }
            }
            fast_res_1r = daemon.run_fast_cycle(tickers=tickers_1r, now_dt=t2)
            events_1r = fast_res_1r["events"]
            self.assertEqual(len(events_1r), 1)
            self.assertEqual(events_1r[0]["type"], "AUTO_BREAKEVEN_LOCKED")
            self.assertEqual(events_1r[0]["old_sl_order_id"], first_sl_order_id)

            expected_be_sl = calculate_breakeven_sl_price(limit_price, "BCH", DEFAULT_SYMBOL_RULES)
            self.assertEqual(events_1r[0]["new_sl_price"], expected_be_sl)
            self.assertGreater(expected_be_sl, limit_price)

            pf_be = daemon.oco_manager.load_portfolio()
            pos_be = pf_be["positions"][0]
            self.assertTrue(pos_be["breakeven_locked"])
            self.assertEqual(pos_be["stop_loss_price"], expected_be_sl)
            second_sl_order_id = pos_be["sl_order_id"]
            self.assertNotEqual(second_sl_order_id, first_sl_order_id)

            # 旧SL注文が確実に CANCELED になっていることを確認
            old_sl_info = daemon.broker.get_orders([first_sl_order_id])[0]
            self.assertEqual(old_sl_info["status"], "CANCELED")

            # 4. TP到達 (`bid >= take_profit_price`) ➔ SL取消＆自動利確 (`TP_EXECUTED`)
            t3 = t2 + timedelta(minutes=30)
            tickers_tp = {
                "BCH": {
                    "symbol": "BCH",
                    "ask": tp_price + 30.0,
                    "bid": tp_price + 10.0,
                    "last": tp_price + 15.0,
                    "volume": 1500.0,
                }
            }
            fast_res_tp = daemon.run_fast_cycle(tickers=tickers_tp, now_dt=t3)
            events_tp = fast_res_tp["events"]
            self.assertEqual(len(events_tp), 1)
            self.assertEqual(events_tp[0]["type"], "TP_EXECUTED")
            self.assertEqual(events_tp[0]["cancelled_sl_order_id"], second_sl_order_id)
            self.assertGreater(events_tp[0]["pnl_jpy"], 0)

            pf_final = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_final["positions"]), 0)
            self.assertEqual(len(pf_final["closed_trades"]), 1)
            closed = pf_final["closed_trades"][0]
            self.assertEqual(closed["reason"], "TP")
            self.assertGreater(closed["pnl_jpy"], 0)
            self.assertGreater(pf_final["jpy_balance"], 10000)
            self.assertEqual(pf_final["realized_pnl_total_jpy"], closed["pnl_jpy"])

    def test_04_unfilled_order_4h_expiry_and_sl_execution(self) -> None:
        """
        - Post-Only (SOK) 板ぶつかり時の1ティック自動調整
        - 未約定のまま4時間 (`ORDER_EXPIRY_HOURS=4`) 経過した買い指値の自動タイムアウト取消
        - SL逆指値の取引所約定検知 (`SL_EXECUTED`)
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            broker = PaperBroker(
                paper_orders_path=tmp_dir / "paper_orders.json",
                portfolio_path=tmp_dir / "portfolio.json",
            )
            oco = PseudoOCOManager(
                broker=broker,
                portfolio_path=tmp_dir / "portfolio.json",
                order_expiry_hours=4.0,
                dry_run_notify=True,
            )

            t0 = datetime(2026, 10, 21, 10, 0, 0, tzinfo=JST)
            # 1) Post-Only (SOK) で ask 以上の指値を出した場合、自動で best_bid (44000) へ1ティック調整されて WAITING になることを確認
            tickers_init = {"BCH": {"symbol": "BCH", "ask": 44010.0, "bid": 44000.0, "last": 44005.0}}
            res_place = oco.place_new_buy_order(
                decision={
                    "symbol": "BCH",
                    "limit_price": 44020,  # ask(44010) より高い -> SOK 1ティック調整発動
                    "size": 0.1,
                    "take_profit_price": 46000,
                    "stop_loss_price": 42500,
                    "expected_loss_jpy": 150,
                    "reasoning": "SOK調整＆4h期限切れテスト",
                },
                tickers=tickers_init,
                now_dt=t0,
            )
            self.assertEqual(res_place["status"], "PENDING_BUY")
            self.assertTrue(res_place["pending_order"]["post_only_adjusted"])
            self.assertEqual(res_place["pending_order"]["limit_price"], 44000.0)

            # 2) 3時間59分経過時点ではまだキャンセルされない
            tickers_higher = {"BCH": {"symbol": "BCH", "ask": 44510.0, "bid": 44500.0, "last": 44505.0}}
            events_before = oco.step(tickers=tickers_higher, now_dt=t0 + timedelta(hours=3, minutes=59))
            self.assertEqual(events_before, [])
            self.assertEqual(len(oco.load_portfolio()["pending_orders"]), 1)

            # 3) 4時間01分経過時点で自動的に `BUY_ORDER_EXPIRED` としてキャンセルされる
            events_expired = oco.step(tickers=tickers_higher, now_dt=t0 + timedelta(hours=4, minutes=1))
            self.assertEqual(len(events_expired), 1)
            self.assertEqual(events_expired[0]["type"], "BUY_ORDER_EXPIRED")
            self.assertEqual(len(oco.load_portfolio()["pending_orders"]), 0)
            self.assertEqual(len(broker.get_active_orders("BCH")), 0)

            # 4) 続けて新規買い注文が約定した後に急落し、SL (`STOP`) が発動・約定検知 (`SL_EXECUTED`) されるケース
            t_buy2 = t0 + timedelta(hours=5)
            oco.place_new_buy_order(
                decision={
                    "symbol": "BCH",
                    "limit_price": 44000,
                    "size": 0.1,
                    "take_profit_price": 46000,
                    "stop_loss_price": 42500,
                    "expected_loss_jpy": 150,
                    "reasoning": "SL約定テスト",
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44100.0, "bid": 44050.0, "last": 44080.0}},
                now_dt=t_buy2,
            )
            # 約定させて SL_ACTIVE へ
            ev_fill = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 43990.0, "bid": 43980.0, "last": 43985.0}},
                now_dt=t_buy2 + timedelta(minutes=5),
            )
            self.assertEqual(ev_fill[0]["type"], "BUY_FILLED_AND_SL_PLACED")

            # 価格が 42,400円 (<= SL 42,500円) へ下落し、取引所の STOP 注文が約定
            ev_sl = oco.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 42420.0, "bid": 42400.0, "last": 42410.0}},
                now_dt=t_buy2 + timedelta(minutes=15),
            )
            self.assertEqual(len(ev_sl), 1)
            self.assertEqual(ev_sl[0]["type"], "SL_EXECUTED")
            pf_after_sl = oco.load_portfolio()
            self.assertEqual(len(pf_after_sl["positions"]), 0)
            self.assertEqual(len(pf_after_sl["closed_trades"]), 1)
            self.assertEqual(pf_after_sl["closed_trades"][0]["reason"], "SL")
            self.assertLess(pf_after_sl["closed_trades"][0]["pnl_jpy"], 0)

    def test_05_saturday_maintenance_sleep_and_reconciliation(self) -> None:
        """
        毎週土曜日 08:55〜11:05 JST のメンテナンス自動スリープと、
        11:05 JST メンテナンス明けの自動照合 (Reconciliation) ＆ 欠落SL再発注を検証
        """
        # 2026-10-24 は土曜日 (weekday == 5)
        sat_0854 = datetime(2026, 10, 24, 8, 54, 0, tzinfo=JST)
        sat_0855 = datetime(2026, 10, 24, 8, 55, 0, tzinfo=JST)
        sat_1000 = datetime(2026, 10, 24, 10, 0, 0, tzinfo=JST)
        sat_1104 = datetime(2026, 10, 24, 11, 4, 59, tzinfo=JST)
        sat_1105 = datetime(2026, 10, 24, 11, 5, 0, tzinfo=JST)

        self.assertFalse(is_in_saturday_maintenance_window(sat_0854))
        self.assertTrue(is_in_saturday_maintenance_window(sat_0855))
        self.assertTrue(is_in_saturday_maintenance_window(sat_1000))
        self.assertTrue(is_in_saturday_maintenance_window(sat_1104))
        self.assertFalse(is_in_saturday_maintenance_window(sat_1105))

        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            daemon = AutoTraderDaemon(base_dir=tmp_dir, execution_mode="DRY_RUN", dry_run_notify=True)

            # メンテ前に保有ポジションがあり、万一メンテ中にSL注文が消えていた場合をシミュレート
            pf = daemon.oco_manager.load_portfolio()
            pf["positions"] = [
                {
                    "symbol": "BCH",
                    "size": 0.095,
                    "entry_price": 43880.0,
                    "take_profit_price": 46500.0,
                    "stop_loss_price": 42300.0,
                    "sl_order_id": "9999999999",  # 存在しない注文ID
                    "state": "SL_ACTIVE",
                    "opened_at": sat_0854.isoformat(timespec="seconds"),
                }
            ]
            daemon.oco_manager.save_portfolio(pf, now_dt=sat_0854)

            # 08:55 JST: 自動スリープに入る
            res_sleep = daemon.run_fast_cycle(
                tickers={"BCH": {"symbol": "BCH", "ask": 44010.0, "bid": 44000.0, "last": 44005.0}},
                now_dt=sat_0855,
            )
            self.assertEqual(res_sleep["status"], "MAINTENANCE_SLEEP")
            self.assertTrue(daemon.in_maintenance_sleep)

            # 11:05 JST: メンテ明けに自動で reconcile_after_maintenance() が走り、欠落していた SL 注文が再発注される
            res_wake = daemon.run_fast_cycle(
                tickers={"BCH": {"symbol": "BCH", "ask": 44010.0, "bid": 44000.0, "last": 44005.0}},
                now_dt=sat_1105,
            )
            self.assertEqual(res_wake["status"], "OK")
            self.assertFalse(daemon.in_maintenance_sleep)
            self.assertIsNotNone(res_wake["reconciled"])
            self.assertEqual(res_wake["reconciled"]["status"], "RECONCILED")
            self.assertEqual(res_wake["reconciled"]["active_orders_count"], 0)  # 再発注前のカウント

            pf_reconciled = daemon.oco_manager.load_portfolio()
            new_sl_id = pf_reconciled["positions"][0]["sl_order_id"]
            self.assertNotEqual(new_sl_id, "9999999999")
            self.assertEqual(len(daemon.broker.get_active_orders("BCH")), 1)

    def test_06_migration_check_and_powershell_ast_ascii(self) -> None:
        """
        `migrate_from_v1.py --check` / `execute_migration`（一時ディレクトリ出力）および
        PowerShell スクリプト (`run_v2_daemon.ps1`, `register_v2_task.ps1`) の Pure ASCII & AST 構文検証
        """
        v1_pf_path = Path(r"e:\gmo-ai-trader\portfolio.json")
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_v2_pf = Path(tmp_dir_str) / "portfolio.json"
            check_res = run_preflight_check(v1_portfolio_path=v1_pf_path, v2_portfolio_path=tmp_v2_pf)
            self.assertTrue(check_res["v1_portfolio_exists"])
            self.assertTrue(check_res["valid_v1_portfolio"])
            self.assertEqual(check_res["errors"], [])
            self.assertFalse(tmp_v2_pf.exists())  # --check は一切ファイルを書き換えない

            exec_res = execute_migration(v1_portfolio_path=v1_pf_path, v2_portfolio_path=tmp_v2_pf)
            self.assertTrue(exec_res["migrated"])
            self.assertTrue(tmp_v2_pf.exists())

        # PowerShell スクリプトの Pure ASCII 検証 & PowerShell AST パース検証
        for ps_name in ("run_v2_daemon.ps1", "register_v2_task.ps1"):
            ps_path = ROOT_DIR / ps_name
            raw_bytes = ps_path.read_bytes()
            non_ascii = [b for b in raw_bytes if b > 127]
            self.assertEqual(len(non_ascii), 0, f"{ps_name} に非ASCIIバイトが含まれています")

            cmd = [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                (
                    f"$errors = $null; "
                    f"[void][System.Management.Automation.Language.Parser]::ParseFile('{ps_path}', [ref]$null, [ref]$errors); "
                    f"if ($errors.Count -gt 0) {{ Write-Error ($errors | Out-String); exit 1 }}"
                ),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            self.assertEqual(proc.returncode, 0, f"{ps_name} AST parse failed: {proc.stderr}")

    def test_07_partial_fill_expiry_and_safety_circuit_breakers(self) -> None:
        """
        - 部分約定のまま4時間経過した際の「残りキャンセル＋約定分のみのSLセット」
        - 1日の最大取引回数制限 (`MAX_DAILY_TRADES`) 到達時の HOLD 強制オーバーライド
        - API連続エラー (`MAX_CONSECUTIVE_API_ERRORS`) 到達時の自動一時停止 (`CIRCUIT_BREAKER_PAUSED`)
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            daemon = AutoTraderDaemon(
                base_dir=tmp_dir,
                execution_mode="DRY_RUN",
                ai_judge=AIJudge(provider="rule_based"),
                max_daily_trades=1,
                max_consecutive_api_errors=3,
                dry_run_notify=True,
            )

            t0 = datetime(2026, 10, 21, 13, 0, 0, tzinfo=JST)
            # 1) 買い指値 0.2 BCH を発注後、半分 (0.1 BCH) だけ部分約定した状態で4時間経過させる
            res_place = daemon.oco_manager.place_new_buy_order(
                decision={
                    "symbol": "BCH",
                    "limit_price": 43800,
                    "size": 0.2,
                    "take_profit_price": 46000,
                    "stop_loss_price": 42300,
                    "expected_loss_jpy": 150,
                    "reasoning": "部分約定4h期限テスト",
                },
                tickers={"BCH": {"symbol": "BCH", "ask": 44000.0, "bid": 43900.0, "last": 43950.0}},
                now_dt=t0,
            )
            oid = res_place["pending_order"]["order_id"]

            # PaperBroker 内部で 0.1 BCH の部分約定をシミュレート
            state = daemon.broker._ensure_state_file()
            for o in state["orders"]:
                if str(o["order_id"]) == oid:
                    daemon.broker._fill_order_in_state(
                        state=state,
                        order_record=o,
                        fill_price=43800.0,
                        is_maker=True,
                        now_str=(t0 + timedelta(hours=1)).isoformat(timespec="seconds"),
                        fill_size=0.1,
                    )
            daemon.broker._save_state(state)

            # 4時間05分経過 ➔ 未約定残り (0.1 BCH) はキャンセルされ、約定済み (0.1 BCH) のみ SL_ACTIVE になる
            events_partial = daemon.oco_manager.step(
                tickers={"BCH": {"symbol": "BCH", "ask": 44100.0, "bid": 44050.0, "last": 44080.0}},
                now_dt=t0 + timedelta(hours=4, minutes=5),
            )
            self.assertEqual(len(events_partial), 1)
            self.assertEqual(events_partial[0]["type"], "BUY_FILLED_AND_SL_PLACED")
            self.assertTrue(events_partial[0]["partial_expired"])
            self.assertAlmostEqual(events_partial[0]["size"], 0.1)

            pf_partial = daemon.oco_manager.load_portfolio()
            self.assertEqual(len(pf_partial["positions"]), 1)
            self.assertAlmostEqual(pf_partial["positions"][0]["size"], 0.1)

            # 2) 1日の最大取引回数 (max_daily_trades=1) 到達後の新規 BUY が HOLD に自動補正されることを確認
            daemon._increment_daily_trade_count(t0)
            btc_summary = _make_bullish_dip_market_summary("BTC", 13000000.0)
            sol_summary = _make_bullish_dip_market_summary("SOL", 24000.0)
            res_limit = daemon.run_radar_and_ai_cycle(
                market_summaries={"BTC": btc_summary, "SOL": sol_summary},
                symbol_rules=DEFAULT_SYMBOL_RULES,
                tickers={
                    "BTC": {"symbol": "BTC", "ask": 13001000.0, "bid": 12999000.0, "last": 13000000.0},
                    "SOL": {"symbol": "SOL", "ask": 24010.0, "bid": 23990.0, "last": 24000.0},
                },
                force_ai=True,
                now_dt=t0 + timedelta(hours=4, minutes=10),
            )
            self.assertEqual(res_limit["decision"]["action"], "HOLD")
            self.assertTrue(res_limit["decision"].get("daily_trade_limit_reached"))

            # 3) API連続エラー 3回で CIRCUIT_BREAKER_PAUSED になることを確認
            t_err = t0 + timedelta(hours=4, minutes=20)
            for _ in range(3):
                daemon.record_api_error(RuntimeError("Simulated API timeout"), now_dt=t_err)
            res_paused = daemon.run_fast_cycle(now_dt=t_err + timedelta(minutes=1))
            self.assertEqual(res_paused["status"], "CIRCUIT_BREAKER_PAUSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
