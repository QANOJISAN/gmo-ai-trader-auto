# 🤖 GMOコイン 24時間常時監視・現物全自動売買システム (v2: `e:\gmo-ai-trader-auto`)

旧v1システム（手動発注支援、現在は廃止・`archive_v1/` に最終記録のみ退避）の後継となる **「v2：24時間常時監視・GMOコイン現物全自動売買システム（全8本ディープリサーチ統合フルスペック版）」** です。

---

## 🛡️ 実装済み 12大コアメカニズム（Gemini / ChatGPT / Claude 全8本リサーチ完全統合）

1. **SQLite WAL シングルライター台帳 ＆ 日本税制（総平均法・移動平均法 Method A/B）完全対応（[`src/database_wal.py`](file:///e:/gmo-ai-trader-auto/src/database_wal.py)）**
   - `PRAGMA journal_mode=WAL` / `synchronous=FULL` / `busy_timeout=5000` による単一ライター直列化。
   - 注文状態遷移の単調増加ラティス（`INTENT_LOGGED(10)` ➔ `ORDERED(20)` ➔ `PARTIALLY_FILLED(30)` ➔ `CANCELED/EXPIRED(90)` / `EXECUTED(100)`）で WebSocket と REST の順序逆転を完全防御。
   - `Decimal`（文字列）と `satoshi_int`（$\times 10^8$ 整数）のハイブリッド保存、および国税庁（NTA）準拠の移動平均法（Method A: 毎回1円未満切り上げ / Method B: 売却時一括切り上げ）と年間総平均法ビュー（`v_annual_total_average_summary`）を自動算出。
2. **優先度付きトークンバケット・時刻自動校正・現物 Private API クライアント（[`src/gmo_private_client.py`](file:///e:/gmo-ai-trader-auto/src/gmo_private_client.py)）**
   - `PriorityTokenBucket`: GET / POST 独立バケットを 3階層（Tier 0: 緊急SL・成行脱出・キャンセル、Tier 1: 通常発注・資産照会、Tier 2: バックグラウンド照合）に分割し、レート制限下でも損切り・緊急脱出の枠を100%死守。
   - `ERR-5008` / `ERR-5009` 検知時の Cristian アルゴリズムによる `clock_offset_ms` 自動時刻校正。
   - `ERR-5003` / タイムアウト時の「盲目的再発注禁止（先に `activeOrders` / `latestExecutions` を照会）」による二重発注防止。
3. **WebSocket × REST ハイブリッド同期エンジン ＆ 4フェーズ Pre-Subscription Buffered Sync（[`src/ws_hybrid_engine.py`](file:///e:/gmo-ai-trader-auto/src/ws_hybrid_engine.py)）**
   - 35分周期の `PUT /private/v1/ws-auth` トークン自動延長（失敗時は `POST` 再取得フォールバック）。
   - `1.2秒` 直列購読間隔、`trades` チャンネル `TAKER_ONLY` 指定、75秒無通信ハートビート再接続。
   - 4フェーズ同期（Phase 1: WS購読＋メモリキュー蓄積 ➔ Phase 2: RESTスナップショット取得 ➔ Phase 3: メモリキューを単調ラティス＆`executionId` べき等適用 ➔ Phase 4: `LIVE` 遷移）。
4. **3層マイクロストラクチャ逆選択（Adverse Selection）防御モジュール（[`src/adverse_selection_guard.py`](file:///e:/gmo-ai-trader-auto/src/adverse_selection_guard.py)）**
   - Layer 1（板不均衡 $OBI_5$ & Level-1 買板劣化検知）、Layer 2（60秒 Taker Flow Imbalance $TFI_{60s}$ & 5秒5連続価格切り下げ SELL スイープ検知 & パッシブ・オフセット指値）、Layer 3（短期急落 $\Delta P_{60s} < -0.8 \times ATR_{5m}$ & データ鮮度 5.0秒チェック）。
5. **WAL意図ログ付き 8状態擬似OCO・原子的成行決済・4段階トレーリングSL（[`src/pseudo_oco_manager.py`](file:///e:/gmo-ai-trader-auto/src/pseudo_oco_manager.py)）**
   - 0.2秒周期の Fast Recovery Guardian（未保護状態を `< 0.5秒` で検知し STOP 再設置または緊急成行決済）。
   - `MARKET / FAK / SELL` + `cancel_before=True` による原子的緊急決済（`ERR-201` 発生時は実残高再取得リトライ、最小ロット未満は `DUST_QUARANTINED` 隔離）。
   - フラッシュクラッシュ等で `STOP (FAK)` が一部約定＋残量失効した際の自動成行追撃掃討。
   - 4段階トレーリング SL（`+1.0R` 建値微益ロック、`+1.5R` $+0.65R$ ロック、`+2.0R` シャンデリア追従＋$0.20R$ ヒステリシス）。
6. **3-Way Reconciliation（3者照合・自己修復）＆ 06:00 JST 手数料端数調整吸収（[`src/reconciler.py`](file:///e:/gmo-ai-trader-auto/src/reconciler.py)）**
   - 孤児注文（Orphan Orders）・幽霊注文（Ghost Orders）・裸の現物ポジション（Naked Positions）を検知し自動修復。
   - 毎朝 `06:00〜06:15 JST` の `±1〜50円` 以内の日本円残高差分を `FEE_REFUND_ADJUSTMENT` として自動吸収し、それ以外の `> 50円` 異常乖離は `DEGRADED_RECONCILING` へ移行して新規発注をロック。
7. **4段階銘柄スクリーニングゲート ＆ BTCマクロレジーム・ヒステリシス（[`src/radar.py`](file:///e:/gmo-ai-trader-auto/src/radar.py), [`src/regime_filter.py`](file:///e:/gmo-ai-trader-auto/src/regime_filter.py)）**
   - Gate 0（ステーブルコイン除外）、Gate 1（Decimal 文字列量子化）、Gate 2（流動性・スプレッド審査）、Gate 3（1万円口座リスク予算＆最小ロットSL距離審査）。
   - BTC レジームフィルタ（`RISK_OFF_CRASH` 検知時の新規 BUY 即時遮断・待機 BUY 一括取消・解除後2時間ヒステリシス冷却）。
8. **AI 「拒否権（Veto）＋リスク調整」化・Counter-Thesis First・Stale Signal Gate（[`src/ai_judge.py`](file:///e:/gmo-ai-trader-auto/src/ai_judge.py), [`src/llm_shadow_evaluator.py`](file:///e:/gmo-ai-trader-auto/src/llm_shadow_evaluator.py)）**
   - 弱気反証（`bearish_counter_thesis` 最低2項目）を先に列挙させ、`conviction_score >= 75` 未満や R:R `< 1.5` は `HOLD` へ強制降格。
   - AI 推論中に価格が `> 0.30%` 乖離した場合は Stale Signal Gate で発注を破棄。
   - 4h / 12h / 24h ホライズンの経路依存 MFE / MAE シャドウ評価器を搭載。
9. **5階層サーキットブレーカー（L1〜L5）＆ Windows 常駐耐障害性（[`src/circuit_breaker.py`](file:///e:/gmo-ai-trader-auto/src/circuit_breaker.py), [`auto_trader.py`](file:///e:/gmo-ai-trader-auto/auto_trader.py)）**
   - L1（個別銘柄除外）〜 L5（`Best Bid × (1 - Taker手数料)` 保守的純資産 `< 8,500円` 3回連続検知でハードキルスイッチ発動・全ポジション成行決済）。
   - Windows `SetThreadExecutionState(0x80000041)` スリープ抑止、`SingleInstanceLock` 二重起動防止、土曜 `08:50 JST` メンテ前待機 BUY 自動クリア＆ `11:05 JST` `PREOPEN` 待機。
10. **リモート緊急制御・リプレイ攻撃防御・3階層通知ディスパッチャ（[`src/remote_control.py`](file:///e:/gmo-ai-trader-auto/src/remote_control.py)）**
    - `PAUSE` / `CANCEL_ENTRIES`（既存 STOP 注文は絶対維持）と、`FLATTEN_ALL`（有効期限60秒のワンタイム `nonce` による2段階承認＋リプレイ攻撃完全拒否）。
11. **最小ロット実弾プローブ検証器 ＆ 保守的 SOK 貫通バックテスター（[`src/live_probe.py`](file:///e:/gmo-ai-trader-auto/src/live_probe.py), [`src/wfa_backtester.py`](file:///e:/gmo-ai-trader-auto/src/wfa_backtester.py)）**
    - `GMOCoinLiveProbe`: `DOGE` 最小ロット（10 DOGE）で SOK / STOP / `changeOrder` 仕様を実測し `capabilities.json` を出力（`try...finally` で残存注文ゼロを100%保証）。
    - `ConservativeSpotBacktester`: SOK 厳格貫通モデル（`Low < P_limit`）＋非線形 STOP スリッページ＋ローリング WFA（IS=60日 / OOS=20日）。
12. **全8カテゴリ・64項目＋4大複合障害 カオスエンジニアリング監査スイート（[`tests/test_v2_chaos_audit.py`](file:///e:/gmo-ai-trader-auto/tests/test_v2_chaos_audit.py)）**

---

## 🧪 テスト＆CLI実行コマンド

```powershell
# 全テストスイート（基本テスト7本 ＋ カオス監査スイート64項目＆4複合シナリオ）の実行
$env:PYTHONDONTWRITEBYTECODE = "1"
e:\gmo-ai-trader\.venv\Scripts\python.exe -B -m unittest discover -s e:\gmo-ai-trader-auto\tests -p "test_*.py" -v

# 保守的WFAバックテスト実行
e:\gmo-ai-trader\.venv\Scripts\python.exe -B e:\gmo-ai-trader-auto\auto_trader.py --backtest

# 最小ロット実弾プローブ検証
e:\gmo-ai-trader\.venv\Scripts\python.exe -B e:\gmo-ai-trader-auto\auto_trader.py --probe-live
```
