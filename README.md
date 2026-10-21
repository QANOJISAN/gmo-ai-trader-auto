# 🤖 GMOコイン 24時間常時監視・現物全自動売買システム (v2: `e:\gmo-ai-trader-auto`)

現行の v1 システム（`e:\gmo-ai-trader`：毎日17:00 JST 定時判定＋手動発注支援）を完全に維持・稼働させたまま、第2の選択肢として構築された **「v2：24時間常時監視・GMOコイン現物全自動売買システム」** です。

---

## 🛡️ 設計方針と安全機構

1. **既定モードは安全な `DRY_RUN`（仮想約定ペーパートレード）**
   - `.env` の `EXECUTION_MODE=DRY_RUN`（初期値）では、GMOコイン Private API を一切呼び出さず、Public API のリアルタイム板（`ask` / `bid` / `last`）と照合して `paper_orders.json` 上で本物そっくりの仮想約定シミュレーション（`PaperBroker`）を行います。
   - 現行稼働中の v1（`e:\gmo-ai-trader`）や GMOコイン実口座に一切干渉しません。
2. **現物 API 向け「擬似 OCO ＆ 自動トレーリング」ステートマシン（[`src/pseudo_oco_manager.py`](file:///e:/gmo-ai-trader-auto/src/pseudo_oco_manager.py)）**
   - GMOコイン現物 Private API の「単発注文のみ（OCO非対応）・逆指値 SL 発注中は現物保有数量がロックされる仕様」を完全に制御します：
     * **状態1 `PENDING_BUY`**：Post-Only (`SOK`) で買い指値を発注（板が動いて即時失効した場合は1ティック自動調整して再試行）。約定を検知したら直ちにサーバー側へ損切り逆指値（`STOP` / `FAK`）を自動発注して `SL_ACTIVE` へ遷移。また、`ORDER_EXPIRY_HOURS`（既定4時間）経過しても未約定なら自動キャンセル。
     * **状態2 `SL_ACTIVE`**：取引所サーバー側に `STOP` 注文が置かれており、PCが停止しても下値が保護されている状態。
     * **自動建値ロック（Auto-Breakeven）**：現在価格（Bid）が `+1.0R` 以上に到達し、かつ現在 SL が建値微益 SL（`entry_price * 1.002` の `tick_size` 切り上げ）未満の場合、AI判定を待たずに自動で「旧 SL 注文を `cancel_order` ➔ 新しい建値微益 SL 注文を `place_order`」して利益をロック。
     * **利確スイッチ（TP Execution）**：現在価格（Bid）が `take_profit_price` 以上に到達したら、即座に「発注中の SL 注文を `cancel_order` してロック解除 ➔ 直ちに利確売り注文を `place_order`」して決済完了。
3. **24時間テクニカル・レーダー（AI呼び出し最適化フィルタ：[`src/radar.py`](file:///e:/gmo-ai-trader-auto/src/radar.py)）**
   - 15分おきにスクリーニング銘柄の日足・4時間足・1時間足をスキャンし、無条件に毎回 Gemini を呼ぶのではなく、以下のトリガー条件を満たしたときだけ [`AIJudge`](file:///e:/gmo-ai-trader-auto/src/ai_judge.py) を呼び出します：
     1. **新規エントリー好機**：空き枠（保有＋待機指値が2枠未満かつキルスイッチ未発動）があり、上位足トレンドと1時間足の押し目反発が合致した銘柄が1つ以上あるとき
     2. **保有ポジション再評価**：保有ポジションが24時間以上経過（タイムストップ審査）、または含み損益・R倍数が大きく変動したとき
     3. **定期ヘルスチェック**：最後の AI 呼び出しから6時間以上経過したとき
4. **土曜定期メンテナンス自動スリープ＆再開時 Reconciliation（[`auto_trader.py`](file:///e:/gmo-ai-trader-auto/auto_trader.py)）**
   - 毎週土曜日 `08:55〜11:05 JST` は自動的に API 発注・変更をスリープし、`11:05 JST` に自動で `get_assets()` と `get_active_orders()` を照合（Reconciliation）して再開します。

---

## 🚀 将来 v1 から v2 (`LIVE` モード) へ移行する手順

1. **v1 の定時タスクを停止**
   ```powershell
   Disable-ScheduledTask -TaskName "GMO-AI-Trader-Daily"
   ```
2. **`.env` に GMOコイン Private API キーを設定**
   - `e:\gmo-ai-trader-auto\.env` の `GMO_API_KEY` と `GMO_API_SECRET` を設定します。
3. **移行プリフライトチェック（確認モード）を実行**
   ```powershell
   e:\gmo-ai-trader\.venv\Scripts\python.exe -B e:\gmo-ai-trader-auto\migrate_from_v1.py --check
   ```
4. **v1 の `portfolio.json` を v2 へ引き継ぐ（実行モード）**
   ```powershell
   e:\gmo-ai-trader\.venv\Scripts\python.exe -B e:\gmo-ai-trader-auto\migrate_from_v1.py --execute
   ```
5. **`EXECUTION_MODE=LIVE` に変更して v2 デーモンを起動（またはタスクスケジューラ登録）**
   ```powershell
   # タスクスケジューラへ登録する場合
   powershell -ExecutionPolicy Bypass -File e:\gmo-ai-trader-auto\register_v2_task.ps1
   ```
