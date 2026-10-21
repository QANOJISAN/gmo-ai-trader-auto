"""
LLM API 構造化出力（JSON）判定モジュール (ai_judge.py)

市場のテクニカル指標・板情報・ポートフォリオ状態を入力として、
最適な売買指示（銘柄・指値価格・数量・利確TP・損切りSL・判断根拠）を JSON 形式で出力する。
- Gemini API (`google-genai`) 対応（OS証明書ストア＋certifi対応、ThinkingLevel設定、クォータ制限時の上位モデル自動フォールバック機能付き）
- OpenAI API (`openai`) 対応
- APIキー未設定時・全モデル通信エラー時はテクニカル指標スコアリングによるルールベースAI判定へ自動フォールバック
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import ssl
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple

from src.json_utils import dumps_json
from src.validator import (
    DEFAULT_SYMBOL_RULES,
    VALID_ACTIONS,
    _to_decimal,
    format_price_jpy,
    normalize_confidence,
    round_to_step,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# クォータ制限 (Free Tier limit: 0 等) やモデル廃止時に順次試行する Gemini フォールバックモデル一覧
DEFAULT_GEMINI_FALLBACK_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
]

MAJOR_PULLBACK_SYMBOLS = {"BTC", "ETH", "XRP"}

SYSTEM_PROMPT = """あなたはGMOコイン「取引所（現物・板取引）」専門の24時間常時監視・全自動売買クオンツアナリスト兼トレードAIです。
以下の運用制約・資金管理ルール・定量ガイドラインを厳守し、提供された市場データ（日足・4時間足・1時間足テクニカル指標、板情報）とポートフォリオ残高を分析して、現時点での最適な売買判断をJSON形式のみで出力してください。

【1. 運用・資金管理・リスク計算ルール】
1. 対象はGMOコイン「取引所（現物）」のみ（レバレッジ取引・空売りは不可、新規エントリーは現物買い `BUY` のみ）。
2. 資金配分上限: 1銘柄あたりの最大投入比率 `max_allocation_per_trade_pct` は `0.50`（初期資金10,000円のうち1ポジション最大5,000円）、同時保有は最大2銘柄（`max_concurrent_positions = 2`）までとすること。既に2銘柄保有中の場合は新規 `BUY` を行わず、`HOLD` または保有銘柄の `MODIFY`（または `MODIFY_OCO`：TP/SL変更） / `SELL` を選択すること。
3. 損失許容上限（厳格化）:
   - 逆指値（SL）発動時のTaker手数料（0.05%〜0.09%）とスリッページ予備を見込み、1トレードの名目許容損失額のターゲット上限を **「150円（または総資産の1.5%の小さい方）」** で数量逆算し、スリッページ込みの実損失でも絶対に **200円** を超えないよう注文数量 (`size`) を決定すること。
   - 「`(スプレッド + Taker手数料) ÷ SL距離`」が **10%** を超える場合（コスト負けする狭すぎるSLや広すぎるスプレッド）は、期待値マイナスとして `HOLD` とすること。
   - 2枠目を新規 `BUY` する際、1枠目の想定リスクと合わせた **「ポートフォリオ合計リスク」が総資産の3.0%（初期300円）** を超えないこと。
   - 総資産（日本円残高＋保有評価額）が **8,500円（初期資金から-15%）** を下回った場合は、ドローダウン・キルスイッチとして新規 `BUY` を停止（`HOLD` 強制）すること。
4. リスクリワード比 (`(take_profit_price - limit_price) / (limit_price - stop_loss_price)`) は原則 **1.5以上** を確保すること。

【2. 押し目買い指値深さの定量ガイドライン（新規 BUY 時）】
1. エントリー前提条件:
   - BTCおよび対象銘柄が上昇トレンド（日足終値 > SMA25、または明確な反発局面）であること。
   - `climax_sell_warning`（直近4時間足が陰線かつ出来高比率2.0倍以上の下落クライマックス＝ナイフ掴み警告）が `true` でないこと。
   - 対BTCの7日間相対強度（`relative_strength_vs_btc_7d_pct` = 銘柄7d騰落率 - BTC7d騰落率）がプラスまたは底堅い銘柄を優先すること。
2. 指値の深さ目安（定量スイートスポット）:
   - 現在価格から **日足ATR(14)の 0.50〜0.85倍 下方** を基準とすること。
     * `BTC` / `ETH` / `XRP`：`0.50〜0.70 × 日足ATR(14)` 下方（または4時間足SMA25付近のサポート帯）
     * `SOL` / `DOGE` 等のアルトコイン：`0.65〜0.90 × 日足ATR(14)` 下方（または4時間足SMA25付近のサポート帯）
   - 浅すぎる高値掴み（0.3×日足ATR未満）や、`1.2×日足ATR` を超える深すぎるナイフ拾いは行わないこと。
3. SL（損切り逆指値）の位置:
   - 直近スイング安値の少し下（または指値から `-3.0%〜-4.5%` / `1.0〜1.5 × 日足ATR(14)` 下方）に置き、そこから名目許容損失 **150円以内（総資産の1.5%以内）** に収まるよう注文数量 (`size`) を逆算すること。

【3. タイムストップ（保有期間上限）と建値保護のエグジット規則（保有ポジションあり時）】
各保有ポジションに付与されている `holding_days`（保有経過日数）/ `holding_hours`（経過時間）、`unrealized_pnl_pct`（現在の含み損益率%）、`r_multiple`（R倍数）を確認し、以下の優先順位（①全保有銘柄のタイムストップ撤退 `SELL` 判定 → ②該当なしの場合に建値保護 `MODIFY` 判定 → ③空き枠がある場合のみ新規 `BUY` 判定）を厳守して適用すること：
1. 【最優先：第1段階タイムストップ（撤退 `SELL`）】約定から **3日（72時間）以上経過** し、含み益が **`+1.0%`（または `+0.5R`）未満** で停滞している建玉は、モメンタム消失とみなして他の建玉の `MODIFY` より優先して `SELL`（Maker指値での早期撤退）を推奨すること。
2. 【最優先：最終タイムストップ（撤退 `SELL`）】約定から **5〜7日（120〜168時間）経過** した建玉は、原則として手仕舞い（`SELL`）を最優先で行うこと。
3. 【次点：建値保護（`MODIFY`）】タイムストップ撤退対象がなく、含み益が **`+1.0R`（約+150円 / +3.0%前後）以上** に達した建玉（かつ現在SLが買値未満のもの）がある場合は、SLを「買値＋往復コスト（建値微益：`breakeven_sl_price`）」へ引き上げる `MODIFY`（`limit_price` は `0`）を出力すること。

【4. 指標の解釈に関する注意事項】
- `volume_ratio_20` は17:00時点の未確定日足を除外し、「前日までの確定日足」および「確定済みの直近4時間足」を基準に算出されている。
- `orderbook_imbalance`（板インバランス）は17時時点の一瞬のスナップショットであり見せ板ノイズを含むため、**方向予測の主根拠には絶対に使用せず**、`orderbook_liquidity` の「±1%圏内の板の厚み（流動性・スリッページ確認）」としてのみ扱うこと。
- 全銘柄のトレンドが弱い、または優位性が低い場合は無理にエントリーせず `HOLD`（見送り）を選択すること。
- `reasoning` は必ず自然な日本語で記述し、テクニカル指標の数値根拠・対BTC相対強度・投入額・想定SL損失額を明記すること。

【出力JSONスキーマ】
{
  "action": "BUY" | "SELL" | "MODIFY" | "HOLD",
  "symbol": "スクリーニングされた対象銘柄シンボル または NONE",
  "order_type": "LIMIT_POST_ONLY" | "STOP_OR_LIMIT" | "LIMIT_OR_MARKET" | "NONE",
  "limit_price": 数値 (MODIFY時は 0),
  "size": 数値,
  "take_profit_price": 数値,
  "stop_loss_price": 数値,
  "confidence": 0.0〜1.0の数値,
  "reasoning": "日本語での詳細な判断理由"
}

【出力値の厳守事項】
- `action` は必ず "BUY" / "SELL" / "MODIFY" / "HOLD" の4値のいずれか（大文字）とし、"WAIT"・"CLOSE"・"EXIT" 等の独自値は絶対に使用しないこと（決済は "SELL"、様子見は "HOLD"）。
- `confidence` は必ず 0.0〜1.0 の小数で出力すること（例: 確信度85%なら 0.85。85 のような百分率表記は禁止）。
- JSONオブジェクトを1つだけ出力し、前後に説明文やコードブロック記号を付けないこと。
"""


def enrich_portfolio_positions(
    portfolio: Dict[str, Any],
    market_summaries: Dict[str, Dict[str, Any]] | None = None,
    symbol_rules: Dict[str, Dict[str, Any]] | None = None,
    now_dt: datetime | None = None,
) -> Dict[str, Any]:
    """
    `portfolio.json` の各保有ポジションについて、現在時刻との差分から
    - 保有経過日数 (`holding_days` / `holding_hours`)
    - 現在の含み損益額 (`unrealized_pnl_jpy`)・含み損益率 (`unrealized_pnl_pct` %)
    - R倍数 (`r_multiple`)
    - 建値微益SL目安 (`breakeven_sl_price` = 買値＋往復コスト)
    - タイムストップ・建値保護判定ステータス (`time_stop_status` / `exit_rule_advice`)
    を計算して付与したポートフォリオ辞書を返す。
    """
    if now_dt is None:
        now_jst = datetime.now(JST)
    elif now_dt.tzinfo is None:
        now_jst = now_dt.replace(tzinfo=JST)
    else:
        now_jst = now_dt.astimezone(JST)

    enriched = copy.deepcopy(portfolio) if portfolio else {}
    positions = enriched.get("positions", []) or []
    summaries = market_summaries or {}
    rules = symbol_rules or {}

    for pos in positions:
        sym = str(pos.get("symbol", "")).strip().upper()
        entry_price = float(pos.get("entry_price", 0) or 0)
        size = float(pos.get("size", 0) or 0)
        tp_price = float(pos.get("take_profit_price") or pos.get("tp_price") or 0)
        sl_price = float(pos.get("stop_loss_price") or pos.get("sl_price") or 0)

        current_price = float(
            summaries.get(sym, {}).get("price", {}).get("last", 0)
            or pos.get("current_price", 0)
            or entry_price
        )

        # 保有経過時間・経過日数の算出
        opened_str = str(
            pos.get("opened_at") or pos.get("entry_timestamp") or pos.get("entry_time") or pos.get("timestamp") or ""
        ).strip()
        holding_hours: float | None = None
        if opened_str:
            try:
                opened_dt = datetime.fromisoformat(opened_str)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=JST)
                else:
                    opened_dt = opened_dt.astimezone(JST)
                holding_hours = max(0.0, (now_jst - opened_dt).total_seconds() / 3600.0)
            except Exception:
                holding_hours = None

        if holding_hours is None:
            if pos.get("holding_hours") is not None:
                holding_hours = max(0.0, float(pos["holding_hours"]))
            elif pos.get("holding_days") is not None:
                holding_hours = max(0.0, float(pos["holding_days"]) * 24.0)
            else:
                holding_hours = 0.0

        holding_days = round(holding_hours / 24.0, 2)
        holding_hours = round(holding_hours, 1)

        # 含み損益（円・%）および R倍数 の算出
        unrealized_pnl_jpy = (
            round((current_price - entry_price) * size, 2)
            if (entry_price > 0 and size > 0)
            else 0.0
        )
        unrealized_pnl_pct = (
            round(((current_price - entry_price) / entry_price * 100.0), 2)
            if entry_price > 0
            else 0.0
        )

        sym_rule = rules.get(sym) or DEFAULT_SYMBOL_RULES.get(sym, {})
        taker_fee = float(sym_rule.get("taker_fee", 0.0005) or 0.0005)
        if entry_price > 0 and 0 < sl_price < entry_price and size > 0:
            initial_risk_jpy = max(((entry_price - sl_price) + (sl_price * taker_fee)) * size, 1.0)
        else:
            initial_risk_jpy = float(
                pos.get("initial_risk_jpy") or pos.get("expected_loss_jpy") or 150.0
            )

        r_multiple = round(unrealized_pnl_jpy / initial_risk_jpy, 2) if initial_risk_jpy > 0 else 0.0
        if entry_price > 0:
            entry_dec = _to_decimal(entry_price)
            mult_dec = Decimal("1") + max(_to_decimal(taker_fee) * Decimal("2"), Decimal("0.002"))
            raw_be_dec = entry_dec * mult_dec
            tick_dec = _to_decimal(sym_rule.get("tick_size", "1"), default=Decimal("1"))
            if tick_dec <= 0:
                tick_dec = Decimal("1")
            be_rounded_dec = round_to_step(raw_be_dec, tick_dec, rounding=ROUND_UP)
            breakeven_sl_price: int | float = (
                int(be_rounded_dec)
                if be_rounded_dec == be_rounded_dec.to_integral_value()
                else float(be_rounded_dec)
            )
        else:
            breakeven_sl_price = 0.0

        # タイムストップ・建値保護の判定
        if holding_hours >= 120.0:
            time_stop_status = "FINAL_TIME_STOP"
            exit_rule_advice = (
                "【最終タイムストップ到達（5〜7日 / 120時間以上経過）】"
                "原則として手仕舞い（SELL）または建値以上へのタイトなSL引き上げ（MODIFY）を実行してください。"
            )
        elif holding_hours >= 72.0 and (unrealized_pnl_pct < 1.0 or r_multiple < 0.5):
            time_stop_status = "STAGE1_TIME_STOP"
            exit_rule_advice = (
                "【第1段階タイムストップ該当（3日 / 72時間以上経過かつ含み益+1.0% / +0.5R未満）】"
                "モメンタム消失とみなしてSELL（Maker指値での早期撤退）を推奨します。"
            )
        elif (r_multiple >= 1.0 or unrealized_pnl_jpy >= 150.0 or unrealized_pnl_pct >= 3.0) and (
            sl_price <= 0 or sl_price < entry_price
        ):
            time_stop_status = "BREAKEVEN_PROTECT"
            exit_rule_advice = (
                f"【建値保護該当（含み益+1.0R以上到達）】"
                f"SLを買値＋往復コスト（建値微益目安: {breakeven_sl_price}円）へ引き上げる MODIFY を優先してください。"
            )
        else:
            time_stop_status = "ACTIVE"
            exit_rule_advice = "継続保有（TP/SL待機）"

        pos["current_price"] = current_price
        pos["holding_hours"] = holding_hours
        pos["holding_days"] = holding_days
        pos["unrealized_pnl_jpy"] = unrealized_pnl_jpy
        pos["unrealized_pnl_pct"] = unrealized_pnl_pct
        pos["initial_risk_jpy"] = round(initial_risk_jpy, 2)
        pos["r_multiple"] = r_multiple
        pos["breakeven_sl_price"] = breakeven_sl_price
        pos["time_stop_status"] = time_stop_status
        pos["exit_rule_advice"] = exit_rule_advice
        if tp_price > 0 and "take_profit_price" not in pos:
            pos["take_profit_price"] = tp_price
        if sl_price > 0 and "stop_loss_price" not in pos:
            pos["stop_loss_price"] = sl_price

    return enriched


def normalize_gemini_model_and_thinking(
    raw_model: str,
    env_thinking_level: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """
    `gemini-4-Argon-High` のような表示名・サフィックス付き指定を
    API正式モデルID (`gemini-4-argon`) と ThinkingLevel (`HIGH`) に正規化する。
    """
    cleaned = raw_model.strip()
    if cleaned.lower().startswith("models/"):
        cleaned = cleaned[7:]

    thinking_level = (env_thinking_level or "").strip().upper() or None
    lower_val = cleaned.lower()

    for suffix, level in [
        ("-high", "HIGH"),
        ("-medium", "MEDIUM"),
        ("-low", "LOW"),
        ("-minimal", "MINIMAL"),
    ]:
        if lower_val.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            if not thinking_level:
                thinking_level = level
            break

    return cleaned.lower(), thinking_level


def _extract_json_dict(raw_text: str) -> Dict[str, Any]:
    """
    LLMの応答テキストから最初の有効な判定JSONオブジェクトを抽出してパースする（L-2 / Phase 20 M-2）。
    - ```json コードブロック内を優先し、なければ全文を走査
    - 各 `{` 位置から `json.JSONDecoder.raw_decode` を試行し、前置き文中の小さな辞書（`{"BTC": 13000000}` 等）を
      誤抽出しないよう、判定スキーマの必須キー（`"action"` 等）を含む辞書を最優先で返す
    """
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("AI応答が空のためJSONを抽出できません。")
    candidates: List[str] = []
    for fence in re.finditer(r"```(?:json|JSON)?\s*(.*?)```", text, flags=re.DOTALL):
        candidates.append(fence.group(1).strip())
    candidates.append(text)

    decoder = json.JSONDecoder()
    fallback_dict: Optional[Dict[str, Any]] = None
    for cand in candidates:
        pos = cand.find("{")
        while pos != -1:
            try:
                obj, _end = decoder.raw_decode(cand, pos)
                if isinstance(obj, dict):
                    if any(k in obj for k in ("action", "reasoning", "order_type")):
                        return obj
                    if fallback_dict is None:
                        fallback_dict = obj
            except json.JSONDecodeError:
                pass
            pos = cand.find("{", pos + 1)
    if fallback_dict is not None:
        return fallback_dict
    raise ValueError("AI応答から有効なJSONオブジェクトを抽出できませんでした。")


class AIJudge:
    """AI 売買判定クラス（Gemini / OpenAI / ルールベースフォールバック対応）"""

    def __init__(self, provider: str | None = None) -> None:
        self.provider = (provider or os.getenv("AI_PROVIDER", "gemini")).strip().lower()
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash").strip()
        self.gemini_thinking_level = os.getenv("GEMINI_THINKING_LEVEL", "HIGH").strip()
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

    def _has_valid_key(self, key: str) -> bool:
        return bool(key) and not key.startswith("your_")

    def judge(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        portfolio: Dict[str, Any],
        symbol_rules: Dict[str, Dict[str, Any]],
        now_dt: datetime | None = None,
    ) -> Dict[str, Any]:
        """市場データとポートフォリオを分析し、構造化JSON判定を返す"""
        now_jst = now_dt.astimezone(JST) if (now_dt and now_dt.tzinfo) else (now_dt.replace(tzinfo=JST) if now_dt else datetime.now(JST))
        now_str = now_jst.isoformat(timespec="seconds")

        enriched_portfolio = enrich_portfolio_positions(
            portfolio=portfolio,
            market_summaries=market_summaries,
            symbol_rules=symbol_rules,
            now_dt=now_jst,
        )

        user_payload = {
            "timestamp_jst": now_str,
            "portfolio": enriched_portfolio,
            "symbol_rules": symbol_rules,
            "market_summaries": market_summaries,
        }

        decision: Dict[str, Any] | None = None
        used_provider = "rule_based"

        if self.provider == "gemini" and self._has_valid_key(self.gemini_api_key):
            try:
                decision, used_model_label = self._call_gemini(user_payload)
                used_provider = f"gemini:{used_model_label}"
            except Exception as exc:
                logger.warning("Gemini API 全候補モデルの呼び出しに失敗したためルールベース判定へフォールバックします: %s", exc)

        elif self.provider == "openai" and self._has_valid_key(self.openai_api_key):
            try:
                decision = self._call_openai(user_payload)
                used_provider = f"openai:{self.openai_model}"
            except Exception as exc:
                logger.warning("OpenAI API 呼び出し失敗のためルールベース判定へフォールバックします: %s", exc)

        if decision is None:
            decision = self._rule_based_judge(market_summaries, enriched_portfolio, symbol_rules)
            used_provider = "rule_based_quant"

        act_norm = str(decision.get("action", "HOLD")).strip().upper()
        if act_norm == "MODIFY_OCO":
            act_norm = "MODIFY"
        if act_norm not in VALID_ACTIONS:
            # M-1: 未知のアクション（WAIT / CLOSE 等）は SELL 扱いにせず必ず HOLD へ正規化
            logger.warning("AIが未知のアクション '%s' を返したため HOLD へ正規化します", act_norm)
            decision["reasoning"] = f"[未知のアクション '{act_norm}' のためHOLDへ正規化] " + str(
                decision.get("reasoning", "")
            )
            act_norm = "HOLD"
        decision["action"] = act_norm
        # M-2: 確信度を [0.0, 1.0] に正規化（85 → 0.85）
        decision["confidence"] = normalize_confidence(decision.get("confidence", 0))
        if act_norm == "MODIFY":
            decision["limit_price"] = 0

        decision["timestamp"] = now_str
        decision["provider"] = used_provider
        return decision

    def _call_gemini(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Google GenAI SDK を用いて構造化 JSON 判定を取得する。
        指定モデル（例: `gemini-4-Argon-High` -> `gemini-4-argon` + `thinking_level=HIGH`）が
        Free Tier クォータ制限 (429) 等で呼び出せない場合は、利用可能な上位モデル（`gemini-3.8-flash` 等）へ自動フォールバックする。
        """
        import httpx
        from google import genai
        from google.genai import types

        ctx = ssl.create_default_context()
        try:
            import certifi

            ctx.load_verify_locations(cafile=certifi.where())
        except Exception:
            pass

        primary_model, thinking_level = normalize_gemini_model_and_thinking(
            self.gemini_model,
            self.gemini_thinking_level,
        )

        candidate_models: List[str] = [primary_model]
        for fb in DEFAULT_GEMINI_FALLBACK_MODELS:
            if fb not in candidate_models:
                candidate_models.append(fb)

        prompt = (
            f"{SYSTEM_PROMPT}\n\n"
            f"【入力データ (JSON)】\n{dumps_json(payload, indent=2)}"
        )

        last_err: Optional[Exception] = None
        with httpx.Client(verify=ctx, timeout=90.0) as http_client:
            client = genai.Client(
                api_key=self.gemini_api_key,
                http_options=types.HttpOptions(httpx_client=http_client),
            )

            for model_id in candidate_models:
                try:
                    cfg_kwargs: Dict[str, Any] = {
                        "response_mime_type": "application/json",
                        "temperature": 0.2,
                    }
                    if thinking_level:
                        cfg_kwargs["thinking_config"] = types.ThinkingConfig(
                            thinking_level=thinking_level
                        )

                    logger.info(
                        "Gemini API 呼び出し中: model=%s (thinking_level=%s)",
                        model_id,
                        thinking_level or "DEFAULT",
                    )
                    response = client.models.generate_content(
                        model=model_id,
                        contents=prompt,
                        config=types.GenerateContentConfig(**cfg_kwargs),
                    )
                    text = (response.text or "").strip()
                    parsed = _extract_json_dict(text)
                    label = f"{model_id}({thinking_level})" if thinking_level else model_id
                    return parsed, label
                except Exception as exc:
                    last_err = exc
                    logger.warning(
                        "Gemini モデル '%s' 呼び出し失敗 (次候補へフォールバック): %s",
                        model_id,
                        str(exc).splitlines()[0][:200],
                    )

        raise RuntimeError(f"すべての Gemini モデル候補で呼び出しに失敗しました: {last_err}") from last_err

    def _call_openai(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """OpenAI SDK を用いて構造化 JSON 判定を取得する"""
        from openai import OpenAI

        client = OpenAI(api_key=self.openai_api_key)
        response = client.chat.completions.create(
            model=self.openai_model,
            response_format={"type": "json_object"},
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": dumps_json(payload, indent=2),
                },
            ],
        )
        content = response.choices[0].message.content or "{}"
        return _extract_json_dict(content)

    def _rule_based_judge(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        portfolio: Dict[str, Any],
        symbol_rules: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        LLM API キー未設定時・疎通確認時に動作するクオンツ・ルールベース判定エンジン。
        - 保有建玉のタイムストップ（3日停滞撤退・5〜7日最終撤退）および建値保護（+1.0R到達時MODIFY）を最優先評価
        - 1ポジション最大50%（5,000円）・同時最大2銘柄
        - 1トレード名目許容損失 min(150円, 総資産の1.5%) 以内
        - 指値深さ：BTC/ETH/XRP は 0.50〜0.70×日足ATR、アルトは 0.65〜0.90×日足ATR
        """
        positions = portfolio.get("positions", []) or []
        max_concurrent = int(portfolio.get("max_concurrent_positions", 2))
        held_symbols = {str(p.get("symbol", "")).upper() for p in positions}

        # 1a. 全保有ポジションについて最優先でタイムストップ撤退（SELL）を走査
        for pos in positions:
            p_sym = str(pos.get("symbol", "")).upper()
            p_size = float(pos.get("size", 0) or 0)
            p_entry = float(pos.get("entry_price", 0) or 0)
            p_curr = float(pos.get("current_price", p_entry) or p_entry)
            p_days = float(pos.get("holding_days", 0.0) or 0.0)
            p_hours = float(pos.get("holding_hours", 0.0) or 0.0)
            p_pnl_pct = float(pos.get("unrealized_pnl_pct", 0.0) or 0.0)
            p_r = float(pos.get("r_multiple", 0.0) or 0.0)
            p_status = str(pos.get("time_stop_status", "ACTIVE"))

            if p_status == "STAGE1_TIME_STOP":
                return {
                    "action": "SELL",
                    "symbol": p_sym,
                    "order_type": "LIMIT_OR_MARKET",
                    "limit_price": p_curr,
                    "size": p_size,
                    "take_profit_price": 0,
                    "stop_loss_price": 0,
                    "confidence": 0.80,
                    "reasoning": (
                        f"【第1段階タイムストップ発動: {p_sym} 早期撤退 (SELL)】"
                        f"約定から {p_days:.1f}日 ({p_hours:.0f}時間) 経過しましたが、"
                        f"現在含み損益率 {p_pnl_pct:+.2f}% ({p_r:+.2f}R) と +1.0% (+0.5R) 未満で停滞しているため、"
                        "モメンタム消失とみなしてMaker指値での早期撤退（SELL）を推奨します。"
                    ),
                }

            if p_status == "FINAL_TIME_STOP":
                return {
                    "action": "SELL",
                    "symbol": p_sym,
                    "order_type": "LIMIT_OR_MARKET",
                    "limit_price": p_curr,
                    "size": p_size,
                    "take_profit_price": 0,
                    "stop_loss_price": 0,
                    "confidence": 0.85,
                    "reasoning": (
                        f"【最終タイムストップ到達: {p_sym} 手仕舞い (SELL)】"
                        f"約定から {p_days:.1f}日 ({p_hours:.0f}時間) が経過し保有期間上限（5〜7日）に達したため、"
                        f"現在の含み損益率 {p_pnl_pct:+.2f}% ({p_r:+.2f}R) で手仕舞い（SELL）を行います。"
                    ),
                }

        # 1b. タイムストップ撤退対象がない場合、全保有ポジションについて建値保護（MODIFY）を走査
        for pos in positions:
            p_sym = str(pos.get("symbol", "")).upper()
            p_size = float(pos.get("size", 0) or 0)
            p_entry = float(pos.get("entry_price", 0) or 0)
            p_rule = (symbol_rules or {}).get(p_sym) or DEFAULT_SYMBOL_RULES.get(p_sym, {})
            p_tick = _to_decimal(p_rule.get("tick_size", "1"), default=Decimal("1"))
            if p_tick <= 0:
                p_tick = Decimal("1")
            p_taker_fee = _to_decimal(p_rule.get("taker_fee", "0.0005"), default=Decimal("0.0005"))
            raw_tp_val = pos.get("take_profit_price") or pos.get("tp_price")
            if raw_tp_val:
                p_tp = float(raw_tp_val)
            else:
                tp_dec = round_to_step(_to_decimal(p_entry) * Decimal("1.06"), p_tick, rounding=ROUND_UP)
                p_tp = int(tp_dec) if tp_dec == tp_dec.to_integral_value() else float(tp_dec)
            p_pnl_pct = float(pos.get("unrealized_pnl_pct", 0.0) or 0.0)
            p_r = float(pos.get("r_multiple", 0.0) or 0.0)
            p_status = str(pos.get("time_stop_status", "ACTIVE"))
            raw_be_val = pos.get("breakeven_sl_price")
            if raw_be_val:
                be_sl = float(raw_be_val)
            else:
                be_mult = Decimal("1") + max(p_taker_fee * Decimal("2"), Decimal("0.002"))
                be_dec = round_to_step(_to_decimal(p_entry) * be_mult, p_tick, rounding=ROUND_UP)
                be_sl = int(be_dec) if be_dec == be_dec.to_integral_value() else float(be_dec)

            if p_status == "BREAKEVEN_PROTECT":
                return {
                    "action": "MODIFY",
                    "symbol": p_sym,
                    "order_type": "STOP_OR_LIMIT",
                    "limit_price": 0,
                    "size": p_size,
                    "take_profit_price": p_tp,
                    "stop_loss_price": be_sl,
                    "confidence": 0.85,
                    "reasoning": (
                        f"【建値保護発動: {p_sym} SL引き上げ (MODIFY)】"
                        f"含み益が {p_pnl_pct:+.2f}% ({p_r:+.2f}R >= +1.0R) に達したため、"
                        f"損切り逆指値（SL）を買値＋往復コストの建値微益水準（{format_price_jpy(be_sl)}円）へ引き上げて損失リスクをゼロ化します。"
                    ),
                }

        if len(positions) >= max_concurrent:
            return {
                "action": "HOLD",
                "symbol": "NONE",
                "order_type": "NONE",
                "limit_price": 0,
                "size": 0,
                "take_profit_price": 0,
                "stop_loss_price": 0,
                "confidence": 0.70,
                "reasoning": (
                    f"現在 {len(positions)} 銘柄 ({', '.join(sorted(held_symbols))}) を保有中であり、"
                    f"最大同時保有数 ({max_concurrent}銘柄) に達しているため、本日の新規エントリーは見送り (HOLD) とします。"
                ),
            }

        jpy_balance = float(portfolio.get("jpy_balance", 10000))
        max_alloc_pct = float(portfolio.get("max_allocation_per_trade_pct", 0.50))
        invested_jpy = sum(
            float(p.get("current_price") or p.get("entry_price", 0)) * float(p.get("size", 0))
            for p in positions
        )
        total_equity = jpy_balance + invested_jpy
        budget_jpy = min(jpy_balance, total_equity * max_alloc_pct)

        target_risk_cfg = float(portfolio.get("target_risk_per_trade_jpy", 150))
        aim_risk_jpy = min(target_risk_cfg, total_equity * 0.015)

        best_symbol = "NONE"
        best_score = -999.0
        best_details: Dict[str, Any] = {}

        for sym, summary in market_summaries.items():
            if sym.upper() in held_symbols:
                continue

            # クライマックス売り警告（直近4h陰線＆出来高2.0倍以上）が出ている銘柄はナイフ掴み回避のため除外
            if summary.get("climax_sell_warning") or summary.get("h4_indicators", {}).get("climax_sell_warning"):
                continue

            price_info = summary.get("price", {})
            ind = summary.get("daily_indicators", {})
            h4 = summary.get("h4_indicators", {})
            rule = symbol_rules.get(sym) or DEFAULT_SYMBOL_RULES.get(sym.upper(), {})

            last_price = float(price_info.get("last", 0))
            if last_price <= 0:
                continue

            min_size = float(rule.get("min_order_size", 0.0001))
            if min_size * last_price > budget_jpy:
                continue

            score = 0.0
            reasons = []

            trend = str(ind.get("trend", ""))
            if trend == "STRONG_UPTREND":
                score += 35.0
                reasons.append("日足がSMA25・SMA75を上回る強い上昇トレンド")
            elif trend == "UPTREND":
                score += 20.0
                reasons.append("日足がSMA25上の堅調推移")
            elif trend == "STRONG_DOWNTREND":
                score -= 25.0
                reasons.append("日足が下降トレンド")
            else:
                score -= 10.0

            rel_btc_7d = float(
                price_info.get(
                    "relative_strength_vs_btc_7d_pct",
                    ind.get("relative_strength_vs_btc_7d_pct", 0.0),
                )
                or 0.0
            )
            if rel_btc_7d > 2.0:
                score += 10.0
                reasons.append(f"対BTC7日間相対強度が良好({rel_btc_7d:+.1f}%)")
            elif rel_btc_7d < -5.0:
                score -= 10.0

            rsi = float(ind.get("rsi_14", 50.0))
            if 40.0 <= rsi <= 65.0:
                score += 20.0
                reasons.append(f"RSI({rsi:.1f})が過熱感のない順張り好機水準")
            elif rsi < 35.0 and trend in ("STRONG_UPTREND", "UPTREND"):
                score += 15.0
                reasons.append(f"上昇トレンド内の押し目水準(RSI {rsi:.1f})")
            elif rsi > 72.0:
                score -= 20.0
                reasons.append(f"RSI({rsi:.1f})が高値警戒圏")

            if ind.get("macd_hist_rising"):
                score += 15.0
                reasons.append("MACDヒストグラムが改善基調")

            if float(ind.get("macd_hist", 0.0)) > 0:
                score += 10.0

            h4_rsi = float(h4.get("rsi_14", 50.0)) if h4 else 50.0
            if 35.0 <= h4_rsi <= 62.0:
                score += 10.0

            if score > best_score:
                best_score = score
                best_symbol = sym
                best_details = {
                    "summary": summary,
                    "reasons": reasons,
                    "score": score,
                }

        if best_symbol == "NONE" or best_score < 25.0:
            return {
                "action": "HOLD",
                "symbol": best_symbol if best_symbol != "NONE" else "BTC",
                "order_type": "NONE",
                "limit_price": 0,
                "size": 0,
                "take_profit_price": 0,
                "stop_loss_price": 0,
                "confidence": 0.65,
                "reasoning": (
                    f"全スクリーニング銘柄 ({len(market_summaries)}銘柄) のテクニカルスコアを評価した結果、"
                    f"最高スコア銘柄 ({best_symbol}: {best_score:.0f}点) でもエントリー基準 (25点以上) に満たないため、"
                    "本日は新規注文を見送り (HOLD) とします。"
                ),
            }

        chosen = best_details["summary"]
        rule = symbol_rules.get(best_symbol) or DEFAULT_SYMBOL_RULES.get(best_symbol.upper(), {})
        taker_fee = max(float(rule.get("taker_fee", 0.0005)), 0.0)
        last_price = float(chosen["price"]["last"])
        best_bid = float(chosen["price"].get("best_bid", last_price))
        atr_14 = float(chosen["daily_indicators"].get("atr_14", last_price * 0.02))
        if atr_14 <= 0:
            atr_14 = last_price * 0.02

        # 押し目買い指値深さの定量スイートスポット:
        # BTC/ETH/XRP は 0.60×日足ATR (0.50〜0.70×ATR)、アルトは 0.75×日足ATR (0.65〜0.90×ATR)
        pullback_mult = 0.60 if best_symbol.upper() in MAJOR_PULLBACK_SYMBOLS else 0.75
        limit_price = min(best_bid, last_price - (pullback_mult * atr_14))

        # SLの位置: 指値から 1.2×日足ATR（かつ指値の -3.0%〜-4.5% の範囲に調整）
        raw_sl_dist = atr_14 * 1.2
        sl_dist = max(limit_price * 0.030, min(limit_price * 0.045, raw_sl_dist))
        sl_price = limit_price - sl_dist
        tp_price = limit_price + max(atr_14 * 2.0, sl_dist * 1.8)

        # 名目許容損失上限 min(150円, 総資産の1.5%) から注文数量を逆算
        unit_loss = sl_dist + (sl_price * taker_fee)
        size_by_risk = (aim_risk_jpy / unit_loss) if unit_loss > 0 else 0.0
        size_by_budget = (budget_jpy / (1.0 + taker_fee)) / limit_price if limit_price > 0 else 0.0
        proposed_size = min(size_by_risk, size_by_budget)

        confidence = min(0.90, max(0.55, round(best_score / 100.0, 2)))
        reason_str = " / ".join(best_details["reasons"])

        return {
            "action": "BUY",
            "symbol": best_symbol,
            "order_type": "LIMIT_POST_ONLY",
            "limit_price": limit_price,
            "size": proposed_size,
            "take_profit_price": tp_price,
            "stop_loss_price": sl_price,
            "confidence": confidence,
            "reasoning": (
                f"【クオンツ判定: {best_symbol} 押し目買い推奨 (スコア {best_score:.0f}点)】"
                f" 根拠: {reason_str}。現在値から {pullback_mult:.2f}×日足ATR 下方の押し目指値・"
                f"1ポジション上限 {int(budget_jpy):,}円・名目許容損失 {int(aim_risk_jpy):,}円以内（実損失上限200円以内）・"
                "RR比約1.8倍の現物Maker指値買い（Post-Only）および約定後SL/TP注文を設定します。"
            ),
        }
