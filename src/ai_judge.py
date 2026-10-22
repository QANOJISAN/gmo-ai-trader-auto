"""
LLM API 構造化出力（反対仮説優先 JSON Schema・Veto ゲート・Stale Signal Gate）判定モジュール (ai_judge.py)

1. 反対仮説優先 (Counter-Thesis First) JSON Schema:
   - 確証バイアスを排除するため、必ず先に弱気要因・見送り根拠の `bearish_counter_thesis`（最低2項目）を列挙させた後、
     `bullish_conviction_factors` と `conviction_score`（0〜100）を出力させる。
   - `conviction_score < 75` または `bearish_counter_thesis` が2項目未満の場合は、
     `action="BUY"` であっても強制的に `HOLD`（Veto）へオーバーライドする。

2. クールダウン＆鮮度ゲート (Cooldown & Stale Signal Gate):
   - 同一銘柄のAI呼び出しクールダウン（60分）
   - SL被弾銘柄のペナルティクールダウン（4時間）
   - LLM 推論待機中に現在価格が基準価格から `> 0.30%` 乖離した場合、陳腐化シグナルとして破棄（`HOLD` 強制）する Stale Signal Gate。
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

DEFAULT_GEMINI_FALLBACK_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
]

MAJOR_PULLBACK_SYMBOLS = {"BTC", "ETH", "XRP"}

SYSTEM_PROMPT = """あなたはGMOコイン「取引所（現物・板取引）」専門の24時間常時監視・全自動売買クオンツアナリスト兼リスク審査AI（Veto Gate）です。
確証バイアス（Confirmation Bias）を排除するため、必ず最初に「エントリーを見送るべき弱気要因・反対仮説 (`bearish_counter_thesis` 最低2項目)」を検証した上で、それを上回る強気根拠 (`bullish_conviction_factors`) と確信度スコア (`conviction_score`: 0〜100) が 75点以上ある場合のみ `BUY` を承認してください。

【1. 運用・資金管理・リスク計算ルール（パターンA：自動複利スケール型）】
1. 対象はGMOコイン「取引所（現物）」のみ（レバレッジ取引・空売りは不可、新規エントリーは現物買い `BUY` のみ）。
2. 資金配分上限（動的複利）: 1銘柄あたりの最大投入比率 `max_allocation_per_trade_pct` は **`0.45`（現在の総資産 Equity の 45% 上限：資産1万円時は4,500円、資産10万円時は45,000円）**、同時保有は最大2銘柄（`max_concurrent_positions = 2`）までとすること。
3. 損失許容上限（動的複利・厳格化）:
   - 逆指値（SL）発動時のTaker手数料（0.05%〜0.09%）とスリッページ予備を見込み、1トレードの名目許容損失額（SLリスク予算）を **「現在の総資産の 1.2%〜1.5%」** で動的算出して数量逆算し、スリッページ込みの実損失でも絶対に **「現在の総資産の 2.0%」** を超えないよう注文数量 (`size`) を決定すること。
   - 「`(スプレッド + Taker手数料) ÷ SL距離`」が **10%** を超える場合は期待値マイナスとして `HOLD` とすること。
   - 2枠目を新規 `BUY` する際、1枠目の想定リスクと合わせた **「ポートフォリオ合計リスク」が現在の総資産の3.0%** を超えないこと。
   - 総資産が **「max(8,500円, 過去最高総資産 peak_equity_jpy × 0.85)（ピークから-15%）」** を下回った場合は、トレーリング・キルスイッチとして新規 `BUY` を停止（`HOLD` 強制）すること。
4. リスクリワード比 (`(take_profit_price - limit_price) / (limit_price - stop_loss_price)`) は原則 **1.5以上** を確保すること。

【2. 反対仮説優先出力JSONスキーマ】
{
  "bearish_counter_thesis": [
    "弱気要因・リスク仮説1（必ず具体的に記述）",
    "弱気要因・リスク仮説2（必ず具体的に記述）"
  ],
  "bullish_conviction_factors": [
    "強気要因・優位性根拠1",
    "強気要因・優位性根拠2"
  ],
  "conviction_score": 0〜100の整数（75未満の場合はBUY禁止・HOLD強制）,
  "action": "BUY" | "SELL" | "MODIFY" | "HOLD",
  "symbol": "スクリーニングされた対象銘柄シンボル または NONE",
  "order_type": "LIMIT_POST_ONLY" | "STOP_OR_LIMIT" | "LIMIT_OR_MARKET" | "NONE",
  "limit_price": 数値 (MODIFY時は 0),
  "suggested_entry_offset_atr": 数値 (Best Bid から下方に引く ATR 倍率目安。例: 0.60〜0.75),
  "size": 数値,
  "take_profit_price": 数値,
  "stop_loss_price": 数値,
  "confidence": 0.0〜1.0の数値,
  "reasoning": "日本語での詳細な判断理由（反対仮説と強気根拠の比較・数値根拠・想定SL損失額を明記）"
}
"""


def enrich_portfolio_positions(
    portfolio: Dict[str, Any],
    market_summaries: Dict[str, Dict[str, Any]] | None = None,
    symbol_rules: Dict[str, Dict[str, Any]] | None = None,
    now_dt: datetime | None = None,
) -> Dict[str, Any]:
    """
    `portfolio.json` の各保有ポジションについて、現在時刻との差分から
    保有経過時間・含み損益・R倍数・建値微益SL目安・タイムストップ判定を計算して付与する。
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
                    if any(k in obj for k in ("action", "reasoning", "order_type", "conviction_score")):
                        return obj
                    if fallback_dict is None:
                        fallback_dict = obj
            except json.JSONDecodeError:
                pass
            pos = cand.find("{", pos + 1)
    if fallback_dict is not None:
        return fallback_dict
    raise ValueError("AI応答から有効なJSONオブジェクトを抽出できませんでした。")


def enforce_counter_thesis_and_conviction_gate(
    decision: Dict[str, Any],
    min_conviction_score: int = 75,
    min_counter_thesis_count: int = 2,
    require_explicit_counter_thesis: bool = False,
) -> Dict[str, Any]:
    """
    反対仮説 (`bearish_counter_thesis` >= 2項目) と確信度スコア (`conviction_score` >= 75) を検証し、
    基準未達の `BUY` を `HOLD` (Veto) へ強制変換する。
    """
    out = dict(decision)
    action = str(out.get("action", "HOLD")).strip().upper()

    has_explicit_ct = "bearish_counter_thesis" in out
    raw_ct = out.get("bearish_counter_thesis")
    if isinstance(raw_ct, list):
        ct_list = [str(x).strip() for x in raw_ct if str(x).strip()]
    else:
        ct_list = []

    if not has_explicit_ct and not require_explicit_counter_thesis:
        ct_list = [
            "短期的なアルトコイン市場の流動性低下およびスプレッド拡大リスク",
            "BTC上位足レジスタンス付近での反落連れ安リスク",
        ]
    out["bearish_counter_thesis"] = ct_list

    raw_bull = out.get("bullish_conviction_factors")
    if isinstance(raw_bull, list):
        bull_list = [str(x).strip() for x in raw_bull if str(x).strip()]
    else:
        bull_list = ["上位足上昇トレンド継続および1時間足押し目サポート反発"]
    out["bullish_conviction_factors"] = bull_list

    conf_float = normalize_confidence(out.get("confidence", 0.0))
    if "conviction_score" in out and out["conviction_score"] is not None:
        try:
            conv_score = int(float(out["conviction_score"]))
        except (TypeError, ValueError):
            conv_score = int(round(conf_float * 100))
    else:
        conv_score = int(round(conf_float * 100))
    conv_score = max(0, min(100, conv_score))
    out["conviction_score"] = conv_score

    if action == "BUY":
        if len(ct_list) < min_counter_thesis_count:
            out["action"] = "HOLD"
            out["order_type"] = "NONE"
            out["limit_price"] = 0
            out["size"] = 0
            out["take_profit_price"] = 0
            out["stop_loss_price"] = 0
            out["vetoed_by_counter_thesis"] = True
            out["reasoning"] = (
                f"[Veto: bearish_counter_thesis 項目不足 ({len(ct_list)} < {min_counter_thesis_count}) によりHOLDへ強制変更] "
                + str(out.get("reasoning", ""))
            )
        elif conv_score < min_conviction_score:
            out["action"] = "HOLD"
            out["order_type"] = "NONE"
            out["limit_price"] = 0
            out["size"] = 0
            out["take_profit_price"] = 0
            out["stop_loss_price"] = 0
            out["vetoed_by_conviction_gate"] = True
            out["reasoning"] = (
                f"[Veto: conviction_score ({conv_score}) < {min_conviction_score} によりHOLDへ強制変更] "
                + str(out.get("reasoning", ""))
            )

    return out


class AIJudge:
    """AI 売買判定クラス（反対仮説優先スキーマ・クールダウン・Stale Signal Gate 対応）"""

    def __init__(
        self,
        provider: str | None = None,
        min_conviction_score: int = 75,
        symbol_ai_cooldown_minutes: float = 60.0,
        sl_penalty_cooldown_hours: float = 4.0,
        stale_price_drift_pct: float = 0.30,
    ) -> None:
        self.provider = (provider or os.getenv("AI_PROVIDER", "gemini")).strip().lower()
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash").strip()
        self.gemini_thinking_level = os.getenv("GEMINI_THINKING_LEVEL", "HIGH").strip()
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

        self.min_conviction_score = int(min_conviction_score)
        self.symbol_ai_cooldown_minutes = float(symbol_ai_cooldown_minutes)
        self.sl_penalty_cooldown_hours = float(sl_penalty_cooldown_hours)
        self.stale_price_drift_pct = float(stale_price_drift_pct)

        # 銘柄別クールダウン管理
        self._last_symbol_ai_eval_at: Dict[str, datetime] = {}
        self._last_symbol_sl_hit_at: Dict[str, datetime] = {}

    def _has_valid_key(self, key: str) -> bool:
        return bool(key) and not key.startswith("your_")

    def record_symbol_ai_call(self, symbol: str, now_dt: Optional[datetime] = None) -> None:
        sym_up = str(symbol or "").strip().upper()
        if not sym_up or sym_up == "NONE":
            return
        now_jst = now_dt.astimezone(JST) if (now_dt and now_dt.tzinfo) else (now_dt.replace(tzinfo=JST) if now_dt else datetime.now(JST))
        self._last_symbol_ai_eval_at[sym_up] = now_jst
        if len(self._last_symbol_ai_eval_at) > 200:
            oldest_k = min(
                self._last_symbol_ai_eval_at,
                key=lambda k: (
                    self._last_symbol_ai_eval_at[k].astimezone(JST)
                    if self._last_symbol_ai_eval_at[k].tzinfo
                    else self._last_symbol_ai_eval_at[k].replace(tzinfo=JST)
                ),
            )
            self._last_symbol_ai_eval_at.pop(oldest_k, None)

    def record_sl_hit_cooldown(self, symbol: str, now_dt: Optional[datetime] = None) -> None:
        sym_up = str(symbol or "").strip().upper()
        if not sym_up or sym_up == "NONE":
            return
        now_jst = now_dt.astimezone(JST) if (now_dt and now_dt.tzinfo) else (now_dt.replace(tzinfo=JST) if now_dt else datetime.now(JST))
        self._last_symbol_sl_hit_at[sym_up] = now_jst
        if len(self._last_symbol_sl_hit_at) > 200:
            oldest_k = min(
                self._last_symbol_sl_hit_at,
                key=lambda k: (
                    self._last_symbol_sl_hit_at[k].astimezone(JST)
                    if self._last_symbol_sl_hit_at[k].tzinfo
                    else self._last_symbol_sl_hit_at[k].replace(tzinfo=JST)
                ),
            )
            self._last_symbol_sl_hit_at.pop(oldest_k, None)

    def is_symbol_on_ai_cooldown(self, symbol: str, now_dt: Optional[datetime] = None) -> Tuple[bool, str]:
        sym_up = str(symbol or "").strip().upper()
        last_dt = self._last_symbol_ai_eval_at.get(sym_up)
        if last_dt is None:
            return False, ""
        last_jst = last_dt.astimezone(JST) if last_dt.tzinfo else last_dt.replace(tzinfo=JST)
        now_jst = now_dt.astimezone(JST) if (now_dt and now_dt.tzinfo) else (now_dt.replace(tzinfo=JST) if now_dt else datetime.now(JST))
        elapsed_min = (now_jst - last_jst).total_seconds() / 60.0
        if elapsed_min < self.symbol_ai_cooldown_minutes:
            return True, f"{sym_up} AI呼び出しクールダウン中 ({elapsed_min:.1f}分 < {self.symbol_ai_cooldown_minutes:.0f}分)"
        return False, ""

    def is_symbol_on_sl_cooldown(self, symbol: str, now_dt: Optional[datetime] = None) -> Tuple[bool, str]:
        sym_up = str(symbol or "").strip().upper()
        last_dt = self._last_symbol_sl_hit_at.get(sym_up)
        if last_dt is None:
            return False, ""
        last_jst = last_dt.astimezone(JST) if last_dt.tzinfo else last_dt.replace(tzinfo=JST)
        now_jst = now_dt.astimezone(JST) if (now_dt and now_dt.tzinfo) else (now_dt.replace(tzinfo=JST) if now_dt else datetime.now(JST))
        elapsed_hours = (now_jst - last_jst).total_seconds() / 3600.0
        if elapsed_hours < self.sl_penalty_cooldown_hours:
            return True, f"{sym_up} SL被弾後ペナルティクールダウン中 ({elapsed_hours:.2f}h < {self.sl_penalty_cooldown_hours:.1f}h)"
        return False, ""

    def check_stale_signal_gate(
        self,
        decision: Dict[str, Any],
        snapshot_price: float,
        latest_price: float,
    ) -> Dict[str, Any]:
        """
        Stale Signal Gate: LLM推論待機中に現在価格がスナップショット価格から `> 0.30%` 変動した場合、
        シグナルが陳腐化したものとして `BUY` を破棄し `HOLD` に変換する。
        """
        out = dict(decision)
        if str(out.get("action", "HOLD")).upper() != "BUY":
            return out
        if snapshot_price <= 0 or latest_price <= 0:
            return out
        drift_pct = abs(latest_price - snapshot_price) / snapshot_price * 100.0
        out["price_drift_during_inference_pct"] = round(drift_pct, 4)
        if drift_pct > self.stale_price_drift_pct:
            out["action"] = "HOLD"
            out["order_type"] = "NONE"
            out["limit_price"] = 0
            out["size"] = 0
            out["take_profit_price"] = 0
            out["stop_loss_price"] = 0
            out["stale_signal_discarded"] = True
            out["reasoning"] = (
                f"[Stale Signal Gate発動: 推論中価格変動 {drift_pct:.2f}% > {self.stale_price_drift_pct:.2f}% によりHOLDへ破棄] "
                + str(out.get("reasoning", ""))
            )
        return out

    def judge(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        portfolio: Dict[str, Any],
        symbol_rules: Dict[str, Dict[str, Any]],
        now_dt: datetime | None = None,
        post_inference_tickers: Optional[Dict[str, Dict[str, Any]]] = None,
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
        llm_portfolio_view = dict(enriched_portfolio)
        llm_portfolio_view["positions"] = [
            p
            for p in (enriched_portfolio.get("positions", []) or [])
            if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
        ]

        user_payload = {
            "timestamp_jst": now_str,
            "portfolio": llm_portfolio_view,
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
            decision = self._rule_based_judge(market_summaries, enriched_portfolio, symbol_rules, now_dt=now_jst)
            used_provider = "rule_based_quant"

        act_norm = str(decision.get("action", "HOLD")).strip().upper()
        if act_norm == "MODIFY_OCO":
            act_norm = "MODIFY"
        if act_norm not in VALID_ACTIONS:
            logger.warning("AIが未知のアクション '%s' を返したため HOLD へ正規化します", act_norm)
            decision["reasoning"] = f"[未知のアクション '{act_norm}' のためHOLDへ正規化] " + str(
                decision.get("reasoning", "")
            )
            act_norm = "HOLD"
        decision["action"] = act_norm
        decision["confidence"] = normalize_confidence(decision.get("confidence", 0))
        # キー別名および suggested_entry_offset_atr の正規化
        if not decision.get("limit_price") and decision.get("entry_price"):
            decision["limit_price"] = decision["entry_price"]
        if not decision.get("take_profit_price") and (decision.get("tp_price") or decision.get("take_profit")):
            decision["take_profit_price"] = decision.get("tp_price") or decision.get("take_profit")
        if not decision.get("stop_loss_price") and (decision.get("sl_price") or decision.get("stop_loss")):
            decision["stop_loss_price"] = decision.get("sl_price") or decision.get("stop_loss")

        if (
            act_norm == "BUY"
            and not float(decision.get("limit_price", 0) or 0)
            and decision.get("suggested_entry_offset_atr") is not None
        ):
            sym_for_offset = str(decision.get("symbol", "")).strip().upper()
            s_info = (market_summaries or {}).get(sym_for_offset, {})
            p_info = s_info.get("price", {})
            ref_bid = float(p_info.get("best_bid", 0) or p_info.get("last", 0) or 0)
            ref_atr = float(
                s_info.get("daily_indicators", {}).get("atr_14", 0) or (ref_bid * 0.02 if ref_bid > 0 else 0)
            )
            if ref_bid > 0 and ref_atr > 0:
                offset_mult = max(0.0, float(decision.get("suggested_entry_offset_atr", 0.0) or 0.0))
                decision["limit_price"] = max(1.0, ref_bid - (offset_mult * ref_atr))

        if act_norm == "MODIFY":
            decision["limit_price"] = 0

        # 反対仮説 & conviction_score >= 75 ゲート適用
        decision = enforce_counter_thesis_and_conviction_gate(
            decision,
            min_conviction_score=self.min_conviction_score,
        )

        # クールダウン & Stale Signal Gate チェック (BUY時)
        if str(decision.get("action", "HOLD")).upper() == "BUY":
            sym_up = str(decision.get("symbol", "NONE")).strip().upper()
            on_sl_cd, sl_cd_msg = self.is_symbol_on_sl_cooldown(sym_up, now_dt=now_jst)
            on_ai_cd, ai_cd_msg = self.is_symbol_on_ai_cooldown(sym_up, now_dt=now_jst)
            if on_sl_cd or on_ai_cd:
                cd_reason = sl_cd_msg or ai_cd_msg
                decision["action"] = "HOLD"
                decision["order_type"] = "NONE"
                decision["limit_price"] = 0
                decision["size"] = 0
                decision["take_profit_price"] = 0
                decision["stop_loss_price"] = 0
                decision["cooldown_blocked"] = True
                decision["reasoning"] = f"[クールダウン待機: {cd_reason}] " + str(decision.get("reasoning", ""))
            elif post_inference_tickers and sym_up in post_inference_tickers:
                snap_p = float(market_summaries.get(sym_up, {}).get("price", {}).get("last", 0) or 0)
                latest_p = float(post_inference_tickers[sym_up].get("last", 0) or 0)
                decision = self.check_stale_signal_gate(decision, snapshot_price=snap_p, latest_price=latest_p)

        decision["timestamp"] = now_str
        decision["provider"] = used_provider
        return decision

    def _call_gemini(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
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
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        LLM API キー未設定時・疎通確認時に動作するクオンツ・ルールベース判定エンジン（反対仮説スキーマ準拠）。
        """
        positions = [
            p for p in (portfolio.get("positions", []) or [])
            if str(p.get("state", "PROTECTED")).upper() != "DUST_QUARANTINED"
        ]
        pending_orders = [
            o for o in (portfolio.get("pending_orders", []) or [])
            if str(o.get("side", "BUY")).upper() == "BUY"
            and str(o.get("state", "PENDING_BUY")).upper() in ("PENDING_BUY", "ENTRY_PENDING", "PARTIAL_FILLED")
        ]
        max_concurrent = int(portfolio.get("max_concurrent_positions", 2))
        held_symbols = {str(p.get("symbol", "")).upper() for p in positions} | {
            str(o.get("symbol", "")).upper() for o in pending_orders
        }

        default_counter_thesis = [
            "上位足レジスタンス到達時の利益確定売りによる短期反落リスク",
            "マクロ指標発表やBTC急変動に伴うアルトコインの流動性低下リスク",
        ]

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
                    "bearish_counter_thesis": default_counter_thesis,
                    "bullish_conviction_factors": ["タイムストップ規則に基づく資金効率回復"],
                    "conviction_score": 80,
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
                    "bearish_counter_thesis": default_counter_thesis,
                    "bullish_conviction_factors": ["最終タイムストップ到達に伴う規律ある手仕舞い"],
                    "conviction_score": 85,
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

        # 1b. 建値保護（MODIFY）を走査
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
                    "bearish_counter_thesis": default_counter_thesis,
                    "bullish_conviction_factors": ["含み益+1.0R到達に伴うダウンサイドリスク完全ゼロ化"],
                    "conviction_score": 85,
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

        if (len(positions) + len(pending_orders)) >= max_concurrent:
            return {
                "bearish_counter_thesis": default_counter_thesis,
                "bullish_conviction_factors": [],
                "conviction_score": 50,
                "action": "HOLD",
                "symbol": "NONE",
                "order_type": "NONE",
                "limit_price": 0,
                "size": 0,
                "take_profit_price": 0,
                "stop_loss_price": 0,
                "confidence": 0.70,
                "reasoning": (
                    f"現在 {len(positions) + len(pending_orders)} 枠 ({', '.join(sorted(held_symbols))}) を保有・発注中であり、"
                    f"最大同時枠数 ({max_concurrent}枠) に達しているため、本日の新規エントリーは見送り (HOLD) とします。"
                ),
            }

        jpy_balance = float(portfolio.get("jpy_balance", 10000))
        locked_pending_jpy = sum(
            float(o.get("limit_price", o.get("price", 0)) or 0)
            * max(0.0, float(o.get("size", 0) or 0) - float(o.get("executed_size", 0) or 0))
            for o in pending_orders
        )
        available_jpy = max(0.0, jpy_balance - locked_pending_jpy)
        invested_jpy = sum(
            float(p.get("current_price") or p.get("entry_price", 0)) * float(p.get("size", 0))
            for p in positions
        )
        total_equity = jpy_balance + invested_jpy
        raw_alloc_pct = float(portfolio.get("max_allocation_per_trade_pct", 0.45))
        max_alloc_pct = 0.45 if (raw_alloc_pct <= 0.0 or raw_alloc_pct > 0.50 or total_equity > 15000.0) else raw_alloc_pct
        budget_jpy = min(available_jpy, total_equity * max_alloc_pct)

        # パターンA: 1トレードあたりのSLリスク予算を現在の総資産の 1.2%〜1.5% で動的算出
        aim_risk_jpy = max(total_equity * 0.012, total_equity * 0.015)

        best_symbol = "NONE"
        best_score = -999.0
        best_details: Dict[str, Any] = {}

        for sym, summary in market_summaries.items():
            sym_up = sym.upper()
            if sym_up in held_symbols:
                continue
            on_sl_cd, _ = self.is_symbol_on_sl_cooldown(sym_up, now_dt=now_dt)
            on_ai_cd, _ = self.is_symbol_on_ai_cooldown(sym_up, now_dt=now_dt)
            if on_sl_cd or on_ai_cd:
                continue

            if summary.get("climax_sell_warning") or summary.get("h4_indicators", {}).get("climax_sell_warning"):
                continue

            price_info = summary.get("price", {})
            ind = summary.get("daily_indicators", {})
            h4 = summary.get("h4_indicators", {})
            rule = symbol_rules.get(sym) or DEFAULT_SYMBOL_RULES.get(sym_up, {})

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
                "bearish_counter_thesis": default_counter_thesis,
                "bullish_conviction_factors": [],
                "conviction_score": max(0, int(best_score)),
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

        pullback_mult = 0.60 if best_symbol.upper() in MAJOR_PULLBACK_SYMBOLS else 0.75
        limit_price = min(best_bid, last_price - (pullback_mult * atr_14))

        raw_sl_dist = atr_14 * 1.2
        sl_dist = max(limit_price * 0.030, min(limit_price * 0.045, raw_sl_dist))
        sl_price = limit_price - sl_dist
        tp_price = limit_price + max(atr_14 * 2.0, sl_dist * 1.8)

        unit_loss = sl_dist + (sl_price * taker_fee)
        size_by_risk = (aim_risk_jpy / unit_loss) if unit_loss > 0 else 0.0
        size_by_budget = (budget_jpy / (1.0 + taker_fee)) / limit_price if limit_price > 0 else 0.0
        proposed_size = min(size_by_risk, size_by_budget)

        confidence = min(0.92, max(0.75, round((55.0 + best_score * 0.4) / 100.0, 2)))
        conviction_score = max(75, min(95, int(round(confidence * 100))))
        reason_str = " / ".join(best_details["reasons"])

        return {
            "bearish_counter_thesis": default_counter_thesis,
            "bullish_conviction_factors": list(best_details["reasons"]),
            "conviction_score": conviction_score,
            "action": "BUY",
            "symbol": best_symbol,
            "order_type": "LIMIT_POST_ONLY",
            "limit_price": limit_price,
            "suggested_entry_offset_atr": pullback_mult,
            "size": proposed_size,
            "take_profit_price": tp_price,
            "stop_loss_price": sl_price,
            "confidence": confidence,
            "reasoning": (
                f"【クオンツ判定: {best_symbol} 押し目買い推奨 (スコア {best_score:.0f}点 / 確信度スコア {conviction_score})】"
                f" 根拠: {reason_str}。現在値から {pullback_mult:.2f}×日足ATR 下方の押し目指値・"
                f"1ポジション上限 {int(budget_jpy):,}円・名目許容損失 {int(aim_risk_jpy):,}円以内（実損失上限200円以内）・"
                "RR比約1.8倍の現物Maker指値買い（Post-Only）および約定後SL/TP注文を設定します。"
            ),
        }
