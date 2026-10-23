"""
Discord Webhook / LINE 通知モジュール (notifier.py)

AI判定結果と v2 24時間常時監視・全自動売買デーモンの自動執行ステータス
（Post-Only 指値買い発注・4時間自動有効期限・約定後0.2秒ガーディアンによる自動SL/TP保護）を
分かりやすい日本語メッセージに整形し、コンソール・ログ・Discord・LINEへ通知する。
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict

import requests

from src.gmo_client import SystemSSLAdapter
from src.validator import format_decimal_plain, format_price_jpy, normalize_confidence

logger = logging.getLogger(__name__)

_SECRET_ENV_KEYS = (
    "GMO_API_KEY",
    "GMO_API_SECRET",
    "GEMINI_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "LINE_CHANNEL_ACCESS_TOKEN",
    "CRYPTOPANIC_API_KEY",
)


def mask_secrets(text: str) -> str:
    """通知メッセージやログ文字列から APIキー・シークレット・Webhookトークンを完全マスキングする"""
    if not text:
        return ""
    masked = str(text)
    for env_key in _SECRET_ENV_KEYS:
        val = os.getenv(env_key, "").strip()
        if val and len(val) >= 6 and not val.startswith("your_"):
            masked = masked.replace(val, "***REDACTED***")
    masked = re.sub(
        r"https://discord(?:app)?\.com/api/webhooks/[^/\s]+/[^\s]+",
        "https://discord.com/api/webhooks/***/***REDACTED***",
        masked,
    )
    masked = re.sub(r"AIza[0-9A-Za-z\-_]{20,}", "AIza***REDACTED***", masked)
    return masked


def format_order_type_label(action: str, order_type: str) -> str:
    """order_type コードを人間向けの日本語表記に変換する"""
    act = action.upper()
    ot = order_type.upper()
    if act == "HOLD" or ot == "NONE":
        return "なし（見送り）"
    if act == "BUY" or ot in ("LIMIT_POST_ONLY", "IFD-OCO", "LIMIT"):
        return "現物・指値（Post-Only）＋約定後SL/TP設定"
    if act in ("MODIFY", "MODIFY_OCO") or ot in ("STOP_OR_LIMIT", "OCO"):
        return "待機中売注文の取消＋新SL/TP再発注"
    if act == "SELL":
        return "現物・決済売（指値または成行）"
    return order_type


def build_manual_order_guide(decision: Dict[str, Any]) -> str:
    """v2 24時間全自動売買デーモンの自動執行ステータス案内テキストを生成する（手動操作不要）"""
    action = str(decision.get("action", "HOLD")).upper()
    symbol = str(decision.get("symbol", "NONE"))
    limit_str = format_price_jpy(decision.get("limit_price", 0) or 0)
    size_str = format_decimal_plain(decision.get("size", 0) or 0)
    tp_str = format_price_jpy(decision.get("take_profit_price", 0) or 0)
    sl_str = format_price_jpy(decision.get("stop_loss_price", 0) or 0)

    exec_res = decision.get("execution_result") if isinstance(decision.get("execution_result"), dict) else {}
    exec_status = str(exec_res.get("status", "") or "").upper()
    pending_ord = exec_res.get("pending_order") if isinstance(exec_res.get("pending_order"), dict) else {}
    raw_ord = exec_res.get("order") if isinstance(exec_res.get("order"), dict) else {}
    order_id = (
        str(pending_ord.get("order_id") or raw_ord.get("order_id") or decision.get("order_id") or "").strip()
        or "自動発番"
    )
    skip_reason = str(
        exec_res.get("cancel_reason")
        or exec_res.get("error")
        or exec_res.get("reason")
        or decision.get("skip_reason")
        or ""
    ).strip()

    header = "【🤖 v2 全自動執行ステータス（手動操作不要）】"

    if action == "HOLD":
        if str(decision.get("original_action", "")).upper() == "BUY" or skip_reason:
            reason_label = skip_reason or str(decision.get("status", "執行前検証による見送り"))
            return (
                f"{header}\n"
                f"BUY候補を検知しましたが、執行前検証・口座状態により自動発注を見送りました（理由: {reason_label}）。\n"
                "24時間常駐デーモンが引き続き15秒周期で市場と口座を自動監視します（手動操作は一切不要です）。"
            )
        return (
            f"{header}\n"
            "本日は新規発注の見送り（HOLD）です。24時間常駐デーモンが引き続き15秒周期で市場と口座を自動監視します（手動操作は一切不要です）。"
        )

    if action == "BUY":
        if exec_status in ("CANCELED", "ORDER_REJECTED", "SKIPPED", "REJECTED", "FAILED"):
            reason_label = skip_reason or f"発注ステータス={exec_status}"
            return (
                f"{header}\n"
                f"BUY候補を検知しましたが、執行前検証・口座状態により自動発注を見送りました（理由: {reason_label}）。"
            )
        return (
            f"{header}\n"
            f"GMOコイン取引所（現物）へ Post-Only 指値買い注文（注文ID: {order_id}）を自動発注しました。"
            f"未約定のまま4時間経過した場合は自動キャンセルされ、約定した場合は0.2秒ガーディアンが直ちに逆指値（SL: {sl_str} 円）を自動発注して保護します（手動操作は一切不要です）。"
        )

    if action in ("MODIFY", "MODIFY_OCO"):
        return (
            f"{header}\n"
            f"保有中ポジション（{symbol} / 数量: {size_str} {symbol}）の逆指値（SL: {sl_str} 円）・利確目標（TP: {tp_str} 円）を自動更新しました（手動操作は一切不要です）。"
        )

    if action == "SELL":
        return (
            f"{header}\n"
            f"保有中ポジション（{symbol} / 数量: {size_str} {symbol} / 目安単価: {limit_str} 円）の自動決済売り注文を執行しました（手動操作は一切不要です）。"
        )

    return (
        f"{header}\n"
        f"・対象銘柄: {symbol} / 数量: {size_str} {symbol} / 指値: {limit_str} 円 / 利確(TP): {tp_str} 円 / 損切(SL): {sl_str} 円（自動執行・手動操作不要）"
    )


def format_notification_message(decision: Dict[str, Any], max_length: int = 1900) -> str:
    """通知用の全体テキストメッセージを整形する（Discord 2,000文字上限を確実に回避するため1,900文字ハードガード付き）"""
    guide = build_manual_order_guide(decision)
    action = str(decision.get("action", "HOLD")).strip().upper()
    if action == "MODIFY_OCO":
        action = "MODIFY"
    symbol = str(decision.get("symbol", "NONE"))
    order_type_raw = str(decision.get("order_type", "NONE"))
    order_type_label = format_order_type_label(action, order_type_raw)
    confidence = normalize_confidence(decision.get("confidence", 0.0)) * 100.0
    provider = decision.get("provider", "unknown")
    limit_val = float(decision.get("limit_price", 0) or 0)
    limit_str = format_price_jpy(limit_val)
    size_str = format_decimal_plain(decision.get("size", 0) or 0)
    tp_str = format_price_jpy(decision.get("take_profit_price", 0) or 0)
    sl_str = format_price_jpy(decision.get("stop_loss_price", 0) or 0)
    cost = int(round(float(decision.get("expected_cost_jpy", 0) or 0)))
    loss = int(round(float(decision.get("expected_loss_jpy", 0) or 0)))

    if action == "MODIFY":
        limit_line = "■ 新規指値  : -- 円 (TP/SL変更のみ・新規指値不要)"
    elif action == "SELL":
        limit_line = f"■ 決済目安  : {limit_str} 円" if limit_val > 0 else "■ 決済目安  : 成行または現在値指値"
    else:
        limit_line = f"■ 新規指値  : {limit_str} 円"

    header_lines = []
    position_alerts = decision.get("position_alerts") or []
    if position_alerts:
        for alert_msg in position_alerts:
            header_lines.append(str(alert_msg))
        header_lines.append("")

    header_lines.extend(
        [
            "==============================",
            "📊 GMOコイン AIトレード判定レポート",
            f"🕒 判定時刻: {decision.get('timestamp', '')}",
            f"🤖 判定エンジン: {provider}",
            "------------------------------",
            f"■ アクション: {action} ({symbol})",
            f"■ 注文方式  : {order_type_label}",
            limit_line,
            f"■ 注文数量  : {size_str} {symbol if symbol != 'NONE' else ''}".strip(),
            f"■ 利確 (TP) : {tp_str} 円",
            f"■ 損切 (SL) : {sl_str} 円",
            f"■ 想定代金  : 約 {cost:,} 円 (総資産45%枠上限)",
            f"■ 想定最大損失: 約 {loss:,} 円 (総資産1.2〜1.5%目標 / 2.0%上限)",
            f"■ RR比 / 確信度: {decision.get('risk_reward_ratio', 0.0)} / {confidence:.0f}%",
            "------------------------------",
            "【AI判断根拠】",
        ]
    )

    reasoning_text = str(decision.get("reasoning", ""))
    footer_lines = [
        "------------------------------",
        guide,
        "==============================",
    ]

    full_message = "\n".join(header_lines + [reasoning_text] + footer_lines)
    if max_length > 0 and len(full_message) > max_length:
        suffix = "...（全文はWebアプリで確認）"
        shell_len = len("\n".join(header_lines + [""] + footer_lines))
        allowed_reasoning_len = max(0, max_length - shell_len - len(suffix))
        if allowed_reasoning_len > 0:
            trimmed_reasoning = reasoning_text[:allowed_reasoning_len].rstrip() + suffix
        else:
            trimmed_reasoning = suffix
        full_message = "\n".join(header_lines + [trimmed_reasoning] + footer_lines)
        # 最終セーフティネット: shell_len 自体が長大な場合でも 100% 確実に max_length 以下へ収める
        if len(full_message) > max_length:
            hard_suffix = "\n...（省略）"
            if max_length > len(hard_suffix):
                full_message = full_message[: max_length - len(hard_suffix)].rstrip() + hard_suffix
            else:
                full_message = full_message[:max_length]

    return full_message


class Notifier:
    """Discord Webhook および LINE Messaging API 通知クラス"""

    def __init__(self) -> None:
        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        self.line_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
        self.line_user_id = os.getenv("LINE_USER_ID", "").strip()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "gmo-ai-trader-v2/2.0"})
        self.session.mount("https://", SystemSSLAdapter())

    def _is_configured(self, value: str) -> bool:
        return bool(value) and not value.startswith("your_") and "your_webhook" not in value

    def notify(self, decision: Dict[str, Any], dry_run: bool = False) -> Dict[str, str]:
        """判定結果をコンソールおよびログに出力し、設定済みの外部チャネルへ送信する"""
        message = mask_secrets(format_notification_message(decision))
        if len(message) > 1900:
            message = message[:1890].rstrip() + "\n...（省略）"
        logger.info("AI判定レポート出力:\n%s", message)

        results: Dict[str, str] = {"console": "sent"}
        if dry_run:
            logger.info("dry-run モードのため外部通知 (Discord/LINE) はスキップしました。")
            results["external"] = "skipped (dry_run)"
            return results

        if self._is_configured(self.discord_webhook_url):
            try:
                resp = self.session.post(
                    self.discord_webhook_url,
                    json={"content": message},
                    timeout=10,
                )
                resp.raise_for_status()
                logger.info("Discord Webhook への通知送信が完了しました。")
                results["discord"] = "sent"
            except Exception as exc:
                logger.error("Discord Webhook 送信エラー: %s", exc)
                results["discord"] = f"error: {exc}"

        if self._is_configured(self.line_token) and self._is_configured(self.line_user_id):
            try:
                resp = self.session.post(
                    "https://api.line.me/v2/bot/message/push",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.line_token}",
                    },
                    json={
                        "to": self.line_user_id,
                        "messages": [{"type": "text", "text": message}],
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                logger.info("LINE への通知送信が完了しました。")
                results["line"] = "sent"
            except Exception as exc:
                logger.error("LINE 送信エラー: %s", exc)
                results["line"] = f"error: {exc}"

        return results

    def notify_event(
        self,
        title: str,
        message: str,
        level: str = "INFO",
        dry_run: bool = False,
    ) -> Dict[str, str]:
        """v2 自動売買イベント（指値発注・約定＋自動SL配置・+1.0R建値ロック・TP利確・SL損切・期限切れ取消・メンテ照合）を通知する"""
        prefix_map = {
            "INFO": "🤖",
            "BUY": "🟢",
            "FILL": "✅",
            "BREAKEVEN": "🛡️",
            "TP": "🎯",
            "SL": "⚠️",
            "WARN": "⚠️",
            "WARNING": "⚠️",
            "EXPIRE": "⏱️",
            "MAINT": "🔧",
            "ERROR": "🚨",
            "CRITICAL": "🚨",
        }
        icon = prefix_map.get(level.upper(), "🤖")
        safe_title = mask_secrets(title)
        safe_msg = mask_secrets(message)
        formatted = f"{icon} **{safe_title}**\n{safe_msg}".strip()
        if len(formatted) > 1900:
            formatted = formatted[:1890].rstrip() + "\n...（省略）"

        logger.info("[v2 Event %s] %s | %s", level, safe_title, safe_msg.replace("\n", " "))
        results: Dict[str, str] = {"console": "sent"}
        if dry_run:
            results["external"] = "skipped (dry_run)"
            return results

        if self._is_configured(self.discord_webhook_url):
            try:
                resp = self.session.post(
                    self.discord_webhook_url,
                    json={"content": formatted},
                    timeout=10,
                )
                resp.raise_for_status()
                results["discord"] = "sent"
            except Exception as exc:
                logger.error("Discord イベント通知送信エラー: %s", exc)
                results["discord"] = f"error: {exc}"

        if self._is_configured(self.line_token) and self._is_configured(self.line_user_id):
            try:
                resp = self.session.post(
                    "https://api.line.me/v2/bot/message/push",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.line_token}",
                    },
                    json={
                        "to": self.line_user_id,
                        "messages": [{"type": "text", "text": formatted}],
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                results["line"] = "sent"
            except Exception as exc:
                logger.error("LINE イベント通知送信エラー: %s", exc)
                results["line"] = f"error: {exc}"

        return results
