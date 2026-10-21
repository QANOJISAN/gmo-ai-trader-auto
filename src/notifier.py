"""
Discord Webhook / LINE 通知モジュール (notifier.py)

AI判定結果とGMOコインスマートフォンアプリ（トレーダーモード）「取引所（現物）」での
手動発注手順（【指値/逆指値】タブでの指値（Post-Only）＋約定後SL/TP設定）を
分かりやすい日本語メッセージに整形し、コンソール・ログ・Discord・LINEへ通知する。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

import requests

from src.gmo_client import SystemSSLAdapter
from src.validator import format_decimal_plain, format_price_jpy, normalize_confidence

logger = logging.getLogger(__name__)


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
    """GMOコインスマートフォンアプリ（トレーダーモード）での手動発注手順テキストを生成する"""
    action = str(decision.get("action", "HOLD")).upper()
    symbol = str(decision.get("symbol", "NONE"))
    limit_str = format_price_jpy(decision.get("limit_price", 0) or 0)
    size_str = format_decimal_plain(decision.get("size", 0) or 0)
    tp_str = format_price_jpy(decision.get("take_profit_price", 0) or 0)
    sl_str = format_price_jpy(decision.get("stop_loss_price", 0) or 0)
    cost = int(round(float(decision.get("expected_cost_jpy", 0) or 0)))
    loss = int(round(float(decision.get("expected_loss_jpy", 0) or 0)))
    rr = decision.get("risk_reward_ratio", 0.0)

    common_notes = (
        "※前日発注して未約定のまま24時間経過した買い指値注文がある場合は、必ず【注文一覧】から取消してください。\n"
        "※夜間の無防備な約定を避けたい場合は、就寝前に未約定指値を取り消す運用も推奨されます。"
    )

    if action == "HOLD":
        return (
            "本日は新規発注の見送り（HOLD）です。\n"
            "GMOコインアプリでの新規注文操作は不要です。\n"
            f"{common_notes}"
        )

    if action == "BUY":
        return (
            f"【GMOコインアプリ 手動発注手順：BUY（新規買い）】\n"
            f"① GMOコインアプリ下部【レート】＞上部【取引所 現物】＞銘柄「{symbol}」＞右上【注文】＞【指値/逆指値】タブを開く\n"
            f"② 売買区分【買】／注文タイプ【指値】／執行条件【Post-Only】を選択し、取引数量「{size_str} {symbol}」と新規指値レート「{limit_str} 円」（想定受渡代金: 約 {cost:,} 円）を入力して発注\n"
            f"③ 買い指値が約定したら、専用Webアプリで「買付反映」をタップし、GMOコインアプリの【指値/逆指値】タブ＞売買区分【売】＞注文タイプ【逆指値】で損切レート（SL: {sl_str} 円 / 想定最大損失: 約 {loss:,} 円）を発注して待機（利確レート TP: {tp_str} 円付近まで上昇したら注文取消して【指値】で利確TPを発注 / RR比 1 : {rr}）\n"
            f"{common_notes}"
        )

    if action in ("MODIFY", "MODIFY_OCO"):
        return (
            f"【GMOコインアプリ 手動操作手順：TP/SL変更 ({symbol})】\n"
            f"① 【保有/履歴】＞【注文一覧】から現在有効な「{symbol}」の売りの逆指値（または指値）注文を取り消す\n"
            f"② 【指値/逆指値】タブにて新しい損切レート（SL: {sl_str} 円）または利確レート（TP: {tp_str} 円）で再度【売】注文（数量: {size_str} {symbol}）を発注し、専用WebアプリでTP/SL変更を反映\n"
            f"{common_notes}"
        )

    if action == "SELL":
        return (
            f"【GMOコインアプリ 手動操作手順：SELL（決済売り・{symbol}）】\n"
            f"① 【保有/履歴】＞【注文一覧】から待機中の「{symbol}」の売り注文を取り消す\n"
            f"② 【指値/逆指値】（目安レート: {limit_str} 円）または【成行】の売買区分【売】（数量: {size_str} {symbol}）で決済発注し、専用Webアプリで決済反映\n"
            f"{common_notes}"
        )

    return (
        f"【GMOコインアプリ 手動操作手順 ({action})】\n"
        f"・対象銘柄: {symbol}\n"
        f"・数量: {size_str} {symbol} / 指値: {limit_str} 円 / 利確(TP): {tp_str} 円 / 損切(SL): {sl_str} 円\n"
        f"{common_notes}"
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
            f"■ 想定代金  : 約 {cost:,} 円 (上限5,000円枠)",
            f"■ 想定最大損失: 約 {loss:,} 円 (目標150〜200円以内)",
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
        self.session.mount("https://", SystemSSLAdapter())

    def _is_configured(self, value: str) -> bool:
        return bool(value) and not value.startswith("your_") and "your_webhook" not in value

    def notify(self, decision: Dict[str, Any], dry_run: bool = False) -> Dict[str, str]:
        """判定結果をコンソールおよびログに出力し、設定済みの外部チャネルへ送信する"""
        message = format_notification_message(decision)
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
            "EXPIRE": "⏱️",
            "MAINT": "🔧",
            "ERROR": "🚨",
        }
        icon = prefix_map.get(level.upper(), "🤖")
        formatted = f"{icon} **{title}**\n{message}".strip()
        if len(formatted) > 1900:
            formatted = formatted[:1890].rstrip() + "\n...（省略）"

        logger.info("[v2 Event %s] %s | %s", level, title, message.replace("\n", " "))
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
