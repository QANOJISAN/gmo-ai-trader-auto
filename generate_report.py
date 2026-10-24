"""
運用成績集計・レポート自動生成スクリプト (generate_report.py)

`portfolio.json` (残高・保有ポジション・決済履歴 `closed_trades`) および
`history_log.csv` (日次AI判定履歴) を読み込み、以下の項目を集計して
コンソール表示および `PERFORMANCE_REPORT.md` へ出力する。

【集計項目】
1. 現在の総資産、初期資金 (10,000円) からの通算損益額・損益率 (%)
2. 総決済回数、勝ち数、負け数、勝率 (%)、プロフィットファクター (PF = 総利益 / 総損失)、
   平均利益、平均損失、最大ドローダウン (円・%)
3. 月別 (YYYY-MM ごと) の決済回数・月間損益額・月末時点資産の推移テーブル
4. `history_log.csv` からのAI判定内訳 (稼働日数、BUY/HOLD/MODIFY/SELL回数と比率、銘柄別BUY回数)

【使い方】
  # コンソール表示 & PERFORMANCE_REPORT.md 更新
  python generate_report.py

  # 集計サマリーを Discord Webhook / LINE へ通知送信
  python generate_report.py --notify
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from src.dpapi_vault import get_secret_env, load_and_unprotect_dotenv
from src.gmo_client import SystemSSLAdapter
from src.phase1_dsr_validator import (
    calc_dsr_and_mintrl,
    compute_wfe_and_oos_pf,
    evaluate_phase1_to_phase2_promotion,
)
from src.validator import format_decimal_plain, format_price_jpy

BASE_DIR = Path(__file__).resolve().parent
PORTFOLIO_PATH = BASE_DIR / "portfolio.json"
HISTORY_CSV_PATH = BASE_DIR / "history_log.csv"
REPORT_MD_PATH = BASE_DIR / "PERFORMANCE_REPORT.md"
JST = timezone(timedelta(hours=9))

logger = logging.getLogger(__name__)


def _configure_console() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _now_jst() -> datetime:
    return datetime.now(JST)


def load_portfolio_data(path: Path | None = None) -> Dict[str, Any]:
    target_path = path if path is not None else PORTFOLIO_PATH
    if not target_path.exists():
        return {
            "initial_capital_jpy": 20000,
            "jpy_balance": 20000,
            "realized_pnl_total_jpy": 0,
            "total_realized_pnl_jpy": 0,
            "drawdown_kill_switch_jpy": 17000,
            "positions": [],
            "closed_trades": [],
        }
    with target_path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    pnl_val = int(
        data.get("realized_pnl_total_jpy", data.get("total_realized_pnl_jpy", 0)) or 0
    )
    data["realized_pnl_total_jpy"] = pnl_val
    data["total_realized_pnl_jpy"] = pnl_val
    return data


def load_history_rows(path: Path | None = None) -> List[Dict[str, str]]:
    target_path = path if path is not None else HISTORY_CSV_PATH
    if not target_path.exists() or target_path.stat().st_size == 0:
        return []
    rows: List[Dict[str, str]] = []
    with target_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row and row.get("timestamp"):
                rows.append(row)
    return rows


def compute_performance_metrics(
    portfolio: Dict[str, Any],
    history_rows: List[Dict[str, str]],
) -> Dict[str, Any]:
    """ポートフォリオとAI判定履歴から各種運用パフォーマンス指標を算出する"""
    now_str = _now_jst().isoformat(timespec="seconds")
    init_cap = int(portfolio.get("initial_capital_jpy", 20000) or 20000)
    jpy_balance = int(portfolio.get("jpy_balance", 0) or 0)
    realized_pnl_total = int(
        portfolio.get("realized_pnl_total_jpy", portfolio.get("total_realized_pnl_jpy", 0)) or 0
    )
    positions: List[Dict[str, Any]] = portfolio.get("positions", []) or []
    active_positions: List[Dict[str, Any]] = [
        p for p in positions if str(p.get("state", "PROTECTED")).strip().upper() != "DUST_QUARANTINED"
    ]
    closed_trades: List[Dict[str, Any]] = portfolio.get("closed_trades", []) or []

    invested_jpy = 0
    for pos in active_positions:
        if pos.get("cost_jpy") is not None:
            invested_jpy += int(
                Decimal(str(pos["cost_jpy"])).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
        else:
            ep_dec = Decimal(str(pos.get("entry_price", 0) or 0))
            sz_dec = Decimal(str(pos.get("size", 0) or 0))
            invested_jpy += int(
                (ep_dec * sz_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )

    total_asset_jpy = jpy_balance + invested_jpy
    total_pnl_jpy = total_asset_jpy - init_cap
    total_pnl_pct = (total_pnl_jpy / init_cap * 100.0) if init_cap > 0 else 0.0

    # 決済履歴を日時順にソート
    sorted_trades = sorted(
        closed_trades,
        key=lambda t: str(t.get("closed_at", "")),
    )

    total_trades = len(sorted_trades)
    wins = [t for t in sorted_trades if float(t.get("pnl_jpy", 0)) > 0]
    losses = [t for t in sorted_trades if float(t.get("pnl_jpy", 0)) < 0]
    evens = [t for t in sorted_trades if float(t.get("pnl_jpy", 0)) == 0]

    win_count = len(wins)
    loss_count = len(losses)
    even_count = len(evens)
    win_rate_pct = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0

    gross_profit_jpy = sum(int(round(float(t.get("pnl_jpy", 0)))) for t in wins)
    gross_loss_jpy = sum(abs(int(round(float(t.get("pnl_jpy", 0))))) for t in losses)

    if gross_loss_jpy > 0:
        pf_val = round(gross_profit_jpy / gross_loss_jpy, 2)
        pf_display = f"{pf_val:.2f}"
    elif gross_profit_jpy > 0:
        pf_val = None
        pf_display = "∞ (損失なし)"
    else:
        pf_val = 0.0
        pf_display = "0.00"

    avg_win_jpy = round(gross_profit_jpy / win_count, 1) if win_count > 0 else 0.0
    avg_loss_jpy = -round(gross_loss_jpy / loss_count, 1) if loss_count > 0 else 0.0
    max_win_jpy = max((int(round(float(t.get("pnl_jpy", 0)))) for t in wins), default=0)
    max_loss_jpy = min((int(round(float(t.get("pnl_jpy", 0)))) for t in losses), default=0)

    # 決済理由別カウント（v2拡張理由 BREAKEVEN_SL / BOT_REDUNDANT_SL / SL_PARTIAL_FAK_SWEEP / AI_SELL 等も正確に集計）
    reason_counts: Dict[str, int] = {"TP": 0, "SL": 0, "MANUAL": 0}
    for t in sorted_trades:
        r = str(t.get("reason", "MANUAL")).strip().upper()
        if r not in reason_counts:
            reason_counts[r] = 0
        reason_counts[r] += 1
        if r != "SL" and "SL" in r:
            reason_counts["SL"] += 1
        elif r not in ("TP", "SL", "MANUAL") and "SL" not in r:
            reason_counts["MANUAL"] += 1

    # 最大ドローダウン算出 (初期資金からの決済ごと資産推移ベース)
    equity = init_cap
    peak_equity = init_cap
    max_dd_jpy = 0
    max_dd_pct = 0.0

    for t in sorted_trades:
        pnl = int(round(float(t.get("pnl_jpy", 0))))
        equity += pnl
        if equity > peak_equity:
            peak_equity = equity
        dd_jpy = peak_equity - equity
        dd_pct = (dd_jpy / peak_equity * 100.0) if peak_equity > 0 else 0.0
        if dd_jpy > max_dd_jpy:
            max_dd_jpy = dd_jpy
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    # 現在資産がピークを下回っている場合も考慮
    if total_asset_jpy < peak_equity:
        curr_dd_jpy = peak_equity - total_asset_jpy
        curr_dd_pct = (curr_dd_jpy / peak_equity * 100.0) if peak_equity > 0 else 0.0
        if curr_dd_jpy > max_dd_jpy:
            max_dd_jpy = curr_dd_jpy
        if curr_dd_pct > max_dd_pct:
            max_dd_pct = curr_dd_pct

    # 月別集計 (YYYY-MM)
    months_set = set()
    trades_by_month: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for t in sorted_trades:
        c_at = str(t.get("closed_at", ""))
        ym = c_at[:7] if len(c_at) >= 7 else _now_jst().strftime("%Y-%m")
        months_set.add(ym)
        trades_by_month[ym].append(t)

    for row in history_rows:
        ts = str(row.get("timestamp", ""))
        if len(ts) >= 7:
            months_set.add(ts[:7])

    if not months_set:
        months_set.add(_now_jst().strftime("%Y-%m"))

    monthly_table: List[Dict[str, Any]] = []
    running_equity = init_cap
    for ym in sorted(months_set):
        m_trades = trades_by_month.get(ym, [])
        m_count = len(m_trades)
        m_wins = sum(1 for t in m_trades if float(t.get("pnl_jpy", 0)) > 0)
        m_losses = sum(1 for t in m_trades if float(t.get("pnl_jpy", 0)) < 0)
        m_evens = sum(1 for t in m_trades if float(t.get("pnl_jpy", 0)) == 0)
        m_win_rate = (m_wins / m_count * 100.0) if m_count > 0 else 0.0
        m_pnl = sum(int(round(float(t.get("pnl_jpy", 0)))) for t in m_trades)
        running_equity += m_pnl
        cum_pnl_pct = ((running_equity - init_cap) / init_cap * 100.0) if init_cap > 0 else 0.0
        monthly_table.append(
            {
                "month": ym,
                "trades": m_count,
                "wins": m_wins,
                "losses": m_losses,
                "evens": m_evens,
                "win_rate_pct": round(m_win_rate, 1),
                "monthly_pnl_jpy": m_pnl,
                "end_equity_jpy": running_equity,
                "cumulative_pnl_pct": round(cum_pnl_pct, 2),
            }
        )

    # history_log.csv からのAI判定内訳
    total_days = len(history_rows)
    action_counts: Dict[str, int] = {"BUY": 0, "HOLD": 0, "MODIFY": 0, "SELL": 0}
    buy_symbol_counter: Counter[str] = Counter()

    for row in history_rows:
        act = str(row.get("action", "HOLD")).strip().upper()
        sym = str(row.get("symbol", "NONE")).strip().upper()
        if act in ("MODIFY_OCO", "MODIFY"):
            action_counts["MODIFY"] += 1
        elif act in action_counts:
            action_counts[act] += 1
        else:
            action_counts[act] = action_counts.get(act, 0) + 1

        if act == "BUY" and sym and sym != "NONE":
            buy_symbol_counter[sym] += 1

    action_pcts: Dict[str, float] = {}
    for k, cnt in action_counts.items():
        action_pcts[k] = round((cnt / total_days * 100.0), 1) if total_days > 0 else 0.0

    buy_by_symbol = dict(buy_symbol_counter.most_common())

    trade_returns = [
        float(t.get("pnl_jpy", t.get("realized_pnl_jpy", 0.0)) or 0.0) / float(max(init_cap, 1))
        for t in closed_trades
    ]
    dsr_info = calc_dsr_and_mintrl(trade_returns, sr_0=0.0, confidence=0.95) if len(trade_returns) >= 2 else {
        "T": len(trade_returns),
        "dsr": 0.0,
        "min_trl": float("inf"),
        "gate1_dsr_pass": False,
        "gate2_mintrl_pass": False,
    }
    wfe_info = compute_wfe_and_oos_pf(trade_returns) if len(trade_returns) >= 8 else {
        "wfe": 0.0,
        "pf_total": pf_val,
        "pf_h1": pf_val,
        "pf_h2": pf_val,
        "gate3_wfe_pass": False,
        "gate5_oos_pf_pass": False,
    }

    return {
        "generated_at": now_str,
        "initial_capital_jpy": init_cap,
        "jpy_balance": jpy_balance,
        "invested_jpy": invested_jpy,
        "open_positions_count": len(active_positions),
        "total_asset_jpy": total_asset_jpy,
        "total_pnl_jpy": total_pnl_jpy,
        "total_pnl_pct": round(total_pnl_pct, 2),
        "realized_pnl_jpy": realized_pnl_total,
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": loss_count,
        "even_count": even_count,
        "win_rate_pct": round(win_rate_pct, 1),
        "gross_profit_jpy": gross_profit_jpy,
        "gross_loss_jpy": gross_loss_jpy,
        "profit_factor": pf_val,
        "profit_factor_display": pf_display,
        "avg_win_jpy": avg_win_jpy,
        "avg_loss_jpy": avg_loss_jpy,
        "max_win_jpy": max_win_jpy,
        "max_loss_jpy": max_loss_jpy,
        "max_drawdown_jpy": max_dd_jpy,
        "max_drawdown_pct": round(max_dd_pct, 2),
        "reason_counts": reason_counts,
        "monthly_table": monthly_table,
        "ai_total_days": total_days,
        "ai_action_counts": action_counts,
        "ai_action_pcts": action_pcts,
        "ai_buy_by_symbol": buy_by_symbol,
        "dsr_5gate_summary": {**dsr_info, **wfe_info},
    }


def build_markdown_report(metrics: Dict[str, Any], portfolio: Dict[str, Any]) -> str:
    """PERFORMANCE_REPORT.md 用のMarkdownテキストを生成する（小数価格・極小数量の正確な表示対応）"""
    m = metrics
    closed_trades: List[Dict[str, Any]] = portfolio.get("closed_trades", []) or []

    buy_sym_str = (
        " / ".join(f"{sym}: {cnt}回" for sym, cnt in m["ai_buy_by_symbol"].items())
        if m["ai_buy_by_symbol"]
        else "まだBUY判定はありません"
    )

    lines = [
        "# 📈 GMOコイン AIトレード 運用パフォーマンスレポート (`PERFORMANCE_REPORT.md`)",
        "",
        f"- **集計日時**: `{m['generated_at']}`",
        "- **運用テスト期間**: 第1回 2026年10月20日 〜 2027年7月9日 00:00 JST ／ 第2回 2027年7月10日 00:00 〜 2027年12月10日 00:00 JST",
        "- **取引対象**: GMOコイン「取引所（現物・板取引）」のみ（指値 Post-Only ＋ 約定後SL/TP・パターンA自動複利）",
        "",
        "---",
        "",
        "## 1. 資産・通算損益サマリー",
        "",
        "| 項目 | 数値 |",
        "| :--- | :--- |",
        f"| **初期運用資金** | **{m['initial_capital_jpy']:,} 円** |",
        f"| **現在の総資産（簿価）** | **{m['total_asset_jpy']:,} 円**（現金: {m['jpy_balance']:,} 円 / 保有取得額: {m['invested_jpy']:,} 円 [{m['open_positions_count']}銘柄]） |",
        f"| **通算損益額（損益率）** | **{m['total_pnl_jpy']:+,} 円（{m['total_pnl_pct']:+.2f}%）** |",
        f"| **累計確定損益額** | **{m['realized_pnl_jpy']:+,} 円** |",
        f"| **最大ドローダウン (Max DD)** | **-{m['max_drawdown_jpy']:,} 円（-{m['max_drawdown_pct']:.2f}%）** |",
        "",
        "---",
        "",
        "## 2. トレード成績統計（決済履歴ベース）",
        "",
        "| 指標 | 成績 |",
        "| :--- | :--- |",
        f"| **総決済回数** | **{m['total_trades']} 回**（勝ち: {m['win_count']} / 負け: {m['loss_count']} / 引分: {m['even_count']}） |",
        f"| **勝率 (Win Rate)** | **{m['win_rate_pct']:.1f}%** |",
        f"| **プロフィットファクター (PF)** | **{m['profit_factor_display']}**（総利益: +{m['gross_profit_jpy']:,} 円 / 総損失: -{m['gross_loss_jpy']:,} 円） |",
        f"| **平均利益 / 平均損失** | **+{m['avg_win_jpy']:,.1f} 円** / **{m['avg_loss_jpy']:,.1f} 円** |",
        f"| **最大利益 / 最大損失** | **+{m['max_win_jpy']:,} 円** / **{m['max_loss_jpy']:,} 円** |",
        f"| **決済理由内訳** | 🎯 TP利確: {m['reason_counts'].get('TP', 0)}回 / 🛡️ SL損切: {m['reason_counts'].get('SL', 0)}回 / ✋ 手動決済: {m['reason_counts'].get('MANUAL', 0)}回 |",
        "",
        "---",
        "",
        "## 3. 月別パフォーマンス推移（YYYY-MM）",
        "",
        "| 対象月 | 決済回数 | 勝敗 (勝-負) | 月間勝率 | 月間確定損益 | 月末時点資産 | 通算損益率 |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    for row in m["monthly_table"]:
        m_even_suffix = f" {row.get('evens', 0)}分" if row.get("evens", 0) > 0 else ""
        lines.append(
            f"| `{row['month']}` | {row['trades']} 回 | {row['wins']}勝 {row['losses']}敗{m_even_suffix} | "
            f"{row['win_rate_pct']:.1f}% | **{row['monthly_pnl_jpy']:+,} 円** | "
            f"**{row['end_equity_jpy']:,} 円** | {row['cumulative_pnl_pct']:+.2f}% |"
        )

    ac = m["ai_action_counts"]
    ap = m["ai_action_pcts"]

    lines.extend(
        [
            "",
            "---",
            "",
            "## 4. AI判定内訳（`history_log.csv` 集計）",
            "",
            f"- **総稼働（判定）回数**: **{m['ai_total_days']} 回**",
            f"- **BUY（新規買い指示）**: **{ac.get('BUY', 0)} 回**（{ap.get('BUY', 0.0):.1f}%）",
            f"- **HOLD（見送り・待機）**: **{ac.get('HOLD', 0)} 回**（{ap.get('HOLD', 0.0):.1f}%）",
            f"- **MODIFY（TP/SL変更）**: **{ac.get('MODIFY', 0)} 回**（{ap.get('MODIFY', 0.0):.1f}%）",
            f"- **SELL（決済売り指示）**: **{ac.get('SELL', 0)} 回**（{ap.get('SELL', 0.0):.1f}%）",
            f"- **銘柄別 BUY シグナル回数**: {buy_sym_str}",
            "",
            "---",
            "",
            "## 5. 決済履歴一覧（最新順）",
            "",
        ]
    )

    if not closed_trades:
        lines.append("_現在、確定した決済履歴（`closed_trades`）はありません。_")
    else:
        lines.extend(
            [
                "| 決済日時 | 銘柄 | 決済理由 | 数量 | 取得単価 | 決済単価 | 確定損益 (円) | 損益率 (%) |",
                "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
            ]
        )
        sorted_for_display = sorted(
            closed_trades,
            key=lambda t: str(t.get("closed_at", "")),
            reverse=True,
        )
        for t in sorted_for_display:
            c_at = str(t.get("closed_at", "-")).replace("T", " ")[:16]
            sym = t.get("symbol", "-")
            reason = t.get("reason", "MANUAL")
            size_str = format_decimal_plain(t.get("size", 0))
            entry_str = format_price_jpy(t.get("entry_price", 0))
            exit_str = format_price_jpy(t.get("exit_price", 0))
            pnl_jpy = int(round(float(t.get("pnl_jpy", 0))))
            pnl_pct = float(t.get("pnl_pct", 0.0))
            lines.append(
                f"| `{c_at}` | **{sym}** | `{reason}` | {size_str} | {entry_str} 円 | {exit_str} 円 | "
                f"**{pnl_jpy:+,} 円** | {pnl_pct:+.2f}% |"
            )

    lines.append("")
    return "\n".join(lines)


def build_discord_summary(metrics: Dict[str, Any], max_length: int = 1900) -> str:
    """Discord / LINE 通知用の月次・通算運用成績サマリーテキストを生成する（1,900文字ハードガード付き）"""
    m = metrics
    ac = m["ai_action_counts"]
    ap = m["ai_action_pcts"]

    monthly_lines = []
    for row in m["monthly_table"][-6:]:
        monthly_lines.append(
            f"  ・{row['month']}: {row['trades']}回 ({row['wins']}勝{row['losses']}敗) "
            f"| 損益 {row['monthly_pnl_jpy']:+,}円 | 資産 {row['end_equity_jpy']:,}円 ({row['cumulative_pnl_pct']:+.2f}%)"
        )
    monthly_str = "\n".join(monthly_lines) if monthly_lines else "  ・データなし"

    buy_sym_str = (
        ", ".join(f"{sym}:{cnt}回" for sym, cnt in m["ai_buy_by_symbol"].items())
        if m["ai_buy_by_symbol"]
        else "なし"
    )

    lines = [
        "==========================================",
        "📈 【GMOコイン AIトレード 運用成績レポート】",
        f"🕒 集計日時: {m['generated_at']}",
        "------------------------------------------",
        f"💰 現在の総資産: {m['total_asset_jpy']:,} 円 (初期資金 {m['initial_capital_jpy']:,} 円)",
        f"📊 通算損益    : {m['total_pnl_jpy']:+,} 円 ({m['total_pnl_pct']:+.2f}%) [確定損益: {m['realized_pnl_jpy']:+,} 円]",
        f"💵 内訳        : 現金 {m['jpy_balance']:,} 円 / 保有取得額 {m['invested_jpy']:,} 円 ({m['open_positions_count']}銘柄)",
        "------------------------------------------",
        f"🏆 総決済回数  : {m['total_trades']} 回 (勝ち {m['win_count']} / 負け {m['loss_count']} / 引分 {m['even_count']})",
        f"🎯 勝率 / PF   : 勝率 {m['win_rate_pct']:.1f}% / PF {m['profit_factor_display']}",
        f"⚖️ 平均損益    : 平均利益 +{m['avg_win_jpy']:,.1f} 円 / 平均損失 {m['avg_loss_jpy']:,.1f} 円",
        f"📉 最大DD      : -{m['max_drawdown_jpy']:,} 円 (-{m['max_drawdown_pct']:.2f}%)",
        f"🏷️ 決済理由    : TP利確 {m['reason_counts'].get('TP', 0)}回 / SL損切 {m['reason_counts'].get('SL', 0)}回 / 手動 {m['reason_counts'].get('MANUAL', 0)}回",
        "------------------------------------------",
        "📅 【月別推移 (YYYY-MM)】",
        monthly_str,
        "------------------------------------------",
        f"🤖 【AI判定内訳 (計 {m['ai_total_days']} 回)】",
        f"  ・BUY: {ac.get('BUY', 0)}回 ({ap.get('BUY', 0.0):.1f}%) / HOLD: {ac.get('HOLD', 0)}回 ({ap.get('HOLD', 0.0):.1f}%)",
        f"  ・MODIFY: {ac.get('MODIFY', 0)}回 / SELL: {ac.get('SELL', 0)}回",
        f"  ・銘柄別BUY: {buy_sym_str}",
        "==========================================",
    ]
    msg = "\n".join(lines)
    if max_length > 0 and len(msg) > max_length:
        suffix = "\n...（全文はWebアプリで確認）"
        allowed = max(0, max_length - len(suffix))
        msg = (msg[:allowed].rstrip() + suffix) if allowed > 0 else msg[:max_length]
        if len(msg) > max_length:
            msg = msg[:max_length]
    return msg


def send_report_notification(message: str, dry_run: bool = False) -> Dict[str, str]:
    """生成した成績サマリーを Discord Webhook / LINE に送信する"""
    load_and_unprotect_dotenv(BASE_DIR / ".env")
    discord_url = get_secret_env("DISCORD_WEBHOOK_URL")
    line_token = get_secret_env("LINE_CHANNEL_ACCESS_TOKEN")
    line_user_id = get_secret_env("LINE_USER_ID")

    results: Dict[str, str] = {}
    if dry_run:
        results["external"] = "skipped (dry_run)"
        return results

    session = requests.Session()
    session.headers.update({"User-Agent": "gmo-ai-trader-v2/2.0"})
    session.mount("https://", SystemSSLAdapter())

    if discord_url and not discord_url.startswith("your_") and "your_webhook" not in discord_url:
        try:
            resp = session.post(discord_url, json={"content": message}, timeout=10)
            resp.raise_for_status()
            results["discord"] = "sent"
            print("✅ Discord Webhook へ運用成績レポートを送信しました。")
        except Exception as exc:
            results["discord"] = f"error: {exc}"
            print(f"⚠️ Discord Webhook 送信エラー: {exc}")

    if line_token and line_user_id and not line_token.startswith("your_"):
        try:
            resp = session.post(
                "https://api.line.me/v2/bot/message/push",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {line_token}",
                },
                json={"to": line_user_id, "messages": [{"type": "text", "text": message}]},
                timeout=10,
            )
            resp.raise_for_status()
            results["line"] = "sent"
        except Exception as exc:
            results["line"] = f"error: {exc}"

    return results


def generate_report(
    output_path: Path | None = None,
    notify: bool = False,
    dry_run: bool = False,
    print_console: bool = True,
    portfolio_path: Path | None = None,
    history_csv_path: Path | None = None,
) -> Dict[str, Any]:
    """レポートを生成して PERFORMANCE_REPORT.md に保存し、必要に応じて通知する"""
    if print_console or notify:
        _configure_console()
    target_output = output_path if output_path is not None else REPORT_MD_PATH
    portfolio = load_portfolio_data(path=portfolio_path)
    history_rows = load_history_rows(path=history_csv_path)
    metrics = compute_performance_metrics(portfolio, history_rows)

    md_text = build_markdown_report(metrics, portfolio)
    target_output.write_text(md_text, encoding="utf-8")

    summary_text = build_discord_summary(metrics)
    if print_console:
        print(summary_text)
        print(f"\n📄 レポートファイルを出力しました: {target_output}")

    if notify:
        send_report_notification(summary_text, dry_run=dry_run)

    return metrics


def main() -> int:
    _configure_console()
    parser = argparse.ArgumentParser(description="GMOコイン AIトレード 運用成績集計・レポート生成ツール")
    parser.add_argument(
        "--notify",
        action="store_true",
        help="集計サマリーを Discord Webhook / LINE に送信する",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="--notify 指定時でも外部送信をスキップする",
    )
    args = parser.parse_args()

    generate_report(notify=args.notify, dry_run=args.dry_run, print_console=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
