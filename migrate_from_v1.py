"""
v1 (e:\\gmo-ai-trader) から v2 (e:\\gmo-ai-trader-auto) への移行＆プリフライト検証スクリプト (migrate_from_v1.py)

将来、現行の v1 システム（1日1回手動発注支援）を停止し、v2（24時間全自動売買）へ切り替える際に使用する。
※ v1 フォルダ (`e:\\gmo-ai-trader`) は常に「読み取り専用」として扱い、1バイトも変更しない。

【主な機能】
1. v1 の `portfolio.json` の整合性検証（初期資金・残高・保有ポジション・決済履歴の検証）
2. `LIVE` モード移行前のプリフライトチェック:
   - `.env` の `GMO_API_KEY` / `GMO_API_SECRET` 設定有無と Private API 疎通確認
   - GMOコイン実口座の日本円余力 (`JPY available`)・現物保有数量と `portfolio.json` の照合
   - GMOコイン実口座の未約定有効注文 (`activeOrders`) の確認（v1で出した手動指値や逆指値の有無）
3. `--execute` 指定時のみ、v1 の `portfolio.json` を v2 形式（`pending_orders`, `state="SL_ACTIVE"`, `initial_stop_loss_price` 等を補完）に変換して v2 の `portfolio.json` へ引き継ぐ。

【使い方】
  # 確認のみ（ファイル書き換えなし・推奨）
  python -B migrate_from_v1.py --check

  # 実際に v1 の portfolio.json を v2 の portfolio.json へ引き継ぐ
  python -B migrate_from_v1.py --execute
"""

from __future__ import annotations

import argparse
import copy
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from src.gmo_client import GMOPublicClient
from src.gmo_private_client import GMOPrivateClient
from src.json_utils import load_json_file, write_json_file
from src.validator import DEFAULT_SYMBOL_RULES, _to_decimal, format_decimal_plain, format_price_jpy

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_V1_DIR = Path(r"e:\gmo-ai-trader")
DEFAULT_V1_PORTFOLIO = DEFAULT_V1_DIR / "portfolio.json"
DEFAULT_V2_PORTFOLIO = BASE_DIR / "portfolio.json"

JST = timezone(timedelta(hours=9))


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def validate_and_convert_v1_portfolio(
    v1_data: Dict[str, Any],
    now_dt: Optional[datetime] = None,
) -> tuple[Dict[str, Any], List[str], List[str]]:
    """
    v1 の `portfolio.json` 辞書を検証し、v2 互換のポートフォリオ辞書・警告一覧・エラー一覧を返す。
    """
    now_jst = _now_jst(now_dt)
    warnings: List[str] = []
    errors: List[str] = []

    if not isinstance(v1_data, dict):
        return {}, warnings, ["v1 portfolio.json のルートが JSON オブジェクトではありません。"]

    init_cap = int(_to_decimal(v1_data.get("initial_capital_jpy", 10000)))
    jpy_balance = int(_to_decimal(v1_data.get("jpy_balance", 0)))
    realized_pnl = int(
        _to_decimal(v1_data.get("realized_pnl_total_jpy", v1_data.get("total_realized_pnl_jpy", 0)))
    )

    if init_cap <= 0:
        errors.append(f"initial_capital_jpy が不正です: {init_cap}")
    if jpy_balance < 0:
        errors.append(f"jpy_balance が負の値です: {jpy_balance}")

    raw_positions = v1_data.get("positions", [])
    if not isinstance(raw_positions, list):
        errors.append("positions がリストではありません。")
        raw_positions = []

    converted_positions: List[Dict[str, Any]] = []
    for idx, pos in enumerate(raw_positions):
        if not isinstance(pos, dict):
            errors.append(f"positions[{idx}] が辞書ではありません。")
            continue
        sym = str(pos.get("symbol", "")).strip().upper()
        size = float(_to_decimal(pos.get("size", 0)))
        entry_price = float(_to_decimal(pos.get("entry_price", 0)))
        tp_price = float(_to_decimal(pos.get("take_profit_price") or pos.get("tp_price") or 0))
        sl_price = float(_to_decimal(pos.get("stop_loss_price") or pos.get("sl_price") or 0))

        if not sym or sym not in DEFAULT_SYMBOL_RULES:
            errors.append(f"positions[{idx}] の銘柄シンボルが不正または未対応です: '{sym}'")
            continue
        if size <= 0 or entry_price <= 0:
            errors.append(f"positions[{idx}] ({sym}) の size または entry_price が 0 以下です。")
            continue

        new_pos = copy.deepcopy(pos)
        new_pos["symbol"] = sym
        new_pos["size"] = size
        new_pos["entry_price"] = entry_price
        new_pos["take_profit_price"] = tp_price
        new_pos["stop_loss_price"] = sl_price
        new_pos.setdefault("initial_stop_loss_price", sl_price)
        new_pos.setdefault("state", "SL_ACTIVE")
        new_pos.setdefault("sl_order_id", str(pos.get("sl_order_id", "")))
        new_pos.setdefault("breakeven_locked", bool(sl_price >= entry_price > 0))
        converted_positions.append(new_pos)

        if not new_pos.get("sl_order_id"):
            warnings.append(
                f"保有建玉 {sym} ({format_decimal_plain(size)} {sym}) に sl_order_id が未設定です。"
                "LIVE モード起動時の初回 Reconciliation で自動的に逆指値(STOP)が発注または紐付けされます。"
            )

    closed_trades = copy.deepcopy(v1_data.get("closed_trades", []) if isinstance(v1_data.get("closed_trades"), list) else [])

    v2_portfolio: Dict[str, Any] = {
        "initial_capital_jpy": init_cap,
        "jpy_balance": jpy_balance,
        "realized_pnl_total_jpy": realized_pnl,
        "total_realized_pnl_jpy": realized_pnl,
        "max_allocation_per_trade_pct": float(v1_data.get("max_allocation_per_trade_pct", 0.50)),
        "max_concurrent_positions": int(v1_data.get("max_concurrent_positions", 2)),
        "target_risk_per_trade_jpy": int(v1_data.get("target_risk_per_trade_jpy", 150)),
        "max_risk_per_trade_jpy": int(v1_data.get("max_risk_per_trade_jpy", 200)),
        "drawdown_kill_switch_jpy": int(v1_data.get("drawdown_kill_switch_jpy", 8500)),
        "positions": converted_positions,
        "closed_trades": closed_trades,
        "pending_orders": [],
        "execution_mode": os.getenv("EXECUTION_MODE", "DRY_RUN").strip().upper(),
        "migrated_from_v1_at": now_jst.isoformat(timespec="seconds"),
        "last_updated": now_jst.isoformat(timespec="seconds"),
    }

    return v2_portfolio, warnings, errors


def run_preflight_check(
    v1_portfolio_path: Path = DEFAULT_V1_PORTFOLIO,
    v2_portfolio_path: Path = DEFAULT_V2_PORTFOLIO,
    private_client: Optional[GMOPrivateClient] = None,
    public_client: Optional[GMOPublicClient] = None,
) -> Dict[str, Any]:
    """
    v1 の `portfolio.json` の検証と、GMO Private API のプリフライトチェックを実行する（読み取り専用）。
    """
    report: Dict[str, Any] = {
        "v1_portfolio_exists": v1_portfolio_path.exists(),
        "v1_portfolio_path": str(v1_portfolio_path),
        "v2_portfolio_path": str(v2_portfolio_path),
        "valid_v1_portfolio": False,
        "warnings": [],
        "errors": [],
        "converted_portfolio": None,
        "exchange_status": "UNKNOWN",
        "api_credentials_configured": False,
        "live_account_check": None,
    }

    if not v1_portfolio_path.exists():
        report["errors"].append(f"v1 の portfolio.json が見つかりません: {v1_portfolio_path}")
        return report

    try:
        v1_raw = load_json_file(v1_portfolio_path)
    except Exception as exc:
        report["errors"].append(f"v1 の portfolio.json の読み込みに失敗しました: {exc}")
        return report

    converted, warnings, errors = validate_and_convert_v1_portfolio(v1_raw)
    report["warnings"].extend(warnings)
    report["errors"].extend(errors)
    report["converted_portfolio"] = converted
    report["valid_v1_portfolio"] = len(errors) == 0

    pub = public_client or GMOPublicClient()
    try:
        report["exchange_status"] = pub.get_status()
    except Exception as exc:
        report["warnings"].append(f"GMO Public API 稼働ステータス確認スキップ: {exc}")

    priv = private_client or GMOPrivateClient(public_client=pub)
    report["api_credentials_configured"] = priv.has_credentials()

    if priv.has_credentials():
        try:
            balances = priv.get_jpy_and_spot_balances()
            active_orders = priv.get_active_orders()
            jpy_diff = abs(float(balances.get("jpy_amount", 0.0)) - float(converted.get("jpy_balance", 0.0)))
            if jpy_diff > 500:
                report["warnings"].append(
                    f"実口座の日本円残高 ({int(balances.get('jpy_amount', 0)):,} 円) と "
                    f"v1 portfolio.json の残高 ({int(converted.get('jpy_balance', 0)):,} 円) に差額 ({int(jpy_diff):,} 円) があります。"
                    "未約定の指値注文で拘束中か確認してください。"
                )
            report["live_account_check"] = {
                "status": "OK",
                "jpy_amount": balances.get("jpy_amount", 0.0),
                "jpy_available": balances.get("jpy_available", 0.0),
                "spot_assets": balances.get("spot_assets", {}),
                "active_orders_count": len(active_orders),
                "active_orders": active_orders,
            }
        except Exception as exc:
            report["live_account_check"] = {
                "status": "ERROR",
                "error": str(exc),
            }
            report["warnings"].append(f"GMO Private API 実口座照合エラー: {exc}")
    else:
        report["warnings"].append(
            "現在 .env の GMO_API_KEY / GMO_API_SECRET は未設定です（DRY_RUN ペーパートレード中は未設定で正常です）。"
            "将来 LIVE モードへ切り替える前に API キーを設定して再度 `--check` を実行してください。"
        )

    return report


def execute_migration(
    v1_portfolio_path: Path = DEFAULT_V1_PORTFOLIO,
    v2_portfolio_path: Path = DEFAULT_V2_PORTFOLIO,
    private_client: Optional[GMOPrivateClient] = None,
    public_client: Optional[GMOPublicClient] = None,
) -> Dict[str, Any]:
    """
    プリフライトチェックを実行し、エラーがなければ v2 の `portfolio.json` に引き継ぎデータを書き込む。
    ※ v1 のファイルは一切変更しない。
    """
    check_res = run_preflight_check(
        v1_portfolio_path=v1_portfolio_path,
        v2_portfolio_path=v2_portfolio_path,
        private_client=private_client,
        public_client=public_client,
    )
    if not check_res["valid_v1_portfolio"] or check_res["converted_portfolio"] is None:
        check_res["migrated"] = False
        return check_res

    if v2_portfolio_path.exists():
        backup_path = v2_portfolio_path.with_suffix(".backup_before_migration.json")
        existing_v2 = load_json_file(v2_portfolio_path)
        write_json_file(backup_path, existing_v2)
        check_res["v2_backup_path"] = str(backup_path)

    write_json_file(v2_portfolio_path, check_res["converted_portfolio"])
    try:
        from src.database_wal import DatabaseWAL

        wal_path = v2_portfolio_path.parent / "v2_state_wal.db"
        wal = DatabaseWAL(wal_path, portfolio_json_path=v2_portfolio_path, auto_sync_files=True)
        try:
            conv_pf = check_res["converted_portfolio"]
            for p in conv_pf.get("positions", []) or []:
                wal.upsert_position(
                    symbol=str(p.get("symbol", "")),
                    state=str(p.get("state", "SL_ACTIVE")),
                    size=p.get("size", 0),
                    entry_price=p.get("entry_price", 0),
                    take_profit_price=p.get("take_profit_price", 0),
                    stop_loss_price=p.get("stop_loss_price", 0),
                    initial_stop_loss_price=p.get("initial_stop_loss_price", p.get("stop_loss_price", 0)),
                    sl_order_id=str(p.get("sl_order_id", "")),
                    breakeven_locked=bool(p.get("breakeven_locked", False)),
                )
            wal.sync_portfolio_to_Sqlite_and_json(conv_pf)
        finally:
            wal.close()
    except Exception:
        pass
    check_res["migrated"] = True
    return check_res


def print_migration_summary(res: Dict[str, Any], mode_label: str) -> None:
    print("==========================================")
    print(f"🔄 GMO AI Trader v1 ➔ v2 移行ツール ({mode_label})")
    print("==========================================")
    print(f"・コピー元 (v1 読み取り専用): {res.get('v1_portfolio_path')}")
    print(f"・移行先   (v2 出力先)      : {res.get('v2_portfolio_path')}")
    print(f"・取引所ステータス          : {res.get('exchange_status')}")
    print(f"・Private API キー設定済み  : {'はい' if res.get('api_credentials_configured') else 'いいえ (DRY_RUN 既定)'}")

    conv = res.get("converted_portfolio") or {}
    if conv:
        positions = conv.get("positions", []) or []
        closed = conv.get("closed_trades", []) or []
        print("------------------------------------------")
        print(f"・初期資金        : {int(conv.get('initial_capital_jpy', 0)):,} 円")
        print(f"・日本円残高      : {int(conv.get('jpy_balance', 0)):,} 円")
        print(f"・累計確定損益    : {int(conv.get('realized_pnl_total_jpy', 0)):+,} 円")
        print(f"・保有ポジション数: {len(positions)} 件")
        for p in positions:
            print(
                f"    * {p['symbol']}: 数量 {format_decimal_plain(p['size'])} / "
                f"取得単価 {format_price_jpy(p['entry_price'])} 円 / "
                f"TP {format_price_jpy(p['take_profit_price'])} 円 / SL {format_price_jpy(p['stop_loss_price'])} 円"
            )
        print(f"・過去決済履歴数  : {len(closed)} 件")

    if res.get("warnings"):
        print("------------------------------------------")
        print("【注意事項・警告】")
        for w in res["warnings"]:
            print(f"  ⚠️ {w}")

    if res.get("errors"):
        print("------------------------------------------")
        print("【エラー】")
        for e in res["errors"]:
            print(f"  ❌ {e}")

    if res.get("migrated"):
        print("------------------------------------------")
        print(f"✅ v1 から v2 への portfolio.json 引き継ぎが完了しました: {res.get('v2_portfolio_path')}")
    print("==========================================")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    load_dotenv(BASE_DIR / ".env")

    parser = argparse.ArgumentParser(description="v1 (e:\\gmo-ai-trader) から v2 への移行・プリフライト検証スクリプト")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="確認モード（ファイル書き換えなし・既定）")
    group.add_argument("--execute", action="store_true", help="実行モード（v1のportfolio.jsonをv2へ引き継ぐ）")
    parser.add_argument("--v1-portfolio", type=Path, default=DEFAULT_V1_PORTFOLIO, help="v1 portfolio.json のパス")
    parser.add_argument("--v2-portfolio", type=Path, default=DEFAULT_V2_PORTFOLIO, help="v2 portfolio.json のパス")
    args = parser.parse_args()

    if args.execute:
        res = execute_migration(v1_portfolio_path=args.v1_portfolio, v2_portfolio_path=args.v2_portfolio)
        print_migration_summary(res, mode_label="EXECUTE 移行実行モード")
        return 0 if res.get("migrated") else 1

    res = run_preflight_check(v1_portfolio_path=args.v1_portfolio, v2_portfolio_path=args.v2_portfolio)
    print_migration_summary(res, mode_label="CHECK プリフライト確認モード")
    return 0 if res.get("valid_v1_portfolio") else 1


if __name__ == "__main__":
    sys.exit(main())
