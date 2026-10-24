"""
SQLite WAL 統合台帳 ＆ 日本の暗号資産税制完全対応 Append-Only 元帳 (database_wal.py)

- `PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;` による単一 Writer 直列化エンジン
- 全8コアテーブル + 年次総平均法 VIEW (`v_tax_total_average_annual`):
  1. `intents`: 先行書き込み発注意図ログ (`PLANNED`, `SENT_OK`, `REJECTED`, `UNKNOWN`, `COMMITTED`)
  2. `orders`: 注文状態ラティス単調更新 (`WAITING:10 < ORDERED:20 < MODIFYING:25 < CANCELLING:30 < EXECUTED/CANCELED/EXPIRED:90`)
     ※終端状態からの逆行や古いイベントによる上書きを SQLite トリガーとアプリ層の双方で完全拒絶
  3. `executions`: 部分約定1件＝1行の税務正本 (`execution_id` 主キー、`INSERT OR IGNORE` べき等記録、
     `BEFORE UPDATE` / `BEFORE DELETE` を拒絶する Append-Only トリガー付き、Decimal文字列と $10^{-8}$ Satoshi 整数スケール保持、
     方式A［原則法：Maker報奨金を雑所得独立計上］・方式B［純受渡法：取得価額加減算］の双方を1円単位整数で記録)
  4. `tax_moving_avg_ledger` & `v_tax_total_average_annual`: 移動平均法のリアルタイム簿価・実現損益台帳および総平均法年次集計 VIEW
  5. `fee_adjustments`: 毎朝06:00 JST の Maker切下げ/Taker切上げ端数の合算精算 (`FEE_REFUND_ADJUSTMENT`) 記録
  6. `positions`: 8状態ステートマシン管理および `DUST_QUARANTINED` 端数在庫隔離
  7. `control_commands`: スマホ/Discord からの非同期コマンドキュー (`nonce` べき等キー付き)
  8. `llm_shadow_evaluations`: Gemini 判定の 4h / 12h / 24h 経路依存事後検証テーブル
- トランザクション完了ごとに人間・PWA 閲覧用 `portfolio.json` および `history_log.csv` をアトミック同期出力。
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.json_utils import dumps_json, load_json_file
from src.validator import _to_decimal, format_decimal_plain

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
SATOSHI_SCALE = Decimal("100000000")  # 10^8

# 注文状態ラティス定義（単調非減少のみ許可）
ORDER_STATE_RANK: Dict[str, int] = {
    "WAITING": 10,
    "ORDERED": 20,
    "MODIFYING": 25,
    "CANCELLING": 30,
    "CANCELING": 30,
    "EXECUTED": 90,
    "CANCELED": 90,
    "EXPIRED": 90,
    "REJECTED": 90,
}

# 8状態ポジション・ステートマシン定義
POSITION_STATES = frozenset(
    {
        "ENTRY_PENDING",          # 1. 新規買い指値待機中 (旧互換: PENDING_BUY)
        "PARTIAL_FILLED",         # 2. 部分約定・60秒グレースタイマー/トランシェ保護中
        "UNPROTECTED_HOLD",       # 3. 約定直後・SL発注完了前の極短時間状態 (最大3秒Dead-Man監視)
        "PROTECTED_ACTIVE",       # 4. 取引所STOP有効・保護完了状態 (旧互換: SL_ACTIVE)
        "MODIFYING_STOP_INTENT",  # 5. トレーリングSL変更中 (WAL intent 保護中)
        "EXIT_PENDING",           # 6. Smart TP 利確チェイスまたは緊急決済執行中
        "CLOSED",                 # 7. 決済完了
        "DUST_QUARANTINED",       # 8. minOrderSize未満の端数在庫隔離状態
        # 旧名称・互換エイリアス
        "PENDING_BUY",
        "SL_ACTIVE",
        "PROTECTED",
        "STOP_REARMING",
        "STOP_EXPIRED_UNFILLED",
        "TP_MAKER_TRIAL",
        "GRACEFUL_MAKER_UNWIND",
    }
)

INTENT_STATES = frozenset({"PLANNED", "SENT_OK", "REJECTED", "UNKNOWN", "COMMITTED"})


def to_satoshi_int(val: Union[str, int, float, Decimal]) -> int:
    """数量または価格を $10^8$ スケールの整数へ変換する"""
    d = _to_decimal(val)
    return int((d * SATOSHI_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def from_satoshi_int(sat: int) -> Decimal:
    """$10^8$ スケール整数を Decimal に戻す"""
    return (Decimal(int(sat)) / SATOSHI_SCALE).normalize()


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """
    一時ファイル書き込み＋os.replace によるクラッシュ耐性アトミックファイル更新。
    Windows 環境で Windows Defender やインデクサが一時的にハンドルを掴むことによる
    `PermissionError` (`[WinError 32]` / `[WinError 5]`) に対し、指数バックオフ付きリトライを行う。
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with tmp_path.open("w", encoding=encoding, newline="\n") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        last_err: Optional[OSError] = None
        for attempt in range(6):
            try:
                os.replace(tmp_path, target)
                last_err = None
                break
            except OSError as exc:
                last_err = exc
                time.sleep(0.015 * (attempt + 1))
        if last_err is not None:
            # 最終フォールバック: target へ直接書き込み試行
            with target.open("w", encoding=encoding, newline="\n") as f_direct:
                f_direct.write(content)
                f_direct.flush()
                os.fsync(f_direct.fileno())
    finally:
        if tmp_path.exists():
            for _ in range(3):
                try:
                    tmp_path.unlink()
                    break
                except OSError:
                    time.sleep(0.01)


class OrderLatticeViolationError(RuntimeError):
    """注文状態ラティスの逆行（終端状態からの巻き戻り等）が試みられた場合の例外"""


class AppendOnlyViolationError(RuntimeError):
    """Append-Only テーブル (`executions`) の UPDATE / DELETE が試みられた場合の例外"""


def classify_jp_crypto_tax_regime(
    ts_jst: Optional[Union[str, int, float, datetime]] = None,
    realized_pnl_jpy: Union[int, float, Decimal] = 0.0,
) -> Dict[str, Any]:
    """
    日本国内暗号資産税制の年度別区分（2027年以前 vs 2028年1月1日以降の申告分離課税移行）を判定する。

    - `<= 2027-12-31 23:59:59 JST` (`tax_year <= 2027`):
      * `tax_regime = "AGGREGATE_MISC"`（総合課税・雑所得）
      * `effective_tax_rate = 0.55945`（最高実効税率想定値 55.945%）
      * `loss_carryforward_years = 0`（損失繰越控除なし）
    - `>= 2028-01-01 00:00:00 JST` (`tax_year >= 2028`):
      * `tax_regime = "SEPARATE_20315"`（国内金融庁登録業者 GMOコイン `JP_FSA_REGISTERED_GMO` における特定暗号資産現物・申告分離課税）
      * `effective_tax_rate = 0.20315`（20.315%: 所得税 15% + 復興特別所得税 0.315% + 住民税 5%）
      * `loss_carryforward_years = 3`（損失繰越控除 3年間）
    """
    if ts_jst is None:
        dt_jst = datetime.now(JST)
    elif isinstance(ts_jst, datetime):
        dt_jst = ts_jst.replace(tzinfo=JST) if ts_jst.tzinfo is None else ts_jst.astimezone(JST)
    elif isinstance(ts_jst, (int, float)):
        # ミリ秒または秒のUNIXタイムスタンプ判定
        ts_val = float(ts_jst)
        if ts_val > 1e11:
            ts_val /= 1000.0
        dt_jst = datetime.fromtimestamp(ts_val, tz=JST)
    else:
        raw_str = str(ts_jst).strip()
        try:
            parsed = datetime.fromisoformat(raw_str.replace("Z", "+00:00"))
            dt_jst = parsed.replace(tzinfo=JST) if parsed.tzinfo is None else parsed.astimezone(JST)
        except Exception:
            dt_jst = datetime.now(JST)

    tax_year = int(dt_jst.year)
    if tax_year >= 2028:
        tax_regime = "SEPARATE_20315"
        effective_tax_rate = 0.20315
        loss_carryforward_years = 3
    else:
        tax_regime = "AGGREGATE_MISC"
        effective_tax_rate = 0.55945
        loss_carryforward_years = 0

    pnl_val = float(realized_pnl_jpy or 0.0)
    if pnl_val > 0.0:
        estimated_tax_jpy = round(pnl_val * effective_tax_rate, 2)
    else:
        estimated_tax_jpy = 0.0
    after_tax_net_pnl_jpy = round(pnl_val - estimated_tax_jpy, 2)
    carryforward_loss_jpy = round(abs(pnl_val), 2) if (pnl_val < 0.0 and loss_carryforward_years > 0) else 0.0

    return {
        "timestamp_jst": dt_jst.isoformat(timespec="seconds"),
        "tax_year": tax_year,
        "tax_regime": tax_regime,
        "effective_tax_rate": effective_tax_rate,
        "loss_carryforward_years": loss_carryforward_years,
        "exchange_registration": "JP_FSA_REGISTERED_GMO",
        "is_specified_crypto_spot": True,
        "realized_pnl_jpy": round(pnl_val, 2),
        "estimated_tax_jpy": estimated_tax_jpy,
        "after_tax_net_pnl_jpy": after_tax_net_pnl_jpy,
        "carryforward_loss_jpy": carryforward_loss_jpy,
        "post_2028_tax_regime": "SEPARATE_20315",
        "post_2028_separate_tax_rate": 0.20315,
    }


def connect_forensic_readonly(db_path: Union[str, Path]) -> sqlite3.Connection:
    """
    外部の税務・監査アプリ（`Forensic Ledger` 等）から `v2_state_wal.db` を
    ロック競合ゼロ・書き込み完全禁止で読み取るための Read-Only WAL 共有バス接続ヘルパー。
    - `file:.../v2_state_wal.db?mode=ro` (`uri=True`)
    - `PRAGMA query_only=ON;`
    - `PRAGMA busy_timeout=5000;`
    - `PRAGMA temp_store=MEMORY;`
    - `PRAGMA mmap_size=268435456;`
    """
    posix_path = Path(db_path).resolve().as_posix()
    uri = f"file:{posix_path}?mode=ro"
    conn = sqlite3.connect(
        uri,
        uri=True,
        timeout=5.0,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA mmap_size=268435456;")
    return conn


def _apply_high_availability_wal_pragmas(conn: sqlite3.Connection) -> None:
    """
    高可用性 SQLite WAL PRAGMA 6項目＋外部キー制約を接続へ標準適用する:
    1. `PRAGMA journal_mode=WAL;`
    2. `PRAGMA synchronous=NORMAL;`
    3. `PRAGMA busy_timeout=5000;`
    4. `PRAGMA temp_store=MEMORY;`
    5. `PRAGMA mmap_size=268435456;`
    6. `PRAGMA wal_autocheckpoint=1000;`
    """
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA mmap_size=268435456;")
    conn.execute("PRAGMA wal_autocheckpoint=1000;")
    conn.execute("PRAGMA foreign_keys=ON;")


class _WalSessionLock:
    """
    `with self._lock:` の最外層ブロック開始時に SQLite 接続を開き、
    終了時に接続をクローズするリエントラントセッションロック（Windows TempDir ロック競合を完全防止）。
    """

    def __init__(self, owner: "DatabaseWAL") -> None:
        self._owner = owner
        self._rlock = threading.RLock()
        self._depth = 0

    def __enter__(self) -> "_WalSessionLock":
        self._rlock.acquire()
        self._depth += 1
        if self._owner._conn is None:
            conn = sqlite3.connect(
                str(self._owner.db_path),
                timeout=10.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            _apply_high_availability_wal_pragmas(conn)
            self._owner._conn = conn
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._depth -= 1
        if self._depth <= 0:
            self._depth = 0
            if self._owner._conn is not None:
                try:
                    self._owner._conn.close()
                except Exception:
                    pass
                self._owner._conn = None
        self._rlock.release()


class DatabaseWAL:
    """SQLite WAL 単一 Writer 統合台帳エンジン"""

    def __init__(
        self,
        db_path: Union[str, Path] = "v2_ledger.db",
        portfolio_json_path: Optional[Union[str, Path]] = None,
        history_csv_path: Optional[Union[str, Path]] = None,
        auto_sync_files: bool = True,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.portfolio_json_path = (
            Path(portfolio_json_path)
            if portfolio_json_path is not None
            else self.db_path.parent / "portfolio.json"
        )
        self.history_csv_path = (
            Path(history_csv_path)
            if history_csv_path is not None
            else self.db_path.parent / "history_log.csv"
        )
        self.auto_sync_files = auto_sync_files
        self._conn: Optional[sqlite3.Connection] = None
        self._write_count: int = 0
        self._lock = _WalSessionLock(self)
        self._initialize_pragmas_and_schema()

    def _initialize_pragmas_and_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            _apply_high_availability_wal_pragmas(self._conn)

            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS account_meta (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_ts_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS intents (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('PLANNED','SENT_OK','REJECTED','UNKNOWN','COMMITTED')),
                    order_id TEXT NOT NULL DEFAULT '',
                    created_ts_ms INTEGER NOT NULL,
                    updated_ts_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    execution_type TEXT NOT NULL,
                    time_in_force TEXT NOT NULL DEFAULT 'SOK',
                    price_str TEXT NOT NULL DEFAULT '0',
                    size_str TEXT NOT NULL DEFAULT '0',
                    executed_size_str TEXT NOT NULL DEFAULT '0',
                    remaining_size_str TEXT NOT NULL DEFAULT '0',
                    state TEXT NOT NULL,
                    state_rank INTEGER NOT NULL,
                    intent_id TEXT NOT NULL DEFAULT '',
                    updated_ts_ms INTEGER NOT NULL,
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TRIGGER IF NOT EXISTS trg_orders_monotonic_lattice
                BEFORE UPDATE ON orders
                BEGIN
                    SELECT CASE
                        WHEN OLD.state_rank >= 90 AND NEW.state != OLD.state THEN
                            RAISE(ABORT, 'Order lattice violation: cannot transition out of terminal state')
                        WHEN NEW.state_rank < OLD.state_rank THEN
                            RAISE(ABORT, 'Order lattice violation: state_rank regression forbidden')
                    END;
                END;

                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
                    execution_type TEXT NOT NULL,
                    size_str TEXT NOT NULL,
                    size_satoshi INTEGER NOT NULL,
                    price_str TEXT NOT NULL,
                    price_satoshi INTEGER NOT NULL,
                    gross_amount_jpy INTEGER NOT NULL,
                    taker_fee_jpy INTEGER NOT NULL,
                    maker_rebate_jpy INTEGER NOT NULL,
                    net_cashflow_jpy INTEGER NOT NULL,
                    method_a_cost_basis_delta_jpy INTEGER NOT NULL,
                    method_a_rebate_misc_income_jpy INTEGER NOT NULL,
                    method_a_realized_pnl_jpy INTEGER NOT NULL,
                    method_b_cost_basis_delta_jpy INTEGER NOT NULL,
                    method_b_realized_pnl_jpy INTEGER NOT NULL,
                    executed_ts_ms INTEGER NOT NULL,
                    tax_year INTEGER NOT NULL,
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TRIGGER IF NOT EXISTS trg_executions_append_only_no_update
                BEFORE UPDATE ON executions
                BEGIN
                    SELECT RAISE(ABORT, 'Append-only ledger violation: UPDATE on executions is forbidden');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_executions_append_only_no_delete
                BEFORE DELETE ON executions
                BEGIN
                    SELECT RAISE(ABORT, 'Append-only ledger violation: DELETE on executions is forbidden');
                END;

                CREATE TABLE IF NOT EXISTS tax_moving_avg_ledger (
                    symbol TEXT PRIMARY KEY,
                    total_size_str TEXT NOT NULL DEFAULT '0',
                    total_size_satoshi INTEGER NOT NULL DEFAULT 0,
                    book_value_a_jpy INTEGER NOT NULL DEFAULT 0,
                    avg_cost_a_jpy REAL NOT NULL DEFAULT 0.0,
                    realized_pnl_a_jpy INTEGER NOT NULL DEFAULT 0,
                    misc_rebate_income_a_jpy INTEGER NOT NULL DEFAULT 0,
                    book_value_b_jpy INTEGER NOT NULL DEFAULT 0,
                    avg_cost_b_jpy REAL NOT NULL DEFAULT 0.0,
                    realized_pnl_b_jpy INTEGER NOT NULL DEFAULT 0,
                    updated_ts_ms INTEGER NOT NULL DEFAULT 0
                );

                CREATE VIEW IF NOT EXISTS v_tax_total_average_annual AS
                SELECT
                    tax_year,
                    symbol,
                    SUM(CASE WHEN side = 'BUY' THEN size_satoshi ELSE 0 END) AS total_buy_size_satoshi,
                    SUM(CASE WHEN side = 'BUY' THEN (gross_amount_jpy + taker_fee_jpy) ELSE 0 END) AS total_buy_cost_method_a_jpy,
                    SUM(CASE WHEN side = 'BUY' THEN (gross_amount_jpy + taker_fee_jpy - maker_rebate_jpy) ELSE 0 END) AS total_buy_cost_method_b_jpy,
                    SUM(CASE WHEN side = 'SELL' THEN size_satoshi ELSE 0 END) AS total_sell_size_satoshi,
                    SUM(CASE WHEN side = 'SELL' THEN (gross_amount_jpy - taker_fee_jpy) ELSE 0 END) AS total_sell_proceeds_method_a_jpy,
                    SUM(CASE WHEN side = 'SELL' THEN (gross_amount_jpy - taker_fee_jpy + maker_rebate_jpy) ELSE 0 END) AS total_sell_proceeds_method_b_jpy,
                    SUM(maker_rebate_jpy) AS total_maker_rebate_jpy,
                    SUM(taker_fee_jpy) AS total_taker_fee_jpy,
                    COUNT(*) AS execution_count
                FROM executions
                GROUP BY tax_year, symbol;

                CREATE TABLE IF NOT EXISTS tax_ledger_events (
                    event_id TEXT PRIMARY KEY,
                    timestamp_jst TEXT NOT NULL,
                    tax_year INTEGER NOT NULL,
                    tax_regime TEXT NOT NULL CHECK(tax_regime IN ('AGGREGATE_MISC', 'SEPARATE_20315')),
                    effective_tax_rate REAL NOT NULL,
                    loss_carryforward_years INTEGER NOT NULL DEFAULT 0,
                    exchange_registration TEXT NOT NULL DEFAULT 'JP_FSA_REGISTERED_GMO',
                    symbol TEXT NOT NULL DEFAULT 'JPY',
                    event_type TEXT NOT NULL,
                    side TEXT NOT NULL DEFAULT '',
                    size REAL NOT NULL DEFAULT 0.0,
                    price_jpy REAL NOT NULL DEFAULT 0.0,
                    gross_proceeds_jpy REAL NOT NULL DEFAULT 0.0,
                    cost_basis_jpy REAL NOT NULL DEFAULT 0.0,
                    fee_or_rebate_jpy REAL NOT NULL DEFAULT 0.0,
                    realized_pnl_jpy REAL NOT NULL DEFAULT 0.0,
                    estimated_tax_jpy REAL NOT NULL DEFAULT 0.0,
                    after_tax_net_pnl_jpy REAL NOT NULL DEFAULT 0.0,
                    source_order_id TEXT NOT NULL DEFAULT '',
                    created_ts_ms INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tax_ledger_events_year_regime
                ON tax_ledger_events (tax_year, tax_regime, symbol);

                CREATE VIEW IF NOT EXISTS v_forensic_tax_ledger AS
                SELECT
                    event_id,
                    timestamp_jst,
                    tax_year,
                    tax_regime,
                    effective_tax_rate,
                    loss_carryforward_years,
                    exchange_registration,
                    symbol,
                    event_type,
                    side,
                    size,
                    price_jpy,
                    gross_proceeds_jpy,
                    cost_basis_jpy,
                    fee_or_rebate_jpy,
                    realized_pnl_jpy,
                    estimated_tax_jpy,
                    after_tax_net_pnl_jpy,
                    source_order_id,
                    created_ts_ms
                FROM tax_ledger_events
                ORDER BY created_ts_ms ASC, event_id ASC;

                CREATE TABLE IF NOT EXISTS fee_adjustments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_date TEXT UNIQUE NOT NULL,
                    delta_jpy INTEGER NOT NULL,
                    jpy_balance_before INTEGER NOT NULL,
                    jpy_balance_after INTEGER NOT NULL,
                    reason TEXT NOT NULL DEFAULT 'FEE_REFUND_ADJUSTMENT',
                    created_ts_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    state TEXT NOT NULL,
                    size_str TEXT NOT NULL,
                    size_satoshi INTEGER NOT NULL,
                    entry_price_str TEXT NOT NULL,
                    take_profit_price_str TEXT NOT NULL DEFAULT '0',
                    stop_loss_price_str TEXT NOT NULL DEFAULT '0',
                    initial_stop_loss_price_str TEXT NOT NULL DEFAULT '0',
                    initial_risk_jpy REAL NOT NULL DEFAULT 150.0,
                    buy_order_id TEXT NOT NULL DEFAULT '',
                    sl_order_id TEXT NOT NULL DEFAULT '',
                    breakeven_locked INTEGER NOT NULL DEFAULT 0,
                    trailing_stage INTEGER NOT NULL DEFAULT 0,
                    opened_at TEXT NOT NULL,
                    unprotected_since_ts_ms INTEGER NOT NULL DEFAULT 0,
                    updated_ts_ms INTEGER NOT NULL,
                    extra_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS control_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command TEXT NOT NULL,
                    nonce TEXT UNIQUE NOT NULL,
                    requested_by TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('PENDING','AWAITING_CONFIRM','EXECUTED','EXPIRED','REJECTED')),
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_ts_ms INTEGER NOT NULL,
                    executed_ts_ms INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS llm_shadow_evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    eval_id TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    conviction_score INTEGER NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_loss_price REAL NOT NULL,
                    take_profit_price REAL NOT NULL,
                    bearish_counter_thesis TEXT NOT NULL DEFAULT '',
                    bullish_conviction_factors TEXT NOT NULL DEFAULT '',
                    mfe_4h_pct REAL,
                    mae_4h_pct REAL,
                    pnl_4h_pct REAL,
                    mfe_12h_pct REAL,
                    mae_12h_pct REAL,
                    pnl_12h_pct REAL,
                    mfe_24h_pct REAL,
                    mae_24h_pct REAL,
                    pnl_24h_pct REAL,
                    stop_hit_first INTEGER NOT NULL DEFAULT 0,
                    outcome_class TEXT NOT NULL DEFAULT 'PENDING',
                    created_ts_ms INTEGER NOT NULL,
                    evaluated_ts_ms INTEGER NOT NULL DEFAULT 0
                );
                """
            )

    def get_pragmas(self) -> Dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            jm = cur.execute("PRAGMA journal_mode;").fetchone()[0]
            sync = int(cur.execute("PRAGMA synchronous;").fetchone()[0])
            bt = int(cur.execute("PRAGMA busy_timeout;").fetchone()[0])
            ts = int(cur.execute("PRAGMA temp_store;").fetchone()[0])
            mmap_row = cur.execute("PRAGMA mmap_size;").fetchone()
            mmap_val = int(mmap_row[0]) if mmap_row else 0
            autockp = int(cur.execute("PRAGMA wal_autocheckpoint;").fetchone()[0])
            sync_names = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}
            ts_names = {0: "DEFAULT", 1: "FILE", 2: "MEMORY"}
            return {
                "journal_mode": str(jm).upper(),
                "synchronous": sync,
                "synchronous_name": sync_names.get(sync, str(sync)),
                "busy_timeout": bt,
                "temp_store": ts,
                "temp_store_name": ts_names.get(ts, str(ts)),
                "mmap_size": mmap_val,
                "wal_autocheckpoint": autockp,
            }

    def connect_forensic_readonly(self) -> sqlite3.Connection:
        """
        `Forensic Ledger` 等の外部税務・監査システムが `v2_state_wal.db` を
        ロック競合ゼロ・書き込み完全禁止 (`mode=ro`, `PRAGMA query_only=ON;`) で読み取るための接続を返す。
        """
        return connect_forensic_readonly(self.db_path)

    def checkpoint_wal(self, mode: str = "PASSIVE") -> Dict[str, Any]:
        """SQLite WAL チェックポイントを実行し、WAL ファイル (`.db-wal`) の肥大化を防止する"""
        valid_modes = {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}
        mode_up = str(mode or "PASSIVE").strip().upper()
        if mode_up not in valid_modes:
            mode_up = "PASSIVE"
        with self._lock:
            row = self._conn.execute(f"PRAGMA wal_checkpoint({mode_up});").fetchone()
            if row is not None and len(row) >= 3:
                return {
                    "mode": mode_up,
                    "busy": int(row[0] or 0),
                    "log_frames": int(row[1] or 0),
                    "checkpointed_frames": int(row[2] or 0),
                }
            return {"mode": mode_up, "busy": 0, "log_frames": 0, "checkpointed_frames": 0}

    def checkpoint_truncate(self) -> Dict[str, Any]:
        """`PRAGMA wal_checkpoint(TRUNCATE);` を実行し、WALログを本体DBへ完全統合して `.db-wal` を0バイトへ切り詰める"""
        return self.checkpoint_wal("TRUNCATE")

    def verify_startup_integrity(self, full_check: bool = False) -> Dict[str, Any]:
        """
        起動時の WAL リカバリ競合 (`SQLITE_BUSY_RECOVERY: 261`) を防止しつつ、
        `PRAGMA quick_check;`（または `PRAGMA integrity_check;`）で DB の整合性を検証する。
        """
        pragma_sql = "PRAGMA integrity_check;" if full_check else "PRAGMA quick_check;"
        last_err: Optional[Exception] = None
        for attempt in range(5):
            try:
                with self._lock:
                    row = self._conn.execute(pragma_sql).fetchone()
                    status_str = str(row[0] if row else "unknown").strip()
                    is_ok = status_str.lower() == "ok"
                    return {
                        "ok": is_ok,
                        "status": status_str,
                        "db_path": str(self.db_path),
                        "attempts": attempt + 1,
                    }
            except sqlite3.OperationalError as exc:
                last_err = exc
                time.sleep(0.1 * (attempt + 1))
        return {
            "ok": False,
            "status": f"ERROR: {last_err}",
            "db_path": str(self.db_path),
            "attempts": 5,
        }

    def create_online_backup(
        self,
        dest_path: Union[str, Path],
        pages: int = 0,
        sleep_sec: float = 0.05,
    ) -> Dict[str, Any]:
        """
        `sqlite3.Connection.backup()`（SQLite Online Backup API）を用いて、
        WAL書き込みを止めずに一貫性のある単一ファイルのオンラインバックアップを作成し、
        バックアップ先の `PRAGMA integrity_check;` を検証する。
        """
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_dest = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.bak.tmp")
        try:
            with self._lock:
                dst_conn = sqlite3.connect(str(tmp_dest), timeout=10.0)
                try:
                    self._conn.backup(dst_conn, pages=int(pages), sleep=float(sleep_sec))
                    chk_row = dst_conn.execute("PRAGMA integrity_check;").fetchone()
                    integrity_str = str(chk_row[0] if chk_row else "unknown").strip()
                finally:
                    dst_conn.close()

            if integrity_str.lower() != "ok":
                raise RuntimeError(f"バックアップ先の integrity_check が失敗しました: {integrity_str}")

            os.replace(tmp_dest, dest)
            return {
                "ok": True,
                "source_path": str(self.db_path),
                "backup_path": str(dest),
                "size_bytes": int(dest.stat().st_size) if dest.exists() else 0,
                "integrity": integrity_str,
            }
        finally:
            if tmp_dest.exists():
                try:
                    tmp_dest.unlink()
                except OSError:
                    pass

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 1. Intents (先行書き込み発注意図ログ)
    # ------------------------------------------------------------------
    def create_intent(
        self,
        kind: str,
        symbol: str,
        payload: Dict[str, Any],
        intent_id: Optional[str] = None,
        now_ts_ms: Optional[int] = None,
    ) -> str:
        iid = intent_id or f"intent-{uuid.uuid4().hex[:12]}"
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT INTO intents (id, kind, symbol, payload_json, state, order_id, created_ts_ms, updated_ts_ms)
                    VALUES (?, ?, ?, ?, 'PLANNED', '', ?, ?)
                    """,
                    (iid, kind.strip().upper(), symbol.strip().upper(), dumps_json(payload, indent=None), ts, ts),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise
        return iid

    def update_intent_state(
        self,
        intent_id: str,
        state: str,
        order_id: str = "",
        now_ts_ms: Optional[int] = None,
    ) -> None:
        st = state.strip().upper()
        if st not in INTENT_STATES:
            raise ValueError(f"不正な intent state です: {state}")
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                if order_id:
                    self._conn.execute(
                        "UPDATE intents SET state = ?, order_id = ?, updated_ts_ms = ? WHERE id = ?",
                        (st, str(order_id), ts, intent_id),
                    )
                else:
                    self._conn.execute(
                        "UPDATE intents SET state = ?, updated_ts_ms = ? WHERE id = ?",
                        (st, ts, intent_id),
                    )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def get_intent(self, intent_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM intents WHERE id = ?", (intent_id,)).fetchone()
            return dict(row) if row else None

    def get_unresolved_intents(self) -> List[Dict[str, Any]]:
        """クラッシュリカバリ・再接続時に照合が必要な `PLANNED` / `UNKNOWN` / `SENT_OK` の未完了意図を取得する"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM intents WHERE state IN ('PLANNED', 'UNKNOWN', 'SENT_OK') ORDER BY created_ts_ms ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 2. Orders (注文状態ラティス単調更新)
    # ------------------------------------------------------------------
    def upsert_order(
        self,
        order_id: Union[str, int],
        symbol: str,
        side: str,
        execution_type: str,
        state: str,
        price: Union[str, int, float, Decimal] = 0,
        size: Union[str, int, float, Decimal] = 0,
        executed_size: Union[str, int, float, Decimal] = 0,
        time_in_force: str = "SOK",
        intent_id: str = "",
        now_ts_ms: Optional[int] = None,
        raw_data: Optional[Dict[str, Any]] = None,
        strict_raise: bool = False,
    ) -> bool:
        """
        注文状態ラティス (`WAITING:10 < ORDERED:20 < MODIFYING:25 < CANCELLING:30 < EXECUTED/CANCELED/EXPIRED:90`)
        に従って注文を登録・更新する。
        古いイベントや終端状態からの逆行は拒絶し、`False`（または `strict_raise=True` 時は `OrderLatticeViolationError`）を返す。
        """
        oid = str(order_id).strip()
        sym = symbol.strip().upper()
        side_up = side.strip().upper()
        exec_up = execution_type.strip().upper()
        st_up = state.strip().upper()
        new_rank = ORDER_STATE_RANK.get(st_up, 10)
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)

        size_d = _to_decimal(size)
        exec_d = _to_decimal(executed_size)
        rem_d = max(Decimal("0"), size_d - exec_d)
        price_str = format_decimal_plain(price)
        size_str = format_decimal_plain(size_d)
        exec_str = format_decimal_plain(exec_d)
        rem_str = format_decimal_plain(rem_d)
        raw_json = dumps_json(raw_data or {}, indent=None)

        with self._lock:
            existing = self._conn.execute(
                "SELECT state, state_rank, executed_size_str FROM orders WHERE order_id = ?",
                (oid,),
            ).fetchone()

            if existing is not None:
                old_state = str(existing["state"]).upper()
                old_rank = int(existing["state_rank"])
                old_exec_d = _to_decimal(existing["executed_size_str"])

                # ラティス単調性チェック
                if (old_rank >= 90 and st_up != old_state) or (new_rank < old_rank) or (
                    new_rank == old_rank and exec_d < old_exec_d
                ):
                    if strict_raise:
                        raise OrderLatticeViolationError(
                            f"Order lattice regression rejected for order_id={oid}: {old_state}({old_rank}) -> {st_up}({new_rank})"
                        )
                    logger.debug(
                        "注文ラティス逆行イベントを無視しました (order_id=%s: %s -> %s)",
                        oid,
                        old_state,
                        st_up,
                    )
                    return False

                self._conn.execute("BEGIN IMMEDIATE;")
                try:
                    self._conn.execute(
                        """
                        UPDATE orders
                        SET price_str = ?, size_str = ?, executed_size_str = ?, remaining_size_str = ?,
                            state = ?, state_rank = ?, updated_ts_ms = ?, raw_json = ?
                        WHERE order_id = ?
                        """,
                        (price_str, size_str, exec_str, rem_str, st_up, new_rank, ts, raw_json, oid),
                    )
                    self._conn.execute("COMMIT;")
                except sqlite3.IntegrityError as exc:
                    self._conn.execute("ROLLBACK;")
                    if strict_raise:
                        raise OrderLatticeViolationError(str(exc)) from exc
                    return False
                except Exception:
                    self._conn.execute("ROLLBACK;")
                    raise
                return True

            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, symbol, side, execution_type, time_in_force,
                        price_str, size_str, executed_size_str, remaining_size_str,
                        state, state_rank, intent_id, updated_ts_ms, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        oid,
                        sym,
                        side_up,
                        exec_up,
                        time_in_force.strip().upper(),
                        price_str,
                        size_str,
                        exec_str,
                        rem_str,
                        st_up,
                        new_rank,
                        intent_id,
                        ts,
                        raw_json,
                    ),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise
            return True

    def get_order(self, order_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM orders WHERE order_id = ?", (str(order_id),)).fetchone()
            return dict(row) if row else None

    def get_active_orders_in_db(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if symbol:
                rows = self._conn.execute(
                    "SELECT * FROM orders WHERE state_rank < 90 AND symbol = ? ORDER BY updated_ts_ms ASC",
                    (symbol.strip().upper(),),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM orders WHERE state_rank < 90 ORDER BY updated_ts_ms ASC"
                ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 3. Executions & Tax Ledger (部分約定1件＝1行・方式A/方式B・移動平均法・総平均法)
    # ------------------------------------------------------------------
    def record_execution(
        self,
        execution_id: Union[str, int],
        order_id: Union[str, int],
        symbol: str,
        side: str,
        size: Union[str, int, float, Decimal],
        price: Union[str, int, float, Decimal],
        fee_jpy: Union[str, int, float, Decimal] = 0,
        execution_type: str = "LIMIT",
        executed_ts_ms: Optional[int] = None,
        raw_data: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        約定1件をべき等 (`INSERT OR IGNORE`) に記録し、新規挿入された場合のみ
        移動平均法台帳 (`tax_moving_avg_ledger`) を方式A（原則法）・方式B（純受渡法）の双方で更新する。
        - GMOコイン仕様に従い、買付/売却の約定金額は切り捨て、Taker手数料(+fee)は切り上げ、Maker報奨金(-fee)は切り捨ての整数円で計上。
        """
        exec_id = str(execution_id).strip()
        oid = str(order_id).strip()
        sym = symbol.strip().upper()
        side_up = side.strip().upper()
        exec_up = execution_type.strip().upper()

        size_d = _to_decimal(size)
        price_d = _to_decimal(price)
        fee_d = _to_decimal(fee_jpy)

        size_sat = to_satoshi_int(size_d)
        price_sat = to_satoshi_int(price_d)
        gross_jpy = int((size_d * price_d).to_integral_value(rounding=ROUND_DOWN))

        if fee_d > 0:
            taker_fee_jpy = int(fee_d.to_integral_value(rounding=ROUND_CEILING))
            maker_rebate_jpy = 0
        elif fee_d < 0:
            taker_fee_jpy = 0
            maker_rebate_jpy = int(abs(fee_d).to_integral_value(rounding=ROUND_FLOOR))
        else:
            taker_fee_jpy = 0
            maker_rebate_jpy = 0

        if side_up == "BUY":
            net_cashflow_jpy = -(gross_jpy + taker_fee_jpy - maker_rebate_jpy)
        else:
            net_cashflow_jpy = +(gross_jpy - taker_fee_jpy + maker_rebate_jpy)

        ts_ms = int(executed_ts_ms if executed_ts_ms is not None else time.time() * 1000)
        dt_jst = datetime.fromtimestamp(ts_ms / 1000.0, tz=JST)
        tax_year = dt_jst.year

        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?",
                (exec_id,),
            ).fetchone()
            if existing is not None:
                return False, dict(existing)

            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                ledger_row = self._conn.execute(
                    "SELECT * FROM tax_moving_avg_ledger WHERE symbol = ?",
                    (sym,),
                ).fetchone()

                if ledger_row is None:
                    old_sat = 0
                    book_a = 0
                    pnl_a_total = 0
                    misc_a_total = 0
                    book_b = 0
                    pnl_b_total = 0
                else:
                    old_sat = int(ledger_row["total_size_satoshi"])
                    book_a = int(ledger_row["book_value_a_jpy"])
                    pnl_a_total = int(ledger_row["realized_pnl_a_jpy"])
                    misc_a_total = int(ledger_row["misc_rebate_income_a_jpy"])
                    book_b = int(ledger_row["book_value_b_jpy"])
                    pnl_b_total = int(ledger_row["realized_pnl_b_jpy"])

                if side_up == "BUY":
                    # 方式A（原則法）: 取得価額 = 約定代金 + Taker手数料、Maker報奨金は雑所得として独立計上
                    delta_a = gross_jpy + taker_fee_jpy
                    rebate_misc_a = maker_rebate_jpy
                    realized_a = 0
                    new_book_a = book_a + delta_a

                    # 方式B（純受渡法）: 取得価額 = 約定代金 + Taker手数料 - Maker報奨金
                    delta_b = gross_jpy + taker_fee_jpy - maker_rebate_jpy
                    realized_b = 0
                    new_book_b = book_b + delta_b

                    new_sat = old_sat + size_sat
                else:
                    # SELL: 移動平均簿価から売却数量分の譲渡原価を算出
                    sell_sat = min(old_sat, size_sat) if old_sat > 0 else size_sat
                    if old_sat > 0 and sell_sat > 0:
                        cost_sold_a = int(
                            (Decimal(book_a) * Decimal(sell_sat) / Decimal(old_sat)).to_integral_value(
                                rounding=ROUND_HALF_UP
                            )
                        )
                        cost_sold_b = int(
                            (Decimal(book_b) * Decimal(sell_sat) / Decimal(old_sat)).to_integral_value(
                                rounding=ROUND_HALF_UP
                            )
                        )
                    else:
                        cost_sold_a = 0
                        cost_sold_b = 0

                    delta_a = -cost_sold_a
                    rebate_misc_a = maker_rebate_jpy
                    proceeds_a = gross_jpy - taker_fee_jpy
                    realized_a = proceeds_a - cost_sold_a
                    new_book_a = max(0, book_a - cost_sold_a)

                    delta_b = -cost_sold_b
                    proceeds_b = gross_jpy - taker_fee_jpy + maker_rebate_jpy
                    realized_b = proceeds_b - cost_sold_b
                    new_book_b = max(0, book_b - cost_sold_b)

                    new_sat = max(0, old_sat - size_sat)

                new_size_dec = from_satoshi_int(new_sat)
                avg_a = float(Decimal(new_book_a) / new_size_dec) if new_sat > 0 else 0.0
                avg_b = float(Decimal(new_book_b) / new_size_dec) if new_sat > 0 else 0.0

                cur = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO executions (
                        execution_id, order_id, symbol, side, execution_type,
                        size_str, size_satoshi, price_str, price_satoshi,
                        gross_amount_jpy, taker_fee_jpy, maker_rebate_jpy, net_cashflow_jpy,
                        method_a_cost_basis_delta_jpy, method_a_rebate_misc_income_jpy, method_a_realized_pnl_jpy,
                        method_b_cost_basis_delta_jpy, method_b_realized_pnl_jpy,
                        executed_ts_ms, tax_year, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        exec_id,
                        oid,
                        sym,
                        side_up,
                        exec_up,
                        format_decimal_plain(size_d),
                        size_sat,
                        format_decimal_plain(price_d),
                        price_sat,
                        gross_jpy,
                        taker_fee_jpy,
                        maker_rebate_jpy,
                        net_cashflow_jpy,
                        delta_a,
                        rebate_misc_a,
                        realized_a,
                        delta_b,
                        realized_b,
                        ts_ms,
                        tax_year,
                        dumps_json(raw_data or {}, indent=None),
                    ),
                )
                if cur.rowcount == 1:
                    self._conn.execute(
                        """
                        INSERT INTO tax_moving_avg_ledger (
                            symbol, total_size_str, total_size_satoshi,
                            book_value_a_jpy, avg_cost_a_jpy, realized_pnl_a_jpy, misc_rebate_income_a_jpy,
                            book_value_b_jpy, avg_cost_b_jpy, realized_pnl_b_jpy, updated_ts_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            total_size_str = excluded.total_size_str,
                            total_size_satoshi = excluded.total_size_satoshi,
                            book_value_a_jpy = excluded.book_value_a_jpy,
                            avg_cost_a_jpy = excluded.avg_cost_a_jpy,
                            realized_pnl_a_jpy = excluded.realized_pnl_a_jpy,
                            misc_rebate_income_a_jpy = excluded.misc_rebate_income_a_jpy,
                            book_value_b_jpy = excluded.book_value_b_jpy,
                            avg_cost_b_jpy = excluded.avg_cost_b_jpy,
                            realized_pnl_b_jpy = excluded.realized_pnl_b_jpy,
                            updated_ts_ms = excluded.updated_ts_ms
                        """,
                        (
                            sym,
                            format_decimal_plain(new_size_dec),
                            new_sat,
                            new_book_a,
                            avg_a,
                            pnl_a_total + realized_a,
                            misc_a_total + rebate_misc_a,
                            new_book_b,
                            avg_b,
                            pnl_b_total + realized_b,
                            ts_ms,
                        ),
                    )
                    # Forensic Ledger 共有バス (`tax_ledger_events`) への自動記録
                    if side_up == "BUY":
                        tax_meta_buy = classify_jp_crypto_tax_regime(dt_jst, realized_pnl_jpy=0)
                        self._insert_tax_ledger_event_unlocked(
                            event_id=f"exec_open_{exec_id}",
                            timestamp_jst=tax_meta_buy["timestamp_jst"],
                            tax_year=tax_meta_buy["tax_year"],
                            tax_regime=tax_meta_buy["tax_regime"],
                            effective_tax_rate=tax_meta_buy["effective_tax_rate"],
                            loss_carryforward_years=tax_meta_buy["loss_carryforward_years"],
                            exchange_registration=tax_meta_buy["exchange_registration"],
                            symbol=sym,
                            event_type="SPOT_TRADE_BUY",
                            side=side_up,
                            size=float(size_d),
                            price_jpy=float(price_d),
                            gross_proceeds_jpy=float(gross_jpy),
                            cost_basis_jpy=float(delta_b),
                            fee_or_rebate_jpy=float(maker_rebate_jpy - taker_fee_jpy),
                            realized_pnl_jpy=0.0,
                            estimated_tax_jpy=0.0,
                            after_tax_net_pnl_jpy=0.0,
                            source_order_id=oid,
                            created_ts_ms=ts_ms,
                        )
                    elif side_up == "SELL":
                        tax_meta_sell = classify_jp_crypto_tax_regime(dt_jst, realized_pnl_jpy=realized_b)
                        self._insert_tax_ledger_event_unlocked(
                            event_id=f"exec_close_{exec_id}",
                            timestamp_jst=tax_meta_sell["timestamp_jst"],
                            tax_year=tax_meta_sell["tax_year"],
                            tax_regime=tax_meta_sell["tax_regime"],
                            effective_tax_rate=tax_meta_sell["effective_tax_rate"],
                            loss_carryforward_years=tax_meta_sell["loss_carryforward_years"],
                            exchange_registration=tax_meta_sell["exchange_registration"],
                            symbol=sym,
                            event_type="SPOT_TRADE_CLOSE",
                            side=side_up,
                            size=float(size_d),
                            price_jpy=float(price_d),
                            gross_proceeds_jpy=float(gross_jpy),
                            cost_basis_jpy=float(abs(delta_b)),
                            fee_or_rebate_jpy=float(maker_rebate_jpy - taker_fee_jpy),
                            realized_pnl_jpy=float(realized_b),
                            estimated_tax_jpy=tax_meta_sell["estimated_tax_jpy"],
                            after_tax_net_pnl_jpy=tax_meta_sell["after_tax_net_pnl_jpy"],
                            source_order_id=oid,
                            created_ts_ms=ts_ms,
                        )
                    if maker_rebate_jpy > 0:
                        tax_meta_reb = classify_jp_crypto_tax_regime(dt_jst, realized_pnl_jpy=maker_rebate_jpy)
                        self._insert_tax_ledger_event_unlocked(
                            event_id=f"rebate_{exec_id}",
                            timestamp_jst=tax_meta_reb["timestamp_jst"],
                            tax_year=tax_meta_reb["tax_year"],
                            tax_regime=tax_meta_reb["tax_regime"],
                            effective_tax_rate=tax_meta_reb["effective_tax_rate"],
                            loss_carryforward_years=tax_meta_reb["loss_carryforward_years"],
                            exchange_registration=tax_meta_reb["exchange_registration"],
                            symbol=sym,
                            event_type="MAKER_REBATE",
                            side=side_up,
                            size=float(size_d),
                            price_jpy=float(price_d),
                            gross_proceeds_jpy=float(maker_rebate_jpy),
                            cost_basis_jpy=0.0,
                            fee_or_rebate_jpy=float(maker_rebate_jpy),
                            realized_pnl_jpy=float(maker_rebate_jpy),
                            estimated_tax_jpy=tax_meta_reb["estimated_tax_jpy"],
                            after_tax_net_pnl_jpy=tax_meta_reb["after_tax_net_pnl_jpy"],
                            source_order_id=oid,
                            created_ts_ms=ts_ms,
                        )
                    elif taker_fee_jpy > 0:
                        tax_meta_fee = classify_jp_crypto_tax_regime(dt_jst, realized_pnl_jpy=-taker_fee_jpy)
                        self._insert_tax_ledger_event_unlocked(
                            event_id=f"fee_{exec_id}",
                            timestamp_jst=tax_meta_fee["timestamp_jst"],
                            tax_year=tax_meta_fee["tax_year"],
                            tax_regime=tax_meta_fee["tax_regime"],
                            effective_tax_rate=tax_meta_fee["effective_tax_rate"],
                            loss_carryforward_years=tax_meta_fee["loss_carryforward_years"],
                            exchange_registration=tax_meta_fee["exchange_registration"],
                            symbol=sym,
                            event_type="TAKER_FEE",
                            side=side_up,
                            size=float(size_d),
                            price_jpy=float(price_d),
                            gross_proceeds_jpy=0.0,
                            cost_basis_jpy=float(taker_fee_jpy),
                            fee_or_rebate_jpy=float(-taker_fee_jpy),
                            realized_pnl_jpy=float(-taker_fee_jpy),
                            estimated_tax_jpy=0.0,
                            after_tax_net_pnl_jpy=float(-taker_fee_jpy),
                            source_order_id=oid,
                            created_ts_ms=ts_ms,
                        )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

            inserted_row = self._conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?",
                (exec_id,),
            ).fetchone()
            return True, dict(inserted_row) if inserted_row else None

    def _insert_tax_ledger_event_unlocked(
        self,
        event_id: str,
        timestamp_jst: str,
        tax_year: int,
        tax_regime: str,
        effective_tax_rate: float,
        loss_carryforward_years: int,
        exchange_registration: str,
        symbol: str,
        event_type: str,
        side: str,
        size: float,
        price_jpy: float,
        gross_proceeds_jpy: float,
        cost_basis_jpy: float,
        fee_or_rebate_jpy: float,
        realized_pnl_jpy: float,
        estimated_tax_jpy: float,
        after_tax_net_pnl_jpy: float,
        source_order_id: str,
        created_ts_ms: int,
    ) -> bool:
        cur = self._conn.execute(
            """
            INSERT OR REPLACE INTO tax_ledger_events (
                event_id, timestamp_jst, tax_year, tax_regime, effective_tax_rate,
                loss_carryforward_years, exchange_registration, symbol, event_type, side,
                size, price_jpy, gross_proceeds_jpy, cost_basis_jpy, fee_or_rebate_jpy,
                realized_pnl_jpy, estimated_tax_jpy, after_tax_net_pnl_jpy, source_order_id, created_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event_id).strip(),
                str(timestamp_jst).strip(),
                int(tax_year),
                str(tax_regime).strip().upper(),
                float(effective_tax_rate),
                int(loss_carryforward_years),
                str(exchange_registration or "JP_FSA_REGISTERED_GMO").strip(),
                str(symbol or "JPY").strip().upper(),
                str(event_type or "SPOT_TRADE_CLOSE").strip().upper(),
                str(side or "").strip().upper(),
                float(size or 0.0),
                float(price_jpy or 0.0),
                float(gross_proceeds_jpy or 0.0),
                float(cost_basis_jpy or 0.0),
                float(fee_or_rebate_jpy or 0.0),
                float(realized_pnl_jpy or 0.0),
                float(estimated_tax_jpy or 0.0),
                float(after_tax_net_pnl_jpy or 0.0),
                str(source_order_id or "").strip(),
                int(created_ts_ms),
            ),
        )
        return cur.rowcount > 0

    def record_tax_ledger_event(
        self,
        event_id: str,
        symbol: str,
        event_type: str = "SPOT_TRADE_CLOSE",
        side: str = "SELL",
        size: Union[int, float, Decimal] = 0.0,
        price_jpy: Union[int, float, Decimal] = 0.0,
        gross_proceeds_jpy: Union[int, float, Decimal] = 0.0,
        cost_basis_jpy: Union[int, float, Decimal] = 0.0,
        fee_or_rebate_jpy: Union[int, float, Decimal] = 0.0,
        realized_pnl_jpy: Union[int, float, Decimal] = 0.0,
        source_order_id: str = "",
        timestamp_jst: Optional[Union[str, int, float, datetime]] = None,
        now_ts_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        2027年以前（`AGGREGATE_MISC`, `55.945%`, 繰越0年）および
        2028年1月1日以降（`SEPARATE_20315`, `20.315%`, 損失繰越3年, `JP_FSA_REGISTERED_GMO`）の税務メタデータを付与し、
        `tax_ledger_events` テーブル（`v_forensic_tax_ledger` ビュー）へ記録する。
        """
        tax_meta = classify_jp_crypto_tax_regime(
            ts_jst=timestamp_jst if timestamp_jst is not None else now_ts_ms,
            realized_pnl_jpy=realized_pnl_jpy,
        )
        dt_parsed = datetime.fromisoformat(tax_meta["timestamp_jst"])
        ts = int(now_ts_ms if now_ts_ms is not None else dt_parsed.timestamp() * 1000)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._insert_tax_ledger_event_unlocked(
                    event_id=event_id,
                    timestamp_jst=tax_meta["timestamp_jst"],
                    tax_year=tax_meta["tax_year"],
                    tax_regime=tax_meta["tax_regime"],
                    effective_tax_rate=tax_meta["effective_tax_rate"],
                    loss_carryforward_years=tax_meta["loss_carryforward_years"],
                    exchange_registration=tax_meta["exchange_registration"],
                    symbol=symbol,
                    event_type=event_type,
                    side=side,
                    size=float(size or 0.0),
                    price_jpy=float(price_jpy or 0.0),
                    gross_proceeds_jpy=float(gross_proceeds_jpy or 0.0),
                    cost_basis_jpy=float(cost_basis_jpy or 0.0),
                    fee_or_rebate_jpy=float(fee_or_rebate_jpy or 0.0),
                    realized_pnl_jpy=tax_meta["realized_pnl_jpy"],
                    estimated_tax_jpy=tax_meta["estimated_tax_jpy"],
                    after_tax_net_pnl_jpy=tax_meta["after_tax_net_pnl_jpy"],
                    source_order_id=source_order_id,
                    created_ts_ms=ts,
                )
                self._conn.execute("COMMIT;")
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

        return {
            "event_id": str(event_id),
            **tax_meta,
            "symbol": str(symbol).strip().upper(),
            "event_type": str(event_type).strip().upper(),
            "side": str(side).strip().upper(),
            "size": float(size or 0.0),
            "price_jpy": float(price_jpy or 0.0),
            "gross_proceeds_jpy": float(gross_proceeds_jpy or 0.0),
            "cost_basis_jpy": float(cost_basis_jpy or 0.0),
            "fee_or_rebate_jpy": float(fee_or_rebate_jpy or 0.0),
            "source_order_id": str(source_order_id or ""),
            "created_ts_ms": ts,
        }

    def query_forensic_tax_ledger(
        self,
        tax_year: Optional[int] = None,
        tax_regime: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Read-Only WAL 共有バス (`connect_forensic_readonly`) 経由で `v_forensic_tax_ledger` を照会する。
        """
        ro_conn = self.connect_forensic_readonly()
        try:
            clauses: List[str] = []
            params: List[Any] = []
            if tax_year is not None:
                clauses.append("tax_year = ?")
                params.append(int(tax_year))
            if tax_regime:
                clauses.append("tax_regime = ?")
                params.append(str(tax_regime).strip().upper())
            if event_type:
                clauses.append("event_type = ?")
                params.append(str(event_type).strip().upper())
            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = ro_conn.execute(
                f"SELECT * FROM v_forensic_tax_ledger {where_sql} ORDER BY created_ts_ms ASC, event_id ASC",
                tuple(params),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            ro_conn.close()

    def has_execution_for_order(self, order_id: Union[str, int]) -> bool:
        """指定された `order_id` に紐づく約定レコードが既に `executions` テーブルに存在するか確認する"""
        oid = str(order_id).strip()
        if not oid:
            return False
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM executions WHERE order_id = ? LIMIT 1",
                (oid,),
            ).fetchone()
            return row is not None

    def get_executed_size_for_order(self, order_id: Union[str, int]) -> Decimal:
        """指定された `order_id` について既に `executions` テーブルに記録済みの合計約定数量を返す"""
        oid = str(order_id).strip()
        if not oid:
            return Decimal("0")
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(size_satoshi), 0) AS total_sat FROM executions WHERE order_id = ?",
                (oid,),
            ).fetchone()
            total_sat = int(row["total_sat"] or 0) if row is not None else 0
            return Decimal(total_sat) / Decimal(SATOSHI_SCALE)

    def get_high_watermark_execution_id(self, symbol: Optional[str] = None) -> int:
        """記録済み `execution_id`（数値変換可能なもの）の最大値（ハイウォーターマーク）を返す"""
        with self._lock:
            if symbol:
                rows = self._conn.execute(
                    "SELECT execution_id FROM executions WHERE symbol = ?",
                    (symbol.strip().upper(),),
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT execution_id FROM executions").fetchall()
            max_id = 0
            for r in rows:
                eid_str = str(r["execution_id"]).strip()
                if eid_str.isdigit():
                    val = int(eid_str)
                    if val > max_id:
                        max_id = val
            return max_id

    def get_tax_moving_avg_ledger(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if symbol:
                rows = self._conn.execute(
                    "SELECT * FROM tax_moving_avg_ledger WHERE symbol = ?",
                    (symbol.strip().upper(),),
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM tax_moving_avg_ledger ORDER BY symbol ASC").fetchall()
            return [dict(r) for r in rows]

    def get_annual_total_average_summary(self, tax_year: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if tax_year is not None:
                rows = self._conn.execute(
                    "SELECT * FROM v_tax_total_average_annual WHERE tax_year = ? ORDER BY symbol ASC",
                    (int(tax_year),),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM v_tax_total_average_annual ORDER BY tax_year ASC, symbol ASC"
                ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 4. Fee Adjustments (朝06:00 JST 端数返金吸収)
    # ------------------------------------------------------------------
    def record_fee_adjustment(
        self,
        business_date: str = "",
        delta_jpy: int = 0,
        jpy_balance_before: int = 0,
        jpy_balance_after: int = 0,
        reason: str = "FEE_REFUND_ADJUSTMENT",
        now_ts_ms: Optional[int] = None,
        date_str: Optional[str] = None,
        amount_jpy: Optional[int] = None,
        note: Optional[str] = None,
    ) -> bool:
        eff_date = str(date_str or business_date or datetime.now(JST).strftime("%Y-%m-%d")).strip()
        eff_delta = int(amount_jpy if amount_jpy is not None else delta_jpy)
        eff_reason = str(note or reason or "FEE_REFUND_ADJUSTMENT").strip()
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        try:
            dt_event = datetime.fromisoformat(f"{eff_date}T06:00:30+09:00")
        except Exception:
            dt_event = datetime.fromtimestamp(ts / 1000.0, tz=JST)
        tax_meta = classify_jp_crypto_tax_regime(dt_event, realized_pnl_jpy=eff_delta)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO fee_adjustments (
                        business_date, delta_jpy, jpy_balance_before, jpy_balance_after, reason, created_ts_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (eff_date, eff_delta, int(jpy_balance_before), int(jpy_balance_after), eff_reason, ts),
                )
                inserted = cur.rowcount == 1
                if inserted:
                    self._insert_tax_ledger_event_unlocked(
                        event_id=f"fee_refund_{eff_date}",
                        timestamp_jst=tax_meta["timestamp_jst"],
                        tax_year=tax_meta["tax_year"],
                        tax_regime=tax_meta["tax_regime"],
                        effective_tax_rate=tax_meta["effective_tax_rate"],
                        loss_carryforward_years=tax_meta["loss_carryforward_years"],
                        exchange_registration=tax_meta["exchange_registration"],
                        symbol="JPY",
                        event_type="DAILY_FEE_FRACTION_REFUND",
                        side="REFUND",
                        size=0.0,
                        price_jpy=1.0,
                        gross_proceeds_jpy=float(eff_delta),
                        cost_basis_jpy=0.0,
                        fee_or_rebate_jpy=float(eff_delta),
                        realized_pnl_jpy=float(eff_delta),
                        estimated_tax_jpy=tax_meta["estimated_tax_jpy"],
                        after_tax_net_pnl_jpy=tax_meta["after_tax_net_pnl_jpy"],
                        source_order_id=f"DAILY_0600_{eff_date}",
                        created_ts_ms=ts,
                    )
                self._conn.execute("COMMIT;")
                return inserted
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    # ------------------------------------------------------------------
    # 5. Positions (8状態ステートマシン ＆ DUST_QUARANTINED 端数隔離)
    # ------------------------------------------------------------------
    def upsert_position(
        self,
        symbol: str = "",
        state: str = "PROTECTED_ACTIVE",
        size: Union[str, int, float, Decimal] = 0,
        entry_price: Union[str, int, float, Decimal] = 0,
        take_profit_price: Union[str, int, float, Decimal] = 0,
        stop_loss_price: Union[str, int, float, Decimal] = 0,
        initial_stop_loss_price: Union[str, int, float, Decimal] = 0,
        initial_risk_jpy: float = 150.0,
        buy_order_id: str = "",
        sl_order_id: str = "",
        breakeven_locked: bool = False,
        trailing_stage: int = 0,
        opened_at: str = "",
        unprotected_since_ts_ms: int = 0,
        extra: Optional[Dict[str, Any]] = None,
        now_ts_ms: Optional[int] = None,
        position_id: Optional[str] = None,
    ) -> None:
        sym_up = symbol.strip().upper()
        pid = str(position_id or f"pos-{sym_up}")
        st_up = state.strip().upper()
        if st_up not in POSITION_STATES:
            raise ValueError(f"未定義のポジション状態です: {state}")
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        size_d = _to_decimal(size)
        size_sat = to_satoshi_int(size_d)
        if st_up == "UNPROTECTED_HOLD" and unprotected_since_ts_ms <= 0:
            unprotected_since_ts_ms = ts
        elif st_up in ("PROTECTED_ACTIVE", "PROTECTED", "SL_ACTIVE", "CLOSED", "DUST_QUARANTINED"):
            unprotected_since_ts_ms = 0

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT INTO positions (
                        position_id, symbol, state, size_str, size_satoshi,
                        entry_price_str, take_profit_price_str, stop_loss_price_str, initial_stop_loss_price_str,
                        initial_risk_jpy, buy_order_id, sl_order_id, breakeven_locked, trailing_stage,
                        opened_at, unprotected_since_ts_ms, updated_ts_ms, extra_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(position_id) DO UPDATE SET
                        symbol = excluded.symbol,
                        state = excluded.state,
                        size_str = excluded.size_str,
                        size_satoshi = excluded.size_satoshi,
                        entry_price_str = excluded.entry_price_str,
                        take_profit_price_str = excluded.take_profit_price_str,
                        stop_loss_price_str = excluded.stop_loss_price_str,
                        initial_stop_loss_price_str = excluded.initial_stop_loss_price_str,
                        initial_risk_jpy = excluded.initial_risk_jpy,
                        buy_order_id = excluded.buy_order_id,
                        sl_order_id = excluded.sl_order_id,
                        breakeven_locked = excluded.breakeven_locked,
                        trailing_stage = excluded.trailing_stage,
                        opened_at = excluded.opened_at,
                        unprotected_since_ts_ms = excluded.unprotected_since_ts_ms,
                        updated_ts_ms = excluded.updated_ts_ms,
                        extra_json = excluded.extra_json
                    """,
                    (
                        pid,
                        sym_up,
                        st_up,
                        format_decimal_plain(size_d),
                        size_sat,
                        format_decimal_plain(entry_price),
                        format_decimal_plain(take_profit_price),
                        format_decimal_plain(stop_loss_price),
                        format_decimal_plain(initial_stop_loss_price or stop_loss_price),
                        float(initial_risk_jpy),
                        str(buy_order_id),
                        str(sl_order_id),
                        1 if breakeven_locked else 0,
                        int(trailing_stage),
                        opened_at or datetime.fromtimestamp(ts / 1000.0, tz=JST).isoformat(timespec="seconds"),
                        int(unprotected_since_ts_ms),
                        ts,
                        dumps_json(extra or {}, indent=None),
                    ),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def quarantine_dust_position(
        self,
        symbol: str,
        dust_size: Union[str, int, float, Decimal],
        entry_price: Union[str, int, float, Decimal] = 0,
        reason: str = "SUB_MIN_ORDER_SIZE_DUST",
        now_ts_ms: Optional[int] = None,
    ) -> str:
        """最小注文数量 (`minOrderSize`) 未満の端数在庫を `DUST_QUARANTINED` 状態として台帳に隔離記録する"""
        sym = symbol.strip().upper()
        pid = f"dust-{sym}-{int(now_ts_ms or time.time() * 1000)}"
        self.upsert_position(
            position_id=pid,
            symbol=sym,
            state="DUST_QUARANTINED",
            size=dust_size,
            entry_price=entry_price,
            extra={"quarantine_reason": reason},
            now_ts_ms=now_ts_ms,
        )
        return pid

    def get_positions_by_states(self, states: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if states:
                placeholders = ",".join("?" for _ in states)
                rows = self._conn.execute(
                    f"SELECT * FROM positions WHERE state IN ({placeholders}) ORDER BY updated_ts_ms ASC",
                    tuple(s.upper() for s in states),
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM positions ORDER BY updated_ts_ms ASC").fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 6. Control Commands (スマホ/Discord 非同期コマンドキュー)
    # ------------------------------------------------------------------
    def enqueue_control_command(
        self,
        command: str,
        nonce: str,
        requested_by: str,
        status: str = "PENDING",
        payload: Optional[Dict[str, Any]] = None,
        now_ts_ms: Optional[int] = None,
    ) -> bool:
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO control_commands (
                        command, nonce, requested_by, status, payload_json, created_ts_ms, executed_ts_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        command.strip().upper(),
                        nonce.strip(),
                        str(requested_by),
                        status.strip().upper(),
                        dumps_json(payload or {}, indent=None),
                        ts,
                    ),
                )
                self._conn.execute("COMMIT;")
                return cur.rowcount == 1
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def fetch_pending_control_commands(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM control_commands WHERE status = 'PENDING' ORDER BY id ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_control_command_by_nonce(self, nonce: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM control_commands WHERE nonce = ?",
                (str(nonce or "").strip(),),
            ).fetchone()
            return dict(row) if row is not None else None

    def mark_control_command(
        self,
        nonce: str,
        status: str,
        now_ts_ms: Optional[int] = None,
    ) -> bool:
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = self._conn.execute(
                    "UPDATE control_commands SET status = ?, executed_ts_ms = ? WHERE nonce = ?",
                    (status.strip().upper(), ts, nonce.strip()),
                )
                self._conn.execute("COMMIT;")
                return cur.rowcount > 0
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    # ------------------------------------------------------------------
    # 7. LLM Shadow Evaluations (4h / 12h / 24h 経路依存検証テーブル)
    # ------------------------------------------------------------------
    def insert_llm_shadow_evaluation(
        self,
        eval_id: str,
        symbol: str,
        decision: str,
        conviction_score: int,
        entry_price: float,
        stop_loss_price: float,
        take_profit_price: float,
        bearish_counter_thesis: str = "",
        bullish_conviction_factors: str = "",
        now_ts_ms: Optional[int] = None,
    ) -> None:
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO llm_shadow_evaluations (
                        eval_id, symbol, decision, conviction_score,
                        entry_price, stop_loss_price, take_profit_price,
                        bearish_counter_thesis, bullish_conviction_factors,
                        created_ts_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        eval_id,
                        symbol.strip().upper(),
                        decision.strip().upper(),
                        int(conviction_score),
                        float(entry_price),
                        float(stop_loss_price),
                        float(take_profit_price),
                        str(bearish_counter_thesis),
                        str(bullish_conviction_factors),
                        ts,
                    ),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def update_llm_shadow_outcome(
        self,
        eval_id: str,
        mfe_4h_pct: float,
        mae_4h_pct: float,
        pnl_4h_pct: float,
        mfe_12h_pct: float,
        mae_12h_pct: float,
        pnl_12h_pct: float,
        mfe_24h_pct: float,
        mae_24h_pct: float,
        pnl_24h_pct: float,
        stop_hit_first: bool,
        outcome_class: str,
        now_ts_ms: Optional[int] = None,
    ) -> None:
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    UPDATE llm_shadow_evaluations
                    SET mfe_4h_pct = ?, mae_4h_pct = ?, pnl_4h_pct = ?,
                        mfe_12h_pct = ?, mae_12h_pct = ?, pnl_12h_pct = ?,
                        mfe_24h_pct = ?, mae_24h_pct = ?, pnl_24h_pct = ?,
                        stop_hit_first = ?, outcome_class = ?, evaluated_ts_ms = ?
                    WHERE eval_id = ?
                    """,
                    (
                        float(mfe_4h_pct),
                        float(mae_4h_pct),
                        float(pnl_4h_pct),
                        float(mfe_12h_pct),
                        float(mae_12h_pct),
                        float(pnl_12h_pct),
                        float(mfe_24h_pct),
                        float(mae_24h_pct),
                        float(pnl_24h_pct),
                        1 if stop_hit_first else 0,
                        outcome_class.strip().upper(),
                        ts,
                        eval_id,
                    ),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    # ------------------------------------------------------------------
    # 8. Portfolio JSON & History CSV アトミック同期出力
    # ------------------------------------------------------------------
    def sync_portfolio_to_Sqlite_and_json(
        self,
        portfolio: Dict[str, Any],
        now_ts_ms: Optional[int] = None,
    ) -> None:
        """
        ポートフォリオ辞書を SQLite の `account_meta` および `positions` テーブルと同期し、
        さらに `portfolio.json` へ一時ファイル＋`os.replace` でアトミック出力する。
        """
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT INTO account_meta (key, value_json, updated_ts_ms)
                    VALUES ('portfolio_snapshot', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_ts_ms = excluded.updated_ts_ms
                    """,
                    (dumps_json(portfolio, indent=None), ts),
                )
                peak_eq_val = portfolio.get("peak_equity_jpy", portfolio.get("initial_capital_jpy", 20000))
                self._conn.execute(
                    """
                    INSERT INTO account_meta (key, value_json, updated_ts_ms)
                    VALUES ('peak_equity_jpy', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_ts_ms = excluded.updated_ts_ms
                    """,
                    (dumps_json(peak_eq_val, indent=None), ts),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
                if self._write_count % 100 == 0:
                    try:
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    except Exception:
                        pass
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

            if self.auto_sync_files and self.portfolio_json_path is not None:
                atomic_write_text(self.portfolio_json_path, dumps_json(portfolio, indent=2))

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute(
                "SELECT value_json FROM account_meta WHERE key = ?",
                (str(key),),
            ).fetchone()
            if row:
                try:
                    return json.loads(row["value_json"])
                except Exception:
                    return row["value_json"]
            return default

    def set_meta(self, key: str, value: Any, now_ts_ms: Optional[int] = None) -> None:
        ts = int(now_ts_ms if now_ts_ms is not None else time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    INSERT INTO account_meta (key, value_json, updated_ts_ms)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_ts_ms = excluded.updated_ts_ms
                    """,
                    (str(key), dumps_json(value, indent=None), ts),
                )
                self._conn.execute("COMMIT;")
                self._write_count += 1
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def load_portfolio_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value_json FROM account_meta WHERE key = 'portfolio_snapshot'"
            ).fetchone()
            if row:
                try:
                    return json.loads(row["value_json"])
                except Exception:
                    pass
            if self.portfolio_json_path and self.portfolio_json_path.exists():
                try:
                    return load_json_file(self.portfolio_json_path)
                except Exception:
                    pass
            return None

    # ------------------------------------------------------------------
    # 9. モジュール間連携エイリアス（reconciler / remote_control / shadow_evaluator 互換）
    # ------------------------------------------------------------------
    def record_execution_idempotent(
        self,
        execution_id: str,
        order_id: str,
        symbol: str,
        side: str,
        price_str: str,
        size_str: str,
        fee_jpy_str: str = "0",
        timestamp_str: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ts_ms: Optional[int] = None
        if timestamp_str:
            try:
                dt = datetime.fromisoformat(str(timestamp_str).replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
            except Exception:
                ts_ms = None
        return self.record_execution(
            execution_id=execution_id,
            order_id=order_id,
            symbol=symbol,
            side=side,
            size=size_str,
            price=price_str,
            fee_jpy=fee_jpy_str,
            executed_ts_ms=ts_ms,
        )

    def record_fee_refund_adjustment(
        self,
        adjustment_jpy: int,
        balance_before: int,
        balance_after: int,
        reason: str = "FEE_REFUND_ADJUSTMENT",
        detected_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        dt = detected_at or datetime.now(JST)
        b_date = dt.strftime("%Y-%m-%d")
        inserted = self.record_fee_adjustment(
            business_date=b_date,
            delta_jpy=adjustment_jpy,
            jpy_balance_before=balance_before,
            jpy_balance_after=balance_after,
            reason=reason,
            now_ts_ms=int(dt.timestamp() * 1000),
        )
        return {
            "inserted": inserted,
            "business_date": b_date,
            "adjustment_jpy": int(adjustment_jpy),
            "balance_before_jpy": int(balance_before),
            "balance_after_jpy": int(balance_after),
            "reason": reason,
        }

    def register_control_command(
        self,
        nonce: str,
        command: str,
        actor_id: str = "LOCAL_OWNER",
        require_confirm: bool = False,
        now_dt: Optional[datetime] = None,
    ) -> bool:
        dt = now_dt or datetime.now(JST)
        status = "AWAITING_CONFIRM" if require_confirm else "PENDING"
        return self.enqueue_control_command(
            command=command,
            nonce=nonce,
            requested_by=actor_id,
            status=status,
            now_ts_ms=int(dt.timestamp() * 1000),
        )

    def confirm_and_execute_command(
        self,
        nonce: str,
        actor_id: str = "LOCAL_OWNER",
        now_dt: Optional[datetime] = None,
    ) -> bool:
        dt = now_dt or datetime.now(JST)
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM control_commands WHERE nonce = ?",
                (nonce.strip(),),
            ).fetchone()
            if row is None or str(row["status"]).upper() == "EXECUTED":
                return False
        return self.mark_control_command(
            nonce=nonce,
            status="EXECUTED",
            now_ts_ms=int(dt.timestamp() * 1000),
        )

    def record_shadow_evaluation(
        self,
        eval_id: str,
        symbol: str,
        ai_action: str,
        conviction_score: int,
        entry_price: Union[str, float],
        tp_price: Union[str, float] = 0.0,
        sl_price: Union[str, float] = 0.0,
        radar_score: float = 0.0,
        bearish_counter_thesis: str = "",
        bullish_conviction_factors: str = "",
        evaluated_at: Optional[datetime] = None,
    ) -> None:
        dt = evaluated_at or datetime.now(JST)
        self.insert_llm_shadow_evaluation(
            eval_id=eval_id,
            symbol=symbol,
            decision=ai_action,
            conviction_score=conviction_score,
            entry_price=float(entry_price or 0.0),
            stop_loss_price=float(sl_price or 0.0),
            take_profit_price=float(tp_price or 0.0),
            bearish_counter_thesis=bearish_counter_thesis,
            bullish_conviction_factors=bullish_conviction_factors,
            now_ts_ms=int(dt.timestamp() * 1000),
        )

    def fetch_recent_shadow_evaluations(self, limit: int = 30) -> List[Dict[str, Any]]:
        """直近 N 件のシャドー評価レコードを取得する（閉ループ自己較正スコアカード用）"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM llm_shadow_evaluations ORDER BY created_ts_ms DESC, id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_shadow_evaluation_metrics(
        self,
        eval_id: str,
        mfe_4h_pct: Optional[float] = None,
        mae_4h_pct: Optional[float] = None,
        mfe_12h_pct: Optional[float] = None,
        mae_12h_pct: Optional[float] = None,
        mfe_24h_pct: Optional[float] = None,
        mae_24h_pct: Optional[float] = None,
        virtual_pnl_24h_jpy: Optional[float] = None,
        outcome_24h: str = "HOLD_EXPIRY",
    ) -> None:
        self.update_llm_shadow_outcome(
            eval_id=eval_id,
            mfe_4h_pct=float(mfe_4h_pct or 0.0),
            mae_4h_pct=float(mae_4h_pct or 0.0),
            pnl_4h_pct=0.0,
            mfe_12h_pct=float(mfe_12h_pct or 0.0),
            mae_12h_pct=float(mae_12h_pct or 0.0),
            pnl_12h_pct=0.0,
            mfe_24h_pct=float(mfe_24h_pct or 0.0),
            mae_24h_pct=float(mae_24h_pct or 0.0),
            pnl_24h_pct=float(virtual_pnl_24h_jpy or 0.0),
            stop_hit_first=(str(outcome_24h).upper() == "SL_HIT"),
            outcome_class=outcome_24h,
        )

    def save_sprt_cusum_state(self, state: Dict[str, Any]) -> None:
        """Wald's SPRT ＆ Page's Lower CUSUM（銘柄別CUSUM・動的ブラックリスト含む）の状態を account_meta へ永続化する"""
        self.set_meta("sprt_cusum_state", state)

    def load_sprt_cusum_state(self) -> Dict[str, Any]:
        """永続化された Wald's SPRT ＆ Page's Lower CUSUM の状態を読み込む"""
        val = self.get_meta("sprt_cusum_state", {})
        return dict(val) if isinstance(val, dict) else {}
