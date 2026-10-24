r"""
SWMR 共有リスクバス (`risk_bus.db`) ＆ 全ボット連動3段階グローバル・サーキットブレーカー (global_risk_bus.py)

【設計仕様】
1. Single-Writer / Multi-Reader (SWMR) トポロジー:
   - `RiskBusSentinelWriter`（Single-Writer）：
     * `PRAGMA locking_mode = NORMAL;`（`EXCLUSIVE` は絶対使用禁止）
     * `PRAGMA journal_mode = WAL;`
     * `PRAGMA synchronous = NORMAL;`
     * `PRAGMA busy_timeout = 5000;`
     * `PRAGMA wal_autocheckpoint = 1000;`
     * `PRAGMA mmap_size = 268435456;`
     * `PRAGMA cache_size = -64000;`
     * `PRAGMA temp_store = MEMORY;`
     * 書き込みトランザクションは必ず `BEGIN IMMEDIATE; ... COMMIT;` で開始し、
       共有ロックからの昇格デッドロック (`SQLITE_BUSY`) を根絶する。
   - `RiskBusBotReader`（Multi-Reader）：
     * SQLite URI `file:{db_path}?mode=ro`（`immutable=1` は付与しない）で接続。
     * `read_breaker_state()` で1行フェッチ後ただちに `cursor.close()` を行い、
       古いスナップショット保持による WAL チェックポイント阻害を防止する。

2. 3段階グローバル・サーキットブレーカー (`evaluate_cross_venue_breaker_level`):
   - Level `0 (NORMAL)`：全指標正常
   - Level `1 (THROTTLE)`：$Z_{\text{BTC}} < -2.5$ または $\bar{d} < 0.35$ または日次DD $> 1.5\%$
   - Level `2 (HALT_HEDGE)`：$Z_{\text{BTC}} < -4.0$ または日次DD $> 3.0\%$
   - 2028年以降の税務区分整合型ヘッジ比率：
     * 申告分離課税（20.315%）の GMOコイン現物と、総合課税（最大55.945%）の Hyperliquid Perp / QANX DEX は
       損益通算が禁止されるため、2028年以降は GMO現物を GMO内で優先決済 (`gmo_direct_unwind_priority=True`) し、
       総合課税バケット (`QANX_DEX` / `HYPERLIQUID`) のダウンサイド・ベータ相当額のみを
       `hedge_short_target_usd` へ計上する。
"""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

BREAKER_LEVEL_NORMAL: int = 0
BREAKER_LEVEL_THROTTLE: int = 1
BREAKER_LEVEL_HALT_HEDGE: int = 2

BREAKER_LEVEL_NAMES: Dict[int, str] = {
    BREAKER_LEVEL_NORMAL: "NORMAL",
    BREAKER_LEVEL_THROTTLE: "THROTTLE",
    BREAKER_LEVEL_HALT_HEDGE: "HALT_HEDGE",
}


def compute_mantegna_avg_corr_distance(
    corr_matrix: Optional[Union[Sequence[Sequence[float]], Dict[Tuple[str, str], float]]] = None,
) -> float:
    r"""
    相関係数行列 $\rho_{i,j}$ から Mantegna 相関距離 $d_{i,j} = \sqrt{0.5(1 - \rho_{i,j})}$ の
    非対角平均 $\bar{d}$ を算出する。
    全銘柄が一斉下落して $\rho_{i,j} \to 1$ に収束すると $\bar{d} \to 0$ となる。
    """
    if corr_matrix is None:
        return 1.0

    distances: List[float] = []
    if isinstance(corr_matrix, dict):
        seen_pairs = set()
        for k, rho in corr_matrix.items():
            if not isinstance(k, tuple) or len(k) != 2:
                continue
            a, b = str(k[0]).upper(), str(k[1]).upper()
            if a == b:
                continue
            pair_key = tuple(sorted((a, b)))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            rho_clamped = max(-1.0, min(1.0, float(rho)))
            distances.append(math.sqrt(max(0.0, 0.5 * (1.0 - rho_clamped))))
    else:
        n = len(corr_matrix)
        for i in range(n):
            row = corr_matrix[i]
            for j in range(i + 1, min(n, len(row))):
                rho_clamped = max(-1.0, min(1.0, float(row[j])))
                distances.append(math.sqrt(max(0.0, 0.5 * (1.0 - rho_clamped))))

    if not distances:
        return 1.0
    return float(sum(distances) / len(distances))


def evaluate_cross_venue_breaker_level(
    *,
    btc_z_score: Optional[float] = None,
    btc_return_5m: Optional[float] = None,
    btc_ewma_vol_5m: Optional[float] = None,
    avg_corr_distance: Optional[float] = None,
    corr_matrix: Optional[Union[Sequence[Sequence[float]], Dict[Tuple[str, str], float]]] = None,
    daily_drawdown_pct: float = 0.0,
    gmo_spot_notional_jpy: float = 0.0,
    comprehensive_alt_notional_jpy: float = 0.0,
    positions_notional_by_venue: Optional[Dict[str, float]] = None,
    downside_betas: Optional[Dict[str, float]] = None,
    default_downside_beta: float = 1.0,
    fx_usdjpy: float = 150.0,
    tax_year: Optional[int] = None,
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    r"""
    BTCショックZスコア $Z_{\text{BTC}}$、平均相関距離 $\bar{d}$、日次ドローダウン率から
    3段階グローバル・サーキットブレーカー (`0: NORMAL`, `1: THROTTLE`, `2: HALT_HEDGE`) を判定し、
    2027年以前／2028年以降の税制区分に応じた最適ヘッジショート目標額を算出する。
    """
    if btc_z_score is not None:
        z_btc = float(btc_z_score)
    elif btc_return_5m is not None and btc_ewma_vol_5m is not None and float(btc_ewma_vol_5m) > 0.0:
        z_btc = float(btc_return_5m) / float(btc_ewma_vol_5m)
    else:
        z_btc = 0.0

    if avg_corr_distance is not None:
        d_bar = float(avg_corr_distance)
    elif corr_matrix is not None:
        d_bar = compute_mantegna_avg_corr_distance(corr_matrix)
    else:
        d_bar = 1.0

    dd_pct = abs(float(daily_drawdown_pct or 0.0))

    if tax_year is not None:
        eff_tax_year = int(tax_year)
    elif now_dt is not None:
        dt_jst = now_dt.replace(tzinfo=JST) if now_dt.tzinfo is None else now_dt.astimezone(JST)
        eff_tax_year = int(dt_jst.year)
    else:
        eff_tax_year = int(datetime.now(JST).year)

    betas = {str(k).upper(): float(v) for k, v in (downside_betas or {}).items()}
    beta_def = float(default_downside_beta)

    gmo_notional = float(gmo_spot_notional_jpy or 0.0)
    comp_notional = float(comprehensive_alt_notional_jpy or 0.0)
    if positions_notional_by_venue:
        for venue_raw, val_jpy in positions_notional_by_venue.items():
            v_up = str(venue_raw).strip().upper()
            v_val = max(0.0, float(val_jpy or 0.0))
            if v_up in ("GMO_COIN", "GMO", "JP_FSA_REGISTERED_GMO"):
                gmo_notional += v_val
            else:
                comp_notional += v_val

    gmo_beta = betas.get("GMO_COIN", betas.get("GMO", beta_def))
    comp_beta = betas.get("COMPREHENSIVE_MISC", betas.get("QANX_DEX", beta_def))

    gmo_beta_delta_jpy = gmo_notional * gmo_beta
    comp_beta_delta_jpy = comp_notional * comp_beta
    portfolio_beta_delta_jpy = gmo_beta_delta_jpy + comp_beta_delta_jpy

    reasons: List[str] = []
    if z_btc < -4.0:
        reasons.append(f"BTC_SHOCK_EXTREME(Z={z_btc:.2f}<-4.0)")
    if dd_pct > 3.0:
        reasons.append(f"DAILY_DD_EXTREME({dd_pct:.2f}%>3.0%)")

    if reasons:
        breaker_level = BREAKER_LEVEL_HALT_HEDGE
    else:
        if z_btc < -2.5:
            reasons.append(f"BTC_SHOCK_MODERATE(Z={z_btc:.2f}<-2.5)")
        if d_bar < 0.35:
            reasons.append(f"CORR_CONVERGENCE(d_bar={d_bar:.3f}<0.35)")
        if dd_pct > 1.5:
            reasons.append(f"DAILY_DD_WARNING({dd_pct:.2f}%>1.5%)")

        if reasons:
            breaker_level = BREAKER_LEVEL_THROTTLE
        else:
            breaker_level = BREAKER_LEVEL_NORMAL
            reasons.append("ALL_CLEAR")

    fx_rate = max(1.0, float(fx_usdjpy or 150.0))
    if breaker_level == BREAKER_LEVEL_HALT_HEDGE:
        if eff_tax_year >= 2028:
            # 2028年以降: 分離課税(GMO)と総合課税(Hyperliquid/QANX)は損益通算不可のため、
            # GMO現物はGMO内で直接決済優先とし、総合課税エクスポージャーのみをPerpショートヘッジ対象とする
            gmo_direct_unwind_priority = True
            gmo_direct_unwind_target_jpy = gmo_beta_delta_jpy
            hedge_short_target_jpy = comp_beta_delta_jpy
        else:
            # 2027年以前: 全口座総合課税（内部通算可）のためポートフォリオ全体のベータデルタをヘッジ可能
            gmo_direct_unwind_priority = False
            gmo_direct_unwind_target_jpy = 0.0
            hedge_short_target_jpy = portfolio_beta_delta_jpy
        hedge_short_target_usd = round(hedge_short_target_jpy / fx_rate, 2)
    else:
        gmo_direct_unwind_priority = bool(eff_tax_year >= 2028 and gmo_notional > 0)
        gmo_direct_unwind_target_jpy = 0.0
        hedge_short_target_jpy = 0.0
        hedge_short_target_usd = 0.0

    reason_code = "|".join(reasons)
    return {
        "breaker_level": breaker_level,
        "breaker_name": BREAKER_LEVEL_NAMES[breaker_level],
        "btc_z_score": round(z_btc, 4),
        "avg_corr_distance": round(d_bar, 4),
        "daily_drawdown_pct": round(dd_pct, 4),
        "portfolio_beta_delta_jpy": round(portfolio_beta_delta_jpy, 2),
        "gmo_beta_delta_jpy": round(gmo_beta_delta_jpy, 2),
        "comprehensive_beta_delta_jpy": round(comp_beta_delta_jpy, 2),
        "hedge_short_target_jpy": round(hedge_short_target_jpy, 2),
        "hedge_short_target_usd": hedge_short_target_usd,
        "gmo_direct_unwind_priority": gmo_direct_unwind_priority,
        "gmo_direct_unwind_target_jpy": round(gmo_direct_unwind_target_jpy, 2),
        "tax_year": eff_tax_year,
        "reason_code": reason_code,
    }


class RiskBusSentinelWriter:
    """
    `risk_bus.db` の Single-Writer クラス（Global Risk Sentinel 用）。
    - `PRAGMA locking_mode = NORMAL;`（`EXCLUSIVE` は絶対使用禁止）
    - `PRAGMA journal_mode = WAL;`
    - すべての更新は `BEGIN IMMEDIATE; ... COMMIT;` で即時予約ロックを取得し、`SQLITE_BUSY` デッドロックを防止する。
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(
            str(self.db_path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self._init_pragmas_and_schema()

    def _init_pragmas_and_schema(self) -> None:
        with self._lock:
            self.conn.execute("PRAGMA locking_mode = NORMAL;")
            self.conn.execute("PRAGMA journal_mode = WAL;")
            self.conn.execute("PRAGMA synchronous = NORMAL;")
            self.conn.execute("PRAGMA busy_timeout = 5000;")
            self.conn.execute("PRAGMA wal_autocheckpoint = 1000;")
            self.conn.execute("PRAGMA mmap_size = 268435456;")
            self.conn.execute("PRAGMA cache_size = -64000;")
            self.conn.execute("PRAGMA temp_store = MEMORY;")

            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS global_risk_state (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        breaker_level INTEGER NOT NULL DEFAULT 0,
                        btc_z_score REAL NOT NULL DEFAULT 0.0,
                        avg_corr_distance REAL NOT NULL DEFAULT 1.0,
                        portfolio_beta_delta_jpy REAL NOT NULL DEFAULT 0.0,
                        hedge_short_target_usd REAL NOT NULL DEFAULT 0.0,
                        reason_code TEXT NOT NULL DEFAULT 'INIT',
                        updated_epoch_ms INTEGER NOT NULL DEFAULT 0
                    );
                    """
                )
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO global_risk_state
                    (id, breaker_level, btc_z_score, avg_corr_distance,
                     portfolio_beta_delta_jpy, hedge_short_target_usd, reason_code, updated_epoch_ms)
                    VALUES (1, 0, 0.0, 1.0, 0.0, 0.0, 'INIT', ?);
                    """,
                    (int(time.time() * 1000),),
                )
                self.conn.execute("COMMIT;")
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

    def verify_pragmas(self) -> Dict[str, Any]:
        """設定されている SQLite PRAGMA 値を辞書で返す（監査検証用）"""
        with self._lock:
            locking_mode = str(self.conn.execute("PRAGMA locking_mode;").fetchone()[0]).lower()
            journal_mode = str(self.conn.execute("PRAGMA journal_mode;").fetchone()[0]).lower()
            synchronous = int(self.conn.execute("PRAGMA synchronous;").fetchone()[0])
            busy_timeout = int(self.conn.execute("PRAGMA busy_timeout;").fetchone()[0])
            wal_autocheckpoint = int(self.conn.execute("PRAGMA wal_autocheckpoint;").fetchone()[0])
            mmap_size = int(self.conn.execute("PRAGMA mmap_size;").fetchone()[0])
            cache_size = int(self.conn.execute("PRAGMA cache_size;").fetchone()[0])
            temp_store = int(self.conn.execute("PRAGMA temp_store;").fetchone()[0])
            return {
                "locking_mode": locking_mode,
                "journal_mode": journal_mode,
                "synchronous": synchronous,
                "busy_timeout": busy_timeout,
                "wal_autocheckpoint": wal_autocheckpoint,
                "mmap_size": mmap_size,
                "cache_size": cache_size,
                "temp_store": temp_store,
            }

    def publish_state(
        self,
        breaker_level: int,
        btc_z_score: float,
        avg_corr_distance: float,
        portfolio_beta_delta_jpy: float,
        hedge_short_target_usd: float,
        reason_code: str,
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """`BEGIN IMMEDIATE; ... COMMIT;` で `global_risk_state` をアトミック更新する"""
        now_ms = int(now_epoch_ms if now_epoch_ms is not None else time.time() * 1000)
        lvl = max(0, min(2, int(breaker_level)))
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                self.conn.execute(
                    """
                    UPDATE global_risk_state
                    SET breaker_level = ?,
                        btc_z_score = ?,
                        avg_corr_distance = ?,
                        portfolio_beta_delta_jpy = ?,
                        hedge_short_target_usd = ?,
                        reason_code = ?,
                        updated_epoch_ms = ?
                    WHERE id = 1;
                    """,
                    (
                        lvl,
                        float(btc_z_score),
                        float(avg_corr_distance),
                        float(portfolio_beta_delta_jpy),
                        float(hedge_short_target_usd),
                        str(reason_code or "NORMAL"),
                        now_ms,
                    ),
                )
                self.conn.execute("COMMIT;")
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

        return {
            "breaker_level": lvl,
            "breaker_name": BREAKER_LEVEL_NAMES.get(lvl, "NORMAL"),
            "btc_z_score": float(btc_z_score),
            "avg_corr_distance": float(avg_corr_distance),
            "portfolio_beta_delta_jpy": float(portfolio_beta_delta_jpy),
            "hedge_short_target_usd": float(hedge_short_target_usd),
            "reason_code": str(reason_code or "NORMAL"),
            "updated_epoch_ms": now_ms,
        }

    def evaluate_and_publish(self, **kwargs: Any) -> Dict[str, Any]:
        """`evaluate_cross_venue_breaker_level` で判定した結果をそのまま `risk_bus.db` へ書き込む"""
        eval_res = evaluate_cross_venue_breaker_level(**kwargs)
        pub_res = self.publish_state(
            breaker_level=eval_res["breaker_level"],
            btc_z_score=eval_res["btc_z_score"],
            avg_corr_distance=eval_res["avg_corr_distance"],
            portfolio_beta_delta_jpy=eval_res["portfolio_beta_delta_jpy"],
            hedge_short_target_usd=eval_res["hedge_short_target_usd"],
            reason_code=eval_res["reason_code"],
        )
        merged = dict(eval_res)
        merged["updated_epoch_ms"] = pub_res["updated_epoch_ms"]
        return merged

    def close(self, truncate_wal: bool = False) -> None:
        with self._lock:
            try:
                mode = "TRUNCATE" if truncate_wal else "PASSIVE"
                self.conn.execute(f"PRAGMA wal_checkpoint({mode});")
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass


class RiskBusBotReader:
    """
    各取引ボット（`auto_trader.py` 等）から `risk_bus.db` を非ブロッキング・読み取り専用 (`?mode=ro`) で参照する
    Multi-Reader クラス。
    ※ ライブ WAL 更新を認識できるよう `immutable=1` は付与せず、フェッチ直後にカーソルを閉じる。
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path).resolve()
        posix_path = self.db_path.as_posix()
        self.uri = f"file:{posix_path}?mode=ro"
        self.conn = sqlite3.connect(
            self.uri,
            uri=True,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only = ON;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.conn.execute("PRAGMA mmap_size = 268435456;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")

    def read_breaker_state(self) -> Dict[str, Any]:
        """
        最新のグローバルブレーカー状態を1行フェッチし、即座に `cur.close()` して
        WAL Read Snapshot を解放する。
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                SELECT breaker_level, btc_z_score, avg_corr_distance,
                       portfolio_beta_delta_jpy, hedge_short_target_usd,
                       reason_code, updated_epoch_ms
                FROM global_risk_state
                WHERE id = 1;
                """
            )
            row = cur.fetchone()
        finally:
            cur.close()

        if row is None:
            return {
                "breaker_level": BREAKER_LEVEL_NORMAL,
                "breaker_name": "NORMAL",
                "btc_z_score": 0.0,
                "avg_corr_distance": 1.0,
                "portfolio_beta_delta_jpy": 0.0,
                "hedge_short_target_usd": 0.0,
                "reason_code": "EMPTY",
                "updated_epoch_ms": 0,
            }

        lvl = int(row["breaker_level"])
        return {
            "breaker_level": lvl,
            "breaker_name": BREAKER_LEVEL_NAMES.get(lvl, "NORMAL"),
            "btc_z_score": float(row["btc_z_score"]),
            "avg_corr_distance": float(row["avg_corr_distance"]),
            "portfolio_beta_delta_jpy": float(row["portfolio_beta_delta_jpy"]),
            "hedge_short_target_usd": float(row["hedge_short_target_usd"]),
            "reason_code": str(row["reason_code"]),
            "updated_epoch_ms": int(row["updated_epoch_ms"]),
        }

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
