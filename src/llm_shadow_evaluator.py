"""
LLM シャドー評価エンジン：Veto 付加価値 (Counterfactual Edge) ＆ 4h/12h/24h MFE/MAE パス依存評価 (llm_shadow_evaluator.py)

1. LLMShadowEvaluator:
   - TechnicalRadar が抽出した候補銘柄に対し、AIJudge が `BUY`（承認）または `HOLD`（Veto：見送り）と判定した際、
     判定時点の基準価格 (`entry_price`)・仮想 TP (`tp_price`)・仮想 SL (`sl_price`) を SQLite WAL (`llm_shadow_evaluations` テーブル)
     およびメモリ台帳に記録する。
   - 4h / 12h / 24h 経過後のローソク足（High/Low/Close パス）または時系列価格パスから、
     * MFE (Maximum Favorable Excursion: 最大順行幅 %)
     * MAE (Maximum Adverse Excursion: 最大逆行幅 %)
     * パス依存仮想損益（SL先行タッチなら `-1.0R` 損失確定、TP先行タッチなら `+RR` 利益確定、どちらも未到達なら期間末終値評価）
     を自動算出・集計し、LLM Veto が損失トレードをどれだけ回避したか（Counterfactual Edge）を定量評価する。
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.database_wal import DatabaseWAL

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Deep Research #2 【追加実装3】: Shadowデータの未約定バイアス割引重み (w_shadow = 0.25)
SHADOW_DISCOUNT_WEIGHT = 0.25


def evaluate_mintrl_freeze_stage(live_trade_count: int) -> Dict[str, Any]:
    """
    Deep Research #2 【追加実装3】: MinTRL 100件凍結ゲート
    - 実約定件数 < 100件  : `STAGE0_HARD_FREEZE` (全ハイパーパラメータ・閾値最適化を完全凍結)
    - 100〜249件          : `STAGE1_BOUNDARY_AUDIT` (境界監査のみ許可・コア構造固定)
    - >= 250件            : `STAGE2_MINTRL_UNLOCKED` (統計的有意サンプル到達・フル更新解禁)
    """
    n_live = max(0, int(live_trade_count or 0))
    if n_live < 100:
        stage = "STAGE0_HARD_FREEZE"
        allow_param_tuning = False
        allow_boundary_audit = False
    elif n_live < 250:
        stage = "STAGE1_BOUNDARY_AUDIT"
        allow_param_tuning = False
        allow_boundary_audit = True
    else:
        stage = "STAGE2_MINTRL_UNLOCKED"
        allow_param_tuning = True
        allow_boundary_audit = True

    return {
        "live_trade_count": n_live,
        "mintrl_stage": stage,
        "parameters_frozen": not allow_param_tuning,
        "allow_parameter_tuning": allow_param_tuning,
        "allow_boundary_audit": allow_boundary_audit,
        "remaining_to_stage1": max(0, 100 - n_live),
        "remaining_to_stage2": max(0, 250 - n_live),
    }


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


class LLMShadowEvaluator:
    """
    AI の BUY 判定および HOLD (Veto) 判定の事後パフォーマンス（4h / 12h / 24h MFE, MAE, パス依存PnL）を評価するエンジン
    """

    MAX_IN_MEMORY_RECORDS = 1000

    def __init__(self, db_wal: Optional[DatabaseWAL] = None) -> None:
        self.db_wal = db_wal
        self.records: Dict[str, Dict[str, Any]] = {}

    def record_decision(
        self,
        symbol: str,
        ai_action: str,
        conviction_score: int,
        entry_price: float,
        tp_price: float = 0.0,
        sl_price: float = 0.0,
        radar_score: float = 0.0,
        bearish_counter_thesis: Optional[List[str]] = None,
        bullish_conviction_factors: Optional[List[str]] = None,
        evaluated_at: Optional[datetime] = None,
    ) -> str:
        """AI判定（BUY または Veto HOLD）をシャドー評価対象として登録する"""
        now_jst = _now_jst(evaluated_at)
        eval_id = f"shd_{now_jst.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        sym_up = str(symbol or "NONE").strip().upper()
        act_up = str(ai_action or "HOLD").strip().upper()

        if sl_price <= 0 and entry_price > 0:
            sl_price = round(entry_price * 0.965, 4)
        if tp_price <= 0 and entry_price > 0:
            tp_price = round(entry_price * 1.060, 4)

        ct_str = " / ".join(str(x) for x in (bearish_counter_thesis or []) if str(x).strip())
        bull_str = " / ".join(str(x) for x in (bullish_conviction_factors or []) if str(x).strip())

        rec = {
            "eval_id": eval_id,
            "symbol": sym_up,
            "evaluated_at": now_jst.isoformat(timespec="seconds"),
            "radar_score": float(radar_score),
            "ai_action": act_up,
            "conviction_score": int(conviction_score),
            "entry_price": float(entry_price),
            "tp_price": float(tp_price),
            "sl_price": float(sl_price),
            "bearish_counter_thesis": ct_str,
            "bullish_conviction_factors": bull_str,
            "mfe_4h_pct": None,
            "mae_4h_pct": None,
            "mfe_12h_pct": None,
            "mae_12h_pct": None,
            "mfe_24h_pct": None,
            "mae_24h_pct": None,
            "virtual_pnl_24h_jpy": None,
            "outcome_24h": "PENDING",
        }
        self.records[eval_id] = rec
        if len(self.records) > self.MAX_IN_MEMORY_RECORDS:
            completed_keys = [k for k, v in self.records.items() if v.get("outcome_24h") != "PENDING"]
            evict_key = completed_keys[0] if completed_keys else next(iter(self.records))
            self.records.pop(evict_key, None)
        if self.db_wal is not None:
            self.db_wal.record_shadow_evaluation(
                eval_id=eval_id,
                symbol=sym_up,
                ai_action=act_up,
                conviction_score=int(conviction_score),
                entry_price=str(entry_price),
                tp_price=str(tp_price),
                sl_price=str(sl_price),
                radar_score=float(radar_score),
                bearish_counter_thesis=ct_str,
                bullish_conviction_factors=bull_str,
                evaluated_at=now_jst,
            )
        return eval_id

    @staticmethod
    def evaluate_price_path(
        entry_price: float,
        tp_price: float,
        sl_price: float,
        bars: List[Dict[str, float]],
        notional_jpy: float = 5000.0,
    ) -> Dict[str, Any]:
        """
        時系列バー (`[{"high": ..., "low": ..., "close": ...}, ...]`) を走査し、
        MFE (%), MAE (%), およびパス依存の勝敗・仮想損益（同一バー内で Low <= SL かつ High >= TP の場合は
        保守的に SL 先行ヒットとみなす）を算出する。
        """
        if entry_price <= 0 or not bars:
            return {
                "mfe_pct": 0.0,
                "mae_pct": 0.0,
                "outcome": "NO_DATA",
                "exit_price": entry_price,
                "virtual_pnl_jpy": 0.0,
            }

        max_high = entry_price
        min_low = entry_price
        outcome = "HOLD_EXPIRY"
        exit_price = float(bars[-1].get("close", entry_price))

        for bar in bars:
            b_high = float(bar.get("high", bar.get("close", entry_price)))
            b_low = float(bar.get("low", bar.get("close", entry_price)))
            if b_high > max_high:
                max_high = b_high
            if b_low < min_low:
                min_low = b_low

            if outcome == "HOLD_EXPIRY":
                # 保守的判定: 同一足で SL と TP の両方に達した場合は SL 先行とみなす
                if sl_price > 0 and b_low <= sl_price:
                    outcome = "SL_HIT"
                    exit_price = sl_price
                elif tp_price > 0 and b_high >= tp_price:
                    outcome = "TP_HIT"
                    exit_price = tp_price

        mfe_pct = round((max_high - entry_price) / entry_price * 100.0, 4)
        mae_pct = round((min_low - entry_price) / entry_price * 100.0, 4)
        size = notional_jpy / entry_price
        virtual_pnl_jpy = round((exit_price - entry_price) * size, 2)

        return {
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "outcome": outcome,
            "exit_price": exit_price,
            "virtual_pnl_jpy": virtual_pnl_jpy,
        }

    def update_with_horizon_bars(
        self,
        eval_id: str,
        bars_4h: Optional[List[Dict[str, float]]] = None,
        bars_12h: Optional[List[Dict[str, float]]] = None,
        bars_24h: Optional[List[Dict[str, float]]] = None,
    ) -> Dict[str, Any]:
        """4h / 12h / 24h のローソク足リストから MFE/MAE と 24h 仮想損益を更新する"""
        rec = self.records.get(eval_id)
        if rec is None:
            raise KeyError(f"Shadow evaluation record not found: {eval_id}")

        entry_p = float(rec["entry_price"])
        tp_p = float(rec["tp_price"])
        sl_p = float(rec["sl_price"])

        res_4h = self.evaluate_price_path(entry_p, tp_p, sl_p, bars_4h or [])
        res_12h = self.evaluate_price_path(entry_p, tp_p, sl_p, bars_12h or bars_4h or [])
        res_24h = self.evaluate_price_path(entry_p, tp_p, sl_p, bars_24h or bars_12h or bars_4h or [])

        rec["mfe_4h_pct"] = res_4h["mfe_pct"]
        rec["mae_4h_pct"] = res_4h["mae_pct"]
        rec["mfe_12h_pct"] = res_12h["mfe_pct"]
        rec["mae_12h_pct"] = res_12h["mae_pct"]
        rec["mfe_24h_pct"] = res_24h["mfe_pct"]
        rec["mae_24h_pct"] = res_24h["mae_pct"]
        rec["virtual_pnl_24h_jpy"] = res_24h["virtual_pnl_jpy"]
        rec["outcome_24h"] = res_24h["outcome"]

        if self.db_wal is not None:
            self.db_wal.update_shadow_evaluation_metrics(
                eval_id=eval_id,
                mfe_4h_pct=rec["mfe_4h_pct"],
                mae_4h_pct=rec["mae_4h_pct"],
                mfe_12h_pct=rec["mfe_12h_pct"],
                mae_12h_pct=rec["mae_12h_pct"],
                mfe_24h_pct=rec["mfe_24h_pct"],
                mae_24h_pct=rec["mae_24h_pct"],
                virtual_pnl_24h_jpy=rec["virtual_pnl_24h_jpy"],
                outcome_24h=rec["outcome_24h"],
            )
        return rec

    def summarize_counterfactual_edge(self) -> Dict[str, Any]:
        """
        BUY承認群と HOLD (Veto) 見送り群の仮想損益を比較し、
        LLM Veto の付加価値 (Counterfactual Edge = Mean(BUY PnL) - Mean(Vetoed HOLD PnL)) を集計する。
        """
        buy_pnls: List[float] = []
        veto_pnls: List[float] = []
        veto_avoided_sl_count = 0

        for rec in self.records.values():
            pnl = rec.get("virtual_pnl_24h_jpy")
            if pnl is None:
                continue
            act = str(rec.get("ai_action", "HOLD")).upper()
            if act == "BUY":
                buy_pnls.append(float(pnl))
            elif act == "HOLD":
                veto_pnls.append(float(pnl))
                if rec.get("outcome_24h") == "SL_HIT" or float(pnl) < 0:
                    veto_avoided_sl_count += 1

        avg_buy_pnl = sum(buy_pnls) / len(buy_pnls) if buy_pnls else 0.0
        avg_veto_pnl = sum(veto_pnls) / len(veto_pnls) if veto_pnls else 0.0
        counterfactual_edge_jpy = round(avg_buy_pnl - avg_veto_pnl, 2)

        return {
            "buy_count": len(buy_pnls),
            "veto_count": len(veto_pnls),
            "avg_buy_pnl_24h_jpy": round(avg_buy_pnl, 2),
            "avg_veto_pnl_24h_jpy": round(avg_veto_pnl, 2),
            "counterfactual_edge_jpy": counterfactual_edge_jpy,
            "veto_avoided_losses_count": veto_avoided_sl_count,
        }

    def evaluate_pending_records(
        self,
        market_summaries: Dict[str, Dict[str, Any]],
        now_dt: Optional[datetime] = None,
    ) -> int:
        """
        デーモンの定期サイクルから呼び出され、未評価レコードの4h/12h/24h MFE/MAE・仮想損益を
        直近の1時間足 (または15分足) バーから自動更新する。
        """
        updated_count = 0
        for eval_id, rec in list(self.records.items()):
            if rec.get("outcome_24h") not in (None, "PENDING"):
                continue
            sym = str(rec.get("symbol", "")).upper()
            summary = (market_summaries or {}).get(sym, {})
            klines = summary.get("klines_1h") or summary.get("klines_15m") or []
            if not klines:
                continue
            bars_24h = [
                {
                    "high": float(k.get("high", k.get("close", 0)) or 0),
                    "low": float(k.get("low", k.get("close", 0)) or 0),
                    "close": float(k.get("close", 0) or 0),
                }
                for k in klines[-24:]
                if float(k.get("close", 0) or 0) > 0
            ]
            if not bars_24h:
                continue
            bars_4h = bars_24h[:4]
            bars_12h = bars_24h[:12]
            self.update_with_horizon_bars(
                eval_id=eval_id,
                bars_4h=bars_4h,
                bars_12h=bars_12h,
                bars_24h=bars_24h,
            )
            updated_count += 1
        return updated_count

    def _collect_recent_records_for_calibration(self, limit: int = 30) -> List[Dict[str, Any]]:
        """メモリ台帳および SQLite WAL から直近 N 件のレコードを取得し正規化する"""
        merged: Dict[str, Dict[str, Any]] = {}
        if self.db_wal is not None and hasattr(self.db_wal, "fetch_recent_shadow_evaluations"):
            try:
                db_rows = self.db_wal.fetch_recent_shadow_evaluations(limit=limit)
                for r in db_rows:
                    eid = str(r.get("eval_id", ""))
                    if not eid:
                        continue
                    merged[eid] = {
                        "eval_id": eid,
                        "symbol": str(r.get("symbol", "")).upper(),
                        "ai_action": str(r.get("decision") or r.get("ai_action") or "HOLD").upper(),
                        "conviction_score": int(r.get("conviction_score", 0) or 0),
                        "entry_price": float(r.get("entry_price", 0.0) or 0.0),
                        "tp_price": float(r.get("take_profit_price") or r.get("tp_price") or 0.0),
                        "sl_price": float(r.get("stop_loss_price") or r.get("sl_price") or 0.0),
                        "mfe_24h_pct": r.get("mfe_24h_pct"),
                        "mae_24h_pct": r.get("mae_24h_pct"),
                        "virtual_pnl_24h_jpy": r.get("pnl_24h_pct") if r.get("virtual_pnl_24h_jpy") is None else r.get("virtual_pnl_24h_jpy"),
                        "outcome_24h": str(r.get("outcome_class") or r.get("outcome_24h") or "PENDING").upper(),
                        "bearish_counter_thesis": str(r.get("bearish_counter_thesis", "")),
                        "bullish_conviction_factors": str(r.get("bullish_conviction_factors", "")),
                    }
            except Exception:
                pass

        for eid, rec in self.records.items():
            merged[eid] = dict(rec)

        all_recs = list(merged.values())
        return all_recs[-max(1, int(limit)) :]

    def compute_self_calibration_scorecard(
        self,
        limit: int = 30,
        alpha_0: float = 2.0,
        beta_0: float = 2.0,
        k_weight: float = 5.0,
        live_trade_count: int = 0,
        live_wins: int = 0,
        live_losses: int = 0,
        w_shadow: float = SHADOW_DISCOUNT_WEIGHT,
    ) -> Dict[str, Any]:
        """
        直近20〜30件の `llm_shadow_evaluations` レコードから閉ループ自己較正スコアカードを集計する。
        - ① FPR（過信率）：`BUY` 判定後に SL 被弾した割合と直近の失敗要約
        - ② FNR（機会損失率）：`HOLD`（Veto）判定後に `+2.0R` 以上上昇（または TP 到達）した割合と逃したトレード要約
        - ③ TNR（Veto正解率）：`HOLD` 判定により下落・SL 被弾を回避した割合
        - ④ Shadowデータ重み割引 (`w_shadow = 0.25`) と MinTRL 100件凍結ステージ (`STAGE0_HARD_FREEZE` / `STAGE1_BOUNDARY_AUDIT` / `STAGE2_MINTRL_UNLOCKED`)
        - ベータ・二項ベイズ較正パラメータと動的閾値 `effective_threshold = max(0.72, Quantile_0.75)`
        """
        recent_recs = self._collect_recent_records_for_calibration(limit=limit)
        buy_total = 0
        buy_wins = 0
        buy_sl_hits = 0
        hold_total = 0
        hold_missed_rallies = 0
        hold_avoided_drops = 0

        recent_failed_buys: List[str] = []
        recent_missed_holds: List[str] = []
        raw_confidences: List[float] = []

        for rec in recent_recs:
            conv = int(rec.get("conviction_score", 0) or 0)
            p_raw = max(0.0, min(1.0, conv / 100.0))
            if conv > 0:
                raw_confidences.append(p_raw)

            outcome = str(rec.get("outcome_24h", "PENDING") or "PENDING").upper()
            pnl = rec.get("virtual_pnl_24h_jpy")
            if outcome in ("PENDING", "NO_DATA") and pnl is None:
                continue

            act = str(rec.get("ai_action", "HOLD")).upper()
            sym = str(rec.get("symbol", "UNKNOWN")).upper()
            entry_p = float(rec.get("entry_price", 0.0) or 0.0)
            sl_p = float(rec.get("sl_price", 0.0) or 0.0)
            mfe_24 = float(rec.get("mfe_24h_pct", 0.0) or 0.0)
            mae_24 = float(rec.get("mae_24h_pct", 0.0) or 0.0)
            pnl_val = float(pnl or 0.0)
            risk_pct = (
                abs(entry_p - sl_p) / entry_p * 100.0
                if (entry_p > 0 and 0 < sl_p < entry_p)
                else 1.5
            )
            two_r_pct = max(2.0, risk_pct * 2.0)

            if act == "BUY":
                buy_total += 1
                if outcome == "SL_HIT" or pnl_val < 0.0:
                    buy_sl_hits += 1
                    recent_failed_buys.append(
                        f"{sym} BUY失敗(score={conv}, outcome={outcome}, MAE={mae_24:+.2f}%, PnL={pnl_val:+.0f}円)"
                    )
                elif outcome == "TP_HIT" or pnl_val > 0.0:
                    buy_wins += 1
            elif act == "HOLD":
                hold_total += 1
                if outcome == "TP_HIT" or mfe_24 >= two_r_pct:
                    hold_missed_rallies += 1
                    recent_missed_holds.append(
                        f"{sym} HOLD機会損失(score={conv}, MFE={mfe_24:+.2f}% >= +2.0R[{two_r_pct:.2f}%])"
                    )
                if outcome == "SL_HIT" or mae_24 <= -risk_pct or pnl_val <= 0.0:
                    hold_avoided_drops += 1

        fpr_rate = (buy_sl_hits / buy_total) if buy_total > 0 else 0.0
        fnr_rate = (hold_missed_rallies / hold_total) if hold_total > 0 else 0.0
        tnr_rate = (hold_avoided_drops / hold_total) if hold_total > 0 else 0.0

        if raw_confidences:
            sorted_c = sorted(raw_confidences)
            idx_75 = 0.75 * (len(sorted_c) - 1)
            lo_idx = int(idx_75)
            hi_idx = min(len(sorted_c) - 1, lo_idx + 1)
            frac = idx_75 - lo_idx
            q75 = sorted_c[lo_idx] + frac * (sorted_c[hi_idx] - sorted_c[lo_idx])
            mean_conf = sum(sorted_c) / len(sorted_c)
        else:
            q75 = 0.72
            mean_conf = 0.72

        effective_threshold = max(0.72, round(q75, 4))
        empirical_win_rate = (buy_wins / buy_total) if buy_total > 0 else 0.50

        w_shd = max(0.0, min(1.0, float(w_shadow)))
        n_live = max(int(live_trade_count or 0), int(live_wins or 0) + int(live_losses or 0))
        weighted_shadow_wins = round(w_shd * float(buy_wins), 4)
        weighted_shadow_losses = round(w_shd * float(buy_sl_hits), 4)
        weighted_shadow_total = round(w_shd * float(buy_total), 4)
        weighted_shadow_sample_size = round(w_shd * float(buy_total + hold_total), 4)
        effective_weighted_wins = round(float(live_wins or 0) + weighted_shadow_wins, 4)
        effective_weighted_total = round(float(n_live) + weighted_shadow_total, 4)
        mintrl_info = evaluate_mintrl_freeze_stage(n_live)

        return {
            "evaluated_sample_size": buy_total + hold_total,
            "shadow_weight": w_shd,
            "weighted_shadow_sample_size": weighted_shadow_sample_size,
            "weighted_shadow_wins": weighted_shadow_wins,
            "weighted_shadow_losses": weighted_shadow_losses,
            "weighted_shadow_total": weighted_shadow_total,
            "live_trade_count": n_live,
            "live_wins": int(live_wins or 0),
            "live_losses": int(live_losses or 0),
            "effective_weighted_wins": effective_weighted_wins,
            "effective_weighted_total": effective_weighted_total,
            "mintrl_stage": mintrl_info["mintrl_stage"],
            "mintrl_info": mintrl_info,
            "buy_total": buy_total,
            "buy_wins": buy_wins,
            "buy_sl_hits": buy_sl_hits,
            "hold_total": hold_total,
            "hold_missed_rallies": hold_missed_rallies,
            "hold_avoided_drops": hold_avoided_drops,
            "fpr_overconfidence_rate": round(fpr_rate, 4),
            "fnr_missed_opportunity_rate": round(fnr_rate, 4),
            "tnr_veto_accuracy_rate": round(tnr_rate, 4),
            "empirical_win_rate": round(empirical_win_rate, 4),
            "mean_raw_confidence": round(mean_conf, 4),
            "quantile_75_confidence": round(q75, 4),
            "effective_threshold": round(effective_threshold, 4),
            "recent_failed_buys": recent_failed_buys[-3:],
            "recent_missed_holds": recent_missed_holds[-3:],
            "bayesian_prior": {"alpha_0": alpha_0, "beta_0": beta_0, "k_weight": k_weight},
        }

    def calibrate_confidence_and_threshold(
        self,
        raw_confidence: float,
        limit: int = 30,
        alpha_0: float = 2.0,
        beta_0: float = 2.0,
        k_weight: float = 5.0,
        live_trade_count: int = 0,
        live_wins: int = 0,
        live_losses: int = 0,
        w_shadow: float = SHADOW_DISCOUNT_WEIGHT,
    ) -> Dict[str, Any]:
        """
        ベータ・二項ベイズ較正＆動的分位点閾値調整を実行する:
          p_calibrated = (alpha_0 + W + k * p_raw) / (alpha_0 + beta_0 + N + k)
          p_shadow_discounted = (alpha_0 + (W_live + 0.25*W_shd) + k * p_raw) / (alpha_0 + beta_0 + (N_live + 0.25*N_shd) + k)
          effective_threshold = max(0.72, Quantile_0.75)
        """
        p_raw = max(0.0, min(1.0, float(raw_confidence or 0.0)))
        scorecard = self.compute_self_calibration_scorecard(
            limit=limit,
            alpha_0=alpha_0,
            beta_0=beta_0,
            k_weight=k_weight,
            live_trade_count=live_trade_count,
            live_wins=live_wins,
            live_losses=live_losses,
            w_shadow=w_shadow,
        )
        w_wins = float(scorecard["buy_wins"])
        n_total = float(scorecard["buy_total"])
        denom = float(alpha_0) + float(beta_0) + n_total + float(k_weight)
        p_calibrated = (
            (float(alpha_0) + w_wins + float(k_weight) * p_raw) / denom
            if denom > 0
            else p_raw
        )

        w_eff = float(scorecard["effective_weighted_wins"])
        n_eff = float(scorecard["effective_weighted_total"])
        denom_eff = float(alpha_0) + float(beta_0) + n_eff + float(k_weight)
        p_shadow_discounted = (
            (float(alpha_0) + w_eff + float(k_weight) * p_raw) / denom_eff
            if denom_eff > 0
            else p_raw
        )

        effective_threshold = float(scorecard["effective_threshold"])
        return {
            "raw_confidence": round(p_raw, 4),
            "calibrated_confidence": round(p_calibrated, 4),
            "shadow_discounted_calibrated_confidence": round(p_shadow_discounted, 4),
            "shadow_weight": scorecard["shadow_weight"],
            "mintrl_stage": scorecard["mintrl_stage"],
            "parameters_frozen": scorecard.get("parameters_frozen", True),
            "quantile_75_confidence": scorecard["quantile_75_confidence"],
            "effective_threshold": round(effective_threshold, 4),
            "passes_dynamic_threshold": p_raw >= effective_threshold,
            "scorecard": scorecard,
        }
