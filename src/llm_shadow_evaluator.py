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
