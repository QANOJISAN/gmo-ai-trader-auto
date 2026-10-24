r"""
2027年7月（Phase 1終了時）の「DSR・MinTRL・WFE・PBO・OOS PF」5ゲート統計検証パイプライン (phase1_dsr_validator.py)

【設計仕様】
1. 外部パッケージ非依存（Python標準 `math`, `statistics`, `itertools` のみで高精度実装）。
2. Bailey & López de Prado (2014, 2017) 準拠の5ゲート統計検証：
   - ① `compute_effective_trials(returns_matrix)`:
     $T \times N$ リターン行列の非対角平均相関 $\bar{\rho}$ から独立有効試行回数
     $N_{\text{eff}} = 1 + (N - 1)(1 - \bar{\rho})$ を算出。
   - ② `expected_max_sr(sr_list, n_eff)`:
     オイラー・マスケローニ定数 $\gamma = 0.57721566490153286$ と標準正規分布の逆累積分布関数 $\Phi^{-1}$ を用い、
     1観測あたりスケールでの帰無仮説下の期待最大シャープレシオ $SR_0$ を算出。
   - ③ `calc_dsr_and_mintrl(returns, sr_0, confidence=0.95)`:
     標本サイズ $T$、1観測あたり $\widehat{SR}^*$、標本歪度 $\hat{\gamma}_3$、ピアソン尖度 $\hat{\gamma}_4$（正規分布＝3.0）から、
     非正規性補正済みの `Deflated Sharpe Ratio (DSR)` と最小必要トラックレコード長 `MinTRL` を算出。
   - ④ `compute_pbo_cscv(returns_matrix, n_partitions=8)`:
     CSCV（$\binom{S}{S/2}$ 通り、$S=8$ なら70通り）により、In-Sample 最良候補が Out-of-Sample で中央値を下回る
     ロジット $\lambda_c \le 0$ の割合 `PBO (Probability of Backtest Overfitting)` を算出。
   - ⑤ `compute_wfe_and_oos_pf(returns, n_folds=4, is_ratio=0.75)`:
     ローリング Walk-Forward 効率 `WFE` と、OOS全体・前半（H1）・後半（H2）のプロフィットファクター `PF` を算出。
   - ⑥ `evaluate_phase1_to_phase2_promotion(live_returns, shadow_returns_matrix, live_candidate_idx=0)`:
     Live-to-Shadow Execution Haircut $\Delta_{\text{exec}} \ge 0$ を全Shadow候補から控除した上で5つのハードゲートを判定し、
     ケースA（`PROMOTE_CURRENT_LIVE_PARAMETER`）・ケースB（`CANARY_PROMOTE_NEW_SHADOW_PARAMETER`）・
     ケースC（`KEEP_FROZEN_OR_SCALE_DOWN`）を決定する。
"""

from __future__ import annotations

import itertools
import math
import statistics
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

EULER_MASCHERONI_GAMMA: float = 0.57721566490153286
_STD_NORMAL = statistics.NormalDist(0.0, 1.0)


def norm_cdf(x: float) -> float:
    r"""標準正規分布の累積分布関数 $\Phi(x)$"""
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def norm_ppf(p: float) -> float:
    r"""標準正規分布の逆累積分布関数 $\Phi^{-1}(p)$"""
    p_clamped = max(1e-15, min(1.0 - 1e-15, float(p)))
    return float(_STD_NORMAL.inv_cdf(p_clamped))


class EffectiveTrialsResult(float):
    """
    `float` としてそのまま数値演算に使え、かつ `res["n_eff"]` / `res["avg_correlation"]` / `res["n_raw"]` の
    辞書キーアクセスも可能なハイブリッド戻り値型。
    """

    def __new__(cls, n_eff: float, n_raw: int = 1, avg_correlation: float = 0.0) -> "EffectiveTrialsResult":
        obj = super().__new__(cls, float(n_eff))
        obj.n_eff = float(n_eff)
        obj.n_raw = int(n_raw)
        obj.avg_correlation = float(avg_correlation)
        return obj

    def __getitem__(self, key: str) -> Any:
        if key == "n_eff":
            return self.n_eff
        if key == "n_raw":
            return self.n_raw
        if key in ("avg_correlation", "avg_rho"):
            return self.avg_correlation
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def to_dict(self) -> Dict[str, Any]:
        return {
            "n_eff": self.n_eff,
            "n_raw": self.n_raw,
            "avg_correlation": self.avg_correlation,
        }


def _compute_sample_moments(returns: Sequence[float]) -> Tuple[int, float, float, float]:
    r"""
    1観測あたりリターン系列から $(T, \widehat{SR}, \hat{\gamma}_3, \hat{\gamma}_4)$ を算出する。
    ※ $\hat{\gamma}_4$ はピアソン尖度（正規分布 = 3.0）。
    """
    vals = [float(x) for x in returns]
    n = len(vals)
    if n < 2:
        return n, 0.0, 0.0, 3.0

    mean_r = sum(vals) / n
    diffs = [x - mean_r for x in vals]
    var_sample = sum(d * d for d in diffs) / (n - 1)
    std_sample = math.sqrt(max(0.0, var_sample))
    if std_sample <= 1e-15:
        return n, 0.0, 0.0, 3.0

    sr_hat = mean_r / std_sample
    m2_pop = sum(d * d for d in diffs) / n
    std_pop = math.sqrt(max(1e-30, m2_pop))
    z_scores = [d / std_pop for d in diffs]
    skew_hat = sum(z ** 3 for z in z_scores) / n
    kurt_pearson = sum(z ** 4 for z in z_scores) / n
    return n, sr_hat, skew_hat, kurt_pearson


def _pairwise_corr(col_a: Sequence[float], col_b: Sequence[float]) -> float:
    n = min(len(col_a), len(col_b))
    if n < 2:
        return 0.0
    ma = sum(col_a[:n]) / n
    mb = sum(col_b[:n]) / n
    da = [col_a[i] - ma for i in range(n)]
    db = [col_b[i] - mb for i in range(n)]
    va = sum(x * x for x in da)
    vb = sum(y * y for y in db)
    if va <= 1e-30 or vb <= 1e-30:
        return 0.0
    cov = sum(da[i] * db[i] for i in range(n))
    return max(-1.0, min(1.0, cov / math.sqrt(va * vb)))


def compute_effective_trials(returns_matrix: Sequence[Sequence[float]]) -> EffectiveTrialsResult:
    r"""
    $T \times N$ リターン行列（$T$: 観測数、$N$: 候補パラメータ数）から
    非対角平均相関 $\bar{\rho}$ と独立有効試行回数 $N_{\text{eff}} = 1 + (N - 1)(1 - \bar{\rho})$ を算出する。
    """
    if not returns_matrix:
        return EffectiveTrialsResult(1.0, n_raw=1, avg_correlation=0.0)

    n_rows = len(returns_matrix)
    n_cols = len(returns_matrix[0]) if n_rows > 0 else 0
    if n_cols <= 1:
        return EffectiveTrialsResult(1.0, n_raw=max(1, n_cols), avg_correlation=0.0)

    cols: List[List[float]] = [
        [float(returns_matrix[t][j]) for t in range(n_rows)]
        for j in range(n_cols)
    ]

    corrs: List[float] = []
    for i in range(n_cols):
        for j in range(i + 1, n_cols):
            corrs.append(_pairwise_corr(cols[i], cols[j]))

    avg_rho = sum(corrs) / len(corrs) if corrs else 0.0
    avg_rho_clamped = max(0.0, min(1.0, avg_rho))
    n_eff = 1.0 + (n_cols - 1.0) * (1.0 - avg_rho_clamped)
    n_eff = max(1.0, min(float(n_cols), n_eff))
    return EffectiveTrialsResult(n_eff, n_raw=n_cols, avg_correlation=avg_rho)


def expected_max_sr(sr_list: Sequence[float], n_eff: float) -> float:
    r"""
    False Strategy Theorem に基づき、帰無仮説 ($\text{True } SR = 0$) の下で $N_{\text{eff}}$ 回の
    独立試行から得られる期待最大シャープレシオ $SR_0$（1観測あたりスケール）を算出する：
      $$SR_0 = \sqrt{\mathbb{V}[\{\widehat{SR}_n\}]} \cdot \left((1 - \gamma)\Phi^{-1}\left(1 - \frac{1}{N_{\text{eff}}}\right) + \gamma \Phi^{-1}\left(1 - \frac{1}{N_{\text{eff}} \cdot e}\right)\right)$$
    """
    n_eff_val = float(n_eff)
    if n_eff_val <= 1.0 or len(sr_list) < 2:
        return 0.0

    vals = [float(s) for s in sr_list]
    mean_sr = sum(vals) / len(vals)
    var_sr = sum((s - mean_sr) ** 2 for s in vals) / (len(vals) - 1)
    std_sr = math.sqrt(max(0.0, var_sr))

    term1 = (1.0 - EULER_MASCHERONI_GAMMA) * norm_ppf(1.0 - 1.0 / n_eff_val)
    term2 = EULER_MASCHERONI_GAMMA * norm_ppf(1.0 - 1.0 / (n_eff_val * math.e))
    return float(std_sr * (term1 + term2))


def calc_dsr_and_mintrl(
    returns: Optional[Sequence[float]] = None,
    sr_0: float = 0.0,
    confidence: float = 0.95,
    *,
    T: Optional[int] = None,
    sr_hat: Optional[float] = None,
    skew: Optional[float] = None,
    kurt_pearson: Optional[float] = None,
) -> Dict[str, Any]:
    r"""
    非正規性（歪度 $\hat{\gamma}_3$・ピアソン尖度 $\hat{\gamma}_4$）および多重比較バイアス ($SR_0$) を補正した
    `Deflated Sharpe Ratio (DSR)` と最小必要トラックレコード長 `MinTRL` を算出する。
    `returns` 生配列の指定、または `(T, sr_hat, skew, kurt_pearson)` の直接指定の双方に対応する。
    """
    if returns is not None and len(returns) >= 2 and (T is None or sr_hat is None):
        t_calc, sr_calc, skew_calc, kurt_calc = _compute_sample_moments(returns)
        t_val = int(T if T is not None else t_calc)
        sr_val = float(sr_hat if sr_hat is not None else sr_calc)
        g3 = float(skew if skew is not None else skew_calc)
        g4 = float(kurt_pearson if kurt_pearson is not None else kurt_calc)
    else:
        t_val = int(T or 0)
        sr_val = float(sr_hat or 0.0)
        g3 = float(skew or 0.0)
        g4 = float(kurt_pearson if kurt_pearson is not None else 3.0)

    sr0_val = float(sr_0)
    if t_val < 2:
        return {
            "T": t_val,
            "sr_hat": sr_val,
            "sr_0": sr0_val,
            "skew": g3,
            "kurt_pearson": g4,
            "dsr": 0.0,
            "min_trl": float("inf"),
            "gate1_dsr_pass": False,
            "gate2_mintrl_pass": False,
        }

    # 非正規性分散補正項: 1 - \gamma_3 * SR + ((\gamma_4 - 1) / 4) * SR^2
    var_adj = max(1e-12, 1.0 - g3 * sr_val + ((g4 - 1.0) / 4.0) * (sr_val ** 2))
    denom = math.sqrt(var_adj)
    z_dsr = ((sr_val - sr0_val) * math.sqrt(t_val - 1.0)) / denom
    dsr = float(norm_cdf(z_dsr))

    if sr_val <= sr0_val:
        min_trl = float("inf")
    else:
        z_alpha = norm_ppf(float(confidence))
        min_trl = 1.0 + var_adj * ((z_alpha / (sr_val - sr0_val)) ** 2)

    return {
        "T": t_val,
        "sr_hat": round(sr_val, 6),
        "sr_0": round(sr0_val, 6),
        "skew": round(g3, 6),
        "kurt_pearson": round(g4, 6),
        "non_normality_var_factor": round(var_adj, 6),
        "z_dsr": round(z_dsr, 6),
        "dsr": round(dsr, 6),
        "min_trl": round(min_trl, 2) if math.isfinite(min_trl) else float("inf"),
        "gate1_dsr_pass": bool(dsr >= float(confidence)),
        "gate2_mintrl_pass": bool(t_val >= min_trl),
    }


def compute_pbo_cscv(
    returns_matrix: Sequence[Sequence[float]],
    n_partitions: int = 8,
) -> Dict[str, Any]:
    r"""
    Combinatorially Symmetric Cross-Validation (CSCV, Bailey et al. 2017) により
    バックテスト過剰適合確率 `PBO (Probability of Backtest Overfitting)` を算出する。
    $S = \text{n\_partitions}$（既定 8 $\implies \binom{8}{4} = 70$ 通り）の部分行列分割において、
    In-Sample 最良候補 $n^*$ が Out-of-Sample で中央値を下回る ($\lambda_c \le 0$) 比率を返す。
    """
    if not returns_matrix:
        return {"pbo": 1.0, "n_combinations": 0, "logits": [], "gate4_pbo_pass": False}

    n_rows = len(returns_matrix)
    n_cols = len(returns_matrix[0]) if n_rows > 0 else 0
    if n_cols < 2 or n_rows < n_partitions:
        return {"pbo": 0.0, "n_combinations": 0, "logits": [], "gate4_pbo_pass": True}

    s_parts = int(n_partitions)
    if s_parts % 2 != 0:
        s_parts += 1
    half_s = s_parts // 2
    chunk_size = n_rows // s_parts
    if chunk_size < 2:
        return {"pbo": 0.0, "n_combinations": 0, "logits": [], "gate4_pbo_pass": True}

    partitions: List[List[List[float]]] = []
    for p_idx in range(s_parts):
        start_r = p_idx * chunk_size
        end_r = (p_idx + 1) * chunk_size if p_idx < s_parts - 1 else n_rows
        partitions.append([[float(returns_matrix[r][c]) for c in range(n_cols)] for r in range(start_r, end_r)])

    def _col_sharpe(rows_subset: List[List[float]], col_idx: int) -> float:
        vals = [r[col_idx] for r in rows_subset]
        m = len(vals)
        if m < 2:
            return 0.0
        mu = sum(vals) / m
        var = sum((x - mu) ** 2 for x in vals) / (m - 1)
        sd = math.sqrt(max(0.0, var))
        if sd <= 1e-15:
            return mu * 1e6 if mu != 0 else 0.0
        return mu / sd

    all_indices = tuple(range(s_parts))
    logits: List[float] = []
    under_median_count = 0

    for is_indices in itertools.combinations(all_indices, half_s):
        is_set = set(is_indices)
        oos_indices = [idx for idx in all_indices if idx not in is_set]

        is_rows = [row for idx in is_indices for row in partitions[idx]]
        oos_rows = [row for idx in oos_indices for row in partitions[idx]]

        is_sharpes = [_col_sharpe(is_rows, c) for c in range(n_cols)]
        oos_sharpes = [_col_sharpe(oos_rows, c) for c in range(n_cols)]

        best_is_col = max(range(n_cols), key=lambda c: is_sharpes[c])
        target_oos_sr = oos_sharpes[best_is_col]

        # 昇順ランク (1 .. N)
        less_count = sum(1 for s in oos_sharpes if s < target_oos_sr)
        equal_count = sum(1 for s in oos_sharpes if s == target_oos_sr)
        rank_pos = less_count + 0.5 * (equal_count + 1.0)
        omega = max(1.0 / (n_cols + 1.0), min(n_cols / (n_cols + 1.0), rank_pos / (n_cols + 1.0)))
        lam = math.log(omega / (1.0 - omega))
        logits.append(lam)
        if lam <= 0.0:
            under_median_count += 1

    n_comb = len(logits)
    pbo = float(under_median_count / n_comb) if n_comb > 0 else 1.0
    return {
        "pbo": round(pbo, 6),
        "n_combinations": n_comb,
        "under_median_count": under_median_count,
        "logits": [round(x, 4) for x in logits],
        "gate4_pbo_pass": bool(pbo < 0.50),
    }


def _calc_profit_factor(returns: Sequence[float]) -> float:
    gross_profit = sum(float(r) for r in returns if float(r) > 0.0)
    gross_loss = abs(sum(float(r) for r in returns if float(r) < 0.0))
    if gross_loss <= 1e-15:
        return 99.0 if gross_profit > 0.0 else 0.0
    return float(gross_profit / gross_loss)


def compute_wfe_and_oos_pf(
    returns: Sequence[float],
    n_folds: int = 4,
    is_ratio: float = 0.75,
) -> Dict[str, Any]:
    """
    ローリング Walk-Forward 効率 (`WFE`) と、OOS全体・前半（H1）・後半（H2）の
    プロフィットファクター (`PF_total`, `PF_H1`, `PF_H2`) を算出する。
    """
    vals = [float(x) for x in returns]
    n = len(vals)
    if n < 8:
        pf_all = _calc_profit_factor(vals)
        return {
            "wfe": 0.0,
            "pf_total": round(pf_all, 4),
            "pf_h1": round(pf_all, 4),
            "pf_h2": round(pf_all, 4),
            "gate3_wfe_pass": False,
            "gate5_oos_pf_pass": False,
        }

    folds = max(2, int(n_folds))
    fold_len = max(4, n // folds)
    is_means: List[float] = []
    oos_means: List[float] = []

    for f_idx in range(folds):
        start_i = f_idx * fold_len
        end_i = (f_idx + 1) * fold_len if f_idx < folds - 1 else n
        seg = vals[start_i:end_i]
        if len(seg) < 4:
            continue
        split_pt = max(1, min(len(seg) - 1, int(len(seg) * float(is_ratio))))
        is_seg = seg[:split_pt]
        oos_seg = seg[split_pt:]
        is_means.append(sum(is_seg) / len(is_seg))
        oos_means.append(sum(oos_seg) / len(oos_seg))

    avg_is = sum(is_means) / len(is_means) if is_means else 0.0
    avg_oos = sum(oos_means) / len(oos_means) if oos_means else 0.0
    if avg_is <= 1e-12:
        wfe = 0.0 if avg_oos <= 0.0 else 1.0
    else:
        wfe = float(avg_oos / avg_is)

    mid = n // 2
    pf_total = _calc_profit_factor(vals)
    pf_h1 = _calc_profit_factor(vals[:mid])
    pf_h2 = _calc_profit_factor(vals[mid:])

    gate3_pass = bool(wfe >= 0.60)
    gate5_pass = bool(pf_total > 1.0 and pf_h1 >= 1.0 and pf_h2 >= 1.0)

    return {
        "wfe": round(wfe, 4),
        "pf_total": round(pf_total, 4),
        "pf_h1": round(pf_h1, 4),
        "pf_h2": round(pf_h2, 4),
        "gate3_wfe_pass": gate3_pass,
        "gate5_oos_pf_pass": gate5_pass,
    }


def evaluate_phase1_to_phase2_promotion(
    live_returns: Sequence[float],
    shadow_returns_matrix: Sequence[Sequence[float]],
    live_candidate_idx: int = 0,
    confidence: float = 0.95,
) -> Dict[str, Any]:
    r"""
    2027年7月（Phase 1 終了時）の5ゲート統計検証を実行し、Phase 2 への昇格アクションを決定する：
      1. Live実約定リターンとShadow仮想リターンの平均執行乖離（Live-to-Shadow Execution Haircut $\Delta_{\text{exec}} \ge 0$）を算出。
      2. 全Shadow候補のリターン系列から $\Delta_{\text{exec}}$ を控除。
      3. $N_{\text{eff}}$ と $SR_0$、CSCV `PBO` を算出。
      4. 現行Liveパラメータおよび最良Shadow候補 $n^*$ に対して5つのハードゲートを判定：
         - Gate 1: $\text{DSR} \ge 0.95$
         - Gate 2: $T \ge \text{MinTRL}$
         - Gate 3: $\text{WFE} \ge 0.60$
         - Gate 4: $\text{PBO} < 0.50$
         - Gate 5: $\text{PF}_{\text{total}} > 1.0 \land \text{PF}_{\text{H1}} \ge 1.0 \land \text{PF}_{\text{H2}} \ge 1.0$
      5. 判定結果に基づき以下の3ケースへ分岐：
         - ケースA (`PROMOTE_CURRENT_LIVE_PARAMETER`): 現行Liveパラメータが全5ゲートを突破
         - ケースB (`CANARY_PROMOTE_NEW_SHADOW_PARAMETER`): 現行Liveは未達だが、ヘアカット控除後の最良Shadow候補が全5ゲートを突破（1週間・50%ロットのカナリア昇格）
         - ケースC (`KEEP_FROZEN_OR_SCALE_DOWN`): いずれも全5ゲートを突破できず、パラメータ変更禁止・現行維持または縮小
    """
    live_vals = [float(x) for x in live_returns]
    t_len = len(live_vals)
    n_cands = len(shadow_returns_matrix[0]) if shadow_returns_matrix and len(shadow_returns_matrix) > 0 else 0

    if t_len < 2 or n_cands < 1:
        return {
            "decision": "KEEP_FROZEN_OR_SCALE_DOWN",
            "case": "CASE_C",
            "reason": "INSUFFICIENT_OBSERVATIONS",
            "execution_haircut": 0.0,
        }

    idx_live = max(0, min(n_cands - 1, int(live_candidate_idx)))
    shadow_live_col = [float(shadow_returns_matrix[t][idx_live]) for t in range(min(t_len, len(shadow_returns_matrix)))]
    mean_live = sum(live_vals) / len(live_vals)
    mean_shadow_live = sum(shadow_live_col) / len(shadow_live_col)
    exec_haircut = max(0.0, mean_shadow_live - mean_live)

    # 全Shadow候補から Live-to-Shadow Execution Haircut を控除
    n_rows = min(t_len, len(shadow_returns_matrix))
    adj_matrix: List[List[float]] = [
        [float(shadow_returns_matrix[t][c]) - exec_haircut for c in range(n_cands)]
        for t in range(n_rows)
    ]

    eff_res = compute_effective_trials(adj_matrix)
    n_eff = float(eff_res)

    cand_cols: List[List[float]] = [
        [adj_matrix[t][c] for t in range(n_rows)]
        for c in range(n_cands)
    ]
    cand_moments = [_compute_sample_moments(col) for col in cand_cols]
    sr_list = [m[1] for m in cand_moments]
    sr_0 = expected_max_sr(sr_list, n_eff=n_eff)

    pbo_res = compute_pbo_cscv(adj_matrix, n_partitions=8)
    gate4_pbo_pass = bool(pbo_res["gate4_pbo_pass"])

    # 現行Liveパラメータの5ゲート評価
    live_dsr_res = calc_dsr_and_mintrl(live_vals, sr_0=sr_0, confidence=confidence)
    live_wfe_res = compute_wfe_and_oos_pf(live_vals)
    live_gates = {
        "gate1_dsr_pass": bool(live_dsr_res["gate1_dsr_pass"]),
        "gate2_mintrl_pass": bool(live_dsr_res["gate2_mintrl_pass"]),
        "gate3_wfe_pass": bool(live_wfe_res["gate3_wfe_pass"]),
        "gate4_pbo_pass": gate4_pbo_pass,
        "gate5_oos_pf_pass": bool(live_wfe_res["gate5_oos_pf_pass"]),
    }
    all_live_pass = all(live_gates.values())

    # 最良Shadow候補 n* の5ゲート評価
    best_cand_idx = max(range(n_cands), key=lambda c: sr_list[c])
    best_col = cand_cols[best_cand_idx]
    best_dsr_res = calc_dsr_and_mintrl(best_col, sr_0=sr_0, confidence=confidence)
    best_wfe_res = compute_wfe_and_oos_pf(best_col)
    best_shadow_gates = {
        "gate1_dsr_pass": bool(best_dsr_res["gate1_dsr_pass"]),
        "gate2_mintrl_pass": bool(best_dsr_res["gate2_mintrl_pass"]),
        "gate3_wfe_pass": bool(best_wfe_res["gate3_wfe_pass"]),
        "gate4_pbo_pass": gate4_pbo_pass,
        "gate5_oos_pf_pass": bool(best_wfe_res["gate5_oos_pf_pass"]),
    }
    all_shadow_pass = all(best_shadow_gates.values())

    if all_live_pass:
        decision = "PROMOTE_CURRENT_LIVE_PARAMETER"
        case_code = "CASE_A"
        canary_lot_ratio = 1.0
    elif all_shadow_pass and best_cand_idx != idx_live:
        decision = "CANARY_PROMOTE_NEW_SHADOW_PARAMETER"
        case_code = "CASE_B"
        canary_lot_ratio = 0.50
    else:
        decision = "KEEP_FROZEN_OR_SCALE_DOWN"
        case_code = "CASE_C"
        canary_lot_ratio = 0.0

    return {
        "decision": decision,
        "case": case_code,
        "canary_lot_ratio": canary_lot_ratio,
        "execution_haircut": round(exec_haircut, 6),
        "n_raw_candidates": n_cands,
        "n_eff": round(n_eff, 4),
        "avg_correlation": round(eff_res.avg_correlation, 4),
        "sr_0": round(sr_0, 6),
        "pbo": pbo_res["pbo"],
        "live_candidate_idx": idx_live,
        "best_shadow_candidate_idx": best_cand_idx,
        "live_metrics": {**live_dsr_res, **live_wfe_res, "pbo": pbo_res["pbo"]},
        "live_gates": live_gates,
        "all_5_gates_live_pass": all_live_pass,
        "best_shadow_metrics": {**best_dsr_res, **best_wfe_res, "pbo": pbo_res["pbo"]},
        "best_shadow_gates": best_shadow_gates,
        "all_5_gates_shadow_pass": all_shadow_pass,
    }
