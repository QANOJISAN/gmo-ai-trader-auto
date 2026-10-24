"""
Forensic Ledger 高精度税務監査エンジン (forensic_tax_auditor.py)

【設計仕様】
1. 固定小数点 (`decimal.Decimal`, 精度50桁) 演算による IEEE 754 浮動小数点丸め誤差の完全排除。
2. 全口座合算の取得単価プール (`AssetCostPool`):
   - 国税庁（所得税法施行令第118条の2）に準拠し、取引所 (`GMO_COIN`, `HYPERLIQUID`, `QANX_DEX`) や
     課税区分を問わず、同一銘柄ごとに全口座合算の単一プールで「移動平均法 (`moving_unit_cost_jpy`)」と
     「総平均法 (`periodic_average_unit_cost_jpy`)」を並列計算し、両評価法間の乖離額
     (`basis_method_divergence_jpy`：年末の総平均法トラップ監視指標) を算出する。
3. Makerマイナス手数料（リベート）の二方式切替対応 (`rebate_incorporation_mode`):
   - `False`（①雑所得独立計上方式）：約定代金はグロス計上し、Makerリベートは `rebate_income_jpy` として独立計上。
   - `True`（②取得価額・譲渡費用加減算方式）：買付時リベートは取得価額から控除、売却時リベートは譲渡収入へ加算。
4. 2028年申告分離課税 (`SEPARATE_20315`) と総合課税 (`COMPREHENSIVE_MISC`) の厳格箱分け ＆ 損益通算禁止監査:
   - 2027年12月31日23:59:59 JSTまでは全取引を `COMPREHENSIVE_MISC`（内部通算可・繰越不可）として計算。
   - 2028年1月1日00:00:00 JST以降は、国内金融庁登録業者 (`GMO_COIN`) の特定暗号資産現物譲渡損益を
     `SEPARATE_20315`（税率 `20.315%`・3年間損失繰越控除 `carryforward_loss_3yr_jpy`）、
     海外Perp/DEX (`HYPERLIQUID`, `QANX_DEX`) を `COMPREHENSIVE_MISC`（最大 `55.945%`・繰越不可）へ分離し、
     **両バケット間の損益通算を厳格に禁止**（片方が黒字・他方が赤字でも相殺せず、`0` でフロア）する。
5. 2027年12月特例タックス・ハーベスティング監査アラート (`evaluate_2027_year_end_tax_harvesting`):
   - 年内確定利益 `> 0` かつ含み損建玉がある場合の年内損出し決済推奨 (`RECOMMEND_2027_LOSS_HARVESTING`)
   - GMOコイン現物に大きな含み益がある場合の2028年1月1日以降への利確繰り延べ推奨 (`RECOMMEND_DEFER_GMO_GAIN_TO_2028`)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, getcontext
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Union

from src.database_wal import connect_forensic_readonly

getcontext().prec = 50

JST = timezone(timedelta(hours=9))

SEPARATE_TAX_RATE = Decimal("0.20315")        # 所得税 15% + 復興特別所得税 0.315% + 住民税 5%
MAX_COMPREHENSIVE_TAX_RATE = Decimal("0.55945")  # 最高実効税率想定値 (所得税45% + 復興0.945% + 住民税10%)

DOMESTIC_REGISTERED_VENUES = frozenset({"GMO_COIN", "GMO", "JP_FSA_REGISTERED_GMO"})


class TaxCategory(str, Enum):
    COMPREHENSIVE_MISC = "COMPREHENSIVE_MISC"  # 総合課税（雑所得）：2027年以前の全取引、2028年以降の海外Perp/DEX・独立リベート
    SEPARATE_20315 = "SEPARATE_20315"          # 申告分離課税（20.315%）：2028年以降の国内登録業者（GMOコイン）現物譲渡損益


def _to_dec(val: Union[str, int, float, Decimal, None]) -> Decimal:
    if val is None:
        return Decimal("0")
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def _parse_jst(ts: Optional[Union[str, int, float, datetime]] = None) -> datetime:
    if ts is None:
        return datetime.now(JST)
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=JST) if ts.tzinfo is None else ts.astimezone(JST)
    if isinstance(ts, (int, float)):
        v = float(ts)
        if v > 1e11:
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=JST)
    raw = str(ts).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.replace(tzinfo=JST) if dt.tzinfo is None else dt.astimezone(JST)
    except Exception:
        return datetime.now(JST)


@dataclass
class AssetCostPool:
    """
    全口座合算の銘柄別取得単価プール（移動平均法 ＆ 総平均法の並列追跡）
    """

    asset: str
    moving_qty: Decimal = Decimal("0")
    moving_cost_jpy: Decimal = Decimal("0")
    year_start_qty: Decimal = Decimal("0")
    year_start_cost_jpy: Decimal = Decimal("0")
    year_buy_qty: Decimal = Decimal("0")
    year_buy_cost_jpy: Decimal = Decimal("0")
    year_sell_qty: Decimal = Decimal("0")
    year_sell_gross_proceeds_jpy: Decimal = Decimal("0")
    year_sell_expense_jpy: Decimal = Decimal("0")
    year_moving_realized_pnl_jpy: Decimal = Decimal("0")

    @property
    def moving_unit_cost_jpy(self) -> Decimal:
        if self.moving_qty <= Decimal("0"):
            return Decimal("0")
        return self.moving_cost_jpy / self.moving_qty

    @property
    def moving_unit_cost_ceil_yen(self) -> Decimal:
        """国税庁様式（1円未満の端数切り上げ）による移動平均単価"""
        u = self.moving_unit_cost_jpy
        if u <= Decimal("0"):
            return Decimal("0")
        return u.to_integral_value(rounding=ROUND_CEILING)

    @property
    def periodic_average_unit_cost_jpy(self) -> Decimal:
        """総平均法の年間取得単価 = (年初繰越原価 + 年中買付総額) / (年初数量 + 年中買付数量)"""
        total_q = self.year_start_qty + self.year_buy_qty
        if total_q <= Decimal("0"):
            return Decimal("0")
        return (self.year_start_cost_jpy + self.year_buy_cost_jpy) / total_q

    @property
    def periodic_average_unit_cost_ceil_yen(self) -> Decimal:
        """国税庁様式（1円未満の端数切り上げ）による総平均単価"""
        u = self.periodic_average_unit_cost_jpy
        if u <= Decimal("0"):
            return Decimal("0")
        return u.to_integral_value(rounding=ROUND_CEILING)

    @property
    def year_periodic_realized_pnl_jpy(self) -> Decimal:
        """総平均法による年間実現譲渡損益"""
        if self.year_sell_qty <= Decimal("0"):
            return Decimal("0")
        periodic_cost_basis = self.periodic_average_unit_cost_jpy * self.year_sell_qty
        return self.year_sell_gross_proceeds_jpy - periodic_cost_basis - self.year_sell_expense_jpy

    @property
    def basis_method_divergence_jpy(self) -> Decimal:
        """移動平均法と総平均法の年間実現損益乖離額 (`moving_pnl - periodic_pnl`)"""
        return self.year_moving_realized_pnl_jpy - self.year_periodic_realized_pnl_jpy


@dataclass
class AuditedTradeRecord:
    record_id: str
    timestamp_jst: str
    tax_year: int
    venue: str
    asset: str
    side: str
    qty: Decimal
    price_jpy: Decimal
    gross_jpy: Decimal
    fee_jpy: Decimal
    tax_category: TaxCategory
    realized_pnl_jpy: Decimal
    rebate_income_jpy: Decimal
    moving_unit_cost_after_jpy: Decimal
    cost_basis_jpy: Decimal = Decimal("0")


class ForensicTaxAuditor:
    """
    シングルノード・マルチボット対応の Forensic 高精度税務監査エンジン。
    """

    def __init__(self, rebate_incorporation_mode: bool = False) -> None:
        """
        :param rebate_incorporation_mode:
            `False`（既定・方式①）：Makerリベートを `rebate_income_jpy`（雑所得）として独立計上
            `True`（方式②）：Makerリベートを買付時は取得価額から控除、売却時は譲渡収入へ加算
        """
        self.rebate_incorporation_mode: bool = bool(rebate_incorporation_mode)
        self.pools: Dict[str, AssetCostPool] = {}
        self.records: List[AuditedTradeRecord] = []
        # 年度別集計バケット: year -> dict
        self._year_buckets: Dict[int, Dict[str, Decimal]] = {}
        # 2028年以降の申告分離課税バケット損失繰越プール: {origin_year: remaining_loss_jpy}
        self.separate_loss_carryforward_ledger: Dict[int, Decimal] = {}

    def _ensure_year_bucket(self, tax_year: int) -> Dict[str, Decimal]:
        yr = int(tax_year)
        if yr not in self._year_buckets:
            self._year_buckets[yr] = {
                "separate_spot_pnl_jpy": Decimal("0"),
                "comprehensive_spot_pnl_jpy": Decimal("0"),
                "comprehensive_perp_defi_pnl_jpy": Decimal("0"),
                "comprehensive_rebate_income_jpy": Decimal("0"),
                "fee_fraction_refund_jpy": Decimal("0"),
            }
        return self._year_buckets[yr]

    def get_pool(self, asset: str) -> AssetCostPool:
        sym = str(asset).strip().upper()
        if sym not in self.pools:
            self.pools[sym] = AssetCostPool(asset=sym)
        return self.pools[sym]

    def rollover_tax_year(self, new_tax_year: int) -> Dict[str, Any]:
        """
        年度切り替え時に各銘柄プールの期末数量・期末移動平均原価を翌期首 (`year_start_qty`, `year_start_cost_jpy`) へ繰り越し、
        年中買付・売却アキュムレータをリセットする。
        """
        rolled_assets: Dict[str, Dict[str, str]] = {}
        for sym, pool in self.pools.items():
            pool.year_start_qty = pool.moving_qty
            pool.year_start_cost_jpy = pool.moving_cost_jpy
            pool.year_buy_qty = Decimal("0")
            pool.year_buy_cost_jpy = Decimal("0")
            pool.year_sell_qty = Decimal("0")
            pool.year_sell_gross_proceeds_jpy = Decimal("0")
            pool.year_sell_expense_jpy = Decimal("0")
            pool.year_moving_realized_pnl_jpy = Decimal("0")
            rolled_assets[sym] = {
                "year_start_qty": str(pool.year_start_qty),
                "year_start_cost_jpy": str(pool.year_start_cost_jpy),
            }
        self._ensure_year_bucket(int(new_tax_year))
        return {"new_tax_year": int(new_tax_year), "rolled_assets": rolled_assets}

    def record_spot_fill(
        self,
        *,
        venue: str,
        asset: str,
        side: str,
        qty: Union[str, int, float, Decimal],
        price_jpy: Union[str, int, float, Decimal],
        fee_jpy: Union[str, int, float, Decimal] = "0",
        timestamp_jst: Optional[Union[str, int, float, datetime]] = None,
        is_registered_domestic_spot: Optional[bool] = None,
        record_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        現物約定を記録し、全口座合算の移動平均法・総平均法プールを更新するとともに、
        年度・取引所登録区分に応じて `SEPARATE_20315` または `COMPREHENSIVE_MISC` へ損益を振り分ける。
        `fee_jpy` は正の値＝Taker支払手数料、負の値＝Maker受取リベートを表す。
        """
        dt_jst = _parse_jst(timestamp_jst)
        tax_year = int(dt_jst.year)
        venue_up = str(venue or "GMO_COIN").strip().upper()
        asset_up = str(asset).strip().upper()
        side_up = str(side).strip().upper()
        qty_d = _to_dec(qty)
        price_d = _to_dec(price_jpy)
        fee_d = _to_dec(fee_jpy)

        if qty_d <= Decimal("0"):
            raise ValueError(f"qty は正の数である必要があります: {qty}")

        if is_registered_domestic_spot is None:
            is_dom_spot = venue_up in DOMESTIC_REGISTERED_VENUES
        else:
            is_dom_spot = bool(is_registered_domestic_spot)

        if tax_year <= 2027:
            tax_cat = TaxCategory.COMPREHENSIVE_MISC
        else:
            tax_cat = TaxCategory.SEPARATE_20315 if is_dom_spot else TaxCategory.COMPREHENSIVE_MISC

        gross_jpy = qty_d * price_d
        pool = self.get_pool(asset_up)
        bucket = self._ensure_year_bucket(tax_year)

        realized_pnl_jpy = Decimal("0")
        rebate_income_jpy = Decimal("0")
        cost_basis_jpy = Decimal("0")

        if side_up == "BUY":
            if fee_d < Decimal("0"):
                # Maker マイナス手数料（リベート受取）
                if self.rebate_incorporation_mode:
                    # 方式②：取得価額からリベート額を控除
                    acq_cost = gross_jpy + fee_d
                else:
                    # 方式①：取得価額はグロス計上し、リベートは雑所得へ独立計上
                    acq_cost = gross_jpy
                    rebate_income_jpy = -fee_d
            else:
                acq_cost = gross_jpy + fee_d

            pool.moving_qty += qty_d
            pool.moving_cost_jpy += acq_cost
            pool.year_buy_qty += qty_d
            pool.year_buy_cost_jpy += acq_cost
            cost_basis_jpy = acq_cost

        elif side_up == "SELL":
            if pool.moving_qty < qty_d:
                raise ValueError(
                    f"{asset_up} の保有数量 ({pool.moving_qty}) を超える売却 ({qty_d}) は記録できません。"
                )
            unit_cost = pool.moving_unit_cost_jpy
            cost_basis_jpy = unit_cost * qty_d
            pool.moving_qty -= qty_d
            pool.moving_cost_jpy -= cost_basis_jpy
            if pool.moving_qty == Decimal("0"):
                pool.moving_cost_jpy = Decimal("0")

            if fee_d < Decimal("0"):
                if self.rebate_incorporation_mode:
                    # 方式②：売却時リベートを譲渡収入へ加算（譲渡費用マイナスとして処理）
                    sell_expense = fee_d
                else:
                    # 方式①：売却損益はグロス計算し、リベートは雑所得へ独立計上
                    sell_expense = Decimal("0")
                    rebate_income_jpy = -fee_d
            else:
                sell_expense = fee_d

            realized_pnl_jpy = gross_jpy - cost_basis_jpy - sell_expense
            pool.year_sell_qty += qty_d
            pool.year_sell_gross_proceeds_jpy += gross_jpy
            pool.year_sell_expense_jpy += sell_expense
            pool.year_moving_realized_pnl_jpy += realized_pnl_jpy
        else:
            raise ValueError(f"不正な売買区分です: {side}")

        if tax_cat == TaxCategory.SEPARATE_20315:
            bucket["separate_spot_pnl_jpy"] += realized_pnl_jpy
        else:
            bucket["comprehensive_spot_pnl_jpy"] += realized_pnl_jpy

        bucket["comprehensive_rebate_income_jpy"] += rebate_income_jpy

        rec_id = record_id or f"fill-{len(self.records) + 1:06d}"
        rec = AuditedTradeRecord(
            record_id=rec_id,
            timestamp_jst=dt_jst.isoformat(timespec="seconds"),
            tax_year=tax_year,
            venue=venue_up,
            asset=asset_up,
            side=side_up,
            qty=qty_d,
            price_jpy=price_d,
            gross_jpy=gross_jpy,
            fee_jpy=fee_d,
            tax_category=tax_cat,
            realized_pnl_jpy=realized_pnl_jpy,
            rebate_income_jpy=rebate_income_jpy,
            moving_unit_cost_after_jpy=pool.moving_unit_cost_jpy,
            cost_basis_jpy=cost_basis_jpy,
        )
        self.records.append(rec)

        return {
            "record_id": rec.record_id,
            "tax_year": tax_year,
            "venue": venue_up,
            "asset": asset_up,
            "side": side_up,
            "tax_category": tax_cat.value,
            "realized_pnl_jpy": realized_pnl_jpy,
            "rebate_income_jpy": rebate_income_jpy,
            "cost_basis_jpy": cost_basis_jpy,
            "moving_unit_cost_jpy": pool.moving_unit_cost_jpy,
            "periodic_average_unit_cost_jpy": pool.periodic_average_unit_cost_jpy,
            "basis_method_divergence_jpy": pool.basis_method_divergence_jpy,
        }

    def record_perp_or_defi_pnl(
        self,
        *,
        venue: str,
        asset: str = "USDC",
        realized_pnl_jpy: Union[str, int, float, Decimal],
        funding_or_gas_fee_jpy: Union[str, int, float, Decimal] = "0",
        timestamp_jst: Optional[Union[str, int, float, datetime]] = None,
    ) -> Dict[str, Any]:
        """
        海外Perp (`HYPERLIQUID`) の差金決済損益・Funding Fee、または DEX (`QANX_DEX`) の
        スワップ・流動性提供損益を記録する。
        2028年以降も常に `COMPREHENSIVE_MISC`（総合課税・雑所得）へ分類される。
        """
        dt_jst = _parse_jst(timestamp_jst)
        tax_year = int(dt_jst.year)
        venue_up = str(venue or "HYPERLIQUID").strip().upper()
        pnl_d = _to_dec(realized_pnl_jpy) - _to_dec(funding_or_gas_fee_jpy)
        bucket = self._ensure_year_bucket(tax_year)
        bucket["comprehensive_perp_defi_pnl_jpy"] += pnl_d

        return {
            "tax_year": tax_year,
            "venue": venue_up,
            "asset": str(asset).strip().upper(),
            "tax_category": TaxCategory.COMPREHENSIVE_MISC.value,
            "net_pnl_jpy": pnl_d,
        }

    def compute_basis_method_comparison(self) -> Dict[str, Dict[str, Any]]:
        """
        全保有・取引銘柄について「移動平均法」と「総平均法」の単価・実現損益・乖離額を一覧化する。
        """
        comparison: Dict[str, Dict[str, Any]] = {}
        for sym, pool in sorted(self.pools.items()):
            div = pool.basis_method_divergence_jpy
            comparison[sym] = {
                "asset": sym,
                "ending_qty": pool.moving_qty,
                "moving_unit_cost_jpy": pool.moving_unit_cost_jpy,
                "moving_unit_cost_ceil_yen": pool.moving_unit_cost_ceil_yen,
                "periodic_average_unit_cost_jpy": pool.periodic_average_unit_cost_jpy,
                "periodic_average_unit_cost_ceil_yen": pool.periodic_average_unit_cost_ceil_yen,
                "moving_realized_pnl_jpy": pool.year_moving_realized_pnl_jpy,
                "periodic_realized_pnl_jpy": pool.year_periodic_realized_pnl_jpy,
                "basis_method_divergence_jpy": div,
                "periodic_average_trap_warning": bool(abs(div) >= Decimal("10000")),
            }
        return comparison

    def audit_year_summary(
        self,
        tax_year: int,
        prior_separate_loss_carryforward_jpy: Union[str, int, float, Decimal] = "0",
    ) -> Dict[str, Any]:
        """
        指定年度の税務区分別損益を集計し、2028年以降の「申告分離課税 (`SEPARATE_20315`) と
        総合課税 (`COMPREHENSIVE_MISC`) の損益通算禁止ルール」および3年間繰越控除を厳密適用する。
        """
        yr = int(tax_year)
        bucket = self._ensure_year_bucket(yr)
        prior_cf = max(Decimal("0"), _to_dec(prior_separate_loss_carryforward_jpy))

        sep_raw = bucket["separate_spot_pnl_jpy"]
        comp_raw = (
            bucket["comprehensive_spot_pnl_jpy"]
            + bucket["comprehensive_perp_defi_pnl_jpy"]
            + bucket["comprehensive_rebate_income_jpy"]
        )

        if yr <= 2027:
            # 2027年以前: 全取引が総合課税（雑所得）であり、GMO現物と海外Perp/DEXの内部通算が可能
            combined_2027 = sep_raw + comp_raw
            separate_taxable = Decimal("0")
            comprehensive_taxable = max(Decimal("0"), combined_2027)
            carryforward_loss_3yr = Decimal("0")
            prior_cf_used = Decimal("0")
            prior_cf_remaining = Decimal("0")
            cross_bucket_offset_blocked = False
            estimated_separate_tax = Decimal("0")
            estimated_max_comp_tax = (comprehensive_taxable * MAX_COMPREHENSIVE_TAX_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            # 2028年以降: SEPARATE_20315 (GMO現物) と COMPREHENSIVE_MISC (Hyperliquid/QANX/独立リベート) の損益通算は厳格禁止
            cross_bucket_offset_blocked = bool(
                (sep_raw > Decimal("0") and comp_raw < Decimal("0"))
                or (sep_raw < Decimal("0") and comp_raw > Decimal("0"))
            )

            if sep_raw > Decimal("0"):
                prior_cf_used = min(sep_raw, prior_cf)
                separate_taxable = sep_raw - prior_cf_used
                prior_cf_remaining = prior_cf - prior_cf_used
                carryforward_loss_3yr = Decimal("0")
            else:
                prior_cf_used = Decimal("0")
                separate_taxable = Decimal("0")
                prior_cf_remaining = prior_cf
                carryforward_loss_3yr = abs(sep_raw)
                if carryforward_loss_3yr > Decimal("0"):
                    self.separate_loss_carryforward_ledger[yr] = carryforward_loss_3yr

            # 総合課税バケットは赤字でも分離課税の黒字と相殺できず、翌年への繰越も不可 (0でフロア)
            comprehensive_taxable = max(Decimal("0"), comp_raw)

            estimated_separate_tax = (separate_taxable * SEPARATE_TAX_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            estimated_max_comp_tax = (comprehensive_taxable * MAX_COMPREHENSIVE_TAX_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        total_income_for_deduction_check = separate_taxable + comprehensive_taxable

        return {
            "tax_year": yr,
            "rebate_incorporation_mode": self.rebate_incorporation_mode,
            "separate_20315_raw_pnl_jpy": sep_raw,
            "comprehensive_misc_raw_pnl_jpy": comp_raw if yr >= 2028 else (sep_raw + comp_raw),
            "comprehensive_spot_pnl_jpy": bucket["comprehensive_spot_pnl_jpy"],
            "comprehensive_perp_defi_pnl_jpy": bucket["comprehensive_perp_defi_pnl_jpy"],
            "comprehensive_rebate_income_jpy": bucket["comprehensive_rebate_income_jpy"],
            "fee_fraction_refund_jpy": bucket["fee_fraction_refund_jpy"],
            "separate_20315_taxable_income_jpy": separate_taxable,
            "comprehensive_misc_taxable_income_jpy": comprehensive_taxable,
            "prior_carryforward_used_jpy": prior_cf_used,
            "prior_carryforward_remaining_jpy": prior_cf_remaining,
            "carryforward_loss_3yr_jpy": carryforward_loss_3yr,
            "cross_bucket_offset_prohibited": bool(yr >= 2028),
            "cross_bucket_offset_blocked": cross_bucket_offset_blocked,
            "estimated_separate_tax_jpy": estimated_separate_tax,
            "estimated_max_comprehensive_tax_jpy": estimated_max_comp_tax,
            "total_estimated_tax_jpy": estimated_separate_tax + estimated_max_comp_tax,
            "total_income_for_deduction_check_jpy": total_income_for_deduction_check,
            "basis_method_comparison": self.compute_basis_method_comparison(),
        }

    def record_daily_fee_fraction_refund(
        self,
        *,
        amount_jpy: Union[str, int, float, Decimal],
        timestamp_jst: Optional[Union[str, int, float, datetime]] = None,
        venue: str = "GMO_COIN",
        tax_regime_hint: Optional[str] = None,
        record_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        毎朝 06:00 JST の GMOコイン手数料端数返金 (`DAILY_FEE_FRACTION_REFUND`) を記録する。
        - 2027年以前: `COMPREHENSIVE_MISC`（総合課税・雑所得）バケットへ算入。
        - 2028年以降: 国内登録業者 (`GMO_COIN`) の手数料端数精算として、`SEPARATE_20315` ヒントまたは
          `rebate_incorporation_mode=True` の場合は `SEPARATE_20315` バケット、それ以外は `COMPREHENSIVE_MISC` へ算入。
        """
        dt_jst = _parse_jst(timestamp_jst)
        tax_year = int(dt_jst.year)
        venue_up = str(venue or "GMO_COIN").strip().upper()
        amt_d = _to_dec(amount_jpy)
        bucket = self._ensure_year_bucket(tax_year)
        bucket["fee_fraction_refund_jpy"] += amt_d

        is_dom = venue_up in DOMESTIC_REGISTERED_VENUES
        hint_up = str(tax_regime_hint or "").strip().upper()
        if tax_year >= 2028 and is_dom and (self.rebate_incorporation_mode or hint_up == "SEPARATE_20315"):
            tax_cat = TaxCategory.SEPARATE_20315
            bucket["separate_spot_pnl_jpy"] += amt_d
        else:
            tax_cat = TaxCategory.COMPREHENSIVE_MISC
            bucket["comprehensive_rebate_income_jpy"] += amt_d

        rec_id = record_id or f"refund-{len(self.records) + 1:06d}"
        rec = AuditedTradeRecord(
            record_id=rec_id,
            timestamp_jst=dt_jst.isoformat(timespec="seconds"),
            tax_year=tax_year,
            venue=venue_up,
            asset="JPY",
            side="REFUND",
            qty=Decimal("0"),
            price_jpy=Decimal("1"),
            gross_jpy=amt_d,
            fee_jpy=-amt_d,
            tax_category=tax_cat,
            realized_pnl_jpy=amt_d if tax_cat == TaxCategory.SEPARATE_20315 else Decimal("0"),
            rebate_income_jpy=amt_d if tax_cat == TaxCategory.COMPREHENSIVE_MISC else Decimal("0"),
            moving_unit_cost_after_jpy=Decimal("0"),
            cost_basis_jpy=Decimal("0"),
        )
        self.records.append(rec)
        return {
            "record_id": rec_id,
            "tax_year": tax_year,
            "venue": venue_up,
            "tax_category": tax_cat.value,
            "amount_jpy": amt_d,
        }

    @classmethod
    def load_and_audit_from_wal_db(
        cls,
        db_path: Union[str, Path],
        tax_year: Optional[int] = None,
        rebate_incorporation_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        `v2_state_wal.db` へ Read-Only WAL モード (`?mode=ro`, `PRAGMA query_only=ON;`) で接続し、
        `v_forensic_tax_ledger` ビューから全税務イベント (`SPOT_TRADE_BUY`, `SPOT_TRADE_CLOSE`,
        `MAKER_REBATE`, `TAKER_FEE`, `DAILY_FEE_FRACTION_REFUND`) を時系列順にリプレイして
        `Decimal` 精度で移動平均法・総平均法および2027総合課税 / 2028申告分離課税バケットを監査集計する。
        """
        auditor = cls(rebate_incorporation_mode=rebate_incorporation_mode)
        ro_conn = connect_forensic_readonly(db_path)
        try:
            rows = ro_conn.execute(
                """
                SELECT * FROM v_forensic_tax_ledger
                ORDER BY
                    created_ts_ms ASC,
                    timestamp_jst ASC,
                    CASE event_type
                        WHEN 'SPOT_TRADE_BUY' THEN 1
                        WHEN 'SPOT_TRADE_CLOSE' THEN 2
                        WHEN 'MAKER_REBATE' THEN 3
                        WHEN 'TAKER_FEE' THEN 4
                        WHEN 'DAILY_FEE_FRACTION_REFUND' THEN 5
                        ELSE 6
                    END ASC,
                    event_id ASC
                """
            ).fetchall()
            ledger_rows = [dict(r) for r in rows]
        finally:
            ro_conn.close()

        # `record_execution` が生成する `exec_open_<id>` / `exec_close_<id>` と `rebate_<id>` / `fee_<id>` のペア特定
        companion_fee_by_exec_id: Dict[str, Decimal] = {}
        paired_companion_event_ids: Set[str] = set()
        spot_exec_ids: Set[str] = set()

        for r in ledger_rows:
            ev_id = str(r.get("event_id", "")).strip()
            ev_type = str(r.get("event_type", "")).strip().upper()
            if ev_type in ("SPOT_TRADE_BUY", "SPOT_TRADE_CLOSE"):
                if ev_id.startswith("exec_open_"):
                    spot_exec_ids.add(ev_id[len("exec_open_") :])
                elif ev_id.startswith("exec_close_"):
                    spot_exec_ids.add(ev_id[len("exec_close_") :])

        for r in ledger_rows:
            ev_id = str(r.get("event_id", "")).strip()
            ev_type = str(r.get("event_type", "")).strip().upper()
            if ev_type == "MAKER_REBATE" and ev_id.startswith("rebate_"):
                ex_id = ev_id[len("rebate_") :]
                if ex_id in spot_exec_ids:
                    paired_companion_event_ids.add(ev_id)
                    companion_fee_by_exec_id[ex_id] = -abs(_to_dec(r.get("fee_or_rebate_jpy", 0)))
            elif ev_type == "TAKER_FEE" and ev_id.startswith("fee_"):
                ex_id = ev_id[len("fee_") :]
                if ex_id in spot_exec_ids:
                    paired_companion_event_ids.add(ev_id)
                    companion_fee_by_exec_id[ex_id] = abs(
                        _to_dec(r.get("cost_basis_jpy") or r.get("fee_or_rebate_jpy") or 0)
                    )

        event_type_counts: Dict[str, int] = {}
        current_year: Optional[int] = None

        for r in ledger_rows:
            ev_id = str(r.get("event_id", "")).strip()
            ev_type = str(r.get("event_type", "")).strip().upper()
            row_yr = int(r.get("tax_year") or _parse_jst(r.get("timestamp_jst")).year)
            event_type_counts[ev_type] = event_type_counts.get(ev_type, 0) + 1

            if current_year is None:
                current_year = row_yr
                auditor._ensure_year_bucket(current_year)
            elif row_yr > current_year:
                for yr_step in range(current_year + 1, row_yr + 1):
                    auditor.rollover_tax_year(yr_step)
                current_year = row_yr

            ex_reg = str(r.get("exchange_registration", "JP_FSA_REGISTERED_GMO")).strip().upper()
            venue = "GMO_COIN" if ("GMO" in ex_reg or "FSA" in ex_reg) else ex_reg
            sym = str(r.get("symbol", "BTC")).strip().upper()
            ts_jst = r.get("timestamp_jst")
            qty_d = _to_dec(r.get("size", 0))
            price_d = _to_dec(r.get("price_jpy", 0))
            gross_d = _to_dec(r.get("gross_proceeds_jpy", 0))
            cost_d = _to_dec(r.get("cost_basis_jpy", 0))
            fee_or_rebate_d = _to_dec(r.get("fee_or_rebate_jpy", 0))
            realized_d = _to_dec(r.get("realized_pnl_jpy", 0))

            if ev_type == "SPOT_TRADE_BUY":
                ex_id = ev_id[len("exec_open_") :] if ev_id.startswith("exec_open_") else ""
                if fee_or_rebate_d != Decimal("0"):
                    fee_jpy_val = -fee_or_rebate_d
                elif ex_id and ex_id in companion_fee_by_exec_id:
                    fee_jpy_val = companion_fee_by_exec_id[ex_id]
                else:
                    fee_jpy_val = Decimal("0")

                if qty_d > Decimal("0") and price_d > Decimal("0"):
                    auditor.record_spot_fill(
                        venue=venue,
                        asset=sym,
                        side="BUY",
                        qty=qty_d,
                        price_jpy=price_d,
                        fee_jpy=fee_jpy_val,
                        timestamp_jst=ts_jst,
                        record_id=ev_id,
                    )

            elif ev_type == "SPOT_TRADE_CLOSE":
                ex_id = ev_id[len("exec_close_") :] if ev_id.startswith("exec_close_") else ""
                if fee_or_rebate_d != Decimal("0"):
                    fee_jpy_val = -fee_or_rebate_d
                elif ex_id and ex_id in companion_fee_by_exec_id:
                    fee_jpy_val = companion_fee_by_exec_id[ex_id]
                else:
                    fee_jpy_val = Decimal("0")

                pool = auditor.get_pool(sym)
                if qty_d > Decimal("0") and price_d <= Decimal("0") and gross_d > Decimal("0"):
                    price_d = gross_d / qty_d

                if qty_d > Decimal("0") and price_d > Decimal("0"):
                    # もし単独の SPOT_TRADE_CLOSE イベントで先行する BUY プール数量が不足している場合は、
                    # イベントに記録された cost_basis_jpy (または gross - realized) から期首/先行取得原価を補完する
                    if pool.moving_qty < qty_d:
                        missing_qty = qty_d - pool.moving_qty
                        inferred_total_cost = cost_d if cost_d > Decimal("0") else max(Decimal("0"), gross_d - realized_d)
                        missing_cost = max(Decimal("0"), inferred_total_cost - pool.moving_cost_jpy)
                        pool.moving_qty += missing_qty
                        pool.moving_cost_jpy += missing_cost
                        pool.year_buy_qty += missing_qty
                        pool.year_buy_cost_jpy += missing_cost

                    auditor.record_spot_fill(
                        venue=venue,
                        asset=sym,
                        side="SELL",
                        qty=qty_d,
                        price_jpy=price_d,
                        fee_jpy=fee_jpy_val,
                        timestamp_jst=ts_jst,
                        record_id=ev_id,
                    )
                else:
                    # 数量を持たないサマリー型 SPOT_TRADE_CLOSE レコードの場合
                    bucket = auditor._ensure_year_bucket(row_yr)
                    is_dom = venue in DOMESTIC_REGISTERED_VENUES
                    if row_yr >= 2028 and is_dom:
                        bucket["separate_spot_pnl_jpy"] += realized_d
                    else:
                        bucket["comprehensive_spot_pnl_jpy"] += realized_d

            elif ev_type == "MAKER_REBATE":
                if ev_id in paired_companion_event_ids:
                    continue
                reb_amt = abs(fee_or_rebate_d if fee_or_rebate_d != Decimal("0") else realized_d)
                bucket = auditor._ensure_year_bucket(row_yr)
                is_dom = venue in DOMESTIC_REGISTERED_VENUES
                if auditor.rebate_incorporation_mode and row_yr >= 2028 and is_dom:
                    bucket["separate_spot_pnl_jpy"] += reb_amt
                else:
                    bucket["comprehensive_rebate_income_jpy"] += reb_amt

            elif ev_type == "TAKER_FEE":
                if ev_id in paired_companion_event_ids:
                    continue
                fee_amt = abs(cost_d if cost_d != Decimal("0") else (fee_or_rebate_d if fee_or_rebate_d != Decimal("0") else realized_d))
                bucket = auditor._ensure_year_bucket(row_yr)
                is_dom = venue in DOMESTIC_REGISTERED_VENUES
                if row_yr >= 2028 and is_dom:
                    bucket["separate_spot_pnl_jpy"] -= fee_amt
                else:
                    bucket["comprehensive_spot_pnl_jpy"] -= fee_amt

            elif ev_type == "DAILY_FEE_FRACTION_REFUND":
                refund_amt = realized_d if realized_d != Decimal("0") else fee_or_rebate_d
                auditor.record_daily_fee_fraction_refund(
                    amount_jpy=refund_amt,
                    timestamp_jst=ts_jst,
                    venue=venue,
                    tax_regime_hint=str(r.get("tax_regime", "")),
                    record_id=ev_id,
                )

        years_present = sorted(auditor._year_buckets.keys())
        if tax_year is not None and int(tax_year) not in auditor._year_buckets:
            auditor._ensure_year_bucket(int(tax_year))
            years_present = sorted(auditor._year_buckets.keys())

        summaries_by_year: Dict[int, Dict[str, Any]] = {}
        running_cf = Decimal("0")
        for yr in years_present:
            yr_sum = auditor.audit_year_summary(yr, prior_separate_loss_carryforward_jpy=running_cf)
            summaries_by_year[yr] = yr_sum
            if yr >= 2028:
                running_cf = yr_sum["prior_carryforward_remaining_jpy"] + yr_sum["carryforward_loss_3yr_jpy"]

        target_yr = int(tax_year) if tax_year is not None else (years_present[-1] if years_present else datetime.now(JST).year)
        target_summary = summaries_by_year.get(target_yr) or auditor.audit_year_summary(target_yr)

        return {
            "auditor": auditor,
            "db_path": str(db_path),
            "readonly_verified": True,
            "events_processed": len(ledger_rows),
            "event_type_counts": event_type_counts,
            "years_present": years_present,
            "tax_year": target_yr,
            "summary": target_summary,
            "summaries_by_year": summaries_by_year,
            "basis_method_comparison": auditor.compute_basis_method_comparison(),
        }

    def evaluate_2027_year_end_tax_harvesting(
        self,
        *,
        realized_pnl_2027_jpy: Union[str, int, float, Decimal],
        open_positions: Sequence[Dict[str, Any]],
        assumed_2027_marginal_tax_rate: Union[str, float, Decimal] = MAX_COMPREHENSIVE_TAX_RATE,
    ) -> Dict[str, Any]:
        """
        2027年12月（総合課税最終月）における特例タックス・ハーベスティング監査アラートを生成する：
          ① 年内確定利益 `> 0` かつ含み損ポジションがある場合：
             2027年の損失は2028年へ繰り越せないため、2027年12月31日23:59 JSTまでに含み損ポジションを
             損出し決済（利確との内部通算）することを推奨 (`RECOMMEND_2027_LOSS_HARVESTING`)。
          ② GMOコイン現物に大きな含み益がある場合：
             2028年1月1日00:00 JST以降まで利確を繰り延べることで、適用税率を最大55.945%（総合課税）から
             20.315%（申告分離課税）へ引き下げ可能 (`RECOMMEND_DEFER_GMO_GAIN_TO_2028`)。
        """
        realized_2027 = _to_dec(realized_pnl_2027_jpy)
        marginal_rate_2027 = _to_dec(assumed_2027_marginal_tax_rate)

        harvestable_loss_jpy = Decimal("0")
        loss_positions: List[Dict[str, Any]] = []
        deferrable_gmo_gain_jpy = Decimal("0")
        gmo_gain_positions: List[Dict[str, Any]] = []

        for pos in open_positions or []:
            venue_up = str(pos.get("venue", "GMO_COIN")).strip().upper()
            sym_up = str(pos.get("symbol", pos.get("asset", ""))).strip().upper()
            unrealized = _to_dec(pos.get("unrealized_pnl_jpy", 0))
            if unrealized == Decimal("0") and pos.get("size") and pos.get("entry_price") and pos.get("current_price"):
                sz = _to_dec(pos.get("size"))
                ep = _to_dec(pos.get("entry_price"))
                cp = _to_dec(pos.get("current_price"))
                unrealized = (cp - ep) * sz

            if unrealized < Decimal("0"):
                loss_abs = abs(unrealized)
                harvestable_loss_jpy += loss_abs
                loss_positions.append({"venue": venue_up, "symbol": sym_up, "unrealized_loss_jpy": loss_abs})
            elif unrealized > Decimal("0") and venue_up in DOMESTIC_REGISTERED_VENUES:
                deferrable_gmo_gain_jpy += unrealized
                gmo_gain_positions.append({"venue": venue_up, "symbol": sym_up, "unrealized_gain_jpy": unrealized})

        alerts: List[str] = []
        offsettable_loss_jpy = min(max(Decimal("0"), realized_2027), harvestable_loss_jpy)
        estimated_loss_harvest_tax_saving_jpy = (offsettable_loss_jpy * marginal_rate_2027).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if realized_2027 > Decimal("0") and harvestable_loss_jpy > Decimal("0"):
            alerts.append("RECOMMEND_2027_LOSS_HARVESTING")

        rate_diff = max(Decimal("0"), marginal_rate_2027 - SEPARATE_TAX_RATE)
        estimated_deferral_tax_saving_jpy = (deferrable_gmo_gain_jpy * rate_diff).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if deferrable_gmo_gain_jpy > Decimal("0"):
            alerts.append("RECOMMEND_DEFER_GMO_GAIN_TO_2028")

        return {
            "realized_pnl_2027_jpy": realized_2027,
            "harvestable_loss_jpy": harvestable_loss_jpy,
            "offsettable_loss_jpy": offsettable_loss_jpy,
            "estimated_loss_harvest_tax_saving_jpy": estimated_loss_harvest_tax_saving_jpy,
            "deferrable_gmo_gain_jpy": deferrable_gmo_gain_jpy,
            "estimated_deferral_tax_saving_jpy": estimated_deferral_tax_saving_jpy,
            "loss_positions": loss_positions,
            "gmo_gain_positions": gmo_gain_positions,
            "alerts": alerts,
        }


def load_and_audit_from_wal_db(
    db_path: Union[str, Path],
    tax_year: Optional[int] = None,
    rebate_incorporation_mode: bool = False,
) -> Dict[str, Any]:
    """
    `ForensicTaxAuditor.load_and_audit_from_wal_db` のモジュールレベル・ブリッジ関数。
    """
    return ForensicTaxAuditor.load_and_audit_from_wal_db(
        db_path=db_path,
        tax_year=tax_year,
        rebate_incorporation_mode=rebate_incorporation_mode,
    )

