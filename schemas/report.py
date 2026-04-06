# schemas/report.py
"""
Pydantic models for final parcel due diligence report output.

Used as the final validated response contract after deterministic synthesis
in gemini_service.py.

Notes:
- This file is still required.
- gemini_models.py should contain only intermediate Gemini extraction schemas.
- This file defines the final API/UI-facing ParcelReport model.
"""

from __future__ import annotations

from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


# ─────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────

def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except (ValueError, TypeError):
        return None


def _to_int_zero(v: Any) -> int:
    result = _to_int(v)
    return result if result is not None else 0


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _ensure_str_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out = []
        for item in v:
            if item is None:
                continue
            s = str(item).strip()
            if s:
                out.append(s)
        return out
    s = str(v).strip()
    return [s] if s else []


class CleanBaseModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    @field_validator("*", mode="before")
    @classmethod
    def strip_strings(cls, v: Any):
        if isinstance(v, str):
            return v.strip()
        return v


# ─────────────────────────────────────────────────────────────
# Homepage Summary
# ─────────────────────────────────────────────────────────────

class HomepageSummary(CleanBaseModel):
    parcel_size_acres: Optional[float] = Field(default=None)
    parcel_size_sqft: Optional[int] = Field(default=None)
    satellite_map_link: Optional[str] = Field(default=None)
    all_liens: Optional[str] = Field(default=None)
    assessed_value: Optional[int] = Field(default=None)
    estimated_market_value_mid: Optional[int] = Field(default=None, description="Mid MMV")
    avg_sold_price_per_acre: Optional[int] = Field(default=None)
    avg_active_price_per_acre: Optional[int] = Field(default=None)
    recommended_bid_ceiling: Optional[int] = Field(default=None, description="max_bid_ceiling from auction section")
    suggested_resale_price: Optional[int] = Field(default=None, description="0.90 × MMV")
    deal_score: Optional[int] = Field(default=None)
    risk_score: Optional[int] = Field(default=None)
    verdict: Optional[str] = Field(default=None)

    @field_validator(
        "parcel_size_sqft", "assessed_value", "estimated_market_value_mid",
        "avg_sold_price_per_acre", "avg_active_price_per_acre",
        "recommended_bid_ceiling", "suggested_resale_price",
        "deal_score", "risk_score",
        mode="before"
    )
    @classmethod
    def round_to_int(cls, v: Any) -> Optional[int]:
        return _to_int(v)

    @field_validator("parcel_size_acres", mode="before")
    @classmethod
    def parse_acres(cls, v: Any) -> Optional[float]:
        return _to_float(v)


# ─────────────────────────────────────────────────────────────
# Comp models
# ─────────────────────────────────────────────────────────────

class SoldComp(CleanBaseModel):
    acreage: float = Field(description="Acreage of the comp parcel")
    sold_price: int = Field(description="Final sold price in USD")
    sold_date: Optional[str] = Field(default=None, description="Date of sale e.g. 2025-08-15")
    distance_or_location: Optional[str] = Field(default=None, description="Distance or rough location from subject parcel")
    price_per_acre: int = Field(description="Computed price per acre")
    access_notes: Optional[str] = Field(default=None)
    terrain_notes: Optional[str] = Field(default=None)
    zoning: Optional[str] = Field(default=None)
    source_url: Optional[str] = Field(default=None)

    @field_validator("acreage", mode="before")
    @classmethod
    def parse_acreage(cls, v: Any) -> float:
        return _to_float(v) or 0.0

    @field_validator("sold_price", "price_per_acre", mode="before")
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)


class ActiveListing(CleanBaseModel):
    acreage: float = Field(description="Acreage of the listing")
    listing_price: int = Field(description="Current asking price in USD")
    price_per_acre: int = Field(description="Computed price per acre")
    days_on_market: Optional[int] = Field(default=0)
    terrain_and_access_notes: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    source_url: Optional[str] = Field(default=None)

    @field_validator("acreage", mode="before")
    @classmethod
    def parse_acreage(cls, v: Any) -> float:
        return _to_float(v) or 0.0

    @field_validator("listing_price", "price_per_acre", mode="before")
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)

    @field_validator("days_on_market", mode="before")
    @classmethod
    def round_dom(cls, v: Any) -> int:
        return _to_int_zero(v)


# ─────────────────────────────────────────────────────────────
# Section models
# ─────────────────────────────────────────────────────────────

class QuickSummary(CleanBaseModel):
    verdict: str = Field(description="One of: STRONG BUY, BUY, HOLD, PASS")
    bid_recommendation: Optional[str] = Field(
        default=None,
        description="e.g. 'Recommended Max Bid: $12,857 | Resale: $18,000 | Profit: $5,143 | STRONG BUY'"
    )
    summary: str = Field(description="2-3 sentence executive summary")
    key_strengths: list[str] = Field(default_factory=list)
    key_concerns: list[str] = Field(default_factory=list)

    @field_validator("key_strengths", "key_concerns", mode="before")
    @classmethod
    def ensure_lists(cls, v: Any) -> list[str]:
        return _ensure_str_list(v)


class BasicParcelInfo(CleanBaseModel):
    apn: str
    county: str
    state: str
    acreage: float
    sq_ft: int
    street_address: Optional[str] = None
    gps_coordinates: Optional[str] = None
    google_maps_link: Optional[str] = None
    google_satellite_link: Optional[str] = None
    google_street_view_link: Optional[str] = Field(
        default=None,
        description="https://maps.google.com/?q=LAT,LNG&layer=c"
    )
    parcel_boundary_map_link: Optional[str] = Field(
        default=None,
        description="County GIS or Regrid parcel boundary URL"
    )
    legal_description: Optional[str] = None
    county_assessed_value: Optional[int] = None
    assessed_year: Optional[str] = None
    owner_name: Optional[str] = None
    tax_status: Optional[str] = Field(
        default=None,
        description="Current | Delinquent | Unknown"
    )
    liens_beyond_tax: Optional[str] = Field(
        default=None,
        description="None identified, or list of liens found"
    )

    @field_validator("acreage", mode="before")
    @classmethod
    def parse_acreage(cls, v: Any) -> float:
        return _to_float(v) or 0.0

    @field_validator("sq_ft", mode="before")
    @classmethod
    def round_sq_ft(cls, v: Any) -> int:
        return _to_int_zero(v)

    @field_validator("county_assessed_value", mode="before")
    @classmethod
    def round_assessed(cls, v: Any) -> Optional[int]:
        return _to_int(v)


class ZoningInfo(CleanBaseModel):
    zoning_code: Optional[str] = None
    zoning_description: Optional[str] = None
    allowed_uses: Optional[str] = None
    minimum_lot_size: Optional[str] = None
    buildable: Optional[bool] = None
    residential_allowed: Optional[bool] = None
    rv_allowed: Optional[bool] = None
    mobile_homes_allowed: Optional[bool] = None
    tiny_homes_allowed: Optional[bool] = None
    camping_allowed: Optional[bool] = None
    off_grid_allowed: Optional[bool] = None
    hoa_present: Optional[bool] = Field(default=False)
    hoa_fees: Optional[str] = None


class UtilityInfo(CleanBaseModel):
    electricity_available: Optional[bool] = None
    water_available: Optional[bool] = None
    sewer_available: Optional[bool] = None
    septic_required: Optional[bool] = None
    well_required: Optional[bool] = None
    gas_available: Optional[bool] = None
    utility_at_street: Optional[bool] = None
    utility_cost_estimate: Optional[str] = None


class AccessAndLocation(CleanBaseModel):
    road_type: Optional[str] = None
    road_description: Optional[str] = None
    legal_access_status: Optional[str] = None
    easements: Optional[str] = None
    landlocked: Optional[bool] = Field(default=False)
    distance_to_highway: Optional[str] = None
    nearest_city: Optional[str] = None
    distance_to_nearest_city: Optional[str] = None
    distance_to_lake_or_water: Optional[str] = None
    distance_to_major_attraction: Optional[str] = None
    nearby_structures: Optional[str] = None
    nearby_housing_development: Optional[str] = None
    power_lines_visible: Optional[bool] = Field(default=False)
    population_growth_trend: Optional[str] = None
    county_growth_rate: Optional[str] = None
    building_permit_growth: Optional[str] = None
    zoning: ZoningInfo = Field(default_factory=ZoningInfo)
    utilities: UtilityInfo = Field(default_factory=UtilityInfo)


class TerrainOverview(CleanBaseModel):
    terrain_description: Optional[str] = None
    slope_classification: Optional[str] = None
    washes_or_arroyos: Optional[str] = None
    nearby_parcel_usage: Optional[str] = None
    flood_zone: Optional[bool] = Field(default=False)
    flood_zone_designation: Optional[str] = None
    wetlands_risk: Optional[bool] = Field(default=False)
    landslide_risk: Optional[str] = None
    fire_risk: Optional[str] = None
    soil_suitability: Optional[str] = None
    protected_land_status: Optional[str] = None
    environmental_restrictions: Optional[str] = None


class EstimatedMarketValue(CleanBaseModel):
    avg_sold_price_per_acre: Optional[int] = Field(default=None)
    low_price_per_acre: int
    mid_price_per_acre: int
    high_price_per_acre: int
    low_estimated_value: int
    mid_estimated_value: int
    high_estimated_value: int
    valuation_notes: Optional[str] = None

    @field_validator("avg_sold_price_per_acre", mode="before")
    @classmethod
    def round_avg(cls, v: Any) -> Optional[int]:
        return _to_int(v)

    @field_validator(
        "low_price_per_acre", "mid_price_per_acre", "high_price_per_acre",
        "low_estimated_value", "mid_estimated_value", "high_estimated_value",
        mode="before"
    )
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)


class EducationalOfferRange(CleanBaseModel):
    explanation: str
    low_offer: int
    high_offer: int
    discount_percentage: str = Field(description="e.g. '35-60% below market'")

    @field_validator("low_offer", "high_offer", mode="before")
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)


class ResalePriceRange(CleanBaseModel):
    conservative_resale: int = Field(description="80% of MMV — faster sale")
    mid_resale: int = Field(description="90% of MMV — standard listing")
    aggressive_resale: int = Field(description="95% of MMV — top of market")
    suggested_resale_price: int = Field(
        description="Suggested sell price = (1 - 0.10) × MMV = 0.90 × MMV"
    )
    resale_timeline: Optional[str] = Field(
        default=None,
        description="e.g. '90-180 days' based on market classification"
    )
    resale_formula_note: Optional[str] = Field(
        default=None,
        description="Conservative=80% MMV | Mid/Suggested=90% MMV | Aggressive=95% MMV"
    )
    recommended_tier_explanation: Optional[str] = None

    @field_validator(
        "conservative_resale", "mid_resale", "aggressive_resale", "suggested_resale_price",
        mode="before"
    )
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)

    @model_validator(mode="before")
    @classmethod
    def alias_typical_resale(cls, values: Any) -> Any:
        if isinstance(values, dict):
            if "typical_resale" in values and "mid_resale" not in values:
                values["mid_resale"] = values.pop("typical_resale")
        return values


class AuctionBidCeiling(CleanBaseModel):
    risk_tier: Optional[str] = Field(
        default=None,
        description="Very Low Risk | Moderate Risk | High Risk | Extreme Risk"
    )
    recovery_percentage: Optional[str] = Field(default=None)
    low_bid_threshold: int = Field(description="max_bid × 0.85 — conservative buffer")
    mid_bid_threshold: int = Field(description="max_bid × 0.925 — balanced")
    max_bid_ceiling: int = Field(description="(0.90/1.40) × MMV — HARD STOP")
    suggested_resale_price: Optional[int] = Field(
        default=None,
        description="0.90 × MMV — echoed here for UI display"
    )
    expected_profit: Optional[int] = Field(
        default=None,
        description="suggested_resale_price - max_bid_ceiling"
    )
    profit_margin_pct: Optional[str] = Field(
        default=None,
        description="(expected_profit / max_bid_ceiling × 100) e.g. '40.0%'"
    )
    bid_formula_note: Optional[str] = Field(
        default=None,
        description="MAX_BID = (0.90/1.40) × MMV | DR=10% | PM=40%"
    )
    risk_factors: list[str] = Field(default_factory=list)

    @field_validator(
        "low_bid_threshold", "mid_bid_threshold", "max_bid_ceiling",
        mode="before"
    )
    @classmethod
    def round_to_int(cls, v: Any) -> int:
        return _to_int_zero(v)

    @field_validator("suggested_resale_price", "expected_profit", mode="before")
    @classmethod
    def round_optional_int(cls, v: Any) -> Optional[int]:
        return _to_int(v)

    @field_validator("risk_factors", mode="before")
    @classmethod
    def ensure_risk_factors(cls, v: Any) -> list[str]:
        return _ensure_str_list(v)


class DaysOnMarket(CleanBaseModel):
    market_classification: str = Field(description="Fast | Normal | Slow | Very Slow | Unknown")
    estimated_dom_range: Optional[str] = None
    median_dom: Optional[int] = None
    inventory_count: Optional[int] = None
    land_sales_last_6_months: Optional[int] = None
    land_sales_last_12_months: Optional[int] = None
    active_listings_count: Optional[int] = None
    zip_turnover_rate_6mo: Optional[str] = None
    zip_turnover_rate_12mo: Optional[str] = None
    sales_velocity: Optional[str] = None
    sold_to_active_ratio: Optional[str] = None

    @field_validator(
        "median_dom", "inventory_count", "land_sales_last_6_months",
        "land_sales_last_12_months", "active_listings_count",
        mode="before"
    )
    @classmethod
    def round_to_int(cls, v: Any) -> Optional[int]:
        return _to_int(v)


class DealScore(CleanBaseModel):
    score: int = Field(description="Deal score 0-100, higher is better")
    explanation: str
    scoring_factors: list[str] = Field(default_factory=list)

    @field_validator("score", mode="before")
    @classmethod
    def round_score(cls, v: Any) -> int:
        return _to_int_zero(v)

    @field_validator("scoring_factors", mode="before")
    @classmethod
    def flatten_scoring_factors(cls, v: Any) -> list[str]:
        if not isinstance(v, list):
            return []
        result = []
        for item in v:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    result.append(s)
            elif isinstance(item, dict):
                factor = item.get("factor", item.get("name", item.get("category", "")))
                score = item.get("score", item.get("points", item.get("value", "")))
                notes = item.get("notes", item.get("description", item.get("detail", "")))
                parts = []
                if factor:
                    parts.append(str(factor))
                if notes:
                    parts.append(str(notes))
                if score not in (None, ""):
                    parts.append(f"({score})")
                text = " — ".join(parts) if parts else str(item)
                text = text.strip()
                if text:
                    result.append(text)
            elif item is not None:
                s = str(item).strip()
                if s:
                    result.append(s)
        return result


class RiskScore(CleanBaseModel):
    score: int = Field(description="Risk score 0-100, LOWER is better (less risk)")
    explanation: str
    risk_factors_applied: list[str] = Field(
        default_factory=list,
        description="List of risks that contributed to the score e.g. 'Flood zone: +20'"
    )

    @field_validator("score", mode="before")
    @classmethod
    def round_score(cls, v: Any) -> int:
        return _to_int_zero(v)

    @field_validator("risk_factors_applied", mode="before")
    @classmethod
    def ensure_risk_list(cls, v: Any) -> list[str]:
        return _ensure_str_list(v)





# ─────────────────────────────────────────────────────────────
# Root model
# ─────────────────────────────────────────────────────────────

class ParcelReport(CleanBaseModel):
    homepage_summary: Optional[HomepageSummary] = Field(
        default=None,
        description="Core signals for main APN load screen"
    )
    quick_summary: QuickSummary
    basic_parcel_info: BasicParcelInfo
    access_and_location: AccessAndLocation
    terrain_overview: TerrainOverview
    sold_comps: list[SoldComp] = Field(default_factory=list)
    active_listings: list[ActiveListing] = Field(default_factory=list)
    estimated_market_value: EstimatedMarketValue
    educational_offer_range: EducationalOfferRange
    resale_price_range: ResalePriceRange
    auction_bid_ceiling: AuctionBidCeiling
    days_on_market: DaysOnMarket
    deal_score: DealScore
    risk_score: Optional[RiskScore] = Field(
        default=None,
        description="Separate risk score 0-100, lower = less risky"
    )

    red_flags: list[str] = Field(default_factory=list)
    next_learning_step: Optional[str] = None
    sources_checked: list[str] = Field(default_factory=list)
    compliance_disclaimer: Optional[str] = Field(
        default=(
            "This report is for educational purposes only. It uses public information that can change at any time. "
            "This is not financial, legal, or tax advice and is not a recommendation to buy, sell, or offer any amount "
            "on this or any other property."
        )
    )

    @field_validator("red_flags", "sources_checked", mode="before")
    @classmethod
    def ensure_root_lists(cls, v: Any) -> list[str]:
        return _ensure_str_list(v)
