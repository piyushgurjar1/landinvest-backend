# gemini_models.py
from __future__ import annotations

from typing import Any, Optional, Literal
from pydantic import BaseModel, Field, field_validator


UtilityStatus = Literal["confirmed", "not_available", "unknown"]
YesNoUnknown = Literal["yes", "no", "unknown"]


def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


class StripMixin(BaseModel):
    @field_validator("*", mode="before")
    @classmethod
    def _strip(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v


class Stage1Core(StripMixin):
    apn: str = ""
    county: str = ""
    state: str = ""

    street_address: Optional[str] = None
    gps_coordinates: Optional[str] = None

    google_maps_link: Optional[str] = None
    google_satellite_link: Optional[str] = None
    google_street_view_link: Optional[str] = None
    parcel_boundary_map_link: Optional[str] = None

    acreage: Optional[float] = None
    sq_ft: Optional[int] = None

    legal_description: Optional[str] = None
    county_assessed_value: Optional[int] = None
    assessed_year: Optional[str] = None
    owner_name: Optional[str] = None
    tax_status: Optional[str] = None
    liens_beyond_tax: Optional[str] = None

    distance_to_nearest_city: Optional[str] = None
    nearest_city_name: Optional[str] = None
    distance_to_highway: Optional[str] = None
    distance_to_lake_or_water: Optional[str] = None
    distance_to_major_attraction: Optional[str] = None
    nearby_housing_development: Optional[str] = None

    population_growth_trend: Optional[str] = None
    county_growth_rate: Optional[str] = None
    building_permit_growth: Optional[str] = None

    road_type: Optional[str] = None
    legal_access_status: Optional[str] = None
    road_description: Optional[str] = None
    easements: Optional[str] = None
    landlocked: Optional[bool] = None
    power_lines_visible: Optional[bool] = None
    nearby_structures: Optional[str] = None

    zoning_code: Optional[str] = None
    zoning_description: Optional[str] = None
    buildable: Optional[bool] = None
    minimum_lot_size: Optional[str] = None
    residential_allowed: Optional[bool] = None
    mobile_homes_allowed: Optional[bool] = None
    rv_allowed: Optional[bool] = None
    tiny_homes_allowed: Optional[bool] = None
    camping_allowed: Optional[bool] = None
    off_grid_allowed: Optional[bool] = None
    hoa_present: Optional[bool] = None
    hoa_fees: Optional[str] = None
    allowed_uses: Optional[str] = None

    flood_zone: Optional[bool] = None
    flood_zone_designation: Optional[str] = None
    wetlands_risk: Optional[bool] = None
    terrain_description: Optional[str] = None
    slope_classification: Optional[str] = None
    washes_or_arroyos: Optional[str] = None
    landslide_risk: Optional[str] = None
    fire_risk: Optional[str] = None
    soil_suitability: Optional[str] = None
    protected_land_status: Optional[str] = None
    environmental_restrictions: Optional[str] = None
    nearby_parcel_usage: Optional[str] = None

    sources_used_stage1_core: list[str] = Field(default_factory=list)

    @field_validator("sq_ft", "county_assessed_value", mode="before")
    @classmethod
    def _int_fields(cls, v):
        return _to_int(v)

    @field_validator("acreage", mode="before")
    @classmethod
    def _float_fields(cls, v):
        return _to_float(v)


class UtilityEvidence(StripMixin):
    status: UtilityStatus = "unknown"
    provider_name: Optional[str] = None
    basis: Optional[str] = None
    evidence_note: Optional[str] = None
    evidence_url: Optional[str] = None
    distance_to_service: Optional[str] = None


class BinaryEvidence(StripMixin):
    status: YesNoUnknown = "unknown"
    evidence_note: Optional[str] = None
    evidence_url: Optional[str] = None


class Stage1Utilities(StripMixin):
    electricity: UtilityEvidence = Field(default_factory=UtilityEvidence)
    water: UtilityEvidence = Field(default_factory=UtilityEvidence)
    sewer: UtilityEvidence = Field(default_factory=UtilityEvidence)
    gas: UtilityEvidence = Field(default_factory=UtilityEvidence)

    well_required: BinaryEvidence = Field(default_factory=BinaryEvidence)
    septic_required: BinaryEvidence = Field(default_factory=BinaryEvidence)
    utility_at_street: BinaryEvidence = Field(default_factory=BinaryEvidence)

    utility_cost_estimate: Optional[str] = None
    sources_used_stage1_utilities: list[str] = Field(default_factory=list)


class RawSoldComp(StripMixin):
    apn: Optional[str] = None
    sold_price: int = 0
    price_per_acre: int = 0
    acreage: float = 0.0
    distance_or_location: Optional[str] = None
    sold_date: Optional[str] = None
    days_on_market: Optional[int] = None
    access_notes: Optional[str] = None
    terrain_notes: Optional[str] = None
    zoning: Optional[str] = None
    has_structures: bool = False
    source_url: str = ""

    @field_validator("sold_price", "price_per_acre", "days_on_market", mode="before")
    @classmethod
    def _int_fields(cls, v):
        return _to_int(v) or 0

    @field_validator("acreage", mode="before")
    @classmethod
    def _float_fields(cls, v):
        return _to_float(v) or 0.0

    @field_validator("source_url", mode="before")
    @classmethod
    def _url_required(cls, v):
        return (v or "").strip()


class RawActiveListing(StripMixin):
    apn: Optional[str] = None
    listing_price: int = 0
    price_per_acre: int = 0
    acreage: float = 0.0
    days_on_market: Optional[int] = None
    terrain_and_access_notes: Optional[str] = None
    source: Optional[str] = None
    source_url: str = ""

    @field_validator("listing_price", "price_per_acre", "days_on_market", mode="before")
    @classmethod
    def _int_fields(cls, v):
        return _to_int(v) or 0

    @field_validator("acreage", mode="before")
    @classmethod
    def _float_fields(cls, v):
        return _to_float(v) or 0.0

    @field_validator("source_url", mode="before")
    @classmethod
    def _url_required(cls, v):
        return (v or "").strip()


class Stage2Raw(StripMixin):
    raw_sold_comps: list[RawSoldComp] = Field(default_factory=list)
    raw_active_listings: list[RawActiveListing] = Field(default_factory=list)
    sources_used_stage2: list[str] = Field(default_factory=list)