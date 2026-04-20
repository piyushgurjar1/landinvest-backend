# gemini_models.py
from __future__ import annotations

from typing import Any, Optional, Literal
from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Type aliases
# ─────────────────────────────────────────────────────────────────────────────

UtilityStatus = Literal["confirmed", "not_available", "unknown"]
YesNoUnknown   = Literal["yes", "no", "unknown"]


# ─────────────────────────────────────────────────────────────────────────────
# Scalar coercers (kept from original)
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# StripMixin (kept from original)
# ─────────────────────────────────────────────────────────────────────────────

class StripMixin(BaseModel):
    @field_validator("*", mode="before")
    @classmethod
    def _strip(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Shared utility sub-models (kept from original — richer than simple UtilityDetail)
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1A — Parcel Identity, Ownership & Address
# Minimal fast call. Provides address + GPS to all parallel stages.
# ─────────────────────────────────────────────────────────────────────────────

class Stage1Identity(StripMixin):
    apn: str = ""
    county: str = ""
    state: str = ""

    street_address: Optional[str] = Field(
        default=None,
        description="Full mailing address: Street Number Street Name, City, State ZIP",
    )
    gps_coordinates: Optional[str] = Field(
        default=None,
        description='Decimal degrees: "lat, lng" e.g. "35.1234, -114.5678"',
    )

    google_maps_link: Optional[str] = None
    google_satellite_link: Optional[str] = None
    google_street_view_link: Optional[str] = None
    parcel_boundary_map_link: Optional[str] = None

    acreage: Optional[float] = None
    sq_ft: Optional[int] = None
    legal_description: Optional[str] = None

    owner_name: Optional[str] = None
    county_assessed_value: Optional[int] = Field(
        default=None, description="Numeric dollar amount only"
    )
    assessed_year: Optional[str] = None
    tax_status: Optional[str] = Field(
        default=None, description="Exactly: Current | Delinquent | Unknown"
    )
    liens_beyond_tax: Optional[str] = Field(
        default=None,
        description="Description of any non-tax liens, or 'None identified'",
    )

    sources_used_stage1a: list[str] = Field(default_factory=list)

    @field_validator("sq_ft", "county_assessed_value", mode="before")
    @classmethod
    def _int_fields(cls, v):
        return _to_int(v)

    @field_validator("acreage", mode="before")
    @classmethod
    def _float_fields(cls, v):
        return _to_float(v)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1B — Zoning & All Permitted Uses
# Runs in PARALLEL with 1C, 1D, Stage 2 after 1A completes.
# ─────────────────────────────────────────────────────────────────────────────

class Stage1Zoning(StripMixin):
    zoning_code: Optional[str] = Field(default=None, description="e.g. AR-1, RU, AG")
    zoning_description: Optional[str] = Field(
        default=None, description="Full name e.g. Agricultural Residential"
    )
    allowed_uses: Optional[str] = Field(
        default=None, description="Free-text summary of all permitted uses"
    )
    minimum_lot_size: Optional[str] = None
    setbacks: Optional[str] = Field(
        default=None, description="Front/rear/side setback requirements"
    )

    # Permitted uses — yes | no | unknown
    buildable: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    residential_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    mobile_homes_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    rv_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    tiny_homes_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    camping_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    off_grid_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    commercial_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    agricultural_allowed: Optional[str] = Field(default="unknown", description="yes | no | unknown")

    # HOA
    hoa_present: Optional[str] = Field(default="unknown", description="yes | no | unknown")
    hoa_fees: Optional[str] = None
    hoa_name: Optional[str] = None

    # Contact / source
    planning_dept_phone: Optional[str] = None
    zoning_source_url: Optional[str] = Field(
        default=None, description="Direct URL to zoning ordinance or county zoning map"
    )

    sources_used_stage1b: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1C — Utilities & Infrastructure
# Runs in PARALLEL with 1B, 1D, Stage 2 after 1A completes.
# Extends original Stage1Utilities with internet + road fields.
# ─────────────────────────────────────────────────────────────────────────────

class Stage1Utilities(StripMixin):
    # Core utilities (uses richer UtilityEvidence — kept from original)
    electricity: UtilityEvidence = Field(default_factory=UtilityEvidence)
    water: UtilityEvidence = Field(default_factory=UtilityEvidence)
    sewer: UtilityEvidence = Field(default_factory=UtilityEvidence)
    gas: UtilityEvidence = Field(default_factory=UtilityEvidence)

    # Requirements
    well_required: BinaryEvidence = Field(default_factory=BinaryEvidence)
    septic_required: BinaryEvidence = Field(default_factory=BinaryEvidence)
    utility_at_street: BinaryEvidence = Field(default_factory=BinaryEvidence)
    utility_cost_estimate: Optional[str] = Field(
        default=None, description="Estimated cost to bring utilities to site"
    )

    # Internet & telecom (NEW)
    internet_provider: Optional[str] = None
    internet_type: Optional[str] = Field(
        default=None,
        description="fiber | cable | DSL | fixed wireless | satellite | none",
    )
    cell_coverage: Optional[str] = Field(default=None, description="yes | no | unknown")
    cell_carriers: Optional[str] = Field(
        default=None, description="Carriers with coverage e.g. Verizon, T-Mobile"
    )

    # Road & access infrastructure (NEW — moved here from Stage1Core)
    road_type: Optional[str] = Field(default=None, description="paved | gravel | dirt | none")
    road_name: Optional[str] = None
    road_condition: Optional[str] = Field(default=None, description="good | fair | poor | unknown")
    road_maintained_by: Optional[str] = Field(
        default=None, description="county | city | private | unknown"
    )
    year_round_access: Optional[str] = Field(default=None, description="yes | no | unknown")
    distance_to_paved_road: Optional[str] = Field(
        default=None, description="Miles to nearest paved road if unpaved"
    )

    # Legal access & easements
    legal_access_status: Optional[str] = Field(
        default=None,
        description="Confirmed legal access | Easement required | Landlocked | Unknown"
    )
    road_description: Optional[str] = Field(
        default=None,
        description="Short description of road access situation, e.g. 'Dirt road off County Rd 42, maintained by county'"
    )
    easements: Optional[str] = Field(
        default=None,
        description="Known easements on or needed for the parcel, or null if none found"
    )
    landlocked: Optional[str] = Field(
        default=None,
        description="yes | no | unknown — whether the parcel has no direct road access"
    )

    # RENAMED: sources_used_stage1_utilities → sources_used_stage1c
    sources_used_stage1c: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1D — Environmental Risks, Growth & Nearby Context
# Runs in PARALLEL with 1B, 1C, Stage 2 after 1A completes.
# Combines flood/fire/terrain/growth/proximity fields from old Stage1Core.
# ─────────────────────────────────────────────────────────────────────────────

class Stage1Environment(StripMixin):
    # Flood
    flood_zone: Optional[bool] = Field(
        default=None, description="True if any part of parcel is in a FEMA flood zone"
    )
    flood_zone_designation: Optional[str] = Field(
        default=None, description="e.g. Zone AE, Zone X, Zone A"
    )
    flood_map_url: Optional[str] = None

    # Wetlands
    wetlands_risk: Optional[bool] = None
    wetlands_notes: Optional[str] = None

    # Fire
    fire_risk: Optional[str] = Field(
        default=None,
        description="Low | Moderate | High | Very High | Extreme | Unknown",
    )
    fire_risk_source: Optional[str] = None
    fire_risk_url: Optional[str] = None

    # Other hazards
    landslide_risk: Optional[str] = Field(
        default=None, description="Low | Moderate | High | Unknown"
    )

    # Terrain
    terrain_description: Optional[str] = Field(
        default=None,
        description="flat | gently rolling | hilly | mountainous | desert | etc.",
    )
    slope_classification: Optional[str] = Field(
        default=None,
        description="Flat (0-2%) | Gentle (2-8%) | Moderate (8-15%) | Steep (15%+)",
    )
    washes_or_arroyos: Optional[bool] = Field(
        default=None, description="Drainage washes visible on satellite"
    )
    soil_suitability: Optional[str] = Field(
        default=None, description="Good | Fair | Poor | Unknown — for building/septic"
    )
    protected_land_status: Optional[str] = None
    environmental_restrictions: Optional[str] = None

    # Nearby context — distances
    nearest_city_name: Optional[str] = None
    distance_to_nearest_city: Optional[str] = None
    distance_to_highway: Optional[str] = None
    distance_to_lake_or_water: Optional[str] = None
    distance_to_major_attraction: Optional[str] = None
    major_attraction_name: Optional[str] = None

    # Nearby context — observations
    nearby_parcel_usage: Optional[str] = None
    nearby_housing_development: Optional[str] = Field(
        default=None, description="yes | no | unknown"
    )
    nearby_structures: Optional[str] = Field(default=None, description="yes | no | unknown")
    power_lines_visible: Optional[str] = Field(default=None, description="yes | no | unknown")

    # Growth
    population_growth_trend: Optional[str] = Field(
        default=None, description="Growing | Stable | Declining | Unknown"
    )
    county_growth_rate: Optional[str] = Field(default=None, description="e.g. 3.2%/year")
    building_permit_growth: Optional[str] = Field(
        default=None, description="Increasing | Stable | Declining | Unknown"
    )
    growth_notes: Optional[str] = None

    sources_used_stage1d: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — Comparable Sales & Active Listings (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

class RawSoldComp(StripMixin):
    apn: Optional[str] = None
    address: Optional[str] = None
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
    source: Optional[str] = None
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
    address: Optional[str] = None
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