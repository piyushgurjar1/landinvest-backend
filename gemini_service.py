# gemini_service.py
from __future__ import annotations

import json
import asyncio
import random
import requests
import time
import logging
import html as _html_module
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any, Optional
from urllib.parse import urlparse
from html import unescape as html_unescape
from typing import Any
import requests
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, quote_plus, unquote

from schemas.report import ParcelReport

from gemini_models import (
    Stage1Identity,
    Stage1Zoning,
    Stage1Utilities,
    Stage1Environment,
    Stage2Raw,
    RawSoldComp,
    RawActiveListing,
)
from gemini_prompts import (
    STAGE1A_IDENTITY_PROMPT,
    STAGE1B_ZONING_PROMPT,
    STAGE1C_UTILITIES_PROMPT,
    STAGE1D_ENVIRONMENT_PROMPT,
    STAGE2_PROMPT,
    THINKING_STAGE1A,
    THINKING_STAGE1B,
    THINKING_STAGE1C,
    THINKING_STAGE1D,
    THINKING_STAGE2,
    generate_structured,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "close",
}

session = requests.Session()
session.headers.update(HEADERS)
# ─────────────────────────────────────────────────────────────────────────────
# Scalar helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except Exception:
        return None


def _to_int_zero(v: Any) -> int:
    return _to_int(v) or 0


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _norm_text(v: Any) -> str:
    return str(v or "").strip().lower()


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _maybe_percent(num: Optional[float]) -> Optional[str]:
    if num is None:
        return None
    return f"{num:.1f}%"


def _money(n: Optional[float]) -> str:
    if n is None:
        return "$0"
    return f"${int(round(n)):,.0f}"


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    fmts = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%Y/%m/%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def _median_int(values: list[float | int]) -> int:
    values = [float(v) for v in values if v is not None]
    if not values:
        return 0
    return int(round(median(values)))


def _avg_int(values: list[float | int]) -> int:
    values = [float(v) for v in values if v is not None]
    if not values:
        return 0
    return int(round(sum(values) / len(values)))


def _relative_diff_pct(a: float, b: float) -> float:
    if a <= 0 and b <= 0:
        return 0.0
    denom = max((abs(a) + abs(b)) / 2.0, 1.0)
    return abs(a - b) / denom * 100.0


def _build_maps_links(gps: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    gps = _clean_str(gps)
    if not gps or "," not in gps:
        return None, None, None
    q = gps.replace(" ", "")
    return (
        f"https://maps.google.com/?q={q}",
        f"https://maps.google.com/?q={q}&t=k",
        f"https://maps.google.com/?q={q}&layer=c",
    )


def _build_baseline_context(parcel_info: Any) -> str:
    if not parcel_info:
        return "- None"

    lines = []
    fields = [
        ("address", "Address"),
    ]
    for attr, label in fields:
        val = getattr(parcel_info, attr, None)
        if val not in (None, ""):
            lines.append(f"- {label}: {val}")

    lat = getattr(parcel_info, "latitude", None)
    lng = getattr(parcel_info, "longitude", None)
    if lat and lng:
        lines.append(f"- GPS: {lat}, {lng}")

    return "\n".join(lines) if lines else "- None"


def _utility_status_to_bool(status: str) -> Optional[bool]:
    if status == "confirmed":
        return True
    if status == "not_available":
        return False
    return None


def _yes_no_unknown_to_bool(status: str) -> Optional[bool]:
    if status == "yes":
        return True
    if status == "no":
        return False
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Stage merger — combines outputs of all 4 Stage-1 calls into one flat dict
# ─────────────────────────────────────────────────────────────────────────────

def _merged_all_stages(
    identity: Stage1Identity,
    zoning: Stage1Zoning,
    utilities: Stage1Utilities,
    environment: Stage1Environment,
    maps_link: Optional[str],
    sat_link: Optional[str],
    street_link: Optional[str],
) -> dict[str, Any]:
    return {
        # ── Identity (1A) ────────────────────────────────────────────────────
        "apn": identity.apn,
        "county": identity.county,
        "state": identity.state,
        "street_address": identity.street_address,
        "gps_coordinates": identity.gps_coordinates,
        "google_maps_link": identity.google_maps_link or maps_link,
        "google_satellite_link": identity.google_satellite_link or sat_link,
        "google_street_view_link": identity.google_street_view_link or street_link,
        "parcel_boundary_map_link": identity.parcel_boundary_map_link,
        "acreage": identity.acreage,
        "sq_ft": identity.sq_ft or (
            int(round((identity.acreage or 0) * 43560)) if identity.acreage else 0
        ),
        "legal_description": identity.legal_description,
        "county_assessed_value": identity.county_assessed_value,
        "assessed_year": identity.assessed_year,
        "owner_name": identity.owner_name,
        "tax_status": identity.tax_status,
        "liens_beyond_tax": identity.liens_beyond_tax,

        # ── Zoning (1B) ──────────────────────────────────────────────────────
        "zoning_code": zoning.zoning_code,
        "zoning_description": zoning.zoning_description,
        "allowed_uses": zoning.allowed_uses,
        "minimum_lot_size": zoning.minimum_lot_size,
        "setbacks": zoning.setbacks,
        "buildable": _yes_no_unknown_to_bool(zoning.buildable),
        "residential_allowed": _yes_no_unknown_to_bool(zoning.residential_allowed),
        "mobile_homes_allowed": _yes_no_unknown_to_bool(zoning.mobile_homes_allowed),
        "rv_allowed": _yes_no_unknown_to_bool(zoning.rv_allowed),
        "tiny_homes_allowed": _yes_no_unknown_to_bool(zoning.tiny_homes_allowed),
        "camping_allowed": _yes_no_unknown_to_bool(zoning.camping_allowed),
        "off_grid_allowed": _yes_no_unknown_to_bool(zoning.off_grid_allowed),
        "commercial_allowed": _yes_no_unknown_to_bool(zoning.commercial_allowed),
        "agricultural_allowed": _yes_no_unknown_to_bool(zoning.agricultural_allowed),
        "hoa_present": _yes_no_unknown_to_bool(zoning.hoa_present),
        "hoa_fees": zoning.hoa_fees,
        "hoa_name": zoning.hoa_name,
        "planning_dept_phone": zoning.planning_dept_phone,
        "zoning_source_url": zoning.zoning_source_url,

        # ── Utilities & Infrastructure (1C) ──────────────────────────────────
        "electricity_available": _utility_status_to_bool(utilities.electricity.status),
        "water_available": _utility_status_to_bool(utilities.water.status),
        "sewer_available": _utility_status_to_bool(utilities.sewer.status),
        "gas_available": _utility_status_to_bool(utilities.gas.status),
        "well_required": _yes_no_unknown_to_bool(utilities.well_required.status),
        "septic_required": _yes_no_unknown_to_bool(utilities.septic_required.status),
        "utility_at_street": _yes_no_unknown_to_bool(utilities.utility_at_street.status),
        "utility_cost_estimate": utilities.utility_cost_estimate,
        "internet_provider": utilities.internet_provider,
        "internet_type": utilities.internet_type,
        "cell_coverage": utilities.cell_coverage,
        "cell_carriers": utilities.cell_carriers,
        "road_type": utilities.road_type,
        "road_name": utilities.road_name,
        "road_condition": utilities.road_condition,
        "road_maintained_by": utilities.road_maintained_by,
        "year_round_access": utilities.year_round_access,
        "distance_to_paved_road": utilities.distance_to_paved_road,
        "utility_evidence": utilities.model_dump(),

        # ── Environment, terrain & growth (1D) ───────────────────────────────
        "flood_zone": environment.flood_zone,
        "flood_zone_designation": environment.flood_zone_designation,
        "flood_map_url": environment.flood_map_url,
        "wetlands_risk": environment.wetlands_risk,
        "wetlands_notes": environment.wetlands_notes,
        "fire_risk": environment.fire_risk,
        "fire_risk_source": environment.fire_risk_source,
        "fire_risk_url": environment.fire_risk_url,
        "landslide_risk": environment.landslide_risk,
        "terrain_description": environment.terrain_description,
        "slope_classification": environment.slope_classification,
        "washes_or_arroyos": environment.washes_or_arroyos,
        "soil_suitability": environment.soil_suitability,
        "protected_land_status": environment.protected_land_status,
        "environmental_restrictions": environment.environmental_restrictions,
        "nearest_city_name": environment.nearest_city_name,
        "distance_to_nearest_city": environment.distance_to_nearest_city,
        "distance_to_highway": environment.distance_to_highway,
        "distance_to_lake_or_water": environment.distance_to_lake_or_water,
        "distance_to_major_attraction": environment.distance_to_major_attraction,
        "major_attraction_name": environment.major_attraction_name,
        "nearby_parcel_usage": environment.nearby_parcel_usage,
        "nearby_housing_development": environment.nearby_housing_development,
        "nearby_structures": environment.nearby_structures,
        "power_lines_visible": _yes_no_unknown_to_bool(environment.power_lines_visible),
        "population_growth_trend": environment.population_growth_trend,
        "county_growth_rate": environment.county_growth_rate,
        "building_permit_growth": environment.building_permit_growth,
        "growth_notes": environment.growth_notes,

        # road/access fallbacks
        "legal_access_status": utilities.legal_access_status,
        "road_description": utilities.road_description,
        "easements": utilities.easements,
        "landlocked": utilities.landlocked,

        # ── All sources merged ────────────────────────────────────────────────
        "sources_used_stage1": _dedupe_keep_order(
            identity.sources_used_stage1a
            + zoning.sources_used_stage1b
            + utilities.sources_used_stage1c
            + environment.sources_used_stage1d
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Comp / listing de-dupe helpers
# ─────────────────────────────────────────────────────────────────────────────

def _dedupe_comps(comps: list[RawSoldComp]) -> list[RawSoldComp]:
    seen = set()
    out = []
    for c in comps:
        if not c.acreage or not c.sold_price:
            continue
        if c.acreage <= 0 or c.sold_price <= 0:
            continue
        if not c.price_per_acre:
            c.price_per_acre = int(round(c.sold_price / c.acreage))
        key = (
            _clean_str(c.apn),
            c.sold_price,
            round(c.acreage, 4),
            _clean_str(c.sold_date),
            _clean_str(c.source_url),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _dedupe_listings(rows: list[RawActiveListing]) -> list[RawActiveListing]:
    seen = set()
    out = []
    for r in rows:
        if not r.acreage or not r.listing_price:
            continue
        if r.acreage <= 0 or r.listing_price <= 0:
            continue
        if not r.price_per_acre:
            r.price_per_acre = int(round(r.listing_price / r.acreage))
        key = (
            _clean_str(r.apn),
            r.listing_price,
            round(r.acreage, 4),
            _clean_str(r.source_url),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _filter_outliers_by_median_band(
    values: list[int], lower_mult: float, upper_mult: float
) -> tuple[int, float, float]:
    med = _median_int(values)
    return med, med * lower_mult, med * upper_mult


# ─────────────────────────────────────────────────────────────────────────────
# Market data cleaner
# ─────────────────────────────────────────────────────────────────────────────

def _clean_market_data(stage2_raw: Stage2Raw) -> dict[str, Any]:
    raw_comps = _dedupe_comps(stage2_raw.raw_sold_comps)
    raw_listings = _dedupe_listings(stage2_raw.raw_active_listings)

    removed = []
    base_comps = []
    for c in raw_comps:
        if c.has_structures:
            removed.append({
                "apn": c.apn,
                "sold_price": c.sold_price,
                "price_per_acre": c.price_per_acre,
                "acreage": c.acreage,
                "source_url": c.source_url,
                "removal_reason": "Has structures / not pure vacant land",
            })
        else:
            base_comps.append(c)

    strict_clean = base_comps[:]
    notes = []

    if base_comps:
        vals = [c.price_per_acre for c in base_comps if c.price_per_acre]
        med, lower, upper = _filter_outliers_by_median_band(vals, 0.40, 2.5)
        strict_clean = []
        for c in base_comps:
            if lower <= c.price_per_acre <= upper:
                strict_clean.append(c)
            else:
                removed.append({
                    "apn": c.apn,
                    "sold_price": c.sold_price,
                    "price_per_acre": c.price_per_acre,
                    "acreage": c.acreage,
                    "source_url": c.source_url,
                    "removal_reason": f"Price/acre outside strict median fence ({int(lower)} - {int(upper)})",
                })
        notes.append(
            f"Strict comp fence based on median ${med:,}/acre: lower={int(lower):,}, upper={int(upper):,}"
        )

        if len(strict_clean) < 3:
            med, lower, upper = _filter_outliers_by_median_band(vals, 0.30, 3.5)
            strict_clean = []
            removed = [
                x for x in removed
                if x["removal_reason"] == "Has structures / not pure vacant land"
            ]
            for c in base_comps:
                if lower <= c.price_per_acre <= upper:
                    strict_clean.append(c)
                else:
                    removed.append({
                        "apn": c.apn,
                        "sold_price": c.sold_price,
                        "price_per_acre": c.price_per_acre,
                        "acreage": c.acreage,
                        "source_url": c.source_url,
                        "removal_reason": f"Price/acre outside relaxed median fence ({int(lower)} - {int(upper)})",
                    })
            notes.append(f"Relaxed comp fence used: lower={int(lower):,}, upper={int(upper):,}")

    clean_comps = strict_clean

    clean_listings = raw_listings[:]
    if raw_listings:
        vals = [r.price_per_acre for r in raw_listings if r.price_per_acre]
        if vals:
            med, lower, upper = _filter_outliers_by_median_band(vals, 0.40, 2.5)
            clean_listings = [r for r in raw_listings if lower <= r.price_per_acre <= upper]
            notes.append(
                f"Listing fence based on median ${med:,}/acre: lower={int(lower):,}, upper={int(upper):,}"
            )

    sold_ppa = [c.price_per_acre for c in clean_comps if c.price_per_acre]
    active_ppa = [r.price_per_acre for r in clean_listings if r.price_per_acre]

    median_sold_ppa = _median_int(sold_ppa)
    avg_sold_ppa = _avg_int(sold_ppa)
    avg_active_ppa = _avg_int(active_ppa)

    today = date.today()
    d6 = today - timedelta(days=183)
    d12 = today - timedelta(days=365)

    sales_6 = 0
    sales_12 = 0
    doms = []
    for c in clean_comps:
        sd = _parse_date(c.sold_date)
        if sd:
            if sd >= d6:
                sales_6 += 1
            if sd >= d12:
                sales_12 += 1
        if c.days_on_market is not None and c.days_on_market >= 0:
            doms.append(c.days_on_market)

    inventory = len(clean_listings)
    sold_to_active = round(sales_12 / inventory, 2) if inventory else None
    median_dom = _median_int(doms) if doms else None
    sales_velocity = f"{sales_12 / 12:.1f} sales/month"

    if median_dom is None and sold_to_active is None:
        market_classification = "Unknown"
    elif (median_dom is not None and median_dom > 180) or (
        sold_to_active is not None and sold_to_active < 0.2
    ):
        market_classification = "Very Slow"
    elif (median_dom is not None and 120 <= median_dom <= 180) and (
        sold_to_active is not None and 0.2 <= sold_to_active < 0.4
    ):
        market_classification = "Slow"
    elif (median_dom is not None and 60 <= median_dom <= 120) and (
        sold_to_active is not None and 0.4 <= sold_to_active <= 0.8
    ):
        market_classification = "Normal"
    elif (median_dom is not None and median_dom < 60) and (
        sold_to_active is not None and sold_to_active > 0.8
    ):
        market_classification = "Fast"
    else:
        market_classification = "Normal" if clean_comps else "Unknown"

    dom_range_map = {
        "Fast": "30-90 days",
        "Normal": "90-180 days",
        "Slow": "180-365 days",
        "Very Slow": "12-24 months",
        "Unknown": None,
    }

    zip_turnover_6 = (sales_6 / inventory * 100.0) if inventory else None
    zip_turnover_12 = (sales_12 / inventory * 100.0) if inventory else None

    diff_pct = (
        _relative_diff_pct(median_sold_ppa, avg_sold_ppa)
        if median_sold_ppa and avg_sold_ppa
        else 100.0
    )
    if len(clean_comps) >= 6 and diff_pct <= 20:
        confidence = "High"
    elif len(clean_comps) >= 3 and diff_pct <= 40:
        confidence = "Medium"
    elif len(clean_comps) >= 1:
        confidence = "Low"
    else:
        confidence = "Low"

    return {
        "removed_comps": removed,
        "clean_sold_comps": [
            {
                "apn": c.apn,
                "address": c.address,
                "sold_price": c.sold_price,
                "price_per_acre": c.price_per_acre,
                "acreage": c.acreage,
                "distance_or_location": c.distance_or_location,
                "sold_date": c.sold_date,
                "days_on_market": c.days_on_market,
                "terrain_notes": c.terrain_notes,
                "zoning": c.zoning,
                "source": c.source,
                "source_url": c.source_url,
            }
            for c in clean_comps
        ],
        "clean_active_listings": [
            {
                "apn": r.apn,
                "address": r.address,
                "listing_price": r.listing_price,
                "price_per_acre": r.price_per_acre,
                "acreage": r.acreage,
                "days_on_market": r.days_on_market,
                "terrain_and_access_notes": r.terrain_and_access_notes,
                "source": r.source,
                "source_url": r.source_url,
            }
            for r in clean_listings
        ],
        "median_sold_price_per_acre": median_sold_ppa,
        "avg_sold_price_per_acre": avg_sold_ppa,
        "avg_active_price_per_acre": avg_active_ppa,
        "comps_before_filter": len(raw_comps),
        "comps_after_filter": len(clean_comps),
        "listings_before_filter": len(raw_listings),
        "listings_after_filter": len(clean_listings),
        "outlier_filter_notes": " | ".join(notes),
        "market_demand": {
            "land_sales_last_6_months": sales_6,
            "land_sales_last_12_months": sales_12,
            "active_listings_count": inventory,
            "inventory_count": inventory,
            "sold_to_active_ratio": f"{sold_to_active:.2f}" if sold_to_active is not None else None,
            "median_dom": median_dom,
            "sales_velocity": sales_velocity,
            "zip_turnover_rate_6mo": _maybe_percent(zip_turnover_6),
            "zip_turnover_rate_12mo": _maybe_percent(zip_turnover_12),
            "market_classification": market_classification,
            "estimated_dom_range": dom_range_map[market_classification],
            "market_notes": (
                f"{len(clean_comps)} clean sold comps, {inventory} clean active listings, "
                f"confidence={confidence}."
            ),
        },
        "data_confidence": confidence,
        "sources_used_stage2": stage2_raw.sources_used_stage2,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────────────────────────────────────

def _liens_found(liens_beyond_tax: Optional[str]) -> bool:
    s = _norm_text(liens_beyond_tax)
    if not s:
        return False
    return s not in {"none identified", "none", "no liens found", "unknown"}


def _protected_or_env(core: dict[str, Any]) -> bool:
    a = _norm_text(core.get("protected_land_status"))
    b = _norm_text(core.get("environmental_restrictions"))
    bad = {"", "none", "none identified", "n/a", "na", "unknown", "not found", "no known restrictions"}
    return a not in bad or b not in bad


def _risk_score(stage1: dict[str, Any], stage2b: dict[str, Any]) -> tuple[int, list[str], str]:
    score = 0
    factors = []

    if stage1.get("flood_zone") is True:
        score += 20
        factors.append("Flood zone: +20")
    if stage1.get("wetlands_risk") is True:
        score += 15
        factors.append("Wetlands risk: +15")
    if stage1.get("landlocked") is True:
        score += 25
        factors.append("Landlocked: +25")

    fire = _norm_text(stage1.get("fire_risk"))
    if any(x in fire for x in ["high", "very high", "extreme"]):
        score += 15
        factors.append("High/extreme fire risk: +15")
    elif "moderate" in fire:
        score += 8
        factors.append("Moderate fire risk: +8")

    if _protected_or_env(stage1):
        score += 15
        factors.append("Protected land / environmental restriction: +15")

    util_count = sum(
        1 for k in ["electricity_available", "water_available", "sewer_available", "gas_available"]
        if stage1.get(k) is True
    )
    if util_count == 0:
        score += 10
        factors.append("No confirmed public utilities: +10")

    if _liens_found(stage1.get("liens_beyond_tax")):
        score += 10
        factors.append("Liens/encumbrances found: +10")

    landslide = _norm_text(stage1.get("landslide_risk"))
    if "high" in landslide:
        score += 10
        factors.append("High landslide risk: +10")

    market_class = stage2b["market_demand"].get("market_classification")
    if market_class == "Very Slow":
        score += 5
        factors.append("Very slow market: +5")

    score = min(score, 100)
    explanation = "Low numbers are safer. Score is based on environmental, access, utility, title, and market risks."
    return score, factors, explanation


def _deal_score(
    stage1: dict[str, Any], stage2b: dict[str, Any], profit_margin_pct_num: float
) -> tuple[int, list[str], str]:
    factors = []
    total = 0

    road   = _norm_text(stage1.get("road_type"))
    access = _norm_text(stage1.get("legal_access_status") or stage1.get("road_condition") or "")

    if ("paved" in road) and ("legal" in access or "confirmed" in access or "good" in access):
        pts  = 5
        note = "Location: 5/5 — Paved road and legal access confirmed"
    elif ("gravel" in road) and ("legal" in access or "confirmed" in access or "good" in access):
        pts  = 4
        note = "Location: 4/5 — Gravel road and legal access confirmed"
    elif ("dirt" in road) or ("unclear" in access) or ("unknown" in access) or not road:
        pts  = 2
        note = "Location: 2/5 — Dirt road or access unclear"
    else:
        pts  = 1
        note = "Location: 1/5 — Weak access signal"
    total += pts
    factors.append(note)

    market = stage2b["market_demand"].get("market_classification")
    market_pts_map = {"Fast": 20, "Normal": 15, "Slow": 8, "Very Slow": 3, "Unknown": 8}
    pts = market_pts_map.get(market, 8)
    total += pts
    factors.append(f"Market: {pts}/20 — {market or 'Unknown'} market")

    util3 = sum(
        1 for k in ["electricity_available", "water_available", "sewer_available"]
        if stage1.get(k) is True
    )
    if util3 == 3:
        pts = 20
    elif util3 == 2:
        pts = 14
    elif util3 == 1:
        pts = 8
    else:
        pts = 2
    total += pts
    factors.append(f"Utilities: {pts}/20 — {util3} of 3 core utilities confirmed")

    if profit_margin_pct_num > 200:
        pts = 50
    elif 100 <= profit_margin_pct_num <= 200:
        pts = 38
    elif 50 <= profit_margin_pct_num < 100:
        pts = 25
    else:
        pts = 10
    total += pts
    factors.append(f"Profit: {pts}/50 — Expected margin {profit_margin_pct_num:.1f}%")

    risk_score_num, _, _ = _risk_score(stage1, stage2b)
    if 0 <= risk_score_num <= 19:
        pts = 5
    elif 20 <= risk_score_num <= 39:
        pts = 4
    elif 40 <= risk_score_num <= 59:
        pts = 2
    else:
        pts = 0
    total += pts
    factors.append(f"Risk: {pts}/5 — Risk score {risk_score_num}")

    explanation = "Deal score blends profit margin (50), market speed (20), utilities (20), access (5), and risk (5)."
    return total, factors, explanation


def _verdict(score: int) -> str:
    if score >= 75:
        return "STRONG BUY"
    if score >= 55:
        return "BUY"
    if score >= 35:
        return "HOLD"
    return "PASS"


def _risk_tier(risk_score_num: int) -> str:
    if risk_score_num <= 19:
        return "Very Low Risk"
    if risk_score_num <= 39:
        return "Moderate Risk"
    if risk_score_num <= 59:
        return "High Risk"
    return "Extreme Risk"


def _build_red_flags(
    stage1: dict[str, Any],
    stage2b: dict[str, Any],
    max_bid_ceiling: int,
) -> list[str]:
    flags = []

    if stage1.get("landlocked") is True:
        flags.append("Parcel appears landlocked")

    road = _norm_text(stage1.get("road_type") or "")
    if not road or road in {"none", "unknown"}:
        flags.append("Road type unknown or no road access found")

    if stage1.get("flood_zone") is True:
        flags.append(f"Flood zone: {stage1.get('flood_zone_designation') or 'confirmed'}")
    if stage1.get("wetlands_risk") is True:
        flags.append("Wetlands risk present")
    if "extreme" in _norm_text(stage1.get("fire_risk")):
        flags.append("Extreme fire risk")
    if stage2b["market_demand"].get("market_classification") == "Very Slow":
        flags.append("Very slow market")
    if _liens_found(stage1.get("liens_beyond_tax")):
        flags.append("Liens or encumbrances found")
    if _protected_or_env(stage1):
        flags.append("Protected land or environmental restriction needs review")

    util_count = sum(
        1 for k in ["electricity_available", "water_available", "sewer_available", "gas_available"]
        if stage1.get(k) is True
    )
    if util_count == 0:
        flags.append("No public utilities confirmed")

    if stage2b.get("data_confidence") == "Low":
        flags.append("Low comp confidence")

    return flags


def _next_learning_step(stage1: dict[str, Any], red_flags: list[str]) -> str:
    if stage1.get("landlocked") is True or "Road type unknown or no road access found" in red_flags:
        return "Order a title report and confirm deeded legal access or recorded easements before bidding."
    if any(stage1.get(k) is None for k in ["electricity_available", "water_available", "sewer_available"]):
        return (
            "Call the relevant utility providers or county planning office to confirm "
            "parcel-front utility service and extension cost."
        )
    if stage1.get("flood_zone") is True or stage1.get("wetlands_risk") is True:
        return "Review FEMA and wetlands maps with parcel boundary overlay before bidding."
    return "Order title, survey, and a basic access/utility verification package before placing a bid."


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic MLF + CF formula helpers
# ─────────────────────────────────────────────────────────────────────────────

def _compute_mlf(market_classification: str) -> float:
    return {
        "Fast": 0.90,
        "Normal": 0.85,
        "Slow": 0.80,
        "Very Slow": 0.80,
        "Unknown": 0.85,
    }.get(market_classification, 0.80)


def _compute_cf(stage1: dict[str, Any]) -> tuple[float, list[str]]:
    cf = 0.12
    adjustments: list[str] = []

    water_avail = stage1.get("water_available")
    elec_avail = stage1.get("electricity_available")
    if water_avail is not True and elec_avail is not True:
        cf += 0.03
        adjustments.append("No water & electricity confirmed: +3%")

    if stage1.get("septic_required") is True or stage1.get("well_required") is True:
        cf += 0.03
        adjustments.append("Septic/well required: +3%")

    return cf, adjustments


# ─────────────────────────────────────────────────────────────────────────────
# DuckDuckGo URL enrichment  ← FULLY REPLACED
# Exact same logic as the working standalone script.
# Sequential (one at a time), no domain mapping, source field drives matching.
# ─────────────────────────────────────────────────────────────────────────────

_logger = logging.getLogger(__name__)


def _normalize_source_domain(source: str) -> str:
    s = (source or "").strip().lower()

    if not s:
        return ""

    if not s.startswith(("http://", "https://")):
        if "." not in s:
            s = f"https://{s}.com"
        else:
            s = f"https://{s}"

    netloc = urlparse(s).netloc.lower().replace("www.", "")
    return netloc.rstrip("/")


def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    query = f"{(address or '').strip()} {(source or '').strip()}".strip()
    if not query:
        return None

    target_domain = _normalize_source_domain(source)
    if not target_domain:
        return None

    encoded_query = quote(query)

    search_urls = [
        f"https://html.duckduckgo.com/html/?q={encoded_query}",
        f"https://duckduckgo.com/html/?q={encoded_query}",
    ]

    for attempt in range(1, 4):
        for search_url in search_urls:
            try:
                with requests.Session() as session:
                    session.headers.update(HEADERS)
                    session.mount("https://", HTTPAdapter(max_retries=0))
                    session.mount("http://", HTTPAdapter(max_retries=0))

                    res = session.get(
                        search_url,
                        timeout=(3.5, 12),  # (connect_timeout, read_timeout)
                        allow_redirects=True,
                    )

                _logger.info(
                    "DDG attempt=%s address=%s source=%s url=%s status_code=%s final_url=%s",
                    attempt,
                    address,
                    source,
                    search_url,
                    res.status_code,
                    getattr(res, "url", None),
                )

                if res.status_code != 200:
                    continue

                soup = BeautifulSoup(res.text, "html.parser")
                results = soup.select("a.result__a")

                _logger.info(
                    "DDG attempt=%s address=%s source=%s results_empty=%s results_count=%s",
                    attempt,
                    address,
                    source,
                    not bool(results),
                    len(results),
                )

                if not results:
                    continue

                for rank, a in enumerate(results, start=1):
                    raw_href = a.get("href")
                    if not raw_href:
                        continue

                    full_url = urljoin("https://duckduckgo.com", raw_href)
                    parsed = urlparse(full_url)
                    actual_url = parse_qs(parsed.query).get("uddg", [None])[0]

                    if not actual_url and raw_href.startswith(("http://", "https://")):
                        actual_url = raw_href

                    if not actual_url:
                        continue

                    actual_url = requests.utils.unquote(actual_url)
                    candidate_domain = urlparse(actual_url).netloc.lower().replace("www.", "")

                    _logger.info(
                        "DDG attempt=%s rank=%s address=%s source=%s candidate_domain=%s actual_url=%s",
                        attempt,
                        rank,
                        address,
                        source,
                        candidate_domain,
                        actual_url,
                    )

                    if (
                        candidate_domain == target_domain
                        or candidate_domain.endswith("." + target_domain)
                    ):
                        _logger.info(
                            "DDG matched attempt=%s rank=%s address=%s source=%s matched_domain=%s actual_url=%s",
                            attempt,
                            rank,
                            address,
                            source,
                            candidate_domain,
                            actual_url,
                        )
                        return actual_url

            except requests.exceptions.ConnectTimeout as e:
                _logger.warning(
                    "DDG connect timeout attempt=%s address=%s source=%s url=%s error=%s",
                    attempt,
                    address,
                    source,
                    search_url,
                    e,
                )
            except requests.exceptions.ReadTimeout as e:
                _logger.warning(
                    "DDG read timeout attempt=%s address=%s source=%s url=%s error=%s",
                    attempt,
                    address,
                    source,
                    search_url,
                    e,
                )
            except requests.exceptions.RequestException as e:
                _logger.warning(
                    "DDG request failed attempt=%s address=%s source=%s url=%s error=%s",
                    attempt,
                    address,
                    source,
                    search_url,
                    e,
                )
            except Exception as e:
                _logger.exception(
                    "DDG unexpected error attempt=%s address=%s source=%s url=%s error=%s",
                    attempt,
                    address,
                    source,
                    search_url,
                    e,
                )

        if attempt < 3:
            sleep_for = random.uniform(2.5, 5.0)
            _logger.info(
                "DDG backoff attempt=%s address=%s source=%s sleep=%.2fs",
                attempt,
                address,
                source,
                sleep_for,
            )
            time.sleep(sleep_for)

    return None


async def process_item(item: dict[str, Any]) -> dict[str, Any]:
    address = item.get("address")
    source = item.get("source")

    if not address or not source:
        return {**item, "url": None}

    url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
    return {**item, "url": url}


async def _enrich_source_urls(stage2b: dict[str, Any]) -> None:
    sold_items = stage2b.get("clean_sold_comps", [])
    active_items = stage2b.get("clean_active_listings", [])
    items = sold_items + active_items

    if not items:
        return

    semaphore = asyncio.Semaphore(1)  # start with 1 on Render; raise to 2 only if stable

    async def sem_task(item: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            await asyncio.sleep(random.uniform(1.5, 3.5))
            return await process_item(item)

    results = await asyncio.gather(*(sem_task(item) for item in items))

    for original_item, result in zip(items, results):
        url = result.get("url")
        original_item["source_url"] = url

        if url:
            _logger.info("Enriched URL: %s -> %s", original_item.get("address"), url)
        else:
            _logger.debug("No DDG URL found for: %s", original_item.get("address"))

        print(f"{original_item.get('address')} -> {url}")

# ─────────────────────────────────────────────────────────────────────────────
# Florida-specific bid engine
# ─────────────────────────────────────────────────────────────────────────────

_FL_SELL_COST   = 0.10
_FL_CARRY_COST  = 0.04
_FL_RISK_BUFFER = 0.08
_FL_AUCTION_FEE = 0.05
_FL_TARGET_MARGIN = 0.40
_FL_FIXED_COSTS = 0
_FL_LF_DEFAULT  = 0.92


def _is_florida(state: str | None) -> bool:
    return _norm_text(state or "") in {"fl", "florida"}


def _compute_florida_bid(
    mmv: float,
    stage1: dict[str, Any],
    stage2b: dict[str, Any],
    fixed_costs: float = _FL_FIXED_COSTS,
    auction_price: float | None = None,
) -> dict[str, Any]:
    lf = _FL_LF_DEFAULT
    exit_price = mmv * lf
    net_mult = 1.0 - _FL_SELL_COST - _FL_CARRY_COST - _FL_RISK_BUFFER
    denom = 1.0 + _FL_TARGET_MARGIN + _FL_AUCTION_FEE

    max_bid = ((exit_price * net_mult) - fixed_costs) / denom if denom else 0
    max_bid = max(int(round(max_bid)), 0)

    low_bid = int(round(max_bid * 0.85))
    mid_bid = int(round(max_bid * 0.925))

    conservative_resale    = int(round(mmv * 0.88))
    suggested_resale_price = int(round(mmv * 0.92))
    aggressive_resale      = int(round(mmv * 0.96))

    auction_val = auction_price or max_bid
    ratio = (auction_val / mmv) if mmv else 0.0
    ratio = round(ratio, 4)

    if ratio > 0.85:
        tier = "NO BID"
        hard_stop = True
    elif ratio > 0.75:
        tier = "CAUTION"
        hard_stop = False
    elif ratio > 0.60:
        tier = "PROCEED"
        hard_stop = False
    else:
        tier = "STRONG BUY"
        hard_stop = False

    bid_formula_note = (
        f"FL MAX_BID = ((MMV×LF × NET_MULT) − FIXED) ÷ DENOM | "
        f"MMV={_money(int(mmv))} | LF={lf} | EXIT={_money(int(exit_price))} | "
        f"NET_MULT={net_mult:.2f} | DENOM={denom:.2f} | "
        f"MAX_BID={_money(max_bid)}"
    )
    resale_formula_note = (
        f"FL Resale: Low=88% of MMV ({_money(conservative_resale)}) | "
        f"Mid=92% of MMV ({_money(suggested_resale_price)}) | "
        f"High=96% of MMV ({_money(aggressive_resale)})"
    )

    return {
        "max_bid_ceiling": max_bid,
        "low_bid_threshold": low_bid,
        "mid_bid_threshold": mid_bid,
        "conservative_resale": conservative_resale,
        "suggested_resale_price": suggested_resale_price,
        "aggressive_resale": aggressive_resale,
        "florida_lf": lf,
        "florida_exit_price": int(round(exit_price)),
        "florida_ratio": ratio,
        "florida_ratio_tier": tier,
        "florida_hard_stop": hard_stop,
        "bid_formula_note": bid_formula_note,
        "resale_formula_note": resale_formula_note,
        "florida_mmv": int(round(mmv)),
        "florida_sell_cost": _FL_SELL_COST,
        "florida_carry_cost": _FL_CARRY_COST,
        "florida_risk_buffer": _FL_RISK_BUFFER,
        "florida_auction_fee": _FL_AUCTION_FEE,
        "florida_target_margin": _FL_TARGET_MARGIN,
        "florida_fixed_costs": int(fixed_costs),
        "florida_net_multiplier": round(net_mult, 4),
        "florida_denominator": round(denom, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Report builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_report(
    stage1: dict[str, Any],
    stage2b: dict[str, Any],
) -> dict[str, Any]:
    acreage = _safe_float(stage1.get("acreage"), 0.0)
    median_ppa = _to_int_zero(stage2b.get("median_sold_price_per_acre"))
    avg_sold_ppa = _to_int(stage2b.get("avg_sold_price_per_acre"))
    avg_active_ppa = _to_int(stage2b.get("avg_active_price_per_acre"))

    mmv = median_ppa * acreage
    low_estimated_value  = int(round(median_ppa * 0.85 * acreage))
    mid_estimated_value  = int(round(mmv))
    high_estimated_value = int(round(median_ppa * 1.15 * acreage))

    low_price_per_acre  = int(round(median_ppa * 0.85))
    mid_price_per_acre  = int(round(median_ppa))
    high_price_per_acre = int(round(median_ppa * 1.15))

    clean_comp_count = stage2b.get("comps_after_filter", 0)
    data_confidence  = stage2b.get("data_confidence", "Low")
    diff_pct = (
        _relative_diff_pct(median_ppa, avg_sold_ppa or 0)
        if median_ppa and avg_sold_ppa else None
    )
    valuation_notes = (
        f"Median sold price/acre {_money(median_ppa)}, average sold price/acre {_money(avg_sold_ppa)}, "
        f"using {clean_comp_count} clean comps; confidence={data_confidence}."
    )
    if diff_pct is not None and diff_pct > 30:
        valuation_notes += (
            f" Median vs average differs by {diff_pct:.1f}%, which suggests spread in comp quality."
        )

    market_classification = stage2b["market_demand"].get("market_classification", "Unknown")
    mlf = _compute_mlf(market_classification)
    cf, cf_adjustments = _compute_cf(stage1)
    pm = 0.40

    max_bid_ceiling   = int(round(mmv * mlf * (1 - pm - cf)))
    low_bid_threshold = int(round(max_bid_ceiling * 0.85))
    mid_bid_threshold = int(round(max_bid_ceiling * 0.925))

    conservative_resale = int(round(mmv * (mlf - 0.05)))
    mid_resale          = int(round(mmv * mlf))
    aggressive_resale   = int(round(mmv * (mlf + 0.03)))
    suggested_resale_price = mid_resale

    expected_profit    = int(round(suggested_resale_price - max_bid_ceiling))
    profit_margin_num  = (expected_profit / max_bid_ceiling * 100.0) if max_bid_ceiling else 0.0
    profit_margin_pct  = f"{profit_margin_num:.1f}%"

    low_offer  = int(round(mmv * 0.40))
    high_offer = int(round(mmv * 0.65))

    cf_pct  = int(round(cf * 100))
    mlf_pct = int(round(mlf * 100))

    cf_note = f"CF={cf_pct}% (baseline 12%"
    if cf_adjustments:
        cf_note += " + " + " + ".join(cf_adjustments)
    cf_note += ")"

    bid_formula_note = (
        f"MAX_BID = MMV × MLF × (1 − PM − CF) | "
        f"MMV={_money(int(mmv))} | MLF={mlf_pct}% ({market_classification}) | "
        f"PM=40% | {cf_note} | "
        f"= {_money(int(mmv))} × {mlf} × {round(1 - pm - cf, 4):.4f}"
    )
    resale_formula_note = (
        f"Low={mlf_pct - 5}% of MMV | Mid={mlf_pct}% of MMV (MLF) | "
        f"High={mlf_pct + 3}% of MMV | MLF driven by {market_classification} market"
    )

    risk_score_num, risk_factors_applied, risk_explanation = _risk_score(stage1, stage2b)
    deal_score_num, scoring_factors, deal_explanation = _deal_score(stage1, stage2b, profit_margin_num)
    verdict = _verdict(deal_score_num)

    market = stage2b["market_demand"]
    resale_timeline = market.get("estimated_dom_range")
    red_flags = _build_red_flags(stage1, stage2b, max_bid_ceiling)
    next_learning_step = _next_learning_step(stage1, red_flags)

    state_val = stage1.get("state") or ""
    florida_bid_data = None
    if _is_florida(state_val):
        florida_bid_data = _compute_florida_bid(mmv, stage1, stage2b)
        if florida_bid_data["florida_hard_stop"]:
            red_flags.insert(0, f"🚫 FL HARD STOP — Auction/MMV ratio {florida_bid_data['florida_ratio']:.2%} exceeds 85% threshold. DO NOT BID.")
            verdict = "NO BID"
        elif florida_bid_data["florida_ratio_tier"] == "CAUTION":
            red_flags.insert(0, f"⚠️ FL CAUTION — Auction/MMV ratio {florida_bid_data['florida_ratio']:.2%} is in the 75-85% warning zone.")

    strengths = []
    if market.get("market_classification") in {"Fast", "Normal"}:
        strengths.append(f"Market classified as {market.get('market_classification')}")
    road_type = _norm_text(stage1.get("road_type") or "")
    if road_type and road_type not in {"none", "unknown"}:
        strengths.append(f"Road access: {stage1.get('road_type')}")
    if stage1.get("zoning_code"):
        strengths.append(f"Zoning identified: {stage1.get('zoning_code')}")
    if any(stage1.get(k) is True for k in ["electricity_available", "water_available", "sewer_available"]):
        strengths.append("At least one core utility is confirmed")
    if profit_margin_num >= 100:
        strengths.append(f"Expected margin {profit_margin_pct}")

    concerns = red_flags[:5] if red_flags else ["No major red flags identified from public data"]

    bid_recommendation = (
        f"Recommended Max Bid: {_money(max_bid_ceiling)} | "
        f"Resale: {_money(suggested_resale_price)} | "
        f"Profit: {_money(expected_profit)} | {verdict}"
    )

    summary = (
        f"This {acreage:.2f}-acre parcel in {stage1.get('county')}, {stage1.get('state')} "
        f"has a mid estimated market value of {_money(mid_estimated_value)} based on median sold "
        f"price per acre. The local market is classified as {market.get('market_classification') or 'Unknown'} "
        f"(MLF={mlf_pct}%), CF={cf_pct}%, recommended bid ceiling is {_money(max_bid_ceiling)}, "
        f"and the expected resale target is {_money(suggested_resale_price)}."
    )

    sources_checked = _dedupe_keep_order(
        stage1.get("sources_used_stage1", []) +
        stage2b.get("sources_used_stage2", [])
    )

    report = ParcelReport(
        homepage_summary={
            "parcel_size_acres": acreage or None,
            "parcel_size_sqft": stage1.get("sq_ft"),
            "satellite_map_link": stage1.get("google_satellite_link"),
            "all_liens": stage1.get("liens_beyond_tax"),
            "assessed_value": stage1.get("county_assessed_value"),
            "estimated_market_value_mid": mid_estimated_value,
            "avg_sold_price_per_acre": avg_sold_ppa,
            "avg_active_price_per_acre": avg_active_ppa,
            "recommended_bid_ceiling": max_bid_ceiling,
            "suggested_resale_price": suggested_resale_price,
            "deal_score": deal_score_num,
            "risk_score": risk_score_num,
            "verdict": verdict,
        },
        quick_summary={
            "verdict": verdict,
            "bid_recommendation": bid_recommendation,
            "summary": summary,
            "key_strengths": strengths,
            "key_concerns": concerns,
        },
        basic_parcel_info={
            "apn": stage1.get("apn") or "",
            "county": stage1.get("county") or "",
            "state": stage1.get("state") or "",
            "acreage": acreage,
            "sq_ft": stage1.get("sq_ft") or 0,
            "street_address": stage1.get("street_address"),
            "gps_coordinates": stage1.get("gps_coordinates"),
            "google_maps_link": stage1.get("google_maps_link"),
            "google_satellite_link": stage1.get("google_satellite_link"),
            "google_street_view_link": stage1.get("google_street_view_link"),
            "parcel_boundary_map_link": stage1.get("parcel_boundary_map_link"),
            "legal_description": stage1.get("legal_description"),
            "county_assessed_value": stage1.get("county_assessed_value"),
            "assessed_year": stage1.get("assessed_year"),
            "owner_name": stage1.get("owner_name"),
            "tax_status": stage1.get("tax_status"),
            "liens_beyond_tax": stage1.get("liens_beyond_tax"),
        },
        access_and_location={
            "road_type": stage1.get("road_type"),
            "road_name": stage1.get("road_name"),
            "road_condition": stage1.get("road_condition"),
            "road_maintained_by": stage1.get("road_maintained_by"),
            "year_round_access": stage1.get("year_round_access"),
            "distance_to_paved_road": stage1.get("distance_to_paved_road"),
            "road_description": stage1.get("road_description"),
            "legal_access_status": stage1.get("legal_access_status"),
            "easements": stage1.get("easements"),
            "landlocked": stage1.get("landlocked"),
            "distance_to_highway": stage1.get("distance_to_highway"),
            "nearest_city": stage1.get("nearest_city_name"),
            "distance_to_nearest_city": stage1.get("distance_to_nearest_city"),
            "distance_to_lake_or_water": stage1.get("distance_to_lake_or_water"),
            "distance_to_major_attraction": stage1.get("distance_to_major_attraction"),
            "major_attraction_name": stage1.get("major_attraction_name"),
            "nearby_structures": stage1.get("nearby_structures"),
            "nearby_housing_development": stage1.get("nearby_housing_development"),
            "power_lines_visible": stage1.get("power_lines_visible"),
            "population_growth_trend": stage1.get("population_growth_trend"),
            "county_growth_rate": stage1.get("county_growth_rate"),
            "building_permit_growth": stage1.get("building_permit_growth"),
            "growth_notes": stage1.get("growth_notes"),
            "zoning": {
                "zoning_code": stage1.get("zoning_code"),
                "zoning_description": stage1.get("zoning_description"),
                "allowed_uses": stage1.get("allowed_uses"),
                "minimum_lot_size": stage1.get("minimum_lot_size"),
                "setbacks": stage1.get("setbacks"),
                "buildable": stage1.get("buildable"),
                "residential_allowed": stage1.get("residential_allowed"),
                "rv_allowed": stage1.get("rv_allowed"),
                "mobile_homes_allowed": stage1.get("mobile_homes_allowed"),
                "tiny_homes_allowed": stage1.get("tiny_homes_allowed"),
                "camping_allowed": stage1.get("camping_allowed"),
                "off_grid_allowed": stage1.get("off_grid_allowed"),
                "commercial_allowed": stage1.get("commercial_allowed"),
                "agricultural_allowed": stage1.get("agricultural_allowed"),
                "hoa_present": stage1.get("hoa_present"),
                "hoa_fees": stage1.get("hoa_fees"),
                "hoa_name": stage1.get("hoa_name"),
                "planning_dept_phone": stage1.get("planning_dept_phone"),
                "zoning_source_url": stage1.get("zoning_source_url"),
            },
            "utilities": {
                "electricity_available": stage1.get("electricity_available"),
                "water_available": stage1.get("water_available"),
                "sewer_available": stage1.get("sewer_available"),
                "septic_required": stage1.get("septic_required"),
                "well_required": stage1.get("well_required"),
                "gas_available": stage1.get("gas_available"),
                "utility_at_street": stage1.get("utility_at_street"),
                "utility_cost_estimate": stage1.get("utility_cost_estimate"),
                "internet_provider": stage1.get("internet_provider"),
                "internet_type": stage1.get("internet_type"),
                "cell_coverage": stage1.get("cell_coverage"),
                "cell_carriers": stage1.get("cell_carriers"),
            },
        },
        terrain_overview={
            "terrain_description": stage1.get("terrain_description"),
            "slope_classification": stage1.get("slope_classification"),
            "washes_or_arroyos": stage1.get("washes_or_arroyos"),
            "wetlands_notes": stage1.get("wetlands_notes"),
            "nearby_parcel_usage": stage1.get("nearby_parcel_usage"),
            "flood_zone": stage1.get("flood_zone"),
            "flood_zone_designation": stage1.get("flood_zone_designation"),
            "flood_map_url": stage1.get("flood_map_url"),
            "wetlands_risk": stage1.get("wetlands_risk"),
            "landslide_risk": stage1.get("landslide_risk"),
            "fire_risk": stage1.get("fire_risk"),
            "fire_risk_source": stage1.get("fire_risk_source"),
            "fire_risk_url": stage1.get("fire_risk_url"),
            "soil_suitability": stage1.get("soil_suitability"),
            "protected_land_status": stage1.get("protected_land_status"),
            "environmental_restrictions": stage1.get("environmental_restrictions"),
        },
        sold_comps=stage2b.get("clean_sold_comps", []),
        active_listings=stage2b.get("clean_active_listings", []),
        estimated_market_value={
            "avg_sold_price_per_acre": avg_sold_ppa,
            "low_price_per_acre": low_price_per_acre,
            "mid_price_per_acre": mid_price_per_acre,
            "high_price_per_acre": high_price_per_acre,
            "low_estimated_value": low_estimated_value,
            "mid_estimated_value": mid_estimated_value,
            "high_estimated_value": high_estimated_value,
            "valuation_notes": valuation_notes,
        },
        educational_offer_range={
            "explanation": "Educational offer band based on 40-65% of mid market value.",
            "low_offer": low_offer,
            "high_offer": high_offer,
            "discount_percentage": "35-60% below market",
        },
        resale_price_range={
            "conservative_resale": conservative_resale,
            "mid_resale": mid_resale,
            "aggressive_resale": aggressive_resale,
            "suggested_resale_price": suggested_resale_price,
            "resale_timeline": resale_timeline,
            "resale_formula_note": resale_formula_note,
            "recommended_tier_explanation": (
                f"Suggested tier uses MLF={mlf_pct}% of MMV based on "
                f"{market_classification} market conditions."
            ),
        },
        auction_bid_ceiling={
            "risk_tier": _risk_tier(risk_score_num),
            "mlf": f"{mlf_pct}%",
            "cf": f"{cf_pct}%",
            "cf_breakdown": cf_adjustments if cf_adjustments else ["Baseline 12%, no additional risk factors"],
            "pm": "40%",
            "recovery_percentage": f"{mlf_pct}%",
            "low_bid_threshold": low_bid_threshold,
            "mid_bid_threshold": mid_bid_threshold,
            "max_bid_ceiling": max_bid_ceiling,
            "suggested_resale_price": suggested_resale_price,
            "expected_profit": expected_profit,
            "profit_margin_pct": profit_margin_pct,
            "bid_formula_note": bid_formula_note,
            "risk_factors": risk_factors_applied,
        },
        days_on_market={
            "market_classification": market.get("market_classification") or "Unknown",
            "estimated_dom_range": market.get("estimated_dom_range"),
            "median_dom": market.get("median_dom"),
            "inventory_count": market.get("inventory_count"),
            "land_sales_last_6_months": market.get("land_sales_last_6_months"),
            "land_sales_last_12_months": market.get("land_sales_last_12_months"),
            "active_listings_count": market.get("active_listings_count"),
            "zip_turnover_rate_6mo": market.get("zip_turnover_rate_6mo"),
            "zip_turnover_rate_12mo": market.get("zip_turnover_rate_12mo"),
            "sales_velocity": market.get("sales_velocity"),
            "sold_to_active_ratio": market.get("sold_to_active_ratio"),
        },
        deal_score={
            "score": deal_score_num,
            "explanation": deal_explanation,
            "scoring_factors": scoring_factors,
        },
        risk_score={
            "score": risk_score_num,
            "explanation": risk_explanation,
            "risk_factors_applied": risk_factors_applied,
        },
        red_flags=red_flags,
        next_learning_step=next_learning_step,
        sources_checked=sources_checked,
        florida_bid_data=florida_bid_data,
        compliance_disclaimer=(
            "This report is for educational purposes only. It uses public information "
            "that can change at any time. This is not financial, legal, or tax advice "
            "and is not a recommendation to buy, sell, or offer any amount on this or any other property."
        ),
    )

    return report.model_dump()


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def analyze_apn(
    apn: str,
    county: str,
    state: str,
    parcel_info: Any = None,
    latitude: Optional[str] = None,
    longitude: Optional[str] = None,
    address: Optional[str] = None,
) -> dict[str, Any]:
    baseline_context = _build_baseline_context(parcel_info)

    stage1a = await generate_structured(
        prompt=STAGE1A_IDENTITY_PROMPT.format(
            apn=apn,
            county=county,
            state=state,
            baseline_context=baseline_context,
        ),
        schema_model=Stage1Identity,
        use_search=True,
        thinking_level=THINKING_STAGE1A,
        max_output_tokens=8000,
    )

    if not stage1a.gps_coordinates:
        if latitude and longitude:
            stage1a.gps_coordinates = f"{latitude}, {longitude}"
        elif parcel_info:
            lat = getattr(parcel_info, "latitude", None)
            lng = getattr(parcel_info, "longitude", None)
            if lat and lng:
                stage1a.gps_coordinates = f"{lat}, {lng}"

    maps_link, sat_link, street_link = _build_maps_links(stage1a.gps_coordinates)
    if not stage1a.google_maps_link:
        stage1a.google_maps_link = maps_link
    if not stage1a.google_satellite_link:
        stage1a.google_satellite_link = sat_link
    if not stage1a.google_street_view_link:
        stage1a.google_street_view_link = street_link

    address_val = address or stage1a.street_address or ""
    gps_val     = stage1a.gps_coordinates or f"{county}, {state}"
    acreage_str = str(stage1a.acreage or getattr(parcel_info, "lot_size", "") or "")

    stage1a.street_address = address_val
    stage1a.gps_coordinates = gps_val

    stage1b, stage1c, stage1d, stage2_raw = await asyncio.gather(
        generate_structured(
            prompt=STAGE1B_ZONING_PROMPT.format(
                apn=apn, county=county, state=state,
                street_address=address_val, gps_coordinates=gps_val,
            ),
            schema_model=Stage1Zoning,
            use_search=True,
            thinking_level=THINKING_STAGE1B,
            max_output_tokens=10000,
        ),
        generate_structured(
            prompt=STAGE1C_UTILITIES_PROMPT.format(
                apn=apn, county=county, state=state,
                street_address=address_val, gps_coordinates=gps_val,
            ),
            schema_model=Stage1Utilities,
            use_search=True,
            thinking_level=THINKING_STAGE1C,
            max_output_tokens=10000,
        ),
        generate_structured(
            prompt=STAGE1D_ENVIRONMENT_PROMPT.format(
                apn=apn, county=county, state=state,
                street_address=address_val, gps_coordinates=gps_val,
                acreage=acreage_str,
            ),
            schema_model=Stage1Environment,
            use_search=True,
            thinking_level=THINKING_STAGE1D,
            max_output_tokens=10000,
        ),
        generate_structured(
            prompt=STAGE2_PROMPT.format(
                apn=apn, county=county, state=state,
                street_address=address_val, gps_coordinates=gps_val,
                acreage=acreage_str,
                zoning_code="",
            ),
            schema_model=Stage2Raw,
            use_search=True,
            thinking_level=THINKING_STAGE2,
            max_output_tokens=22000,
        ),
    )

    stage1 = _merged_all_stages(
        identity=stage1a,
        zoning=stage1b,
        utilities=stage1c,
        environment=stage1d,
        maps_link=maps_link,
        sat_link=sat_link,
        street_link=street_link,
    )

    stage2b = _clean_market_data(stage2_raw)

    # DDG URL enrichment — sequential, 1 at a time, 60s total timeout
    try:
        await asyncio.wait_for(_enrich_source_urls(stage2b), timeout=60.0)
    except asyncio.TimeoutError:
        _logger.warning("DDG URL enrichment timed out — using Gemini URLs only")

    final_report = _build_report(
        stage1=stage1,
        stage2b=stage2b,
    )

    validated = ParcelReport.model_validate(final_report)
    return validated.model_dump()


def analyze_apn_sync(
    apn: str,
    county: str,
    state: str,
    parcel_info: Any = None,
    latitude: Optional[str] = None,
    longitude: Optional[str] = None,
    address: Optional[str] = None,
) -> dict[str, Any]:
    return asyncio.run(
        analyze_apn(
            apn=apn,
            county=county,
            state=state,
            parcel_info=parcel_info,
            latitude=latitude,
            longitude=longitude,
            address=address,
        )
    )


if __name__ == "__main__":
    result = analyze_apn_sync(
        apn="YOUR_APN",
        county="Apache",
        state="Arizona",
        parcel_info=None,
    )
    print(json.dumps(result, indent=2))
