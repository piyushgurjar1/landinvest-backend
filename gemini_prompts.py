# gemini_prompts.py
from __future__ import annotations

import os
import copy
import asyncio
import logging
from typing import Any, Optional, Type

from dotenv import load_dotenv
from pydantic import BaseModel
from google import genai
from google.genai import types

load_dotenv()

_logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")  # ✅ valid model name

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is not set")

client = genai.Client(api_key=GEMINI_API_KEY)

# Thinking levels per stage
THINKING_STAGE1A = "low"
THINKING_STAGE1B = "low"
THINKING_STAGE1C = "medium"
THINKING_STAGE1D = "low"
THINKING_STAGE2  = "high"

# ── Per-stage timeout (seconds) ───────────────────────────────────────────────
# Each individual Gemini call gets its own timeout.
# If a stage exceeds this, it raises TimeoutError → retried or failed cleanly.
_STAGE_TIMEOUT = 300.0  # 3 minutes per stage call


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1A — Parcel Identity, Ownership & Address
# ─────────────────────────────────────────────────────────────────────────────
STAGE1A_IDENTITY_PROMPT = """
You are a professional county records researcher.

Task:
Confirm the parcel identity, full mailing address (with ZIP), ownership, assessed value,
tax status, and GPS coordinates for the subject parcel.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}

Known baseline facts from database (use as hints, verify live):
{baseline_context}

Search priority:
1. County assessor website (search by APN)
2. County GIS / parcel viewer
3. Regrid.com (search by APN)
4. USPS address lookup to confirm ZIP code
5. Google Maps / satellite for GPS coordinate confirmation

Rules:
1. Use public web sources only.
2. street_address MUST be the complete mailing address:
   Format: "Street Number Street Name, City, State ZIP"
   Example: "14321 Desert View Rd, Kingman, AZ 86409"
   - Always include ZIP code. Search county assessor or USPS if needed.
   - Never return a partial address without city, state, and ZIP.
3. gps_coordinates must be decimal degrees: "lat, lng" (e.g., "35.1234, -114.5678")
4. tax_status must be exactly: "Current", "Delinquent", or "Unknown"
5. liens_beyond_tax: return "None identified" only after checking county recorder.
6. county_assessed_value: numeric dollar amount only, no formatting.
7. assessed_year: 4-digit year.
8. parcel_boundary_map_link: county GIS or Regrid URL showing this specific parcel.
9. Use null when a field cannot be confirmed — never guess.
10. Collect every URL used in sources_used_stage1a.
11. Return JSON only.
"""


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1B — Zoning & All Permitted Uses
# ─────────────────────────────────────────────────────────────────────────────
STAGE1B_ZONING_PROMPT = """
You are a professional land use and zoning researcher specializing in vacant land.

Task:
Determine the complete zoning classification and ALL permitted land uses for this parcel.
Your goal is to find explicit yes/no answers for every use type listed below.
"Data not found" is NOT acceptable — you must search the county zoning ordinance directly.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}
- Street address: {street_address}
- GPS: {gps_coordinates}

Search priority (MUST follow in this exact order):
1. County assessor website — look for "zoning" field on the parcel detail page
2. County GIS / parcel viewer — many show zoning overlay on the map
3. County planning department website — search "{county} county {state} zoning ordinance"
4. County zoning map — search "{county} county {state} zoning map"
5. County zoning code / land development code — search "{county} county {state} zoning code [zoning_code] permitted uses"
6. Municode.com or American Legal (common hosts for county ordinances)
7. Regrid.com parcel page for this APN
8. Local planning office contact info if the code is unclear

Permitted Use Research Rules:
For EACH use type below, search the actual zoning ordinance text for the identified zoning code.
Return "yes", "no", or "unknown" — never null for these fields:

USE TYPES TO RESEARCH:
- residential_allowed: Can a standard single-family home be built?
- mobile_homes_allowed: Are manufactured/mobile homes permitted by right?
- rv_allowed: Can an RV be used as a primary dwelling or parked long-term?
- tiny_homes_allowed: Are tiny homes or small dwellings explicitly permitted?
- camping_allowed: Is recreational camping or tent camping permitted?
- off_grid_allowed: Is off-grid or primitive living without hookups allowed?
- commercial_allowed: Is any commercial use permitted?
- agricultural_allowed: Is agriculture, farming, or livestock permitted?
- buildable: Can any structure be legally built on this parcel today?

STRATEGY for finding use permissions:
- Look for the zoning code (e.g., "RU", "AR", "AG-1", "R1") in the county ordinance
- Search: "{county} county [zoning_code] zoning permitted uses"
- Search: "{county} county {state} mobile home zoning [zoning_code]"
- Search: "{county} county {state} RV dwelling zoning [zoning_code]"
- Search: "{county} county {state} tiny home zoning ordinance"
- If the zoning code allows "single family residential" → residential_allowed = yes
- If the ordinance lists mobile homes as a conditional or prohibited use → set accordingly
- HOA: search county recorder for any CC&Rs or HOA documents on this parcel/subdivision

Additional fields:
- zoning_code: the actual zoning designation (e.g., "AR-1", "RU", "AG")
- zoning_description: full name (e.g., "Agricultural Residential")
- allowed_uses: free text summary of all permitted uses from the ordinance
- minimum_lot_size: required minimum acreage or sq ft for development
- setbacks: front/rear/side setback requirements if found
- hoa_present: yes, no, or unknown
- hoa_fees: annual or monthly amount if known, else null
- hoa_name: name of HOA if present
- planning_dept_phone: county planning dept phone number if found
- zoning_source_url: direct URL to the zoning ordinance page or map

Rules:
1. You MUST search the actual county zoning ordinance, not just the assessor page.
2. Never return null for the use type boolean fields — return "unknown" only if the
   ordinance genuinely cannot be found after exhaustive search.
3. Include every URL used in sources_used_stage1b.
4. Return JSON only.
"""


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1C — Utilities & Infrastructure
# ─────────────────────────────────────────────────────────────────────────────
STAGE1C_UTILITIES_PROMPT = """
You are a professional vacant-land utility and infrastructure verification researcher.

Task:
Verify utility availability and infrastructure status for this vacant land parcel.
Use a layered search approach in the exact order below.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}
- Street address: {street_address}
- GPS: {gps_coordinates}

Listing sites : (Realtor.com, Zillow, LandWatch, Land.com) — search this APN or address.
═══════════════════════════════════════════════════════
PART A — ELECTRICITY
═══════════════════════════════════════════════════════
Search order:
1. Look for phrases: "electricity at street", "power at lot", "electric available", "no utilities"
2. Google Maps Street View — open the GPS coordinates in Street View.
   Look for: power poles, overhead lines, transformers, utility boxes along the road.
   If electric poles are visible → status=confirmed, basis=street_view_visual
3. Electric utility territory map — search "{county} county {state} electric utility provider"
   then check their service territory map for this address.
4. County GIS utility layer if available.

Evidence note must describe: what was seen on Street View OR what listing said.

═══════════════════════════════════════════════════════
PART B — WATER
═══════════════════════════════════════════════════════
Search order:
1. Listing sites — look for "water available", "city water", "municipal water", "well required"
2. Google Maps Street View — look for:
   FIRE HYDRANTS along the road → confirms municipal water (well is NOT required)
   Blue reflective road markers near curb → indicates underground water main
   If hydrants visible → status=confirmed, basis=street_view_visual, well_required=no
3. County/municipal water district service map — search "{county} water district service area map"
4. State water authority records.

═══════════════════════════════════════════════════════
PART C — SEWER
═══════════════════════════════════════════════════════
Search order:
1. Listing sites — look for "sewer", "septic required", "county sewer"
2. Municipal/county sewer district service area map
3. County GIS sewer layer
Note: If outside a municipal service boundary, assume septic_required=yes unless contradicted.

═══════════════════════════════════════════════════════
PART D — GAS
═══════════════════════════════════════════════════════
Search order:
1. Listing sites
2. Natural gas utility provider service map for this county/zip
3. If rural/remote, propane is typically the only option → gas_available=not_available

═══════════════════════════════════════════════════════
PART E — INTERNET & TELECOM
═══════════════════════════════════════════════════════
Search order:
1. FCC Broadband Map (broadbandmap.fcc.gov) — enter the address to check coverage
2. Major ISPs serving the county — search "{county} county {state} internet providers"
3. Cell coverage maps (T-Mobile, Verizon, AT&T) for this GPS location
Return: internet_provider, internet_type (fiber/cable/DSL/fixed wireless/satellite/none),
        cell_coverage (yes/no/unknown), cell_carriers

═══════════════════════════════════════════════════════
PART F — ROAD & ACCESS INFRASTRUCTURE
═══════════════════════════════════════════════════════
Using Google Maps and Street View:
- road_type: paved / gravel / dirt / none
- road_name: name of the road serving the parcel
- road_condition: good / fair / poor / unknown
- road_maintained_by: county / city / private / unknown
- year_round_access: yes / no / unknown (check for seasonal closures)
- distance_to_paved_road: miles to nearest paved road if road is unpaved

Legal access & easements (IMPORTANT — research carefully):
- legal_access_status: "Confirmed legal access" if parcel fronts a public road or has a recorded easement.
  "Easement required" if access depends on crossing another property.
  "Landlocked" if no road access at all. "Unknown" if you cannot determine.
  Check: county GIS parcel maps, plat maps, listing descriptions, and street view.
- road_description: Describe the access situation in one sentence, e.g.
  "Paved county-maintained road (CR 42) directly fronts the parcel" or
  "Dirt access via private easement off State Rd 80, condition poor"
- easements: Any known easements ON or NEEDED for this parcel.
  Check county records, plat maps, listing descriptions. Return null if none found.
- landlocked: "yes" if parcel has NO direct road access and no recorded easement.
  "no" if it fronts a road or has confirmed easement access. "unknown" if unclear.

Status values for each utility:
- confirmed: parcel-level or road-front evidence confirmed
- not_available: explicitly not in service area
- unknown: area served but parcel-front availability unconfirmed

Basis values:
- parcel, street_front, street_view_visual, listing_description,
  nearby_area, provider_only, unknown

Rules:
- Return null source_url only if no URL found — never fabricate
- evidence_note must describe the specific evidence found
- If Street View imagery is unavailable, state that explicitly
- List every source used in sources_used_stage1c
- Return JSON only.
"""


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1D — Environmental Risks, Growth & Nearby Context
# ─────────────────────────────────────────────────────────────────────────────
STAGE1D_ENVIRONMENT_PROMPT = """
You are a professional vacant-land environmental and growth researcher.

Task:
Research all environmental risks, terrain characteristics, nearby context, and growth
indicators for this vacant land parcel.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}
- Street address: {street_address}
- GPS: {gps_coordinates}
- Acreage: {acreage}

Search priority:
1. FEMA Flood Map (msc.fema.gov) — enter the address to confirm flood zone designation
2. Official state fire hazard severity zone map — search "{state} fire hazard severity zone map"
3. National Wetlands Inventory (fws.gov/program/national-wetlands-inventory) — check GPS location
4. USGS Landslide Hazard map
5. Google Satellite / Google Maps — terrain, slope, vegetation, washes
6. County assessor / GIS — soil type, terrain classification
7. Census / official county data — population growth
8. County planning / recorder — nearby development permits, subdivisions
9. Google Maps — nearby cities, highways, lakes, attractions

Environmental Fields:
- flood_zone: true/false (is any part of parcel in a FEMA flood zone?)
- flood_zone_designation: exact FEMA designation (e.g., "Zone AE", "Zone X", "Zone A")
- flood_map_url: direct link to FEMA map for this location
- wetlands_risk: true/false
- wetlands_notes: description if wetlands risk is present
- fire_risk: "Low", "Moderate", "High", "Very High", "Extreme", or "Unknown"
- fire_risk_source: official source name
- fire_risk_url: URL to the official fire map
- landslide_risk: "Low", "Moderate", "High", or "Unknown"
- terrain_description: flat / gently rolling / hilly / mountainous / desert / etc.
- slope_classification: "Flat (0-2%)" / "Gentle (2-8%)" / "Moderate (8-15%)" / "Steep (15%+)"
- washes_or_arroyos: true/false — are drainage washes visible on satellite?
- soil_suitability: "Good", "Fair", "Poor", "Unknown" — for building/septic
- protected_land_status: any protected status (wilderness, conservation easement, etc.)
- environmental_restrictions: any known deed restrictions or environmental covenants

Nearby Context Fields:
- nearest_city_name: name of nearest city or town
- distance_to_nearest_city: miles to nearest city
- distance_to_highway: miles to nearest major highway
- distance_to_lake_or_water: miles to nearest lake, river, or body of water
- distance_to_major_attraction: miles to nearest notable attraction (national park, etc.)
- major_attraction_name: name of the attraction
- nearby_parcel_usage: how are adjacent parcels being used?
- nearby_housing_development: any new subdivisions or housing within 5 miles? yes/no/unknown
- nearby_structures: any structures visible on satellite within 0.5 miles?
- power_lines_visible: are power lines visible on satellite imagery? yes/no/unknown

Growth Signals:
- population_growth_trend: "Growing", "Stable", "Declining", or "Unknown"
- county_growth_rate: percentage if found (e.g., "3.2%/year")
- building_permit_growth: "Increasing", "Stable", "Declining", or "Unknown"
- growth_notes: any relevant development activity noted

Rules:
1. For flood zone — check msc.fema.gov directly, not just county records.
2. For fire risk — only use the official state fire hazard map, not news articles.
3. For wetlands — check NWI mapper at the GPS coordinates.
4. Use null when truly unconfirmable. Do not guess.
5. List every source URL used in sources_used_stage1d.
6. Return JSON only.
"""


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — Comparable Sales & Active Listings
# ─────────────────────────────────────────────────────────────────────────────
STAGE2_PROMPT = """
You are a professional land market analyst.

Task:
Find vacant land sold comps and active vacant land listings near the subject parcel.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}
- Street address: {street_address}
- GPS: {gps_coordinates}
- Acreage: {acreage}
- Zoning: {zoning_code}

Search priority:
1. Realtor land sold
2. Zillow sold land + active land
3. Land.com sold + active
4. Redfin sold land
5. LandWatch sold + active
6. AcreValue / county parcel sales / recorder if available
7. Regrid / parcel portals if useful

Rules:
- Vacant land only
- Exclude homes, cabins, manufactured homes, mobile homes, barns, improved parcels,
  or parcels whose value is obviously driven by installed well/septic/utilities
- Sold comps must have confirmed sold_date within the last 24 months
- Radius: within 15 miles of the subject parcel
- CRITICAL — source_url must be the EXACT direct URL to that specific property listing page.
  Do NOT return:
    ✗ A generic website homepage (e.g., https://www.realtor.com)
    ✗ A search results page URL
    ✗ A category or browse URL
    ✗ A URL you guessed or reconstructed
  DO return:
    ✓ The actual listing detail page URL for that specific property
      (e.g., https://www.zillow.com/homedetails/123-Main-St/12345678_zpid/)
      (e.g., https://www.landwatch.com/arizona-land-for-sale/mohave-county/listing/1234567)
  If you cannot find the exact listing URL → set source_url to null.
  A null source_url is better than a broken or fabricated link.
- Include APN if available
- address: provide the FULL street address of each comp/listing
- source: set to the name of the website (e.g. "Zillow", "LandWatch", "Redfin", "Realtor.com")
- Calculate price_per_acre for every comp and listing
- days_on_market may be null if not found
- Return as many valid comps and listings as possible
- List every source actually used in sources_used_stage2
- Return JSON only
"""


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _inline_json_schema_refs(schema: dict[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(schema)
    defs = schema.pop("$defs", {})

    def _resolve(node: Any):
        if isinstance(node, dict):
            if "$ref" in node:
                ref = node.pop("$ref")
                key = ref.split("/")[-1]
                target = copy.deepcopy(defs.get(key, {}))
                node.clear()
                node.update(target)
                _resolve(node)
                return
            for _, value in list(node.items()):
                _resolve(value)
        elif isinstance(node, list):
            for item in node:
                _resolve(item)

    _resolve(schema)
    return schema


def _make_config(
    use_search: bool,
    thinking_level: str,
    response_schema: Optional[dict[str, Any]] = None,
    max_output_tokens: int = 62000,
) -> types.GenerateContentConfig:
    tools = [types.Tool(google_search=types.GoogleSearch())] if use_search else []

    kwargs = dict(
        temperature=0.0,
        max_output_tokens=max_output_tokens,
        thinking_config=types.ThinkingConfig(thinking_level=thinking_level),
        tools=tools,
    )

    if response_schema is not None:
        kwargs["response_mime_type"] = "application/json"
        kwargs["response_schema"] = response_schema

    return types.GenerateContentConfig(**kwargs)


async def generate_structured(
    prompt: str,
    schema_model: Type[BaseModel],
    use_search: bool,
    thinking_level: str,
    max_output_tokens: int = 32000,
    retries: int = 3,
    retry_delay: float = 30.0,   # ✅ 30s base delay — handles Gemini 429 rate limits
) -> BaseModel:
    schema_dict = _inline_json_schema_refs(schema_model.model_json_schema())
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            config = _make_config(
                use_search=use_search,
                thinking_level=thinking_level,
                response_schema=schema_dict,
                max_output_tokens=max_output_tokens,
            )

            # ✅ Per-stage timeout — each Gemini call has 3 minutes max
            # If Gemini hangs at network level, this raises TimeoutError cleanly
            response = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config,
                ),
                timeout=_STAGE_TIMEOUT,
            )

            text = (response.text or "").strip()
            if not text:
                raise ValueError("Gemini returned empty response")

            _logger.info("Gemini stage completed (attempt %d/%d)", attempt, retries)
            return schema_model.model_validate_json(text)

        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Gemini stage timed out after {_STAGE_TIMEOUT}s")
            _logger.warning("Gemini timeout on attempt %d/%d", attempt, retries)
            if attempt < retries:
                await asyncio.sleep(10.0)  # short wait before retry on timeout

        except Exception as e:
            last_error = e
            _logger.warning("Gemini error on attempt %d/%d: %s", attempt, retries, e)
            if attempt < retries:
                # Exponential backoff — 30s, 60s — handles rate limits
                wait = retry_delay * attempt
                _logger.info("Retrying in %.0fs...", wait)
                await asyncio.sleep(wait)

    raise RuntimeError(
        f"Gemini structured call failed after {retries} attempts: {last_error}"
    )