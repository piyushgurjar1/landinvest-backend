# gemini_prompts.py
from __future__ import annotations

import os
import copy
import asyncio
from typing import Any, Optional, Type

from dotenv import load_dotenv
from pydantic import BaseModel
from google import genai
from google.genai import types


load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is not set")

client = genai.Client(api_key=GEMINI_API_KEY)

THINKING_STAGE1_CORE = "high"
THINKING_STAGE1_UTIL = "high"
THINKING_STAGE2 = "high"


STAGE1_CORE_PROMPT = """
You are a professional vacant-land due diligence researcher.

Task:
Find parcel identity, parcel location, zoning, access, environmental risks, growth signals, tax/owner data, and nearby context.
Do NOT determine utilities in this step.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}

Known baseline facts from database (use as hints, still verify live):
{baseline_context}

Search priority:
1. County assessor / tax records
2. County GIS / parcel viewer / Regrid
3. Google Maps and satellite
4. County zoning / planning
5. FEMA flood map
6. Official state fire hazard map
7. National Wetlands Inventory
8. County recorder / lien / tax status sources
9. County / Census / official growth sources

Rules:
1. Use public web sources only.
2. Prefer official county assessor, county GIS, county planning/zoning, FEMA, official state fire maps, wetlands mapper, county recorder, and official demographic sources.
3. Use null when a field cannot be confirmed.
4. Do not guess parcel-level facts.
5. Google Maps links must use real coordinates if coordinates are confirmed.
6. parcel_boundary_map_link should be a county GIS or Regrid parcel page when available.
7. liens_beyond_tax should be "None identified" only if you checked and found no lien/encumbrance evidence.
8. tax_status should be "Current", "Delinquent", or "Unknown".
9. Collect every URL actually used in sources_used_stage1_core.
10. Return JSON only.
"""

STAGE1_UTILITIES_PROMPT = """
You are a professional vacant-land utility verification researcher.

Task:
Verify parcel-level or road-front utility availability for a vacant land parcel.

Subject parcel:
- APN: {apn}
- County: {county}
- State: {state}
- Street address: {street_address}
- GPS: {gps_coordinates}

Search priority:
1. Official electric utility territory/service maps
2. Official water district / municipal water service maps
3. Official sewer district / municipality sewer maps
4. Official gas utility service maps
5. County GIS utility layers / subdivision improvement maps / development maps
6. Parcel-specific evidence from county or listing pages only if official maps are unavailable

Rules:
- This step is ONLY about utilities.
- County-wide provider presence does NOT mean parcel availability.
- For each utility, choose status:
  - confirmed: parcel-level or immediate road-front evidence exists
  - not_available: official source explicitly indicates public service is not available or parcel is outside service
  - unknown: provider exists in area but parcel/street-edge availability is not confirmed
- basis should be one of: parcel, street_front, nearby_area, provider_only, unknown
- well_required / septic_required / utility_at_street must be yes, no, or unknown based only on explicit evidence
- If evidence is weak, return unknown
- Include provider_name, evidence_note, evidence_url, and distance_to_service when available
- List every source actually used in sources_used_stage1_utilities
- Return JSON only.
"""

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
1. Zillow sold land + active land
2. Redfin sold land
3. LandWatch sold + active
4. Land.com sold + active
5. Realtor land sold
6. AcreValue / county parcel sales / recorder if available
7. Regrid / parcel portals if useful

Rules:
- Vacant land only
- Exclude homes, cabins, manufactured homes, mobile homes, barns, improved parcels, or parcels whose value is obviously driven by installed well/septic/utilities
- Sold comps must have confirmed sold_date within the last 24 months
- Radius: within 15 miles of the subject parcel
- source_url is required for every comp and listing
- Include APN if available
- Calculate price_per_acre for every comp and listing
- days_on_market may be null if not found
- Return as many valid comps and listings as possible
- List every source actually used in sources_used_stage2
- Return JSON only
"""


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
    max_output_tokens: int = 12000,
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
    max_output_tokens: int = 12000,
    retries: int = 3,
    retry_delay: float = 2.0,
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

            response = await client.aio.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=config,
            )

            text = (response.text or "").strip()
            if not text:
                raise ValueError("Gemini returned empty response")

            return schema_model.model_validate_json(text)

        except Exception as e:
            last_error = e
            if attempt < retries:
                await asyncio.sleep(retry_delay * attempt)

    raise RuntimeError(f"Gemini structured call failed after {retries} attempts: {last_error}")