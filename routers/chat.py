"""
Chat router – context-aware, multi-turn conversation about a parcel report.
"""

import os
import json
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from sqlalchemy.orm import Session

from database import get_db
from models.user import User
from models.apn import APNReport
from utils.auth import get_current_user

from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CHAT_MODEL     = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is not set")

_client = genai.Client(api_key=GEMINI_API_KEY)

router = APIRouter(prefix="/api/chat", tags=["Chat"])

SEARCH_NEEDED_MARKER = "__SEARCH_NEEDED__"

NO_DATA_VALUES = {
    "data not found", "not found", "unknown", "n/a", "none",
    "not available", "unavailable", "not confirmed", "unconfirmed",
    "none identified", "no data", "", "null",
}


# ── Prompts ───────────────────────────────────────────────────────────────────

REPORT_QA_SYSTEM = """You are a factual land investment assistant with memory of the full conversation.

Parcel: {apn} | {county}, {state} | Address: {address}

ANSWER PRIORITY — follow this exact order every time:
1. CONVERSATION HISTORY FIRST: Scan the full chat history above before doing anything else.
   If a previous assistant message already answered the user's current question \
(e.g. from a prior web search result), use that answer directly. \
Do NOT re-check the report and do NOT trigger a new search.
2. REPORT DATA: If history does not contain the answer, check the report data below. \
Only use fields with confirmed non-null, non-placeholder values.
3. MISSING: If NEITHER history nor report has the answer → trigger {marker}.

CRITICAL RULES:
1. A field is MISSING in the report if its value is: null, "Data not found", "Unknown", \
"N/A", "None", "not available", empty string, or any similar placeholder. \
Treat ALL such values as if the data does not exist.
2. DO NOT infer or derive any answer from the street address, road name, GPS, \
parcel shape, or any indirect clue. Only explicit confirmed field values count. \
Example: a road named "Marion Way" does NOT imply it is paved.
3. ALWAYS check conversation history (priority 1) before declaring data missing (priority 3). \
If history already has the answer from a prior web search → answer from it directly.
4. If SOME fields have data (from history or report) and others are missing — \
answer the confirmed fields first in 1-2 sentences, then add {marker} for the missing ones only.
5. When triggering {marker}: write {marker} on its own line, followed by exactly one \
sentence naming which field(s) are missing and offering to search the web. \
Write nothing else.
6. Keep total response under 4 sentences. No preamble or filler. Use exact values.

REPORT DATA:
{report_json}
"""

SEARCH_QA_SYSTEM = """You are a concise land investment assistant. \
The user asked about a parcel whose report lacked specific data. \
Use web search to find the answer.

Parcel context:
- APN: {apn}
- County: {county}
- State: {state}
- Address: {address}
- GPS: {gps}

RULES:
1. Answer in 1-3 sentences. Be direct and specific.
2. Cite the source briefly (e.g. "Per county GIS…").
3. Answer ONLY what was asked — no extra context.
4. If you cannot find the answer after searching, say so in one sentence.
"""


# ── Schemas ───────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str   # "user" or "bot"
    text: str


class ChatRequest(BaseModel):
    report_id: int
    message: str
    use_search: bool = False
    history: List[ChatMessage] = []


class ChatResponse(BaseModel):
    reply: str
    needs_search: bool = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_no_data(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in NO_DATA_VALUES:
        return True
    return False


def _clean_report(obj):
    """
    Normalize report data for Gemini:
    - Keep "Data not found" strings explicit so Gemini sees them clearly.
    - Normalize nulls to "Data not found".
    - Drop empty dicts/lists (noise with no signal).
    """
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            cv = _clean_report(v)
            if isinstance(cv, str) and cv.strip().lower() in NO_DATA_VALUES:
                cleaned[k] = "Data not found"
            elif cv in ({}, []):
                pass
            elif cv is None:
                cleaned[k] = "Data not found"
            else:
                cleaned[k] = cv
        return cleaned
    if isinstance(obj, list):
        return [_clean_report(i) for i in obj if i not in (None, "", [], {})]
    return obj


def _extract_report_summary(report_data: dict) -> str:
    cleaned = _clean_report(report_data)
    result  = json.dumps(cleaned, indent=2, default=str)
    if len(result) > 60000:
        result = result[:60000] + "\n... (truncated)"
    return result


def _get_field(report_data: dict, *keys: str, default="N/A") -> str:
    """Safely dig into nested dicts across multiple key names."""
    for key in keys:
        # Check top level first
        if isinstance(report_data, dict) and key in report_data:
            val = report_data[key]
            if not _is_no_data(val):
                return str(val)
        # Then check one level deep
        for section in (report_data.values() if isinstance(report_data, dict) else []):
            if isinstance(section, dict) and key in section:
                val = section[key]
                if not _is_no_data(val):
                    return str(val)
    return default


def _build_contents(history: List[ChatMessage], current_message: str) -> list:
    """
    Convert frontend message history + current message into Gemini multi-turn
    contents: [Content(role="user"|"model", parts=[Part(text=...)])]
    Gemini requires roles to strictly alternate user/model.
    """
    contents = []

    for msg in history:
        gemini_role = "user" if msg.role == "user" else "model"
        contents.append(
            types.Content(
                role=gemini_role,
                parts=[types.Part(text=msg.text)],
            )
        )

    # Current user message is always last
    contents.append(
        types.Content(
            role="user",
            parts=[types.Part(text=current_message)],
        )
    )

    return contents


def _check_truncated(response) -> bool:
    """Return True if Gemini cut off the response due to token limit."""
    try:
        reason = str(response.candidates[0].finish_reason)
        return reason in ("MAX_TOKENS", "2")
    except Exception:
        return False


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("", response_model=ChatResponse)
async def chat(
    req: ChatRequest,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    # 1. Load report
    report = db.query(APNReport).filter(APNReport.id == req.report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    report_data = report.report_data or {}

    # 2. Extract parcel identity (check both nested and top-level keys)
    bpi           = report_data.get("basic_parcel_info", {})
    actual_apn    = bpi.get("apn")    or report_data.get("apn")    or report.apn    or "N/A"
    actual_county = bpi.get("county") or report_data.get("county") or report.county or "N/A"
    actual_state  = bpi.get("state")  or report_data.get("state")  or report.state  or "N/A"
    actual_address = _get_field(report_data, "street_address", "address", default="N/A")
    actual_gps     = _get_field(report_data, "gps_coordinates", "gps", "coordinates", default="N/A")

    # 3. Build multi-turn contents (history + current message)
    contents = _build_contents(req.history, req.message)

    try:
        if req.use_search:
            # ── Web search mode ───────────────────────────────────────────────
            system_prompt = SEARCH_QA_SYSTEM.format(
                apn=actual_apn,
                county=actual_county,
                state=actual_state,
                address=actual_address,
                gps=actual_gps,
            )

            config = types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=1024,
                tools=[types.Tool(google_search=types.GoogleSearch())],
                system_instruction=system_prompt,
            )

            response = await _client.aio.models.generate_content(
                model=CHAT_MODEL,
                contents=contents,
                config=config,
            )

            reply_text = (response.text or "").strip()
            if not reply_text:
                reply_text = "I couldn't find this information online. Try rephrasing your question."

            return ChatResponse(reply=reply_text, needs_search=False)

        else:
            # ── Report-only mode ──────────────────────────────────────────────
            report_json_str = _extract_report_summary(report_data)

            system_prompt = REPORT_QA_SYSTEM.format(
                apn=actual_apn,
                county=actual_county,
                state=actual_state,
                address=actual_address,
                report_json=report_json_str,
                marker=SEARCH_NEEDED_MARKER,
            )

            config = types.GenerateContentConfig(
                temperature=0.0,        # strictly deterministic — no creative filling
                max_output_tokens=1024,
                system_instruction=system_prompt,
            )

            response = await _client.aio.models.generate_content(
                model=CHAT_MODEL,
                contents=contents,
                config=config,
            )

            # Guard: return clean message if response was token-truncated
            if _check_truncated(response):
                return ChatResponse(
                    reply="I wasn't able to complete my response. Please try a more specific question.",
                    needs_search=False,
                )

            reply_text = (response.text or "").strip()
            if not reply_text:
                reply_text = "I couldn't generate a response. Please try again."

            needs_search = SEARCH_NEEDED_MARKER in reply_text
            reply_text   = reply_text.replace(SEARCH_NEEDED_MARKER, "").strip()

            return ChatResponse(reply=reply_text, needs_search=needs_search)

    except Exception as e:
        import traceback
        with open("chat_error.txt", "w") as f:
            f.write(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")