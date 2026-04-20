import io
import asyncio
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
from database import get_db, SessionLocal
from models.user import User
from models.apn import APNReport
from models.parcel_data import ParcelData
from schemas.apn import APNLookupRequest, APNReportResponse
from utils.auth import get_current_user
from gemini_service import analyze_apn


router = APIRouter(prefix="/api/apn", tags=["APN Reports"])


@router.get("/check/{apn}")
def check_apn(
    apn: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Check if APN exists in parcel_data and if a report already exists."""
    apn = str(apn).replace("-", "").replace(" ", "").strip()
    parcel = db.query(ParcelData).filter(ParcelData.apn == apn).first()
    existing_report = (
        db.query(APNReport)
        .filter(APNReport.apn == apn, APNReport.status == "completed")
        .order_by(APNReport.created_at.desc())
        .first()
    )

    return {
        "has_parcel_data": parcel is not None,
        "has_existing_report": existing_report is not None,
        "existing_report_id": existing_report.id if existing_report else None,
        "parcel_county": parcel.county if parcel else None,
        "parcel_state": parcel.state if parcel else None,
    }


@router.post("/lookup")
async def lookup_apn(
    request: APNLookupRequest,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    normalized_apn = str(request.apn).replace("-", "").replace(" ", "").strip()
    request.apn = normalized_apn

    # ── Step 1: Quick DB reads (connection is fresh, completes in ms) ──────────
    parcel_data = db.query(ParcelData).filter(ParcelData.apn == normalized_apn).first()

    county = (parcel_data.county if parcel_data and parcel_data.county else request.county) or "Unknown"
    state = (parcel_data.state if parcel_data and parcel_data.state else request.state) or "Unknown"

    # Cache all parcel fields we need later — after db.close() the ORM object is detached
    parcel_address = getattr(parcel_data, "address", None)
    parcel_lat = getattr(parcel_data, "latitude", None)
    parcel_lng = getattr(parcel_data, "longitude", None)
    has_parcel = parcel_data is not None

    # ── Step 2: Release DB connection BEFORE the long Gemini call ───────────────
    # Gemini takes 5–10 minutes. Cloud DBs kill idle connections after ~5 minutes.
    # Holding the connection open through the AI call guarantees an SSL drop error.
    db.close()

    # ── Step 3: Run Gemini (no DB connection held during this) ──────────────────
    try:
        report_data = await asyncio.wait_for(
            analyze_apn(
                request.apn,
                county,
                state,
                parcel_info=parcel_data,
                latitude=request.latitude,
                longitude=request.longitude,
                address=request.address,
            ),
            timeout=660.0,  # 11 minute hard cap — fails cleanly instead of hanging forever
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Analysis timed out after 11 minutes. Please try again.",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    # ── Step 4: Overlay parcel CSV data into the report ─────────────────────────
    if has_parcel:
        basic = report_data.get("basic_parcel_info", {})
        if parcel_address:
            basic["street_address"] = parcel_address
        if parcel_lat and parcel_lng:
            basic["gps_coordinates"] = f"{parcel_lat}, {parcel_lng}"
            basic["google_maps_link"] = f"https://maps.google.com/?q={parcel_lat},{parcel_lng}"
            basic["google_satellite_link"] = f"https://maps.google.com/?q={parcel_lat},{parcel_lng}&t=k"
        report_data["basic_parcel_info"] = basic

    # ── Step 5: Open a FRESH DB session just for the write ──────────────────────
    # pool_pre_ping ensures this connection is alive before use.
    # The write itself takes <1 second so no risk of timeout here.
    fresh_db = SessionLocal()
    try:
        parcel = report_data.get("basic_parcel_info", {})
        market_val = report_data.get("estimated_market_value", {})
        bid = report_data.get("auction_bid_ceiling", {})
        score = report_data.get("deal_score", {})

        report = APNReport(
            apn=request.apn,
            county=parcel.get("county") or county,
            state=parcel.get("state") or state,
            acreage=parcel.get("acreage"),
            assessed_value=market_val.get("mid_estimated_value"),
            status="completed",
            report_data=report_data,
            deal_score=score.get("score"),
            bid_ceiling=bid.get("mid_bid_threshold"),
            estimated_market_value=market_val.get("mid_estimated_value"),
        )
        fresh_db.add(report)
        fresh_db.commit()
        fresh_db.refresh(report)

        return {
            "id": report.id,
            "apn": report.apn,
            "county": report.county,
            "state": report.state,
            "acreage": report.acreage,
            "assessed_value": report.assessed_value,
            "status": report.status,
            "deal_score": report.deal_score,
            "bid_ceiling": report.bid_ceiling,
            "estimated_market_value": report.estimated_market_value,
            "report_data": report.report_data,
            "created_at": str(report.created_at) if report.created_at else None,
        }
    except Exception as e:
        fresh_db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to save report: {str(e)}")
    finally:
        fresh_db.close()


@router.get("/reports/{report_id}")
def get_report(
    report_id: int,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    report = db.query(APNReport).filter(APNReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return {
        "id": report.id,
        "apn": report.apn,
        "county": report.county,
        "state": report.state,
        "acreage": report.acreage,
        "assessed_value": report.assessed_value,
        "status": report.status,
        "deal_score": report.deal_score,
        "bid_ceiling": report.bid_ceiling,
        "estimated_market_value": report.estimated_market_value,
        "report_data": report.report_data,
        "created_at": str(report.created_at) if report.created_at else None,
    }


@router.get("/reports")
def list_reports(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
    search: Optional[str] = Query(None, description="Search by APN"),
    limit: int = Query(10, ge=1, le=100),
):
    """List reports. Optional APN search. Returns recent reports, not user-specific."""
    query = db.query(APNReport).filter(APNReport.status == "completed")

    if search:
        search = str(search).replace("-", "").replace(" ", "").strip()
        query = query.filter(APNReport.apn.ilike(f"%{search}%"))

    return (
        query
        .order_by(APNReport.created_at.desc())
        .limit(limit)
        .all()
    )