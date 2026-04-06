import io
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
from database import get_db
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

    # Check if CSV-uploaded parcel data exists for this APN
    parcel_data = db.query(ParcelData).filter(ParcelData.apn == normalized_apn).first()

    # Auto-fill county & state from parcel_data (CSV takes priority over UI input)
    county = (parcel_data.county if parcel_data and parcel_data.county else request.county) or "Unknown"
    state = (parcel_data.state if parcel_data and parcel_data.state else request.state) or "Unknown"

    # Extract bidding_start_value if available
    bidding_start_value = None
    if parcel_data and parcel_data.bidding_start_value:
        bidding_start_value = float(parcel_data.bidding_start_value)

    try:
        report_data = await analyze_apn(
            request.apn,
            county,
            state,
            parcel_info=parcel_data,
            bidding_start_value=bidding_start_value,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    # Extract key fields from the Pydantic-structured response
    parcel = report_data.get("basic_parcel_info", {})
    market_val = report_data.get("estimated_market_value", {})
    bid = report_data.get("auction_bid_ceiling", {})
    score = report_data.get("deal_score", {})

    # If CSV data exists, overlay known fields into report_data
    if parcel_data:
        basic = report_data.get("basic_parcel_info", {})
        if parcel_data.address:
            basic["street_address"] = parcel_data.address
        if parcel_data.latitude and parcel_data.longitude:
            basic["gps_coordinates"] = f"{parcel_data.latitude}, {parcel_data.longitude}"
            basic["google_maps_link"] = f"https://maps.google.com/?q={parcel_data.latitude},{parcel_data.longitude}"
            basic["google_satellite_link"] = f"https://maps.google.com/?q={parcel_data.latitude},{parcel_data.longitude}&t=k"
        if parcel_data.total_assessed_value:
            basic["county_assessed_value"] = int(parcel_data.total_assessed_value)
        if parcel_data.assessment_year:
            basic["assessed_year"] = str(parcel_data.assessment_year)
        report_data["basic_parcel_info"] = basic

        # Overlay terrain/env data
        terrain = report_data.get("terrain_overview", {})
        if parcel_data.flood_risk:
            terrain["flood_zone"] = parcel_data.flood_risk.lower() not in ("none", "no", "minimal", "")
            terrain["flood_zone_designation"] = parcel_data.flood_risk
        if parcel_data.environmental_hazard_status:
            terrain["environmental_restrictions"] = parcel_data.environmental_hazard_status
        report_data["terrain_overview"] = terrain

        # Overlay zoning
        access = report_data.get("access_and_location", {})
        zoning = access.get("zoning", {})
        if parcel_data.zoning:
            zoning["zoning_code"] = parcel_data.zoning
        access["zoning"] = zoning
        report_data["access_and_location"] = access

    report = APNReport(
        apn=request.apn,
        county=county,
        state=state,
        acreage=parcel.get("acreage"),
        assessed_value=market_val.get("mid_estimated_value"),
        status="completed",
        report_data=report_data,
        deal_score=score.get("score"),
        bid_ceiling=bid.get("mid_bid_threshold"),
        estimated_market_value=market_val.get("mid_estimated_value"),
    )
    db.add(report)
    db.commit()
    db.refresh(report)

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
