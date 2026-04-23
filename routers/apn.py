import asyncio
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from database import get_db, SessionLocal
from models.user import User
from models.apn import APNReport
from models.batch import BatchJob, BatchItem
from schemas.apn import APNLookupRequest, APNReportResponse
from utils.auth import get_current_user
from gemini_service import analyze_apn


router = APIRouter(prefix="/api/apn", tags=["APN Reports"])


# ─────────────────────────────────────────────────────────────────────────────
# Single APN endpoints (unchanged logic — only ParcelData references removed)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/check/{apn}")
def check_apn(
    apn: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Check if a report already exists for this APN."""
    apn = str(apn).replace("-", "").replace(" ", "").strip()
    existing_report = (
        db.query(APNReport)
        .filter(APNReport.apn == apn, APNReport.status == "completed")
        .order_by(APNReport.created_at.desc())
        .first()
    )

    return {
        "has_existing_report": existing_report is not None,
        "existing_report_id": existing_report.id if existing_report else None,
    }


@router.post("/lookup")
async def lookup_apn(
    request: APNLookupRequest,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    normalized_apn = str(request.apn).replace("-", "").replace(" ", "").strip()
    request.apn = normalized_apn

    county = request.county or "Unknown"
    state = request.state or "Unknown"

    # Release DB before long Gemini call
    db.close()

    try:
        report_data = await asyncio.wait_for(
            analyze_apn(
                request.apn,
                county,
                state,
                parcel_info=None,
                latitude=request.latitude,
                longitude=request.longitude,
                address=request.address,
            ),
            timeout=660.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Analysis timed out after 11 minutes. Please try again.",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

    # Save with a fresh DB session
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


# ─────────────────────────────────────────────────────────────────────────────
# Report retrieval endpoints (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

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
    """List reports. Optional APN search."""
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


# ─────────────────────────────────────────────────────────────────────────────
# Batch endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/batches")
def list_batches(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
    limit: int = Query(10, ge=1, le=50),
):
    """List recent batch jobs."""
    return [
        {
            "id": b.id,
            "filename": b.filename,
            "total_properties": b.total_properties,
            "processed_count": b.processed_count,
            "status": b.status,
            "created_at": str(b.created_at) if b.created_at else None,
        }
        for b in (
            db.query(BatchJob)
            .order_by(BatchJob.created_at.desc())
            .limit(limit)
            .all()
        )
    ]


@router.get("/batches/{batch_id}")
def get_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Get batch detail with all items."""
    batch = db.query(BatchJob).filter(BatchJob.id == batch_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    items = (
        db.query(BatchItem)
        .filter(BatchItem.batch_id == batch_id)
        .order_by(BatchItem.id)
        .all()
    )

    return {
        "id": batch.id,
        "filename": batch.filename,
        "total_properties": batch.total_properties,
        "processed_count": batch.processed_count,
        "status": batch.status,
        "created_at": str(batch.created_at) if batch.created_at else None,
        "items": [
            {
                "id": item.id,
                "apn": item.apn,
                "county": item.county,
                "state": item.state,
                "address": item.address,
                "status": item.status,
                "report_id": item.report_id,
                "error_message": item.error_message,
            }
            for item in items
        ],
    }