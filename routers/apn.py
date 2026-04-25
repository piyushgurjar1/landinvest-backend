import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
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
_logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_close(db: Session) -> None:
    """Close a DB session without ever raising."""
    try:
        db.close()
    except Exception:
        try:
            db.invalidate()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Single APN endpoints — Background Processing
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
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Start APN analysis in background. Returns immediately with job ID."""
    normalized_apn = str(request.apn).replace("-", "").replace(" ", "").strip()
    request.apn = normalized_apn

    county = request.county or "Unknown"
    state = request.state or "Unknown"

    # 1. Create report row with status="queued" immediately
    report = APNReport(
        apn=request.apn,
        county=county,
        state=state,
        address=request.address,
        status="queued",
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    report_id = report.id

    # 2. Launch background task (runs after response is sent)
    background_tasks.add_task(
        _run_single_analysis,
        report_id,
        request.apn, county, state,
        request.latitude, request.longitude, request.address,
    )

    _logger.info("APN lookup queued: report_id=%s apn=%s", report_id, request.apn)

    # 3. Return immediately
    return {
        "id": report_id,
        "apn": request.apn,
        "status": "queued",
        "message": "Analysis started. Track progress in Reports.",
    }


async def _run_single_analysis(
    report_id: int,
    apn: str, county: str, state: str,
    latitude: str = None, longitude: str = None, address: str = None,
):
    """Background worker for single APN analysis. Never raises."""

    # Mark as processing
    db = SessionLocal()
    try:
        report = db.query(APNReport).filter(APNReport.id == report_id).first()
        if report:
            report.status = "processing"
            db.commit()
    except Exception as e:
        _logger.error("Failed to mark report %s as processing: %s", report_id, e)
    finally:
        _safe_close(db)

    # Run analysis (NO db session held)
    report_data = None
    error_message = None
    try:
        report_data = await asyncio.wait_for(
            analyze_apn(
                apn, county, state,
                parcel_info=None,
                latitude=latitude,
                longitude=longitude,
                address=address,
            ),
            timeout=1200.0,
        )
    except asyncio.TimeoutError:
        error_message = "Analysis timed out after 20 minutes"
        _logger.error("Report %s (APN %s) timed out", report_id, apn)
    except Exception as e:
        error_message = str(e)[:500]
        _logger.error("Report %s (APN %s) failed: %s", report_id, apn, e)

    # Save results with fresh session
    db = SessionLocal()
    try:
        report = db.query(APNReport).filter(APNReport.id == report_id).first()
        if not report:
            return

        if report_data:
            parcel = report_data.get("basic_parcel_info", {})
            market_val = report_data.get("estimated_market_value", {})
            bid = report_data.get("auction_bid_ceiling", {})
            score = report_data.get("deal_score", {})

            report.county = parcel.get("county") or county
            report.state = parcel.get("state") or state
            report.acreage = parcel.get("acreage")
            report.assessed_value = market_val.get("mid_estimated_value")
            report.status = "completed"
            report.report_data = report_data
            report.deal_score = score.get("score")
            report.bid_ceiling = bid.get("mid_bid_threshold")
            report.estimated_market_value = market_val.get("mid_estimated_value")
            _logger.info("Report %s completed successfully", report_id)
        else:
            report.status = "failed"
            report.error_message = error_message or "Unknown error"
            _logger.warning("Report %s failed: %s", report_id, error_message)

        db.commit()
    except Exception as e:
        _logger.error("Failed to save report %s: %s", report_id, e)
        try:
            db.rollback()
            report = db.query(APNReport).filter(APNReport.id == report_id).first()
            if report:
                report.status = "failed"
                report.error_message = f"Save error: {str(e)[:200]}"
                db.commit()
        except Exception:
            _logger.error("Failed even to mark report %s as failed", report_id)
    finally:
        _safe_close(db)


# ─────────────────────────────────────────────────────────────────────────────
# Report retrieval endpoints (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/reports/{report_id}/status")
def get_report_status(
    report_id: int,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Lightweight status check for polling."""
    report = db.query(APNReport).filter(APNReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return {
        "id": report.id,
        "status": report.status,
        "error_message": report.error_message,
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
        "address": report.address,
        "acreage": report.acreage,
        "assessed_value": report.assessed_value,
        "status": report.status,
        "error_message": report.error_message,
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
    """List reports. All statuses. Optional APN search."""
    query = db.query(APNReport)

    if search:
        search = str(search).replace("-", "").replace(" ", "").strip()
        query = query.filter(APNReport.apn.ilike(f"%{search}%"))

    reports = (
        query
        .order_by(APNReport.created_at.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": r.id,
            "apn": r.apn,
            "county": r.county,
            "state": r.state,
            "address": r.address,
            "acreage": r.acreage,
            "assessed_value": r.assessed_value,
            "status": r.status,
            "error_message": r.error_message,
            "deal_score": r.deal_score,
            "bid_ceiling": r.bid_ceiling,
            "estimated_market_value": r.estimated_market_value,
            "created_at": str(r.created_at) if r.created_at else None,
        }
        for r in reports
    ]


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