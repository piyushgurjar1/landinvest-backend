import io
import asyncio
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, BackgroundTasks
from sqlalchemy.orm import Session
import pandas as pd

from database import get_db, SessionLocal
from models.batch import BatchJob, BatchItem
from models.apn import APNReport
from utils.auth import get_current_user
from gemini_service import analyze_apn

router = APIRouter(prefix="/api/csv", tags=["CSV Upload"])
_logger = logging.getLogger(__name__)


def _safe_close(db):
    """Close a DB session without ever raising. Invalidates dead connections."""
    try:
        db.close()
    except Exception:
        try:
            db.invalidate()
        except Exception:
            pass


@router.post("/upload")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Upload a CSV to start a batch analysis. Returns immediately with batch ID."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    contents = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")

    # Normalize column names
    df.columns = [c.strip().lower().replace("#", "").replace(" ", "_") for c in df.columns]

    if "apn" not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="CSV is missing the required 'apn' column"
        )

    # Parse rows — only APN is required
    items_data = []
    skipped = 0
    for _, row in df.iterrows():
        apn_val = str(row.get("apn", "")).replace("-", "").replace(" ", "").strip()
        if not apn_val:
            skipped += 1
            continue

        items_data.append({
            "apn": apn_val,
            "county": _clean_str(row.get("county")),
            "state": _clean_str(row.get("state")),
            "latitude": _clean_float(row.get("latitude")),
            "longitude": _clean_float(row.get("longitude")),
            "address": _clean_str(row.get("address")),
        })

    if not items_data:
        raise HTTPException(status_code=400, detail="No valid APN rows found in CSV")

    # Create batch job
    batch = BatchJob(
        filename=file.filename,
        total_properties=len(items_data),
        processed_count=0,
        status="processing",
    )
    db.add(batch)
    db.flush()  # get batch.id

    # Create batch items
    for item_data in items_data:
        db.add(BatchItem(batch_id=batch.id, **item_data))

    db.commit()
    batch_id = batch.id

    # Start background processing
    background_tasks.add_task(_run_batch_analysis, batch_id)

    return {
        "batch_id": batch_id,
        "filename": file.filename,
        "total_properties": len(items_data),
        "skipped": skipped,
        "status": "processing",
    }


def _clean_str(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def _clean_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


async def _run_batch_analysis(batch_id: int):
    """Process each APN in a batch sequentially. Each item gets its own DB session.

    Design principles:
    - Fresh DB session per item → no SSL drops killing the batch
    - Each item is fully isolated → one failure never affects others
    - Batch always completes → status is always updated to 'completed' or 'completed_with_errors'
    """

    # 1. Load item IDs with a short-lived session (just reading, takes <1s)
    db = SessionLocal()
    try:
        batch = db.query(BatchJob).filter(BatchJob.id == batch_id).first()
        if not batch:
            return
        item_ids = [
            row.id for row in
            db.query(BatchItem.id)
            .filter(BatchItem.batch_id == batch_id, BatchItem.status == "pending")
            .order_by(BatchItem.id)
            .all()
        ]
    finally:
        _safe_close(db)

    if not item_ids:
        _safe_update_batch_status(batch_id, "completed")
        return

    # 2. Process each item with its own DB session
    for item_id in item_ids:
        await _process_single_batch_item(batch_id, item_id)

    # 3. Mark batch as completed (always — even if some items failed)
    _safe_update_batch_status(batch_id, "completed")


async def _process_single_batch_item(batch_id: int, item_id: int):
    """Process one batch item with full isolation. Never raises."""

    # Read item data with a fresh session
    db = SessionLocal()
    try:
        item = db.query(BatchItem).filter(BatchItem.id == item_id).first()
        if not item or item.status != "pending":
            return
        # Cache fields before closing session
        apn = item.apn
        county = item.county or "Unknown"
        state = item.state or "Unknown"
        latitude = item.latitude
        longitude = item.longitude
        address = item.address
        # Mark as processing
        item.status = "processing"
        db.commit()
    except Exception as e:
        _logger.error("Failed to read/mark batch item %s: %s", item_id, e)
        return
    finally:
        _safe_close(db)

    # Run analysis (NO db session held during this long operation)
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
            timeout=660.0,
        )
    except asyncio.TimeoutError:
        error_message = "Analysis timed out after 11 minutes"
        _logger.error("Batch item %s (APN %s) timed out", item_id, apn)
    except Exception as e:
        error_message = str(e)[:500]
        _logger.error("Batch item %s (APN %s) failed: %s", item_id, apn, e)

    # Write results with a FRESH session
    db = SessionLocal()
    try:
        item = db.query(BatchItem).filter(BatchItem.id == item_id).first()
        if not item:
            return

        if report_data:
            # Save report
            parcel = report_data.get("basic_parcel_info", {})
            market_val = report_data.get("estimated_market_value", {})
            bid = report_data.get("auction_bid_ceiling", {})
            score = report_data.get("deal_score", {})

            report = APNReport(
                apn=apn,
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
            db.add(report)
            db.flush()
            item.status = "completed"
            item.report_id = report.id
        else:
            item.status = "failed"
            item.error_message = error_message or "Unknown error"

        # Update batch processed count
        batch = db.query(BatchJob).filter(BatchJob.id == batch_id).first()
        if batch:
            batch.processed_count += 1

        db.commit()
    except Exception as e:
        _logger.error("Failed to save results for batch item %s: %s", item_id, e)
        try:
            db.rollback()
            item = db.query(BatchItem).filter(BatchItem.id == item_id).first()
            if item:
                item.status = "failed"
                item.error_message = f"DB save error: {str(e)[:200]}"
            batch = db.query(BatchJob).filter(BatchJob.id == batch_id).first()
            if batch:
                batch.processed_count += 1
            db.commit()
        except Exception:
            _logger.error("Failed even to mark item %s as failed", item_id)
    finally:
        _safe_close(db)


def _safe_update_batch_status(batch_id: int, status: str):
    """Update batch status with its own session. Never raises."""
    db = SessionLocal()
    try:
        batch = db.query(BatchJob).filter(BatchJob.id == batch_id).first()
        if batch:
            batch.status = status
            db.commit()
    except Exception as e:
        _logger.error("Failed to update batch %s status to %s: %s", batch_id, status, e)
    finally:
        _safe_close(db)

