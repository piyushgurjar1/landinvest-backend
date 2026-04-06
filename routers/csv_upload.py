import io
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from database import get_db
from models.parcel_data import ParcelData
from utils.auth import get_current_user

router = APIRouter(prefix="/api/csv", tags=["CSV Upload"])

# Expected CSV columns → DB column mapping
CSV_TO_DB = {
    "apn": "apn",
    "state": "state",
    "county": "county",
    "latitude": "latitude",
    "longitude": "longitude",
    "address": "address",
    "lot size": "lot_size",
    "zoning": "zoning",
    "assessment year": "assessment_year",
    "total assessed value": "total_assessed_value",
    "market value year": "market_value_year",
    "total market value": "total_market_value",
    "flood risk": "flood_risk",
    "environmental hazard status": "environmental_hazard_status",
    "bidding start value": "bidding_start_value",
}


@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """Upload a CSV of parcel data. Upserts by APN (update if exists)."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    contents = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")

    # Normalize column names to lowercase for matching
    df.columns = [c.strip().lower() for c in df.columns]

    expected_cols = set(CSV_TO_DB.keys())
    actual_cols = set(df.columns)
    missing_cols = expected_cols - actual_cols

    if missing_cols:
        raise HTTPException(
            status_code=400, 
            detail=f"CSV is missing the following required columns: {', '.join(sorted(missing_cols))}"
        )

    imported = 0
    skipped = 0

    for _, row in df.iterrows():
        apn_val = str(row.get("apn", "")).replace("-", "").replace(" ", "").strip()
        if not apn_val:
            skipped += 1
            continue

        record = {}
        for csv_col, db_col in CSV_TO_DB.items():
            val = row.get(csv_col)
            if pd.notna(val) and val != "":
                # Clean numeric fields if they come in as string with commas
                if db_col in ("total_assessed_value", "total_market_value", "bidding_start_value", "latitude", "longitude"):
                    if isinstance(val, str):
                        try:
                            val = float(val.replace(",", "").replace("$", "").strip())
                        except ValueError:
                            pass # fallback to original if parsing fails
                if db_col == "assessment_year" or db_col == "market_value_year":
                    if isinstance(val, float):
                        val = str(int(val)) # avoid 2026.0

                record[db_col] = val
            else:
                record[db_col] = None

        record["apn"] = apn_val

        # Upsert: insert or update on conflict
        stmt = pg_insert(ParcelData).values(**record)
        stmt = stmt.on_conflict_do_update(
            index_elements=["apn"],
            set_={k: v for k, v in record.items() if k != "apn"},
        )
        db.execute(stmt)
        imported += 1

    db.commit()

    return {
        "message": f"Successfully imported {imported} parcels",
        "imported": imported,
        "skipped": skipped,
        "total_rows": len(df),
    }
