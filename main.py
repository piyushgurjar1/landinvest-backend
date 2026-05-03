import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import engine, Base, SessionLocal
from routers import auth, apn, csv_upload, chat
from models.user import User
from models.batch import BatchJob, BatchItem  # noqa: F401 — ensure table creation
from utils.auth import hash_password

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="LandInvestAI API",
    description="AI-powered land acquisition due diligence tool",
    version="1.0.0",
)


import asyncio
from sqlalchemy import text
from models.apn import APNReport
from routers.csv_upload import _run_batch_analysis
from routers.apn import _run_single_analysis

# Create demo user, run migrations, and recover stalled tasks on startup
@app.on_event("startup")
async def startup_event():
    # 0. DB migrations — add columns if missing
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE \"Users-prod\" ADD COLUMN IF NOT EXISTS role VARCHAR DEFAULT 'user'"))
            conn.execute(text("ALTER TABLE \"Users-prod\" ADD COLUMN IF NOT EXISTS is_approved BOOLEAN DEFAULT FALSE"))
            # Auto-approve all existing users (prevent lockout)
            conn.execute(text("UPDATE \"Users-prod\" SET is_approved = TRUE WHERE is_approved IS NULL OR is_approved = FALSE"))
            conn.commit()
            print("✅ User table migration complete")
        except Exception as e:
            print(f"⚠️ User migration note: {e}")

    db = SessionLocal()
    try:
        # 1. Create / update demo user
        demo_email = os.getenv("DEMO_EMAIL", "demo@gmail.com")
        demo_password = os.getenv("DEMO_PASSWORD", "landinvestai")
        demo_name = os.getenv("DEMO_NAME", "Demo User")

        existing = db.query(User).filter(User.email == demo_email).first()
        if not existing:
            user = User(
                email=demo_email,
                password=hash_password(demo_password),
                name=demo_name,
                role="admin",
                is_approved=True,
            )
            db.add(user)
            db.commit()
            print(f"✅ Demo admin created: {demo_email} / {demo_password}")
        else:
            # Ensure demo user is admin and approved
            existing.role = "admin"
            existing.is_approved = True
            db.commit()
            print(f"✅ Demo admin exists: {demo_email} / {demo_password}")

        # 2. Recover stalled batches
        stalled_items = db.query(BatchItem).filter(BatchItem.status == "processing").all()
        recovered_batches = set()
        for item in stalled_items:
            item.status = "pending"
            recovered_batches.add(item.batch_id)
        if stalled_items:
            db.commit()
            print(f"✅ Recovered {len(stalled_items)} stalled batch items to pending")

        # Start a background task for any batch that is still processing
        stalled_batches = db.query(BatchJob).filter(BatchJob.status == "processing").all()
        for batch in stalled_batches:
            print(f"✅ Auto-resuming stalled batch: {batch.id}")
            asyncio.create_task(_run_batch_analysis(batch.id))

        # 3. Recover stalled single APN lookups
        stalled_reports = db.query(APNReport).filter(APNReport.status == "processing").all()
        for report in stalled_reports:
            print(f"✅ Auto-resuming stalled single APN lookup: {report.apn}")
            report.status = "queued"
            asyncio.create_task(_run_single_analysis(
                report.id, report.apn, report.county, report.state,
                None, None, report.address
            ))
        if stalled_reports:
            db.commit()

    except Exception as e:
        print(f"❌ Error during startup recovery: {e}")
    finally:
        db.close()

# CORS — allow frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://landinvest-frontend.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(auth.router)
app.include_router(apn.router)
app.include_router(csv_upload.router)
app.include_router(chat.router)


@app.get("/")
def root():
    return {"message": "LandInvestAI API is running", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "healthy"}
