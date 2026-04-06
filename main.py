import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import engine, Base, SessionLocal
from routers import auth, apn, csv_upload
from models.user import User
from models.parcel_data import ParcelData  # noqa: F401 — ensure table creation
from utils.auth import hash_password

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="LandInvestAI API",
    description="AI-powered land acquisition due diligence tool",
    version="1.0.0",
)


# Create demo user on startup
@app.on_event("startup")
def create_demo_user():
    db = SessionLocal()
    try:
        demo_email = os.getenv("DEMO_EMAIL", "demo@gmail.com")
        demo_password = os.getenv("DEMO_PASSWORD", "landinvestai")
        demo_name = os.getenv("DEMO_NAME", "Demo User")

        existing = db.query(User).filter(User.email == demo_email).first()
        if not existing:
            user = User(
                email=demo_email,
                password=hash_password(demo_password),
                name=demo_name,
            )
            db.add(user)
            db.commit()
            print(f"✅ Demo user created: {demo_email} / {demo_password}")
        else:
            print(f"✅ Demo user exists: {demo_email} / {demo_password}")
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


@app.get("/")
def root():
    return {"message": "LandInvestAI API is running", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "healthy"}
