from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.sql import func
from database import Base


class ParcelData(Base):
    """Stores CSV-uploaded parcel data for pre-populating Gemini prompts."""
    __tablename__ = "parcel_data"

    id = Column(Integer, primary_key=True, index=True)
    apn = Column(String, index=True, unique=True, nullable=False)
    state = Column(String, nullable=True)
    county = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    address = Column(String, nullable=True)
    lot_size = Column(String, nullable=True)
    zoning = Column(String, nullable=True)
    assessment_year = Column(String, nullable=True)
    total_assessed_value = Column(Float, nullable=True)
    market_value_year = Column(String, nullable=True)
    total_market_value = Column(Float, nullable=True)
    flood_risk = Column(String, nullable=True)
    environmental_hazard_status = Column(String, nullable=True)
    bidding_start_value = Column(Float, nullable=True)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())
