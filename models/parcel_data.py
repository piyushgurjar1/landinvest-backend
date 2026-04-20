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
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())
