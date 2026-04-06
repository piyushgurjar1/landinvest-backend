from sqlalchemy import Column, Integer, String, DateTime, JSON, Float
from sqlalchemy.sql import func
from database import Base


class APNReport(Base):
    __tablename__ = "apn_reports"

    id = Column(Integer, primary_key=True, index=True)
    apn = Column(String, index=True, nullable=False)
    county = Column(String, nullable=True)
    state = Column(String, nullable=True)
    acreage = Column(Float, nullable=True)
    assessed_value = Column(Float, nullable=True)
    status = Column(String, default="pending")  # pending, completed, failed
    report_data = Column(JSON, nullable=True)
    deal_score = Column(Integer, nullable=True)
    bid_ceiling = Column(Float, nullable=True)
    estimated_market_value = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
