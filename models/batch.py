from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class BatchJob(Base):
    """One row per CSV upload batch."""
    __tablename__ = "batch_jobs"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    total_properties = Column(Integer, nullable=False, default=0)
    processed_count = Column(Integer, nullable=False, default=0)
    status = Column(String, nullable=False, default="processing")  # processing / completed / failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    items = relationship("BatchItem", back_populates="batch", order_by="BatchItem.id")


class BatchItem(Base):
    """One row per APN inside a batch."""
    __tablename__ = "batch_items"

    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(Integer, ForeignKey("batch_jobs.id"), nullable=False, index=True)
    apn = Column(String, nullable=False)
    county = Column(String, nullable=True)
    state = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    address = Column(String, nullable=True)
    status = Column(String, nullable=False, default="pending")  # pending / processing / completed / failed
    report_id = Column(Integer, ForeignKey("apn_reports.id"), nullable=True)
    error_message = Column(String, nullable=True)

    batch = relationship("BatchJob", back_populates="items")
