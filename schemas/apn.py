from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime


class APNLookupRequest(BaseModel):
    apn: str
    county: Optional[str] = None
    state: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    address: Optional[str] = None


class APNReportResponse(BaseModel):
    id: int
    apn: str
    county: Optional[str] = None
    state: Optional[str] = None
    acreage: Optional[float] = None
    assessed_value: Optional[float] = None
    status: str
    deal_score: Optional[int] = None
    bid_ceiling: Optional[float] = None
    estimated_market_value: Optional[float] = None
    report_data: Optional[Any] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
