from sqlalchemy import Column, Integer, String, Text, Boolean
from database import Base

class User(Base):
    __tablename__ = "Users-prod"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=True)
    email = Column(Text, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
    role = Column(String, default="user")        # "admin" or "user"
    is_approved = Column(Boolean, default=False)  # requires admin approval

