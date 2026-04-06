from sqlalchemy import Column, Integer, String, Text
from database import Base

class User(Base):
    __tablename__ = "Users-prod"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=True)
    email = Column(Text, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
