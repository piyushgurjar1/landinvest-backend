from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,        # pings before using any pooled connection — discards dead ones
    pool_size=5,
    max_overflow=10,
    pool_recycle=240,          # recycle every 4 min (before cloud DB's 5 min idle timeout kills them)
    pool_timeout=30,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 60,      # send TCP keepalive after 60s idle
        "keepalives_interval": 10,  # retry keepalive every 10s
        "keepalives_count": 5,      # give up after 5 failed keepalives
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()