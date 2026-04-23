import logging
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL

_logger = logging.getLogger(__name__)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,        # pings before using any pooled connection — discards dead ones
    pool_size=5,
    max_overflow=10,
    pool_recycle=120,           # recycle every 2 min — aggressive, prevents cloud DB idle kills
    pool_timeout=30,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,      # send TCP keepalive after 30s idle (was 60s — too long)
        "keepalives_interval": 10,  # retry keepalive every 10s
        "keepalives_count": 3,      # give up after 3 failed keepalives (was 5)
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """Yield a DB session and guarantee crash-proof cleanup.

    The finally block MUST never raise — if the underlying connection
    is dead (SSL dropped), a bare db.close() throws psycopg2.OperationalError
    which crashes the ASGI app. We catch that and invalidate the connection
    so the pool discards it instead of reusing a broken socket.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception:
            # Connection is dead — invalidate it so the pool discards it
            try:
                db.invalidate()
            except Exception:
                pass
            _logger.warning("DB session close failed — connection invalidated")