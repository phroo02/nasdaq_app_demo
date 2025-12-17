from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .config import DATABASE_URL

def get_engine(echo: bool = False) -> Engine:
    return create_engine(DATABASE_URL, echo=echo)

def get_session(echo: bool = False) -> Session:
    engine = get_engine(echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return SessionLocal()

def test_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
