import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://budget_user:budget_pass@postgres:5432/budget_analytics"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def fetch_all(query: str, params: dict | None = None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row._mapping) for row in result]
