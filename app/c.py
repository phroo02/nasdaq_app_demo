from __future__ import annotations

from sqlalchemy import select

from .db import get_session
from .models import FeatureDaily


def get_features(
    ticker: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
) -> list[FeatureDaily]:
    stmt = select(FeatureDaily).where(FeatureDaily.ticker == ticker)

    if date_from:
        stmt = stmt.where(FeatureDaily.date >= date_from)
    if date_to:
        stmt = stmt.where(FeatureDaily.date <= date_to)

    stmt = stmt.order_by(FeatureDaily.date.asc()).limit(limit)

    session = get_session(echo=False)
    try:
        return session.scalars(stmt).all()
    finally:
        session.close()
