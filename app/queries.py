from __future__ import annotations

from sqlalchemy import select

from .db import get_session
from .models import Price


def get_prices(
    ticker: str,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
) -> list[Price]:
    stmt = select(Price).where(Price.ticker == ticker)

    if date_from:
        stmt = stmt.where(Price.date >= date_from)
    if date_to:
        stmt = stmt.where(Price.date <= date_to)

    stmt = stmt.order_by(Price.date.asc()).limit(limit)

    session = get_session(echo=False)
    try:
        return session.scalars(stmt).all()
    finally:
        session.close()
