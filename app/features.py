from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .db import get_session
from .models import Price, FeatureDaily


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def build_features_for_ticker(ticker: str) -> int:
    """
    Build features for a single ticker from curated prices and upsert into features_daily.
    Returns number of rows upserted.
    """
    session = get_session(echo=False)
    try:
        prices = session.scalars(
            select(Price).where(Price.ticker == ticker).order_by(Price.date.asc())
        ).all()

        if not prices:
            return 0

        close_history: list[float] = []
        prev_close: float | None = None

        to_upsert: list[dict] = []

        for p in prices:
            close = float(p.close)
            close_history.append(close)

            # 1d return
            ret_1d = None
            if prev_close is not None and prev_close != 0:
                ret_1d = close / prev_close - 1.0

            # range percent
            range_pct = None
            if close != 0:
                range_pct = (float(p.high) - float(p.low)) / close

            # moving averages
            ma_5 = _mean(close_history[-5:]) if len(close_history) >= 5 else None
            ma_20 = _mean(close_history[-20:]) if len(close_history) >= 20 else None

            to_upsert.append(
                {
                    "ticker": p.ticker,
                    "date": p.date,
                    "return_1d": ret_1d,
                    "range_pct": range_pct,
                    "ma_5": ma_5,
                    "ma_20": ma_20,
                }
            )

            prev_close = close

        stmt = sqlite_insert(FeatureDaily).values(to_upsert)
        # Upsert on (ticker, date)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker", "date"],
            set_={
                "return_1d": stmt.excluded.return_1d,
                "range_pct": stmt.excluded.range_pct,
                "ma_5": stmt.excluded.ma_5,
                "ma_20": stmt.excluded.ma_20,
            },
        )

        session.execute(stmt)
        session.commit()

        return len(to_upsert)
    finally:
        session.close()
