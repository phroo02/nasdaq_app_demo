from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import get_engine


def build_curated(reset: bool = True) -> None:
    engine: Engine = get_engine(echo=False)

    # Uses a window function to keep 1 row per (ticker, date)
    insert_sql = text("""
    INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close)
    SELECT ticker, date, open, high, low, close
    FROM (
        SELECT
            ticker, date, open, high, low, close,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, date
                ORDER BY ingested_at DESC, id DESC
            ) AS rn
        FROM raw_prices
        WHERE
            open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            AND high >= low
            AND open > 0
            AND close > 0
    )
    WHERE rn = 1;
    """)

    with engine.begin() as conn:
        if reset:
            conn.execute(text("DELETE FROM prices;"))
        conn.execute(insert_sql)

        raw_cnt = conn.execute(text("SELECT COUNT(*) FROM raw_prices;")).scalar_one()
        curated_cnt = conn.execute(text("SELECT COUNT(*) FROM prices;")).scalar_one()
        distinct_pairs = conn.execute(
            text("SELECT COUNT(*) FROM (SELECT DISTINCT ticker, date FROM raw_prices);")
        ).scalar_one()

    print(f"raw_prices rows:        {raw_cnt}")
    print(f"raw distinct (t,d):     {distinct_pairs}")
    print(f"curated prices rows:    {curated_cnt}")
