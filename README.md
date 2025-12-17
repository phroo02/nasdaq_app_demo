# nasdaq_app_demo

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

Commands

Create tables:

python -m app init-db


Load raw CSVs:

python -m app load-raw --path kaggleData


Build curated table:

python -m app build-curated


Query prices:

python -m app query-prices --ticker AACB --from 2025-04-07 --to 2025-04-14 --limit 50

python -m app build-features --ticker AACB

python -m app query-features --ticker AACB --from 2025-04-07 --to 2025-04-22 --limit 50

python -m app backup-db



---

## What to emphasize in the meeting (talking points)

### SQL + Python interaction
- Ingestion uses chunked inserts and transactions (`engine.begin()`).
- Query layer uses SQLAlchemy ORM `Session` to return model objects.

### Data modeling
- `prices` uses composite primary key `(ticker, date)` to enforce uniqueness.
- Constraints ensure data quality (`high >= low`, `open > 0`, `close > 0`).
- Indexes added for common access patterns (ticker/date, date).

### Data manipulation with Python
- Curated build uses window function + dedupe logic.
- Feature generation shows a “dataset generation tool” pattern: compute → store → query.

### Reliability mindset
- Idempotent ingestion with `ingestion_files`.
- DB backup command simulates disaster recovery for SQLite.
- Transactions prevent partial writes.

---

## Quick “demo script” for the interview
Run these in order:

```bash
python -m app init-db
python -m app load-raw --path kaggleData --max-files 3
python -m app build-curated
python -m app query-prices --ticker AACB --limit 5
python -m app build-features --ticker AACB
python -m app query-features --ticker AACB --limit 5
python -m app backup-db
