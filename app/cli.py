import argparse

from .db import get_engine, test_connection
from .config import DB_PATH, DATABASE_URL
from .schema import init_db
from .ingest import ingest_folder
from .transform import build_curated
from .queries import get_prices
from .features import build_features_for_ticker
from .feature_queries import get_features
from .utils import backup_db



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="app", description="Axyon data platform demo")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("info", help="Show DB path and URL")
    sub.add_parser("ping-db", help="Test SQLAlchemy connection to SQLite")
    sub.add_parser("init-db", help="Create tables in the SQLite database")
    sub.add_parser("build-curated", help="Build curated prices table from raw_prices")

    load = sub.add_parser("load-raw", help="Load CSV files into raw_prices (idempotent)")
    load.add_argument("--path", required=True, help="Folder containing CSV files (e.g., kaggleData)")
    load.add_argument("--force", action="store_true", help="Reload files even if already ingested")
    load.add_argument("--max-files", type=int, default=None, help="Limit number of CSV files (for quick testing)")

    q = sub.add_parser("query-prices", help="Query prices by ticker and optional date range")
    q.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")
    q.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD")
    q.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD")
    q.add_argument("--limit", type=int, default=20, help="Max rows to print")

    bf = sub.add_parser("build-features", help="Build features_daily from curated prices")
    bf.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")
   
    fq = sub.add_parser("query-features", help="Query features_daily by ticker and optional date range")
    fq.add_argument("--ticker", required=True, help="Ticker symbol (e.g., AACB)")
    fq.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD")
    fq.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD")
    fq.add_argument("--limit", type=int, default=20, help="Max rows to print")

    sub.add_parser("backup-db", help="Create a timestamped copy of the SQLite DB in ./backups")


    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "info":
        print(f"DB_PATH: {DB_PATH}")
        print(f"DATABASE_URL: {DATABASE_URL}")
        return 0

    if args.command == "ping-db":
        engine = get_engine(echo=False)
        test_connection(engine)
        print("OK: DB connection works")
        return 0

    if args.command == "init-db":
        init_db(echo=False)
        print("OK: tables created (if not already present)")
        return 0

    if args.command == "load-raw":
        ingest_folder(folder=args.path, force=args.force, max_files=args.max_files)
        return 0

    if args.command == "build-curated":
        build_curated(reset=True)
        print("OK: curated prices built")
        return 0

    if args.command == "query-prices":
        rows = get_prices(
            ticker=args.ticker,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
        )

        print("ticker,date,open,high,low,close")
        for r in rows:
            print(f"{r.ticker},{r.date},{r.open},{r.high},{r.low},{r.close}")

        print(f"rows_returned: {len(rows)}")
        return 0
    if args.command == "build-features":
       n = build_features_for_ticker(args.ticker)
       print(f"OK: features built for {args.ticker}, rows_upserted={n}")
       return 0
    
    if args.command == "query-features":
       rows = get_features(
              ticker=args.ticker,
              date_from=args.date_from,
              date_to=args.date_to,
              limit=args.limit,
       )

       print("ticker,date,return_1d,range_pct,ma_5,ma_20")
       for r in rows:
              print(f"{r.ticker},{r.date},{r.return_1d},{r.range_pct},{r.ma_5},{r.ma_20}")

       print(f"rows_returned: {len(rows)}")
       return 0

    if args.command == "backup-db":
       path = backup_db()
       print(f"OK: backup created at {path}")
       return 0




    return 1
