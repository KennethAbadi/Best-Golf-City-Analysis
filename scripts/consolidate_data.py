#!/usr/bin/env python3
"""
Consolidate Teeradar raw pages into a single Parquet/NDJSON file and/or SQLite DB.
Usage examples:
  python scripts/consolidate_data.py --raw-dir data/raw --out-parquet data/processed/teeradar_courses.parquet
  python scripts/consolidate_data.py --raw-dir data/raw --out-parquet data/processed/teeradar_courses.parquet --out-ndjson data/processed/teeradar_courses.ndjson --sqlite-db data/db/golf.db

"""
import argparse
import glob
import json
import os
import sqlite3
from datetime import datetime
from tempfile import NamedTemporaryFile

import pandas as pd


def read_raw_courses(raw_dir: str):
    files = sorted(glob.glob(os.path.join(raw_dir, "teeradar_page_*.json")))
    if not files:
        print("No raw files found in", raw_dir)
        return []

    rows = []
    for fname in files:
        with open(fname, "r", encoding="utf-8") as f:
            wrapped = json.load(f)
        fetched_at = wrapped.get("fetched_at")
        offset = wrapped.get("offset")
        payload = wrapped.get("payload", {}) or {}
        for c in payload.get("courses", []):
            # attach provenance metadata
            c["_fetched_at"] = fetched_at
            c["_offset"] = offset
            c["_raw_file"] = os.path.basename(fname)
            rows.append(c)
    return rows


def to_parquet_atomic(df: pd.DataFrame, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with NamedTemporaryFile(delete=False, dir=os.path.dirname(out_path), suffix=".parquet") as tmp:
        tmp_name = tmp.name
    df.to_parquet(tmp_name, index=False)
    os.replace(tmp_name, out_path)
    print("Wrote Parquet:", out_path)


def to_ndjson_atomic(records, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, out_path)
    print("Wrote NDJSON:", out_path)


def to_sqlite_replace(df: pd.DataFrame, db_path: str, table_name: str = "teeradar_courses"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    if "course_id" in df.columns:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_course_id ON {table_name}(course_id)")
            conn.commit()
        except Exception as e:
            print("Warning: could not create index:", e)
    conn.close()
    print("Wrote SQLite DB:", db_path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", default="data/raw", help="Directory with teeradar_page_*.json")
    p.add_argument("--out-parquet", default=None, help="Output Parquet path (optional)")
    p.add_argument("--out-ndjson", default=None, help="Output NDJSON path (optional)")
    p.add_argument("--sqlite-db", default=None, help="Output SQLite DB path (optional)")
    p.add_argument("--dedupe-key", default="course_id", help="Unique key to dedupe by (default: course_id)")
    args = p.parse_args()

    rows = read_raw_courses(args.raw_dir)
    if not rows:
        print("No data to process. Exiting.")
        return

    df = pd.DataFrame(rows)

    # Normalize numeric fields
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    if "ratings_count" in df.columns:
        df["ratings_count"] = pd.to_numeric(df["ratings_count"], errors="coerce").fillna(0).astype(int)

    # Deduplication: keep latest by _fetched_at if possible
    if args.dedupe_key in df.columns:
        if "_fetched_at" in df.columns:
            df["_fetched_at"] = pd.to_datetime(df["_fetched_at"], errors="coerce")
            df = df.sort_values(by=args.dedupe_key).drop_duplicates(subset=args.dedupe_key, keep="last")
        else:
            df = df.drop_duplicates(subset=args.dedupe_key, keep="last")
        print("Deduplicated on", args.dedupe_key, "-> rows:", len(df))

    # Outputs
    if args.out_parquet:
        to_parquet_atomic(df, args.out_parquet)
    if args.out_ndjson:
        # remove NaN for JSON; convert records
        recs = json.loads(df.to_json(orient="records", force_ascii=False))
        to_ndjson_atomic(recs, args.out_ndjson)
    if args.sqlite_db:
        to_sqlite_replace(df, args.sqlite_db)

    print("All done. Processed", len(df), "unique courses.")


if __name__ == "__main__":
    main()