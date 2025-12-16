"""
Fetch Teeradar course pages and save raw JSON responses.
Handles pagination, rate limiting, and server errors.
"""
import argparse
import json
import os
import time
from datetime import datetime
from typing import Optional

import requests


def load_api_key(path: str = "secrets/TEERADAR_API_KEY.txt") -> Optional[str]:
    key = os.environ.get("TEERADAR_API_KEY")
    if key:
        return key.strip()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            k = f.read().strip()
            return k or None
    return None


def save_raw_response(payload: dict, out_dir: str, offset: int):
    os.makedirs(out_dir, exist_ok=True)
    filename = os.path.join(out_dir, f"teeradar_page_{offset}.json")
    wrapped = {
        "fetched_at": datetime.now().isoformat() + "Z",
        "offset": offset,
        "payload": payload,
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)
    print("Saved:", filename)


def fetch_pages(api_key: str, min_rating: Optional[float], offset: int, limit: int, max_pages: Optional[int], out_dir: str):
    base = "http://teeradar.online/api/v1/courses.php"
    headers = {"X-API-Key": api_key}
    page = 0
    while True:
        params = {"country": "United States", "limit": limit, "offset": offset}
        if min_rating is not None:
            params["min_rating"] = min_rating
        print(f"Fetching offset {offset} (page {page+1})")
        try:
            resp = requests.get(base, headers=headers, params=params, timeout=15)
        except Exception as e:
            print("Request failed:", e)
            time.sleep(5)
            continue
        if resp.status_code == 429:
            print("Rate limited (429). Backing off for 60s")
            time.sleep(60)
            continue
        if resp.status_code >= 500:
            print(f"Server error {resp.status_code}. Retrying in 10s")
            time.sleep(10)
            continue
        resp.raise_for_status()
        payload = resp.json()
        # defensive client-side country filtering
        courses = [c for c in payload.get("courses", []) if str(c.get("country", "")).lower() in ("united states", "us", "usa")]
        payload["courses"] = courses
        save_raw_response(payload, out_dir, offset)

        count = payload.get("count", len(courses))
        if count < limit:
            print("Last page reached (count < limit). Stopping.")
            break
        offset += limit
        page += 1
        if max_pages and page >= max_pages:
            print("Reached max_pages limit. Stopping.")
            break
        time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-rating", type=float, default=None)
    parser.add_argument("--limit", type=int, default=100, help="Number of items per page (API limit may vary)")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset for fetching pages")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after this many pages (for testing)")
    parser.add_argument("--out-dir", default="data/raw", help="Directory to save raw JSON pages")
    parser.add_argument("--api-key-file", default="secrets/TEERADAR_API_KEY.txt", help="File to read API key from if env var not set")
    args = parser.parse_args()

    api_key = load_api_key(args.api_key_file)
    if not api_key:
        print("No API key found. Please add it to environment variable TEERADAR_API_KEY or file:", args.api_key_file)
        return
    fetch_pages(api_key, args.min_rating, args.offset, args.limit, args.max_pages, args.out_dir)


if __name__ == "__main__":
    main()
