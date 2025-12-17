"""Compute city-level golf metrics from consolidated Teeradar courses.

This script assumes you have already consolidated raw Teeradar pages into
`data/processed/teeradar_courses.parquet` (see `scripts/consolidate_data.py`).

Outputs:
- data/processed/city_golf_metrics.parquet
- outputs/city_golf_metrics.csv

It aggregates by `city` and `state` fields present on course records, computes a
centroid derived from course locations for mapping, counts, and averages.
"""
import argparse
import os
import pandas as pd
import numpy as np
from math import radians, sin, cos, asin, sqrt


def compute_metrics(courses_df: pd.DataFrame, weights: dict = None, state_golfable_csv: str = None):
    """Aggregate per-city metrics and compute a composite score using available fields.

    The function no longer depends on lat/lon. It uses columns present in the input to
    compute aggregates and a normalized weighted score. Supported source fields (if present):
    - rating (higher better)
    - tee_fee (lower better)
    - ratings_count (higher better)
    - length_yards (higher may be preferred; depends on your preference)

    Returns a DataFrame with per-city aggregates plus 'score' and 'rank'.
    """
    # drop duplicates by course_id if present
    if "course_id" in courses_df.columns:
        courses_df = courses_df.sort_values("_fetched_at").drop_duplicates(subset="course_id", keep="last")

    # group by city/state
    group_cols = [c for c in ["city", "state"] if c in courses_df.columns]
    if not group_cols:
        raise ValueError("No city/state columns found to group by; ensure courses include 'city' or 'state'")

    # build aggregation dict dynamically based on available columns
    agg_dict = {
        "num_golf_courses": ("course_id" if "course_id" in courses_df.columns else "name", "nunique"),
    }
    if "rating" in courses_df.columns:
        agg_dict["avg_rating"] = ("rating", "mean")
    if "ratings_count" in courses_df.columns:
        agg_dict["sum_ratings_count"] = ("ratings_count", "sum")
    if "tee_fee" in courses_df.columns:
        agg_dict["median_tee_fee"] = ("tee_fee", "median")
    if "length_yards" in courses_df.columns:
        agg_dict["avg_length_yards"] = ("length_yards", "mean")

    agg = courses_df.groupby(group_cols).agg(**agg_dict).reset_index()

    # If a state golfability CSV is provided, merge it into the aggregates.
    if state_golfable_csv and os.path.exists(state_golfable_csv):
        states = pd.read_csv(state_golfable_csv)
        # normalize candidate column names to expected names
        cols_lower = {c.lower(): c for c in states.columns}
        rename_map = {}
        if 'state' in cols_lower:
            rename_map[cols_lower['state']] = 'state'
        if 'state_name' in cols_lower:
            rename_map[cols_lower['state_name']] = 'state_name'
        if 'golfable_year_round' in cols_lower:
            rename_map[cols_lower['golfable_year_round']] = 'golfable_year_round'
        elif 'golfable' in cols_lower:
            rename_map[cols_lower['golfable']] = 'golfable_year_round'
        if rename_map:
            states = states.rename(columns=rename_map)
        if 'golfable_year_round' in states.columns:
            states['golfable_year_round'] = states['golfable_year_round'].astype(int)
            if 'state' in agg.columns and 'state' in states.columns:
                agg = agg.merge(states[['state', 'golfable_year_round']], on='state', how='left')
            elif 'state' in agg.columns and 'state_name' in states.columns:
                agg = agg.merge(states[['state_name', 'golfable_year_round']], left_on='state', right_on='state_name', how='left')
            agg['state_golfable'] = agg['golfable_year_round'].fillna(0).astype(int)
            agg.drop(columns=[c for c in ['golfable_year_round', 'state_name'] if c in agg.columns], inplace=True)

    # prepare for scoring: collect available scoring columns
    score_cols = []
    invert_cols = set()  # metrics where lower is better
    if "avg_rating" in agg.columns:
        score_cols.append("avg_rating")
    if "median_tee_fee" in agg.columns:
        score_cols.append("median_tee_fee")
        invert_cols.add("median_tee_fee")
    if "num_golf_courses" in agg.columns:
        score_cols.append("num_golf_courses")
    if "sum_ratings_count" in agg.columns:
        score_cols.append("sum_ratings_count")
    if "avg_length_yards" in agg.columns:
        score_cols.append("avg_length_yards")
    if "state_golfable" in agg.columns:
        score_cols.append("state_golfable")

    # default weights (include state_golfable if available)
    default_weights = {"avg_rating": 0.35, "num_golf_courses": 0.25, "median_tee_fee": 0.2, "sum_ratings_count": 0.1, "avg_length_yards": 0.0, "state_golfable": 0.1}
    if weights is None:
        weights = default_weights

    # keep only available weights
    weights = {k: v for k, v in weights.items() if k in score_cols}
    total_weight = sum(weights.values()) or 1.0

    # build matrix for scaling
    if score_cols:
        from sklearn.preprocessing import MinMaxScaler
        matrix = agg[score_cols].astype(float).copy()
        # invert metrics where lower is better
        for col in invert_cols:
            matrix[col] = matrix[col].max() - matrix[col]
        matrix = matrix.fillna(matrix.mean())
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(matrix)
        scaled_df = pd.DataFrame(scaled, columns=score_cols, index=agg.index)

        # compute weighted score
        score = 0
        for c in score_cols:
            w = weights.get(c, 0) / total_weight
            score = score + scaled_df[c] * w
        agg["score"] = score
    else:
        agg["score"] = 0

    agg = agg.sort_values("score", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1

    # rounding for readability
    if "avg_rating" in agg.columns:
        agg["avg_rating"] = agg["avg_rating"].round(2)
    if "median_tee_fee" in agg.columns:
        agg["median_tee_fee"] = agg["median_tee_fee"].round(2)
    if "avg_length_yards" in agg.columns:
        agg["avg_length_yards"] = agg["avg_length_yards"].round(1)

    return agg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--courses", default="data/processed/teeradar_courses.parquet", help="Consolidated courses parquet")
    parser.add_argument("--output", default="data/processed/city_golf_metrics.parquet")
    parser.add_argument("--csv-out", default="outputs/city_golf_metrics.csv")
    parser.add_argument("--state-golfable-csv", default=None, help="Optional CSV of states with golfable_year_round (columns: state,golfable_year_round)")
    args = parser.parse_args()

    df = pd.read_parquet(args.courses)
    metrics = compute_metrics(df, state_golfable_csv=args.state_golfable_csv)

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    metrics.to_parquet(args.output, index=False)
    os.makedirs(os.path.dirname(args.csv_out) or '.', exist_ok=True)
    metrics.to_csv(args.csv_out, index=False)
    print("Saved city metrics to:", args.output, "and", args.csv_out)


if __name__ == "__main__":
    main()
