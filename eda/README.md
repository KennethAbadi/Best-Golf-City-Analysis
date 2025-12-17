# EDA helper files

This folder contains helper scripts and data for the golf-city EDA.

State golfability file
- `data/state_golfable_year_round.csv` — conservative indicator (`golfable_year_round` 0/1) for whether the state generally supports year-round golf.

Usage
- Pass the CSV file to `eda/compute_city_metrics.py` with `--state-golfable-csv data/state_golfable_year_round.csv`. The script will merge the indicator into the city-level table as `state_golfable` and include it in scoring with default weight.
