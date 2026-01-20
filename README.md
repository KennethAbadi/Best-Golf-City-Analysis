# Best Golf City Analysis

Analyze and rank U.S. cities for golf using Teeradar course data plus state golfability metadata. The pipeline fetches raw course pages, consolidates them, computes per-city metrics, and visualizes results in a notebook.


## Quickstart
1) Install deps (Python 3.10+ recommended):
```bash
pip install -r requirements.txt
```
2) Fetch raw Teeradar pages (requires API key in env `TEERADAR_API_KEY` or `secrets/TEERADAR_API_KEY.txt`):
```bash
python scripts/fetch_teeradar.py --out-dir data/raw --limit 100 --offset 0
```
3) Consolidate raw pages into Parquet / NDJSON / SQLite:
```bash
python scripts/consolidate_data.py \
  --raw-dir data/raw \
  --out-parquet data/processed/teeradar_courses.parquet \
  --out-ndjson data/processed/teeradar_courses.ndjson \
  --sqlite-db data/db/golf.db
```
4) Compute city metrics (optionally include state golfability):
```bash
python eda/compute_city_metrics.py \
  --courses data/processed/teeradar_courses.parquet \
  --state-golfable-csv data/state_golfable_year_round.csv \
  --output data/processed/city_golf_metrics.parquet \
  --csv-out outputs/city_golf_metrics.csv
```
5) Explore visuals and comparisons in the notebook:
- Open notebooks/golf_city_analysis.ipynb and run all cells to see maps, heatmaps, ranking bars, and state-level tables.

## Findings
- Overall Orlando, Florida has been deemed the best city to live for golf in the US, with Scottsdale, Arizona trailing behind with only a minor difference in score.
- I was expecting to find more cities from california to be within the top 20 best cities but based on the analysis that I have done, this wouldnt be the case. They are still well placed in the top 70 best cities though.
- It was expected that the top 10 cities would most likely be the same regardless of whether the state is golfable year round and this was found to be true. This could not be the same for the top 10-20 cities where there was changes in their rankings due to the removal of state_golfable as a variable

## Improvements
- Provide more information with regards to tee times (Cost, General Availability) and whether or not the golf club is Private or not.
- Find a more equal/meaningful weighing with regards to the scoring due to it being kind of arbitrary. Depends on the users wants and needs with regards to what they want in a city (preference to private/public courses, Yardage preference, etc )
- Potentially include a radius/range that includes other cities based on how much the user is willing to commute. Example: Scottsdale and Arizona is relatively close so if the user is willing to commute then these scores would be combined/aggregated leading to a higher overall score.

## Repository layout
- scripts/fetch_teeradar.py — fetch paginated Teeradar course data, saves raw JSON.
- scripts/consolidate_data.py — dedupe and merge raw pages into Parquet/NDJSON/SQLite outputs.
- eda/compute_city_metrics.py — aggregate per-city metrics, normalize via MinMax scaling, and produce composite scores/ranks.
- notebooks/golf_city_analysis.ipynb — interactive visuals for city and state rankings.
- data/processed/ — generated Parquet/NDJSON city/course tables.
- data/state_golfable_year_round.csv — optional state-level golfability flag.
- data/uscities.csv — city coordinates used for mapping.
- outputs/city_golf_metrics.csv — sample exported metrics.

## Notes
- State metrics in the notebook aggregate total courses and ratings counts, compute average rating, and add an aggregate score combining volume and quality.
- If you re-run fetching, clear or version `data/raw` to avoid mixing datasets.
- For custom scoring weights, edit the `weights` argument when calling `compute_metrics` inside the notebook or script.
