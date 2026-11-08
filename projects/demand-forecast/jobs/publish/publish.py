"""Publish: turns the daily forecast into what the stores order from — weekly units per store and
family in `forecast_weekly`, next to the same weeks one year earlier. Fails if the forecast is
missing or incomplete, so that a stale table is never published.
"""

import json
import sys

import pandas as pd
import pyarrow as pa

import graal

predictions = graal.read_table("forecast_predictions")
history = graal.read_table("forecast_sales_history")
if predictions.empty:
    sys.exit("forecast_predictions is empty: run the training notebook first")

expected = history["units"].isna().sum()
if len(predictions) != expected:
    sys.exit(f"forecast_predictions has {len(predictions)} rows, {expected} days are to forecast")
if (predictions["predicted_units"] < 0).any():
    sys.exit("negative forecast")

predictions["day"] = pd.to_datetime(predictions["day"])
history["day"] = pd.to_datetime(history["day"])
predictions["week"] = predictions["day"].dt.to_period("W-SUN").dt.start_time
weekly = (
    predictions.groupby(["store", "family", "week"])
    .agg(forecast_units=("predicted_units", "sum"), days=("day", "size"))
    .reset_index()
)

# Same weeks one year earlier (52 weeks, so that weekdays line up).
last_year = history.dropna(subset=["units"]).copy()
last_year["week"] = (last_year["day"] + pd.Timedelta(weeks=52)).dt.to_period("W-SUN").dt.start_time
last_year = last_year.groupby(["store", "family", "week"])["units"].sum().rename("units_last_year")
weekly = weekly.join(last_year, on=["store", "family", "week"])
weekly["forecast_units"] = weekly["forecast_units"].round(0)
weekly["model_run"] = predictions["model_run"].iloc[0]

table = pa.Table.from_pandas(weekly, preserve_index=False).cast(
    pa.schema(
        [
            ("store", pa.string()),
            ("family", pa.string()),
            ("week", pa.date32()),
            ("forecast_units", pa.float64()),
            ("days", pa.int64()),
            ("units_last_year", pa.float64()),
            ("model_run", pa.string()),
        ]
    )
)
graal.write_table(table, "forecast_weekly", mode="overwrite")

totals = weekly.groupby("store")[["forecast_units", "units_last_year"]].sum().round(0)
print(totals.to_string())
print(
    "GRAAL_RUN_RESULTS "
    + json.dumps(
        {
            "rows": len(weekly),
            "weeks": int(weekly["week"].nunique()),
            "forecast_units": float(weekly["forecast_units"].sum()),
            "model_run": weekly["model_run"].iloc[0],
        }
    )
)
