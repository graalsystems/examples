"""Prepare: two years of daily unit sales for six stores and three product families, into the table
`forecast_sales_history` of the tenant's Iceberg catalog.

The sales are SYNTHETIC (seeded, reproducible) but shaped like retail data: a trend, a weekly cycle
(Saturday peak, Sunday closing for some stores), a yearly cycle that differs per family, promotions
and public holidays. The training job learns these patterns back.

The next HORIZON days are written too, with their calendar (promotion planned, store closed) and
`units` left empty: they are the days the training job forecasts.

Parameters (environment): END_DATE (last day with sales, default 2025-12-31), DAYS (history,
default 730), HORIZON (days to forecast, default 28), SEED (default 11).
"""

import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa

import graal

STORES = {"Lyon": 1.3, "Nantes": 1.0, "Lille": 1.1, "Rennes": 0.8, "Dijon": 0.6, "Tours": 0.7}
CLOSED_ON_SUNDAY = {"Rennes", "Dijon", "Tours"}
FAMILIES = {
    # family: (units per day, yearly amplitude, peak day of year)
    "grocery": (420.0, 0.08, 355),
    "garden": (90.0, 0.55, 140),
    "heating": (60.0, 0.70, 15),
}
HOLIDAYS = {(1, 1), (5, 1), (5, 8), (7, 14), (8, 15), (11, 1), (11, 11), (12, 25)}

end = pd.Timestamp(os.environ.get("END_DATE", "2025-12-31"))
history_days = int(os.environ.get("DAYS", "730"))
horizon = int(os.environ.get("HORIZON", "28"))
days = history_days + horizon
rng = np.random.default_rng(int(os.environ.get("SEED", "11")))
dates = pd.date_range(end=end + pd.Timedelta(days=horizon), periods=days, freq="D")
future = dates > end
day_of_year = dates.day_of_year.to_numpy()
weekday = dates.day_of_week.to_numpy()
holiday = np.array([(d.month, d.day) in HOLIDAYS for d in dates])
weekly = np.array([0.85, 0.9, 0.95, 1.0, 1.15, 1.45, 0.6])[weekday]
trend = 1 + 0.06 * np.arange(days) / 365

rows = []
for index, (store, size) in enumerate(STORES.items()):
    # Promotions: one week in five, a different week for each store.
    promo = (((np.arange(days) // 7) + index) % 5 == 0) & (weekday < 6)
    for family, (units, amplitude, peak) in FAMILIES.items():
        yearly = 1 + amplitude * np.cos(2 * np.pi * (day_of_year - peak) / 365.25)
        mean = units * size * trend * weekly * yearly * np.where(promo, 1.25, 1.0)
        sold = rng.poisson(np.clip(mean, 0, None)).astype(float)
        closed = holiday | ((weekday == 6) & (store in CLOSED_ON_SUNDAY))
        sold[closed] = 0
        units_column = pd.array(sold.astype("int64"), dtype="Int64")
        units_column[future] = pd.NA  # not sold yet: to forecast
        rows.append(
            pd.DataFrame(
                {
                    "store": store,
                    "family": family,
                    "day": dates.date,
                    "units": units_column,
                    "promotion": promo,
                    "closed": closed,
                }
            )
        )
history = pd.concat(rows, ignore_index=True)

table = pa.Table.from_pandas(history, preserve_index=False).cast(
    pa.schema(
        [
            ("store", pa.string()),
            ("family", pa.string()),
            ("day", pa.date32()),
            ("units", pa.int64()),
            ("promotion", pa.bool_()),
            ("closed", pa.bool_()),
        ]
    )
)
graal.write_table(table, "forecast_sales_history", mode="overwrite")

summary = {
    "rows": len(history),
    "series": history.groupby(["store", "family"]).ngroups,
    "history_from": str(dates[0].date()),
    "history_to": str(end.date()),
    "days_to_forecast": horizon,
    "units_sold": int(history["units"].sum()),
}
print(
    f"forecast_sales_history: {summary['rows']} rows, {summary['series']} series, "
    f"sales until {summary['history_to']}, {horizon} days to forecast"
)
print("GRAAL_RUN_RESULTS " + json.dumps(summary))
