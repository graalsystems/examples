"""Ingest: hourly electricity consumption and temperature for four regions, into the table
`energy_hourly` of the tenant's Iceberg catalog.

The readings are SYNTHETIC (seeded, reproducible): the example needs no network access and no
dataset to download. Consumption follows the temperature (heating below 15 °C), the hour of the day
and the day of the week, plus noise, so that the next jobs have a realistic shape to work on.

Parameters (environment): START_DATE (default 2025-01-01), DAYS (default 365), SEED (default 7).
"""

import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa

import graal  # the tenant's catalog, provided to every python run

REGIONS = {
    # region: (mean consumption in MWh per hour, winter mean temperature, summer mean temperature)
    "Bretagne": (2600.0, 7.5, 18.0),
    "Normandie": (2900.0, 6.0, 18.5),
    "Pays de la Loire": (3100.0, 7.0, 20.0),
    "Centre-Val de Loire": (2200.0, 4.5, 21.0),
}

start = pd.Timestamp(os.environ.get("START_DATE", "2025-01-01"))
days = int(os.environ.get("DAYS", "365"))
rng = np.random.default_rng(int(os.environ.get("SEED", "7")))

hours = pd.date_range(start, periods=days * 24, freq="h")
day_of_year = hours.day_of_year.to_numpy()
hour = hours.hour.to_numpy()
weekend = hours.day_of_week.to_numpy() >= 5
season = np.cos(2 * np.pi * (day_of_year - 15) / 365.25)  # 1 in mid-January, -1 in mid-July
daily_profile = 1 + 0.18 * np.sin(np.pi * (hour - 6) / 12) * (hour >= 6) * (hour <= 22)

frames = []
for region, (base, winter, summer) in REGIONS.items():
    mean_temp = (winter + summer) / 2 + (winter - summer) / 2 * season
    temperature = (
        mean_temp - 3 * np.cos(2 * np.pi * (hour - 4) / 24) + rng.normal(0, 1.8, len(hours))
    )
    heating = np.clip(15 - temperature, 0, None) * 0.035
    consumption = base * daily_profile * (1 + heating) * np.where(weekend, 0.9, 1.0)
    consumption *= 1 + rng.normal(0, 0.03, len(hours))
    frames.append(
        pd.DataFrame(
            {
                "region": region,
                "measured_at": hours,
                "consumption_mwh": consumption.round(1),
                "temperature_c": temperature.round(1),
            }
        )
    )
readings = pd.concat(frames, ignore_index=True)

# Iceberg stores timestamps in microseconds: say so explicitly rather than rely on a downcast.
table = pa.Table.from_pandas(readings, preserve_index=False).cast(
    pa.schema(
        [
            ("region", pa.string()),
            ("measured_at", pa.timestamp("us")),
            ("consumption_mwh", pa.float64()),
            ("temperature_c", pa.float64()),
        ]
    )
)
written = graal.write_table(table, "energy_hourly", mode="overwrite")

summary = {
    "rows": len(readings),
    "regions": len(REGIONS),
    "from": str(hours[0]),
    "to": str(hours[-1]),
    "snapshot": written.current_snapshot().snapshot_id,
}
print(f"energy_hourly: {summary['rows']} rows, {summary['from']} -> {summary['to']}")
# A line starting with GRAAL_RUN_RESULTS becomes the results of the run, shown in the console.
print("GRAAL_RUN_RESULTS " + json.dumps(summary))
