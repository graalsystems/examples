"""Aggregate: from `energy_hourly`, one row per region and day in `energy_daily` — total and peak
consumption, mean, minimum and maximum temperature, and the heating degree-days (base 15 °C) that
explain most of the winter consumption.
"""

import json

import pyarrow as pa

import graal

hourly = graal.read_table("energy_hourly")
hourly["day"] = hourly["measured_at"].dt.date

daily = (
    hourly.groupby(["region", "day"])
    .agg(
        consumption_mwh=("consumption_mwh", "sum"),
        peak_mwh=("consumption_mwh", "max"),
        temperature_mean_c=("temperature_c", "mean"),
        temperature_min_c=("temperature_c", "min"),
        temperature_max_c=("temperature_c", "max"),
        hours=("consumption_mwh", "size"),
    )
    .reset_index()
)
daily["heating_degree_days"] = (15 - daily["temperature_mean_c"]).clip(lower=0)
daily = daily.round(
    {
        "consumption_mwh": 1,
        "temperature_mean_c": 2,
        "heating_degree_days": 2,
    }
)

table = pa.Table.from_pandas(daily, preserve_index=False).cast(
    pa.schema(
        [
            ("region", pa.string()),
            ("day", pa.date32()),
            ("consumption_mwh", pa.float64()),
            ("peak_mwh", pa.float64()),
            ("temperature_mean_c", pa.float64()),
            ("temperature_min_c", pa.float64()),
            ("temperature_max_c", pa.float64()),
            ("hours", pa.int64()),
            ("heating_degree_days", pa.float64()),
        ]
    )
)
graal.write_table(table, "energy_daily", mode="overwrite")

# Correlation between cold and consumption, per region: what the next job checks.
correlation = {
    region: round(float(group["consumption_mwh"].corr(group["heating_degree_days"])), 3)
    for region, group in daily.groupby("region")
}
summary = {"rows": len(daily), "heating_correlation": correlation}
print(daily.groupby("region")["consumption_mwh"].sum().round(0).to_string())
print("GRAAL_RUN_RESULTS " + json.dumps(summary))
