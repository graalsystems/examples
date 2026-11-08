# Energy consumption: a daily data pipeline

Hourly electricity consumption and temperature for four regions go through three jobs chained by
the workflow `energy-daily-pipeline`:

```text
ingest ──▶ aggregate ──▶ quality
energy_hourly   energy_daily   (fails the workflow on a violation)
```

| Job | Type | Reads | Writes | What it does |
|---|---|---|---|---|
| `energy-ingest` | `python` | — | `energy_hourly` | One year of hourly readings for Bretagne, Normandie, Pays de la Loire and Centre-Val de Loire (35 040 rows) |
| `energy-aggregate` | `python` | `energy_hourly` | `energy_daily` | One row per region and day: total and peak consumption, temperatures, heating degree-days (base 15 °C) |
| `energy-quality` | `python` | `energy_daily` | — | No missing day or value, 24 readings per day, consumption within bounds and rising with the cold; exit code 1 on a failed check |

The readings are **synthetic** (`ingest.py`, seeded): consumption follows the temperature, the hour
and the day of the week, so that the aggregation and the checks have a realistic shape to work on.
Change the period with the job's `env` (`START_DATE`, `DAYS`).

## Deploy and run

```sh
projects/deploy-project.sh projects/energy-consumption --run
```

The three runs take about a minute each on `Standard_Development_D1_v1`. Each run's **results** (the
`GRAAL_RUN_RESULTS` line of its log) are shown in the console: row counts, the correlation between
cold and consumption per region, the failed checks.

## What to try next

- **See the workflow stop**: in `quality.py`, raise the required correlation from `0.5` to `0.95`
  and redeploy. The run of `energy-quality` fails on the regions below it, and the workflow run is
  marked failed.
- **Schedule it**: `graalctl jobs schedule` works per job; for the whole pipeline, set the
  workflow's schedule in the console.
- **Query the result in SQL** from the console's SQL editor, for example the ten coldest days:
  `SELECT region, day, consumption_mwh, temperature_mean_c FROM energy_daily ORDER BY temperature_mean_c LIMIT 10`.
