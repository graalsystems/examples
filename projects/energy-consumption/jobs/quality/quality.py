"""Quality: checks `energy_daily` before anyone uses it. Every failed check is printed, and the run
FAILS (exit code 1) if one of them fails — which stops the workflow and marks it failed.

Checks: one row per region and day with no gap, 24 hourly readings per day, no missing value,
consumption within physical bounds, and consumption that rises with the cold in every region.
"""

import json
import sys

import pandas as pd

import graal

daily = graal.read_table("energy_daily")
failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    if not ok:
        failures.append(name)


check("table is not empty", len(daily) > 0, f"{len(daily)} rows")
check("no missing value", not daily.isna().any().any())
check("one row per region and day", not daily.duplicated(["region", "day"]).any())

days = pd.to_datetime(daily["day"])
for region, group in daily.assign(day=days).groupby("region"):
    expected = pd.date_range(group["day"].min(), group["day"].max(), freq="D")
    missing = len(expected) - group["day"].nunique()
    check(f"{region}: no missing day", missing == 0, f"{missing} missing")
    correlation = group["consumption_mwh"].corr(group["heating_degree_days"])
    check(f"{region}: consumption rises with the cold", correlation > 0.5, f"r = {correlation:.2f}")

check("24 readings per day", (daily["hours"] == 24).all())
check(
    "consumption within bounds",
    daily["consumption_mwh"].between(10_000, 200_000).all(),
    f"{daily['consumption_mwh'].min():.0f} .. {daily['consumption_mwh'].max():.0f} MWh",
)

print("GRAAL_RUN_RESULTS " + json.dumps({"rows": len(daily), "failed_checks": failures}))
if failures:
    print(f"{len(failures)} check(s) failed", file=sys.stderr)
    sys.exit(1)
print("all checks passed")
