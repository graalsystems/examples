"""A batch job packaged as an image: summarise sales, write a CSV, print the result.

Arguments (the job's `parameters`): an optional region to keep, `all` by default.
"""
import os
import sys

import pandas as pd

SALES = [
    ("Bretagne", "Rennes", "2026-01", "apple", 120, 1.25),
    ("Bretagne", "Brest", "2026-01", "pear", 80, 2.10),
    ("Bretagne", "Rennes", "2026-02", "apple", 95, 1.30),
    ("Occitanie", "Toulouse", "2026-01", "apple", 150, 1.20),
    ("Occitanie", "Montpellier", "2026-02", "pear", 60, 2.40),
    ("Occitanie", "Toulouse", "2026-02", "pear", 70, 2.35),
    ("Ile-de-France", "Paris", "2026-01", "apple", 300, 1.45),
    ("Ile-de-France", "Paris", "2026-02", "pear", 210, 2.60),
]


def main(argv: list[str]) -> int:
    region = argv[1] if len(argv) > 1 else "all"
    sales = pd.DataFrame(SALES, columns=["region", "city", "month", "product", "quantity", "unit_price"])
    if region != "all":
        sales = sales[sales["region"] == region]
    if sales.empty:
        print(f"no sale for region {region!r}", file=sys.stderr)
        return 1
    sales["revenue"] = sales["quantity"] * sales["unit_price"]
    report = sales.groupby("region", as_index=False)[["quantity", "revenue"]].sum().round(2)
    # /workspace is the run's writable working directory (/tmp outside graal).
    out_dir = "/workspace" if os.access("/workspace", os.W_OK) else "/tmp"
    out = os.path.join(out_dir, "report.csv")
    report.to_csv(out, index=False)
    print(report.to_string(index=False))
    print(f"written {out} ({len(report)} regions), uid={os.getuid()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
