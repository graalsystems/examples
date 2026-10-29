"""Aggregate a small in-memory time series by month with pandas."""

from io import StringIO

import pandas as pd

DATA = """\
date,col_1
2018-02-26,1
2018-03-05,1
2018-03-12,1
2018-03-19,1
2018-03-26,1
2018-04-01,1
2018-04-02,1
"""


def main() -> None:
    df = pd.read_csv(StringIO(DATA), index_col="date", parse_dates=["date"], date_format="%Y-%m-%d")
    # "ME" = month end (the "M" alias is deprecated since pandas 2.2).
    table = df.groupby(pd.Grouper(freq="ME")).sum()
    print(table)


if __name__ == "__main__":
    main()
