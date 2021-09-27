import pandas as pd
from datetime import datetime
from io import StringIO

data = r"""
date,col_1
2018-02-26,1
2018-03-05,1
2018-03-12,1
2018-03-19,1
2018-03-26,1
2018-04-01,1
2018-04-02,1
"""

df = pd.read_csv(StringIO(data),
                 index_col=['date'],
                 parse_dates=True,
                 date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d')
                 )

table = df.groupby(pd.Grouper(freq='M')).sum()

print(table)