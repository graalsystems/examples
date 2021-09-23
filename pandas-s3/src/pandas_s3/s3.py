import os

import pandas as pd

AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

input = "data.csv"
output = "results.csv"

books_df = pd.read_csv(
    f"s3://{AWS_BUCKET}/{input}",
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "client_kwargs": dict(endpoint_url = AWS_ENDPOINT, verify = "false")
    },
)

table = books_df.groupby(pd.Grouper(freq='M')).sum()

table.to_csv(
    f"s3://{AWS_BUCKET}/{output}",
    index=False,
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "client_kwargs": dict(endpoint_url = AWS_ENDPOINT, verify = "false")
    },
)