import io
import os
from datetime import datetime
import boto3
import pandas as pd

AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

input = "data.csv"
output = "results.csv"

# Read
s3 = boto3.client('s3',
                    endpoint_url=AWS_ENDPOINT,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    verify=False
                  )

books_df = pd.read_csv(
    s3.get_object(Bucket=AWS_BUCKET, Key=input).get("Body"),
    index_col=['date'],
    parse_dates=True,
    date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d')
)

# Compute
table = books_df.groupby(pd.Grouper(freq='M')).sum()

# Write
with io.StringIO() as csv_buffer:
    books_df.to_csv(csv_buffer)
    response = s3.put_object(
        Bucket=AWS_BUCKET, Key=output, Body=csv_buffer.getvalue()
    )