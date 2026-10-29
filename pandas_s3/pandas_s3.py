"""Read a CSV from S3-compatible object storage, aggregate it with pandas and write the result back.

Bucket:       GRAAL_BUCKET (set by graal in every run), else AWS_BUCKET.
              Accepts "name", "name/prefix" or "s3://name/prefix".
Credentials:  the standard boto3 chain (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
              AWS_SESSION_TOKEN, AWS_SHARED_CREDENTIALS_FILE, ...). When the key pair is not
              in the environment, it is read from files of the same name in /workspace/secrets/.
Endpoint:     AWS_ENDPOINT_URL for non-AWS storage (AWS_ENDPOINT is accepted too).
TLS:          always verified; set AWS_CA_BUNDLE to trust an internal certificate authority.
"""

import argparse
import io
import os
import sys
from pathlib import Path

import boto3
import pandas as pd

SECRETS_DIR = Path(os.getenv("SECRETS_DIR", "/workspace/secrets"))


def read_secret(name: str) -> str | None:
    """Return the environment variable `name`, else the content of SECRETS_DIR/name."""
    value = os.getenv(name)
    if value:
        return value
    path = SECRETS_DIR / name
    if path.is_file():
        return path.read_text().strip()
    return None


def resolve_bucket() -> tuple[str, str]:
    value = os.getenv("GRAAL_BUCKET") or os.getenv("AWS_BUCKET")
    if not value:
        sys.exit("No bucket: set GRAAL_BUCKET or AWS_BUCKET.")
    value = value.removeprefix("s3://").removeprefix("s3a://").strip("/")
    bucket, _, prefix = value.partition("/")
    return bucket, f"{prefix}/" if prefix else ""


def s3_client():
    kwargs = {}
    endpoint = os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    access_key = read_secret("AWS_ACCESS_KEY_ID")
    secret_key = read_secret("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    # TLS verification stays on; botocore honours AWS_CA_BUNDLE.
    return boto3.client("s3", **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default="data-pandas-s3.csv", help="object key to read")
    parser.add_argument("--output", default="results.csv", help="object key to write")
    args = parser.parse_args()

    bucket, prefix = resolve_bucket()
    s3 = s3_client()

    body = s3.get_object(Bucket=bucket, Key=prefix + args.input)["Body"]
    df = pd.read_csv(body, index_col="date", parse_dates=["date"], date_format="%Y-%m-%d")

    table = df.groupby(pd.Grouper(freq="ME")).sum()
    print(table)

    with io.StringIO() as buffer:
        table.to_csv(buffer)
        s3.put_object(Bucket=bucket, Key=prefix + args.output, Body=buffer.getvalue().encode())
    print(f"Wrote s3://{bucket}/{prefix}{args.output}")


if __name__ == "__main__":
    main()
