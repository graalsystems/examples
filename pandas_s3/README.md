# pandas_s3

Reads `data-pandas-s3.csv` from object storage, groups the rows by month with pandas and writes
`results.csv` next to it. Shows how a job finds its bucket and its credentials.

- **Job type:** `python` (core type, `python` image, Python 3.11)
- **Entry point:** `pandas_s3.py` (options: `--input KEY`, `--output KEY`)
- **Dependencies:** `requirements.txt`, installed by the `python` image at start-up

## Configuration

| What | Where the script looks |
|---|---|
| Bucket | `GRAAL_BUCKET` (the project's bucket, set by graal in every run), else `AWS_BUCKET`. `name`, `name/prefix` and `s3://name/prefix` are accepted. |
| Credentials | The standard boto3 chain: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_SHARED_CREDENTIALS_FILE`, ... If the key pair is not in the environment, the script reads the files `/workspace/secrets/AWS_ACCESS_KEY_ID` and `/workspace/secrets/AWS_SECRET_ACCESS_KEY`. |
| Endpoint | `AWS_ENDPOINT_URL` for S3-compatible storage other than AWS (`AWS_ENDPOINT` is accepted too), `AWS_DEFAULT_REGION` if needed. |
| TLS | Certificates are always verified. Set `AWS_CA_BUNDLE` to a PEM file to trust an internal certificate authority. |

Keep credentials out of `job.json`: store them as project secrets that the job's identity is
entitled to. graal writes them to `/workspace/secrets/` when the run starts; they never appear in
the job definition or the process environment. If your secrets have other names, adapt
`read_secret()` or set `SECRETS_DIR`.

Upload the sample input first, for example with the AWS CLI:
`aws s3 cp data-pandas-s3.csv s3://$BUCKET/data-pandas-s3.csv`.

## Build

Nothing to build: the script and `requirements.txt` are uploaded as they are.

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload pandas_s3.py)
REQ_KEY=$(graalctl libraries upload requirements.txt)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<REQUIREMENTS_KEY>/$REQ_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

Non-secret settings such as `AWS_ENDPOINT_URL` go in `options.env` of [`job.json`](job.json).
