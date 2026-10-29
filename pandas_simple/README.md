# pandas_simple

Reads a small CSV held in memory, groups the rows by month with pandas and prints the result.
No external data or credentials are needed, which makes it a good first job.

- **Job type:** `python` (core type, `python` image, Python 3.11)
- **Entry point:** `pandas_simple.py`
- **Dependencies:** `requirements.txt`, installed by the `python` image at start-up

## Build

Nothing to build: the script and `requirements.txt` are uploaded as they are.
To try it locally: `pip install -r requirements.txt && python pandas_simple.py`.

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload pandas_simple.py)
REQ_KEY=$(graalctl libraries upload requirements.txt)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<REQUIREMENTS_KEY>/$REQ_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

See [`job.json`](job.json) for the job definition. Pick an instance type available on your
installation with `graalctl instance-types list`.
