# ray_examples

A small, self-contained Ray program:

1. parallel **tasks** that estimate pi by Monte Carlo sampling,
2. an **actor** that keeps state between calls,
3. a **Ray Data** pipeline (`ray.data.range`, two vectorised `map_batches` steps, then aggregations).

- **Job type:** `ray` (specialised type: available only if your installation configured a Ray runtime image)
- **Entry point:** `ray_example.py`; `ray.init()` connects to the cluster given by `RAY_ADDRESS`,
  or starts a local Ray instance
- **Dependencies:** `requirements.txt` (`ray[data]`)

## Build

Nothing to build. To try it locally:
`pip install -r requirements.txt && python ray_example.py`.

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload ray_example.py)
REQ_KEY=$(graalctl libraries upload requirements.txt)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<REQUIREMENTS_KEY>/$REQ_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

If the API answers `501 feature_disabled`, the `ray` type is not enabled on your installation.
The options a specialised type accepts, and whether its image installs `requirements.txt` at
start-up, depend on the runtime image configured there: check the installation's API reference
(`/api/v1/docs`) and adjust [`job.json`](job.json). Without a Ray runtime, the same script also
runs as a `python` job on a local Ray instance.
