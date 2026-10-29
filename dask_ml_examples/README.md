# dask_ml_examples

Machine learning with Dask, on data generated in code:

1. a Dask dataframe aggregation (`dask.datasets.timeseries`),
2. a scikit-learn hyper-parameter search run on the Dask joblib backend,
3. distributed XGBoost training (`xgboost.dask`),
4. distributed LightGBM training (`DaskLGBMClassifier`).

- **Job type:** `dask` (specialised type: available only if your installation configured a Dask runtime image)
- **Entry point:** `dask_ml_example.py`; it connects to the scheduler given by
  `DASK_SCHEDULER_ADDRESS` (or `--scheduler`), or starts a local cluster
- **Dependencies:** `requirements.txt`

## Build

Nothing to build. To try it locally:
`pip install -r requirements.txt && python dask_ml_example.py` (starts a local Dask cluster).

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload dask_ml_example.py)
REQ_KEY=$(graalctl libraries upload requirements.txt)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<REQUIREMENTS_KEY>/$REQ_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

If the API answers `501 feature_disabled`, the `dask` type is not enabled on your installation.
The options a specialised type accepts, and whether its image installs `requirements.txt` at
start-up, depend on the runtime image configured there: check the installation's API reference
(`/api/v1/docs`) and adjust [`job.json`](job.json). Without a Dask runtime, the same script also
runs as a `python` job on a local cluster sized by the instance type.
