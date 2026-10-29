# pyspark-examples

A PySpark application split into an entry script (`spark_submit.py`) and a package (`tools/`)
shipped as a zip. It builds a DataFrame in memory and aggregates it; optionally it also reads a
table of the platform catalog with `spark.table("db.table")` (Iceberg tables served through
Apache Polaris).

- **Job type:** `spark` (core type, `spark-py` image, Apache Spark 3.5)
- **Entry point:** `spark_submit.py`, which registers `tools.zip` with `addPyFile`
- **Optional:** set `SOURCE_TABLE` (for example `sales.orders`) in `options.env` to read a catalog table

## Build

```sh
make package          # creates tools.zip
```

To try it locally: `pip install -r requirements.txt && python spark_submit.py`
(the job itself uses the Spark provided by the `spark-py` image).

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload spark_submit.py)
ZIP_KEY=$(graalctl libraries upload tools.zip)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<TOOLS_ZIP_KEY>/$ZIP_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

See [`job.json`](job.json). The instance type sizes the Spark run; list the available ones with
`graalctl instance-types list`.
