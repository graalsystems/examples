# hive_example

A HiveQL script run by graal: it creates a sales table, loads eight rows, and prints the revenue per
region and the quantity per product and month.

- **Job type:** `hive` (specialised type: available only if your installation configured the `spark`
  runtime image, which runs it)
- **Entry point:** the script itself, one statement or line per entry of `options.lines`; `conf` holds
  Spark SQL settings (`spark.master` and the other deployment keys are ignored: the run is one pod)
- **Engine:** `spark-sql` in local mode, on an **Iceberg** catalogue named `graal`. When the job's
  identity may read the tenant's catalogue, the tables live there and outlive the run, in the
  identity's default namespace. Otherwise the run uses a throw-away catalogue under `/tmp`, and
  nothing survives it — the job still runs, which makes this example safe to try anywhere.
- **No credentials** in the job: no storage key is ever given to the pod (the catalogue signs storage
  requests on its behalf).

## Deploy

```sh
./deploy.sh hive_example --project "$PROJECT_ID" --identity "$IDENTITY_ID" --run
```

`--identity` is optional here: without it, the run uses the throw-away catalogue.

Or by hand:

```sh
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" job.json > job.local.json
JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log ends with the two result sets, for example:

```
Ile-de-France	2	510	981.0
Occitanie	3	280	488.5
Bretagne	3	295	441.5
```

then `Exit with status 0`. `INSERT OVERWRITE` makes the job safe to run again.

If the API answers `501 feature_disabled`, the `hive` type is not enabled on your installation. The
options a specialised type accepts depend on the runtime image configured there: check the
installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
