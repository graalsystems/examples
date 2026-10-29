# dbt_examples

A minimal dbt project (`jaffle_shop/`): two models, the second built on the first with `ref()`,
and `unique` / `not_null` data tests. The profile uses DuckDB, so the project runs without a
warehouse or credentials.

- **Job type:** `dbt` (specialised type: available only if your installation configured a dbt runtime image)
- **Project:** `jaffle_shop/` (`dbt_project.yml`, `profiles.yml`, `models/`)
- **Dependencies:** `requirements.txt` (`dbt-core`, `dbt-duckdb`), for local runs
- **Settings:** `DBT_DUCKDB_PATH` sets the DuckDB file (default `/tmp/jaffle_shop.duckdb`)

To target another warehouse, install its dbt adapter and replace the output in
`jaffle_shop/profiles.yml`. Read credentials with `env_var()` or from the files graal writes to
`/workspace/secrets/`; never put them in the profile.

## Build

Try it locally:

```sh
pip install -r requirements.txt
cd jaffle_shop && dbt build --profiles-dir .
```

Package the project as an archive:

```sh
tar -czf jaffle_shop.tar.gz -C jaffle_shop --exclude target --exclude logs .
```

## Run with graalctl

```sh
ARCHIVE_KEY=$(graalctl libraries upload jaffle_shop.tar.gz)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$ARCHIVE_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

If the API answers `501 feature_disabled`, the `dbt` type is not enabled on your installation.
How a dbt job receives its project (archive or files) and which options it accepts depend on the
runtime image configured there: check the installation's API reference (`/api/v1/docs`) and
adjust [`job.json`](job.json).
