# database_migration_example

A [Liquibase](https://www.liquibase.com/) changelog (`changelog.yaml`) applied to a database by a
graal job: the first change set creates a `person` table, the second adds a column to it. Each run
applies only the change sets the database has not seen yet, then reports the database's change
history to graal, where it is listed with the data warehouse.

- **Job type:** `database-migration` (specialised type: available only if your installation
  configured the `liquibase` runtime image, Liquibase 4.33 with the PostgreSQL and MariaDB JDBC
  drivers)
- **Target:** a data warehouse registered in graal (`datawarehouse_id`): the run connects with its
  URL and user, and receives its password from the warehouse's secret. Nothing about the database
  goes in the job or in the changelog.
- **Changelog:** read from a git repository (`repository`): `url` (https), `revision` (a branch or
  tag) and `path`, the folder that holds `changelog.yaml`. The job above points to this folder of
  the public examples repository; point it to your own repository for real migrations.
- **Network:** the run clones the repository and connects to the database: both must be reachable
  from the run under your installation's egress rules.

## Build

Nothing to build. Commit your changes to `changelog.yaml` in a new change set (never edit a change
set that has already run: Liquibase checks their checksums).

## Run with graalctl

Register the database as a data warehouse first (`POST /api/v1/datawarehouses`, or the console); the
password you give is stored in a secret, never in the job. Note its id as `DATAWAREHOUSE_ID`.

```sh
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<DATAWAREHOUSE_ID>/$DATAWAREHOUSE_ID/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The log shows `liquibase status` (the pending change sets), then `liquibase update`. A second run
applies nothing.

For a private repository, add `"username"` and `"password"` to `repository`. The password reaches
the run through the run's own secret, never through its script, but it is stored with the job
definition and returned when the job is read: anyone who can read the job can see it. Use a
read-only access token limited to that repository, fill it in `job.local.json` only (from an
environment variable), and never commit it.

To run the migration from a CI pipeline, create the job once and run it with `graalctl`, as in
[`ci-github-actions`](../ci-github-actions).

If the API answers `501 feature_disabled`, the `database-migration` type is not enabled on your
installation. The options a specialised type accepts depend on the runtime image configured there:
check the installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
