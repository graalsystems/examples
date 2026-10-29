# flink-example

Apache Flink jobs written in Java, packaged as one jar:

| Main class | What it shows |
|---|---|
| `graal.systems.examples.FlinkLocalData` | DataStream API in batch mode: counts people by gender and by country from in-memory data. |
| `graal.systems.examples.FlinkSql` | Table API / Flink SQL. Without a table, aggregates an in-memory table. With `SOURCE_TABLE` set (or a table name as argument), loads the platform catalog from the SQL file named by `GRAAL_FLINK_CATALOG_SQL`, switches to the `graal` catalog and counts the rows of that table. |

- **Job type:** `flink` (specialised type: available only if your installation configured a Flink runtime image)
- **Entry point:** the jar (`file`) and the class to run (`main_class_name`)
- **Toolchain:** Java 17+, Maven 3.9. Flink 1.20.x (LTS) is a `provided` dependency; set
  `-Dflink.version=...` to match the Flink of your runtime image (the code also compiles with Flink 2.x).

## Build

```sh
mvn -q package -DskipTests
# -> target/flink-example-1.0.0-SNAPSHOT.jar
```

## Run with graalctl

```sh
JAR_KEY=$(graalctl libraries upload target/flink-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

If the API answers `501 feature_disabled`, the `flink` type is not enabled on your installation.
The options a specialised type accepts depend on the runtime image configured there: check the
installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
