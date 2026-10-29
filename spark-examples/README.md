# spark-examples

Spark applications written in Java, packaged as one jar. Each class is a separate example:

| Main class | What it shows |
|---|---|
| `graal.systems.examples.WithLocalData` | Builds a dataset from JavaBeans in memory and aggregates it by category. No external data needed. |
| `graal.systems.examples.WithS3` | Reads `data-spark-java.csv` (option `--path`) from the bucket in `GRAAL_BUCKET` (else `AWS_BUCKET`) through S3A. Credentials and endpoint come from the runtime's Spark/Hadoop S3A configuration. |
| `graal.systems.examples.WithCatalog` | Reads a table of the platform catalog with `spark.table("db.table")` (Iceberg tables served through Apache Polaris). Set the table with `SOURCE_TABLE` in `options.env`. |

- **Job type:** `spark` (core type, `spark-py` image, Apache Spark 3.5)
- **Entry point:** the jar (`file`) and the class to run (`main_class_name`)
- **Toolchain:** Java 17+, Maven 3.9. Spark (3.5.x, Scala 2.12) is a `provided` dependency:
  the runtime image supplies it, only picocli is bundled in the jar.

## Build

```sh
mvn -q package -DskipTests
# -> target/spark-examples-1.0.0-SNAPSHOT.jar
```

For `WithS3`, upload the sample data to the project's bucket first, for example
`aws s3 cp data-spark-java.csv s3://$BUCKET/data-spark-java.csv`.

## Run with graalctl

```sh
JAR_KEY=$(graalctl libraries upload target/spark-examples-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

To run another example, change `options.main_class_name` in [`job.json`](job.json).
