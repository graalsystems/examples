# spring-batch-example

A Spring Batch job (`sales-report`, class `graal.systems.examples.batch.SalesReportApplication`) on
sales embedded in the jar ([`sales.csv`](src/main/resources/sales.csv)). It has two steps:

| Step | What it does |
|---|---|
| `load` | Reads the CSV in chunks of 5, rejects invalid rows (no price, quantity not positive), computes each line's amount and inserts it into a `sale` table. |
| `report` | Prints the revenue per store and per day, and fails the job when more rows were rejected than `sales.max-rejected`. |

The job repository (Spring Batch's `BATCH_*` tables) and the `sale` table live in an in-memory H2
database for the duration of the run: nothing is written to disk, and no external database is needed.

- **Job type:** `spring-batch` (specialised type: available only if your installation configured the
  `jvm` runtime image, Java 21)
- **Entry point:** the jar (`main_library`). It is a Spring Boot executable jar, so `main_class_name`
  is Spring Boot's launcher, `org.springframework.boot.loader.launch.JarLauncher`, which starts the
  application's own main class. Leaving `main_class_name` empty has the same effect (`java -jar`).
- **Configuration:** `properties` become `-D` system properties, which Spring Boot reads like any
  other property: `sales.max-rejected` here, or any `spring.*` setting. The job's `parameters` are
  passed to the application; `key=value` arguments become Spring Batch job parameters.
- **Toolchain:** Java 21, Maven 3.9. The `jvm` image carries no library: Spring Boot 4.1,
  Spring Batch and H2 are packaged in the jar by `spring-boot-maven-plugin`.

## Build

```sh
mvn -q package -DskipTests
# -> target/spring-batch-example-1.0.0-SNAPSHOT.jar
```

## Deploy

With [`deploy.sh`](../deploy.sh), from the repository root: it builds the jar, uploads it, fills in
`job.json`, creates the job and, with `--run`, runs it and streams its logs.

```sh
./deploy.sh spring-batch-example --project "$PROJECT_ID" --run
```

Or by hand:

```sh
JAR_KEY=$(graalctl libraries upload target/spring-batch-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The run's log shows the two rejected rows, the report and the job's status:

```text
Rejected (quantity -3): SaleLine[date=2026-03-03, store=Nantes, product=coffee, quantity=-3, unitPrice=4.50]
Rejected (no price): SaleLine[date=2026-03-03, store=Paris, product=juice, quantity=11, unitPrice=]
Read 21 rows, loaded 19, rejected 2
Revenue per store:
  Paris      8 lines   238 units     691.40 EUR
  Lyon       6 lines    66 units     225.10 EUR
  Nantes     5 lines    69 units     157.40 EUR
Revenue per day:
  2026-03-02     373.00 EUR
  2026-03-03     297.50 EUR
  2026-03-04     403.40 EUR
Total revenue: 1073.90 EUR
... Job: [SimpleJob: [name=sales-report]] completed with the following parameters: [{}] and the following status: [COMPLETED] in 91ms
Exit with status 0
```

The process exits with the job's outcome: set `"sales.max-rejected": "1"` in `properties` and the
`report` step fails, the job ends `FAILED`, the process exits with a non-zero status and the run fails.

If the API answers `501 feature_disabled`, the `spring-batch` type is not enabled on your
installation. The options a specialised type accepts depend on the runtime image configured there:
check the installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
