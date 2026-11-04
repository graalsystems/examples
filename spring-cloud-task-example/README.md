# spring-cloud-task-example

A [Spring Cloud Task](https://spring.io/projects/spring-cloud-task)
(`graal.systems.examples.task.ReconcileTaskApplication`) that reconciles the payments received
against the invoices issued, both embedded in the jar
([`invoices.csv`](src/main/resources/invoices.csv), [`payments.csv`](src/main/resources/payments.csv)).
Each invoice ends up paid, paid with a difference larger than the tolerance, or unpaid; a payment
that matches no invoice is reported as unmatched.

graal runs the task itself: there is no Spring Cloud Data Flow server. Spring Cloud Task records the
execution (name, arguments, start, end, exit code) in its task repository, here an in-memory H2
database that lives for the duration of the run.

- **Job type:** `spring-cloud-data-flow` (specialised type: available only if your installation
  configured the `jvm` runtime image, Java 21)
- **Entry point:** the jar (`main_library`). It is a Spring Boot executable jar, so `main_class_name`
  is Spring Boot's launcher, `org.springframework.boot.loader.launch.JarLauncher`; leaving it empty
  has the same effect (`java -jar`).
- **Task name:** `task_name` becomes `spring.cloud.task.name` (empty: the job's name).
- **Arguments:** the job's `parameters` are passed to the task: `--tolerance-cents=50` accepts a
  difference of up to 0.50 EUR between an invoice and its payment. `properties` become `-D` system
  properties.
- **Toolchain:** Java 21, Maven 3.9. The `jvm` image carries no library: Spring Boot 4.1,
  Spring Cloud Task 5.0 and H2 are packaged in the jar by `spring-boot-maven-plugin`.

## Build

```sh
mvn -q package -DskipTests
# -> target/spring-cloud-task-example-1.0.0-SNAPSHOT.jar
```

## Deploy

With [`deploy.sh`](../deploy.sh), from the repository root: it builds the jar, uploads it, fills in
`job.json`, creates the job and, with `--run`, runs it and streams its logs.

```sh
./deploy.sh spring-cloud-task-example --project "$PROJECT_ID" --run
```

Or by hand:

```sh
JAR_KEY=$(graalctl libraries upload target/spring-cloud-task-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The run's log shows the task execution opened and closed by Spring Cloud Task around the result:

```text
Task 'reconcile-payments' execution 1 started, arguments [--tolerance-cents=50]
Reconciling 10 invoices and 9 payments (tolerance 0.50 EUR)
  UNPAID     INV-1003 Initech        99.90 EUR
  DIFFERENCE INV-1005 Acme          410.00 EUR invoiced,     400.00 EUR paid
  UNPAID     INV-1007 Globex         60.00 EUR
  UNMATCHED  PAY-509 for unknown invoice INV-1099, 75.00 EUR
Reconciled: 7 paid, 1 with a difference, 2 unpaid, 1 unmatched payment(s); outstanding 169.90 EUR
Task 'reconcile-payments' execution 1 ended with exit code 0
Exit with status 0
```

When the task throws, Spring Cloud Task records the failure, the process exits with a non-zero
status and the run fails.

If the API answers `501 feature_disabled`, the `spring-cloud-data-flow` type is not enabled on your
installation. The options a specialised type accepts depend on the runtime image configured there:
check the installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
