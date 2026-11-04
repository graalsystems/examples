# pulsar-example

An [Apache Pulsar](https://pulsar.apache.org) client program (`graal.systems.examples.pulsar.OrderTotals`)
run against a Pulsar broker that graal starts for the run:

1. a producer publishes 16 order events to the `orders` topic, keyed by store;
2. a consumer reads them, acknowledges each one and sums the revenue per store;
3. the totals are published to the `order-totals` topic, then read back and checked.

For a `pulsar` job, graal starts a Pulsar **standalone** (broker, BookKeeper and metadata store in one
process) inside the run's pod, waits until it answers, creates the topics listed in `topics`, runs
your program, then stops the broker. Its data lives under `/tmp` and nothing survives the run: the
type is meant for programs that use Pulsar as a working queue, and for testing Pulsar code.

- **Job type:** `pulsar` (specialised type: available only if your installation configured the
  `pulsar` runtime image, Apache Pulsar 4.0 on Java 21)
- **Topics:** `topics` lists the topics created before the program starts, as short names
  (`orders`, created as `persistent://public/default/orders`) or full names.
- **Entry point:** the jar (`main_library`) and the class to run (`main_class_name`). The job's
  `parameters` are the program arguments, here the two topics. The broker's URLs are in the
  environment (`PULSAR_SERVICE_URL`, `PULSAR_ADMIN_URL`) and in the system properties
  `pulsar.serviceUrl` and `pulsar.adminUrl`.
- **Memory:** the broker and the program share the pod's memory. If the broker does not start in
  time ("Pulsar standalone n'a pas démarré" in the log), choose a larger instance type.
- **Toolchain:** Java 17+, Maven 3.9. The Pulsar client API (`pulsar-client-api` 4.0.14) is a
  `provided` dependency: the image puts its own client libraries (`/pulsar/lib`) on the classpath,
  the jar only holds the example classes.

## Build

```sh
mvn -q package -DskipTests
# -> target/pulsar-example-1.0.0-SNAPSHOT.jar
```

## Deploy

With [`deploy.sh`](../deploy.sh), from the repository root: it builds the jar, uploads it, fills in
`job.json`, creates the job and, with `--run`, runs it and streams its logs.

```sh
./deploy.sh pulsar-example --project "$PROJECT_ID" --run
```

Or by hand:

```sh
JAR_KEY=$(graalctl libraries upload target/pulsar-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The run's log shows the broker starting and the topics being created, then the program's output,
interleaved with the Pulsar client's own `INFO` lines:

```text
Pulsar prêt : pulsar://127.0.0.1:6650
Sujet créé : persistent://public/default/orders
Sujet créé : persistent://public/default/order-totals
Running (pulsar, heap ...)...
Broker pulsar://127.0.0.1:6650, orders persistent://public/default/orders, totals persistent://public/default/order-totals
Published 16 orders
Consumed 16 orders
Revenue per store (from persistent://public/default/order-totals):
  lyon        67.00 EUR
  nantes      50.30 EUR
  paris       53.40 EUR
Order totals OK: 16 orders, 170.70 EUR
Exit with status 0
```

If a message is missing or a total differs, the program exits with a non-zero status and the run fails.

If the API answers `501 feature_disabled`, the `pulsar` type is not enabled on your installation.
The options a specialised type accepts depend on the runtime image configured there: check the
installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
