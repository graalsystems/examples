# pekko-example

An [Apache Pekko](https://pekko.apache.org) Streams pipeline (`graal.systems.examples.pekko.SensorStats`)
over generated temperature readings from five sensors: it drops the invalid readings (about 1 %),
groups the others by sensor, and computes count, min, mean and max per sensor in parallel
sub-streams. The totals are checked against a plain Java pass over the same readings.

Apache Pekko is the Apache 2.0 fork of Akka 2.6, made when Akka moved to the Business Source
License; graal's `akka-data-pipeline` type runs Pekko applications.

- **Job type:** `akka-data-pipeline` (specialised type: available only if your installation
  configured the `jvm` runtime image, Java 21)
- **Entry point:** the jar (`main_library`) and the class to run (`main_class_name`). The job's
  `parameters` are the number of readings to generate (default 100000) and the random seed
  (default 42); `properties` become `-D` system properties, which Pekko's configuration reads
  (for example `pekko.loglevel`).
- **Toolchain:** Java 21, Maven 3.9. The `jvm` image carries no library: Pekko 1.7 and the Scala
  library are shaded into the jar by `maven-shade-plugin`, which concatenates the modules'
  `reference.conf` files.

## Build

```sh
mvn -q package -DskipTests
# -> target/pekko-example-1.0.0-SNAPSHOT.jar
```

## Deploy

With [`deploy.sh`](../deploy.sh), from the repository root: it builds the jar, uploads it, fills in
`job.json`, creates the job and, with `--run`, runs it and streams its logs.

```sh
./deploy.sh pekko-example --project "$PROJECT_ID" --run
```

Or by hand:

```sh
JAR_KEY=$(graalctl libraries upload target/pekko-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The run's log ends with:

```text
200000 readings generated (seed 42)
  sensor         count      min     mean      max
  boiler         39406    54.27    65.00    75.88
  cellar         39453     1.15    12.02    21.64
  kitchen        39639    10.18    20.99    30.87
  roof           39437    -2.59     8.00    17.90
  server-room    40041    16.71    26.98    37.40
197976 valid readings in 5 sensors, 2024 dropped (expected 197976 valid)
Sensor statistics OK
Exit with status 0
```

If the totals do not match, the program exits with a non-zero status and the run fails.

If the API answers `501 feature_disabled`, the `akka-data-pipeline` type is not enabled on your
installation. The options a specialised type accepts depend on the runtime image configured there:
check the installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
