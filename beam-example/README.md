# beam-example

An Apache Beam pipeline written in Java (`graal.systems.examples.beam.WordCount`): it counts the
words of a text embedded in the jar (the opening of *A Tale of Two Cities*), prints the most
frequent ones, and checks the counts read back from Beam metrics against a plain Java count of the
same text. It needs no data and no network.

- **Job type:** `beam` (specialised type: available only if your installation configured the
  `beam-java` runtime image, Beam 2.76.0 on Java 17)
- **Runner:** `"runner": "direct"` runs the pipeline with the DirectRunner inside the run's pod; graal
  adds `--runner=DirectRunner --targetParallelism=<CPUs>` before the job's `parameters`.
  `"runner": "flink"` runs the same jar on a Flink cluster created for the run (FlinkRunner; set
  `job_manager_instance_type`, `task_manager_instance_type` and `number_task_managers`); the top words
  are then printed by a TaskManager, in its own log.
- **Entry point:** the jar (`main_library`) and the class to run (`main_class_name`). The job's
  `parameters` are pipeline options: `--top=8` prints the 8 most frequent words (default 10).
- **Toolchain:** Java 17+, Maven 3.9. The Beam SDK (`beam-sdks-java-core` 2.76.0) is a `provided`
  dependency: the image puts Beam and its runners on the classpath (`/opt/beam/lib`), the jar only
  holds the example classes. Set `-Dbeam.version=...` to match your runtime image.

## Build

```sh
mvn -q package -DskipTests
# -> target/beam-example-1.0.0-SNAPSHOT.jar
```

## Deploy

With [`deploy.sh`](../deploy.sh), from the repository root: it builds the jar, uploads it, fills in
`job.json`, creates the job and, with `--run`, runs it and streams its logs.

```sh
./deploy.sh beam-example --project "$PROJECT_ID" --run
```

Or by hand:

```sh
JAR_KEY=$(graalctl libraries upload target/beam-example-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The run's log ends with:

```text
Top 8 words:
  the           11
  it            10
  of            10
  was           10
  we             4
  age            2
  all            2
  before         2
Counted 85 words, 34 distinct (expected 85 and 34)
Word count OK
Exit with status 0
```

If a count does not match, the program exits with a non-zero status and the run fails.

If the API answers `501 feature_disabled`, the `beam` type is not enabled on your installation.
The options a specialised type accepts depend on the runtime image configured there: check the
installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).
