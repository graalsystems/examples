# graal Data Platform examples

Sample jobs for the [graal Data Platform](https://graal.systems/en/docs), the open-source
(Apache 2.0) data platform that you self-host on Kubernetes. Each directory is a small,
self-contained example with its code, a `README.md` and a `job.json` ready to adapt.

## Prerequisites

- Access to a graal installation: its API URL (ending with `/api/v1`), its OpenID Connect
  (SSO) URL and your tenant.
- [`graalctl`](https://github.com/graalsystems/graalctl), the command-line interface:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/graalsystems/graalctl/main/install.sh | sh

  graalctl config set-context my-install \
    --api-url https://graal.example.com/api/v1 \
    --auth-url https://sso.example.com \
    --tenant my-tenant
  graalctl login
  ```

- A project and an identity to run jobs with:
  `graalctl projects list` and `graalctl identities list`.
- The build tools of the example you pick (Python 3.11, or Java 17 and Maven, Java 21 for the
  Spring and Pekko examples; `zip` for the KNIME and SAS archives).

## Examples

| Example | Job type | What it shows |
|---|---|---|
| [`bash_example`](bash_example) | `bash` | A job defined only by shell lines; prints `GRAAL_LOGICAL_DATE` and `GRAAL_ATTEMPT`. |
| [`pandas_simple`](pandas_simple) | `python` | pandas on in-memory data, with dependencies from `requirements.txt`. |
| [`pandas_s3`](pandas_s3) | `python` | pandas reading from and writing to the project's bucket; credentials from secrets. |
| [`pytorch_examples`](pytorch_examples) | `python` | Training a PyTorch network on CPU or GPU. |
| [`pyspark-examples`](pyspark-examples) | `spark` | A PySpark application with a zipped package; reading a catalog table. |
| [`spark-examples`](spark-examples) | `spark` | Spark applications in Java (jar + main class); object storage and catalog access. |
| [`dask_ml_examples`](dask_ml_examples) | `dask` | Dask dataframes, scikit-learn, XGBoost and LightGBM on Dask. |
| [`ray_examples`](ray_examples) | `ray` | Ray tasks, actors and Ray Data. |
| [`flink-example`](flink-example) | `flink` | Flink DataStream (batch mode) and Flink SQL, including the platform catalog. |
| [`dbt_examples`](dbt_examples) | `dbt` | A dbt project with models and data tests. |
| [`tensorflow_examples`](tensorflow_examples) | `tensorflow` | Distributed TensorFlow training with `MultiWorkerMirroredStrategy`, on CPU or GPU. |
| [`knime_examples`](knime_examples) | `knime` | A KNIME workflow run in batch mode (KNIME 5.8.4). |
| [`sas`](sas) | `sas` | A SAS program run by graal-sas, without a SAS licence: DATA steps, PROC SQL, an HTML report. |
| [`hadoop-examples`](hadoop-examples) | `apachemapreduce`, `apachehadoop` | A MapReduce word count on `s3a://`, as a graal job or submitted through the YARN bridge. |
| [`beam-example`](beam-example) | `beam` | An Apache Beam pipeline in Java (DirectRunner): a word count checked against Beam metrics. |
| [`spring-batch-example`](spring-batch-example) | `spring-batch` | A two-step Spring Batch job (chunked CSV load, report) with its job repository in an in-memory H2. |
| [`spring-cloud-task-example`](spring-cloud-task-example) | `spring-cloud-data-flow` | A Spring Cloud Task, run by graal without a Data Flow server: reconciling payments against invoices. |
| [`pekko-example`](pekko-example) | `akka-data-pipeline` | An Apache Pekko Streams pipeline: per-sensor statistics in parallel sub-streams. |
| [`pulsar-example`](pulsar-example) | `pulsar` | A producer and consumers on the Pulsar standalone that graal starts in the run's pod. |
| [`hive_example`](hive_example) | `hive` | A HiveQL script (spark-sql on an Iceberg catalogue): a table, an insert, two aggregations. |
| [`lowcode_example`](lowcode_example) | `lowcode` | A low-code graph (read, aggregate, quality checks, write) turned into pandas or PySpark at run time. |
| [`notebook_example`](notebook_example) | `notebook` | A parameterised Jupyter notebook run top to bottom as a batch job. |
| [`docker_example`](docker_example) | `docker` | Your own container image, run once with your command under graal's pod rules. |
| [`database_migration_example`](database_migration_example) | `database-migration` | A Liquibase changelog applied to a registered data warehouse. |
| [`ci-github-actions`](ci-github-actions) | any | Running a job from GitHub Actions with `graalctl`. |

`bash`, `python` and `spark` are core types, available on every installation (images `toolbox`,
`python` with Python 3.11, and `spark-py` with Apache Spark 3.5).

`dask`, `ray`, `flink`, `dbt`, `hive`, `notebook`, `tensorflow`, `knime`, `sas`, `apachemapreduce`,
`apachehadoop`, `beam`, `spring-batch`, `spring-cloud-data-flow`, `akka-data-pipeline`, `pulsar` and
`database-migration` are **specialised types**: they exist only if your installation configured
their runtime image. Otherwise the API answers `501 feature_disabled`. Their options
depend on that image: check your installation's API reference (`/api/v1/docs`) before using the
`job.json` of these examples.

## Deploy an example in one command

Every example directory has a `graal.env` that says how to build it and which files to upload.
[`deploy.sh`](deploy.sh) builds the example, uploads its libraries, fills `job.json` and creates the
job — and runs it with `--run`:

```sh
export PROJECT_ID=...   IDENTITY_ID=...          # graalctl projects list / graalctl identities list
./deploy.sh lowcode_example --run                # build + upload + create + run, logs streamed
./deploy.sh hive_example --dry-run               # print the job that would be created, send nothing
```

Some examples need a value of your installation, named in their `graal.env` (`REQUIRES`) and taken
from the environment: `BUCKET` (`hadoop-examples`, `sas`), `CONNECTION_ID` (`knime_examples`),
`DATAWAREHOUSE_ID` (`database_migration_example`), `IMAGE` (`docker_example`). `deploy.sh` refuses to
send a job with a placeholder left.

## How a job runs

Code reaches a run through **libraries**: you upload files, get a key for each, and list the keys
in the job. Before the run starts, the scheduler copies the libraries into `/workspace`; the job's
`file` names the file to run there. The `python` image installs a `requirements.txt` or wheels
found in `/workspace` at start-up. Secrets the job's identity is entitled to are written to
`/workspace/secrets/`.

The flow is the same for every example:

```sh
# 1. Build (if needed) and upload the code: each upload prints a library key
KEY=$(graalctl libraries upload first_job.py)

# 2. Fill in job.json: <IDENTITY_ID>, <LIBRARY_KEY>, and an instance type
#    from `graalctl instance-types list`
sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" -e "s/<LIBRARY_KEY>/$KEY/" job.json > job.local.json

# 3. Create the job
JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)

# 4. Run it and stream its logs; the exit code is the outcome of the run
graalctl jobs run "$JOB_ID" --follow

# Optional: run it on a schedule
graalctl jobs schedule "$JOB_ID" "0 6 * * *"
```

A `job.json` looks like this:

```json
{
  "name": "first-job",
  "identity_id": "<IDENTITY_ID>",
  "timeout_seconds": 900,
  "max_retries": 0,
  "libraries": [{ "type": "file", "key": "<LIBRARY_KEY>" }],
  "options": {
    "type": "python",
    "file": "first_job.py",
    "instance_type": "Standard_Development_D1_v1",
    "env": {}
  }
}
```

## Variables available in runs

| Variable | Content |
|---|---|
| `GRAAL_BUCKET` | The project's bucket |
| `GRAAL_ATTEMPT` | The attempt number when a run is retried |
| `GRAAL_LOGICAL_DATE` | The date a scheduled or backfill run processes |
| `GRAAL_FLINK_CATALOG_SQL` | Flink runs: a file with the SQL that declares the platform catalog |

To read and write catalog tables from Python, see the `graal` module in
[Data and SQL](https://graal.systems/en/docs/data-and-sql). In Spark, read them with
`spark.table("db.table")`.

## License

[Apache License 2.0](LICENSE).
