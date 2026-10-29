# hadoop-examples

A Hadoop MapReduce word count (`graal.systems.examples.WordCount`), written against the stock
Hadoop 3.4 API. The same jar runs two ways:

| How | What runs it |
|---|---|
| A graal job of type `apachemapreduce` | `hadoop jar` in local MapReduce mode, in one pod of the `hadoop` runtime image (Hadoop 3.4.3, Java 11) |
| A stock Hadoop client and the **YARN bridge** | `hadoop jar` on your machine, submitting to graal as if it were a YARN ResourceManager; the application becomes a graal run, each YARN container a pod of that run |

- **Job type:** `apachemapreduce` (specialised type: available only if your installation configured
  the `hadoop` runtime image; `apachehadoop` takes the same options)
- **Entry point:** the jar (`main_library`) and the class to run (`main_class_name`); the job's
  `parameters` are the program arguments (`<input> <output>`), `properties` become `-D key=value`
  generic options
- **Storage:** paths in `s3a://` (or local paths inside the pod). There is no HDFS: graal gives the
  run the keys of a bucket named in an `s3a://` path when the bucket belongs to the project and the
  job's identity can read and write it. No key goes in the job.
- **Toolchain:** Java 11+, Maven 3.9. Hadoop 3.4.3 (`hadoop-client-api`) is a `provided`
  dependency; set `-Dhadoop.version=...` to match your runtime image or client.

## Build

```sh
mvn -q package -DskipTests
# -> target/hadoop-examples-1.0.0-SNAPSHOT.jar
```

## Run as a graal job

Put the sample input in the project's bucket, with any S3 client, for example:

```sh
aws s3 cp data/input.txt "s3://$BUCKET/hadoop-examples/input/input.txt" --endpoint-url "$S3_ENDPOINT"
```

Then create and run the job:

```sh
JAR_KEY=$(graalctl libraries upload target/hadoop-examples-1.0.0-SNAPSHOT.jar)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$JAR_KEY/" \
    -e "s/<BUCKET>/$BUCKET/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

The result is in `s3a://<BUCKET>/hadoop-examples/output/part-r-*`. Like any MapReduce job, it fails
if the output directory already exists: delete it, or change the second parameter, before running
again. The run's results list each MapReduce job with its state and main counters.

If the API answers `501 feature_disabled`, the `apachemapreduce` type is not enabled on your
installation. The options a specialised type accepts depend on the runtime image configured there:
check the installation's API reference (`/api/v1/docs`) and adjust [`job.json`](job.json).

## Submit through the YARN bridge

The YARN bridge is an optional component of graal: when your installation enables it, an
unmodified Hadoop 3.x client (Hadoop 3.4.3 is the tested version) submits MapReduce (and Spark on
YARN) applications to graal. Each application becomes a run of the bridge's job, in the bridge's
project and under the bridge's identity.

1. Create a bridge of type `yarn` (`POST /api/v1/bridges`) for your project, identity and bucket,
   or ask your administrator for one. Creating it requires the right to use that identity and to
   edit that project. A request body looks like this (`mode` is `distributed`, one pod per YARN
   container, or `local`, the whole job replayed in one pod):

   ```json
   {
     "type": "yarn",
     "description": "hadoop-examples",
     "metadata": {
       "project_id": "<PROJECT_ID>",
       "identity_id": "<IDENTITY_ID>",
       "infrastructure_id": "<INFRASTRUCTURE_ID>",
       "bucket": "<BUCKET>",
       "s3_endpoint": "<S3_ENDPOINT>",
       "instance_type": "Standard_Development_D1_v1",
       "mode": "distributed"
     }
   }
   ```

2. Read it (`GET /api/v1/bridges/<BRIDGE_ID>`). Its `configuration` holds `core-site.xml`,
   `yarn-site.xml` and `mapred-site.xml`, a token file `graal-yarn.tokens` (base64) and the bucket
   keys in `graal-s3.env`. Write the three XML files to a directory, decode the token file, and load
   the keys into the environment. Keep the token file and the keys private: whoever holds them acts
   as the bridge's identity.
3. Submit with your Hadoop client:

```sh
export HADOOP_CONF_DIR="$PWD/bridge-conf"          # core-site.xml, yarn-site.xml, mapred-site.xml
export HADOOP_TOKEN_FILE_LOCATION="$PWD/graal-yarn.tokens"
set -a; . ./graal-s3.env; set +a                    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

hadoop fs -mkdir -p "s3a://$BUCKET/hadoop-examples/input"
hadoop fs -put -f data/input.txt "s3a://$BUCKET/hadoop-examples/input/"
hadoop jar target/hadoop-examples-1.0.0-SNAPSHOT.jar graal.systems.examples.WordCount \
  -D mapreduce.job.reduces=2 \
  "s3a://$BUCKET/hadoop-examples/input" "s3a://$BUCKET/hadoop-examples/output-yarn"
hadoop fs -cat "s3a://$BUCKET/hadoop-examples/output-yarn/part-r-*"
```

`yarn application -list` and `yarn application -kill <APPLICATION_ID>` work as with a
ResourceManager; killing an application cancels its run. Without the token file, every call is
refused. If the bridge is not enabled on your installation, the API answers `501 feature_disabled`.
