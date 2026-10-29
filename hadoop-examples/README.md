# Hadoop examples — WordCount on the graal YARN bridge

A plain MapReduce job (`ExampleRunner`, old `mapred` API) submitted with an **unmodified** Hadoop client to the
YARN bridge of graal Data Platform (ADR 0054 of the platform). The job contains no graal-specific code: the
former client library `graal.systems:yarn-rpc` and its `GraalRecordFactoryPBImpl` are no longer needed.

## Build

```sh
mvn package
```

`target/hadoop-examples-1.0-SNAPSHOT-uber.jar` holds the job. The unit test runs the same job with the local job
runner, without a cluster.

## Run on graal

1. In graal, create a bridge of type `yarn` in your project (console, API `POST /api/v1/bridges` or MCP).
2. Fetch its client configuration with a user who may edit the bridge and use its identity:
   `GET /api/v1/bridges/{id}` returns `core-site.xml`, `yarn-site.xml`, `mapred-site.xml`, the token file
   `graal-yarn.tokens` and `graal-s3.env` (the S3 keys of the project bucket). Write them to a directory, for
   example `./graal-conf`. They are credentials: keep them out of git.
3. Submit with a Hadoop 3.4 client:

```sh
export HADOOP_CONF_DIR="$PWD/graal-conf"
export HADOOP_TOKEN_FILE_LOCATION="$PWD/graal-conf/graal-yarn.tokens"
set -a; . ./graal-conf/graal-s3.env; set +a
hadoop jar target/hadoop-examples-1.0-SNAPSHOT-uber.jar graal.systems.examples.ExampleRunner \
  s3a://<bucket>/input/ s3a://<bucket>/output/wordcount
```

The application becomes a run of the project in graal: its status, logs and costs are those of the run.
Storage is S3A only (no HDFS).
