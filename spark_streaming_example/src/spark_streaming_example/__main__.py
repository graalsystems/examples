from pyspark.sql import SparkSession

# Local
# PULSAR_BROKER_ENDPOINT = "pulsar+ssl://127.0.0.1:6651"
# PULSAR_ADMIN_ENDPOINT = "https://127.0.0.1:443"

PULSAR_BROKER_ENDPOINT = "pulsar+ssl://stream.dev.admin.graal.systems:6651"
PULSAR_ADMIN_ENDPOINT = "https://stream.dev.admin.graal.systems:443"
PULSAR_TOPIC = "apache/pulsar/test-topic"
PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.RQVisrLSByQOuZ6-lxuU8CkmlGcjM_jTLB0L4FQjCt83hrNVmWBPiJSIHtCjeIMznsEFePThmHOLEbiI2NqREaMn4mOwgTNgf81IUpmm4n3bzQ1BVZ4YOIuafY-4uzkqG5FUlT6yIaoYRgrmYlr4BHNEehXwBqIco2kbQVhIkDmpw25objIc64mKv-9A2lSlQHehqKmu33UUJmzXvS54Lczqg8X9LlBNvRqfI-01SSBe2wPz4vUDDRY3tH5WevicGM2mGcB4s2kZ0j5vmtTEqw-bMtzRprj9AsKSC8h2JcQUV-ZJBSxV9bZy3ATZNwST_JZO7w2GyKO5nA5FypUuUQ"

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .getOrCreate()

while True:
    stream_df = spark \
            .readStream \
            .format("pulsar") \
            .option("service.url", PULSAR_BROKER_ENDPOINT) \
            .option("admin.url", PULSAR_ADMIN_ENDPOINT) \
            .option("topic", PULSAR_TOPIC) \
            .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken") \
            .option("pulsar.client.authParams","token:" + PULSAR_TOKEN) \
            .option("pulsar.client.tlsAllowInsecureConnection","false") \
            .option("pulsar.client.tlsHostnameVerificationenable","false") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

    stream_df.writeStream.format("console").start().awaitTermination()
