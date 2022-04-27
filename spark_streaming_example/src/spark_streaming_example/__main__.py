from pyspark.sql import SparkSession

# Local
# PULSAR_BROKER_ENDPOINT = "pulsar+ssl://127.0.0.1:6651"
# PULSAR_ADMIN_ENDPOINT = "https://127.0.0.1:443"

PULSAR_BROKER_ENDPOINT = "pulsar+ssl://stream.dev.client.graal.systems:6651"
PULSAR_ADMIN_ENDPOINT = "https://stream.dev.client.graal.systems:443"
PULSAR_TOPIC = "apache/pulsar/test-topic"
PULSAR_AUTH_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.tYDsi1Ly8DJ6P1O-0rMK6VZDzRO6jIJ4mJ47apMRUhDxnHx_hNEJw1rUHCA9FtVvAwNcLTxDpLXzyAGj8YTON2y7LsON0R4d6tiDIq_AHOjRdKO07mqFa8FJmAmKqX_XqcyIrOxt8rYMuvnrkdLukDpcqO5ouYOH2PUVR6RbzNbgjRF5EL4471977jHR39r9yhXlkaR0Bwl9puS2NJ3um9QyAeVwLfMEn5Fw9XcY15V3_QNsT8KVNnHf0fwgkqn2020nhxObs4dcvMeXk8gI1y9A0am-TeI-LpipeMFFnwVGw2PfI-9yrnFQw2UiFsZ5FnbZSeUf2ZYfkd8Hf6GH6g"

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
            .option("pulsar.client.authParams","token:" + PULSAR_AUTH_TOKEN) \
            .option("pulsar.client.tlsAllowInsecureConnection","true") \
            .option("pulsar.client.tlsHostnameVerificationenable","false") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

    stream_df.writeStream.format("console").start().awaitTermination()
