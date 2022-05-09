from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--pulsar_broker", help="URL of Pulsar broker", required=True)
parser.add_argument("--pulsar_admin", help="URL of Pulsar admin", required=True)
parser.add_argument("--pulsar_topic", help="Pulsar topic", required=True)
parser.add_argument("--pulsar_token", help="Pulsar token", required=True)
args = parser.parse_args()

PULSAR_BROKER_ENDPOINT = args.pulsar_broker
PULSAR_ADMIN_ENDPOINT = args.pulsar_admin
PULSAR_AUTH_TOKEN= args.pulsar_token
PULSAR_TOPIC = args.pulsar_topic

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
