from pulsar import Client, AuthenticationToken
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--pulsar_broker", help="URL of Pulsar broker", required=True)
parser.add_argument("--pulsar_topic", help="Pulsar topic", required=True)
parser.add_argument("--pulsar_token", help="Pulsar token", required=True)
args = parser.parse_args()

input_variable="FULL_VariableRecording_2022-04-13_17-45-18.parquet"

PULSAR_BROKER_ENDPOINT = args.pulsar_broker
PULSAR_AUTH_TOKEN= args.pulsar_token
PULSAR_TOPIC = args.pulsar_topic

data_variable = pd.read_parquet(input_variable)

client = Client(PULSAR_BROKER_ENDPOINT, tls_allow_insecure_connection=True, tls_validate_hostname=False, authentication=AuthenticationToken(PULSAR_AUTH_TOKEN))
producer = client.create_producer(PULSAR_TOPIC)

for index, row in data_variable[:10].iterrows():
    producer.send(row.to_json(date_format = "iso").encode('utf-8'))