package graal.systems.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

public class Flink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setAdminUrl("http://127.0.0.1:80")
                .setTopics("apache/pulsar/test-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("my-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        DataStreamSource<String> test = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");

        test.print();

        try {
            env.execute();
        }
        catch (Exception e) {
            System.out.println("Error: " + e);
        }
        System.out.println("Finish!");
    }
}
