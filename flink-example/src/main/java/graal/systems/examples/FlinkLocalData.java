package graal.systems.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Counts people by gender and by country with the DataStream API, in batch execution mode. */
public class FlinkLocalData {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple3<String, String, String>> people = env.fromData(
                Tuple3.of("Reed", "United States", "Female"),
                Tuple3.of("Bradley", "United States", "Female"),
                Tuple3.of("Adams", "United States", "Male"),
                Tuple3.of("Lane", "United States", "Male"),
                Tuple3.of("Marshall", "United States", "Female"),
                Tuple3.of("Garza", "United States", "Male"),
                Tuple3.of("Gutierrez", "United States", "Male"),
                Tuple3.of("Fox", "Germany", "Female"),
                Tuple3.of("Medina", "United States", "Male"),
                Tuple3.of("Nichols", "United States", "Male"),
                Tuple3.of("Woods", "United States", "Male"),
                Tuple3.of("Welch", "United States", "Female"),
                Tuple3.of("Burke", "United States", "Female"),
                Tuple3.of("Russell", "United States", "Female"),
                Tuple3.of("Burton", "United States", "Male"));

        countBy(people.map(p -> Tuple2.of("gender=" + p.f2, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG)))
                .print();
        countBy(people.map(p -> Tuple2.of("country=" + p.f1, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG)))
                .print();

        env.execute("flink-local-data");
    }

    private static DataStream<Tuple2<String, Long>> countBy(DataStream<Tuple2<String, Long>> keyed) {
        // In batch mode, a keyed sum emits one final result per key.
        return keyed.keyBy(t -> t.f0).sum(1);
    }
}
