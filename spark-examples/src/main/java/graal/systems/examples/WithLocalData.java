package graal.systems.examples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;

//
@Slf4j
@CommandLine.Command
public class WithLocalData implements Callable<Integer> {

    public static void main(String... args) {
        int exitCode = new CommandLine(new WithLocalData()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            compute(sparkSession);

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }

    protected static void compute(SparkSession sparkSession) {
        Dataset<Row> data = sparkSession.createDataFrame(
                new JavaSparkContext(sparkSession.sparkContext())
                        .parallelize(Arrays.asList(
                                new Item(1L, "Title1", "Category1", 2.0d),
                                new Item(2L, "Title2", "Category1", 2.0d),
                                new Item(3L, "Title3", "Category2", 2.0d),
                                new Item(4L, "Title4", "Category2", 2.0d),
                                new Item(5L, "Title5", "Category2", 2.0d)
                        )), Item.class);
        data.printSchema();

        data = data.filter(data.col("id").leq(20L)).cache();
        data.groupBy("category").count().show();
        data.groupBy("category").min("price").show();
        data.groupBy("category").max("price").show();
        data.groupBy("category").sum("price").show();
    }

    @AllArgsConstructor
    @Data
    public static class Item implements Serializable {
        private Long id;
        private String title;
        private String category;
        private Double price;
    }
}