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
public class WithS3 implements Callable<Integer> {

    public static void main(String... args) {
        int exitCode = new CommandLine(new WithS3()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            Dataset<Row> data = sparkSession.read().csv("s3://" + System.getenv("AWS_BUCKET"+"/"));
            data.printSchema();

            data = data.filter(data.col("id").leq(20L)).cache();
            data.groupBy("category").count().show();
            data.groupBy("category").min("price").show();
            data.groupBy("category").max("price").show();
            data.groupBy("category").sum("price").show();

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
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