package graal.systems.examples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import picocli.CommandLine;

import java.io.Serializable;
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
            log.info("Running job...");
            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            StructType schema = new StructType(
                    new StructField[]{
                            new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("title", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("category", DataTypes.StringType, false, Metadata.empty()),
                            new StructField("price", DataTypes.FloatType, false, Metadata.empty())
                    }
            );

            Dataset<Row> data = sparkSession.read()
                    .option("header", "true")
                    .schema(schema)
                    .csv("s3://" + System.getenv("AWS_BUCKET") + "/data-spark-java.csv");
            data.printSchema();

            data = data.filter(data.col("id").leq(20L)).cache();
            data.groupBy("category").count().show();
            data.groupBy("category").min("price").show();
            data.groupBy("category").max("price").show();
            data.groupBy("category").sum("price").show();

            log.info("Success!");
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