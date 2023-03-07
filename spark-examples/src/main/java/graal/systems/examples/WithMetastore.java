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
import java.util.Map;
import java.util.concurrent.Callable;

//
@Slf4j
@CommandLine.Command
public class WithMetastore implements Callable<Integer> {

    public static void main(String... args) {
        int exitCode = new CommandLine(new WithMetastore()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {

            String bucket = "fisheries-catch-data";

            SparkSession sparkSession = SparkSession.builder()
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

                    .config("spark.hadoop.fs.s3a.bucket." + bucket + ".aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
                    .config("spark.hadoop.fs.s3a.bucket." + bucket + ".connection.maximum", "15")
                    .config("spark.hadoop.fs.s3a.bucket." + bucket + ".attempts.maximum", "20")
                    .config("spark.hadoop.fs.s3a.bucket." + bucket + ".connection.establish.timeout", "5000")
                    .config("spark.hadoop.fs.s3a.bucket." + bucket + ".connection.timeout", "5000")
                    .getOrCreate();

            sparkSession.sql("CREATE DATABASE IF NOT EXISTS raw");
            if (!sparkSession.catalog().tableExists("raw.fisheries_catch")) {
                Map<String, String> options = Map.of(
                        "source", "csv",
                        "sep", ",",
                        "header", "true",
                        "path", "s3://fisheries-catch-data/global-catch-data/csv/rfmo_12.csv"
                );
                sparkSession.catalog().createTable("raw.fisheries_catch", "csv", options);
            }
            sparkSession.catalog().listTables("raw").show();
            sparkSession.sql("select * from raw.fisheries_catch limit 10").show();

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }
}
