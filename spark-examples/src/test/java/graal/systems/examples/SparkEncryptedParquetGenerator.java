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
import java.util.List;
import java.util.concurrent.Callable;

//
@Slf4j
@CommandLine.Command
public class SparkEncryptedParquetGenerator implements Callable<Integer> {

    public static void main(String... args) {
        int exitCode = new CommandLine(new SparkEncryptedParquetGenerator()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            SparkSession sparkSession = SparkSession.builder()
                    .appName("Encrypt data in Parquet")
                    .master("local[1]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.driver.allowMultipleContexts", "true")
                    .config("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
                    .config("parquet.encryption.kms.client.class", "graal.systems.sdk.parquet.kms.InternalKmsClient")
                    .config("parquet.encryption.kms.instance.id", "platform-demo-internal")
                    .config("parquet.encryption.kms.instance.url", "http://localhost:4200/api/v1/secrets/")
                    .getOrCreate();

            List<Measure> measures = Arrays.asList(
                    new Measure("Title1", 123456789L, 2.0d),
                    new Measure("Title2", 123256789L, 1.0d),
                    new Measure("Title3", 123556789L, 9.0d),
                    new Measure("Title4", 123656789L, 5.0d),
                    new Measure("Title5", 129456789L, 1.0d)
            );
            Dataset<Row> data = sparkSession.createDataFrame(
                    new JavaSparkContext(sparkSession.sparkContext())
                            .parallelize(measures), Measure.class);
            data.printSchema();
            data.write()
                    .option("parquet.encryption.column.keys", "86ad2621-50a3-4868-a864-e3dfe1d3fa0f:metric")
                    .option("parquet.encryption.footer.key", "266f8beb-9c42-4843-a5d6-e8baca97bb47")
                    .parquet("spark-examples/src/test/resources/data-encrypted-parquet.parquet");

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }

    @AllArgsConstructor
    @Data
    public static class Measure implements Serializable {
        private String metric;
        private Long timestamp;
        private Double value;
    }
}