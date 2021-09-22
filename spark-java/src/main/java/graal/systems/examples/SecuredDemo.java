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

@Slf4j
@CommandLine.Command
public class SecuredDemo implements Callable<Integer> {

    public static void main(String... args) {
        int exitCode = new CommandLine(new SecuredDemo()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            SparkSession sparkSession = SparkSession.builder()
                    .appName("Plain")
                    .master("local[2]")
                    .config("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
                    .config("parquet.encryption.kms.client.class", "graal.systems.sdk.parquet.kms.InternalKmsClient")
                    .config("parquet.encryption.kms.instance.id", "sr-default-tenant")
                    .config("parquet.encryption.kms.instance.url", "https://staging.api.graal.systems")
                    .config("parquet.encryption.kms.access.token", System.getenv("GRAAL_TOKEN"))
                    .getOrCreate();

            Dataset<Row> parquet = sparkSession
                    .read()
                    .option("parquet.encryption.column.keys", "examples-field-value-encrypt-key: value")
                    .option("parquet.encryption.footer.key", "examples-footer-encrypt-key")
                    .parquet("s3://...");

            parquet.show(false);

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }

}