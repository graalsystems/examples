package graal.systems.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@Slf4j
@CommandLine.Command
public class WithEncryptedParquet implements Callable<Integer> {

    @CommandLine.Option(names = {"--field-value-encrypt-key"}, description = "Encrypt key for field \"value\"", required = true)
    private String fieldValueEncryptKey = "xxxx-xxxx-xxxx-xxx";

    @CommandLine.Option(names = {"--footer-encrypt-key"}, description = "Encrypt key for footer", required = true)
    private String footerEncryptKey = "xxxx-xxxx-xxxx-xxx";

    @CommandLine.Option(names = {"--file"}, description = "Parquet file", required = true)
    private String file = "data-encrypted-parquet.parquet";

    public static void main(String... args) {
        int exitCode = new CommandLine(new WithEncryptedParquet()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            Dataset<Row> parquet = sparkSession
                    .read()
                    .option("parquet.encryption.column.keys", this.fieldValueEncryptKey)
                    .option("parquet.encryption.footer.key", this.footerEncryptKey)
                    .parquet("s3://" + System.getenv("AWS_BUCKET") + "/" + this.file);

            parquet.show(false);

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }
}