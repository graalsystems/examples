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
                    .parquet("s3://" + System.getenv("AWS_BUCKET") + "/" + this.file);

            compute(parquet);

            return 0;
        } catch (Exception e) {
            log.error("Oupsss...", e);
            return 1;
        }
    }

    public static void compute(Dataset<Row> parquet) {
        parquet.show(false);
    }
}