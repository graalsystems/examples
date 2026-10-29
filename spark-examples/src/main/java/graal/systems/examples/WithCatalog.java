package graal.systems.examples;

import java.util.concurrent.Callable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Reads a table of the platform catalog. Iceberg tables are served through Apache Polaris and
 * read with {@code spark.table("db.table")}; the runtime image configures the catalog.
 */
@CommandLine.Command(name = "with-catalog", mixinStandardHelpOptions = true)
public class WithCatalog implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(WithCatalog.class);

    @CommandLine.Option(names = "--table", description = "Table to read, as db.table (default: $SOURCE_TABLE)",
            defaultValue = "${env:SOURCE_TABLE}")
    private String table;

    @CommandLine.Option(names = "--limit", description = "Rows to show (default: ${DEFAULT-VALUE})")
    private int limit = 10;

    public static void main(String... args) {
        System.exit(new CommandLine(new WithCatalog()).execute(args));
    }

    @Override
    public Integer call() {
        if (table == null || table.isBlank()) {
            LOG.error("No table: pass --table db.table or set SOURCE_TABLE");
            return 2;
        }
        SparkSession spark = SparkSession.builder().appName("with-catalog").getOrCreate();
        try {
            Dataset<Row> data = spark.table(table);
            data.printSchema();
            data.show(limit, false);
            LOG.info("{} has {} rows", table, data.count());
            return 0;
        } catch (Exception e) {
            LOG.error("Job failed", e);
            return 1;
        } finally {
            spark.stop();
        }
    }
}
