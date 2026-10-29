package graal.systems.examples;

import java.util.concurrent.Callable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Reads a CSV file from the project's bucket and aggregates it by category.
 *
 * <p>The bucket comes from GRAAL_BUCKET (set by graal in every run), else AWS_BUCKET. Storage
 * credentials and endpoint are taken from the Spark/Hadoop S3A configuration of the runtime.
 */
@CommandLine.Command(name = "with-s3", mixinStandardHelpOptions = true)
public class WithS3 implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(WithS3.class);

    @CommandLine.Option(names = "--path", description = "Object key of the CSV file (default: ${DEFAULT-VALUE})")
    private String path = "data-spark-java.csv";

    public static void main(String... args) {
        System.exit(new CommandLine(new WithS3()).execute(args));
    }

    @Override
    public Integer call() {
        SparkSession spark = SparkSession.builder().appName("with-s3").getOrCreate();
        try {
            String location = bucketUri() + "/" + path;
            LOG.info("Reading {}", location);

            StructType schema = new StructType()
                    .add("id", DataTypes.LongType, false)
                    .add("title", DataTypes.StringType, false)
                    .add("category", DataTypes.StringType, false)
                    .add("price", DataTypes.DoubleType, false);

            Dataset<Row> data = spark.read().option("header", "true").schema(schema).csv(location);
            data.printSchema();
            Aggregations.byCategory(data);
            return 0;
        } catch (Exception e) {
            LOG.error("Job failed", e);
            return 1;
        } finally {
            spark.stop();
        }
    }

    /** Accepts "name", "name/prefix" or a full URI such as "s3a://name/prefix". */
    static String bucketUri() {
        String bucket = System.getenv("GRAAL_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = System.getenv("AWS_BUCKET");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("No bucket: set GRAAL_BUCKET or AWS_BUCKET");
        }
        bucket = bucket.replaceAll("/+$", "");
        return bucket.contains("://") ? bucket : "s3a://" + bucket;
    }
}
