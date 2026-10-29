package graal.systems.examples;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Builds a dataset in memory and aggregates it by category. */
@CommandLine.Command(name = "with-local-data", mixinStandardHelpOptions = true)
public class WithLocalData implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(WithLocalData.class);

    public static void main(String... args) {
        System.exit(new CommandLine(new WithLocalData()).execute(args));
    }

    @Override
    public Integer call() {
        SparkSession spark = SparkSession.builder().appName("with-local-data").getOrCreate();
        try {
            compute(spark);
            return 0;
        } catch (Exception e) {
            LOG.error("Job failed", e);
            return 1;
        } finally {
            spark.stop();
        }
    }

    static void compute(SparkSession spark) {
        List<Item> items = List.of(
                new Item(1L, "Title1", "Category1", 2.0d),
                new Item(2L, "Title2", "Category1", 2.0d),
                new Item(3L, "Title3", "Category2", 2.0d),
                new Item(4L, "Title4", "Category2", 2.0d),
                new Item(5L, "Title5", "Category2", 2.0d));

        Dataset<Row> data = spark.createDataset(items, Encoders.bean(Item.class)).toDF();
        data.printSchema();
        Aggregations.byCategory(data);
    }

    /** A JavaBean: Spark maps its getters to columns. */
    public static class Item implements Serializable {
        private Long id;
        private String title;
        private String category;
        private Double price;

        public Item() {
        }

        public Item(Long id, String title, String category, Double price) {
            this.id = id;
            this.title = title;
            this.category = category;
            this.price = price;
        }

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getTitle() { return title; }
        public void setTitle(String title) { this.title = title; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public Double getPrice() { return price; }
        public void setPrice(Double price) { this.price = price; }
    }
}
