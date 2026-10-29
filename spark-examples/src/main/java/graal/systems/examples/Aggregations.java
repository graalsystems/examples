package graal.systems.examples;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** The aggregation shared by the examples: count, min, max and sum of prices per category. */
final class Aggregations {

    private Aggregations() {
    }

    static Dataset<Row> byCategory(Dataset<Row> data) {
        Dataset<Row> result = data
                .filter(col("id").leq(20L))
                .groupBy("category")
                .agg(count("*").alias("items"),
                        min("price").alias("min_price"),
                        max("price").alias("max_price"),
                        sum("price").alias("total_price"))
                .orderBy("category");
        result.show(false);
        return result;
    }
}
