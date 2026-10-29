package graal.systems.examples;

import static org.apache.flink.table.api.Expressions.row;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL with the Table API.
 *
 * <p>Without arguments, it aggregates an in-memory table. With a table name as argument (or
 * SOURCE_TABLE), it counts the rows of that table in the platform catalog: graal provides the
 * catalog to the run as SQL, in the file named by GRAAL_FLINK_CATALOG_SQL.
 */
public class FlinkSql {

    public static void main(String[] args) throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        String table = args.length > 0 ? args[0] : System.getenv("SOURCE_TABLE");
        if (table != null && !table.isBlank()) {
            countCatalogTable(tEnv, table);
        } else {
            aggregateInMemory(tEnv);
        }
    }

    private static void aggregateInMemory(TableEnvironment tEnv) {
        Table orders = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("category", DataTypes.STRING()),
                        DataTypes.FIELD("price", DataTypes.DOUBLE())),
                row("Category1", 2.0d),
                row("Category1", 3.5d),
                row("Category2", 1.0d),
                row("Category2", 4.0d),
                row("Category2", 5.0d));
        tEnv.createTemporaryView("orders", orders);

        tEnv.executeSql("SELECT category, COUNT(*) AS items, SUM(price) AS total "
                + "FROM orders GROUP BY category ORDER BY category").print();
    }

    private static void countCatalogTable(TableEnvironment tEnv, String table) throws Exception {
        String catalogSql = System.getenv("GRAAL_FLINK_CATALOG_SQL");
        if (catalogSql == null || catalogSql.isBlank()) {
            throw new IllegalStateException("GRAAL_FLINK_CATALOG_SQL is not set: no catalog available");
        }
        tEnv.executeSql(Files.readString(Path.of(catalogSql)));
        tEnv.useCatalog("graal");

        tEnv.executeSql("SELECT COUNT(*) AS row_count FROM " + table).print();
    }
}
