//package graal.systems.examples;
//
//import org.apache.spark.sql.SparkSession;
//import org.junit.jupiter.api.Test;
//
//public class WithLocalDataTest {
//
//    @Test
//    public void test() {
//
//        // Given
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("Demo")
//                .master("local[2]")
//                .config("spark.ui.enabled", "false")
//                .config("spark.driver.allowMultipleContexts", "true")
//                .getOrCreate();
//
//        WithLocalData.compute(sparkSession);
//
//    }
//}