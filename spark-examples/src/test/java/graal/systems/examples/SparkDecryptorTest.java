package graal.systems.examples;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class SparkDecryptorTest {

//    @Test
    public void test() {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Decrypt data in Parquet")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.allowMultipleContexts", "true")
                .config("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
                .config("parquet.encryption.kms.client.class", "graal.systems.sdk.parquet.kms.InternalKmsClient")
                .config("parquet.encryption.kms.instance.id", "platform-demo-internal")
                .config("parquet.encryption.kms.instance.url", "http://172.23.96.1:8080/api/v1/secrets/")
//                .config("parquet.encryption.key.material.store.internally", "false")
                .getOrCreate();

        sparkSession.read()
                .option("parquet.encryption.column.keys", "89e14113-1f66-416a-a825-1b567b5d66a2:metric")
                .option("parquet.encryption.footer.key", "9deff957-e14e-4fbf-85f4-b14146d27361")
                .parquet("src/test/resources/data-encrypted-parquet.parquet")
                .show();


    }
}