package graal.systems.examples;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.sql.Timestamp;
import java.util.List;

public class ParquetGenerator {

    private static Schema avroSchema = SchemaBuilder
            .record("Example")
            .namespace("example")
            .fields()
            .requiredString("metric")
            .requiredFloat("value")
            .requiredLong("timestamp")
            .endRecord();

    public static void main(String... args) throws Exception {
        try (ParquetWriter<GenericRecord> parquetWriter
                     = AvroParquetWriter.<GenericRecord>builder(new Path("spark-examples/src/test/resources/data-parquet.parquet"))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withSchema(avroSchema).build()) {
            writeData(avroSchema, parquetWriter);
        }
        try (ParquetReader<GenericRecord> parquetReader
                     = AvroParquetReader.<GenericRecord>builder(new Path(
                "spark-examples/src/test/resources/data-parquet.parquet"
        ))
                .build()) {
            readData(parquetReader);
        }
    }

    private static void readData(ParquetReader<GenericRecord> parquetReader) throws java.io.IOException {
        GenericRecord read;
        while ((read = parquetReader.read()) != null) {
            List<Schema.Field> fields = read.getSchema().getFields();
            System.err.println("--------");
            for (Schema.Field f : fields) {
                System.err.println(f.name() + ": " + read.get(f.pos()));
            }
        }
    }

    private static void writeData(Schema avroSchema, ParquetWriter<GenericRecord> parquetWriter) throws Exception {
        create(avroSchema, parquetWriter, "1_1", Timestamp.valueOf("2020-12-08 09:00:01"), 1f);
        create(avroSchema, parquetWriter, "1_1", Timestamp.valueOf("2020-12-08 09:00:02"), 2f);
        create(avroSchema, parquetWriter, "1_1", Timestamp.valueOf("2020-12-08 09:00:03"), 3f);
        create(avroSchema, parquetWriter, "1_1", Timestamp.valueOf("2020-12-08 09:00:04"), 4f);
        create(avroSchema, parquetWriter, "2_1", Timestamp.valueOf("2020-12-08 09:00:01"), 5f);
    }

    private static void create(Schema avroSchema, ParquetWriter parquetWriter, String metric, Timestamp timestamp, float value) throws Exception {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("metric", metric);
        record.put("timestamp", timestamp.getTime());
        record.put("value", value);
        parquetWriter.write(record);
    }
}
