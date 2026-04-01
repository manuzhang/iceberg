package io.github.manuzhang.iceberg.examples;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating Apache Beam read/write operations against an Apache Iceberg table.
 *
 * <p>Uses Beam's built-in <a
 * href="https://beam.apache.org/documentation/io/built-in/iceberg/">Managed IcebergIO connector</a>
 * with a local Hadoop catalog and the Direct Runner so no external services are required.
 */
public class BeamExample {

  private static final Logger LOG = LoggerFactory.getLogger(BeamExample.class);

  static final Schema SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").addInt32Field("age").build();

  public static void main(String[] args) throws Exception {
    LOG.info("Starting Beam Example...");

    BeamExample example = new BeamExample();

    File warehouseDir = Files.createTempDirectory("iceberg-beam-example").toFile();
    try {
      example.writeToIceberg(warehouseDir.getAbsolutePath());
      example.readFromIceberg(warehouseDir.getAbsolutePath());
      LOG.info("Beam example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error in Beam example: {}", e.getMessage(), e);
      System.exit(1);
    } finally {
      deleteDirectory(warehouseDir);
    }
  }

  /**
   * Writes sample rows to an Iceberg table using Beam's Managed IcebergIO write transform.
   *
   * @param warehousePath local path for the Hadoop catalog warehouse
   */
  public void writeToIceberg(String warehousePath) {
    LOG.info("=== Write to Iceberg ===");

    Map<String, Object> config = buildConfig(warehousePath);

    List<Row> rows =
        Arrays.asList(
            Row.withSchema(SCHEMA).addValues(1L, "Alice", 30).build(),
            Row.withSchema(SCHEMA).addValues(2L, "Bob", 25).build(),
            Row.withSchema(SCHEMA).addValues(3L, "Charlie", 35).build());

    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply("CreateRows", Create.of(rows).withRowSchema(SCHEMA))
        .apply("WriteToIceberg", Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    LOG.info("Wrote {} rows to Iceberg table", rows.size());
  }

  /**
   * Reads rows back from the Iceberg table using Beam's Managed IcebergIO read transform and logs
   * each row to stdout.
   *
   * @param warehousePath local path for the Hadoop catalog warehouse
   */
  public void readFromIceberg(String warehousePath) {
    LOG.info("=== Read from Iceberg ===");

    Map<String, Object> config = buildConfig(warehousePath);

    Pipeline pipeline = Pipeline.create();
    PCollection<Row> rows =
        pipeline
            .apply("ReadFromIceberg", Managed.read(Managed.ICEBERG).withConfig(config))
            .getSinglePCollection();

    rows.apply(
        "LogRows",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(@Element Row row) {
                LOG.info(
                    "id={}, name={}, age={}",
                    row.getInt64("id"),
                    row.getString("name"),
                    row.getInt32("age"));
              }
            }));

    pipeline.run().waitUntilFinish();

    LOG.info("Read rows from Iceberg table");
  }

  private static Map<String, Object> buildConfig(String warehousePath) {
    Map<String, Object> catalogProperties = new HashMap<>();
    catalogProperties.put("type", "hadoop");
    catalogProperties.put("warehouse", warehousePath);

    Map<String, Object> config = new HashMap<>();
    config.put("table", "example_db.example_table");
    config.put("catalog_name", "local");
    config.put("catalog_properties", catalogProperties);
    return config;
  }

  private static void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    if (!dir.delete()) {
      LOG.warn("Could not delete {}", dir);
    }
  }
}
