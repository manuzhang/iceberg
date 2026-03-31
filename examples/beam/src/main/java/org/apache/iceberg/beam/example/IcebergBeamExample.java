/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.beam.example;

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

/**
 * An example that demonstrates reading from and writing to Apache Iceberg tables using Apache Beam.
 *
 * <p>The example:
 *
 * <ul>
 *   <li>Writes a small batch of {@link Row} records to an Iceberg table backed by a local Hadoop
 *       catalog.
 *   <li>Reads those records back and prints them to standard output.
 * </ul>
 *
 * <p>Run with Gradle: {@code gradle run}
 */
public class IcebergBeamExample {

  static final Schema SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("name")
          .addInt32Field("age")
          .build();

  public static void main(String[] args) throws Exception {
    File warehouseDir = Files.createTempDirectory("iceberg-beam-example").toFile();
    String warehousePath = warehouseDir.getAbsolutePath();

    System.out.println("Warehouse location: " + warehousePath);

    try {
      writeToIceberg(warehousePath);
      readFromIceberg(warehousePath);
    } finally {
      deleteDirectory(warehouseDir);
    }
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

  static void writeToIceberg(String warehousePath) {
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

    System.out.println("Wrote " + rows.size() + " rows to Iceberg table.");
  }

  static void readFromIceberg(String warehousePath) {
    Map<String, Object> config = buildConfig(warehousePath);

    Pipeline pipeline = Pipeline.create();
    PCollection<Row> rows =
        pipeline
            .apply("ReadFromIceberg", Managed.read(Managed.ICEBERG).withConfig(config))
            .getSinglePCollection();

    rows.apply(
        "PrintRows",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(@Element Row row) {
                System.out.printf(
                    "id=%d, name=%s, age=%d%n",
                    row.getInt64("id"), row.getString("name"), row.getInt32("age"));
              }
            }));

    pipeline.run().waitUntilFinish();

    System.out.println("Read rows from Iceberg table.");
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
      System.err.println("Warning: could not delete " + dir);
    }
  }
}
