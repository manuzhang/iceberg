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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.NestedParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetVectorizedScan extends TestParquetScan {
  @TempDir private Path nestedTemp;

  @Override
  protected boolean vectorized() {
    return true;
  }

  @BeforeEach
  void enableNestedVectorization() {
    spark.conf().set(SparkSQLProperties.PARQUET_NESTED_VECTORIZATION_ENABLED, "true");
  }

  @AfterEach
  void resetNestedVectorization() {
    spark.conf().unset(SparkSQLProperties.PARQUET_NESTED_VECTORIZATION_ENABLED);
  }

  @Test
  void importedNestedFileUsesNameMapping() throws Exception {
    Dataset<Row> source =
        spark.sql(
            "SELECT array(named_struct('value', 3, 'label', 'first')) AS events "
                + "UNION ALL SELECT array(named_struct('value', 4, 'label', 'second'))");
    File directory = nestedTemp.resolve("imported").toFile();
    source.coalesce(1).write().parquet(directory.toString());
    File[] files = directory.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertThat(files).hasSize(1);
    Schema original = SparkSchemaUtil.convert(source.schema());
    Types.NestedField events = original.findField("events");
    Types.ListType list = events.type().asListType();
    Schema projection =
        new Schema(
            Types.NestedField.optional(
                events.fieldId(),
                "renamed_events",
                Types.ListType.ofOptional(
                    list.elementId(),
                    Types.StructType.of(
                        Types.NestedField.optional(
                            original.findField("events.element.label").fieldId(),
                            "renamed_label",
                            Types.StringType.get()),
                        Types.NestedField.optional(
                            original.findField("events.element.value").fieldId(),
                            "renamed_value",
                            Types.LongType.get())))));
    List<String> labels = new ArrayList<>();
    List<Long> values = new ArrayList<>();
    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(Files.localInput(files[0]))
            .project(projection)
            .withNameMapping(MappingUtil.create(original))
            .recordsPerBatch(1)
            .createBatchedReaderFunc(
                type -> NestedParquetReaders.buildReader(projection, type, Map.of()))
            .build()) {
      for (ColumnarBatch batch : batches) {
        labels.add(batch.column(0).getArray(0).getStruct(0, 2).getUTF8String(0).toString());
        values.add(batch.column(0).getArray(0).getStruct(0, 2).getLong(1));
      }
    }
    assertThat(labels).containsExactly("first", "second");
    assertThat(values).containsExactly(3L, 4L);
  }

  @Test
  void nestedProjectionUsesColumnarReaderWithDeletes() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "data",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        Types.NestedField.optional(4, "name", Types.StringType.get()),
                        Types.NestedField.optional(
                            5, "values", Types.ListType.ofOptional(6, Types.IntegerType.get()))))),
            Types.NestedField.optional(
                7,
                "labels",
                Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.StringType.get())));
    Table table =
        new HadoopTables(new Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(
                    TableProperties.FORMAT_VERSION,
                    "2",
                    TableProperties.DEFAULT_FILE_FORMAT,
                    "parquet",
                    TableProperties.PARQUET_VECTORIZATION_ENABLED,
                    "true",
                    TableProperties.PARQUET_BATCH_SIZE,
                    "4"),
                nestedTemp.resolve("table").toString());
    List<Record> records = RandomGenericData.generate(table.schema(), 30, 19L);
    for (int i = 0; i < records.size(); i++) {
      records.get(i).setField("id", (long) i);
    }
    DataFile data =
        FileHelpers.writeDataFile(
            table, Files.localOutput(nestedTemp.resolve("data.parquet").toFile()), records);
    table.newAppend().appendFile(data).commit();

    DeleteFile positions =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(nestedTemp.resolve("positions.parquet").toFile()),
                List.of(Pair.of(data.location(), 7L), Pair.of(data.location(), 14L)))
            .first();
    Schema equalitySchema = table.schema().select("id");
    GenericRecord equality = GenericRecord.create(equalitySchema);
    equality.setField("id", records.get(6).getField("id"));
    DeleteFile equalities =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(nestedTemp.resolve("equalities.parquet").toFile()),
            List.of(equality),
            equalitySchema);
    table.newRowDelta().addDeletes(positions).addDeletes(equalities).commit();

    List<CombinedScanTask> tasks = new ArrayList<>();
    try (CloseableIterable<CombinedScanTask> planned = table.newScan().planTasks()) {
      planned.forEach(tasks::add);
    }
    assertThat(sparkBatch(table, tasks).createReaderFactory())
        .isInstanceOf(SparkColumnarReaderFactory.class);
    spark.conf().unset(SparkSQLProperties.PARQUET_NESTED_VECTORIZATION_ENABLED);
    assertThat(sparkBatch(table, tasks).createReaderFactory())
        .as("Nested vectorization is disabled by default")
        .isInstanceOf(SparkRowReaderFactory.class);
    spark.conf().set(SparkSQLProperties.PARQUET_NESTED_VECTORIZATION_ENABLED, "true");

    List<Row> expected =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, "false")
            .load(table.location())
            .select("data", "labels")
            .collectAsList();
    List<Row> actual =
        spark
            .read()
            .format("iceberg")
            .load(table.location())
            .select("data", "labels")
            .collectAsList();
    assertThat(actual).hasSize(records.size() - 3).containsExactlyInAnyOrderElementsOf(expected);

    List<Row> expectedMarked =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, "false")
            .load(table.location())
            .select("data", MetadataColumns.IS_DELETED.name(), MetadataColumns.ROW_POSITION.name())
            .collectAsList();
    List<Row> actualMarked =
        spark
            .read()
            .format("iceberg")
            .load(table.location())
            .select("data", MetadataColumns.IS_DELETED.name(), MetadataColumns.ROW_POSITION.name())
            .collectAsList();
    assertThat(actualMarked).containsExactlyInAnyOrderElementsOf(expectedMarked);
  }

  private SparkBatch sparkBatch(Table table, List<CombinedScanTask> tasks) {
    return new SparkBatch(
        sc,
        table,
        table::io,
        new SparkReadConf(spark, table),
        Types.StructType.of(),
        tasks,
        table.schema().select("data", "labels"),
        0);
  }
}
