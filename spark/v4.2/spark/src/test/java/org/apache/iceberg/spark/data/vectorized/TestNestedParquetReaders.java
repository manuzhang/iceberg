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
package org.apache.iceberg.spark.data.vectorized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.AvroDataTestBase;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestNestedParquetReaders extends AvroDataTestBase {
  private static final int BATCH_SIZE = 7;

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema schema, Schema expected) throws IOException {
    writeAndValidate(schema, expected, RandomGenericData.generate(schema, 100, 31L));
  }

  @Override
  protected void writeAndValidate(Schema schema, Schema expected, List<Record> records)
      throws IOException {
    validate(schema, expected, records, true, true, ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  private void validate(
      Schema schema,
      Schema expected,
      List<Record> records,
      boolean reuse,
      boolean dictionary,
      ParquetProperties.WriterVersion version)
      throws IOException {
    File file = temp.resolve("nested.parquet").toFile();
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file))
            .overwrite()
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .set(ParquetOutputFormat.ENABLE_DICTIONARY, Boolean.toString(dictionary))
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "256")
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "4096")
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "10")
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "4")
            .writerVersion(version)
            .build()) {
      writer.addAll(records);
    }

    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file))
            .project(expected)
            .recordsPerBatch(BATCH_SIZE)
            .createBatchedReaderFunc(
                type -> NestedParquetReaders.buildReader(expected, type, ID_TO_CONSTANT));
    if (reuse) {
      builder.reuseContainers();
    }
    try (CloseableIterable<ColumnarBatch> batches = builder.build();
        CloseableIterable<InternalRow> rows =
            Parquet.read(Files.localInput(file))
                .project(expected)
                .createReaderFunc(
                    type -> SparkParquetReaders.buildReader(expected, type, ID_TO_CONSTANT))
                .build()) {
      Iterator<InternalRow> expectedRows = rows.iterator();
      UnsafeProjection projection = UnsafeProjection.create(SparkSchemaUtil.convert(expected));
      int count = 0;
      for (ColumnarBatch batch : batches) {
        for (int i = 0; i < batch.numRows(); i++) {
          assertThat(expectedRows.hasNext()).isTrue();
          assertThat(projection.apply(batch.getRow(i)).copy())
              .as("Row %s", count)
              .isEqualTo(projection.apply(expectedRows.next()).copy());
          count += 1;
        }
      }
      assertThat(count).isEqualTo(records.size());
      assertThat(expectedRows.hasNext()).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nestedNullsEmptyCollectionsAndPageBoundaries(boolean dictionary) throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "data",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        optional(4, "name", Types.StringType.get()),
                        optional(
                            5,
                            "values",
                            Types.MapType.ofOptional(
                                6,
                                7,
                                Types.StringType.get(),
                                Types.ListType.ofOptional(8, Types.IntegerType.get())))))));
    List<Record> records = RandomGenericData.generate(schema, 500, 17L);
    for (boolean reuse : new boolean[] {true, false}) {
      for (ParquetProperties.WriterVersion version : ParquetProperties.WriterVersion.values()) {
        validate(schema, schema, records, reuse, dictionary, version);
      }
    }
  }

  @Test
  void wideThreeLevelSchema() throws IOException {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < 1200; i++) {
      fields.add(optional(10 + i, "field_" + i, Types.IntegerType.get()));
    }
    Schema schema =
        new Schema(
            optional(
                1,
                "events",
                Types.ListType.ofOptional(
                    2, Types.StructType.of(optional(3, "payload", Types.StructType.of(fields))))));
    writeAndValidate(schema, schema, RandomGenericData.generate(schema, 10, 31L));
  }

  @Test
  void renamedReorderedPromotedAndMissingChildren() throws IOException {
    Schema schema =
        new Schema(
            optional(
                1,
                "list",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        optional(3, "old_name", Types.IntegerType.get()),
                        optional(4, "value", Types.FloatType.get()),
                        optional(5, "removed", Types.StringType.get())))));
    Schema expected =
        new Schema(
            MetadataColumns.ROW_POSITION,
            optional(
                1,
                "renamed_list",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        optional(4, "value", Types.DoubleType.get()),
                        optional(3, "new_name", Types.LongType.get()),
                        Types.NestedField.optional("removed")
                            .withId(6)
                            .ofType(Types.StringType.get())
                            .withInitialDefault(Literal.of("default"))
                            .build()))));
    writeAndValidate(schema, expected);
  }

  @Test
  void missingChildrenPreserveParentNulls() throws IOException {
    Types.StructType struct = Types.StructType.of(optional(2, "old", Types.IntegerType.get()));
    Schema schema = new Schema(optional(1, "parent", struct));
    Schema expected =
        new Schema(
            optional(
                1, "parent", Types.StructType.of(optional(3, "added", Types.StringType.get()))));
    GenericRecord nullParent = GenericRecord.create(schema);
    GenericRecord presentParent = GenericRecord.create(schema);
    presentParent.setField("parent", GenericRecord.create(struct));
    File file = temp.resolve("missing-children.parquet").toFile();
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(List.of(nullParent, presentParent));
    }
    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(Files.localInput(file))
            .project(expected)
            .createBatchedReaderFunc(
                type -> NestedParquetReaders.buildReader(expected, type, Map.of()))
            .build()) {
      ColumnarBatch batch = batches.iterator().next();
      assertThat(batch.numRows()).isEqualTo(2);
      assertThat(batch.column(0).isNullAt(0)).isTrue();
      assertThat(batch.column(0).isNullAt(1)).isFalse();
      assertThat(batch.column(0).getChild(0).isNullAt(1)).isTrue();
    }
  }

  @Test
  @Override
  public void testUnknownListType() {
    assertThatThrownBy(super::testUnknownListType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert element Parquet: unknown");
  }

  @Test
  @Override
  public void testUnknownMapType() {
    assertThatThrownBy(super::testUnknownMapType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert value Parquet: unknown");
  }
}
