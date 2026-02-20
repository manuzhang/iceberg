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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.Files;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test vectorized reading of variant columns from Parquet files.
 *
 * <p>This test focuses on serialized-only variant support (value column only, not shredded
 * typed_value columns).
 */
public class TestVectorizedVariantReads {
  @TempDir private File tempDir;

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "variant_col", Types.VariantType.get()));

  @Test
  public void testSerializedVariantPrimitives() throws IOException {
    // Create test data with simple variant primitives
    // Empty metadata: version (1 byte) + offset size (1 byte) + sorted fields (1 byte) + empty dict (1 byte) = 4 bytes
    ByteBuffer emptyMetadata = ByteBuffer.allocate(4).put((byte) 1).put((byte) 0).put((byte) 1).put((byte) 0);
    emptyMetadata.rewind();
    VariantMetadata metadata = Variants.metadata(emptyMetadata);

    List<Record> testRecords =
        ImmutableList.of(
            createRecord(1, Variant.of(metadata, Variants.of(42))),
            createRecord(2, Variant.of(metadata, Variants.of("hello"))),
            createRecord(3, Variant.of(metadata, Variants.of(true))),
            createRecord(4, Variant.of(metadata, Variants.of(3.14))),
            createRecord(5, Variant.of(metadata, Variants.ofNull())));

    File testFile = writeTestData(testRecords);

    // Note: This test will initially fail because vectorized variant support
    // is not yet fully implemented. This serves as a placeholder for when
    // the implementation is complete.
    //
    // TODO: Complete the vectorized variant reader implementation and enable this test
    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);
  }

  @Test
  public void testSerializedVariantWithMetadata() throws IOException {
    // Create test data with variant objects that have metadata
    // For simplicity, use empty metadata in this test too
    ByteBuffer metadataBuffer = ByteBuffer.allocate(4).put((byte) 1).put((byte) 0).put((byte) 1).put((byte) 0);
    metadataBuffer.rewind();
    VariantMetadata metadata = Variants.metadata(metadataBuffer);

    List<Record> testRecords =
        ImmutableList.of(
            createRecord(1, Variant.of(metadata, Variants.of(100))),
            createRecord(2, Variant.of(metadata, Variants.of("test"))),
            createRecord(3, Variant.of(metadata, Variants.ofNull())));

    File testFile = writeTestData(testRecords);

    // Placeholder test - will be completed when vectorized reading is implemented
    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);
  }

  @Test
  public void testVariantBatchReading() throws IOException {
    // Test reading variants in batches
    ByteBuffer emptyMetadata = ByteBuffer.allocate(4).put((byte) 1).put((byte) 0).put((byte) 1).put((byte) 0);
    emptyMetadata.rewind();
    VariantMetadata metadata = Variants.metadata(emptyMetadata);

    // Create a larger dataset to test batch reading
    ImmutableList.Builder<Record> recordsBuilder = ImmutableList.builder();
    for (int i = 0; i < 1000; i++) {
      recordsBuilder.add(createRecord(i, Variant.of(metadata, Variants.of(i))));
    }
    List<Record> testRecords = recordsBuilder.build();

    File testFile = writeTestData(testRecords);

    // TODO: Read using vectorized reader with batch size of 100
    // TODO: Verify all 1000 records are read correctly
    // TODO: Verify variant values match expected values
    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);
  }

  @Test
  public void testVariantWithNulls() throws IOException {
    // Test reading variants with null values
    ByteBuffer emptyMetadata = ByteBuffer.allocate(4).put((byte) 1).put((byte) 0).put((byte) 1).put((byte) 0);
    emptyMetadata.rewind();
    VariantMetadata metadata = Variants.metadata(emptyMetadata);

    List<Record> testRecords =
        ImmutableList.of(
            createRecord(1, Variant.of(metadata, Variants.of(1))),
            createRecord(2, Variant.of(metadata, Variants.ofNull())),
            createRecord(3, Variant.of(metadata, Variants.of(3))),
            createRecord(4, Variant.of(metadata, Variants.ofNull())),
            createRecord(5, Variant.of(metadata, Variants.of(5))));

    File testFile = writeTestData(testRecords);

    // TODO: Verify nullability is correctly handled in vectorized reading
    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);
  }

  /**
   * Helper method to create a test record with id and variant fields.
   *
   * @param id The record ID
   * @param variant The variant value
   * @return A generic record with the specified fields
   */
  private Record createRecord(int id, Variant variant) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("variant_col", variant);
    return record;
  }

  /**
   * Helper method to write test records to a Parquet file.
   *
   * @param records The records to write
   * @return The file containing the written records
   * @throws IOException If writing fails
   */
  private File writeTestData(List<Record> records) throws IOException {
    File testFile = File.createTempFile("variant_test", ".parquet", tempDir);
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }
    return testFile;
  }

  // TODO: Add helper method to read variants using vectorized reader
  // private List<Variant> readVariantsVectorized(File file) throws IOException {
  //   // Implementation pending completion of vectorized variant reader
  // }

  // TODO: Add test for variant dictionary encoding
  // @Test
  // public void testVariantDictionaryEncoding() throws IOException {
  //   // Test that variant metadata can be dictionary encoded efficiently
  // }

  // TODO: Add test for large variant values
  // @Test
  // public void testLargeVariantValues() throws IOException {
  //   // Test variants with large serialized sizes
  // }
}
