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
package org.apache.iceberg.vortex;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.formats.CustomFileFormat;
import org.apache.iceberg.puffin.formats.PuffinFormatBridge;
import org.apache.iceberg.puffin.formats.PuffinFormatRegistry;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVortexFormatRegistry {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void writesAndReadsThroughPuffinBridge() throws IOException {
    FileIO io = new TestTables.LocalFileIO();
    PuffinFormatRegistry.register(io);
    OutputFile bridgeOutput = io.newOutputFile(temp.resolve("records.puffin").toString());
    Record first = GenericRecord.create(SCHEMA).copy("id", 1L, "data", "a");
    Record second = GenericRecord.create(SCHEMA).copy("id", 2L, "data", "b");

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(
                FileFormat.PUFFIN,
                Record.class,
                EncryptedFiles.plainAsEncryptedOutput(bridgeOutput))
            .set(PuffinFormatRegistry.CUSTOM_FORMAT, VortexFormatModel.FORMAT_NAME)
            .schema(SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (writer) {
      writer.write(first);
      writer.write(second);
    }

    DataFile dataFile = writer.toDataFile();
    CustomFileFormat customFormat = PuffinFormatBridge.read(bridgeOutput.toInputFile());

    assertThat(dataFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dataFile.path().toString()).isEqualTo(bridgeOutput.location());
    assertThat(customFormat.name()).isEqualTo(VortexFormatModel.FORMAT_NAME);
    assertThat(customFormat.metadataLocation()).endsWith("records.vortex");
    assertThat(io.newInputFile(customFormat.metadataLocation()).exists()).isTrue();

    Table table =
        TestTables.create(temp.toFile(), "custom-vortex", SCHEMA, PartitionSpec.unpartitioned(), 2);
    table.newAppend().appendFile(dataFile).commit();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
      assertThat(reader).containsExactly(first, second);
    }
  }
}
