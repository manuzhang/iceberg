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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBaseDVFileWriter {

  @TempDir private Path temp;

  @Test
  public void testUsesNativeEncryptionKeyMetadataWithFileSize() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile outputFile = Files.localOutput(temp.resolve("test.puffin").toString());
    TestNativeEncryptionKeyMetadata keyMetadata = new TestNativeEncryptionKeyMetadata(123L);
    EncryptedOutputFile encryptedOutputFile = new TestEncryptedOutputFile(outputFile, keyMetadata);

    DVFileWriter writer = new BaseDVFileWriter(encryptedOutputFile, path -> null);
    writer.delete("/path/to/data.parquet", 1L, spec, null);
    writer.close();

    DeleteFile deleteFile = Iterables.getOnlyElement(writer.result().deleteFiles());
    long fileSize = deleteFile.fileSizeInBytes();

    assertThat(fileSize).isPositive();
    assertThat(decode(deleteFile.keyMetadata())).isEqualTo(fileSize);
  }

  @Test
  public void testCopiesNonNativeEncryptionKeyMetadata() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile outputFile = Files.localOutput(temp.resolve("non-native.puffin").toString());
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(456L);
    buffer.flip();
    TestEncryptionKeyMetadata keyMetadata = new TestEncryptionKeyMetadata(buffer);
    EncryptedOutputFile encryptedOutputFile = new TestEncryptedOutputFile(outputFile, keyMetadata);

    DVFileWriter writer = new BaseDVFileWriter(encryptedOutputFile, path -> null);
    writer.delete("/path/to/data.parquet", 1L, spec, null);
    writer.close();

    DeleteFile deleteFile = Iterables.getOnlyElement(writer.result().deleteFiles());
    assertThat(decode(deleteFile.keyMetadata())).isEqualTo(456L);
  }

  @Test
  public void testLeavesKeyMetadataNullWhenUnencrypted() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile outputFile = Files.localOutput(temp.resolve("plain.puffin").toString());

    DVFileWriter writer =
        new BaseDVFileWriter(new TestEncryptedOutputFile(outputFile, null), path -> null);
    writer.delete("/path/to/data.parquet", 1L, spec, null);
    writer.close();

    DeleteFile deleteFile = Iterables.getOnlyElement(writer.result().deleteFiles());
    assertThat(deleteFile.keyMetadata()).isNull();
  }

  @Test
  public void testRewrittenDeleteFilesContainPreviousFileScopedDeletes() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile outputFile = Files.localOutput(temp.resolve("rewrite.puffin").toString());
    DeleteFile previousDeleteFile =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withPath("/path/to/previous.puffin")
            .withFileSizeInBytes(10)
            .withReferencedDataFile("/path/to/data.parquet")
            .withContentOffset(0)
            .withContentSizeInBytes(10)
            .withRecordCount(1)
            .build();
    PositionDeleteIndex previousDeletes =
        new BitmapPositionDeleteIndex(ImmutableList.of(previousDeleteFile));
    previousDeletes.delete(0L);

    DVFileWriter writer =
        new BaseDVFileWriter(
            new TestEncryptedOutputFile(outputFile, null),
            path -> path.equals("/path/to/data.parquet") ? previousDeletes : null);
    writer.delete("/path/to/data.parquet", 1L, spec, null);
    writer.close();

    assertThat(writer.result().rewrittenDeleteFiles()).containsExactly(previousDeleteFile);
  }

  private static long decode(ByteBuffer buffer) {
    return buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  private record TestEncryptedOutputFile(OutputFile outputFile, EncryptionKeyMetadata keyMetadata)
      implements EncryptedOutputFile {

    @Override
    public OutputFile encryptingOutputFile() {
      return outputFile;
    }
  }

  private record TestEncryptionKeyMetadata(ByteBuffer buffer) implements EncryptionKeyMetadata {

    @Override
    public ByteBuffer buffer() {
      return buffer.duplicate();
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return new TestEncryptionKeyMetadata(buffer.duplicate());
    }
  }

  private record TestNativeEncryptionKeyMetadata(long encodedValue)
      implements NativeEncryptionKeyMetadata {

    @Override
    public ByteBuffer encryptionKey() {
      return ByteBuffer.allocate(16);
    }

    @Override
    public ByteBuffer aadPrefix() {
      return ByteBuffer.allocate(4);
    }

    @Override
    public Long fileLength() {
      return encodedValue;
    }

    @Override
    public ByteBuffer buffer() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      buffer.putLong(encodedValue);
      buffer.flip();
      return buffer;
    }

    @Override
    public EncryptionKeyMetadata copy() {
      return new TestNativeEncryptionKeyMetadata(encodedValue);
    }

    @Override
    public NativeEncryptionKeyMetadata copyWithLength(long length) {
      return new TestNativeEncryptionKeyMetadata(length);
    }
  }
}
