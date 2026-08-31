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
package org.apache.iceberg.puffin.formats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestPuffinFormatBridge {
  @TempDir private Path temp;

  @Test
  void roundTrip() throws IOException {
    OutputFile output = Files.localOutput(temp.resolve("custom.puffin").toFile());
    CustomFileFormat expected = CustomFileFormat.of("VORTEX", "file:/data/file.vortex");

    PuffinFormatBridge.write(expected, output);

    assertThat(PuffinFormatBridge.read(output.toInputFile())).isEqualTo(expected);
    assertThat(FileFormat.fromFileName(output.location())).isEqualTo(FileFormat.PUFFIN);
    try (PuffinReader reader = Puffin.read(output.toInputFile()).build()) {
      assertThat(reader.fileMetadata().properties())
          .containsEntry(PuffinFormatBridge.FORMAT_PROPERTY, "vortex")
          .containsEntry(PuffinFormatBridge.METADATA_LOCATION_PROPERTY, "file:/data/file.vortex");
    }
  }

  @Test
  void rejectsInvalidName() {
    assertThatThrownBy(() -> CustomFileFormat.of("not a format", "file:/data/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid custom file format name");
  }
}
