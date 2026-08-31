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
package org.apache.iceberg.formats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCustomFileFormat {
  @TempDir private Path temp;

  @Test
  void jsonRoundTrip() {
    CustomFileFormat format = CustomFileFormat.of("VORTEX", "file:/data/file.vortex");

    String json = CustomFileFormatParser.toJson(format);

    assertThat(json)
        .isEqualTo("{\"format\":\"vortex\",\"metadata-location\":\"file:/data/file.vortex\"}");
    assertThat(CustomFileFormatParser.fromJson(json)).isEqualTo(format);
  }

  @Test
  void fileRoundTrip() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("data.custom").toFile());
    CustomFileFormat expected = CustomFileFormat.of("vortex", "file:/data/file.vortex");

    CustomFileFormatParser.write(expected, outputFile);

    assertThat(CustomFileFormatParser.read(outputFile.toInputFile())).isEqualTo(expected);
  }

  @Test
  void rejectsInvalidFormatName() {
    assertThatThrownBy(() -> CustomFileFormat.of("not a format", "file:/data/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid custom file format name: not a format");
  }
}
