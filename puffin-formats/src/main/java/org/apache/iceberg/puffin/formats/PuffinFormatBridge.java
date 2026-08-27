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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Reads and writes Puffin bridge files for custom data formats. */
public final class PuffinFormatBridge {
  static final String FORMAT_PROPERTY = "iceberg.custom-format";
  static final String METADATA_LOCATION_PROPERTY = "iceberg.custom-format.metadata-location";

  private PuffinFormatBridge() {}

  public static void write(CustomFileFormat format, OutputFile outputFile) throws IOException {
    Preconditions.checkArgument(format != null, "Invalid custom file format: null");
    Preconditions.checkArgument(outputFile != null, "Invalid output file: null");
    try (PuffinWriter writer =
        Puffin.write(outputFile)
            .createdBy(IcebergBuild.fullVersion())
            .set(FORMAT_PROPERTY, format.name())
            .set(METADATA_LOCATION_PROPERTY, format.metadataLocation())
            .build()) {
      writer.finish();
    }
  }

  public static CustomFileFormat read(InputFile inputFile) throws IOException {
    Preconditions.checkArgument(inputFile != null, "Invalid input file: null");
    Map<String, String> properties;
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      properties = reader.fileMetadata().properties();
    }

    String formatName = properties.get(FORMAT_PROPERTY);
    Preconditions.checkArgument(
        formatName != null, "Missing Puffin custom file format property: %s", FORMAT_PROPERTY);
    String metadataLocation = properties.get(METADATA_LOCATION_PROPERTY);
    Preconditions.checkArgument(
        metadataLocation != null,
        "Missing Puffin custom file format property: %s",
        METADATA_LOCATION_PROPERTY);
    return CustomFileFormat.of(formatName, metadataLocation);
  }
}
