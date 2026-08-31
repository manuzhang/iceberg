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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/** Parser for custom file format metadata files. */
public final class CustomFileFormatParser {
  private static final String FORMAT = "format";
  private static final String METADATA_LOCATION = "metadata-location";

  private CustomFileFormatParser() {}

  public static String toJson(CustomFileFormat format) {
    Preconditions.checkArgument(format != null, "Invalid custom file format: null");
    return JsonUtil.generate(generator -> toJson(format, generator), false);
  }

  public static CustomFileFormat fromJson(String json) {
    Preconditions.checkArgument(json != null, "Invalid custom file format JSON: null");
    return JsonUtil.parse(json, CustomFileFormatParser::fromJson);
  }

  public static void write(CustomFileFormat format, OutputFile outputFile) throws IOException {
    Preconditions.checkArgument(outputFile != null, "Invalid output file: null");
    byte[] bytes = toJson(format).getBytes(StandardCharsets.UTF_8);
    try (PositionOutputStream output = outputFile.createOrOverwrite()) {
      output.write(bytes);
    }
  }

  public static CustomFileFormat read(InputFile inputFile) throws IOException {
    Preconditions.checkArgument(inputFile != null, "Invalid input file: null");
    long length = inputFile.getLength();
    Preconditions.checkArgument(
        length <= Integer.MAX_VALUE, "Custom file format metadata is too large: %s", length);
    byte[] bytes = new byte[(int) length];
    IOUtil.readFully(inputFile, 0L, bytes, 0, bytes.length);
    return fromJson(new String(bytes, StandardCharsets.UTF_8));
  }

  private static void toJson(CustomFileFormat format, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(FORMAT, format.name());
    generator.writeStringField(METADATA_LOCATION, format.metadataLocation());
    generator.writeEndObject();
  }

  private static CustomFileFormat fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && json.isObject(), "Invalid custom file format JSON: %s", json);
    return CustomFileFormat.of(
        JsonUtil.getString(FORMAT, json), JsonUtil.getString(METADATA_LOCATION, json));
  }
}
