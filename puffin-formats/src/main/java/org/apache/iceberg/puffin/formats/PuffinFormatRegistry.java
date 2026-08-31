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
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Registers a Puffin dispatcher for custom data formats without modifying Iceberg core. */
public final class PuffinFormatRegistry {
  public static final String CUSTOM_FORMAT = "write.format.custom";

  private PuffinFormatRegistry() {}

  public static void register(FileIO io) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Map<Class<?>, Map<String, CustomFormatModel<?, ?>>> modelsByType = loadModels();
    Preconditions.checkArgument(!modelsByType.isEmpty(), "No Puffin format providers found");
    for (Map<String, CustomFormatModel<?, ?>> models : modelsByType.values()) {
      registerDispatcher(io, models);
    }
  }

  private static Map<Class<?>, Map<String, CustomFormatModel<?, ?>>> loadModels() {
    Map<Class<?>, Map<String, CustomFormatModel<?, ?>>> modelsByType = new LinkedHashMap<>();
    for (PuffinFormatProvider provider : ServiceLoader.load(PuffinFormatProvider.class)) {
      for (CustomFormatModel<?, ?> model : provider.formatModels()) {
        String name = CustomFileFormat.normalize(model.customFormatName());
        Map<String, CustomFormatModel<?, ?>> models =
            modelsByType.computeIfAbsent(model.type(), ignored -> new LinkedHashMap<>());
        CustomFormatModel<?, ?> existing = models.putIfAbsent(name, model);
        Preconditions.checkArgument(
            existing == null,
            "Cannot register %s: %s is registered for custom format=%s type=%s",
            model.getClass(),
            existing == null ? null : existing.getClass(),
            name,
            model.type());
      }
    }

    return modelsByType;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerDispatcher(FileIO io, Map<String, CustomFormatModel<?, ?>> models) {
    CustomFormatModel<?, ?> first = models.values().iterator().next();
    for (CustomFormatModel<?, ?> model : models.values()) {
      Preconditions.checkArgument(
          first.schemaType().equals(model.schemaType()),
          "Conflicting schema types for type %s: %s and %s",
          first.type(),
          first.schemaType(),
          model.schemaType());
    }

    FormatModelRegistry.register(
        new BridgeFormatModel(io, first.type(), first.schemaType(), models));
  }

  private static class BridgeFormatModel<D, S> implements FormatModel<D, S> {
    private final FileIO io;
    private final Class<? extends D> type;
    private final Class<S> schemaType;
    private final Map<String, CustomFormatModel<D, S>> models;

    @SuppressWarnings("unchecked")
    private BridgeFormatModel(
        FileIO io,
        Class<? extends D> type,
        Class<S> schemaType,
        Map<String, CustomFormatModel<?, ?>> models) {
      this.io = io;
      this.type = type;
      this.schemaType = schemaType;
      this.models = (Map<String, CustomFormatModel<D, S>>) (Map<?, ?>) models;
    }

    @Override
    public FileFormat format() {
      return FileFormat.PUFFIN;
    }

    @Override
    public Class<? extends D> type() {
      return type;
    }

    @Override
    public Class<S> schemaType() {
      return schemaType;
    }

    @Override
    public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
      return new BridgeWriteBuilder<>(io, outputFile, models);
    }

    @Override
    public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
      try {
        CustomFileFormat customFormat = PuffinFormatBridge.read(inputFile);
        CustomFormatModel<D, S> model = modelFor(models, customFormat.name());
        ReadBuilder<D, S> builder =
            model.readBuilder(io.newInputFile(customFormat.metadataLocation()));
        return new BridgeReadBuilder<>(builder);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to read Puffin bridge: %s", inputFile.location());
      }
    }
  }

  private static <D, S> CustomFormatModel<D, S> modelFor(
      Map<String, CustomFormatModel<D, S>> models, String name) {
    String normalized = CustomFileFormat.normalize(name);
    CustomFormatModel<D, S> model = models.get(normalized);
    Preconditions.checkArgument(model != null, "Custom format is not registered: %s", normalized);
    return model;
  }

  private static class BridgeReadBuilder<D, S> implements ReadBuilder<D, S> {
    private final ReadBuilder<D, S> delegate;

    private BridgeReadBuilder(ReadBuilder<D, S> delegate) {
      this.delegate = delegate;
    }

    @Override
    public ReadBuilder<D, S> split(long start, long length) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema schema) {
      delegate.project(schema);
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S schema) {
      delegate.engineProjection(schema);
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      delegate.caseSensitive(caseSensitive);
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      delegate.filter(filter);
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      delegate.set(key, value);
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      delegate.reuseContainers();
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int rowsPerBatch) {
      delegate.recordsPerBatch(rowsPerBatch);
      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> idToConstant) {
      delegate.idToConstant(idToConstant);
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      delegate.withNameMapping(nameMapping);
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      return delegate.build();
    }
  }

  private static class BridgeWriteBuilder<D, S> implements ModelWriteBuilder<D, S> {
    private final FileIO io;
    private final EncryptedOutputFile bridgeFile;
    private final Map<String, CustomFormatModel<D, S>> models;
    private final Map<String, String> properties = Maps.newHashMap();
    private final Map<String, String> metadata = Maps.newHashMap();
    private Schema schema;
    private S engineSchema;
    private FileContent content;
    private MetricsConfig metricsConfig;
    private boolean overwrite;
    private ByteBuffer encryptionKey;
    private ByteBuffer aadPrefix;

    private BridgeWriteBuilder(
        FileIO io, EncryptedOutputFile bridgeFile, Map<String, CustomFormatModel<D, S>> models) {
      this.io = io;
      this.bridgeFile = bridgeFile;
      this.models = models;
    }

    @Override
    public ModelWriteBuilder<D, S> schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> engineSchema(S newSchema) {
      this.engineSchema = newSchema;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> set(String property, String value) {
      properties.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> content(FileContent newContent) {
      this.content = newContent;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> overwrite() {
      this.overwrite = true;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withFileEncryptionKey(ByteBuffer newEncryptionKey) {
      this.encryptionKey = newEncryptionKey;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer newAADPrefix) {
      this.aadPrefix = newAADPrefix;
      return this;
    }

    @Override
    public FileAppender<D> build() throws IOException {
      Preconditions.checkArgument(
          content == FileContent.DATA,
          "Puffin custom formats support only data files: %s",
          content);
      String customFormatName = properties.get(CUSTOM_FORMAT);
      Preconditions.checkArgument(
          customFormatName != null, "Custom format is not configured: %s", CUSTOM_FORMAT);
      CustomFormatModel<D, S> model = modelFor(models, customFormatName);
      String targetLocation = targetLocation(bridgeFile.encryptingOutputFile(), model);
      ModelWriteBuilder<D, S> delegate =
          model.writeBuilder(
              EncryptedFiles.plainAsEncryptedOutput(io.newOutputFile(targetLocation)));
      delegate.content(content);
      if (schema != null) {
        delegate.schema(schema);
      }

      if (engineSchema != null) {
        delegate.engineSchema(engineSchema);
      }

      properties.forEach(
          (key, value) -> {
            if (!CUSTOM_FORMAT.equals(key)) {
              delegate.set(key, value);
            }
          });
      metadata.forEach(delegate::meta);
      if (metricsConfig != null) {
        delegate.metricsConfig(metricsConfig);
      }

      if (overwrite) {
        delegate.overwrite();
      }

      if (encryptionKey != null) {
        delegate.withFileEncryptionKey(encryptionKey);
      }

      if (aadPrefix != null) {
        delegate.withAADPrefix(aadPrefix);
      }

      return new BridgeFileAppender<>(
          delegate.build(),
          CustomFileFormat.of(model.customFormatName(), targetLocation),
          bridgeFile.encryptingOutputFile());
    }

    private static String targetLocation(OutputFile bridgeOutput, CustomFormatModel<?, ?> model) {
      String bridgeLocation = bridgeOutput.location();
      String suffix = ".puffin";
      if (bridgeLocation.toLowerCase(Locale.ROOT).endsWith(suffix)) {
        return bridgeLocation.substring(0, bridgeLocation.length() - suffix.length())
            + "."
            + CustomFileFormat.normalize(model.customFormatName());
      }

      return bridgeLocation + "." + CustomFileFormat.normalize(model.customFormatName());
    }
  }

  private static class BridgeFileAppender<D> implements FileAppender<D> {
    private final FileAppender<D> delegate;
    private final CustomFileFormat customFormat;
    private final OutputFile bridgeOutput;
    private boolean closed;

    private BridgeFileAppender(
        FileAppender<D> delegate, CustomFileFormat customFormat, OutputFile bridgeOutput) {
      this.delegate = delegate;
      this.customFormat = customFormat;
      this.bridgeOutput = bridgeOutput;
    }

    @Override
    public void add(D datum) {
      delegate.add(datum);
    }

    @Override
    public Metrics metrics() {
      return delegate.metrics();
    }

    @Override
    public long length() {
      return closed ? bridgeOutput.toInputFile().getLength() : delegate.length();
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        PuffinFormatBridge.write(customFormat, bridgeOutput);
        this.closed = true;
      }
    }
  }
}
