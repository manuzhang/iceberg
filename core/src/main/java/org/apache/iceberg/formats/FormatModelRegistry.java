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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry that manages file-format-specific readers and writers through a unified object model
 * factory interface.
 *
 * <p>This registry provides access to {@link ReadBuilder}s for data consumption and {@link
 * FileWriterBuilder}s for writing various types of Iceberg content files. The appropriate builder
 * is selected based on {@link FileFormat} and object model class.
 *
 * <p>{@link FormatModel} objects are registered through {@link #register(FormatModel)} and used for
 * creating readers and writers.
 */
public final class FormatModelRegistry {
  private FormatModelRegistry() {}

  private static final Logger LOG = LoggerFactory.getLogger(FormatModelRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.data.GenericFormatModels",
          "org.apache.iceberg.arrow.vectorized.ArrowFormatModels",
          "org.apache.iceberg.flink.data.FlinkFormatModels",
          "org.apache.iceberg.spark.source.SparkFormatModels");

  // Format models indexed by file format and object model class
  private static final Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> MODELS =
      Maps.newConcurrentMap();
  private static final Map<Pair<String, Class<?>>, CustomFormatModel<?, ?>> CUSTOM_MODELS =
      Maps.newConcurrentMap();

  static {
    registerSupportedFormats();
    registerProviders();
  }

  /**
   * Registers an {@link FormatModel} in this registry.
   *
   * <p>The {@link FormatModel} creates readers and writers for a specific combinations of file
   * format (Parquet, ORC, Avro) and object model (for example: "generic", "spark", "flink", etc.).
   * Registering custom factories allows integration of new data processing engines for the
   * supported file formats with Iceberg's file access mechanisms.
   *
   * <p>Each factory must be uniquely identified by its combination of file format and object model
   * name. This uniqueness constraint prevents ambiguity when selecting factories for read and write
   * operations.
   *
   * @param formatModel the factory implementation to register
   * @throws IllegalArgumentException if a factory is already registered for the combination of
   *     {@link FormatModel#format()} and {@link FormatModel#type()}
   */
  public static synchronized void register(FormatModel<?, ?> formatModel) {
    if (formatModel instanceof CustomFormatModel) {
      registerCustom((CustomFormatModel<?, ?>) formatModel);
      return;
    }

    Pair<FileFormat, Class<?>> key = Pair.of(formatModel.format(), formatModel.type());

    FormatModel<?, ?> existing = MODELS.get(key);
    Preconditions.checkArgument(
        existing == null,
        "Cannot register %s: %s is registered for format=%s type=%s schemaType=%s",
        formatModel.getClass(),
        existing == null ? null : existing.getClass(),
        key.first(),
        key.second(),
        existing == null ? null : existing.schemaType());

    MODELS.put(key, formatModel);
  }

  private static void registerCustom(CustomFormatModel<?, ?> formatModel) {
    String formatName = CustomFileFormat.normalize(formatModel.customFormatName());
    Pair<String, Class<?>> key = Pair.of(formatName, formatModel.type());
    CustomFormatModel<?, ?> existing = CUSTOM_MODELS.get(key);
    Preconditions.checkArgument(
        existing == null,
        "Cannot register %s: %s is registered for custom format=%s type=%s schemaType=%s",
        formatModel.getClass(),
        existing == null ? null : existing.getClass(),
        key.first(),
        key.second(),
        existing == null ? null : existing.schemaType());

    CUSTOM_MODELS.put(key, formatModel);
  }

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param type the output type
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configured reader builder for the specified format and object model
   */
  public static <D, S> ReadBuilder<D, S> readBuilder(
      FileFormat format, Class<? extends D> type, InputFile inputFile) {
    FormatModel<D, S> model = modelFor(format, type);
    return model.readBuilder(inputFile);
  }

  /**
   * Returns a reader builder for the specified file format and object model, resolving custom file
   * format metadata using the provided {@link FileIO}.
   *
   * @param format the file format that determines the parsing implementation
   * @param type the output type
   * @param inputFile source file to read data or custom format metadata from
   * @param io file IO used to resolve custom format metadata locations
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configured reader builder for the specified format and object model
   */
  public static <D, S> ReadBuilder<D, S> readBuilder(
      FileFormat format, Class<? extends D> type, InputFile inputFile, FileIO io) {
    if (format != FileFormat.CUSTOM) {
      return readBuilder(format, type, inputFile);
    }

    try {
      CustomFileFormat customFormat = CustomFileFormatParser.read(inputFile);
      ReadBuilder<D, S> builder =
          customReadBuilder(
              customFormat.name(), type, io.newInputFile(customFormat.metadataLocation()));
      return new CustomReadBuilder<>(builder);
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to read custom file format metadata: %s", inputFile.location());
    }
  }

  /**
   * Returns a reader builder for the specified custom file format and object model.
   *
   * @param customFormatName the custom file format name
   * @param type the output type
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configured reader builder for the specified custom format and object model
   */
  public static <D, S> ReadBuilder<D, S> customReadBuilder(
      String customFormatName, Class<? extends D> type, InputFile inputFile) {
    CustomFormatModel<D, S> model = customModelFor(customFormatName, type);
    return model.readBuilder(inputFile);
  }

  /**
   * Returns a writer builder for generating a {@link DataFile}.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the provided file format. Unlike basic writers, this writer
   * collects file metadata during the writing process and generates a {@link DataFile} that can be
   * used for table operations.
   *
   * @param format the file format used for writing
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @param <S> the type of the input schema for the writer
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public static <D, S> FileWriterBuilder<DataWriter<D>, S> dataWriteBuilder(
      FileFormat format, Class<? extends D> type, EncryptedOutputFile outputFile) {
    FormatModel<D, S> model = modelFor(format, type);
    return FileWriterBuilderImpl.forDataFile(model, outputFile);
  }

  /**
   * Returns a writer builder for generating a data file in a custom file format.
   *
   * @param customFormatName the custom file format name
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @param <S> the type of the input schema for the writer
   * @return a configured data write builder for the specified custom format and object model
   */
  public static <D, S> FileWriterBuilder<DataWriter<D>, S> customDataWriteBuilder(
      String customFormatName, Class<? extends D> type, EncryptedOutputFile outputFile) {
    CustomFormatModel<D, S> model = customModelFor(customFormatName, type);
    return FileWriterBuilderImpl.forDataFile(model, outputFile);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with equality deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer persists equality delete
   * records that identify rows to be deleted based on the configured equality fields, producing a
   * {@link DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @param <S> the type of the input schema for the writer
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public static <D, S> FileWriterBuilder<EqualityDeleteWriter<D>, S> equalityDeleteWriteBuilder(
      FileFormat format, Class<D> type, EncryptedOutputFile outputFile) {
    FormatModel<D, S> model = modelFor(format, type);
    return FileWriterBuilderImpl.forEqualityDelete(model, outputFile);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with position-based deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer accepts {@link PositionDelete}
   * records that identify rows to be deleted by file path and position, producing a {@link
   * DeleteFile} that can be used for table operations.
   *
   * <p><b>Note:</b> This method is only applicable to format-version 2 tables. Format-version 3
   * tables use deletion vectors, which are always written in Puffin format. Registered {@link
   * FormatModel} implementations for {@link PositionDelete} are not consulted for format-version 3+
   * tables.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public static <D> FileWriterBuilder<PositionDeleteWriter<D>, ?> positionDeleteWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    FormatModel<PositionDelete<D>, ?> model =
        FormatModelRegistry.modelFor(format, PositionDelete.deleteClass());
    return FileWriterBuilderImpl.forPositionDelete(model, outputFile);
  }

  @VisibleForTesting
  static Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> models() {
    return MODELS;
  }

  @VisibleForTesting
  static Map<Pair<String, Class<?>>, CustomFormatModel<?, ?>> customModels() {
    return CUSTOM_MODELS;
  }

  @SuppressWarnings("unchecked")
  private static <D, S> FormatModel<D, S> modelFor(FileFormat format, Class<? extends D> type) {
    FormatModel<D, S> model = (FormatModel<D, S>) MODELS.get(Pair.of(format, type));
    Preconditions.checkArgument(
        model != null, "Format model is not registered for format %s and type %s", format, type);
    return model;
  }

  @SuppressWarnings("unchecked")
  private static <D, S> CustomFormatModel<D, S> customModelFor(
      String customFormatName, Class<? extends D> type) {
    String formatName = CustomFileFormat.normalize(customFormatName);
    CustomFormatModel<D, S> model =
        (CustomFormatModel<D, S>) CUSTOM_MODELS.get(Pair.of(formatName, type));
    Preconditions.checkArgument(
        model != null,
        "Format model is not registered for custom format %s and type %s",
        formatName,
        type);
    return model;
  }

  private static class CustomReadBuilder<D, S> implements ReadBuilder<D, S> {
    private final ReadBuilder<D, S> delegate;

    private CustomReadBuilder(ReadBuilder<D, S> delegate) {
      this.delegate = delegate;
    }

    @Override
    public ReadBuilder<D, S> split(long start, long length) {
      // Scan task ranges describe the custom metadata file, not the referenced data file.
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

  private static void registerSupportedFormats() {
    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      register(classToRegister);
    }
  }

  private static void registerProviders() {
    ServiceLoader.load(FileFormatProvider.class)
        .forEach(provider -> provider.formatModels().forEach(FormatModelRegistry::register));
  }

  /**
   * Invokes the static {@code register} method of the given class, tolerating the failure modes
   * that occur when an optional module (like {@code iceberg-parquet} or {@code iceberg-orc}) is not
   * on the classpath.
   *
   * <p>Besides {@link NoSuchMethodException}, invoking {@code register} can fail with {@link
   * NoClassDefFoundError} when its body references a missing transitive dependency, or {@link
   * ExceptionInInitializerError} when it triggers a failing static initializer.
   */
  @SuppressWarnings("CatchBlockLogException")
  private static void register(String classToRegister) {
    try {
      DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
    } catch (NoSuchMethodException | NoClassDefFoundError | ExceptionInInitializerError e) {
      // failing to register a factory is normal and does not require a stack trace
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      LOG.info(
          "Unable to call register for ({}). Check for missing jars on the classpath: {}",
          classToRegister,
          cause.toString());
    }
  }
}
