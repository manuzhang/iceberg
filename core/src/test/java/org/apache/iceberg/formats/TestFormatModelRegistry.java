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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class TestFormatModelRegistry {
  @TempDir private Path temp;

  @BeforeEach
  void clearRegistry() {
    FormatModelRegistry.models().clear();
    FormatModelRegistry.customModels().clear();
  }

  @Test
  void testSuccessfulRegister() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);
  }

  /** Tests that registering the same class with the same configuration updates the registration. */
  @Test
  void testRegistrationForDifferentType() {
    FormatModel<?, ?> model1 = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModel<?, ?> model2 = new DummyParquetFormatModel(Long.class, Object.class);
    FormatModelRegistry.register(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);

    // Registering a new model with the different format will succeed
    FormatModelRegistry.register(model2);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model2.type())))
        .isSameAs(model2);
  }

  /**
   * Tests that registering different classes, or different schema type for the same file format and
   * type is failing.
   */
  @Test
  void testFailingReRegistrations() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);

    // Registering a new model with different schema type should fail
    assertThatThrownBy(
            () ->
                FormatModelRegistry.register(
                    new DummyParquetFormatModel(Object.class, String.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");

    // Registering a new model with null schema type should fail
    assertThatThrownBy(
            () -> FormatModelRegistry.register(new DummyParquetFormatModel(Object.class, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");
  }

  @Test
  void registerCustomFormatModel() {
    CustomFormatModel<?, ?> model = new DummyCustomFormatModel("VORTEX", Object.class);

    FormatModelRegistry.register(model);

    assertThat(FormatModelRegistry.models()).isEmpty();
    assertThat(FormatModelRegistry.customModels())
        .containsEntry(Pair.of("vortex", Object.class), model);
  }

  @Test
  void rejectDuplicateCustomFormatModel() {
    FormatModelRegistry.register(new DummyCustomFormatModel("vortex", Object.class));

    assertThatThrownBy(
            () -> FormatModelRegistry.register(new DummyCustomFormatModel("VORTEX", Object.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");
  }

  @Test
  @SuppressWarnings("unchecked")
  void resolvesCustomReadBuilder() throws IOException {
    ReadBuilder<Object, Object> delegate = Mockito.mock(ReadBuilder.class);
    DummyCustomFormatModel model = new DummyCustomFormatModel("vortex", Object.class, delegate);
    FormatModelRegistry.register(model);
    String dataLocation = temp.resolve("data.vortex").toString();
    OutputFile descriptorFile = Files.localOutput(temp.resolve("data.custom").toFile());
    CustomFileFormatParser.write(CustomFileFormat.of("vortex", dataLocation), descriptorFile);

    ReadBuilder<Object, Object> builder =
        FormatModelRegistry.readBuilder(
            FileFormat.CUSTOM,
            Object.class,
            descriptorFile.toInputFile(),
            new TestTables.LocalFileIO());
    builder.split(10L, 20L).project(null);

    assertThat(model.inputFile().location()).isEqualTo(dataLocation);
    Mockito.verify(delegate, Mockito.never()).split(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(delegate).project(null);
  }

  @Test
  void registerToleratesMissingClass() {
    assertThatNoException().isThrownBy(() -> register("org.apache.iceberg.formats.DoesNotExist"));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  @Test
  void registerToleratesNoClassDefFoundErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsOnInvoke.class.getName()));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  @Test
  void registerToleratesExceptionInInitializerErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsInitErrorOnInvoke.class.getName()));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  private static void register(String className) throws ReflectiveOperationException {
    Method register = FormatModelRegistry.class.getDeclaredMethod("register", String.class);
    register.setAccessible(true);
    register.invoke(null, className);
  }

  public static class ThrowsOnInvoke {
    public static void register() {
      throw new NoClassDefFoundError("some/missing/TransitiveDependency");
    }
  }

  public static class ThrowsInitErrorOnInvoke {
    public static void register() {
      throw new ExceptionInInitializerError(new RuntimeException("lazy init failed"));
    }
  }

  private static class DummyParquetFormatModel implements FormatModel<Object, Object> {
    private final Class<?> type;
    private final Class<?> schemaType;

    private DummyParquetFormatModel(Class<?> type, Class<?> schemaType) {
      this.type = type;
      this.schemaType = schemaType;
    }

    @Override
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> type() {
      return (Class<Object>) type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> schemaType() {
      return (Class<Object>) schemaType;
    }

    @Override
    public ModelWriteBuilder<Object, Object> writeBuilder(EncryptedOutputFile outputFile) {
      return null;
    }

    @Override
    public ReadBuilder<Object, Object> readBuilder(InputFile inputFile) {
      return null;
    }
  }

  private static class DummyCustomFormatModel implements CustomFormatModel<Object, Object> {
    private final String customFormatName;
    private final Class<?> type;
    private final ReadBuilder<Object, Object> readBuilder;
    private InputFile inputFile;

    private DummyCustomFormatModel(String customFormatName, Class<?> type) {
      this(customFormatName, type, null);
    }

    private DummyCustomFormatModel(
        String customFormatName, Class<?> type, ReadBuilder<Object, Object> readBuilder) {
      this.customFormatName = customFormatName;
      this.type = type;
      this.readBuilder = readBuilder;
    }

    private InputFile inputFile() {
      return inputFile;
    }

    @Override
    public String customFormatName() {
      return customFormatName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> type() {
      return (Class<Object>) type;
    }

    @Override
    public Class<Object> schemaType() {
      return Object.class;
    }

    @Override
    public ModelWriteBuilder<Object, Object> writeBuilder(EncryptedOutputFile outputFile) {
      return null;
    }

    @Override
    public ReadBuilder<Object, Object> readBuilder(InputFile inputFile) {
      this.inputFile = inputFile;
      return readBuilder;
    }
  }
}
