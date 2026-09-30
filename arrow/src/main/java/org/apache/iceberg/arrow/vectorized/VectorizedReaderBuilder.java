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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.ConstantVectorReader;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader<?>> {
  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final BufferAllocator rootAllocator;
  private final Map<Integer, ?> idToConstant;
  private final boolean setArrowValidityVector;
  private final Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory;
  private final BiFunction<org.apache.iceberg.types.Type, Object, Object> convert;
  private MessageType projection = null;

  public VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory) {
    this(
        expectedSchema,
        parquetSchema,
        setArrowValidityVector,
        idToConstant,
        readerFactory,
        (type, value) -> value);
  }

  protected VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      BiFunction<org.apache.iceberg.types.Type, Object, Object> convert) {
    this(
        expectedSchema,
        parquetSchema,
        setArrowValidityVector,
        idToConstant,
        readerFactory,
        convert,
        ArrowAllocation.rootAllocator());
  }

  protected VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      BiFunction<org.apache.iceberg.types.Type, Object, Object> convert,
      BufferAllocator bufferAllocator) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.rootAllocator =
        bufferAllocator.newChildAllocator("VectorizedReadBuilder", 0, Long.MAX_VALUE);
    this.setArrowValidityVector = setArrowValidityVector;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
    this.convert = convert;
  }

  /**
   * Returns whether this builder creates readers for struct, list, and map fields.
   *
   * <p>Those readers return {@link VectorHolder.StructVectorHolder}, {@link
   * VectorHolder.ListVectorHolder}, and {@link VectorHolder.MapVectorHolder}, so a subclass should
   * return true only if the batch reader it creates handles them.
   *
   * @return true if struct, list, and map fields are read, false otherwise
   */
  protected boolean supportsNestedTypes() {
    return false;
  }

  @Override
  public VectorizedReader<?> message(
      Types.StructType expected, MessageType message, List<VectorizedReader<?>> fieldReaders) {
    List<Types.NestedField> icebergFields =
        expected != null ? expected.fields() : ImmutableList.of();
    return vectorizedReader(fieldReaders(icebergFields, message, fieldReaders));
  }

  private List<VectorizedReader<?>> fieldReaders(
      List<Types.NestedField> icebergFields,
      GroupType groupType,
      List<VectorizedReader<?>> fieldReaders) {
    Map<Integer, VectorizedReader<?>> readersById = readersById(groupType, fieldReaders);
    List<VectorizedReader<?>> reorderedFields =
        Lists.newArrayListWithExpectedSize(icebergFields.size());

    for (Types.NestedField field : icebergFields) {
      VectorizedReader<?> reader =
          VectorizedArrowReader.replaceWithMetadataReader(
              field, readersById.get(field.fieldId()), idToConstant, setArrowValidityVector);
      reorderedFields.add(defaultReader(field, reader));
    }

    return reorderedFields;
  }

  private static Map<Integer, VectorizedReader<?>> readersById(
      GroupType groupType, List<VectorizedReader<?>> fieldReaders) {
    Map<Integer, VectorizedReader<?>> readersById = Maps.newHashMap();
    List<Type> fields = groupType.getFields();

    IntStream.range(0, fields.size())
        .filter(pos -> fields.get(pos).getId() != null)
        .forEach(pos -> readersById.put(fields.get(pos).getId().intValue(), fieldReaders.get(pos)));

    return readersById;
  }

  private VectorizedReader<?> defaultReader(Types.NestedField field, VectorizedReader<?> reader) {
    if (reader != null) {
      return reader;
    } else if (field.initialDefault() != null) {
      return constantReader(field, convert.apply(field.type(), field.initialDefault()));
    } else if (field.isOptional()) {
      return VectorizedArrowReader.nulls();
    }

    throw new IllegalArgumentException(String.format("Missing required field: %s", field.name()));
  }

  private <T> ConstantVectorReader<T> constantReader(Types.NestedField field, T constant) {
    return new ConstantVectorReader<>(field, constant);
  }

  protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
    return readerFactory.apply(reorderedFields);
  }

  @Override
  public VectorizedReader<?> struct(
      Types.StructType expected, GroupType groupType, List<VectorizedReader<?>> fieldReaders) {
    if (expected == null) {
      return null;
    }

    if (!supportsNestedTypes()) {
      throw new UnsupportedOperationException(
          "Vectorized reads are not supported yet for struct fields");
    }

    String[] path = currentPath();
    Map<Integer, VectorizedReader<?>> readersById = readersById(groupType, fieldReaders);
    boolean readsField =
        expected.fields().stream().anyMatch(field -> readersById.get(field.fieldId()) != null);
    // like the row reader, a struct whose fields read no column uses the column that pruning kept
    // under it to find whether the struct is present
    ColumnDescriptor presenceColumn =
        readsField || parquetSchema.getMaxDefinitionLevel(path) <= 0 ? null : presenceColumn(path);

    return VectorizedNestedReaders.struct(
        icebergField(groupType),
        arrowReaders(fieldReaders(expected.fields(), groupType, fieldReaders)),
        presenceColumn,
        parquetSchema.getMaxRepetitionLevel(path),
        parquetSchema.getMaxDefinitionLevel(path));
  }

  @Override
  public VectorizedReader<?> list(
      Types.ListType expected, GroupType array, VectorizedReader<?> elementReader) {
    if (expected == null || !supportsNestedTypes()) {
      return super.list(expected, array, elementReader);
    }

    Types.NestedField icebergField = icebergField(array);
    VectorizedArrowReader element = (VectorizedArrowReader) elementReader;
    if (element == null || element.levels() == null) {
      throw new UnsupportedOperationException(
          String.format("Cannot read list %s: no column is read for its elements", icebergField));
    }

    // in a two-level list, the repeated field is the element and is not in the current path
    Type elementType = ParquetSchemaUtil.determineListElementType(array);
    String[] repeatedPath =
        elementType.isRepetition(Type.Repetition.REPEATED)
            ? path(elementType.getName())
            : currentPath();
    return VectorizedNestedReaders.list(
        icebergField,
        element,
        parquetSchema.getMaxRepetitionLevel(repeatedPath) - 1,
        parquetSchema.getMaxDefinitionLevel(repeatedPath) - 1);
  }

  @Override
  public VectorizedReader<?> map(
      Types.MapType expected,
      GroupType map,
      VectorizedReader<?> keyReader,
      VectorizedReader<?> valueReader) {
    if (expected == null || !supportsNestedTypes()) {
      return super.map(expected, map, keyReader, valueReader);
    }

    Types.NestedField icebergField = icebergField(map);
    VectorizedArrowReader key = (VectorizedArrowReader) keyReader;
    if (key == null || key.levels() == null) {
      throw new UnsupportedOperationException(
          String.format("Cannot read map %s: no column is read for its keys", icebergField));
    }

    String[] repeatedPath = currentPath();
    return VectorizedNestedReaders.map(
        icebergField,
        key,
        (VectorizedArrowReader) defaultReader(expected.fields().get(1), valueReader),
        parquetSchema.getMaxRepetitionLevel(repeatedPath) - 1,
        parquetSchema.getMaxDefinitionLevel(repeatedPath) - 1);
  }

  private Types.NestedField icebergField(GroupType groupType) {
    return icebergSchema.findField(groupType.getId().intValue());
  }

  private static List<VectorizedArrowReader> arrowReaders(List<VectorizedReader<?>> readers) {
    List<VectorizedArrowReader> arrowReaders = Lists.newArrayListWithExpectedSize(readers.size());
    for (VectorizedReader<?> reader : readers) {
      arrowReaders.add((VectorizedArrowReader) reader);
    }

    return arrowReaders;
  }

  /** Returns the column that pruning kept under a struct whose fields read no column. */
  private ColumnDescriptor presenceColumn(String[] structPath) {
    if (projection == null) {
      this.projection = ParquetSchemaUtil.pruneColumns(parquetSchema, icebergSchema);
    }

    return projection.getColumns().stream()
        .filter(column -> isUnder(column.getPath(), structPath))
        .min(Comparator.comparingInt(ColumnDescriptor::getMaxRepetitionLevel))
        .map(column -> parquetSchema.getColumnDescription(column.getPath()))
        .orElse(null);
  }

  private static boolean isUnder(String[] path, String[] parentPath) {
    return path.length > parentPath.length
        && Arrays.equals(path, 0, parentPath.length, parentPath, 0, parentPath.length);
  }

  @Override
  public ParquetVariantVisitor<VectorizedReader<?>> variantVisitor() {
    return new VectorizedVariantVisitor(
        currentPath(), parquetSchema, icebergSchema, rootAllocator, setArrowValidityVector);
  }

  @Override
  public VectorizedReader<?> variant(
      Types.VariantType iVariant, GroupType variant, VectorizedReader<?> result) {
    if (supportsNestedTypes() && currentPath().length > 1) {
      throw new UnsupportedOperationException(
          "Vectorized reads are not supported yet for variants in structs, lists, or maps");
    }

    return result;
  }

  @Override
  public VectorizedReader<?> primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {

    // Create arrow vector for this field
    if (primitive.getId() == null) {
      return null;
    }
    int parquetFieldId = primitive.getId().intValue();
    String[] path = currentPath();
    ColumnDescriptor desc = parquetSchema.getColumnDescription(path);
    // Nested types are only supported for vectorized reads when the subclass supports them
    boolean readsLevels = supportsNestedTypes() && path.length > 1;
    if (desc.getMaxRepetitionLevel() > 0 && !readsLevels) {
      return null;
    }
    Types.NestedField icebergField = icebergSchema.findField(parquetFieldId);
    if (icebergField == null) {
      return null;
    }
    // Set the validity buffer if null checking is enabled in arrow
    return new VectorizedArrowReader(
        desc, icebergField, rootAllocator, setArrowValidityVector, readsLevels);
  }
}
