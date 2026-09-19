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
package org.apache.iceberg.spark.data.vectorized;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.execution.datasources.parquet.IcebergNestedParquetReader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** Batch readers for nested Spark Parquet projections. */
public class NestedParquetReaders {
  private NestedParquetReaders() {}

  /** Returns whether the nested field can be decoded by Spark's Parquet reader. */
  public static boolean supports(Types.NestedField field) {
    return switch (field.type().typeId()) {
      case STRUCT ->
          field.type().asStructType().fields().stream().allMatch(NestedParquetReaders::supports);
      case LIST ->
          field.type().asListType().fields().stream().allMatch(NestedParquetReaders::supports);
      case MAP ->
          field.type().asMapType().fields().stream().allMatch(NestedParquetReaders::supports);
      case BOOLEAN,
              INTEGER,
              LONG,
              FLOAT,
              DOUBLE,
              DATE,
              TIMESTAMP,
              STRING,
              UUID,
              FIXED,
              BINARY,
              DECIMAL,
              UNKNOWN ->
          true;
      default -> false;
    };
  }

  /** Builds a reader that preserves Iceberg metadata and field-ID projection. */
  public static VectorizedReader<ColumnarBatch> buildReader(
      Schema expected, MessageType fileSchema, Map<Integer, ?> constants) {
    boolean hasNested = expected.columns().stream().anyMatch(field -> isNested(field, constants));
    return hasNested
        ? new Reader(expected, fileSchema, constants)
        : VectorizedSparkParquetReaders.buildReader(expected, fileSchema, constants);
  }

  private static boolean isNested(Types.NestedField field, Map<Integer, ?> constants) {
    return field.type().isNestedType()
        && !MetadataColumns.isMetadataColumn(field.fieldId())
        && !constants.containsKey(field.fieldId());
  }

  // The physical type comes from ParquetSchemaUtil.pruneColumns, which also determines the columns
  // loaded for each row group, so every column requested here is available.
  private static Type project(Type physical, Types.NestedField expected) {
    if (expected.type().isStructType()) {
      GroupType group = physical.asGroupType();
      List<Type> fields = Lists.newArrayList();
      // Keep physical order; output order is handled by NestedColumnVector.
      for (Type child : group.getFields()) {
        Types.NestedField selected =
            child.getId() != null
                ? expected.type().asStructType().field(child.getId().intValue())
                : null;
        if (selected != null) {
          fields.add(project(child, selected));
        }
      }
      // When every projected child is missing from the file, pruning keeps only the leaf that
      // shows whether the struct is null, or nothing if the struct can never be null.
      return fields.isEmpty() ? group : group.withNewFields(fields);
    } else if (expected.type().isListType()) {
      GroupType group = physical.asGroupType();
      Type element = ParquetSchemaUtil.determineListElementType(group);
      Type selected = project(element, expected.type().asListType().fields().get(0));
      return element.isRepetition(Type.Repetition.REPEATED)
          ? group.withNewFields(selected)
          : group.withNewFields(group.getType(0).asGroupType().withNewFields(selected));
    } else if (expected.type().isMapType()) {
      GroupType group = physical.asGroupType();
      GroupType entries = group.getType(0).asGroupType();
      return group.withNewFields(
          entries.withNewFields(
              project(entries.getType(0), expected.type().asMapType().fields().get(0)),
              project(entries.getType(1), expected.type().asMapType().fields().get(1))));
    }
    return physical;
  }

  // Drops structs with no columns to read; NestedColumnVector reads them as always present.
  private static Type readable(Type type) {
    if (type.isPrimitive()) {
      return type;
    }

    GroupType group = type.asGroupType();
    List<Type> fields = Lists.newArrayList();
    for (Type child : group.getFields()) {
      Type readableChild = readable(child);
      if (readableChild != null) {
        fields.add(readableChild);
      }
    }
    return fields.isEmpty() ? null : group.withNewFields(fields);
  }

  private static class Reader implements VectorizedReader<ColumnarBatch> {
    private final Schema expected;
    private final Type[] parquetTypes;
    private final VectorizedReader<ColumnarBatch> primitives;
    private final IcebergNestedParquetReader nested;
    private final int[] primitiveIndices;
    private final int[] nestedIndices;
    private ColumnarBatch primitiveBatch;
    private ColumnVector[] projected;
    private int batchSize;

    private Reader(Schema expected, MessageType fileSchema, Map<Integer, ?> constants) {
      this.expected = expected;
      this.primitiveIndices = new int[expected.columns().size()];
      this.nestedIndices = new int[expected.columns().size()];
      this.parquetTypes = new Type[expected.columns().size()];
      List<Types.NestedField> primitiveFields = Lists.newArrayList();
      List<Type> nestedFields = Lists.newArrayList();
      MessageType pruned = ParquetSchemaUtil.pruneColumns(fileSchema, expected);
      int[] fileIndices = NestedColumnVector.fieldIndices(pruned, expected.columns());
      for (int i = 0; i < expected.columns().size(); i++) {
        Types.NestedField field = expected.columns().get(i);
        primitiveIndices[i] = -1;
        nestedIndices[i] = -1;
        if (isNested(field, constants)) {
          int index = fileIndices[i];
          if (index >= 0) {
            parquetTypes[i] = project(pruned.getType(index), field);
            Type readable = readable(parquetTypes[i]);
            if (readable != null) {
              nestedIndices[i] = nestedFields.size();
              nestedFields.add(readable);
            }
          }
        } else {
          primitiveIndices[i] = primitiveFields.size();
          primitiveFields.add(field);
        }
      }
      this.nested =
          new IcebergNestedParquetReader(new MessageType(fileSchema.getName(), nestedFields));
      this.primitives =
          primitiveFields.isEmpty()
              ? null
              : VectorizedSparkParquetReaders.buildReader(
                  new Schema(primitiveFields), fileSchema, constants);
    }

    @Override
    public void setBatchSize(int size) {
      this.batchSize = size;
      if (primitives != null) {
        primitives.setBatchSize(size);
      }
      nested.setBatchSize(size);
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      if (primitives != null) {
        primitives.setRowGroupInfo(pages, metadata);
      }
      nested.setRowGroupInfo(pages, metadata);
    }

    @Override
    public ColumnarBatch read(ColumnarBatch reuse, int numRows) {
      if (primitives != null) {
        this.primitiveBatch = primitives.read(reuse == null ? null : primitiveBatch, numRows);
      }
      ColumnarBatch nestedBatch = nested.read(null, numRows);
      if (projected == null) {
        this.projected = new ColumnVector[expected.columns().size()];
        for (int i = 0; i < projected.length; i++) {
          if (primitiveIndices[i] < 0) {
            int index = nestedIndices[i];
            projected[i] =
                NestedColumnVector.project(
                    expected.columns().get(i),
                    parquetTypes[i],
                    index < 0 ? null : (WritableColumnVector) nestedBatch.column(index),
                    batchSize);
          }
        }
      }
      ColumnVector[] vectors = projected.clone();
      for (int i = 0; i < vectors.length; i++) {
        if (primitiveIndices[i] >= 0) {
          vectors[i] = primitiveBatch.column(primitiveIndices[i]);
        }
      }
      return new ColumnarBatch(vectors, numRows);
    }

    @Override
    public void close() {
      try {
        if (primitives != null) {
          primitives.close();
        }
      } finally {
        nested.close();
      }
    }
  }
}
