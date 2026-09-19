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
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Projects decoded nested vectors by Iceberg field ID without copying their values.
 *
 * <p>A struct without a decoded vector has no columns to read because it can never be null; it is
 * always present and its children are read as missing.
 */
class NestedColumnVector extends ColumnVector {
  private final WritableColumnVector delegate;
  private final ColumnVector[] children;
  private final boolean uuid;

  private NestedColumnVector(
      DataType type, WritableColumnVector delegate, ColumnVector[] children, boolean uuid) {
    super(type);
    this.delegate = delegate;
    this.children = children;
    this.uuid = uuid;
  }

  static ColumnVector project(
      Types.NestedField field, Type parquet, WritableColumnVector vector, int batchSize) {
    if (parquet == null) {
      Preconditions.checkArgument(
          field.isOptional() || field.initialDefault() != null,
          "Missing required field: %s",
          field.name());
      return new ConstantColumnVector(
          field.type(), batchSize, SparkUtil.internalToSpark(field.type(), field.initialDefault()));
    }

    Preconditions.checkArgument(
        vector != null || field.type().isStructType(),
        "Missing vector for field: %s",
        field.name());
    List<ColumnVector> children = Lists.newArrayList();
    if (field.type().isStructType()) {
      children.addAll(
          structChildren(field.type().asStructType(), parquet.asGroupType(), vector, batchSize));
    } else if (field.type().isListType()) {
      children.add(
          project(
              field.type().asListType().fields().get(0),
              ParquetSchemaUtil.determineListElementType(parquet.asGroupType()),
              vector.getChild(0),
              batchSize));
    } else if (field.type().isMapType()) {
      GroupType entries = parquet.asGroupType().getType(0).asGroupType();
      for (int i = 0; i < 2; i++) {
        children.add(
            project(
                field.type().asMapType().fields().get(i),
                entries.getType(i),
                vector.getChild(i),
                batchSize));
      }
    }
    return new NestedColumnVector(
        SparkSchemaUtil.convert(field.type()),
        vector,
        children.toArray(new ColumnVector[0]),
        field.type().equals(Types.UUIDType.get()));
  }

  private static List<ColumnVector> structChildren(
      Types.StructType expected, GroupType group, WritableColumnVector vector, int batchSize) {
    List<Types.NestedField> fields = expected.fields();
    int[] indices = fieldIndices(group, fields);
    List<ColumnVector> children = Lists.newArrayList();
    for (int i = 0; i < fields.size(); i++) {
      Type childType = indices[i] < 0 ? null : group.getType(indices[i]);
      children.add(
          project(fields.get(i), childType, childVector(group, indices[i], vector), batchSize));
    }

    return children;
  }

  private static WritableColumnVector childVector(
      GroupType group, int index, WritableColumnVector vector) {
    if (vector == null || index < 0 || isEmpty(group.getType(index))) {
      return null;
    }

    return vector.getChild(readIndex(group, index));
  }

  static boolean isEmpty(Type type) {
    return !type.isPrimitive()
        && type.asGroupType().getFields().stream().allMatch(NestedColumnVector::isEmpty);
  }

  // Empty structs are not decoded, so they do not have child vectors.
  private static int readIndex(GroupType group, int index) {
    int readIndex = 0;
    for (int i = 0; i < index; i++) {
      if (!isEmpty(group.getType(i))) {
        readIndex += 1;
      }
    }
    return readIndex;
  }

  static int[] fieldIndices(GroupType group, List<Types.NestedField> fields) {
    Map<Integer, Integer> byId = Maps.newHashMap();
    for (int i = 0; i < group.getFieldCount(); i++) {
      Type candidate = group.getType(i);
      if (candidate.getId() != null) {
        byId.put(candidate.getId().intValue(), i);
      }
    }
    int[] indices = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      indices[i] = byId.getOrDefault(field.fieldId(), -1);
    }
    return indices;
  }

  @Override
  public void close() {
    // The reader owns the physical vectors, including unprojected children and level buffers.
  }

  @Override
  public boolean hasNull() {
    return delegate != null && delegate.hasNull();
  }

  @Override
  public int numNulls() {
    return delegate == null ? 0 : delegate.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return delegate != null && delegate.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return delegate.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return delegate.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return delegate.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return delegate.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return delegate.dataType().equals(DataTypes.IntegerType)
        ? delegate.getInt(rowId)
        : delegate.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return delegate.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return delegate.dataType().equals(DataTypes.FloatType)
        ? delegate.getFloat(rowId)
        : delegate.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    DecimalType physicalType = (DecimalType) delegate.dataType();
    return delegate.getDecimal(rowId, physicalType.precision(), physicalType.scale());
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return uuid
        ? UTF8String.fromString(UUIDUtil.convert(delegate.getBinary(rowId)).toString())
        : delegate.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return delegate.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return new ColumnarArray(
        children[0], delegate.getArrayOffset(rowId), delegate.getArrayLength(rowId));
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    return new ColumnarMap(
        children[0], children[1], delegate.getArrayOffset(rowId), delegate.getArrayLength(rowId));
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return children[ordinal];
  }
}
