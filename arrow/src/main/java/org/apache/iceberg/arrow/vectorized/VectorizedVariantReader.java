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

import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;

/**
 * Vectorized reader for Variant columns stored in Parquet.
 *
 * <p>This reader handles the serialized variant format where variant data is stored as a GroupType
 * with:
 *
 * <ul>
 *   <li>metadata: required binary column containing variant metadata
 *   <li>value: optional binary column containing serialized variant value
 * </ul>
 *
 * <p>The reader produces a StructVector with two children (metadata and value) that can be
 * converted to Iceberg Variant objects.
 */
public class VectorizedVariantReader implements VectorizedReader<VectorHolder> {
  private final VectorizedArrowReader metadataReader;
  private final VectorizedArrowReader valueReader;
  private final Types.NestedField icebergField;
  private final BufferAllocator allocator;
  private StructVector variantVector;

  public VectorizedVariantReader(
      ColumnDescriptor metadataDesc,
      ColumnDescriptor valueDesc,
      Types.NestedField icebergField,
      BufferAllocator allocator,
      boolean setArrowValidityVector) {
    this.icebergField = icebergField;
    this.allocator = allocator;

    // Create nested field for metadata (required binary)
    Types.NestedField metadataField =
        Types.NestedField.required(
            icebergField.fieldId() * 1000, "metadata", Types.BinaryType.get());
    this.metadataReader =
        new VectorizedArrowReader(metadataDesc, metadataField, allocator, setArrowValidityVector);

    // Create nested field for value (optional binary)
    Types.NestedField valueField =
        Types.NestedField.optional(
            icebergField.fieldId() * 1000 + 1, "value", Types.BinaryType.get());
    this.valueReader =
        new VectorizedArrowReader(valueDesc, valueField, allocator, setArrowValidityVector);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numValsToRead) {
    if (variantVector == null || reuse == null) {
      allocateStructVector();
    }

    // Read metadata column
    VectorHolder metadataHolder = metadataReader.read(null, numValsToRead);
    VarBinaryVector metadataVec = (VarBinaryVector) metadataHolder.vector();

    // Read value column
    VectorHolder valueHolder = valueReader.read(null, numValsToRead);
    VarBinaryVector valueVec = (VarBinaryVector) valueHolder.vector();

    // Set child vectors in the struct
    variantVector.getChild("metadata").clear();
    variantVector.getChild("value").clear();

    // Transfer data from readers to struct children
    metadataVec.makeTransferPair(variantVector.getChild("metadata")).transfer();
    valueVec.makeTransferPair(variantVector.getChild("value")).transfer();

    variantVector.setValueCount(numValsToRead);

    return new VectorHolder(null, variantVector, false, null, null, icebergField);
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.metadataReader.setBatchSize(batchSize);
    this.valueReader.setBatchSize(batchSize);
  }

  @Override
  public void setRowGroupInfo(
      org.apache.parquet.column.page.PageReadStore source,
      Map<ColumnPath, ColumnChunkMetaData> metadata) {
    this.metadataReader.setRowGroupInfo(source, metadata);
    this.valueReader.setRowGroupInfo(source, metadata);
  }

  @Override
  public void close() {
    if (variantVector != null) {
      variantVector.close();
    }
    metadataReader.close();
    valueReader.close();
  }

  private void allocateStructVector() {
    Field metadataField =
        new Field(
            "metadata",
            FieldType.notNullable(ArrowType.Binary.INSTANCE),
            ImmutableList.of());
    Field valueField =
        new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), ImmutableList.of());

    Field structField =
        new Field(
            icebergField.name(),
            FieldType.nullable(new ArrowType.Struct()),
            ImmutableList.of(metadataField, valueField));

    this.variantVector = (StructVector) structField.createVector(allocator);
    this.variantVector.allocateNew();
  }

  @Override
  public String toString() {
    return "VectorizedVariantReader(" + icebergField.name() + ")";
  }
}
