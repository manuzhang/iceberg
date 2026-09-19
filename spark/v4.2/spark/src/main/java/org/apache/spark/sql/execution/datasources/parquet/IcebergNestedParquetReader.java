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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Option;

/**
 * Adapts Spark's nested Parquet decoder to Iceberg row groups.
 *
 * <p>This adapter lives in Spark's package to access its column decoder and nested assembler.
 * Iceberg owns file access, projection, row group selection, and row positions.
 */
public class IcebergNestedParquetReader implements VectorizedReader<ColumnarBatch> {
  private final ParquetColumn schema;
  private final List<ParquetColumnVector> columns = new ArrayList<>();
  private final List<ParquetColumnVector> leaves = new ArrayList<>();
  private int batchSize = 4096;
  private boolean initialized;

  public IcebergNestedParquetReader(MessageType parquetSchema) {
    this.schema =
        new ParquetToSparkSchemaConverter(false, true, true, true, false, false, true, true)
            .convertParquetColumn(
                parquetSchema,
                Option.apply(
                    (StructType)
                        physicalType(ParquetSchemaUtil.convert(parquetSchema).asStruct())));
  }

  private static DataType physicalType(Type type) {
    return switch (type.typeId()) {
        // Spark has no UUID logical type. The output projection converts the bytes to a string.
      case UUID -> DataTypes.BinaryType;
      case STRUCT -> {
        List<StructField> fields = new ArrayList<>();
        for (Types.NestedField field : type.asStructType().fields()) {
          fields.add(
              DataTypes.createStructField(
                  field.name(), physicalType(field.type()), field.isOptional()));
        }
        yield DataTypes.createStructType(fields);
      }
      case LIST ->
          DataTypes.createArrayType(
              physicalType(type.asListType().elementType()), type.asListType().isElementOptional());
      case MAP ->
          DataTypes.createMapType(
              physicalType(type.asMapType().keyType()),
              physicalType(type.asMapType().valueType()),
              type.asMapType().isValueOptional());
      default -> SparkSchemaUtil.convert(type);
    };
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  private void initialize() {
    if (!initialized) {
      for (int i = 0; i < schema.children().size(); i++) {
        ParquetColumn column = schema.children().apply(i);
        WritableColumnVector vector = new OnHeapColumnVector(batchSize, column.sparkType());
        ParquetColumnVector nested =
            new ParquetColumnVector(column, vector, batchSize, Collections.emptySet(), true, null);
        columns.add(nested);
        leaves.addAll(nested.getLeaves());
      }
      this.initialized = true;
    }
  }

  @Override
  public void setRowGroupInfo(PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    initialize();
    try {
      for (ParquetColumnVector leaf : leaves) {
        leaf.setColumnReader(
            new VectorizedColumnReader(
                leaf.getColumn().descriptor().get(),
                leaf.getColumn().required(),
                pages,
                null,
                "CORRECTED",
                "UTC",
                "CORRECTED",
                "UTC",
                null));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to initialize nested Parquet columns", e);
    }
  }

  @Override
  public ColumnarBatch read(ColumnarBatch reuse, int numRows) {
    try {
      for (ParquetColumnVector column : columns) {
        column.reset();
      }
      for (ParquetColumnVector leaf : leaves) {
        leaf.getColumnReader()
            .readBatch(
                numRows,
                leaf.getValueVector(),
                leaf.getRepetitionLevelVector(),
                leaf.getDefinitionLevelVector());
      }
      ColumnVector[] vectors = new ColumnVector[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        columns.get(i).assemble();
        vectors[i] = columns.get(i).getValueVector();
      }
      return new ColumnarBatch(vectors, numRows);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read nested Parquet columns", e);
    }
  }

  @Override
  public void close() {
    for (ParquetColumnVector leaf : leaves) {
      if (leaf.getRepetitionLevelVector() != null) {
        leaf.getRepetitionLevelVector().close();
      }
      if (leaf.getDefinitionLevelVector() != null) {
        leaf.getDefinitionLevelVector().close();
      }
    }
    for (ParquetColumnVector column : columns) {
      column.getValueVector().close();
    }
    columns.clear();
    leaves.clear();
    this.initialized = false;
  }
}
