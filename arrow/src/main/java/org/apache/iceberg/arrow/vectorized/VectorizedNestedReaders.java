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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.arrow.vectorized.parquet.LevelsHolder;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Readers for struct, list, and map fields.
 *
 * <p>A nested reader that reads a file column is asked for the rows of the batch, like the columns
 * under it. It finds its own values, which can be more or fewer than the rows of the batch, from
 * the repetition and definition levels of one of those columns, following the Dremel encoding that
 * Parquet uses. A reader that reads no file column, such as a constant, is asked for the number of
 * values of its parent instead.
 */
class VectorizedNestedReaders {
  private VectorizedNestedReaders() {}

  static VectorizedArrowReader struct(
      Types.NestedField icebergField,
      List<VectorizedArrowReader> fieldReaders,
      ColumnDescriptor presenceColumn,
      int repetitionLevel,
      int definitionLevel) {
    return new StructReader(
        icebergField, fieldReaders, presenceColumn, repetitionLevel, definitionLevel);
  }

  static VectorizedArrowReader list(
      Types.NestedField icebergField,
      VectorizedArrowReader elementReader,
      int repetitionLevel,
      int definitionLevel) {
    return new ListReader(icebergField, elementReader, repetitionLevel, definitionLevel);
  }

  static VectorizedArrowReader map(
      Types.NestedField icebergField,
      VectorizedArrowReader keyReader,
      VectorizedArrowReader valueReader,
      int repetitionLevel,
      int definitionLevel) {
    return new MapReader(icebergField, keyReader, valueReader, repetitionLevel, definitionLevel);
  }

  private abstract static class NestedReader extends VectorizedArrowReader {
    private NullabilityHolder nulls = null;

    NestedReader(Types.NestedField icebergField) {
      super(icebergField);
    }

    /** Returns a nullability holder that can track {@code numValues} values. */
    NullabilityHolder nulls(VectorHolder reuse, int numValues) {
      if (reuse == null || nulls == null) {
        this.nulls = new NullabilityHolder(numValues);
      } else {
        nulls.reset();
        nulls.ensureCapacity(numValues);
      }

      return nulls;
    }
  }

  private static class StructReader extends NestedReader {
    private final List<VectorizedArrowReader> fieldReaders;
    private final PresenceReader presenceReader;
    private final LevelsHolder levels;
    private final int repetitionLevel;
    private final int definitionLevel;
    private final boolean presentWithoutColumn;

    private StructReader(
        Types.NestedField icebergField,
        List<VectorizedArrowReader> fieldReaders,
        ColumnDescriptor presenceColumn,
        int repetitionLevel,
        int definitionLevel) {
      super(icebergField);
      this.fieldReaders = fieldReaders;
      this.presenceReader = presenceColumn != null ? new PresenceReader(presenceColumn) : null;
      this.levels = presenceReader != null ? presenceReader.levels() : fieldLevels(fieldReaders);
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
      // like the row reader, a struct with no column to read is present when it has a constant
      this.presentWithoutColumn =
          icebergField.isRequired()
              || fieldReaders.stream().anyMatch(ConstantVectorReader.class::isInstance);
    }

    private static LevelsHolder fieldLevels(List<VectorizedArrowReader> fieldReaders) {
      return fieldReaders.stream()
          .map(VectorizedArrowReader::levels)
          .filter(fieldLevels -> fieldLevels != null)
          .findFirst()
          .orElse(null);
    }

    @Override
    LevelsHolder levels() {
      return levels;
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      List<VectorHolder> reuseHolders =
          reuse instanceof VectorHolder.StructVectorHolder structReuse
              ? structReuse.fieldHolders()
              : null;
      VectorHolder[] fieldHolders = new VectorHolder[fieldReaders.size()];

      for (int pos = 0; pos < fieldReaders.size(); pos += 1) {
        VectorizedArrowReader reader = fieldReaders.get(pos);
        if (reader.levels() != null) {
          fieldHolders[pos] = reader.read(reuseHolder(reuseHolders, pos), numValsToRead);
        }
      }

      if (presenceReader != null) {
        presenceReader.read(numValsToRead);
      }

      NullabilityHolder nulls;
      int numStructs;
      if (levels != null) {
        nulls = nulls(reuse, levels.numValues());
        numStructs = 0;
        for (int pos = 0; pos < levels.numValues(); pos += 1) {
          if (levels.repetitionLevel(pos) <= repetitionLevel) {
            if (levels.definitionLevel(pos) < definitionLevel) {
              nulls.setNull(numStructs);
            } else {
              nulls.setNotNull(numStructs);
            }

            numStructs += 1;
          }
        }
      } else {
        numStructs = numValsToRead;
        nulls = nulls(reuse, numStructs);
        if (presentWithoutColumn) {
          nulls.setNotNulls(0, numStructs);
        } else {
          nulls.setNulls(0, numStructs);
        }
      }

      for (int pos = 0; pos < fieldReaders.size(); pos += 1) {
        VectorizedArrowReader reader = fieldReaders.get(pos);
        if (reader.levels() == null) {
          fieldHolders[pos] = reader.read(reuseHolder(reuseHolders, pos), numStructs);
        }
      }

      return new VectorHolder.StructVectorHolder(
          icebergField(), numStructs, Arrays.asList(fieldHolders), nulls);
    }

    private static VectorHolder reuseHolder(List<VectorHolder> reuseHolders, int pos) {
      return reuseHolders != null ? reuseHolders.get(pos) : null;
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      for (VectorizedArrowReader reader : fieldReaders) {
        reader.setRowGroupInfo(source, metadata);
      }

      if (presenceReader != null) {
        presenceReader.setRowGroupInfo(source, metadata);
      }
    }

    @Override
    public void setBatchSize(int batchSize) {
      for (VectorizedArrowReader reader : fieldReaders) {
        reader.setBatchSize(batchSize);
      }
    }

    @Override
    public void close() {
      for (VectorizedArrowReader reader : fieldReaders) {
        reader.close();
      }
    }

    @Override
    public String toString() {
      return "StructReader(" + fieldReaders + ")";
    }
  }

  /** Reads only the levels of a column, to find which structs are present. */
  private static class PresenceReader {
    private final ColumnDescriptor desc;
    private final VectorizedColumnIterator columnIterator;
    private final LevelsHolder levels = new LevelsHolder();

    private PresenceReader(ColumnDescriptor desc) {
      this.desc = desc;
      this.columnIterator = new VectorizedColumnIterator(desc, "", false, true);
    }

    LevelsHolder levels() {
      return levels;
    }

    void read(int numRows) {
      columnIterator.nextLevels(levels, numRows);
    }

    void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      ColumnChunkMetaData chunkMetaData = metadata.get(ColumnPath.get(desc.getPath()));
      columnIterator.setRowGroupInfo(
          source.getPageReader(desc), !ParquetUtil.hasNonDictionaryPages(chunkMetaData));
    }
  }

  /** Finds the lists or maps of a batch from the levels of a column under their elements. */
  private abstract static class RepeatedReader extends NestedReader {
    private final int repetitionLevel;
    private final int definitionLevel;
    private int[] offsets = new int[0];
    private int[] lengths = new int[0];
    private NullabilityHolder rowNulls = null;
    private int numRows = 0;
    private int numElements = 0;

    RepeatedReader(Types.NestedField icebergField, int repetitionLevel, int definitionLevel) {
      super(icebergField);
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }

    /**
     * Finds the rows in the levels. A value with a repetition level at or below this field's starts
     * a new row, and a value one level deeper starts a new element in the current row. The
     * definition level of a row's first value shows whether the row is null, empty, or has
     * elements. A null or empty row still has a value in the element readers, which the offsets
     * skip.
     */
    void readRows(VectorHolder reuse, LevelsHolder levels) {
      int numValues = levels.numValues();
      if (reuse == null || offsets.length < numValues) {
        this.offsets = new int[numValues];
        this.lengths = new int[numValues];
      }

      this.rowNulls = nulls(reuse, numValues);
      int row = -1;
      int element = 0;
      for (int pos = 0; pos < numValues; pos += 1) {
        int valueRepetitionLevel = levels.repetitionLevel(pos);
        if (valueRepetitionLevel <= repetitionLevel) {
          row += 1;
          offsets[row] = element;
          int valueDefinitionLevel = levels.definitionLevel(pos);
          if (valueDefinitionLevel < definitionLevel) {
            rowNulls.setNull(row);
            lengths[row] = 0;
          } else {
            rowNulls.setNotNull(row);
            lengths[row] = valueDefinitionLevel > definitionLevel ? 1 : 0;
          }
        } else if (valueRepetitionLevel == repetitionLevel + 1) {
          lengths[row] += 1;
        }

        if (valueRepetitionLevel <= repetitionLevel + 1) {
          element += 1;
        }
      }

      this.numRows = row + 1;
      this.numElements = element;
    }

    int[] offsets() {
      return offsets;
    }

    int[] lengths() {
      return lengths;
    }

    NullabilityHolder rowNulls() {
      return rowNulls;
    }

    int numRows() {
      return numRows;
    }

    int numElements() {
      return numElements;
    }
  }

  private static class ListReader extends RepeatedReader {
    private final VectorizedArrowReader elementReader;

    private ListReader(
        Types.NestedField icebergField,
        VectorizedArrowReader elementReader,
        int repetitionLevel,
        int definitionLevel) {
      super(icebergField, repetitionLevel, definitionLevel);
      this.elementReader = elementReader;
    }

    @Override
    LevelsHolder levels() {
      return elementReader.levels();
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      VectorHolder reuseElements =
          reuse instanceof VectorHolder.ListVectorHolder listReuse
              ? listReuse.elementHolder()
              : null;
      VectorHolder elements = elementReader.read(reuseElements, numValsToRead);
      readRows(reuse, elementReader.levels());
      return new VectorHolder.ListVectorHolder(
          icebergField(), numRows(), elements, offsets(), lengths(), rowNulls());
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      elementReader.setRowGroupInfo(source, metadata);
    }

    @Override
    public void setBatchSize(int batchSize) {
      elementReader.setBatchSize(batchSize);
    }

    @Override
    public void close() {
      elementReader.close();
    }

    @Override
    public String toString() {
      return "ListReader(" + elementReader + ")";
    }
  }

  private static class MapReader extends RepeatedReader {
    private final VectorizedArrowReader keyReader;
    private final VectorizedArrowReader valueReader;

    private MapReader(
        Types.NestedField icebergField,
        VectorizedArrowReader keyReader,
        VectorizedArrowReader valueReader,
        int repetitionLevel,
        int definitionLevel) {
      super(icebergField, repetitionLevel, definitionLevel);
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    LevelsHolder levels() {
      return keyReader.levels();
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      VectorHolder reuseKeys = null;
      VectorHolder reuseValues = null;
      if (reuse instanceof VectorHolder.MapVectorHolder mapReuse) {
        reuseKeys = mapReuse.keyHolder();
        reuseValues = mapReuse.valueHolder();
      }

      VectorHolder keys = keyReader.read(reuseKeys, numValsToRead);
      readRows(reuse, keyReader.levels());
      VectorHolder values =
          valueReader.levels() != null
              ? valueReader.read(reuseValues, numValsToRead)
              : valueReader.read(reuseValues, numElements());
      return new VectorHolder.MapVectorHolder(
          icebergField(), numRows(), keys, values, offsets(), lengths(), rowNulls());
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      keyReader.setRowGroupInfo(source, metadata);
      valueReader.setRowGroupInfo(source, metadata);
    }

    @Override
    public void setBatchSize(int batchSize) {
      keyReader.setBatchSize(batchSize);
      valueReader.setBatchSize(batchSize);
    }

    @Override
    public void close() {
      keyReader.close();
      valueReader.close();
    }

    @Override
    public String toString() {
      return "MapReader(" + keyReader + ", " + valueReader + ")";
    }
  }
}
