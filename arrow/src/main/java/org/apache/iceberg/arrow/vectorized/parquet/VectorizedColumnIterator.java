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
package org.apache.iceberg.arrow.vectorized.parquet;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BaseColumnIterator;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReader;

/**
 * Vectorized version of the ColumnIterator that reads column values in data pages of a column in a
 * row group in a batched fashion.
 */
public class VectorizedColumnIterator extends BaseColumnIterator {

  private final VectorizedPageIterator vectorizedPageIterator;
  private int batchSize;

  public VectorizedColumnIterator(
      ColumnDescriptor desc, String writerVersion, boolean setArrowValidityVector) {
    this(desc, writerVersion, setArrowValidityVector, false);
  }

  /**
   * Creates a column iterator that, when {@code readsLevels} is true, reads values by whole rows
   * together with their repetition and definition levels, so that it can read columns nested in
   * structs, lists, and maps.
   */
  public VectorizedColumnIterator(
      ColumnDescriptor desc,
      String writerVersion,
      boolean setArrowValidityVector,
      boolean readsLevels) {
    super(desc);
    Preconditions.checkArgument(
        readsLevels || desc.getMaxRepetitionLevel() == 0,
        "Only non-nested columns are supported for vectorized reads");
    this.vectorizedPageIterator =
        new VectorizedPageIterator(desc, writerVersion, setArrowValidityVector, readsLevels);
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public Dictionary setRowGroupInfo(PageReader store, boolean allPagesDictEncoded) {
    // setPageSource can result in a data page read. If that happens, we need
    // to know in advance whether all the pages in the row group are dictionary encoded or not
    this.vectorizedPageIterator.setAllPagesDictEncoded(allPagesDictEncoded);
    super.setPageSource(store);
    return dictionary;
  }

  @Override
  protected BasePageIterator pageIterator() {
    return vectorizedPageIterator;
  }

  public boolean producesDictionaryEncodedVector() {
    return vectorizedPageIterator.producesDictionaryEncodedVector();
  }

  /**
   * Reads the levels of the values in the next {@code numRows} rows, without reading the values.
   *
   * @param levels the holder that receives the levels
   * @param numRows the number of rows to read
   */
  public void nextLevels(LevelsHolder levels, int numRows) {
    levels.reset();
    int numValues = nextRowValues(levels, numRows);
    while (numValues > 0) {
      vectorizedPageIterator.skipValues(numValues);
      triplesRead += numValues;
      numValues = nextRowValues(levels, numRows);
    }
  }

  /**
   * Moves to the values of the rows that are still needed to complete a batch of {@code numRows}
   * rows and adds their levels to the holder.
   *
   * @return the number of values to read from the current page, or 0 if the batch is complete
   */
  private int nextRowValues(LevelsHolder levels, int numRows) {
    if (!hasNext()) {
      return 0;
    }

    advance();
    int numValues = vectorizedPageIterator.valueCount(numRows - levels.numRows());
    vectorizedPageIterator.appendLevels(levels, numValues);
    return numValues;
  }

  private static void ensureCapacity(FieldVector vector, NullabilityHolder holder, int numValues) {
    while (vector.getValueCapacity() < numValues) {
      vector.reAlloc();
    }

    holder.ensureCapacity(numValues);
  }

  public abstract class BatchReader {
    public void nextBatch(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
      int rowsReadSoFar = 0;
      while (rowsReadSoFar < batchSize && hasNext()) {
        advance();
        int rowsInThisBatch =
            nextBatchOf(fieldVector, batchSize - rowsReadSoFar, rowsReadSoFar, typeWidth, holder);
        rowsReadSoFar += rowsInThisBatch;
        triplesRead += rowsInThisBatch;
        fieldVector.setValueCount(rowsReadSoFar);
      }
    }

    /**
     * Reads the values of the next {@code numRows} rows and their levels. A row of a repeated
     * column can have any number of values, so the vector and the nullability holder grow to fit
     * them.
     *
     * @param vector the vector that receives the values, starting at index 0
     * @param typeWidth the width of a value in bytes, or -1 for variable width values
     * @param holder the holder that receives the nullability of the values
     * @param levels the holder that receives the levels of the values
     * @param numRows the number of rows to read
     */
    public void nextBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder holder,
        LevelsHolder levels,
        int numRows) {
      levels.reset();
      int valuesReadSoFar = 0;
      int numValues = nextRowValues(levels, numRows);
      while (numValues > 0) {
        ensureCapacity(vector, holder, valuesReadSoFar + numValues);
        nextBatchOf(vector, numValues, valuesReadSoFar, typeWidth, holder);
        valuesReadSoFar += numValues;
        triplesRead += numValues;
        vector.setValueCount(valuesReadSoFar);
        numValues = nextRowValues(levels, numRows);
      }
    }

    protected abstract int nextBatchOf(
        FieldVector vector,
        int expectedBatchSize,
        int numValsInVector,
        int typeWidth,
        NullabilityHolder holder);
  }

  public class IntegerBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .intPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class DictionaryBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator.nextBatchDictionaryIds(
          (IntVector) vector, expectedBatchSize, numValsInVector, holder);
    }
  }

  public class LongBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .longPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class TimestampMillisBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .timestampMillisPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class TimestampInt96BatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .timestampInt96PageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class FloatBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .floatPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class DoubleBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .doublePageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class FixedSizeBinaryBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .fixedSizeBinaryPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class VarWidthTypeBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .varWidthTypePageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public class BooleanBatchReader extends BatchReader {
    @Override
    protected int nextBatchOf(
        final FieldVector vector,
        final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth,
        NullabilityHolder holder) {
      return vectorizedPageIterator
          .booleanPageReader()
          .nextBatch(vector, expectedBatchSize, numValsInVector, typeWidth, holder);
    }
  }

  public IntegerBatchReader integerBatchReader() {
    return new IntegerBatchReader();
  }

  public DictionaryBatchReader dictionaryBatchReader() {
    return new DictionaryBatchReader();
  }

  public LongBatchReader longBatchReader() {
    return new LongBatchReader();
  }

  public TimestampMillisBatchReader timestampMillisBatchReader() {
    return new TimestampMillisBatchReader();
  }

  public TimestampInt96BatchReader timestampInt96BatchReader() {
    return new TimestampInt96BatchReader();
  }

  public FloatBatchReader floatBatchReader() {
    return new FloatBatchReader();
  }

  public DoubleBatchReader doubleBatchReader() {
    return new DoubleBatchReader();
  }

  public FixedSizeBinaryBatchReader fixedSizeBinaryBatchReader() {
    return new FixedSizeBinaryBatchReader();
  }

  public VarWidthTypeBatchReader varWidthTypeBatchReader() {
    return new VarWidthTypeBatchReader();
  }

  public BooleanBatchReader booleanBatchReader() {
    return new BooleanBatchReader();
  }
}
