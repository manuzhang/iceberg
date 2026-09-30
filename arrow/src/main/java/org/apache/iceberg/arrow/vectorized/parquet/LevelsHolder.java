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

import java.util.Arrays;

/** Holds the repetition and definition levels of the values read for a batch of rows. */
public class LevelsHolder {
  private int[] repetitionLevels = new int[0];
  private int[] definitionLevels = new int[0];
  private int numValues = 0;
  private int numRows = 0;

  public int numValues() {
    return numValues;
  }

  public int repetitionLevel(int index) {
    return repetitionLevels[index];
  }

  public int definitionLevel(int index) {
    return definitionLevels[index];
  }

  int numRows() {
    return numRows;
  }

  void reset() {
    this.numValues = 0;
    this.numRows = 0;
  }

  void append(int[] pageRepetitionLevels, int[] pageDefinitionLevels, int offset, int length) {
    ensureCapacity(numValues + length);
    if (pageRepetitionLevels != null) {
      System.arraycopy(pageRepetitionLevels, offset, repetitionLevels, numValues, length);
      for (int pos = offset; pos < offset + length; pos += 1) {
        if (pageRepetitionLevels[pos] == 0) {
          numRows += 1;
        }
      }
    } else {
      Arrays.fill(repetitionLevels, numValues, numValues + length, 0);
      numRows += length;
    }

    System.arraycopy(pageDefinitionLevels, offset, definitionLevels, numValues, length);
    numValues += length;
  }

  private void ensureCapacity(int size) {
    if (size > definitionLevels.length) {
      int newSize = Math.max(size, definitionLevels.length * 2);
      this.repetitionLevels = Arrays.copyOf(repetitionLevels, newSize);
      this.definitionLevels = Arrays.copyOf(definitionLevels, newSize);
    }
  }
}
