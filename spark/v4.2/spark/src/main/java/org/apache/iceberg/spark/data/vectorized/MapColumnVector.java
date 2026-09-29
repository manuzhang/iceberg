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

import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarMap;

class MapColumnVector extends NestedColumnVector {
  private final VectorHolder.MapVectorHolder holder;
  private final ColumnVector keys;
  private final ColumnVector values;

  MapColumnVector(VectorHolder.MapVectorHolder holder, ColumnVector keys, ColumnVector values) {
    super(holder);
    this.holder = holder;
    this.keys = keys;
    this.values = values;
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }

    return new ColumnarMap(keys, values, holder.offset(rowId), holder.length(rowId));
  }

  @Override
  public void close() {
    keys.close();
    values.close();
  }
}
