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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;

class RowIdDeleteIndex {
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final Map<String, List<DeleteFile>> dvByDataFile;

  private RowIdDeleteIndex(Map<String, List<DeleteFile>> dvByDataFile) {
    this.dvByDataFile = dvByDataFile;
  }

  static RowIdDeleteIndex fromDeleteFiles(Iterable<DeleteFile> deleteFiles) {
    Map<String, List<DeleteFile>> dvs = Maps.newHashMap();

    for (DeleteFile deleteFile : deleteFiles) {
      if (!ContentFileUtil.isDV(deleteFile)) {
        continue;
      }

      String referencedDataFile = ContentFileUtil.referencedDataFileLocation(deleteFile);
      if (referencedDataFile == null) {
        continue;
      }

      dvs.computeIfAbsent(referencedDataFile, ignored -> Lists.newArrayList()).add(deleteFile);
    }

    Map<String, List<DeleteFile>> immutableDvs = Maps.newHashMapWithExpectedSize(dvs.size());
    dvs.forEach((path, files) -> immutableDvs.put(path, ImmutableList.copyOf(files)));
    return new RowIdDeleteIndex(immutableDvs);
  }

  boolean isEmpty() {
    return dvByDataFile.isEmpty();
  }

  Set<String> referencedDataFiles() {
    return dvByDataFile.keySet();
  }

  DeleteFile[] forDataFile(DataFile dataFile) {
    if (dataFile.firstRowId() == null) {
      return NO_DELETES;
    }

    List<DeleteFile> deleteFiles = dvByDataFile.get(dataFile.location().toString());
    return deleteFiles != null ? deleteFiles.toArray(new DeleteFile[0]) : NO_DELETES;
  }
}
