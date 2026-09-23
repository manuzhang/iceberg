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
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

class ChangelogRowReader extends BaseRowReader<ChangelogScanTask>
    implements PartitionReader<InternalRow> {

  ChangelogRowReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.io(),
        partition.taskGroup(),
        partition.projection(),
        partition.isCaseSensitive(),
        partition.cacheDeleteFilesOnExecutors());
  }

  ChangelogRowReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<ChangelogScanTask> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    super(
        table,
        fileIO,
        taskGroup,
        ChangelogUtil.dropChangelogMetadata(expectedSchema),
        caseSensitive,
        cacheDeleteFilesOnExecutors);
  }

  @Override
  protected CloseableIterator<InternalRow> open(ChangelogScanTask task) {
    JoinedRow cdcRow = new JoinedRow();

    cdcRow.withRight(changelogMetadata(task));

    CloseableIterable<InternalRow> rows = openChangelogScanTask(task);
    CloseableIterable<InternalRow> cdcRows = CloseableIterable.transform(rows, cdcRow::withLeft);

    return cdcRows.iterator();
  }

  private static InternalRow changelogMetadata(ChangelogScanTask task) {
    InternalRow metadataRow = new GenericInternalRow(3);

    metadataRow.update(0, UTF8String.fromString(task.operation().name()));
    metadataRow.update(1, task.changeOrdinal());
    metadataRow.update(2, task.commitSnapshotId());

    return metadataRow;
  }

  private CloseableIterable<InternalRow> openChangelogScanTask(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      return openDeletedRowsScanTask((DeletedRowsScanTask) task);

    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  CloseableIterable<InternalRow> openAddedRowsScanTask(AddedRowsScanTask task) {
    String filePath = task.file().location();
    SparkDeleteFilter deletes = new SparkDeleteFilter(filePath, task.deletes(), counter(), true);
    Schema readSchema = deletes.requiredSchema();
    return expectedRows(deletes.filter(rows(task, readSchema)), readSchema);
  }

  private CloseableIterable<InternalRow> openDeletedRowsScanTask(DeletedRowsScanTask task) {
    String filePath = task.file().location();
    SparkDeleteFilter addedDeletes =
        new SparkDeleteFilter(filePath, task.addedDeletes(), counter(), true);
    SparkDeleteFilter existingDeletes =
        new SparkDeleteFilter(filePath, task.existingDeletes(), counter(), true);
    Schema readSchema = addedDeletes.requiredSchema();
    int posColumn = readSchema.columns().indexOf(MetadataColumns.ROW_POSITION);
    PositionDeleteIndex addedPositions = addedDeletes.deletedRowPositions();
    PositionDeleteIndex existingPositions = existingDeletes.deletedRowPositions();

    CloseableIterable<InternalRow> deletedRows =
        CloseableIterable.filter(
            rows(task, readSchema),
            row -> {
              long pos = row.getLong(posColumn);
              return addedPositions.isDeleted(pos)
                  && (existingPositions == null || !existingPositions.isDeleted(pos));
            });

    return expectedRows(deletedRows, readSchema);
  }

  private CloseableIterable<InternalRow> openDeletedDataFileScanTask(DeletedDataFileScanTask task) {
    String filePath = task.file().location();
    SparkDeleteFilter deletes =
        new SparkDeleteFilter(filePath, task.existingDeletes(), counter(), true);
    Schema readSchema = deletes.requiredSchema();
    return expectedRows(deletes.filter(rows(task, readSchema)), readSchema);
  }

  // drops the columns that the delete filter appended so the changelog metadata follows the
  // expected columns
  private CloseableIterable<InternalRow> expectedRows(
      CloseableIterable<InternalRow> rows, Schema readSchema) {
    int expectedColumns = expectedSchema().columns().size();
    if (readSchema.columns().size() == expectedColumns) {
      return rows;
    }

    List<Object> ordinals =
        IntStream.range(0, expectedColumns).boxed().collect(Collectors.toList());
    ProjectingInternalRow projection =
        new ProjectingInternalRow(
            SparkSchemaUtil.convert(expectedSchema()),
            JavaConverters.asScala(ordinals).toIndexedSeq());

    return CloseableIterable.transform(
        rows,
        row -> {
          projection.project(row);
          return projection;
        });
  }

  private CloseableIterable<InternalRow> rows(ContentScanTask<DataFile> task, Schema readSchema) {
    Map<Integer, ?> idToConstant = constantsMap(task, readSchema);

    String filePath = task.file().location();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile location = getInputFile(filePath);
    Preconditions.checkNotNull(location, "Could not find InputFile");
    return newIterable(
        location,
        task.file().format(),
        task.start(),
        task.length(),
        task.residual(),
        readSchema,
        idToConstant);
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return addedRowsScanTaskFiles((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      return deletedRowsScanTaskFiles((DeletedRowsScanTask) task);

    } else if (task instanceof DeletedDataFileScanTask) {
      return deletedDataFileScanTaskFiles((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  private static Stream<ContentFile<?>> deletedRowsScanTaskFiles(DeletedRowsScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> addedDeletes = task.addedDeletes();
    List<DeleteFile> existingDeletes = task.existingDeletes();
    return Stream.concat(
        Stream.of(file), Stream.concat(addedDeletes.stream(), existingDeletes.stream()));
  }

  private static Stream<ContentFile<?>> deletedDataFileScanTaskFiles(DeletedDataFileScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> existingDeletes = task.existingDeletes();
    return Stream.concat(Stream.of(file), existingDeletes.stream());
  }

  private static Stream<ContentFile<?>> addedRowsScanTaskFiles(AddedRowsScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> deletes = task.deletes();
    return Stream.concat(Stream.of(file), deletes.stream());
  }
}
