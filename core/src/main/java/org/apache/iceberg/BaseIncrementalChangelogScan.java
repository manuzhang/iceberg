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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  BaseIncrementalChangelogScan(Table table) {
    this(table, table.schema(), TableScanContext.empty());
  }

  private BaseIncrementalChangelogScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  protected IncrementalChangelogScan newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalChangelogScan(newTable, newSchema, newContext);
  }

  @Override
  protected CloseableIterable<ChangelogScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);

    if (changelogSnapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    CloseableIterable<ChangelogScanTask> dataFileTasks = planDataFileTasks(changelogSnapshots);
    CloseableIterable<ChangelogScanTask> deletedRowsTasks =
        planDeletedRowsTasks(changelogSnapshots);
    return CloseableIterable.concat(ImmutableList.of(dataFileTasks, deletedRowsTasks));
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ChangelogScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(
        planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  // builds a collection of changelog snapshots (oldest to newest)
  // the order of the snapshots is important as it is used to determine change ordinals
  private Deque<Snapshot> orderedChangelogSnapshots(Long fromIdExcl, long toIdIncl) {
    Deque<Snapshot> changelogSnapshots = new ArrayDeque<>();

    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table(), toIdIncl, fromIdExcl)) {
      if (!snapshot.operation().equals(DataOperations.REPLACE)) {
        changelogSnapshots.addFirst(snapshot);
      }
    }

    return changelogSnapshots;
  }

  private Set<Long> toSnapshotIds(Collection<Snapshot> snapshots) {
    return snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
  }

  private CloseableIterable<ChangelogScanTask> planDataFileTasks(Deque<Snapshot> snapshots) {
    Set<Long> changelogSnapshotIds = toSnapshotIds(snapshots);

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(snapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.plan(new CreateDataFileChangeTasks(snapshots));
  }

  private CloseableIterable<ChangelogScanTask> planDeletedRowsTasks(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(snapshots);
    ImmutableList.Builder<CloseableIterable<ChangelogScanTask>> snapshotTasks =
        ImmutableList.builder();

    for (Snapshot snapshot : snapshots) {
      List<ManifestFile> deleteManifests = snapshot.deleteManifests(table().io());
      if (deleteManifests.isEmpty()) {
        continue;
      }

      SnapshotChanges.Builder changesBuilder =
          SnapshotChanges.builderFor(snapshot, table().io(), table().specs());
      if (shouldPlanWithExecutor()) {
        changesBuilder.executeWith(planExecutor());
      }

      DeleteFileIndex addedDeletes =
          DeleteFileIndex.builderFor(changesBuilder.build().addedDeleteFiles())
              .specsById(table().specs())
              .build();

      if (addedDeletes.isEmpty()) {
        continue;
      }

      RowIdDeleteIndex rowIdDeletes = null;
      if (table().formatVersion() >= 3) {
        rowIdDeletes = RowIdDeleteIndex.fromDeleteFiles(addedDeletes.referencedDeleteFiles());
        if (rowIdDeletes.isEmpty()) {
          continue;
        }
      }

      DeleteFileIndex existingDeletes = existingDeletesIndex(snapshot);

      ManifestGroup manifestGroup =
          new ManifestGroup(table().io(), snapshot.dataManifests(table().io()), ImmutableList.of())
              .specsById(table().specs())
              .caseSensitive(isCaseSensitive())
              .select(scanColumns())
              .filterData(filter())
              .ignoreDeleted()
              .columnsToKeepStats(columnsToKeepStats());

      if (table().formatVersion() >= 3) {
        Set<String> referencedDataFiles = rowIdDeletes.referencedDataFiles();
        manifestGroup =
            manifestGroup.filterManifestEntries(
                entry -> referencedDataFiles.contains(entry.file().location().toString()));
      }

      if (shouldIgnoreResiduals()) {
        manifestGroup = manifestGroup.ignoreResiduals();
      }

      if (shouldPlanWithExecutor()) {
        manifestGroup = manifestGroup.planWith(planExecutor());
      }

      int changeOrdinal = snapshotOrdinals.get(snapshot.snapshotId());
      snapshotTasks.add(
          manifestGroup.plan(
              new CreateDeletedRowsTasks(
                  changeOrdinal,
                  snapshot.snapshotId(),
                  existingDeletes,
                  addedDeletes,
                  rowIdDeletes)));
    }

    return CloseableIterable.concat(snapshotTasks.build());
  }

  private DeleteFileIndex existingDeletesIndex(Snapshot snapshot) {
    Long parentSnapshotId = snapshot.parentId();
    if (parentSnapshotId == null) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).specsById(table().specs()).build();
    }

    Snapshot parentSnapshot = table().snapshot(parentSnapshotId);
    if (parentSnapshot == null) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).specsById(table().specs()).build();
    }

    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(table().io(), parentSnapshot.deleteManifests(table().io()))
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .filterData(filter());

    if (shouldPlanWithExecutor()) {
      builder = builder.planWith(planExecutor());
    }

    if (shouldIgnoreResiduals()) {
      builder = builder.ignoreResiduals();
    }

    return builder.build();
  }

  private static Map<Long, Integer> computeSnapshotOrdinals(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();

    int ordinal = 0;

    for (Snapshot snapshot : snapshots) {
      snapshotOrdinals.put(snapshot.snapshotId(), ordinal++);
    }

    return snapshotOrdinals;
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

    private final Map<Long, Integer> snapshotOrdinals;

    CreateDataFileChangeTasks(Deque<Snapshot> snapshots) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            int changeOrdinal = snapshotOrdinals.get(commitSnapshotId);
            DataFile dataFile = entry.file().copy(context.shouldKeepStats());

            switch (entry.status()) {
              case ADDED:
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }
  }

  private static class CreateDeletedRowsTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final int changeOrdinal;
    private final long commitSnapshotId;
    private final DeleteFileIndex existingDeleteIndex;
    private final DeleteFileIndex addedDeleteIndex;
    private final RowIdDeleteIndex rowIdDeleteIndex;

    CreateDeletedRowsTasks(
        int changeOrdinal,
        long commitSnapshotId,
        DeleteFileIndex existingDeleteIndex,
        DeleteFileIndex addedDeleteIndex,
        RowIdDeleteIndex rowIdDeleteIndex) {
      this.changeOrdinal = changeOrdinal;
      this.commitSnapshotId = commitSnapshotId;
      this.existingDeleteIndex = existingDeleteIndex;
      this.addedDeleteIndex = addedDeleteIndex;
      this.rowIdDeleteIndex = rowIdDeleteIndex;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {
      CloseableIterable<ChangelogScanTask> tasks =
          CloseableIterable.transform(
              entries,
              entry -> {
                DeleteFile[] addedDeletes =
                    rowIdDeleteIndex != null
                        ? rowIdDeleteIndex.forDataFile(entry.file())
                        : addedDeleteIndex.forEntry(entry);
                if (addedDeletes.length == 0) {
                  return null;
                }

                DeleteFile[] existingDeletes = existingDeleteIndex.forEntry(entry);
                DataFile dataFile = entry.file().copy(context.shouldKeepStats());
                return new BaseDeletedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    addedDeletes,
                    existingDeletes,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());
              });

      return CloseableIterable.filter(tasks, Objects::nonNull);
    }
  }
}
