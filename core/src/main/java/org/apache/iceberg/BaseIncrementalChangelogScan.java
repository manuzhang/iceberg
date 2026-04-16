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

    Set<Long> changelogSnapshotIds = toSnapshotIds(changelogSnapshots);
    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);

    List<ManifestFile> newDeleteManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.deleteManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toList();

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, newDeleteManifests)
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

    CloseableIterable<ChangelogScanTask> dataFileChangeTasks =
        manifestGroup.plan(new CreateDataFileChangeTasks(snapshotOrdinals));
    CloseableIterable<ChangelogScanTask> deleteFileChangeTasks =
        planDeleteFileChanges(changelogSnapshots, snapshotOrdinals);
    return CloseableIterable.concat(ImmutableList.of(dataFileChangeTasks, deleteFileChangeTasks));
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

  private CloseableIterable<ChangelogScanTask> planDeleteFileChanges(
      Deque<Snapshot> snapshots, Map<Long, Integer> snapshotOrdinals) {
    Iterable<CloseableIterable<ChangelogScanTask>> changelogTasks =
        FluentIterable.from(snapshots)
            .transform(snapshot -> planDeleteFileChanges(snapshot, snapshotOrdinals));
    return CloseableIterable.concat(changelogTasks);
  }

  private CloseableIterable<ChangelogScanTask> planDeleteFileChanges(
      Snapshot snapshot, Map<Long, Integer> snapshotOrdinals) {
    List<ManifestFile> snapshotDeleteManifests = snapshot.deleteManifests(table().io());
    List<ManifestFile> addedDeleteManifests =
        FluentIterable.from(snapshotDeleteManifests)
            .filter(manifest -> manifest.snapshotId() == snapshot.snapshotId())
            .toList();

    if (addedDeleteManifests.isEmpty()) {
      return CloseableIterable.empty();
    }

    List<ManifestFile> existingDeleteManifests =
        FluentIterable.from(snapshotDeleteManifests)
            .filter(manifest -> manifest.snapshotId() != snapshot.snapshotId())
            .toList();

    DeleteFileIndex addedDeletes =
        DeleteFileIndex.builderFor(table().io(), addedDeleteManifests)
            .specsById(table().specs())
            .filterData(filter())
            .caseSensitive(isCaseSensitive())
            .build();

    DeleteFileIndex existingDeletes =
        DeleteFileIndex.builderFor(table().io(), existingDeleteManifests)
            .specsById(table().specs())
            .filterData(filter())
            .caseSensitive(isCaseSensitive())
            .build();

    ManifestGroup dataManifestGroup =
        new ManifestGroup(table().io(), snapshot.dataManifests(table().io()), ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      dataManifestGroup = dataManifestGroup.ignoreResiduals();
    }

    if (snapshot.dataManifests(table().io()).size() > 1 && shouldPlanWithExecutor()) {
      dataManifestGroup = dataManifestGroup.planWith(planExecutor());
    }

    CloseableIterable<ChangelogScanTask> tasks =
        dataManifestGroup.plan(
            (entries, context) ->
                CloseableIterable.transform(
                    entries,
                    entry -> {
                      DeleteFile[] addedDeleteFiles = addedDeletes.forEntry(entry);
                      if (addedDeleteFiles.length == 0) {
                        return null;
                      }

                      DeleteFile[] existingDeleteFiles = existingDeletes.forEntry(entry);
                      DataFile dataFile = entry.file().copy(context.shouldKeepStats());
                      return new BaseDeletedRowsScanTask(
                          snapshotOrdinals.get(snapshot.snapshotId()),
                          snapshot.snapshotId(),
                          dataFile,
                          addedDeleteFiles,
                          existingDeleteFiles,
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    }));

    return CloseableIterable.filter(tasks, task -> task != null);
  }

  private Set<Long> toSnapshotIds(Collection<Snapshot> snapshots) {
    return snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
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

    CreateDataFileChangeTasks(Map<Long, Integer> snapshotOrdinals) {
      this.snapshotOrdinals = snapshotOrdinals;
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
}
