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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

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
    Map<Long, DVChanges> dvChangesBySnapshot = loadDVChanges(changelogSnapshots);

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    CloseableIterable<ChangelogScanTask> dataFileTasks =
        newManifestGroup(newDataManifests)
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting()
            .plan(new CreateDataFileChangeTasks(snapshotOrdinals, dvChangesBySnapshot));

    Iterable<CloseableIterable<ChangelogScanTask>> deletedRowsTasks =
        Iterables.transform(
            Iterables.filter(
                changelogSnapshots,
                snapshot -> dvChangesBySnapshot.get(snapshot.snapshotId()).hasAdded()),
            snapshot ->
                planDeletedRowsTasks(
                    snapshot,
                    snapshotOrdinals.get(snapshot.snapshotId()),
                    dvChangesBySnapshot.get(snapshot.snapshotId())));

    return CloseableIterable.concat(
        Iterables.concat(ImmutableList.of(dataFileTasks), deletedRowsTasks));
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

  private static Map<Long, Integer> computeSnapshotOrdinals(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();

    int ordinal = 0;

    for (Snapshot snapshot : snapshots) {
      snapshotOrdinals.put(snapshot.snapshotId(), ordinal++);
    }

    return snapshotOrdinals;
  }

  private ManifestGroup newManifestGroup(Collection<ManifestFile> manifests) {
    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), manifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (manifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup;
  }

  // collects the DVs added and removed by each changelog snapshot and rejects any other delete
  // file that is live in one of them
  private Map<Long, DVChanges> loadDVChanges(Deque<Snapshot> changelogSnapshots) {
    Map<Long, DVChanges> dvChangesBySnapshot = Maps.newHashMap();
    for (Snapshot snapshot : changelogSnapshots) {
      dvChangesBySnapshot.put(snapshot.snapshotId(), new DVChanges());
    }

    Set<ManifestFile> deleteManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.deleteManifests(table().io()))
            .toSet();

    for (ManifestFile manifest : deleteManifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table().io(), table().specs())) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          DVChanges dvChanges = dvChangesBySnapshot.get(entry.snapshotId());
          if (entry.status() == Status.DELETED) {
            if (dvChanges != null) {
              dvChanges.removed(entry.file());
            }
          } else if (entry.status() == Status.ADDED && dvChanges != null) {
            dvChanges.added(entry.file());
          } else {
            checkDV(entry.file());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
      }
    }

    return dvChangesBySnapshot;
  }

  private static void checkDV(DeleteFile file) {
    if (!ContentFileUtil.isDV(file)) {
      throw new UnsupportedOperationException(
          "Only deletion vectors in v3 tables are supported in changelog scans");
    }
  }

  private CloseableIterable<ChangelogScanTask> planDeletedRowsTasks(
      Snapshot snapshot, int changeOrdinal, DVChanges dvChanges) {
    long snapshotId = snapshot.snapshotId();

    return newManifestGroup(snapshot.dataManifests(table().io()))
        .filterManifestEntries(
            entry -> snapshotId != entry.snapshotId() && dvChanges.hasAddedFor(entry.file()))
        .ignoreDeleted()
        .plan(
            (entries, context) ->
                CloseableIterable.transform(
                    entries,
                    entry -> {
                      DataFile dataFile = copy(entry, context);
                      return new BaseDeletedRowsScanTask(
                          changeOrdinal,
                          snapshotId,
                          dataFile,
                          dvChanges.addedFor(dataFile),
                          dvChanges.removedFor(dataFile),
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    }));
  }

  private static DataFile copy(ManifestEntry<DataFile> entry, TaskContext context) {
    return ContentFileUtil.copy(
        entry.file(), context.shouldKeepStats(), context.columnsToKeepStats());
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final Map<Long, Integer> snapshotOrdinals;
    private final Map<Long, DVChanges> dvChangesBySnapshot;

    CreateDataFileChangeTasks(
        Map<Long, Integer> snapshotOrdinals, Map<Long, DVChanges> dvChangesBySnapshot) {
      this.snapshotOrdinals = snapshotOrdinals;
      this.dvChangesBySnapshot = dvChangesBySnapshot;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            int changeOrdinal = snapshotOrdinals.get(commitSnapshotId);
            DVChanges dvChanges = dvChangesBySnapshot.get(commitSnapshotId);
            DataFile dataFile = copy(entry, context);

            switch (entry.status()) {
              case ADDED:
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    dvChanges.addedFor(dataFile),
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    dvChanges.removedFor(dataFile),
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }
  }

  // DVs added and removed by one snapshot, keyed by the data file they reference
  @SuppressWarnings("CollectionUndefinedEquality")
  private static class DVChanges {
    private final CharSequenceMap<DeleteFile> added = CharSequenceMap.create();
    private final CharSequenceMap<DeleteFile> removed = CharSequenceMap.create();

    void added(DeleteFile file) {
      checkDV(file);
      added.put(file.referencedDataFile(), file.copyWithoutStats());
    }

    void removed(DeleteFile file) {
      checkDV(file);
      removed.put(file.referencedDataFile(), file.copyWithoutStats());
    }

    boolean hasAdded() {
      return !added.isEmpty();
    }

    boolean hasAddedFor(DataFile dataFile) {
      return added.containsKey(dataFile.location());
    }

    DeleteFile[] addedFor(DataFile dataFile) {
      return toDeletes(added.get(dataFile.location()));
    }

    DeleteFile[] removedFor(DataFile dataFile) {
      return toDeletes(removed.get(dataFile.location()));
    }

    private static DeleteFile[] toDeletes(DeleteFile dv) {
      return dv != null ? new DeleteFile[] {dv} : NO_DELETES;
    }
  }
}
