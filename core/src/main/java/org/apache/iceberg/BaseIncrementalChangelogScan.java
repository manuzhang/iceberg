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
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.SortedMerge;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];
  private static final String UNSUPPORTED_DELETE_FILES_MESSAGE =
      "Only deletion vectors in v3 tables are supported in changelog scans";

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
    Map<String, DeleteFile> activeDVs = buildActiveDVs(beforeFirstSnapshotId(changelogSnapshots));
    Map<Long, DeleteFileChanges> dvChangesBySnapshot = buildDVChangesBySnapshot(changelogSnapshots);

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
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

    Map<Long, Map<String, DeleteFile>> activeDVsBySnapshot =
        buildActiveDVsBySnapshot(changelogSnapshots, activeDVs, dvChangesBySnapshot);

    CloseableIterable<ChangelogScanTask> dataFileTasks =
        manifestGroup.plan(
            new CreateDataFileChangeTasks(
                changelogSnapshots, dvChangesBySnapshot, activeDVsBySnapshot));
    CloseableIterable<ChangelogScanTask> deletedRowsTasks =
        planDeletedRowsTasks(
            changelogSnapshots, changelogSnapshotIds, activeDVs, dvChangesBySnapshot);

    Comparator<ChangelogScanTask> byOrdinal =
        Comparator.comparing(ChangelogScanTask::changeOrdinal)
            .thenComparing(ChangelogScanTask::commitSnapshotId);

    return new SortedMerge<>(byOrdinal, ImmutableList.of(dataFileTasks, deletedRowsTasks));
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

  private Map<String, DeleteFile> buildActiveDVs(Long fromSnapshotIdExclusive) {
    if (fromSnapshotIdExclusive == null) {
      return Maps.newHashMap();
    }

    Snapshot fromSnapshot = table().snapshot(fromSnapshotIdExclusive);
    Preconditions.checkState(
        fromSnapshot != null, "Cannot find starting snapshot: %s", fromSnapshotIdExclusive);

    Map<String, DeleteFile> activeDVs = Maps.newHashMap();
    for (ManifestFile manifest : fromSnapshot.deleteManifests(table().io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table().io(), table().specs())) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          if (entry.status() != ManifestEntry.Status.DELETED) {
            DeleteFile dv = copySupportedDV(entry.file());
            activeDVs.put(dv.referencedDataFile(), dv);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
      }
    }

    return activeDVs;
  }

  private Map<Long, DeleteFileChanges> buildDVChangesBySnapshot(Deque<Snapshot> snapshots) {
    Map<Long, DeleteFileChanges> changesBySnapshot = Maps.newHashMap();
    for (Snapshot snapshot : snapshots) {
      changesBySnapshot.put(snapshot.snapshotId(), loadDVChanges(snapshot));
    }

    return changesBySnapshot;
  }

  private DeleteFileChanges loadDVChanges(Snapshot snapshot) {
    List<DeleteFile> addedDVs = Lists.newArrayList();
    List<DeleteFile> removedDVs = Lists.newArrayList();

    for (ManifestFile manifest : snapshot.deleteManifests(table().io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table().io(), table().specs())) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          if (entry.snapshotId().equals(snapshot.snapshotId())) {
            DeleteFile dv = copySupportedDV(entry.file());

            if (entry.status() == ManifestEntry.Status.ADDED) {
              addedDVs.add(dv);
            } else if (entry.status() == ManifestEntry.Status.DELETED) {
              removedDVs.add(dv);
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
      }
    }

    return new DeleteFileChanges(addedDVs, removedDVs);
  }

  private DeleteFile copySupportedDV(DeleteFile file) {
    if (!isSupportedDV(file)) {
      throw new UnsupportedOperationException(UNSUPPORTED_DELETE_FILES_MESSAGE);
    }

    return ContentFileUtil.copy(file, true, Set.of(MetadataColumns.DELETE_FILE_PATH.fieldId()));
  }

  private boolean isSupportedDV(DeleteFile file) {
    return TableUtil.formatVersion(table()) >= 3
        && file.content() == FileContent.POSITION_DELETES
        && ContentFileUtil.isDV(file);
  }

  private Map<Long, Map<String, DeleteFile>> buildActiveDVsBySnapshot(
      Deque<Snapshot> snapshots,
      Map<String, DeleteFile> activeDVs,
      Map<Long, DeleteFileChanges> dvChangesBySnapshot) {
    Map<Long, Map<String, DeleteFile>> activeDVsBySnapshot = Maps.newHashMap();
    Map<String, DeleteFile> currentDVs = Maps.newHashMap(activeDVs);

    for (Snapshot snapshot : snapshots) {
      activeDVsBySnapshot.put(snapshot.snapshotId(), Maps.newHashMap(currentDVs));

      DeleteFileChanges changes = dvChangesBySnapshot.get(snapshot.snapshotId());
      for (DeleteFile removedDV : changes.removedDVs()) {
        currentDVs.remove(removedDV.referencedDataFile());
      }

      for (DeleteFile addedDV : changes.addedDVs()) {
        currentDVs.put(addedDV.referencedDataFile(), addedDV);
      }
    }

    return activeDVsBySnapshot;
  }

  private CloseableIterable<ChangelogScanTask> planDeletedRowsTasks(
      Deque<Snapshot> changelogSnapshots,
      Set<Long> changelogSnapshotIds,
      Map<String, DeleteFile> activeDVsAtStart,
      Map<Long, DeleteFileChanges> dvChangesBySnapshot) {
    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);
    List<ChangelogScanTask> tasks = Lists.newArrayList();
    Map<String, ManifestEntry.Status> currentSnapshotFileStatuses =
        buildCurrentSnapshotFileStatuses(changelogSnapshots, changelogSnapshotIds);
    Map<String, DeleteFile> activeDVs = Maps.newHashMap(activeDVsAtStart);

    for (Snapshot snapshot : changelogSnapshots) {
      long snapshotId = snapshot.snapshotId();
      DeleteFileChanges changes = dvChangesBySnapshot.get(snapshotId);

      if (changes.addedDVs().isEmpty()) {
        applyDVChanges(activeDVs, changes);
        continue;
      }

      planDeletedRowsTasks(
          snapshot,
          snapshotOrdinals.get(snapshotId),
          currentSnapshotFileStatuses,
          activeDVs,
          changes.addedDVs(),
          tasks);
      applyDVChanges(activeDVs, changes);
    }

    return CloseableIterable.withNoopClose(tasks);
  }

  private Long beforeFirstSnapshotId(Deque<Snapshot> changelogSnapshots) {
    Snapshot firstSnapshot = changelogSnapshots.peekFirst();
    return firstSnapshot != null ? firstSnapshot.parentId() : null;
  }

  private void applyDVChanges(Map<String, DeleteFile> activeDVs, DeleteFileChanges changes) {
    for (DeleteFile removedDV : changes.removedDVs()) {
      activeDVs.remove(removedDV.referencedDataFile());
    }

    for (DeleteFile addedDV : changes.addedDVs()) {
      activeDVs.put(addedDV.referencedDataFile(), addedDV);
    }
  }

  private Map<String, ManifestEntry.Status> buildCurrentSnapshotFileStatuses(
      Deque<Snapshot> changelogSnapshots, Set<Long> changelogSnapshotIds) {
    Map<String, ManifestEntry.Status> fileStatuses = Maps.newHashMap();

    for (Snapshot snapshot : changelogSnapshots) {
      List<ManifestFile> changedDataManifests =
          FluentIterable.from(snapshot.dataManifests(table().io()))
              .filter(manifest -> manifest.snapshotId().equals(snapshot.snapshotId()))
              .toList();

      ManifestGroup changedGroup =
          new ManifestGroup(table().io(), changedDataManifests, ImmutableList.of())
              .specsById(table().specs())
              .caseSensitive(isCaseSensitive())
              .select(scanColumns())
              .filterData(filter())
              .ignoreExisting()
              .columnsToKeepStats(columnsToKeepStats());

      try (CloseableIterable<ManifestEntry<DataFile>> entries = changedGroup.entries()) {
        for (ManifestEntry<DataFile> entry : entries) {
          if (changelogSnapshotIds.contains(entry.snapshotId())) {
            fileStatuses.put(fileStatusKey(snapshot.snapshotId(), entry.file()), entry.status());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read data manifests for snapshot " + snapshot.snapshotId(), e);
      }
    }

    return fileStatuses;
  }

  private String fileStatusKey(long snapshotId, DataFile file) {
    return snapshotId + "#" + file.location();
  }

  private void planDeletedRowsTasks(
      Snapshot snapshot,
      int changeOrdinal,
      Map<String, ManifestEntry.Status> currentSnapshotFileStatuses,
      Map<String, DeleteFile> activeDVs,
      List<DeleteFile> addedDVs,
      List<ChangelogScanTask> tasks) {
    Map<String, DeleteFile> addedDVsByDataFile = Maps.newHashMap();
    for (DeleteFile addedDV : addedDVs) {
      addedDVsByDataFile.put(addedDV.referencedDataFile(), addedDV);
    }

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), snapshot.dataManifests(table().io()), ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    String schemaString = SchemaParser.toJson(schema());
    Map<Integer, String> specStringCache = Maps.newHashMap();
    Map<Integer, ResidualEvaluator> residualCache = Maps.newHashMap();

    try (CloseableIterable<ManifestEntry<DataFile>> entries = manifestGroup.entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        DataFile dataFile = entry.file();
        String dataFilePath = dataFile.location().toString();

        if (!addedDVsByDataFile.containsKey(dataFilePath)
            || currentSnapshotFileStatuses.containsKey(
                fileStatusKey(snapshot.snapshotId(), dataFile))) {
          continue;
        }

        int specId = dataFile.specId();
        String specString =
            specStringCache.computeIfAbsent(
                specId, id -> PartitionSpecParser.toJson(table().specs().get(id)));
        ResidualEvaluator residuals =
            residualCache.computeIfAbsent(
                specId,
                id -> {
                  PartitionSpec spec = table().specs().get(id);
                  return ResidualEvaluator.of(
                      spec,
                      shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter(),
                      isCaseSensitive());
                });

        DeleteFile addedDV = addedDVsByDataFile.get(dataFilePath);
        DeleteFile existingDV = activeDVs.get(dataFilePath);
        DeleteFile[] existingDeletes =
            existingDV != null ? new DeleteFile[] {existingDV} : NO_DELETES;

        tasks.add(
            new BaseDeletedRowsScanTask(
                changeOrdinal,
                snapshot.snapshotId(),
                dataFile.copy(shouldKeepStats()),
                new DeleteFile[] {addedDV},
                existingDeletes,
                schemaString,
                specString,
                residuals));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read data manifests for snapshot " + snapshot.snapshotId(), e);
    }
  }

  private boolean shouldKeepStats() {
    Set<Integer> columns = columnsToKeepStats();
    return columns != null && !columns.isEmpty();
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final Map<Long, Integer> snapshotOrdinals;
    private final Map<Long, DeleteFileChanges> dvChangesBySnapshot;
    private final Map<Long, Map<String, DeleteFile>> activeDVsBySnapshot;

    CreateDataFileChangeTasks(
        Deque<Snapshot> snapshots,
        Map<Long, DeleteFileChanges> dvChangesBySnapshot,
        Map<Long, Map<String, DeleteFile>> activeDVsBySnapshot) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
      this.dvChangesBySnapshot = dvChangesBySnapshot;
      this.activeDVsBySnapshot = activeDVsBySnapshot;
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
                    addedDVsForDataFile(commitSnapshotId, entry.file()),
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    activeDVsForDataFile(commitSnapshotId, entry.file()),
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }

    private DeleteFile[] addedDVsForDataFile(long snapshotId, DataFile dataFile) {
      DeleteFileChanges changes = dvChangesBySnapshot.get(snapshotId);
      if (changes == null || changes.addedDVs().isEmpty()) {
        return NO_DELETES;
      }

      for (DeleteFile dv : changes.addedDVs()) {
        if (dv.referencedDataFile().contentEquals(dataFile.location())) {
          return new DeleteFile[] {dv};
        }
      }

      return NO_DELETES;
    }

    private DeleteFile[] activeDVsForDataFile(long snapshotId, DataFile dataFile) {
      Map<String, DeleteFile> activeDVs = activeDVsBySnapshot.get(snapshotId);
      if (activeDVs == null) {
        return NO_DELETES;
      }

      DeleteFile dv = activeDVs.get(dataFile.location().toString());
      return dv != null ? new DeleteFile[] {dv} : NO_DELETES;
    }
  }

  private static class DeleteFileChanges {
    private final List<DeleteFile> addedDVs;
    private final List<DeleteFile> removedDVs;

    DeleteFileChanges(List<DeleteFile> addedDVs, List<DeleteFile> removedDVs) {
      this.addedDVs = addedDVs;
      this.removedDVs = removedDVs;
    }

    List<DeleteFile> addedDVs() {
      return addedDVs;
    }

    List<DeleteFile> removedDVs() {
      return removedDVs;
    }
  }
}
