<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Spark 4.1 Changelog Plan for Row Deletes and Row Lineage

## Scope

Support additive row-level changelog generation for Spark 4.1 and use row lineage to compute updates when explicit identifier fields are not available.

This breaks down into two separate deliverables:

1. Add a new row-changelog scan and task path for position-delete-based changes, including DVs.
2. Add a new Spark procedure, tentatively `create_row_changelog_view`, that uses row lineage for update pairing when identifier columns are not provided.

Non-goals:

- Do not change `BaseIncrementalChangelogScan`.
- Do not change `CreateChangelogViewProcedure`.
- Do not change the current behavior of `table.changes` or `create_changelog_view`.

## Phase 1: Additive Row Changelog Scan

### Goal

Add a dedicated row-changelog scan path that can plan and read row-delete tasks for position deletes and DVs without modifying the existing incremental changelog scan.

Equality deletes are out of scope for this work because row lineage does not support them.

### Core Surface

Primary files:

- `api/src/main/java/org/apache/iceberg/Table.java`
- `api/src/main/java/org/apache/iceberg/IncrementalRowChangelogScan.java`
- `core/src/main/java/org/apache/iceberg/BaseTable.java`
- `core/src/main/java/org/apache/iceberg/SerializableTable.java`
- `core/src/main/java/org/apache/iceberg/BaseIncrementalRowChangelogScan.java`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`
- `core/src/main/java/org/apache/iceberg/SnapshotChanges.java`

Work:

- Add a default `Table.newIncrementalRowChangelogScan()` method so the new capability is opt-in and additive.
- Implement the new scan in `BaseTable` and `SerializableTable`.
- Keep `BaseIncrementalChangelogScan` unchanged so `table.changes` behavior does not change.
- Keep the existing task planning for:
  - `AddedRowsScanTask`
  - `DeletedDataFileScanTask`
- Add row-delete task planning in `BaseIncrementalRowChangelogScan`.
- For each changelog snapshot in the new scan:
  - Build `addedDeletes` from position delete files and DVs added in that snapshot.
  - Build `existingDeletes` from position delete files and DVs added in earlier snapshots in the requested changelog window or current branch history.
  - Enumerate live data files in that snapshot.
  - Use `DeleteFileIndex.forDataFile(...)` to determine whether the new delete files apply to each candidate data file.
  - Emit `BaseDeletedRowsScanTask` when at least one new delete file applies.
- Restrict the new scan to the additive task path only; existing changelog scans continue to reject delete manifests.

### Candidate File Enumeration

Primary file:

- `core/src/main/java/org/apache/iceberg/FindFiles.java`

Initial implementation strategy:

- Use `FindFiles.in(table).inSnapshot(snapshotId)` to enumerate live data files in the snapshot being processed.
- Rely on `DeleteFileIndex` for:
  - sequence number filtering
  - partition pruning
  - file-scoped position delete and DV matching

Expected tradeoff:

- That is acceptable for the first implementation as long as task planning is correct and bounded by existing snapshot scan behavior.

### Spark Wiring

Primary files:

- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkRowChangelogTable.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkRowChangelogScanBuilder.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/ChangelogRowReader.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java`

Work:

- Add a dedicated Spark relation for the new row-changelog scan instead of changing `SparkChangelogTable`.
- Implement the `DeletedRowsScanTask` branch in `ChangelogRowReader` instead of throwing `UnsupportedOperationException`.
- Add a helper in `DeleteFilter` that returns only rows deleted by a delete set, not surviving rows.
- Reader order for `DeletedRowsScanTask`:
  1. Read the base data file.
  2. Apply `existingDeletes` first so already-deleted rows do not reappear.
  3. Return only rows deleted by `addedDeletes`.
- Update `referencedFiles(...)` in `ChangelogRowReader` to include both `addedDeletes` and `existingDeletes`.
- Expose `_row_id` and `_last_updated_sequence_number` as metadata columns only on the new relation so the existing `changes` table schema remains unchanged.

### Test Coverage

Primary files:

- `core/src/test/java/org/apache/iceberg/TestBaseIncrementalRowChangelogScan.java`
- `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestChangelogReader.java`
- `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkRowChangelogTable.java`
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestChangelogTable.java`

Tests to add:

- New scan planner emits `DeletedRowsScanTask` for:
  - file-scoped position deletes
  - DVs
  - mixed new and existing delete files
- Reader returns only newly deleted rows and excludes rows deleted earlier.
- New Spark row-changelog relation returns delete records for MOR row deletes on v3 tables.
- Existing `table.changes` behavior remains unchanged for the same snapshots.

## Phase 2: New Procedure for Row Lineage Pairing

### Goal

Add a separate Spark procedure for row-lineage-aware changelog views without modifying `CreateChangelogViewProcedure`.

### Spark Surface

Primary files:

- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/procedures/CreateRowChangelogViewProcedure.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/procedures/SparkProcedures.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkRowChangelogTable.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/ChangelogIterator.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/ComputeUpdateIterator.java`

Preferred approach:

- Add a new procedure, tentatively `create_row_changelog_view`, that reads from the dedicated row-changelog relation rather than `SparkChangelogTable`.
- Keep `CreateChangelogViewProcedure` unchanged.
- In the new procedure, when:
  - `compute_updates = true`
  - explicit `identifier_columns` are not provided
  - the table supports row lineage
  then use `_row_id` internally as the pairing key.

Why this is the lowest-risk Spark 4.1 approach:

- It keeps the current procedure semantics intact for existing users.
- It allows the new procedure to reuse most of the repartition and sort pipeline without forcing new behavior into the current procedure.
- It keeps the actual pairing logic in `ComputeUpdateIterator` unchanged because `_row_id` can be treated like any other identifier field.

### Procedure Behavior

Work:

- Teach the new procedure to select `_row_id` when needed for update computation.
- If `_row_id` was only added for internal pairing, drop it before materializing the final temp view.
- Preserve the current behavior when identifier columns are explicitly supplied.
- Preserve the current `create_changelog_view` semantics unchanged.

### Test Coverage

Primary files:

- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestCreateRowChangelogViewProcedure.java`
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestCreateChangelogViewProcedure.java`
- `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/TestChangelogIterator.java`

Tests to add:

- v3 MOR table without identifier fields can still produce update images through the new procedure.
- Pairing happens by `_row_id` rather than user data columns.
- Iterator behavior is stable when `_row_id` is the only identifier field.
- Existing `create_changelog_view` behavior remains unchanged.

## Suggested Delivery Order

### PR 1

Implement the new row-changelog scan, task planning, and Spark relation end to end.

Why first:

- It establishes the additive data source for the new procedure.
- It is independent of whether updates are paired by identifiers or row lineage.
- It gives a stable base for procedure-level update pairing tests.

### PR 2

Add the new row-lineage-aware procedure for Spark 4.1 changelog views.

Why second:

- It is a Spark-only enhancement on top of a working additive row-changelog stream.
- It is easier to review once the new row-delete-driven relation already exists.

## Open Risks

- Candidate file enumeration may still expand planner work on large snapshots even without equality deletes.
- DVs should behave like position deletes, but tests should verify there is no mismatch between DV-specific semantics and `DeleteFileIndex`.
- The new procedure should not leak `_row_id` into user-visible `SELECT *` results unless explicitly requested.
- The new scan and procedure may duplicate some existing changelog plumbing; factor shared helpers only where it reduces maintenance without coupling the old and new paths together.

## Success Criteria

- `table.changes` and `create_changelog_view` retain their existing behavior.
- The new row-changelog path returns row-level deletes for position-delete-based snapshots, including DVs, in Spark 4.1.
- The new procedure can compute update images on v3 tables using row lineage when no identifier fields are configured.
