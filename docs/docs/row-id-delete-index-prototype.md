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

# Row-ID Delete Delta Index (Prototype Plan)

This document sketches a minimal prototype for reducing incremental changelog planning cost by
materializing deleted row IDs per snapshot.

## Goals

- Work first for **format v3+ tables** with row lineage.
- Preserve existing changelog semantics (`changeOrdinal`, `commitSnapshotId`).
- Preserve compatibility with current `DeletedRowsScanTask` behavior.
- Avoid API breakage and keep fallback to existing delete planning.

## Non-goals

- Changing table format semantics in the first prototype.
- Replacing all delete planning paths at once.
- Supporting equality deletes in the initial milestone.

## Why this is viable

- Row lineage metadata already exists (`_row_id`, `_last_updated_sequence_number`).
- Snapshot metadata already exposes row lineage fields (`firstRowId`, `addedRows`).
- Current changelog delete planning is snapshot-based and can swap in an alternative source for
  row-level delete deltas while preserving task ordering.

## Prototype data model

Introduce an internal, snapshot-scoped sidecar artifact:

`row-delete-delta-<snapshot-id>.avro`

Fields:

- `snapshot_id` (long)
- `parent_snapshot_id` (long)
- `referenced_data_file` (string)
- `delete_file_path` (string)
- `deleted_row_ids` (binary, compressed bitmap)
- `deleted_row_count` (long)

The sidecar stores only row IDs deleted by that snapshot (delta, not cumulative state).

## Commit-time update protocol

For each committed snapshot `S` that adds delete vectors:

1. Collect added DVs in `S`.
2. Resolve referenced data files (DV => referenced data path).
3. For each referenced file:
   - translate deleted row positions to row IDs using `first_row_id + position`.
   - write compressed row-ID bitmap into the sidecar record.
4. Publish sidecar location in snapshot summary metadata.

### Failure and atomicity

- Sidecar is written before snapshot commit finalization.
- Snapshot reference to sidecar is atomic with snapshot publication.
- On sidecar read failure, planner falls back to current delete-index path.

## Read-time protocol (incremental changelog)

Given `(fromSnapshotExclusive, toSnapshotInclusive)`:

1. Build ordered changelog snapshots and ordinals.
2. For each snapshot `S` in range:
   - if a sidecar is present, load deleted row-ID deltas from sidecar;
   - build deleted-row changelog tasks from referenced data files;
   - assign `changeOrdinal` and `commitSnapshotId` exactly as today.
3. If sidecar is absent/unreadable, use existing `DeleteFileIndex` planning path.

## Rollout plan

### Phase 1 (MVP)

- v3+ only, DV-only input.
- sidecar write/read behind a table property flag (default off).
- fallback enabled by default.

### Phase 2

- Include position deletes in v2/v3 tables.
- support mixed delete types by materializing row-ID deltas.

### Phase 3

- Optional cumulative compaction artifacts (prefix snapshots) for faster long-range planning.
- optional engine-specific reader optimizations that consume row-ID bitmaps directly.

## Validation plan

- Unit tests for sidecar write/read and fallback behavior.
- Incremental changelog determinism tests for ordinals and snapshot IDs.
- Cross-branch correctness tests (branch-local snapshots and sidecar references).
- Fault-injection tests (missing/corrupt sidecar should not change correctness).
