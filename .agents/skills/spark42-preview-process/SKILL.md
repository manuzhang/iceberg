---
name: spark42-preview-process
description: Execute the spark4.2-preview branch workflow for Apache Iceberg, including backup branch creation, reset and rebase onto upstream main, spark/v4.2 and spark/v4.1 directory migration, cherry-pick with upstream-first conflict resolution, targeted Spark 4.2 build verification, and cleanup. Use when preparing or re-running the Spark 4.2 preview branch flow.
---

# Spark 4.2 Preview Process

## Overview
Follow this skill to run the full `spark4.2-preview` workflow in a consistent order and validate the result before finalizing.

## Workflow
1. Confirm current branch state and create a backup branch from `spark4.2-preview`.
2. Reset `spark4.2-preview` back by three commits.
3. Fetch latest `upstream/main` and rebase onto it.
4. Remove `spark/v4.2` if present, move `spark/v4.1` to `spark/v4.2`, commit.
5. Copy `spark/v4.2` back to `spark/v4.1`, commit.
6. Cherry-pick the backup branch head.
7. If conflicts occur, resolve with upstream-side changes (ours) when requested.
8. Update `spark/v4.2/build.gradle` so all Spark 4.1 references are Spark 4.2.
9. Run targeted Spark 4.2 assemble tasks.
10. If build passes, amend the latest commit with final `build.gradle` updates.
11. Delete the temporary backup branch.

## Commands
Use this command sequence as the baseline:

```bash
git branch spark4.2-preview-backup spark4.2-preview
git checkout spark4.2-preview
git reset --hard HEAD~3
git fetch upstream main
git rebase upstream/main

rm -rf spark/v4.2
mv spark/v4.1 spark/v4.2
git add -A spark/v4.2 spark/v4.1
git commit -m "Spark: Move 4.1 as 4.2"

cp -R spark/v4.2 spark/v4.1
git add -A spark/v4.2 spark/v4.1
git commit -m "Spark: Copy back 4.2 as 4.1"

git cherry-pick spark4.2-preview-backup
```

If conflict policy is upstream-first:

```bash
git checkout --ours spark/v4.2/build.gradle
git add spark/v4.2/build.gradle
git cherry-pick --continue
```

Build verification:

```bash
./gradlew --no-daemon \
  :iceberg-spark:iceberg-spark-4.2_2.13:assemble \
  :iceberg-spark:iceberg-spark-extensions-4.2_2.13:assemble \
  :iceberg-spark:iceberg-spark-runtime-4.2_2.13:assemble
```

Finalize:

```bash
git add spark/v4.2/build.gradle
git commit --amend --no-edit
git branch -D spark4.2-preview-backup
```
