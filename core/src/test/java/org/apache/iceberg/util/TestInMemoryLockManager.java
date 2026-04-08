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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 5)
public class TestInMemoryLockManager {

  private LockManagers.InMemoryLockManager lockManager;
  private String lockEntityId;
  private String ownerId;

  @BeforeEach
  public void before() {
    lockEntityId = UUID.randomUUID().toString();
    ownerId = UUID.randomUUID().toString();
    lockManager = new LockManagers.InMemoryLockManager(Maps.newHashMap());
  }

  @AfterEach
  public void after() throws Exception {
    lockManager.close();
  }

  @Test
  public void testAcquireOnceSingleProcess() {
    lockManager.acquireOnce(lockEntityId, ownerId);
    assertThatThrownBy(() -> lockManager.acquireOnce(lockEntityId, ownerId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Lock for")
        .hasMessageContaining("currently held by")
        .hasMessageContaining("expiration");
  }

  @Test
  public void testAcquireOnceMultiProcesses() {
    List<Boolean> results =
        IntStream.range(0, 10)
            .parallel()
            .mapToObj(
                i -> {
                  try {
                    lockManager.acquireOnce(lockEntityId, ownerId);
                    return true;
                  } catch (IllegalStateException e) {
                    return false;
                  }
                })
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("only 1 thread should have acquired the lock")
        .isOne();
  }

  @Test
  public void testReleaseAndAcquire() {
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.release(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.acquire(lockEntityId, ownerId))
        .as("acquire after release should succeed")
        .isTrue();
  }

  @Test
  public void testReleaseWithWrongOwner() {
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.release(lockEntityId, UUID.randomUUID().toString()))
        .as("should return false if ownerId is wrong")
        .isFalse();
  }

  @Test
  public void testAcquireSingleProcess() throws Exception {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "2000"));
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    String oldOwner = ownerId;

    CompletableFuture.supplyAsync(
        () -> {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          assertThat(lockManager.release(lockEntityId, oldOwner)).isTrue();
          return null;
        });

    ownerId = UUID.randomUUID().toString();
    long start = System.currentTimeMillis();
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(System.currentTimeMillis() - start)
        .as("should succeed after 200ms")
        .isGreaterThanOrEqualTo(200);
  }

  @Test
  public void testAcquireMultiProcessAllSucceed() {
    lockManager.initialize(ImmutableMap.of(CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500"));
    long start = System.currentTimeMillis();
    List<Boolean> results =
        IntStream.range(0, 3)
            .parallel()
            .mapToObj(
                i -> {
                  String owner = UUID.randomUUID().toString();
                  boolean succeeded = lockManager.acquire(lockEntityId, owner);
                  if (succeeded) {
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                    assertThat(lockManager.release(lockEntityId, owner)).isTrue();
                  }
                  return succeeded;
                })
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("all lock acquire should succeed sequentially")
        .isEqualTo(3);
    assertThat(System.currentTimeMillis() - start)
        .as("must take more than 3 seconds")
        .isGreaterThanOrEqualTo(3000);
  }

  @Test
  public void testAcquireMultiProcessOnlyOneSucceed() {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS, "100",
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "2000"));

    List<Boolean> results =
        IntStream.range(0, 3)
            .parallel()
            .mapToObj(i -> lockManager.acquire(lockEntityId, ownerId))
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("only 1 thread should have acquired the lock")
        .isOne();
  }

  /**
   * Regression test for the race condition where one catalog's close() shuts down the shared static
   * scheduler while another catalog is still using it.
   *
   * <p>The real race: a thread holds a reference to the scheduler (obtained via scheduler()) and
   * then another thread shuts it down. When the first thread calls scheduleAtFixedRate it gets a
   * RejectedExecutionException. The fix adds an isShutdown() guard to scheduler() so it recreates
   * the executor when it has been terminated.
   *
   * <p>This test verifies the guard by shutting down the shared scheduler directly (leaving the
   * field non-null but terminated) and then requiring that a subsequent acquire() still succeeds.
   */
  @Test
  public void testAcquireSucceedsWhenSharedSchedulerWasShutDown() {
    // Prime the shared scheduler (equivalent to another catalog already running)
    ScheduledExecutorService sharedScheduler = lockManager.scheduler();

    // Simulate the scheduler being shut down externally (e.g., by old close() behaviour of
    // another catalog) without nullifying the field, leaving a non-null but terminated reference.
    sharedScheduler.shutdownNow();
    assertThat(sharedScheduler.isShutdown()).isTrue();

    // Without the isShutdown() guard in scheduler(), lockManager.acquire() would:
    //   1. call scheduler() → scheduler != null → return the shut-down executor
    //   2. call scheduleAtFixedRate on a terminated executor → RejectedExecutionException
    // With the fix, scheduler() detects isShutdown() and transparently recreates the executor.
    assertThat(lockManager.acquire(lockEntityId, ownerId))
        .as("acquire must succeed even when the shared scheduler was previously shut down")
        .isTrue();
  }

  /**
   * Tests the {@code scheduler == null} branch of the double-checked locking guard in {@code
   * scheduler()}.
   *
   * <p>The scheduler field is static; setting it to null via reflection simulates a cold start (no
   * prior scheduler has been created). {@code acquire()} must still succeed, proving that {@code
   * scheduler()} correctly initializes the executor from null.
   */
  @Test
  public void testAcquireSucceedsWhenSharedSchedulerIsNull() throws Exception {
    Field schedulerField = LockManagers.BaseLockManager.class.getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    Field schedulerPoolField = LockManagers.BaseLockManager.class.getDeclaredField("schedulerPool");
    schedulerPoolField.setAccessible(true);

    // Null out the static scheduler and pool to simulate a cold-start (scheduler == null branch)
    schedulerField.set(null, null);
    schedulerPoolField.set(null, null);
    assertThat(schedulerField.get(null)).isNull();

    assertThat(lockManager.acquire(lockEntityId, ownerId))
        .as("acquire must succeed when the shared scheduler is null (cold start)")
        .isTrue();
  }

  /**
   * Verifies that when a later catalog instance requires more heartbeat threads than the current
   * shared pool was sized with, the pool is expanded dynamically rather than leaving the instance
   * under-provisioned.
   */
  @Test
  public void testSchedulerPoolExpandsForHigherHeartbeatThreadCount() throws Exception {
    Field schedulerPoolField = LockManagers.BaseLockManager.class.getDeclaredField("schedulerPool");
    schedulerPoolField.setAccessible(true);

    // Warm up the shared pool with the default thread count (4).
    lockManager.scheduler();
    java.util.concurrent.ScheduledThreadPoolExecutor pool =
        (java.util.concurrent.ScheduledThreadPoolExecutor) schedulerPoolField.get(null);
    int initialSize = pool.getCorePoolSize();

    // Create a second instance that requests more threads than the current pool has.
    int largerThreadCount = initialSize + 4;
    LockManagers.InMemoryLockManager highThreadManager =
        new LockManagers.InMemoryLockManager(
            ImmutableMap.of(
                CatalogProperties.LOCK_HEARTBEAT_THREADS, String.valueOf(largerThreadCount)));

    highThreadManager.scheduler();

    assertThat(pool.getCorePoolSize())
        .as("pool must grow to accommodate the larger heartbeat-threads requirement")
        .isGreaterThanOrEqualTo(largerThreadCount);

    highThreadManager.close();
  }
}
