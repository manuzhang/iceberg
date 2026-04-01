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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestLockManagers {

  @Test
  public void testLoadDefaultLockManager() {
    assertThat(LockManagers.defaultLockManager())
        .isInstanceOf(LockManagers.InMemoryLockManager.class);
  }

  @Test
  public void testLoadCustomLockManager() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.LOCK_IMPL, CustomLockManager.class.getName());
    assertThat(LockManagers.from(properties)).isInstanceOf(CustomLockManager.class);
  }

  @Test
  public void testSchedulerUsageRegistersManagerWithoutInitialize() throws Exception {
    TestBaseLockManager firstManager = new TestBaseLockManager();

    try (TestBaseLockManager secondManager = new TestBaseLockManager()) {
      firstManager.scheduler();
      ScheduledExecutorService scheduler = secondManager.scheduler();

      firstManager.close();

      assertThat(scheduler.isShutdown()).isFalse();
    }
  }

  @Test
  public void testCloseWaitsForSchedulerRegistrationAndCreation() throws Exception {
    CountDownLatch creatingScheduler = new CountDownLatch(1);
    CountDownLatch finishCreatingScheduler = new CountDownLatch(1);
    BlockingBaseLockManager manager =
        new BlockingBaseLockManager(creatingScheduler, finishCreatingScheduler);

    CompletableFuture<ScheduledExecutorService> schedulerFuture =
        CompletableFuture.supplyAsync(manager::scheduler);

    assertThat(creatingScheduler.await(5, TimeUnit.SECONDS)).isTrue();

    CompletableFuture<Void> closeFuture =
        CompletableFuture.runAsync(
            () -> {
              try {
                manager.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    assertThat(closeFuture).isNotDone();

    finishCreatingScheduler.countDown();

    ScheduledExecutorService scheduler = schedulerFuture.get(5, TimeUnit.SECONDS);
    closeFuture.get(5, TimeUnit.SECONDS);

    assertThat(scheduler.isShutdown()).isTrue();
  }

  @Test
  public void testCloseDoesNotClearLocksWhileAnotherManagerIsActive() throws Exception {
    LockManagers.InMemoryLockManager firstManager =
        new LockManagers.InMemoryLockManager(Maps.newHashMap());

    try (LockManagers.InMemoryLockManager secondManager =
        new LockManagers.InMemoryLockManager(Maps.newHashMap())) {
      firstManager.acquireOnce("entity", "owner");

      secondManager.close();

      assertThat(firstManager.release("entity", "owner")).isTrue();
    } finally {
      firstManager.close();
    }
  }

  static class CustomLockManager implements LockManager {

    @Override
    public boolean acquire(String entityId, String ownerId) {
      return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void initialize(Map<String, String> properties) {}
  }

  static class TestBaseLockManager extends LockManagers.BaseLockManager {

    @Override
    public boolean acquire(String entityId, String ownerId) {
      return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return false;
    }
  }

  static class BlockingBaseLockManager extends TestBaseLockManager {
    private final CountDownLatch creatingScheduler;
    private final CountDownLatch finishCreatingScheduler;

    BlockingBaseLockManager(
        CountDownLatch creatingScheduler, CountDownLatch finishCreatingScheduler) {
      this.creatingScheduler = creatingScheduler;
      this.finishCreatingScheduler = finishCreatingScheduler;
    }

    @Override
    ScheduledExecutorService newScheduler() {
      creatingScheduler.countDown();
      try {
        assertThat(finishCreatingScheduler.await(5, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return super.newScheduler();
    }
  }
}
