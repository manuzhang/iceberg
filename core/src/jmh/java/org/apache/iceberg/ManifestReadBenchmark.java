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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates manifest-list materialization and manifest-entry scanning.
 *
 * <p>To run only manifest-list materialization: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestReadBenchmark.readManifestList
 * </code>
 *
 * <p>To run only manifest-entry scanning: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestReadBenchmark.readManifestEntries
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class ManifestReadBenchmark {
  private static final int NUM_COLS = 10;
  private static final long RANDOM_SEED = 1L;
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  @State(Scope.Benchmark)
  public static class ManifestListReadState extends BenchmarkState {
    @Param({"1000", "10000", "100000"})
    private int numFiles;

    @Override
    int numFiles() {
      return numFiles;
    }

    @Override
    int numRows() {
      return 1;
    }
  }

  @State(Scope.Benchmark)
  public static class ManifestEntriesReadState extends BenchmarkState {
    @Param({"1000"})
    private int numFiles;

    @Param({"100000"})
    private int numRows;

    @Override
    int numFiles() {
      return numFiles;
    }

    @Override
    int numRows() {
      return numRows;
    }
  }

  public abstract static class BenchmarkState {
    private Path baseDir;
    private InputFile manifestListInputFile;
    private List<ManifestFile> manifests;
    private TestTables.LocalFileIO fileIO;
    private Map<Integer, PartitionSpec> specs;

    abstract int numFiles();

    abstract int numRows();

    InputFile manifestListInputFile() {
      return manifestListInputFile;
    }

    List<ManifestFile> manifests() {
      return manifests;
    }

    TestTables.LocalFileIO fileIO() {
      return fileIO;
    }

    Map<Integer, PartitionSpec> specs() {
      return specs;
    }

    @Setup
    public void before() {
      try {
        baseDir = Files.createTempDirectory("manifest-read-benchmark-");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      String manifestListFile =
          baseDir.resolve(String.format("%s.avro", UUID.randomUUID())).toString();
      Random random = new Random(RANDOM_SEED);

      try (ManifestListWriter listWriter =
          ManifestLists.write(
              1,
              org.apache.iceberg.Files.localOutput(manifestListFile),
              PlaintextEncryptionManager.instance(),
              0,
              1L,
              0,
              0L)) {
        for (int i = 0; i < numFiles(); i++) {
          OutputFile manifestFile =
              org.apache.iceberg.Files.localOutput(
                  baseDir.resolve(String.format("%s.avro", UUID.randomUUID())).toString());

          ManifestWriter<DataFile> writer = ManifestFiles.write(1, SPEC, manifestFile, 1L);
          try (ManifestWriter<DataFile> finalWriter = writer) {
            for (int j = 0; j < numRows(); j++) {
              DataFile dataFile =
                  DataFiles.builder(SPEC)
                      .withFormat(FileFormat.PARQUET)
                      .withPath(String.format("/path/to/data-%s-%s.parquet", i, j))
                      .withFileSizeInBytes(j)
                      .withRecordCount(j)
                      .withMetrics(randomMetrics(random))
                      .build();
              finalWriter.add(dataFile);
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }

          listWriter.add(writer.toManifestFile());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      this.manifestListInputFile = org.apache.iceberg.Files.localInput(manifestListFile);
      this.manifests = ManifestLists.read(manifestListInputFile);
      this.fileIO = new TestTables.LocalFileIO();
      this.specs = ImmutableMap.of(SPEC.specId(), SPEC);
    }

    @TearDown
    public void after() throws IOException {
      if (baseDir != null) {
        try (Stream<Path> walk = Files.walk(baseDir)) {
          walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        baseDir = null;
      }

      manifestListInputFile = null;
      manifests = null;
      fileIO = null;
      specs = null;
    }
  }

  @Benchmark
  @Threads(1)
  public void readManifestList(ManifestListReadState state, Blackhole blackhole) {
    blackhole.consume(ManifestLists.read(state.manifestListInputFile()));
  }

  @Benchmark
  @Threads(1)
  public void readManifestEntries(ManifestEntriesReadState state, Blackhole blackhole)
      throws IOException {
    for (ManifestFile manifestFile : state.manifests()) {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifestFile, state.fileIO(), state.specs());
      try (CloseableIterator<DataFile> it = reader.iterator()) {
        while (it.hasNext()) {
          blackhole.consume(it.next().recordCount());
        }
      }
    }
  }

  private static Metrics randomMetrics(Random random) {
    long rowCount = 100000L + random.nextInt(1000);
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    for (int i = 0; i < NUM_COLS; i++) {
      columnSizes.put(i, 1000000L + random.nextInt(100000));
      valueCounts.put(i, 100000L + random.nextInt(100));
      nullValueCounts.put(i, (long) random.nextInt(5));
      nanValueCounts.put(i, (long) random.nextInt(5));
      byte[] lower = new byte[8];
      random.nextBytes(lower);
      lowerBounds.put(i, ByteBuffer.wrap(lower));
      byte[] upper = new byte[8];
      random.nextBytes(upper);
      upperBounds.put(i, ByteBuffer.wrap(upper));
    }

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }
}
