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
package org.apache.iceberg.spark.source.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that compares Iceberg's vectorized and row-based Parquet readers on nested columns.
 *
 * <p>To run this benchmark for spark-4.2: <code>
 *   ./gradlew -DsparkVersions=4.2 :iceberg-spark:iceberg-spark-4.2_2.13:jmh
 *       -PjmhIncludeRegex=IcebergSourceNestedParquetVectorizedReadBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-nested-parquet-vectorized-read-benchmark-result.txt
 * </code>
 */
// Arrow, used for top-level primitives in vectorized reads, needs direct access to NIO buffers.
@Fork(
    value = 1,
    jvmArgsAppend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    })
public class IcebergSourceNestedParquetVectorizedReadBenchmark extends IcebergSourceBenchmark {

  private static final int NUM_FILES = 5;
  private static final int NUM_ROWS = 1000000;

  @Param({"true", "false"})
  private boolean vectorized;

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected Table initTable() {
    Schema schema =
        new Schema(
            optional(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    optional(3, "a", Types.StringType.get()),
                    optional(4, "b", Types.DoubleType.get()),
                    optional(5, "c", Types.LongType.get()))),
            optional(
                6,
                "arr",
                Types.ListType.ofOptional(
                    7,
                    Types.StructType.of(
                        optional(8, "x", Types.LongType.get()),
                        optional(9, "y", Types.StringType.get())))),
            optional(
                10,
                "m",
                Types.MapType.ofOptional(11, 12, Types.StringType.get(), Types.LongType.get())));
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return new HadoopTables(hadoopConf())
        .create(schema, PartitionSpec.unpartitioned(), properties, newTableLocation());
  }

  @Setup
  public void setupBenchmark() {
    setupSpark(true);
    spark().conf().set("spark.sql.legacy.allowHashOnMapType", "true");
    appendData();
    String plan = load().queryExecution().executedPlan().toString();
    Preconditions.checkState(
        plan.contains("ColumnarToRow") == vectorized,
        "Unexpected scan mode (vectorized=%s): %s",
        vectorized,
        plan);
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void readStruct(Blackhole blackhole) {
    consumeAll(load().select("id", "s"), blackhole);
  }

  @Benchmark
  @Threads(1)
  public void readListOfStructs(Blackhole blackhole) {
    consumeAll(load().select("id", "arr"), blackhole);
  }

  @Benchmark
  @Threads(1)
  public void readMap(Blackhole blackhole) {
    consumeAll(load().select("id", "m"), blackhole);
  }

  @Benchmark
  @Threads(1)
  public void readAll(Blackhole blackhole) {
    consumeAll(load(), blackhole);
  }

  @Benchmark
  @Threads(1)
  public void filterStructField(Blackhole blackhole) {
    blackhole.consume(load().filter("s.c % 10 = 0").select("id", "s.a").count());
  }

  // Hashes every projected value so row-view access is measured, not only decoding.
  private void consumeAll(Dataset<Row> df, Blackhole blackhole) {
    blackhole.consume(df.selectExpr("sum(hash(*))").first());
  }

  private Dataset<Row> load() {
    return spark()
        .read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, Boolean.toString(vectorized))
        .option(SparkReadOptions.PARQUET_NESTED_VECTORIZATION_ENABLED, "true")
        .load(table().location());
  }

  private void appendData() {
    for (int fileNum = 0; fileNum < NUM_FILES; fileNum++) {
      Dataset<Row> df =
          spark()
              .range(NUM_ROWS)
              .withColumn(
                  "s",
                  expr(
                      "IF(id % 13 = 0, NULL, named_struct("
                          + "'a', CAST(id AS string), 'b', CAST(id AS double), 'c', id))"))
              .withColumn(
                  "arr",
                  expr(
                      "IF(id % 5 = 0, array(), transform(sequence(1, CAST(id % 5 AS int)), "
                          + "i -> named_struct('x', id + i, 'y', CAST(i AS string))))"))
              .withColumn("m", expr("map('k1', id, 'k2', id % 100)"));
      appendAsFile(df);
    }
  }
}
