<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Apache Iceberg with Apache Beam Example

This example demonstrates how to read from and write to [Apache Iceberg](https://iceberg.apache.org/)
tables using [Apache Beam](https://beam.apache.org/) and the built-in
[Iceberg I/O connector](https://beam.apache.org/documentation/io/built-in/iceberg/).

## What the example does

1. **Write** – creates a three-row Iceberg table (`example_db.example_table`) in a temporary local
   warehouse directory using a Hadoop catalog.
2. **Read** – reads the rows back and prints them to standard output.

Both pipelines run locally using Beam's [Direct Runner](https://beam.apache.org/documentation/runners/direct/).

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 11 or newer |
| Gradle | 8.x |

## Running the example

```bash
cd examples/beam
gradle run
```

Expected output (row order may vary):

```
Warehouse location: /tmp/iceberg-beam-example12345678
Wrote 3 rows to Iceberg table.
id=1, name=Alice, age=30
id=2, name=Bob, age=25
id=3, name=Charlie, age=35
Read rows from Iceberg table.
```

## Key dependencies

| Artifact | Purpose |
|---|---|
| `org.apache.beam:beam-sdks-java-io-iceberg` | Iceberg I/O connector (read & write) |
| `org.apache.beam:beam-runners-direct-java` | Local Direct Runner |
| `org.apache.hadoop:hadoop-common` | Required by the Hadoop catalog |

## Adapting the example

### Use a different catalog

Replace the `catalog_properties` map. For example, to use a Hive Metastore catalog:

```java
Map<String, Object> catalogProperties = new HashMap<>();
catalogProperties.put("type", "hive");
catalogProperties.put("warehouse", "hdfs://namenode:8020/user/hive/warehouse");

Map<String, Object> configProperties = new HashMap<>();
configProperties.put("hive.metastore.uris", "thrift://metastore-host:9083");

config.put("catalog_properties", catalogProperties);
config.put("config_properties", configProperties);
```

### Run on a distributed runner

Add the desired runner as a `runtimeOnly` dependency in `build.gradle` and pass
`--runner=<RunnerName>` to the pipeline options, e.g.:

```bash
gradle run --args='--runner=FlinkRunner --flinkMaster=localhost:8081'
```
