# Iceberg Java Examples – Beam

This sub-project adds an [Apache Beam](https://beam.apache.org/) read/write example to the
[Iceberg Java examples](../iceberg-java/).

It extends the `iceberg-java` project with Beam's built-in
[Managed IcebergIO connector](https://beam.apache.org/documentation/io/built-in/iceberg/)
(Beam 2.58+) and runs entirely locally using the
[Direct Runner](https://beam.apache.org/documentation/runners/direct/) — no external cluster
required.

## Prerequisites

- Java 17 or higher
- Gradle 8.5 or higher (included via wrapper in `iceberg-examples/iceberg-java`)

## Getting Started

### Build the Project

```bash
./gradlew build
```

### Run the Beam Example

```bash
./gradlew runBeamExample
```

This writes three rows to a temporary local Iceberg table (Hadoop catalog) and reads them back,
printing each row to the log.

## Examples Included

### Beam read/write (`BeamExample.java`)

- Writes `Row` records to an Iceberg table via `Managed.write(Managed.ICEBERG)`
- Reads the rows back via `Managed.read(Managed.ICEBERG)`
- Uses a local Hadoop catalog (no external services needed)
- Runs on the Direct Runner

## Key Dependencies

| Artifact | Purpose |
|---|---|
| `beam-sdks-java-io-iceberg` | Iceberg I/O connector (read & write) |
| `beam-runners-direct-java` | Local Direct Runner |
| `hadoop-common` | Required by the Hadoop catalog |

## Adapting the Example

### Use a different catalog

Replace the `catalog_properties` in `buildConfig()`. For example, to use a Hive Metastore catalog:

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

Add the desired runner as a `runtimeOnly` dependency and pass `--runner=<RunnerName>`:

```bash
./gradlew runBeamExample --args='--runner=FlinkRunner --flinkMaster=localhost:8081'
```

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Beam IcebergIO connector](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [Beam Managed I/O](https://beam.apache.org/documentation/io/managed-io/)
