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
package org.apache.iceberg.spark;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class TestS3HiveCatalogTableLocation {
  private static final String CATALOG = "test";
  // MinIO's latest tag crashes in this test environment; pin to a released image used elsewhere.
  private static final String MINIO_TAG = "RELEASE.2024-12-18T13-15-44Z";
  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_ENDPOINT = "fs.s3a.endpoint";
  private static final String S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";
  private static final String S3A_IMPL = "fs.s3a.impl";
  private static final String S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
  private static final String S3A_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
  private static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
  private static final String S3A_FILE_SYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String S3A_SIMPLE_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer(DockerImageName.parse("minio/minio").withTag(MINIO_TAG))
          .withEnv("MINIO_DOMAIN", "localhost");

  private static TestHiveMetastore metastore;
  private static HiveConf hiveConf;
  private static SparkSession spark;
  private static HiveCatalog validationCatalog;
  private static String bucket;

  @BeforeAll
  static void startMetastoreAndSpark() {
    HiveConf conf = new HiveConf(new Configuration(), TestHiveMetastore.class);
    bucket = "test";
    configureS3A(conf);
    s3Client().createBucket(request -> request.bucket(bucket));
    conf.set(METASTOREWAREHOUSE.varname, String.format("s3a://%s", bucket));

    metastore = new TestHiveMetastore();
    metastore.start(conf);
    hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.testing", "true")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName())
            .config("spark.sql.catalog." + CATALOG + ".type", "hive")
            .config("spark.sql.catalog." + CATALOG + ".uri", hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.catalog." + CATALOG + ".default-namespace", "default")
            .config("spark.sql.catalog." + CATALOG + ".cache-enabled", "false")
            .config(
                "spark.sql.catalog." + CATALOG + ".warehouse",
                String.format("s3a://%s", bucket))
            .config("spark.sql.catalog." + CATALOG + ".io-impl", S3FileIO.class.getName())
            .config(
                "spark.sql.catalog." + CATALOG + "." + S3FileIOProperties.ENDPOINT,
                MINIO.getS3URL())
            .config(
                "spark.sql.catalog." + CATALOG + "." + S3FileIOProperties.PATH_STYLE_ACCESS,
                "true")
            .config(
                "spark.sql.catalog." + CATALOG + "." + S3FileIOProperties.ACCESS_KEY_ID,
                MINIO.getUserName())
            .config(
                "spark.sql.catalog." + CATALOG + "." + S3FileIOProperties.SECRET_ACCESS_KEY,
                MINIO.getPassword())
            .config(
                "spark.sql.catalog." + CATALOG + "." + AwsClientProperties.CLIENT_REGION,
                "us-east-1")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.metrics.conf.*.sink.servlet.class",
                "org.apache.iceberg.spark.DummyMetricsServlet")
            .enableHiveSupport()
            .getOrCreate();

    configureS3A(spark.sparkContext().hadoopConfiguration());

    validationCatalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", Map.of(), hiveConf);

    try {
      validationCatalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  static void stopMetastoreAndSpark() throws Exception {
    validationCatalog = null;

    if (spark != null) {
      spark.stop();
      spark = null;
    }

    if (metastore != null) {
      metastore.stop();
      metastore = null;
      hiveConf = null;
    }
  }

  @AfterEach
  void dropTableAndClearCatalog() {
    sql("DROP TABLE IF EXISTS %s.test.test", CATALOG);
    sql("DROP NAMESPACE IF EXISTS %s.test", CATALOG);
  }

  @Test
  void testCreateTableNormalizesS3Location() {
    sql("CREATE NAMESPACE %s.test", CATALOG);

    spark.range(1).selectExpr("id", "'a' AS data").write()
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("test.test.test");

    assertThat(scalarSql("SELECT COUNT(*) FROM %s.test.test", CATALOG)).isEqualTo(1L);

    Table table = validationCatalog.loadTable(TableIdentifier.of("test", "test"));
    assertThat(table.location()).isEqualTo(String.format("s3a://%s/test.db/test", bucket));
  }

  private static void configureS3A(Configuration conf) {
    Map<String, String> s3aConfig =
        Map.of(
            S3A_IMPL, S3A_FILE_SYSTEM,
            S3A_ENDPOINT, URI.create(MINIO.getS3URL()).getAuthority(),
            S3A_ENDPOINT_REGION, "us-east-1",
            S3A_PATH_STYLE_ACCESS, "true",
            S3A_SSL_ENABLED, "false",
            S3A_ACCESS_KEY, MINIO.getUserName(),
            S3A_SECRET_KEY, MINIO.getPassword(),
            S3A_CREDENTIALS_PROVIDER, S3A_SIMPLE_CREDENTIALS_PROVIDER);

    s3aConfig.forEach(conf::set);
  }

  private static software.amazon.awssdk.services.s3.S3Client s3Client() {
    return software.amazon.awssdk.services.s3.S3Client.builder()
        .endpointOverride(URI.create(MINIO.getS3URL()))
        .forcePathStyle(true)
        .region(software.amazon.awssdk.regions.Region.US_EAST_1)
        .credentialsProvider(
            software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                    MINIO.getUserName(), MINIO.getPassword())))
        .build();
  }

  private List<Row> sql(String query, Object... args) {
    return spark.sql(String.format(query, args)).collectAsList();
  }

  private Object scalarSql(String query, Object... args) {
    List<Row> rows = sql(query, args);
    assertThat(rows).hasSize(1);
    return rows.get(0).get(0);
  }
}
