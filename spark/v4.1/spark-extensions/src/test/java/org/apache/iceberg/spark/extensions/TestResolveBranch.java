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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.ResolveBranch;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.PartialFunction;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

@ExtendWith(ParameterizedTestExtension.class)
public class TestResolveBranch extends ExtensionsTestBase {

  private static final String WAP_BRANCH = "wap";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private String tableLocation;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @BeforeEach
  public void createTable() {
    tableLocation = temp.resolve("path-table").toString();
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    tables.create(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        Map.of(WRITE_AUDIT_PUBLISH_ENABLED, "true"),
        tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    Table table = tables.load(tableLocation);
    table.manageSnapshots().createBranch(WAP_BRANCH, table.currentSnapshot().snapshotId()).commit();
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, WAP_BRANCH);
  }

  @AfterEach
  public void removeTable() {
    spark.conf().unset(SparkSQLProperties.WAP_BRANCH);
  }

  @TestTemplate
  public void testReadPathIdentifier() {
    Dataset<Row> pathRead = spark.read().format("iceberg").load(tableLocation);
    DataSourceV2Relation pathRelation = onlyRelation(pathRead.queryExecution().analyzed());

    assertThat(pathRelation.table()).isInstanceOf(SparkTable.class);
    assertThat(((SparkTable) pathRelation.table()).branch()).isEqualTo(WAP_BRANCH);
    assertThat(pathRelation.identifier().isDefined()).isTrue();
    assertThat(pathRelation.identifier().get()).isInstanceOf(PathIdentifier.class);
    assertThat(((PathIdentifier) pathRelation.identifier().get()).location())
        .isEqualTo(tableLocation + "#branch_" + WAP_BRANCH);
  }

  @TestTemplate
  public void testReadPathIdentifierWithMetadataSelector() {
    Dataset<Row> pathRead = spark.read().format("iceberg").load(tableLocation);
    DataSourceV2Relation pathRelation = onlyRelation(pathRead.queryExecution().analyzed());
    SparkTable unbranchedTable = ((SparkTable) pathRelation.table()).copyWithBranch(null);
    PathIdentifier metadataIdentifier = new PathIdentifier(tableLocation + "#files");
    DataSourceV2Relation preRuleRelation =
        pathRelation.copy(
            unbranchedTable,
            pathRelation.output(),
            pathRelation.catalog(),
            Option.apply(metadataIdentifier),
            pathRelation.options(),
            pathRelation.timeTravelSpec());

    LogicalPlan resolvedPlan = new ResolveBranch(spark).apply(preRuleRelation);

    assertThat(resolvedPlan).isInstanceOf(DataSourceV2Relation.class);
    DataSourceV2Relation resolvedRelation = (DataSourceV2Relation) resolvedPlan;
    assertThat(resolvedRelation.table()).isInstanceOf(SparkTable.class);
    assertThat(((SparkTable) resolvedRelation.table()).branch()).isEqualTo(WAP_BRANCH);
    assertThat(resolvedRelation.identifier().isDefined()).isTrue();
    assertThat(resolvedRelation.identifier().get()).isInstanceOf(PathIdentifier.class);
    assertThat(((PathIdentifier) resolvedRelation.identifier().get()).location())
        .isEqualTo(tableLocation + "#files,branch_" + WAP_BRANCH);
  }

  private DataSourceV2Relation onlyRelation(LogicalPlan plan) {
    List<DataSourceV2Relation> relations = collectRelations(plan);
    assertThat(relations).hasSize(1);
    return relations.get(0);
  }

  private List<DataSourceV2Relation> collectRelations(LogicalPlan plan) {
    Seq<DataSourceV2Relation> collected =
        plan.collect(
            new PartialFunction<>() {
              @Override
              public DataSourceV2Relation apply(LogicalPlan logicalPlan) {
                return (DataSourceV2Relation) logicalPlan;
              }

              @Override
              public boolean isDefinedAt(LogicalPlan logicalPlan) {
                return logicalPlan instanceof DataSourceV2Relation;
              }
            });

    return Lists.newArrayList(CollectionConverters.asJavaCollection(collected));
  }
}
