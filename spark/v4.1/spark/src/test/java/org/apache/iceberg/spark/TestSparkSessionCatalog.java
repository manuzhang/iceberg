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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSparkSessionCatalog extends TestBase {
  private final String envHmsUriKey = "spark.hadoop." + METASTOREURIS.varname;
  private final String catalogHmsUriKey = "spark.sql.catalog.spark_catalog.uri";
  private final String hmsUri = hiveConf.get(METASTOREURIS.varname);

  @BeforeAll
  public static void setUpCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @BeforeEach
  public void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, hmsUri);
  }

  @Test
  public void testValidateHmsUri() {
    // HMS uris match
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // HMS uris doesn't match
    spark.sessionState().catalogManager().reset();
    String catalogHmsUri = "RandomString";
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, catalogHmsUri);

    assertThatThrownBy(() -> spark.sessionState().catalogManager().v2SessionCatalog())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
                hmsUri, catalogHmsUri));

    // no env HMS uri, only catalog HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(catalogHmsUriKey, hmsUri);
    spark.conf().unset(envHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // no catalog HMS uri, only env HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().unset(catalogHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");
  }

  @Test
  public void testLoadFunction() {
    String functionClass = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";

    // load permanent UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE FUNCTION perm_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT perm_upper('xyz')"))
        .as("Load permanent UDF in Hive")
        .isEqualTo("XYZ");

    // load temporary UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE TEMPORARY FUNCTION temp_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT temp_upper('xyz')"))
        .as("Load temporary UDF in Hive")
        .isEqualTo("XYZ");

    // TODO: fix loading Iceberg built-in functions in SessionCatalog
  }

  @Test
  public void listViewsReturnsSessionCatalogViews() throws NoSuchNamespaceException {
    Identifier viewIdent = Identifier.of(new String[] {"default"}, "session_catalog_list_views");
    Identifier[] views = new Identifier[] {viewIdent};
    TableCatalog sessionCatalog =
        mock(
            TableCatalog.class,
            withSettings()
                .extraInterfaces(
                    FunctionCatalog.class, SupportsNamespaces.class, ViewCatalog.class));
    when(((ViewCatalog) sessionCatalog).listViews("default")).thenReturn(views);

    SparkSessionCatalog<?> catalog = new NoViewCatalog<>();
    catalog.initialize("spark_catalog", new CaseInsensitiveStringMap(Collections.emptyMap()));
    catalog.setDelegateCatalog(sessionCatalog);

    assertThat(catalog.listViews("default")).containsExactly(viewIdent);
  }

  @Test
  public void listViewsDelegatesToSessionCatalog() throws NoSuchViewException {
    String viewName = "list_views_session_catalog";
    Identifier viewIdent = Identifier.of(new String[] {"default"}, viewName);

    spark.sql("DROP VIEW IF EXISTS " + viewName);
    spark.sql("CREATE VIEW " + viewName + " AS SELECT 1 AS id");

    try {
      SparkSessionCatalog<?> catalog =
          (SparkSessionCatalog<?>) spark.sessionState().catalogManager().v2SessionCatalog();

      assertThat(catalog.listViews("default")).extracting(Identifier::name).contains(viewName);
      assertThat(catalog.viewExists(viewIdent)).isTrue();

      View view = catalog.loadView(viewIdent);
      assertThat(view.name()).isEqualTo(viewName);
      assertThat(view.schema().fieldNames()).containsExactly("id");
    } finally {
      spark.sql("DROP VIEW IF EXISTS " + viewName);
    }
  }

  @Test
  public void sessionCatalogViewListingMatchesTableListing() {
    String viewName = "list_views_consistent_with_tables_view";
    String tableName = "list_views_consistent_with_tables_table";

    spark.sql("DROP VIEW IF EXISTS " + viewName);
    spark.sql("DROP TABLE IF EXISTS " + tableName);

    try {
      spark.sql("CREATE VIEW " + viewName + " AS SELECT 1 AS id");
      spark.sql("CREATE TABLE " + tableName + " (id INT) USING iceberg");

      Object[] viewRow = row("default", viewName, false);
      Object[] tableRow = row("default", tableName, false);

      assertThat(sql("SHOW VIEWS")).contains(viewRow);
      assertThat(sql("SHOW VIEWS IN default")).contains(viewRow);
      assertThat(sql("SHOW VIEWS IN spark_catalog.default")).contains(viewRow);

      assertThat(sql("SHOW TABLES")).contains(tableRow);
      assertThat(sql("SHOW TABLES IN default")).contains(tableRow);
      assertThat(sql("SHOW TABLES IN spark_catalog.default")).contains(tableRow);
    } finally {
      spark.sql("DROP VIEW IF EXISTS " + viewName);
      spark.sql("DROP TABLE IF EXISTS " + tableName);
    }
  }

  @Test
  public void listViewsMergesViewCatalogAndSessionCatalogViews() throws NoSuchNamespaceException {
    Identifier viewCatalogView =
        Identifier.of(new String[] {"default"}, "list_views_merge_view_catalog");
    Identifier sessionCatalogView =
        Identifier.of(new String[] {"default"}, "list_views_merge_session_catalog");
    SparkSessionCatalog<?> catalog =
        catalogWithIcebergCatalog(
            mockViewCatalog(IcebergViewCatalog.class, viewCatalogView),
            mockViewCatalog(ViewSessionCatalog.class, sessionCatalogView));

    assertThat(catalog.listViews("default")).containsExactly(viewCatalogView, sessionCatalogView);
  }

  @Test
  public void listViewsDelegatesToViewSessionCatalog() throws NoSuchNamespaceException {
    Identifier sessionCatalogView =
        Identifier.of(new String[] {"default"}, "list_views_delegate_session_catalog");
    SparkSessionCatalog<?> catalog =
        catalogWithIcebergCatalog(
            mock(IcebergCatalog.class),
            mockViewCatalog(ViewSessionCatalog.class, sessionCatalogView));

    assertThat(catalog.listViews("default")).containsExactly(sessionCatalogView);
  }

  private static class NoViewCatalog<
          T extends TableCatalog & FunctionCatalog & SupportsNamespaces & ViewCatalog>
      extends SparkSessionCatalog<T> {
    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return mock(TableCatalog.class);
    }
  }

  private SparkSessionCatalog<?> catalogWithIcebergCatalog(
      TableCatalog icebergCatalog, ViewSessionCatalog sessionCatalog) {
    SparkSessionCatalog<?> catalog = new TestableSparkSessionCatalog(icebergCatalog);
    catalog.initialize("test", CaseInsensitiveStringMap.empty());
    catalog.setDelegateCatalog(sessionCatalog);
    return catalog;
  }

  private static <T extends ViewCatalog> T mockViewCatalog(
      Class<T> catalogClass, Identifier... views)
      throws NoSuchNamespaceException {
    T catalog = mock(catalogClass);
    when(catalog.listViews("default")).thenReturn(views);
    return catalog;
  }

  private interface IcebergCatalog extends TableCatalog {}

  private interface IcebergViewCatalog extends TableCatalog, ViewCatalog {}

  private interface SessionCatalog extends TableCatalog, FunctionCatalog, SupportsNamespaces {}

  private interface ViewSessionCatalog extends SessionCatalog, ViewCatalog {}

  private static class TestableSparkSessionCatalog extends SparkSessionCatalog<ViewSessionCatalog> {
    private final TableCatalog icebergCatalog;

    TestableSparkSessionCatalog(TableCatalog icebergCatalog) {
      this.icebergCatalog = icebergCatalog;
    }

    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return icebergCatalog;
    }
  }
}
