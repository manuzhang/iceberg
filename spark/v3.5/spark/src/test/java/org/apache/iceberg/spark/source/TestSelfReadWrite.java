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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestSelfReadWrite extends TestBaseWithCatalog {
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        "self_read_write",
        WriteCheckingCatalog.class.getName(),
        Map.of("type", "hadoop", "cache-enabled", "false")
      }
    };
  }

  @BeforeEach
  void createTable() {
    writeCheckingCatalog().denyWrites = false;
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
  }

  @AfterEach
  void removeTable() {
    writeCheckingCatalog().denyWrites = false;
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void appendFromSameTable() {
    sql("INSERT INTO %s SELECT id + 10, data FROM %s", tableName, tableName);

    assertEquals(
        "Append must retain the original rows and add each source row once",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(11L, "a"), row(12L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void overwriteFromSameTable() {
    sql("INSERT OVERWRITE %s SELECT id + 10, data FROM %s", tableName, tableName);

    assertEquals(
        "Overwrite must replace the original rows using the pre-write snapshot",
        ImmutableList.of(row(11L, "a"), row(12L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void readPermissionDoesNotAuthorizeSelfAppend() {
    writeCheckingCatalog().denyWrites = true;
    assertOriginalRows();

    assertThatThrownBy(() -> sql("INSERT INTO %s SELECT * FROM %s", tableName, tableName))
        .isInstanceOf(SecurityException.class)
        .hasMessage("Write access denied");

    assertOriginalRows();
  }

  @TestTemplate
  void readPermissionDoesNotAuthorizeSelfOverwrite() {
    writeCheckingCatalog().denyWrites = true;
    assertOriginalRows();

    assertThatThrownBy(
            () -> sql("INSERT OVERWRITE %s SELECT id + 10, data FROM %s", tableName, tableName))
        .isInstanceOf(SecurityException.class)
        .hasMessage("Write access denied");

    assertOriginalRows();
  }

  private void assertOriginalRows() {
    assertEquals(
        "Read access remains allowed and denied writes must not change rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  private WriteCheckingCatalog writeCheckingCatalog() {
    return (WriteCheckingCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  public static class WriteCheckingCatalog extends SparkCatalog {
    private boolean denyWrites;

    @Override
    public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
        throws NoSuchTableException {
      if (denyWrites && !writePrivileges.isEmpty()) {
        throw new SecurityException("Write access denied");
      }

      return super.loadTable(ident);
    }
  }
}
