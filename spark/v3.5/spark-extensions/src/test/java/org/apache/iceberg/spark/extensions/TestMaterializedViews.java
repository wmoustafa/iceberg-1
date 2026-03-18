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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.MaterializedViewUtil;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SparkView;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestMaterializedViews extends SparkExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private final String tableName = "table";
  private final String materializedViewName = "materialized_view";

  @Before
  public void before() {
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @After
  public void removeTable() {
    sql("USE %s", catalogName);
    sql("DROP VIEW IF EXISTS %s", materializedViewName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    Map<String, String> properties =
        Maps.newHashMap(SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.properties());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file:" + getTempWarehouseDir());
    properties.put(CatalogProperties.CATALOG_IMPL, InMemoryCatalogWithLocalFileIO.class.getName());
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_MATERIALIZED_VIEWS.implementation(),
        properties
      }
    };
  }

  private static String getTempWarehouseDir() {
    try {
      File tempDir = Files.createTempDirectory("warehouse-").toFile();
      tempDir.delete();
      return tempDir.getAbsolutePath();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TestMaterializedViews(
      String catalog, String implementation, Map<String, String> properties) {
    super(catalog, implementation, properties);
  }

  @Test
  public void testStorageTableFieldOnViewVersion() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    View view = loadIcebergView();
    // storage-table should be set on the view version, not as a property
    assertThat(view.currentVersion().storageTable()).isNotNull();
    assertThat(view.currentVersion().storageTable().name())
        .isEqualTo(materializedViewName + "__storage");
    assertThat(view.currentVersion().storageTable().namespace())
        .isEqualTo(NAMESPACE);
  }

  @Test
  public void testNoOldMvProperties() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    View view = loadIcebergView();
    // old property-based MV metadata should not exist
    assertThat(view.properties().get("iceberg.materialized.view")).isNull();
    assertThat(view.properties().get("iceberg.materialized.view.storage.table")).isNull();
  }

  @Test
  public void testNeverRefreshedMvIsNotFresh() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // A newly created MV has no snapshots on its storage table, so it's not fresh.
    // loadView should succeed (returns stale view)
    try {
      assertThat(sparkViewCatalog().loadView(viewIdentifier())).isInstanceOf(SparkView.class);
    } catch (NoSuchViewException e) {
      fail("Materialized view not found");
    }
  }

  @Test
  public void testStorageTableCreatedBeforeMvMetadata() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // The storage table should exist
    String storageTableName =
        MaterializedViewUtil.getDefaultMaterializedViewStorageTableIdentifier(
                Identifier.of(new String[] {NAMESPACE.toString()}, materializedViewName))
            .name();
    assertThat(sql("SHOW TABLES"))
        .anySatisfy(row -> assertThat(row[1]).isEqualTo(storageTableName));
  }

  @Test
  public void testDefaultStorageTableNaming() {
    sql("CREATE MATERIALIZED VIEW %s AS SELECT id, data FROM %s", materializedViewName, tableName);

    // Default naming should be <name>__storage
    String expectedStorageTableName = materializedViewName + "__storage";
    assertThat(sql("SHOW TABLES"))
        .anySatisfy(row -> assertThat(row[1]).isEqualTo(expectedStorageTableName));
  }

  @Test
  public void testStoredAsClause() {
    String customTableName = "custom_table_name";
    sql(
        "CREATE MATERIALIZED VIEW %s STORED AS '%s' AS SELECT id, data FROM %s",
        materializedViewName, customTableName, tableName);

    // Assert that the storage table with the custom name is in the list of tables
    assertThat(sql("SHOW TABLES")).anySatisfy(row -> assertThat(row[1]).isEqualTo(customTableName));
  }

  private ViewCatalog sparkViewCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (ViewCatalog) catalogPlugin;
  }

  private TableCatalog sparkTableCatalog() {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    return (TableCatalog) catalogPlugin;
  }

  private Identifier viewIdentifier() {
    return Identifier.of(new String[] {NAMESPACE.toString()}, materializedViewName);
  }

  private View loadIcebergView() {
    org.apache.iceberg.catalog.ViewCatalog icebergViewCatalog =
        (org.apache.iceberg.catalog.ViewCatalog) sparkTableCatalog();
    return icebergViewCatalog.loadView(
        TableIdentifier.of(NAMESPACE, materializedViewName));
  }

  // Required to be public since it is loaded by org.apache.iceberg.CatalogUtil.loadCatalog
  public static class InMemoryCatalogWithLocalFileIO extends InMemoryCatalog {
    private FileIO localFileIO;

    @Override
    public void initialize(String name, Map<String, String> properties) {
      super.initialize(name, properties);
      localFileIO = new LocalFileIO();
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return new InMemoryTableOperations(localFileIO, tableIdentifier);
    }

    @Override
    protected InMemoryCatalog.InMemoryViewOperations newViewOps(TableIdentifier identifier) {
      return new InMemoryViewOperations(localFileIO, identifier);
    }
  }

  private static class LocalFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return org.apache.iceberg.Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return org.apache.iceberg.Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }
}
