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

import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceState;
import org.apache.iceberg.view.SourceTableState;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.connector.catalog.Identifier;

public class MaterializedViewUtil {

  private MaterializedViewUtil() {}

  private static final String MATERIALIZED_VIEW_STORAGE_TABLE_IDENTIFIER_SUFFIX = "__storage";

  public static Identifier getDefaultMaterializedViewStorageTableIdentifier(
      Identifier viewIdentifier) {
    return Identifier.of(
        viewIdentifier.namespace(),
        viewIdentifier.name() + MATERIALIZED_VIEW_STORAGE_TABLE_IDENTIFIER_SUFFIX);
  }

  public static boolean isMaterializedView(View view) {
    return view != null && view.currentVersion().storageTable() != null;
  }

  /**
   * Returns true if the materialized view's storage table represents the result of the current view
   * query over the current state of its dependencies.
   *
   * <p>The check inspects the storage table's current snapshot summary for a {@link RefreshState}
   * record. The MV is considered fresh when:
   *
   * <ul>
   *   <li>the storage table has a current snapshot,
   *   <li>the snapshot summary contains a {@code refresh-state} record,
   *   <li>the recorded {@code view-version-id} matches the view's current version, and
   *   <li>every recorded {@link SourceTableState} matches the source table's current snapshot id.
   * </ul>
   */
  public static boolean isFresh(View view, Catalog catalog) {
    if (!isMaterializedView(view)) {
      return false;
    }

    Table storageTable;
    try {
      storageTable = catalog.loadTable(view.currentVersion().storageTable());
    } catch (Exception e) {
      return false;
    }

    if (storageTable.currentSnapshot() == null) {
      return false;
    }

    String refreshStateJson =
        storageTable.currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
    if (refreshStateJson == null) {
      return false;
    }

    RefreshState refreshState = RefreshStateParser.fromJson(refreshStateJson);

    if (refreshState.viewVersionId() != view.currentVersion().versionId()) {
      return false;
    }

    for (SourceState sourceState : refreshState.sourceStates()) {
      if (sourceState instanceof SourceTableState) {
        SourceTableState tableState = (SourceTableState) sourceState;
        TableIdentifier sourceId =
            TableIdentifier.of(
                Namespace.of(tableState.namespace().toArray(new String[0])), tableState.name());
        try {
          Table sourceTable = catalog.loadTable(sourceId);
          long currentSnapshotId =
              sourceTable.currentSnapshot() == null
                  ? -1
                  : sourceTable.currentSnapshot().snapshotId();
          if (currentSnapshotId != tableState.snapshotId()) {
            return false;
          }
        } catch (Exception e) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Returns the storage table identifier for a materialized view if and only if the view is a
   * materialized view AND is currently fresh. Otherwise returns empty.
   */
  public static Optional<TableIdentifier> resolveStorageTable(View view, Catalog catalog) {
    if (isMaterializedView(view) && isFresh(view, catalog)) {
      return Optional.of(view.currentVersion().storageTable());
    }
    return Optional.empty();
  }
}
