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
package org.apache.spark.sql.catalyst.analysis

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog

/**
 * Routes SELECT references to materialized views by rewriting the [[UnresolvedRelation]] to point
 * at the MV's storage table when the MV is fresh.
 *
 * <p>For a fresh MV, this rule replaces the relation's identifier with the storage table's
 * identifier so Spark's normal table resolution loads the precomputed data. For stale or
 * never-refreshed MVs, the rule leaves the relation unchanged and a subsequent
 * [[ResolveViews]] pass expands the view query.
 *
 * <p>Only [[UnresolvedRelation]] nodes are rewritten. DDL paths that reference MVs through
 * [[UnresolvedTableOrView]] (DESCRIBE / DROP VIEW / SHOW CREATE VIEW / ALTER VIEW) are left
 * untouched because the MV's metadata is the relevant target there.
 */
case class ResolveMaterializedViews(spark: SparkSession)
    extends Rule[LogicalPlan]
    with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UnresolvedRelation(nameParts, _, _)
        if catalogManager.v1SessionCatalog.isTempView(nameParts) =>
      u

    case u @ UnresolvedRelation(CatalogAndIdentifier(catalog: SparkCatalog, ident), _, _) =>
      val maybeStorage = catalog.resolveMaterializedView(ident)
      if (maybeStorage.isPresent) {
        val storage = maybeStorage.get()
        val newParts: Seq[String] =
          catalog.name() +: (storage.namespace().toSeq :+ storage.name())
        u.copy(multipartIdentifier = newParts)
      } else {
        u
      }
  }
}
