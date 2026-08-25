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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.view.RefreshState
import org.apache.iceberg.view.RefreshStateParser
import org.apache.iceberg.view.SourceTableState
import org.apache.iceberg.view.SourceViewState
import org.apache.iceberg.view.SQLViewRepresentation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.functions
import scala.jdk.CollectionConverters._

case class RefreshMaterializedViewExec(catalog: ViewCatalog, ident: Identifier)
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val sparkCatalog = catalog.asInstanceOf[SparkCatalog]
    val icebergCatalog = sparkCatalog.icebergCatalog()
    val icebergViewCatalog =
      icebergCatalog.asInstanceOf[org.apache.iceberg.catalog.ViewCatalog]
    val viewId = TableIdentifier.of(Namespace.of(ident.namespace(): _*), ident.name())
    val view = icebergViewCatalog.loadView(viewId)

    val storageTableId = view.currentVersion().storageTable()
    Preconditions.checkState(
      storageTableId != null,
      "Cannot refresh %s: not a materialized view (no storage table)",
      ident)

    // Extract the SQL query from the view's representations
    val sparkSql = view
      .currentVersion()
      .representations()
      .asScala
      .collect { case sql: SQLViewRepresentation if sql.dialect() == "spark" => sql.sql() }
      .headOption
      .getOrElse(throw new IllegalStateException(
        s"Cannot refresh $ident: no Spark SQL representation found"))

    val refreshStartTimestampMs = System.currentTimeMillis()

    // Execute the view's query to get the current result set
    val queryResult = session.sql(sparkSql)

    // Discover source tables and views from the query's logical plan and capture their
    // current state
    val sourceStates = collectSourceStates(queryResult.queryExecution.analyzed)

    // Build refresh state
    val refreshState = new RefreshState(
      view.currentVersion().versionId(),
      sourceStates.asJava,
      refreshStartTimestampMs)
    val refreshStateJson = RefreshStateParser.toJson(refreshState)

    // Write results to storage table, replacing existing data
    val storageTableRef = String.format(
      "%s.%s.%s",
      sparkCatalog.name(),
      storageTableId.namespace().toString,
      storageTableId.name())
    try {
      queryResult
        .writeTo(storageTableRef)
        .option("snapshot-property." + RefreshState.REFRESH_STATE_SUMMARY_KEY, refreshStateJson)
        .overwrite(functions.lit(true))
    } catch {
      case e: NoSuchTableException =>
        throw new IllegalStateException(
          s"Storage table $storageTableRef not found during refresh",
          e)
    }

    Nil
  }

  /**
   * Returns the catalog name to record for a source, or null when the source is in the
   * materialized view's own catalog.
   *
   * <p>A null catalog keeps refresh state portable: a materialized view and its sources that live
   * in the same catalog stay resolvable when that catalog is registered under a different name.
   */
  private def sourceCatalogName(sourceCatalog: HasIcebergCatalog): String = {
    if (sourceCatalog.name() == catalog.name()) null else sourceCatalog.name()
  }

  private def collectSourceStates(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)
      : List[org.apache.iceberg.view.SourceState] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[String]
    val states = scala.collection.mutable.ListBuffer.empty[org.apache.iceberg.view.SourceState]

    // Sources may live in a catalog other than the one that holds the materialized view, so the
    // catalog that resolved each relation is used to load metadata and is recorded alongside the
    // identifier. Filtering to the materialized view's own catalog would silently drop
    // cross-catalog sources, and a materialized view with no recorded sources never goes stale.
    // The test is HasIcebergCatalog rather than a concrete class so that Iceberg tables reached
    // through the session catalog, which SparkSessionCatalog serves, are tracked as well.
    plan.collectLeaves().foreach {
      case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
          if r.catalog.exists(_.isInstanceOf[HasIcebergCatalog]) && r.identifier.isDefined =>
        val sourceCatalog = r.catalog.get.asInstanceOf[HasIcebergCatalog]
        val tableIdent = r.identifier.get
        val key = "table:" + sourceCatalog.name() + "." + tableIdent.toString
        if (seen.add(key)) {
          val icebergId = sourceCatalog.icebergIdentifier(tableIdent)
          try {
            // A SparkTable resolves its snapshot when the relation is resolved and the scan
            // reads exactly that snapshot, so the state is taken from the relation rather
            // than from a second load of the table. Loading the table again would observe
            // whatever snapshot is current now, which may already be newer than the one this
            // refresh reads, and the recorded state would then describe data that was never
            // written. The ref is recorded alongside the snapshot so that freshness is later
            // checked against the ref that was read rather than against the main branch.
            val (uuid, ref, snapshotId) = r.table match {
              case sparkTable: SparkTable =>
                val pinnedSnapshotId = sparkTable.snapshotId()
                (
                  sparkTable.table().uuid().toString,
                  sparkTable.branch(),
                  if (pinnedSnapshotId != null) pinnedSnapshotId.longValue()
                  else RefreshState.NO_SNAPSHOT_ID)
              case _ =>
                val table = sourceCatalog.icebergCatalog().loadTable(icebergId)
                val snapshot = table.currentSnapshot()
                (
                  table.uuid().toString,
                  null,
                  if (snapshot != null) snapshot.snapshotId() else RefreshState.NO_SNAPSHOT_ID)
            }
            states += new SourceTableState(
              icebergId.name(),
              icebergId.namespace().levels().toList.asJava,
              sourceCatalogName(sourceCatalog),
              uuid,
              snapshotId,
              ref)
          } catch {
            case _: Exception => // skip tables we can't load
          }
        }
      case _ => // skip non-iceberg leaves
    }

    // Spark's analyzer replaces every view reference with a View node wrapping the view's
    // expanded query, including transitively for view-of-view chains, so a single pass over
    // the whole plan (not just its leaves) discovers every source view at every nesting depth.
    // Matching View nodes rather than the SubqueryAlias that wraps them keeps tables out of
    // this pass, since Spark aliases table references the same way.
    plan
      .collect { case view: org.apache.spark.sql.catalyst.plans.logical.View => view.desc }
      .foreach { desc =>
        val viewIdent = desc.identifier
        viewIdent.catalog.foreach { catalogName =>
          val key = "view:" + catalogName + "." + viewIdent.unquotedString
          if (seen.add(key)) {
            val icebergId =
              TableIdentifier.of(Namespace.of(viewIdent.database.toList: _*), viewIdent.table)
            try {
              val sourceCatalog = session.sessionState.catalogManager
                .catalog(catalogName)
                .asInstanceOf[HasIcebergCatalog]
              val view = sourceCatalog.icebergViewCatalog().loadView(icebergId)
              states += new SourceViewState(
                icebergId.name(),
                icebergId.namespace().levels().toList.asJava,
                sourceCatalogName(sourceCatalog),
                view.uuid().toString,
                view.currentVersion().versionId())
            } catch {
              case _: Exception => // not an Iceberg catalog, or the view can't be loaded
            }
          }
        }
      }

    states.toList
  }

  override def simpleString(maxFields: Int): String = {
    s"RefreshMaterializedViewExec: ${ident}"
  }
}
