# Materialized Views PR Update Plan

This plan describes the changes needed to update PR #9830 to comply with the evolved materialized views spec (view-spec.md + spec.md Appendix H).

## Key Differences: PR vs. Spec

| Aspect | PR (current) | Spec (new) |
|--------|-------------|------------|
| **MV marker** | Boolean view property `iceberg.materialized.view` | Presence of `storage-table` on the view version |
| **Storage table pointer** | View property `iceberg.materialized.view.storage.table` (string) | View version field `storage-table` (struct: `namespace` + `name`) |
| **Refresh metadata location** | Table properties (`iceberg.base.snapshot.<uuid>`, `iceberg.materialized.view.version`) | Snapshot summary property `refresh-state` (JSON) |
| **Refresh metadata structure** | Flat key-value pairs (UUID -> snapshot ID) | Structured `RefreshState` with `view-version-id`, `source-states` list, `refresh-start-timestamp-ms` |
| **Source dependency tracking** | Only base table snapshot IDs | Typed sources: `table` (uuid, snapshot-id, namespace, name, catalog, ref) and `view` (uuid, version-id, namespace, name, catalog) |
| **Freshness evaluation** | Hard-coded binary check in `SparkCatalog.isFresh()` | Consumer-defined policy (spec leaves it to the consumer) |
| **Creation order** | View first, then storage table | Storage table first, then MV metadata (spec: "storage table must exist and be accessible before the materialized view metadata is committed") |
| **Initial state** | CREATE populates data immediately | Storage table starts with no snapshots; refresh is a separate step |
| **Default storage table name** | `<view_name>.storage.table` | `<view_name>__storage` (per spec example) |

---

## Phase 1: Core Metadata Model Changes

### 1.1 Add `storage-table` field to View Version

**Files:**
- `core/src/main/java/org/apache/iceberg/view/ViewVersion.java`
- `core/src/main/java/org/apache/iceberg/view/ViewVersionParser.java`
- `core/src/main/java/org/apache/iceberg/view/ImmutableViewVersion.java` (or equivalent)

**Changes:**
- Add an optional `storageTable()` method to `ViewVersion` returning a struct with `namespace` (list of strings) and `name` (string), or `null` for regular views
- Update JSON serialization/deserialization to read/write the `storage-table` field on each version
- Remove reliance on view-level properties `iceberg.materialized.view` and `iceberg.materialized.view.storage.table`

### 1.2 Create `RefreshState` model classes

**New files:**
- `core/src/main/java/org/apache/iceberg/view/RefreshState.java`
- `core/src/main/java/org/apache/iceberg/view/SourceTableState.java`
- `core/src/main/java/org/apache/iceberg/view/SourceViewState.java`
- `core/src/main/java/org/apache/iceberg/view/RefreshStateParser.java`

**Details:**

`RefreshState`:
| Field | Type | Description |
|-------|------|-------------|
| `view-version-id` | int | The version-id of the MV when the refresh was performed |
| `source-states` | list | List of `SourceTableState` / `SourceViewState` records |
| `refresh-start-timestamp-ms` | long | Timestamp when the refresh operation started |

`SourceTableState` (type = `"table"`):
| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | Must be `"table"` |
| `name` | yes | Table name |
| `namespace` | yes | List of namespace strings |
| `catalog` | no | Catalog name; null means same catalog as the MV |
| `uuid` | yes | UUID of the source table |
| `snapshot-id` | yes | Snapshot ID read during refresh |
| `ref` | no | Branch name; defaults to `"main"` |

`SourceViewState` (type = `"view"`):
| Field | Required | Description |
|-------|----------|-------------|
| `type` | yes | Must be `"view"` |
| `name` | yes | View name |
| `namespace` | yes | List of namespace strings |
| `catalog` | no | Catalog name; null means same catalog as the MV |
| `uuid` | yes | UUID of the source view |
| `version-id` | yes | Version ID read during refresh |

### 1.3 Update refresh metadata storage location

**Changes:**
- Refresh state is written as a JSON string to the **snapshot summary** under the key `refresh-state`
- Remove all usage of table property patterns:
  - `iceberg.base.snapshot.<uuid>` (table property)
  - `iceberg.materialized.view.version` (table property)

---

## Phase 2: `MaterializedViewUtil` Rewrite

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/MaterializedViewUtil.java`

### 2.1 Update `isMaterializedView()` check

- **Before:** `view.properties().get("iceberg.materialized.view").equals("true")`
- **After:** `view.currentVersion().storageTable() != null`

### 2.2 Rewrite refresh state read/write

- **Write:** Build a `RefreshState` object, serialize to JSON, and set it as the `refresh-state` key in the snapshot summary during the commit that populates/refreshes the storage table
- **Read:** Parse `refresh-state` from the storage table's current snapshot summary (not from table properties)

### 2.3 Update source dependency tracking

- Extend tracking beyond base tables to include:
  - Source views (`SourceViewState` with `uuid` and `version-id`)
  - Source materialized views (represented as two entries: one `view` entry for the MV itself + one `table` entry for its storage table)
- Include full identifier info: `namespace`, `name`, `catalog`, `uuid`
- For source tables, include optional `ref` (branch name)

### 2.4 Make freshness evaluation consumer-driven

- Replace the hard-coded `isFresh()` binary check with a pluggable policy
- The default policy can remain "compare all source snapshot/version IDs match current" but should be overridable
- Support time-based staleness windows using `refresh-start-timestamp-ms`
- Per spec: "Since different systems define freshness differently, it is left to the consumer to evaluate freshness based on its own policy"

---

## Phase 3: Storage Table Lifecycle Changes

### 3.1 Fix creation order

**File:** `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/CreateMaterializedViewExec.scala`

- **Before:** Creates view metadata first, then storage table, then populates data
- **After:** Create the storage table first, then commit the MV view metadata with `storage-table` pointing to it
- Spec requirement: "The storage table must exist and be accessible before the materialized view metadata is committed"

### 3.2 Separate creation from initial refresh

- A newly created MV should have a storage table with **no snapshots** (never-refreshed state)
- The initial data population (`INSERT INTO storage_table`) should be a separate refresh operation
- On refresh, write the `refresh-state` into the new snapshot's summary
- Per spec: "A storage table that has not yet been refreshed has no snapshots"
- Consumers can distinguish never-refreshed from refreshed-with-empty-result by the presence of a snapshot with `refresh-state`

### 3.3 Update default storage table naming

- **Before:** `<view_name>.storage.table`
- **After:** `<view_name>__storage` (matching the spec example)

---

## Phase 4: Spark Integration Updates

### 4.1 Update `SparkCatalog`

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`

- Detect MVs via `storage-table` on the view version (not view properties)
- Parse `refresh-state` from the storage table's current snapshot summary
- Make freshness checking consumer-policy-driven (see Phase 2.4)
- Handle the never-refreshed case (no snapshots on storage table)

### 4.2 Update `CreateMaterializedViewExec`

**File:** `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/CreateMaterializedViewExec.scala`

- Use the `ViewBuilder` API to set `storage-table` on the version (not as view properties)
- Implement the corrected creation order (storage table first, then MV metadata)
- On initial refresh, write `RefreshState` JSON to the snapshot summary
- Build `source-states` list with proper typed entries

### 4.3 Update `DropV2ViewExec`

**File:** `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DropV2ViewExec.scala`

- Detect MV status via `storage-table` field on the version (not `iceberg.materialized.view` property)
- Read storage table identifier from the version's `storage-table` struct (`namespace` + `name`)

### 4.4 Update `SparkMaterializedView`

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkMaterializedView.java`

- Adapt to the new metadata model (read storage table info from version, not properties)

### 4.5 Update parser and plan nodes

**Files:**
- `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/parser/extensions/IcebergSparkSqlExtensionsParser.scala`
- `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/views/CreateIcebergView.scala`
- `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/analysis/RewriteViewCommands.scala`
- `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/ExtendedDataSourceV2Strategy.scala`

- Pass `storage-table` struct through the plan nodes instead of a string identifier property
- Update `MaterializedViewOptions` to carry namespace + name

---

## Phase 5: Test Updates

**File:** `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMaterializedViews.java`

### 5.1 Metadata model assertions
- Verify `storage-table` is set on the view version (not as a view property)
- Verify no `iceberg.materialized.view` property exists
- Verify `refresh-state` appears in storage table snapshot summary (not as table properties)

### 5.2 Refresh state assertions
- Verify `RefreshState` JSON structure: `view-version-id`, `source-states`, `refresh-start-timestamp-ms`
- Verify source states include correct `type`, `namespace`, `name`, `uuid`, `snapshot-id` / `version-id`

### 5.3 New test cases
- Test source dependency tracking for views (not just tables)
- Test MV-to-MV dependencies (diamond dependency pattern)
- Test freshness with `refresh-start-timestamp-ms`
- Test creation order: storage table exists before MV metadata is committed
- Test never-refreshed state: newly created MV has storage table with no snapshots
- Test that freshness evaluation is consumer-driven (configurable)
- Test default storage table naming (`<name>__storage`)

### 5.4 Core model tests
- Add unit tests for `RefreshState` serialization/deserialization
- Add unit tests for `ViewVersion` with `storage-table` field
- Add tests for `SourceTableState` and `SourceViewState` parsing

---

## Summary of Properties/Fields Removed

| Removed | Replacement |
|---------|-------------|
| View property `iceberg.materialized.view` | `storage-table` field on view version (non-null = MV) |
| View property `iceberg.materialized.view.storage.table` | `storage-table` struct on view version |
| Table property `iceberg.base.snapshot.<uuid>` | `source-states` in `refresh-state` snapshot summary |
| Table property `iceberg.materialized.view.version` | `view-version-id` in `refresh-state` snapshot summary |

## Summary of New Structures

| New | Location |
|-----|----------|
| `storage-table` (namespace + name) | View version metadata |
| `refresh-state` JSON | Storage table snapshot summary |
| `RefreshState` (view-version-id, source-states, refresh-start-timestamp-ms) | Encoded in `refresh-state` |
| `SourceTableState` (type, name, namespace, catalog, uuid, snapshot-id, ref) | Inside `source-states` |
| `SourceViewState` (type, name, namespace, catalog, uuid, version-id) | Inside `source-states` |
