# Migrating Fivetran Connections from Snowflake to Managed Data Lake Service (MDLS)

## Goal

Move data currently landing in a Fivetran **Snowflake destination** into Apache Iceberg
tables (registered in a Polaris catalog, stored in S3), then re-point the underlying
Fivetran **connections** to a new **MDLS destination** — without losing sync history or
re-syncing everything from scratch.

## Prerequisites

- A Fivetran account with permissions to create destinations and move connections.
- A Snowflake account with permissions to register catalog integrations and create
  catalog-linked databases.
- Access to the Polaris catalog's **write credentials** (client ID / client secret) —
  read-only credentials will fail with an authorization error when creating namespaces
  or tables.
- Python 3 with `snowflake-connector-python` and `requests` installed, or access to a
  Snowflake Notebook (Snowpark is preinstalled there).

---

## Step 1 — Create a Fivetran Snowflake destination and sync data

Set up the Snowflake destination in Fivetran as usual and let your connections sync
into it. This is the source data we'll migrate out of Snowflake-native tables into
Iceberg.

## Step 2 — Create an MDLS destination in Fivetran

Create the new Managed Data Lake Service destination in Fivetran. This will become the
target destination group that connections are moved to in Step 6.

## Step 3 — Register the catalog in Snowflake

Register the Polaris catalog as a **catalog integration** in Snowflake, using the
**write-capable credentials** (not read-only) so Snowflake can create namespaces and
tables in the remote catalog, not just read from it:

```sql
CREATE OR REPLACE CATALOG INTEGRATION <integration_name>
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  REST_CONFIG = (
    CATALOG_URI = '<polaris_server_endpoint>'
    CATALOG_NAME = '<catalog_name>'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = '<client_id>'
    OAUTH_CLIENT_SECRET = '<client_secret>'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE;
```

Verify it authenticates correctly:

```sql
SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('<integration_name>');
```

## Step 4 — Create a catalog-linked database

Using the catalog integration from Step 3, create a catalog-linked database. This
database auto-syncs with the remote catalog and lets you create namespaces (schemas)
and Iceberg tables directly from Snowflake SQL:

```sql
CREATE DATABASE <catalog_linked_db_name>
  LINKED_CATALOG = (
    CATALOG = '<integration_name>'
  );
```

## Step 5 — Migrate data from Snowflake tables into Iceberg tables

Use the provided script to copy every table from the source Snowflake database into
Iceberg tables in the catalog-linked database, preserving the schema structure:

- **`snowflake_to_iceberg_bulk.py`** — run from a local terminal (needs Snowflake
  account/user/password or SSO credentials).
- **`snowflake_to_iceberg_bulk_notebook.py`** — run inside a Snowflake Notebook /
  Python worksheet in Snowsight (uses your logged-in session automatically, no
  credentials needed).

Both scripts:
- Only read from the source Snowflake database (`DESCRIBE TABLE` / `SELECT`) — nothing
  in the source is altered.
- Mirror the source database's schema structure into the catalog-linked database.
- Handle Iceberg-safe type conversions automatically (e.g. `TIMESTAMP_TZ` →
  `TIMESTAMP_LTZ`, sized `VARCHAR(n)` → unsized `VARCHAR`).
- Default to a dry run first so you can review the generated SQL before anything is
  created.

Once tables are created, they're registered in the Polaris catalog and their
underlying data/metadata files live in S3.

## Step 6 — Move Fivetran connections to the MDLS destination

Fivetran's [Move a Connection API](https://fivetran.com/docs/rest-api/api-reference/connections/connection-moved)
(currently in beta) re-points a connection from one destination group to another,
optionally preserving its sync cursor so it resumes incremental syncs instead of
re-syncing from scratch.

**Before moving a connection, it must meet these requirements:**
- The connection must be **paused**.
- The connection must **not** have transformations attached.
- The connection must **not** use a hybrid deployment destination.
- The connection's schema name must **not** already exist in the target destination
  group.

Use **`move_connections_to_mdls.py`** to move a batch of connections in one run. It:
- Looks up each connection's current pause status and schema before attempting the
  move, so failures are easier to diagnose.
- Supports `CONTINUE` (preserve sync cursor) or `BACKFILL` (reset cursor, full
  historical resync) via config.
- Defaults to a dry run so you can review what would be sent before executing.
- Logs a moved/skipped/failed summary at the end and keeps going if one connection
  fails.

```bash
export FIVETRAN_API_KEY=...
export FIVETRAN_API_SECRET=...
python move_connections_to_mdls.py --dry-run
# review output, then:
python move_connections_to_mdls.py --execute
```

---

## Scripts referenced

| Script | Where it runs | Purpose |
|---|---|---|
| `snowflake_to_iceberg_bulk.py` | Local terminal | Bulk-copy Snowflake tables → Iceberg tables (Step 5) |
| `snowflake_to_iceberg_bulk_notebook.py` | Snowflake Notebook | Same as above, using the active Snowsight session (Step 5) |
| `move_connections_to_mdls.py` | Local terminal | Bulk-move Fivetran connections to the MDLS destination (Step 6) |

## Notes / open items

- Primary key constraints on source tables are **not** carried over automatically to
  the Iceberg tables — flagged as a warning by the migration script if you need to add
  them back with `ALTER ICEBERG TABLE`.
- The "Move a Connection" endpoint is currently in **beta** — check current limitations
  before relying on it for anything business-critical.
- Some connectors can't be moved if their credentials have a dependency on the current
  destination group (e.g. AWS IAM trust policies or Google service principal configs
  set up during initial connector setup). The API returns a clear `400` error in that
  case.
