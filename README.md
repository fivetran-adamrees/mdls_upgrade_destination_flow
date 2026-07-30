# Snowflake → MDLS Migration Toolkit

Scripts and docs for migrating Fivetran connections off a Snowflake destination and
onto a Managed Data Lake Service (MDLS) destination, with the underlying data
converted from native Snowflake tables into Apache Iceberg tables along the way.

## Files in this kit

| File | Type | Runs where | Purpose |
|---|---|---|---|
| `snowflake-to-mdls-migration-runbook.md` | Doc | — | Step-by-step walkthrough of the full migration (start here) |
| `snowflake_to_iceberg_bulk.py` | Script | Local terminal | Bulk-copies every table in a source Snowflake database into Iceberg tables in a catalog-linked database |
| `snowflake_to_iceberg_bulk_notebook.py` | Script | Snowflake Notebook | Same as above, using your logged-in Snowsight session — no credentials needed |
| `move_connections_to_mdls.py` | Script | Local terminal | Bulk-moves Fivetran connections from the Snowflake destination group to the MDLS destination group |
| `README.md` | Doc | — | This file |

## Suggested order of operations

1. Read `snowflake-to-mdls-migration-runbook.md` for the full picture (destinations,
   catalog registration, catalog-linked database).
2. Run `snowflake_to_iceberg_bulk.py` **or** `snowflake_to_iceberg_bulk_notebook.py`
   (whichever fits your environment — see below) to migrate table data into Iceberg.
3. Run `move_connections_to_mdls.py` to re-point connections at the new MDLS
   destination.

## Which Iceberg-migration script do I use?

- **Local terminal, your own laptop/server** → `snowflake_to_iceberg_bulk.py`.
  Needs `pip install snowflake-connector-python --break-system-packages` and
  Snowflake account/user credentials (password, SSO, or key-pair auth).
- **Snowflake Notebook / Python worksheet in Snowsight** →
  `snowflake_to_iceberg_bulk_notebook.py`. Paste it into a Python cell — it uses
  `get_active_session()`, so no credentials are needed at all, just whatever role/
  warehouse your notebook session already has.

Both default to `dry_run: True` in their `CONFIG` dict — always review the printed SQL
before flipping it to `False`.

## Running `move_connections_to_mdls.py`

Requires `pip install requests --break-system-packages` and Fivetran API credentials:

```bash
export FIVETRAN_API_KEY=...
export FIVETRAN_API_SECRET=...
python move_connections_to_mdls.py --dry-run     # review first
python move_connections_to_mdls.py --execute     # then actually move connections
```

Edit the `CONFIG` dict at the top of the file first:
- `destination_group_id` — your MDLS destination's group ID (from `GET /v1/groups`)
- `connection_ids` — the connections you want to move (from `GET /v1/connections`)
- `sync_behavior` — `CONTINUE` (keep sync cursor, resume incrementally) or
  `BACKFILL` (reset cursor, full historical resync)

**Before a connection can be moved, Fivetran requires:**
- It's paused
- It has no transformations attached
- It doesn't use a hybrid deployment destination
- Its schema name doesn't already exist in the target destination group

The script checks pause status up front and reports a clear moved/skipped/failed
summary at the end rather than stopping on the first failure.

Want to run this one from inside a Snowflake Notebook instead of a local terminal?
It's possible but requires setting up an External Access Integration, network rule,
and secret first — ask and I can walk through that or adapt the script for it.

## Requirements summary

| Tool | Needed for |
|---|---|
| `snowflake-connector-python` | `snowflake_to_iceberg_bulk.py` only |
| `requests` | `move_connections_to_mdls.py` only |
| Snowflake catalog write credentials | Registering the catalog integration (runbook Step 3) |
| Fivetran API key/secret | `move_connections_to_mdls.py` |

## Notes

- None of the scripts modify or delete anything in the **source** Snowflake database —
  they only read from it (`DESCRIBE TABLE`, `INFORMATION_SCHEMA`, `SELECT`).
- Primary keys on source tables aren't carried over automatically to the new Iceberg
  tables; the migration script flags this as a warning per table if applicable.
- Fivetran's "Move a Connection" endpoint is currently in **beta** — check Fivetran's
  docs for current limitations before relying on it for anything business-critical.
