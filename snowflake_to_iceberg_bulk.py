#!/usr/bin/env python3
"""
snowflake_to_iceberg_bulk.py

Copies every table in a source Snowflake database into Apache Iceberg tables
inside a catalog-linked database, one schema at a time, mirroring the source
schema structure. The source database is only ever read from (DESCRIBE TABLE
and SELECT) -- nothing in the source database is created, altered, or dropped.

For each source schema:
    1. Create a matching schema in the catalog-linked database if it doesn't
       already exist (this registers a namespace in the remote catalog too).
    2. For each base table in that schema, generate and run a
       CREATE ICEBERG TABLE ... AS SELECT statement, applying Iceberg-safe
       type mapping (see snowflake_to_iceberg.py for the single-table version
       this was built from).

Requires: pip install snowflake-connector-python --break-system-packages

--------------------------------------------------------------------------
CONFIGURE THIS SECTION, then run:  python snowflake_to_iceberg_bulk.py
--------------------------------------------------------------------------
"""

import os
import re
import sys
import argparse

import snowflake.connector


# ===========================================================================
# CONFIG -- edit these values for your environment
# ===========================================================================

CONFIG = {
    # The Snowflake database you're copying FROM. Read-only; never modified.
    "source_database": "ADAM_REES",

    # The catalog-linked database you're copying INTO.
    "catalog_linked_database": "catalog_db_progressed_splinter",

    # Informational only (not used in SQL) -- helpful for logging/sanity
    # checking that you're pointed at the catalog you think you are.
    "catalog_name": "fivetran_catalog_progressed_splinter",

    # Restrict which source schemas to copy. Leave both as None/[] to copy
    # every schema in the source database (except INFORMATION_SCHEMA).
    "schema_include": None,          # e.g. ["SALESFORCE_SANDBOX_LIGHTSPEED_TESTING"]
    "schema_exclude": [],            # e.g. ["SCRATCH", "TEMP"]

    # Restrict which tables to copy (matched by unqualified table name).
    # Leave both as None/[] to copy every base table found.
    "table_include": None,           # e.g. ["ACCOUNT", "ACCOUNT_CONTACT_RELATION"]
    "table_exclude": [],             # e.g. ["HUGE_STAGING_TABLE"]

    # If a table with the same name already exists in the target schema,
    # skip it rather than fail (CREATE OR REPLACE isn't supported for
    # externally managed Iceberg tables).
    "skip_existing_tables": True,

    # Iceberg's standard timestamp/time precision is microseconds (6).
    "iceberg_timestamp_precision": 6,

    # If True, print the SQL that would run for every schema/table but don't
    # execute anything. Recommended for your first pass over a new database.
    "dry_run": True,
}

# ===========================================================================
# End of config
# ===========================================================================


WARN_TYPES = ("VARIANT", "OBJECT", "ARRAY", "MAP", "GEOGRAPHY", "GEOMETRY")


def parse_type(raw_type: str):
    """'TIMESTAMP_TZ(9)' -> ('TIMESTAMP_TZ', [9]); 'BOOLEAN' -> ('BOOLEAN', [])."""
    m = re.match(r"^([A-Z_]+)(\((.*)\))?$", raw_type.strip().upper())
    if not m:
        return raw_type.strip().upper(), []
    base = m.group(1)
    arg_str = m.group(3)
    args = [int(a.strip()) for a in arg_str.split(",")] if arg_str else []
    return base, args


def map_column(col_name: str, raw_type: str, ts_precision: int):
    """Return (iceberg_column_ddl, select_expression, warning_or_None)."""
    base, args = parse_type(raw_type)

    if base in ("VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "NCHAR", "NVARCHAR", "NVARCHAR2", "VARCHAR2"):
        return f"{col_name} VARCHAR", col_name, None

    if base in ("BINARY", "VARBINARY"):
        return f"{col_name} BINARY", col_name, None

    if base in ("NUMBER", "DECIMAL", "NUMERIC"):
        if len(args) == 2:
            return f"{col_name} NUMBER({args[0]},{args[1]})", col_name, None
        return f"{col_name} NUMBER", col_name, None

    if base in ("FLOAT", "DOUBLE", "REAL", "FLOAT4", "FLOAT8"):
        return f"{col_name} FLOAT", col_name, None

    if base in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return f"{col_name} NUMBER(38,0)", col_name, None

    if base == "BOOLEAN":
        return f"{col_name} BOOLEAN", col_name, None

    if base == "DATE":
        return f"{col_name} DATE", col_name, None

    if base == "TIME":
        precision = args[0] if args else 9
        ddl = f"{col_name} TIME({ts_precision})"
        if precision == ts_precision:
            return ddl, col_name, None
        return ddl, f"{col_name}::TIME({ts_precision}) AS {col_name}", None

    if base == "TIMESTAMP_TZ":
        ddl = f"{col_name} TIMESTAMP_LTZ({ts_precision})"
        select = f"{col_name}::TIMESTAMP_LTZ({ts_precision}) AS {col_name}"
        warning = (
            f"{col_name}: TIMESTAMP_TZ -> TIMESTAMP_LTZ cast. Instant in time "
            f"is preserved; each row's original UTC offset is not."
        )
        return ddl, select, warning

    if base in ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP"):
        precision = args[0] if args else 9
        target_type = "TIMESTAMP_NTZ" if base in ("TIMESTAMP_NTZ", "TIMESTAMP") else "TIMESTAMP_LTZ"
        ddl = f"{col_name} {target_type}({ts_precision})"
        if precision == ts_precision:
            return ddl, col_name, None
        return ddl, f"{col_name}::{target_type}({ts_precision}) AS {col_name}", None

    if base in WARN_TYPES:
        warning = (
            f"{col_name}: type {base} needs Iceberg v3 and/or a "
            f"Snowflake-managed catalog. Verify your remote catalog supports "
            f"it before relying on this column."
        )
        return f"{col_name} {base}", col_name, warning

    warning = f"{col_name}: unrecognized type '{raw_type}', passed through as-is. Please review."
    return f"{col_name} {raw_type}", col_name, warning


def build_table_ddl(source_fqn, target_db, target_schema, target_table, columns, primary_keys, ts_precision):
    col_defs, select_exprs, warnings = [], [], []

    for col_name, col_type in columns:
        ddl, select_expr, warning = map_column(col_name, col_type, ts_precision)
        col_defs.append(f"    {ddl}")
        select_exprs.append(f"    {select_expr}")
        if warning:
            warnings.append(warning)

    if primary_keys:
        warnings.append(
            f"Source primary key ({', '.join(primary_keys)}) is not carried "
            f"over automatically."
        )

    sql = (
        f"CREATE ICEBERG TABLE {target_db}.{target_schema}.{target_table} (\n"
        + ",\n".join(col_defs)
        + "\n)\nAS\nSELECT\n"
        + ",\n".join(select_exprs)
        + f"\nFROM {source_fqn};"
    )
    return sql, warnings


# ---------------------------------------------------------------------------
# Snowflake interaction
# ---------------------------------------------------------------------------

def connect(args):
    conn_params = dict(
        account=args.account or os.environ["SNOWFLAKE_ACCOUNT"],
        user=args.user or os.environ["SNOWFLAKE_USER"],
        warehouse=args.warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
        role=args.role or os.environ.get("SNOWFLAKE_ROLE"),
    )
    authenticator = args.authenticator or os.environ.get("SNOWFLAKE_AUTHENTICATOR")
    if authenticator:
        conn_params["authenticator"] = authenticator
    else:
        conn_params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**conn_params)


def list_schemas(cursor, source_db):
    cursor.execute(
        f"""
        SELECT SCHEMA_NAME
        FROM {source_db}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA'
        ORDER BY SCHEMA_NAME
        """
    )
    return [row[0] for row in cursor.fetchall()]


def list_tables(cursor, source_db, schema_name):
    cursor.execute(
        f"""
        SELECT TABLE_NAME
        FROM {source_db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (schema_name,),
    )
    return [row[0] for row in cursor.fetchall()]


def describe_table(cursor, source_fqn):
    cursor.execute(f"DESCRIBE TABLE {source_fqn}")
    rows = cursor.fetchall()
    col_names = [d[0] for d in cursor.description]
    name_idx = col_names.index("name")
    type_idx = col_names.index("type")
    pk_idx = col_names.index("primary key") if "primary key" in col_names else None

    columns, primary_keys = [], []
    for row in rows:
        columns.append((row[name_idx], row[type_idx]))
        if pk_idx is not None and row[pk_idx] == "Y":
            primary_keys.append(row[name_idx])
    return columns, primary_keys


def schema_exists(cursor, target_db, schema_name):
    cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name}' IN DATABASE {target_db}")
    return len(cursor.fetchall()) > 0


def table_exists(cursor, target_db, schema_name, table_name):
    try:
        cursor.execute(f"SHOW ICEBERG TABLES LIKE '{table_name}' IN SCHEMA {target_db}.{schema_name}")
        return len(cursor.fetchall()) > 0
    except snowflake.connector.errors.ProgrammingError:
        # Schema doesn't exist yet, so the table can't either.
        return False


def create_target_schema(cursor, target_db, schema_name, dry_run):
    sql = f"CREATE SCHEMA {target_db}.{schema_name};"
    if dry_run:
        print(f"[DRY RUN] Would run: {sql}")
        return
    cursor.execute(sql)
    print(f"Created schema {target_db}.{schema_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--warehouse", default=None)
    parser.add_argument("--role", default=None)
    parser.add_argument("--authenticator", default=None, help="e.g. externalbrowser, snowflake, oauth")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=None)
    parser.add_argument("--execute", dest="dry_run", action="store_false", help="Override CONFIG['dry_run'] and actually run")
    args = parser.parse_args()

    cfg = CONFIG
    dry_run = cfg["dry_run"] if args.dry_run is None else args.dry_run
    ts_precision = cfg["iceberg_timestamp_precision"]
    source_db = cfg["source_database"]
    target_db = cfg["catalog_linked_database"]

    print(f"Source database:            {source_db}")
    print(f"Catalog-linked database:    {target_db}")
    print(f"Remote catalog (info only): {cfg['catalog_name']}")
    print(f"Dry run:                    {dry_run}")
    print("-" * 70)

    conn = connect(args)
    summary = {"schemas_created": [], "tables_created": [], "tables_skipped": [], "failures": []}

    try:
        cursor = conn.cursor()
        schemas = list_schemas(cursor, source_db)

        for schema_name in schemas:
            if cfg["schema_include"] and schema_name not in cfg["schema_include"]:
                continue
            if schema_name in (cfg["schema_exclude"] or []):
                continue

            print(f"\n=== Schema: {schema_name} ===")

            try:
                if not schema_exists(cursor, target_db, schema_name):
                    create_target_schema(cursor, target_db, schema_name, dry_run)
                    summary["schemas_created"].append(schema_name)
                else:
                    print(f"Schema {target_db}.{schema_name} already exists, skipping creation.")
            except Exception as e:
                print(f"FAILED to create schema {schema_name}: {e}")
                summary["failures"].append((schema_name, None, str(e)))
                continue  # can't create tables in a schema that failed

            tables = list_tables(cursor, source_db, schema_name)

            for table_name in tables:
                if cfg["table_include"] and table_name not in cfg["table_include"]:
                    continue
                if table_name in (cfg["table_exclude"] or []):
                    continue

                source_fqn = f"{source_db}.{schema_name}.{table_name}"

                try:
                    if cfg["skip_existing_tables"] and not dry_run and table_exists(cursor, target_db, schema_name, table_name):
                        print(f"  {table_name}: already exists in target, skipping.")
                        summary["tables_skipped"].append(source_fqn)
                        continue

                    columns, primary_keys = describe_table(cursor, source_fqn)
                    sql, warnings = build_table_ddl(
                        source_fqn, target_db, schema_name, table_name, columns, primary_keys, ts_precision
                    )

                    print(f"\n  --- {table_name} ---")
                    print(sql)
                    for w in warnings:
                        print(f"    WARNING: {w}")

                    if dry_run:
                        print(f"  [DRY RUN] Not executed.")
                    else:
                        cursor.execute(sql)
                        print(f"  Created {target_db}.{schema_name}.{table_name}")
                        summary["tables_created"].append(source_fqn)

                except Exception as e:
                    print(f"  FAILED on {table_name}: {e}")
                    summary["failures"].append((schema_name, table_name, str(e)))
                    continue  # keep going with the next table

    finally:
        conn.close()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Schemas created: {len(summary['schemas_created'])}")
    print(f"Tables created:  {len(summary['tables_created'])}")
    print(f"Tables skipped:  {len(summary['tables_skipped'])}")
    print(f"Failures:        {len(summary['failures'])}")
    if summary["failures"]:
        for schema_name, table_name, err in summary["failures"]:
            where = f"{schema_name}.{table_name}" if table_name else schema_name
            print(f"  - {where}: {err}")

    if dry_run:
        print("\nThis was a dry run. Re-run with --execute (or set CONFIG['dry_run'] = False) to apply.")


if __name__ == "__main__":
    sys.exit(main())
