# =============================================================================
# snowflake_to_iceberg_bulk_notebook.py
#
# Snowflake Notebook / Python worksheet version.
#
# Copies every table in a source Snowflake database into Apache Iceberg
# tables inside a catalog-linked database, mirroring the source schema
# structure. The source database is only ever read from (DESCRIBE TABLE /
# INFORMATION_SCHEMA / SELECT) -- nothing there is created, altered, or
# dropped.
#
# Run this as one or more cells in a Snowflake Notebook. No credentials
# needed -- it uses the session you're already logged into Snowsight with
# (same role, warehouse, etc. as your notebook session).
#
# Paste this whole thing into a Python cell (or split into a few cells --
# CONFIG / functions / run) and execute.
# =============================================================================

from snowflake.snowpark.context import get_active_session
import re

session = get_active_session()

# ===========================================================================
# CONFIG -- edit these values for your environment
# ===========================================================================

CONFIG = {
    # The Snowflake database you're copying FROM. Read-only; never modified.
    "source_database": "ADAM_REES",

    # The catalog-linked database you're copying INTO.
    "catalog_linked_database": "catalog_db_progressed_splinter",

    # Informational only (not used in SQL) -- helpful for confirming you're
    # pointed at the catalog you think you are.
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
# Type mapping (same rules as the single-table / connector versions)
# ===========================================================================

WARN_TYPES = ("VARIANT", "OBJECT", "ARRAY", "MAP", "GEOGRAPHY", "GEOMETRY")


def parse_type(raw_type: str):
    m = re.match(r"^([A-Z_]+)(\((.*)\))?$", raw_type.strip().upper())
    if not m:
        return raw_type.strip().upper(), []
    base = m.group(1)
    arg_str = m.group(3)
    args = [int(a.strip()) for a in arg_str.split(",")] if arg_str else []
    return base, args


def map_column(col_name: str, raw_type: str, ts_precision: int):
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
            f"Source primary key ({', '.join(primary_keys)}) is not carried over automatically."
        )

    sql = (
        f"CREATE ICEBERG TABLE {target_db}.{target_schema}.{target_table} (\n"
        + ",\n".join(col_defs)
        + "\n)\nAS\nSELECT\n"
        + ",\n".join(select_exprs)
        + f"\nFROM {source_fqn};"
    )
    return sql, warnings


# ===========================================================================
# Snowflake interaction (via the active Snowpark session)
# ===========================================================================

def list_schemas(session, source_db):
    rows = session.sql(
        f"""
        SELECT SCHEMA_NAME
        FROM {source_db}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA'
        ORDER BY SCHEMA_NAME
        """
    ).collect()
    return [row["SCHEMA_NAME"] for row in rows]


def list_tables(session, source_db, schema_name):
    rows = session.sql(
        f"""
        SELECT TABLE_NAME
        FROM {source_db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
    ).collect()
    return [row["TABLE_NAME"] for row in rows]


def describe_table(session, source_fqn):
    rows = session.sql(f"DESCRIBE TABLE {source_fqn}").collect()
    columns, primary_keys = [], []
    for row in rows:
        col_name = row["name"]
        col_type = row["type"]
        columns.append((col_name, col_type))
        if row["primary key"] == "Y":
            primary_keys.append(col_name)
    return columns, primary_keys


def schema_exists(session, target_db, schema_name):
    rows = session.sql(f"SHOW SCHEMAS LIKE '{schema_name}' IN DATABASE {target_db}").collect()
    return len(rows) > 0


def table_exists(session, target_db, schema_name, table_name):
    try:
        rows = session.sql(
            f"SHOW ICEBERG TABLES LIKE '{table_name}' IN SCHEMA {target_db}.{schema_name}"
        ).collect()
        return len(rows) > 0
    except Exception:
        return False


def create_target_schema(session, target_db, schema_name, dry_run):
    sql = f"CREATE SCHEMA {target_db}.{schema_name};"
    if dry_run:
        print(f"[DRY RUN] Would run: {sql}")
        return
    session.sql(sql).collect()
    print(f"Created schema {target_db}.{schema_name}")


# ===========================================================================
# Run
# ===========================================================================

def run(session, cfg):
    dry_run = cfg["dry_run"]
    ts_precision = cfg["iceberg_timestamp_precision"]
    source_db = cfg["source_database"]
    target_db = cfg["catalog_linked_database"]

    print(f"Source database:            {source_db}")
    print(f"Catalog-linked database:    {target_db}")
    print(f"Remote catalog (info only): {cfg['catalog_name']}")
    print(f"Dry run:                    {dry_run}")
    print("-" * 70)

    summary = {"schemas_created": [], "tables_created": [], "tables_skipped": [], "failures": []}

    schemas = list_schemas(session, source_db)

    for schema_name in schemas:
        if cfg["schema_include"] and schema_name not in cfg["schema_include"]:
            continue
        if schema_name in (cfg["schema_exclude"] or []):
            continue

        print(f"\n=== Schema: {schema_name} ===")

        try:
            if not schema_exists(session, target_db, schema_name):
                create_target_schema(session, target_db, schema_name, dry_run)
                summary["schemas_created"].append(schema_name)
            else:
                print(f"Schema {target_db}.{schema_name} already exists, skipping creation.")
        except Exception as e:
            print(f"FAILED to create schema {schema_name}: {e}")
            summary["failures"].append((schema_name, None, str(e)))
            continue

        tables = list_tables(session, source_db, schema_name)

        for table_name in tables:
            if cfg["table_include"] and table_name not in cfg["table_include"]:
                continue
            if table_name in (cfg["table_exclude"] or []):
                continue

            source_fqn = f"{source_db}.{schema_name}.{table_name}"

            try:
                if cfg["skip_existing_tables"] and not dry_run and table_exists(session, target_db, schema_name, table_name):
                    print(f"  {table_name}: already exists in target, skipping.")
                    summary["tables_skipped"].append(source_fqn)
                    continue

                columns, primary_keys = describe_table(session, source_fqn)
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
                    session.sql(sql).collect()
                    print(f"  Created {target_db}.{schema_name}.{table_name}")
                    summary["tables_created"].append(source_fqn)

            except Exception as e:
                print(f"  FAILED on {table_name}: {e}")
                summary["failures"].append((schema_name, table_name, str(e)))
                continue

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
        print("\nThis was a dry run. Set CONFIG['dry_run'] = False and re-run this cell to apply.")

    return summary


# Run it
summary = run(session, CONFIG)
