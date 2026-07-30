#!/usr/bin/env python3
"""
snowflake_to_iceberg.py

Duplicates an existing Snowflake table into an Apache Iceberg table inside a
catalog-linked database, generating a Snowflake-safe CREATE ICEBERG TABLE ...
AS SELECT statement automatically from the source table's DESCRIBE TABLE
output.

Handles the type-mapping quirks that Iceberg imposes on Snowflake types:
  - Sized VARCHAR(n) / CHAR(n) / STRING(n)   -> unsized VARCHAR
  - Sized BINARY(n) / VARBINARY(n)           -> unsized BINARY
  - TIMESTAMP_TZ(p)                          -> TIMESTAMP_LTZ(6) with a cast
                                                 (Iceberg has no per-value
                                                 offset type; instant in time
                                                 is preserved, original offset
                                                 is not)
  - TIMESTAMP_NTZ(p) / TIMESTAMP_LTZ(p)      -> same type at precision 6,
                                                 cast added if p != 6
  - TIME(p)                                  -> TIME(6), cast added if p != 6
  - NUMBER(p,s) / FLOAT / BOOLEAN / DATE     -> unchanged
  - VARIANT / OBJECT / ARRAY / MAP / GEOGRAPHY / GEOMETRY
                                              -> passed through as-is with a
                                                 warning (these need Iceberg
                                                 v3 and/or Snowflake-managed
                                                 catalog; verify support for
                                                 your remote catalog before
                                                 relying on this)

Requires: pip install snowflake-connector-python --break-system-packages

Usage:
    python snowflake_to_iceberg.py \\
        --source ADAM_REES.SALESFORCE_SANDBOX_LIGHTSPEED_TESTING.ACCOUNT \\
        --target-db catalog_db_progressed_splinter \\
        --target-schema SALESFORCE_SANDBOX_LIGHTSPEED_TESTING \\
        --account <your_account> \\
        --user <your_user> \\
        --warehouse <your_warehouse> \\
        --authenticator externalbrowser \\
        --dry-run

Drop --dry-run once you're happy with the generated SQL to actually execute it.

Connection credentials can also be supplied via environment variables instead
of CLI flags: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, SNOWFLAKE_AUTHENTICATOR.
"""

import argparse
import os
import re
import sys

import snowflake.connector


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

# Iceberg timestamp / time precision is fixed at microseconds (6) unless you
# opt into the v3 nanosecond preview. Keep this simple and standard by default.
ICEBERG_TIMESTAMP_PRECISION = 6

WARN_TYPES = ("VARIANT", "OBJECT", "ARRAY", "MAP", "GEOGRAPHY", "GEOMETRY")


def parse_type(raw_type: str):
    """Split a DESCRIBE TABLE type string like 'TIMESTAMP_TZ(9)' or
    'VARCHAR(18)' into (base_type, args) where args is a list of ints,
    e.g. ('TIMESTAMP_TZ', [9]) or ('VARCHAR', [18]) or ('NUMBER', [38, 0])."""
    m = re.match(r"^([A-Z_]+)(\((.*)\))?$", raw_type.strip().upper())
    if not m:
        return raw_type.strip().upper(), []
    base = m.group(1)
    arg_str = m.group(3)
    args = [int(a.strip()) for a in arg_str.split(",")] if arg_str else []
    return base, args


def map_column(col_name: str, raw_type: str):
    """Return (iceberg_column_ddl, select_expression, warning_or_None)."""
    base, args = parse_type(raw_type)
    warning = None

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
        # Snowflake stores these as NUMBER(38,0) internally; Iceberg maps
        # cleanly to NUMBER too.
        return f"{col_name} NUMBER(38,0)", col_name, None

    if base == "BOOLEAN":
        return f"{col_name} BOOLEAN", col_name, None

    if base == "DATE":
        return f"{col_name} DATE", col_name, None

    if base == "TIME":
        precision = args[0] if args else 9
        ddl = f"{col_name} TIME({ICEBERG_TIMESTAMP_PRECISION})"
        if precision == ICEBERG_TIMESTAMP_PRECISION:
            return ddl, col_name, None
        return ddl, f"{col_name}::TIME({ICEBERG_TIMESTAMP_PRECISION}) AS {col_name}", None

    if base == "TIMESTAMP_TZ":
        ddl = f"{col_name} TIMESTAMP_LTZ({ICEBERG_TIMESTAMP_PRECISION})"
        select = f"{col_name}::TIMESTAMP_LTZ({ICEBERG_TIMESTAMP_PRECISION}) AS {col_name}"
        warning = (
            f"{col_name}: TIMESTAMP_TZ -> TIMESTAMP_LTZ cast. The instant in "
            f"time is preserved; each row's original UTC offset is not."
        )
        return ddl, select, warning

    if base in ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP"):
        precision = args[0] if args else 9
        target_type = "TIMESTAMP_NTZ" if base in ("TIMESTAMP_NTZ", "TIMESTAMP") else "TIMESTAMP_LTZ"
        ddl = f"{col_name} {target_type}({ICEBERG_TIMESTAMP_PRECISION})"
        if precision == ICEBERG_TIMESTAMP_PRECISION:
            return ddl, col_name, None
        select = f"{col_name}::{target_type}({ICEBERG_TIMESTAMP_PRECISION}) AS {col_name}"
        return ddl, select, None

    if base in WARN_TYPES:
        warning = (
            f"{col_name}: type {base} needs Iceberg v3 and/or a "
            f"Snowflake-managed catalog. Verify your remote catalog "
            f"supports it before relying on this column."
        )
        return f"{col_name} {base}", col_name, warning

    # Fallback: pass the raw type through and flag it for manual review.
    warning = f"{col_name}: unrecognized type '{raw_type}', passed through as-is. Please review."
    return f"{col_name} {raw_type}", col_name, warning


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


def describe_table(cursor, source_table: str):
    cursor.execute(f"DESCRIBE TABLE {source_table}")
    cols = cursor.fetchall()
    col_names = [d[0] for d in cursor.description]
    name_idx = col_names.index("name")
    type_idx = col_names.index("type")
    pk_idx = col_names.index("primary key") if "primary key" in col_names else None

    columns = []
    primary_keys = []
    for row in cols:
        col_name = row[name_idx]
        col_type = row[type_idx]
        columns.append((col_name, col_type))
        if pk_idx is not None and row[pk_idx] == "Y":
            primary_keys.append(col_name)
    return columns, primary_keys


def build_ddl(source_table, target_db, target_schema, target_table, columns, primary_keys):
    col_defs = []
    select_exprs = []
    warnings = []

    for col_name, col_type in columns:
        ddl, select_expr, warning = map_column(col_name, col_type)
        col_defs.append(f"    {ddl}")
        select_exprs.append(f"    {select_expr}")
        if warning:
            warnings.append(warning)

    if primary_keys:
        warnings.append(
            f"Source primary key ({', '.join(primary_keys)}) is not carried "
            f"over automatically. Use ALTER ICEBERG TABLE afterwards if you "
            f"need it reflected as metadata."
        )

    ddl_sql = (
        f"CREATE ICEBERG TABLE {target_table} (\n"
        + ",\n".join(col_defs)
        + "\n)\nAS\nSELECT\n"
        + ",\n".join(select_exprs)
        + f"\nFROM {source_table};"
    )

    use_sql = f"USE DATABASE {target_db};\nUSE SCHEMA {target_schema};\n\n"
    return use_sql + ddl_sql, warnings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="Fully qualified source table, e.g. DB.SCHEMA.TABLE")
    parser.add_argument("--target-db", required=True, help="Catalog-linked database name")
    parser.add_argument("--target-schema", required=True, help="Schema (namespace) inside the catalog-linked database")
    parser.add_argument("--target-table", default=None, help="Target table name (defaults to the source table name)")
    parser.add_argument("--account", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--warehouse", default=None)
    parser.add_argument("--role", default=None)
    parser.add_argument("--authenticator", default=None, help="e.g. externalbrowser, snowflake, oauth")
    parser.add_argument("--dry-run", action="store_true", help="Print the generated SQL without executing it")
    args = parser.parse_args()

    target_table = args.target_table or args.source.split(".")[-1]

    conn = connect(args)
    try:
        cursor = conn.cursor()
        columns, primary_keys = describe_table(cursor, args.source)
        sql, warnings = build_ddl(
            args.source, args.target_db, args.target_schema, target_table, columns, primary_keys
        )

        print("=" * 70)
        print("Generated SQL:")
        print("=" * 70)
        print(sql)
        print("=" * 70)

        if warnings:
            print("\nWarnings:")
            for w in warnings:
                print(f"  - {w}")

        if args.dry_run:
            print("\n--dry-run set: not executing. Re-run without --dry-run to create the table.")
            return

        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        print(f"\nCreated {args.target_db}.{args.target_schema}.{target_table} from {args.source}.")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
