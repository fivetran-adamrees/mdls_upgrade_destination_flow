#!/usr/bin/env python3
"""
move_connections_to_mdls.py

Moves a list of Fivetran connections from their current (Snowflake)
destination group to a new Managed Data Lake Service (MDLS) destination
group, using the "Move a Connection" REST API endpoint:

    POST https://api.fivetran.com/v1/connections/{connection_id}/move

Requirements enforced by the Fivetran API (the script does NOT bypass these
-- if a connection fails one of them, the API call will just error out and
the script logs it and moves on):
    - The connection must be paused before calling this endpoint.
    - The connection must not have transformations attached.
    - The connection must not use a hybrid deployment destination.
    - The connection's schema name must not already exist in the target
      destination group.

Requires: pip install requests --break-system-packages

--------------------------------------------------------------------------
CONFIGURE THIS SECTION, then run:  python move_connections_to_mdls.py
--------------------------------------------------------------------------
"""

import os
import sys
import time
import argparse

import requests

# ===========================================================================
# CONFIG -- edit these values for your environment
# ===========================================================================

CONFIG = {
    # The destination group ID for your new MDLS destination.
    # Find it via GET https://api.fivetran.com/v1/groups
    "destination_group_id": "REPLACE_ME_MDLS_GROUP_ID",

    # CONTINUE preserves the existing sync cursor (incremental sync resumes
    # where it left off). BACKFILL resets the cursor and triggers a full
    # historical resync into the new destination.
    "sync_behavior": "CONTINUE",   # or "BACKFILL"

    # The connection IDs to move. Get these from:
    #   GET https://api.fivetran.com/v1/connections  (List All Connections)
    # or from the Fivetran dashboard URL when viewing a connection.
    "connection_ids": [
        "4107c213907114059a5544ad8fa66c52",
        # add more connection IDs here
    ],

    # Seconds to wait between API calls (basic rate-limit courtesy).
    "request_delay_seconds": 0.5,

    # If True, print what would be sent without actually calling the API.
    "dry_run": True,
}

# ===========================================================================
# End of config
# ===========================================================================

BASE_URL = "https://api.fivetran.com/v1"


def get_credentials(args):
    api_key = args.api_key or os.environ.get("FIVETRAN_API_KEY")
    api_secret = args.api_secret or os.environ.get("FIVETRAN_API_SECRET")
    if not api_key or not api_secret:
        sys.exit(
            "Missing Fivetran API credentials. Set FIVETRAN_API_KEY / "
            "FIVETRAN_API_SECRET environment variables, or pass "
            "--api-key / --api-secret."
        )
    return api_key, api_secret


def get_connection_status(session, connection_id):
    """Look up a connection's current pause status and schema name so we can
    give a clearer error than the raw API response if a precondition isn't met."""
    resp = session.get(f"{BASE_URL}/connections/{connection_id}")
    if resp.status_code != 200:
        return None
    data = resp.json().get("data", {})
    return {
        "paused": data.get("paused"),
        "schema": data.get("schema"),
        "service": data.get("service"),
    }


def move_connection(session, connection_id, destination_group_id, sync_behavior, dry_run):
    payload = {
        "destination_group_id": destination_group_id,
        "sync_behavior": sync_behavior,
    }

    if dry_run:
        print(f"[DRY RUN] Would POST {BASE_URL}/connections/{connection_id}/move")
        print(f"           payload: {payload}")
        return {"status": "dry_run"}

    resp = session.post(f"{BASE_URL}/connections/{connection_id}/move", json=payload)

    if resp.status_code == 200:
        result = resp.json()
        return {"status": "success", "response": result}
    else:
        try:
            error_body = resp.json()
        except ValueError:
            error_body = resp.text
        return {"status": "error", "code": resp.status_code, "response": error_body}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", default=None, help="Fivetran API key (or set FIVETRAN_API_KEY)")
    parser.add_argument("--api-secret", default=None, help="Fivetran API secret (or set FIVETRAN_API_SECRET)")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=None)
    parser.add_argument("--execute", dest="dry_run", action="store_false", help="Override CONFIG['dry_run'] and actually run")
    args = parser.parse_args()

    cfg = CONFIG
    dry_run = cfg["dry_run"] if args.dry_run is None else args.dry_run
    api_key, api_secret = get_credentials(args)

    session = requests.Session()
    session.auth = (api_key, api_secret)
    session.headers.update({"Accept": "application/json;version=2"})

    print(f"Target destination group: {cfg['destination_group_id']}")
    print(f"Sync behavior:            {cfg['sync_behavior']}")
    print(f"Connections to move:      {len(cfg['connection_ids'])}")
    print(f"Dry run:                  {dry_run}")
    print("-" * 70)

    summary = {"moved": [], "skipped": [], "failed": []}

    for connection_id in cfg["connection_ids"]:
        print(f"\n--- {connection_id} ---")

        status = get_connection_status(session, connection_id)
        if status is None:
            print("  Could not retrieve connection details (check the connection ID). Skipping.")
            summary["skipped"].append(connection_id)
            continue

        print(f"  service={status['service']}  schema={status['schema']}  paused={status['paused']}")

        if not status["paused"] and not dry_run:
            print("  SKIPPED: connection must be paused before it can be moved.")
            summary["skipped"].append(connection_id)
            continue

        result = move_connection(session, connection_id, cfg["destination_group_id"], cfg["sync_behavior"], dry_run)

        if result["status"] == "success":
            print(f"  Moved successfully: {result['response']['data']}")
            summary["moved"].append(connection_id)
        elif result["status"] == "dry_run":
            pass
        else:
            print(f"  FAILED ({result['code']}): {result['response']}")
            summary["failed"].append((connection_id, result["response"]))

        time.sleep(cfg["request_delay_seconds"])

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Moved:   {len(summary['moved'])}")
    print(f"Skipped: {len(summary['skipped'])}")
    print(f"Failed:  {len(summary['failed'])}")
    if summary["failed"]:
        for connection_id, err in summary["failed"]:
            print(f"  - {connection_id}: {err}")

    if dry_run:
        print("\nThis was a dry run. Set CONFIG['dry_run'] = False (or pass --execute) to apply.")


if __name__ == "__main__":
    sys.exit(main())
