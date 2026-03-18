"""
Merges a downloaded VM database into the local database.
Inserts rows from the VM DB that don't already exist locally (by primary key).
Usage: python merge_db.py <vm_db_path>
"""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_PATH

TABLES = ["predictions", "incidents"]


def merge(vm_db_path: str, local_db_path: str = DB_PATH) -> None:
    vm = sqlite3.connect(vm_db_path)
    local = sqlite3.connect(local_db_path)

    for table in TABLES:
        rows = vm.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: 0 rows in VM DB, skipping")
            continue

        placeholders = ",".join(["?"] * len(rows[0]))
        cursor = local.executemany(
            f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", rows
        )
        local.commit()
        print(f"  {table}: {cursor.rowcount} new rows inserted")

    vm.close()
    local.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python merge_db.py <vm_db_path>")
        sys.exit(1)
    vm_path = sys.argv[1]
    if not os.path.exists(vm_path):
        print(f"ERROR: {vm_path} not found")
        sys.exit(1)
    print(f"Merging {vm_path} → {DB_PATH}")
    merge(vm_path)
    print("Done.")
