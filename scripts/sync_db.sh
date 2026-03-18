#!/bin/bash
# Downloads the VM database and merges it into the local database.
# Safe to run any time — never overwrites, only adds new rows.

set -e

PROJECT_DIR="/Users/andresbarrientos/Desktop/Master/U of Maryland/Spring 26/ENAI_603/WMATA_Delays_Project"
PYTHON="/Users/andresbarrientos/Desktop/Projects/.venv/bin/python"
SSH_KEY="$HOME/.ssh/oracle_wmata.key"
VM_USER="ubuntu"
VM_HOST="129.153.18.220"
VM_DB_PATH="~/WMATA_Delays_Project/data/wmata.db"
TMP_DB="/tmp/wmata_vm_$(date +%s).db"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting DB sync..."

# Download VM database
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
    "$VM_USER@$VM_HOST:$VM_DB_PATH" "$TMP_DB"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloaded VM DB to $TMP_DB"

# Merge into local database
"$PYTHON" "$PROJECT_DIR/scripts/merge_db.py" "$TMP_DB"

# Cleanup
rm "$TMP_DB"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sync complete."
