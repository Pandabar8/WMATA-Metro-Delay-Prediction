#!/bin/bash
# WMATA Pipeline VM Setup — Ubuntu 22.04 ARM (Oracle Cloud Free Tier)
# Run once after first SSH login: bash deploy/setup.sh
set -e

REPO_URL="https://github.com/Pandabar8/WMATA-Metro-Delay-Prediction.git"
PROJECT_DIR="$HOME/WMATA_Delays_Project"

echo "=== 1. Updating system packages ==="
sudo apt-get update -qq && sudo apt-get upgrade -y -qq

echo "=== 2. Installing Python 3, pip, git ==="
sudo apt-get install -y python3 python3-pip python3-venv git

echo "=== 3. Cloning project repo ==="
if [ -d "$PROJECT_DIR" ]; then
    echo "Directory exists — pulling latest"
    cd "$PROJECT_DIR" && git pull
else
    git clone "$REPO_URL" "$PROJECT_DIR"
    cd "$PROJECT_DIR"
fi

echo "=== 4. Creating Python virtual environment ==="
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q
echo "Dependencies installed."

echo "=== 5. Creating data/ and logs/ directories ==="
mkdir -p data logs

echo "=== 6. Configuring log rotation ==="
sudo tee /etc/logrotate.d/wmata > /dev/null <<EOF
$PROJECT_DIR/logs/*.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
}
EOF
echo "Log rotation configured (weekly, keep 4 weeks)."

echo "=== 7. Initializing SQLite database ==="
cd scripts && ../.venv/bin/python init_db.py && cd ..
echo "Database initialized at data/wmata.db"

echo ""
echo "=== ACTION REQUIRED ==="
echo "Edit deploy/crontab.txt and fill in your API keys:"
echo "  WMATA_API_KEY=<your key>"
echo "  SENDGRID_API_KEY=<your sendgrid key>"
echo ""
read -p "Press Enter once you've updated deploy/crontab.txt..."

echo "=== 8. Installing crontab ==="
crontab deploy/crontab.txt
echo "Crontab installed. Verifying:"
crontab -l

echo ""
echo "=== Setup complete ==="
echo "Pipeline will start collecting in < 2 minutes."
echo "To download the DB locally: scp ubuntu@<VM_IP>:~/WMATA_Delays_Project/data/wmata.db ./data/wmata.db"
