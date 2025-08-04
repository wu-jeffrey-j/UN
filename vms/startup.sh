#!/bin/bash
set -e

# Config
LOG_FILE="/var/log/startup-script.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Startup script activated at $(date)"

VM_NAME=$(hostname)
GROUP_ID="${VM_NAME##*scraper}"
export VM_ID=$GROUP_ID

# Activate environment
CONDA_BASE="/opt/conda"
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate scrape

# Confirm environment
echo "Activated conda environment: $(conda info --envs)"

ulimit -n 50000

# ----- Run your Python script -----
cd /opt/jobs/UN

python3 scraper.py --max_workers 7

echo "Script completed successfully at $(date)"
