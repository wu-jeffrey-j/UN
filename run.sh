#!/usr/bin/env bash

# Fail if anything fails
set -e

# Check if running on a GCP VM
if grep -q "Google" /sys/class/dmi/id/product_name 2>/dev/null; then
  echo "This script is for creating VMs and should not be run on a VM. Exiting."
  exit 1
fi

# -------------------------------
# CONFIGURATION
# -------------------------------

# Base name for VMs (e.g., ltk-hero-image)
BASE_NAME="un-scraper"

# Number of VMs to create
NUM_VMS=4

# GCP zone
ZONE="us-west1-b"

# Machine type
MACHINE_TYPE=n2-standard-4

# Custom image to boot from
IMAGE="scraper-image"

# Startup script file (local path)
STARTUP_SCRIPT="startup.sh"

# -------------------------------
# CREATE VMs
# -------------------------------

echo "Creating $NUM_VMS VMs with base name '$BASE_NAME' in zone $ZONE..."

# Loop over 1..NUM_VMS
for i in $(seq 1 $NUM_VMS); do
  VM_NAME="${BASE_NAME}${i}"

  gcloud compute instances create "$VM_NAME" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image="$IMAGE" \
    --boot-disk-size=300GB \
    --metadata-from-file=startup-script="$STARTUP_SCRIPT" \
    --scopes=storage-full \
    --tags=un-scrapers \
    --quiet &
done

# Wait for all background jobs to finish
wait

echo "All VMs created successfully."
