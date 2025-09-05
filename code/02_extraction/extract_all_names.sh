#!/bin/bash
#$ -N extract_names
#$ -t 1-65
#$ -tc 5
#$ -l h_vmem=16G
#$ -l h_rt=00:30:00
#$ -o /scratch/uncc/nichiflu/managers_revelio/logs/
#$ -e /scratch/uncc/nichiflu/managers_revelio/logs/
#$ -cwd
#$ -j y

# Extract all Revelio names in parallel batches
# This SGE array job extracts 611M+ names from revelio.individual_user table
# 
# Array job parameters:
#   -t 1-65: Run 65 tasks (611M rows / 10M per batch = ~65 batches)
#   -tc 5: Maximum 5 concurrent tasks (WRDS limit)
#
# Each task extracts 10M rows using LIMIT/OFFSET
# Total runtime: ~2-3 hours (65 batches / 5 concurrent × 10-15 min per batch)
#
# Output: 65 parquet files in /scratch/uncc/nichiflu/managers_revelio/name_batches/
#         Each file contains user_id and fullname columns

echo "========================================"
echo "REVELIO NAME EXTRACTION - BATCH ${SGE_TASK_ID}/65"
echo "Job ID: ${JOB_ID}.${SGE_TASK_ID}"
echo "Host: $HOSTNAME"
echo "User: $USER"
echo "Start: $(date)"
echo "========================================"

# Set up environment
export PROJECT_DIR="/home/uncc/nichiflu/managers_revelio"
export SCRATCH_DIR="/scratch/uncc/nichiflu/managers_revelio"

# Create necessary directories
mkdir -p "${SCRATCH_DIR}/name_batches"
mkdir -p "${SCRATCH_DIR}/logs"

# Change to project directory
cd "${PROJECT_DIR}"

# Show Python version
echo "Python version:"
python3 --version
echo ""

# Run extraction for this batch
echo "Extracting batch ${SGE_TASK_ID}..."
python3 "${PROJECT_DIR}/code/02_extraction/extract_names_batch.py"

# Check exit status
if [ $? -eq 0 ]; then
    echo "✓ Batch ${SGE_TASK_ID} completed successfully"
else
    echo "✗ Batch ${SGE_TASK_ID} failed with exit code $?"
fi

echo ""
echo "========================================"
echo "Batch ${SGE_TASK_ID} complete"
echo "End: $(date)"
echo "========================================"