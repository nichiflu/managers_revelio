#!/bin/bash
# Submit all CEO extraction batch jobs to Grid Engine

echo '🚀 Submitting CEO extraction batch jobs...'

# Submit batch 1
qsub -N ceo_batch_01 batch_01.sh
echo '  Submitted batch 1'
sleep 2  # Small delay between submissions

# Submit batch 2
qsub -N ceo_batch_02 batch_02.sh
echo '  Submitted batch 2'
sleep 2  # Small delay between submissions

# Submit batch 3
qsub -N ceo_batch_03 batch_03.sh
echo '  Submitted batch 3'
sleep 2  # Small delay between submissions

# Submit batch 4
qsub -N ceo_batch_04 batch_04.sh
echo '  Submitted batch 4'
sleep 2  # Small delay between submissions

# Submit batch 5
qsub -N ceo_batch_05 batch_05.sh
echo '  Submitted batch 5'
sleep 2  # Small delay between submissions

echo '✅ All batch jobs submitted'
echo '📊 Check job status with: qstat -u $USER'
