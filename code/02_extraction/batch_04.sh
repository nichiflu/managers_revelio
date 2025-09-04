#!/bin/bash
#$ -N ceo_batch_04
#$ -pe openmp 2
#$ -l h_vmem=16G
#$ -l h_rt=02:00:00
#$ -j y
#$ -o /scratch/uncc/nichiflu_managers_revelio/logs/
#$ -m bea
#$ -M nichiflu@wharton.upenn.edu

# CEO LinkedIn Extraction - Batch 4

cd /home/uncc/nichiflu/managers_revelio
python3 code/02_extraction/extract_ceo_batch.py --batch 4
