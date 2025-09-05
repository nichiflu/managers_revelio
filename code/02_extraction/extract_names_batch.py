#!/usr/bin/env python3
"""
Extract a batch of names from Revelio based on row offset
Called by SGE array job with task ID

This script extracts user_id and fullname pairs from the revelio.individual_user table
in chunks of 10M rows each, using LIMIT/OFFSET for efficient batching.
"""

import sys
import os
import wrds
import pandas as pd
from datetime import datetime

def extract_batch(task_id, rows_per_batch=10_000_000):
    """
    Extract one batch of names based on task ID.
    
    Args:
        task_id: SGE task ID (1-65) determining which batch to extract
        rows_per_batch: Number of rows per batch (default 10M)
    """
    
    # Calculate offset based on task ID (task_id starts from 1)
    offset = (task_id - 1) * rows_per_batch
    
    print(f"Task {task_id}: Extracting rows {offset:,} to {offset + rows_per_batch:,}")
    print(f"Start time: {datetime.now()}")
    
    # Connect to WRDS
    print("Connecting to WRDS...")
    db = wrds.Connection(wrds_username=os.environ.get('USER'))
    
    # Extract batch using LIMIT/OFFSET
    # ORDER BY user_id ensures consistent chunking across runs
    query = f"""
    SELECT DISTINCT user_id, fullname
    FROM revelio.individual_user
    WHERE fullname IS NOT NULL
    ORDER BY user_id
    LIMIT {rows_per_batch}
    OFFSET {offset}
    """
    
    print(f"Executing query...")
    start_time = datetime.now()
    
    try:
        batch_df = db.raw_sql(query)
        query_time = (datetime.now() - start_time).total_seconds()
        print(f"Query completed in {query_time:.2f} seconds")
    except Exception as e:
        print(f"ERROR in task {task_id}: {e}")
        db.close()
        sys.exit(1)
    
    if len(batch_df) == 0:
        print(f"Task {task_id}: No data found (likely past end of table)")
        db.close()
        return
    
    # Remove any duplicates within this batch
    before_dedup = len(batch_df)
    batch_df = batch_df.drop_duplicates(subset=['user_id', 'fullname'])
    after_dedup = len(batch_df)
    
    if before_dedup != after_dedup:
        print(f"Removed {before_dedup - after_dedup} duplicates within batch")
    
    # Save batch to scratch
    output_dir = '/scratch/uncc/nichiflu/managers_revelio/name_batches'
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/batch_{task_id:04d}.parquet"
    batch_df.to_parquet(output_file, compression='snappy', index=False)
    
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Task {task_id}: Saved {len(batch_df):,} rows to {output_file}")
    print(f"File size: {file_size_mb:.2f} MB")
    
    # Save stats for tracking
    stats_file = f"{output_dir}/batch_{task_id:04d}_stats.txt"
    with open(stats_file, 'w') as f:
        f.write(f"task_id: {task_id}\n")
        f.write(f"offset: {offset}\n")
        f.write(f"rows: {len(batch_df)}\n")
        f.write(f"unique_users: {batch_df['user_id'].nunique()}\n")
        f.write(f"file_size_mb: {file_size_mb:.2f}\n")
        f.write(f"query_time_seconds: {query_time:.2f}\n")
        f.write(f"extraction_time: {datetime.now()}\n")
    
    db.close()
    print(f"Task {task_id}: Complete at {datetime.now()}")

if __name__ == "__main__":
    # Get task ID from SGE environment variable
    task_id = int(os.environ.get('SGE_TASK_ID', '1'))
    
    # For testing without SGE, can pass task_id as command line argument
    if len(sys.argv) > 1:
        task_id = int(sys.argv[1])
        print(f"Running in test mode with task_id={task_id}")
    
    # Extract this batch
    extract_batch(task_id)