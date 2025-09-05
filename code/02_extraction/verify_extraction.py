#!/usr/bin/env python3
"""
Verify all batches were extracted successfully
Run this after the SGE array job completes to check for missing batches
"""

import os
import glob
import sys

def verify_extraction(batch_dir='/scratch/uncc/nichiflu/managers_revelio/name_batches', 
                     expected_batches=65):
    """
    Check that all expected batches exist and compile statistics.
    
    Args:
        batch_dir: Directory containing batch files
        expected_batches: Number of expected batch files (default 65)
    """
    
    print("=" * 60)
    print("REVELIO NAME EXTRACTION VERIFICATION")
    print("=" * 60)
    
    # Find all batch files
    batch_files = sorted(glob.glob(f"{batch_dir}/batch_*.parquet"))
    stats_files = sorted(glob.glob(f"{batch_dir}/batch_*_stats.txt"))
    
    print(f"\nExpected batches: {expected_batches}")
    print(f"Found batch files: {len(batch_files)}")
    print(f"Found stats files: {len(stats_files)}")
    
    # Check for missing batches
    found_ids = set()
    for batch_file in batch_files:
        # Extract task ID from filename
        filename = os.path.basename(batch_file)
        task_id = int(filename.replace('batch_', '').replace('.parquet', ''))
        found_ids.add(task_id)
    
    missing_ids = set(range(1, expected_batches + 1)) - found_ids
    
    if missing_ids:
        print(f"\n⚠️  MISSING BATCHES: {sorted(missing_ids)}")
        print("You need to rerun these specific tasks:")
        print(f"  qsub -t {','.join(map(str, sorted(missing_ids)))} extract_all_names.sh")
    else:
        print("\n✓ All batches extracted successfully!")
    
    # Calculate total statistics
    print("\n" + "=" * 60)
    print("EXTRACTION STATISTICS")
    print("=" * 60)
    
    total_rows = 0
    total_unique_users = 0
    total_size_mb = 0
    total_query_time = 0
    batch_stats = []
    
    for stats_file in stats_files:
        stats = {}
        with open(stats_file, 'r') as f:
            for line in f:
                if ':' in line:
                    key, value = line.strip().split(':', 1)
                    stats[key.strip()] = value.strip()
        
        if 'rows' in stats:
            rows = int(stats['rows'])
            total_rows += rows
        
        if 'unique_users' in stats:
            unique_users = int(stats['unique_users'])
            total_unique_users += unique_users
        
        if 'file_size_mb' in stats:
            size_mb = float(stats['file_size_mb'])
            total_size_mb += size_mb
        
        if 'query_time_seconds' in stats:
            query_time = float(stats['query_time_seconds'])
            total_query_time += query_time
        
        batch_stats.append(stats)
    
    print(f"\nTotal rows extracted: {total_rows:,}")
    print(f"Total unique users: {total_unique_users:,}")
    print(f"Total size: {total_size_mb:.2f} MB ({total_size_mb/1024:.2f} GB)")
    
    if len(batch_stats) > 0:
        avg_rows_per_batch = total_rows / len(batch_stats)
        avg_size_per_batch = total_size_mb / len(batch_stats)
        avg_query_time = total_query_time / len(batch_stats)
        
        print(f"\nAverage per batch:")
        print(f"  Rows: {avg_rows_per_batch:,.0f}")
        print(f"  Size: {avg_size_per_batch:.2f} MB")
        print(f"  Query time: {avg_query_time:.2f} seconds")
    
    # Show details of first and last few batches
    if batch_stats:
        print("\n" + "=" * 60)
        print("SAMPLE BATCH DETAILS")
        print("=" * 60)
        
        print("\nFirst 3 batches:")
        for i in range(min(3, len(batch_stats))):
            stats = batch_stats[i]
            if 'task_id' in stats:
                print(f"  Batch {stats['task_id']}: {stats.get('rows', 'N/A')} rows, "
                      f"{stats.get('file_size_mb', 'N/A')} MB")
        
        if len(batch_stats) > 6:
            print("\nLast 3 batches:")
            for i in range(max(0, len(batch_stats) - 3), len(batch_stats)):
                stats = batch_stats[i]
                if 'task_id' in stats:
                    print(f"  Batch {stats['task_id']}: {stats.get('rows', 'N/A')} rows, "
                          f"{stats.get('file_size_mb', 'N/A')} MB")
    
    # Create summary file
    summary_file = f"{batch_dir}/extraction_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("REVELIO NAME EXTRACTION SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Total batches expected: {expected_batches}\n")
        f.write(f"Total batches found: {len(batch_files)}\n")
        f.write(f"Missing batches: {sorted(missing_ids) if missing_ids else 'None'}\n\n")
        f.write(f"Total rows: {total_rows:,}\n")
        f.write(f"Total unique users: {total_unique_users:,}\n")
        f.write(f"Total size: {total_size_mb:.2f} MB ({total_size_mb/1024:.2f} GB)\n\n")
        
        if missing_ids:
            f.write("TO RERUN MISSING BATCHES:\n")
            f.write(f"qsub -t {','.join(map(str, sorted(missing_ids)))} extract_all_names.sh\n")
    
    print(f"\n✓ Summary saved to {summary_file}")
    
    # Return status
    return len(missing_ids) == 0

if __name__ == "__main__":
    # Allow custom batch directory as argument
    if len(sys.argv) > 1:
        batch_dir = sys.argv[1]
    else:
        batch_dir = '/scratch/uncc/nichiflu/managers_revelio/name_batches'
    
    # Run verification
    success = verify_extraction(batch_dir)
    
    if not success:
        print("\n⚠️  EXTRACTION INCOMPLETE - Some batches are missing")
        sys.exit(1)
    else:
        print("\n✅ EXTRACTION COMPLETE - All batches present")
        sys.exit(0)