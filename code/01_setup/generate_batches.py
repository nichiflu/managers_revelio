#!/usr/bin/env python3
"""
generate_batches.py

Split CEO data from ceo_names_companies.csv into 5 balanced batches for parallel processing.
Creates batch_01_ceos.csv through batch_05_ceos.csv in data/batches/ directory.

This script must be run BEFORE submitting parallel extraction jobs.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from code.01_setup.config import DATA_DIR, BATCH_DIR

# Constants
NUM_BATCHES = 5  # WRDS allows maximum 5 concurrent jobs per user
INPUT_FILE = DATA_DIR / 'ceo_names_companies.csv'

def create_batch_directory():
    """Create batch directory if it doesn't exist."""
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Batch directory created/verified: {BATCH_DIR}")

def load_ceo_data():
    """Load CEO data from CSV file."""
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} CEOs from {INPUT_FILE}")
    
    # Display first few records for verification
    print("\nSample of CEO data:")
    print(df.head())
    print(f"\nColumns: {df.columns.tolist()}")
    
    return df

def split_into_batches(df, num_batches=NUM_BATCHES):
    """
    Split DataFrame into approximately equal batches.
    
    Args:
        df: DataFrame with CEO data
        num_batches: Number of batches to create (default: 5)
    
    Returns:
        List of DataFrames, one per batch
    """
    # Calculate batch sizes
    total_ceos = len(df)
    base_size = total_ceos // num_batches
    remainder = total_ceos % num_batches
    
    # Create batch sizes (distribute remainder across first batches)
    batch_sizes = [base_size + (1 if i < remainder else 0) for i in range(num_batches)]
    
    print(f"\nSplitting {total_ceos} CEOs into {num_batches} batches:")
    for i, size in enumerate(batch_sizes, 1):
        print(f"  Batch {i}: {size} CEOs")
    
    # Split dataframe
    batches = []
    start_idx = 0
    
    for size in batch_sizes:
        end_idx = start_idx + size
        batch_df = df.iloc[start_idx:end_idx].copy()
        batch_df.reset_index(drop=True, inplace=True)
        batches.append(batch_df)
        start_idx = end_idx
    
    return batches

def save_batches(batches):
    """
    Save each batch to a separate CSV file.
    
    Args:
        batches: List of DataFrames to save
    
    Returns:
        List of saved file paths
    """
    saved_files = []
    
    for i, batch_df in enumerate(batches, 1):
        filename = f"batch_{i:02d}_ceos.csv"
        filepath = BATCH_DIR / filename
        
        batch_df.to_csv(filepath, index=False)
        saved_files.append(filepath)
        
        print(f"Saved batch {i}: {filepath} ({len(batch_df)} CEOs)")
    
    return saved_files

def create_batch_summary(batches, saved_files):
    """
    Create a summary file with batch information.
    
    Args:
        batches: List of batch DataFrames
        saved_files: List of saved file paths
    """
    summary_file = BATCH_DIR / 'batch_summary.txt'
    
    with open(summary_file, 'w') as f:
        f.write("CEO Batch Split Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total CEOs: {sum(len(b) for b in batches)}\n")
        f.write(f"Number of batches: {len(batches)}\n")
        f.write(f"Batch directory: {BATCH_DIR}\n\n")
        
        f.write("Batch Details:\n")
        f.write("-" * 30 + "\n")
        
        for i, (batch_df, filepath) in enumerate(zip(batches, saved_files), 1):
            f.write(f"\nBatch {i:02d}:\n")
            f.write(f"  File: {filepath.name}\n")
            f.write(f"  CEOs: {len(batch_df)}\n")
            f.write(f"  First CEO: {batch_df.iloc[0]['surname']}, {batch_df.iloc[0]['first_name']}\n")
            f.write(f"  Last CEO: {batch_df.iloc[-1]['surname']}, {batch_df.iloc[-1]['first_name']}\n")
    
    print(f"\nBatch summary saved to: {summary_file}")

def verify_batches(original_df, saved_files):
    """
    Verify that all CEOs are present across batches with no duplicates.
    
    Args:
        original_df: Original DataFrame with all CEOs
        saved_files: List of saved batch file paths
    """
    print("\nVerifying batch integrity...")
    
    # Reload all batches
    all_batch_ceos = []
    for filepath in saved_files:
        batch_df = pd.read_csv(filepath)
        all_batch_ceos.append(batch_df)
    
    combined_df = pd.concat(all_batch_ceos, ignore_index=True)
    
    # Check counts
    original_count = len(original_df)
    combined_count = len(combined_df)
    
    if original_count == combined_count:
        print(f"✓ Count verification passed: {original_count} CEOs")
    else:
        print(f"✗ Count mismatch! Original: {original_count}, Combined: {combined_count}")
        return False
    
    # Check for duplicates
    duplicates = combined_df.duplicated().sum()
    if duplicates == 0:
        print("✓ No duplicates found across batches")
    else:
        print(f"✗ Found {duplicates} duplicate records!")
        return False
    
    print("✓ Batch verification complete - all checks passed")
    return True

def main():
    """Main execution function."""
    print("CEO Batch Generation Script")
    print("=" * 50)
    
    try:
        # Create batch directory
        create_batch_directory()
        
        # Load CEO data
        df = load_ceo_data()
        
        # Split into batches
        batches = split_into_batches(df, NUM_BATCHES)
        
        # Save batches
        saved_files = save_batches(batches)
        
        # Create summary
        create_batch_summary(batches, saved_files)
        
        # Verify integrity
        verify_batches(df, saved_files)
        
        print("\n" + "=" * 50)
        print("Batch generation completed successfully!")
        print(f"Next step: Run parallel extraction jobs using the {NUM_BATCHES} batch files")
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
