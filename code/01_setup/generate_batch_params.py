#!/usr/bin/env python3
"""
Generate Batch Parameters for CEO LinkedIn Data Extraction
Splits 1,243 CEOs into 5 balanced batches for parallel processing

Output: batch_params.txt with batch assignments
"""

import pandas as pd
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

def load_ceo_data():
    """Load CEO data from CSV file"""
    
    # Use the CEO_DATA_FILE from config when on WRDS, otherwise local path
    if os.path.exists('/home/uncc/nichiflu'):
        # On WRDS server
        data_file = CEO_DATA_FILE
    else:
        # Local development
        data_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                  "data", "ceo_names_companies.csv")
    
    if not os.path.exists(data_file):
        print(f"❌ CEO data file not found: {data_file}")
        return None
    
    print(f"📂 Loading CEO data from: {data_file}")
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(data_file, encoding=encoding)
            print(f"   Using encoding: {encoding}")
            break
        except UnicodeDecodeError:
            continue
    
    if df is None:
        print("❌ Could not decode CSV file with any standard encoding")
        return None
    
    print(f"✓ Loaded {len(df)} CEO records")
    print(f"   Columns: {', '.join(df.columns.tolist())}")
    
    # Display sample records
    print("\n📋 Sample CEO records:")
    for i, row in df.head(3).iterrows():
        print(f"   {i+1}. {row['firstname']} {row['surname']} - {row['company_name']}")
    
    return df

def validate_ceo_data(df):
    """Validate CEO data for required fields"""
    
    print("\n🔍 Validating CEO data...")
    
    # Check for required columns
    required_columns = ['surname', 'firstname', 'company_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Missing required columns: {', '.join(missing_columns)}")
        return False
    
    # Check for missing values
    for col in required_columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            print(f"⚠️  Column '{col}' has {missing_count} missing values")
    
    # Check for duplicates
    duplicate_count = df.duplicated(subset=['surname', 'firstname']).sum()
    if duplicate_count > 0:
        print(f"⚠️  Found {duplicate_count} duplicate CEO names")
    
    print("✓ Data validation complete")
    return True

def create_batches(df):
    """Split CEOs into balanced batches for parallel processing"""
    
    print(f"\n🔀 Creating {NUM_BATCHES} balanced batches...")
    
    # Shuffle data for random distribution
    df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Calculate batch sizes
    base_size = len(df) // NUM_BATCHES
    remainder = len(df) % NUM_BATCHES
    
    batches = []
    start_idx = 0
    
    for batch_num in range(1, NUM_BATCHES + 1):
        # Add one extra CEO to first 'remainder' batches to balance
        batch_size = base_size + (1 if batch_num <= remainder else 0)
        end_idx = start_idx + batch_size
        
        batch_df = df_shuffled.iloc[start_idx:end_idx].copy()
        batch_df['batch_id'] = batch_num
        batches.append(batch_df)
        
        print(f"   Batch {batch_num}: {len(batch_df)} CEOs")
        
        start_idx = end_idx
    
    return batches

def save_batch_files(batches):
    """Save individual batch files for processing"""
    
    # Use BATCH_DIR from config when on WRDS
    if os.path.exists('/home/uncc/nichiflu'):
        # On WRDS server
        batch_dir = BATCH_DIR
    else:
        # Local development - don't save locally
        print("⚠️  Skipping batch file creation in local mode")
        print("   Run this script on WRDS to generate batch files")
        return None
    
    os.makedirs(batch_dir, exist_ok=True)
    
    print(f"\n💾 Saving batch files to: {batch_dir}")
    
    # Save individual batch files
    for i, batch_df in enumerate(batches, 1):
        filename = os.path.join(batch_dir, f"batch_{i:02d}_ceos.csv")
        batch_df.to_csv(filename, index=False)
        print(f"   Saved {filename} ({len(batch_df)} records)")
    
    # Save master batch parameters file
    params_file = os.path.join(batch_dir, "batch_params.txt")
    with open(params_file, 'w') as f:
        f.write("# Batch parameters for CEO LinkedIn extraction\n")
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total CEOs: {sum(len(b) for b in batches)}\n")
        f.write(f"# Number of batches: {NUM_BATCHES}\n\n")
        f.write("# Format: batch_id,ceo_count,filename\n")
        
        for i, batch_df in enumerate(batches, 1):
            f.write(f"{i},{len(batch_df)},batch_{i:02d}_ceos.csv\n")
    
    print(f"✓ Saved batch parameters to: {params_file}")
    
    return params_file

def generate_grid_engine_scripts(batches):
    """Generate Grid Engine submission scripts for each batch"""
    
    scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                               "02_extraction")
    os.makedirs(scripts_dir, exist_ok=True)
    
    print(f"\n📝 Generating Grid Engine scripts...")
    
    # Master submission script
    master_script = os.path.join(scripts_dir, "submit_all_batches.sh")
    with open(master_script, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("# Submit all CEO extraction batch jobs to Grid Engine\n\n")
        f.write("echo '🚀 Submitting CEO extraction batch jobs...'\n\n")
        
        for i in range(1, NUM_BATCHES + 1):
            f.write(f"# Submit batch {i}\n")
            f.write(f"qsub -N ceo_batch_{i:02d} batch_{i:02d}.sh\n")
            f.write(f"echo '  Submitted batch {i}'\n")
            f.write("sleep 2  # Small delay between submissions\n\n")
        
        f.write("echo '✅ All batch jobs submitted'\n")
        f.write("echo '📊 Check job status with: qstat -u $USER'\n")
    
    os.chmod(master_script, 0o755)
    print(f"   Created master submission script: {master_script}")
    
    # Individual batch scripts
    for i in range(1, NUM_BATCHES + 1):
        batch_script = os.path.join(scripts_dir, f"batch_{i:02d}.sh")
        with open(batch_script, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write(f"#$ -N ceo_batch_{i:02d}\n")
            f.write(f"#$ -pe openmp {BATCH_RESOURCES['cores']}\n")
            f.write(f"#$ -l h_vmem={BATCH_RESOURCES['memory']}\n")
            f.write(f"#$ -l h_rt={BATCH_RESOURCES['time']}\n")
            f.write("#$ -j y\n")
            f.write("#$ -o /scratch/uncc/nichiflu_managers_revelio/logs/\n")
            f.write("#$ -m bea\n")
            f.write("#$ -M nichiflu@wharton.upenn.edu\n\n")
            
            f.write(f"# CEO LinkedIn Extraction - Batch {i}\n\n")
            f.write("cd /home/uncc/nichiflu/managers_revelio\n")
            f.write(f"python3 code/02_extraction/extract_ceo_batch.py --batch {i}\n")
        
        os.chmod(batch_script, 0o755)
    
    print(f"   Created {NUM_BATCHES} individual batch scripts")
    
    return master_script

def main():
    """Main execution function"""
    
    print("👔 CEO LinkedIn Data Extraction - Batch Parameter Generator")
    print("=" * 60)
    
    # Load CEO data
    df = load_ceo_data()
    if df is None:
        return 1
    
    # Validate data
    if not validate_ceo_data(df):
        return 1
    
    # Create batches
    batches = create_batches(df)
    
    # Save batch files
    params_file = save_batch_files(batches)
    
    # Generate Grid Engine scripts
    master_script = generate_grid_engine_scripts(batches)
    
    # Summary
    print("\n" + "=" * 60)
    print("🎉 Batch generation complete!")
    print(f"   Total CEOs: {len(df)}")
    print(f"   Number of batches: {NUM_BATCHES}")
    print(f"   CEOs per batch: ~{len(df) // NUM_BATCHES}")
    print("\n📁 Generated files:")
    print(f"   Batch parameters: batch_params/")
    print(f"   Grid Engine scripts: code/02_extraction/")
    print("\n💡 Next steps:")
    print("   1. Sync files to WRDS: git push")
    print("   2. On WRDS server: cd ~/managers_revelio")
    print("   3. Submit jobs: bash code/02_extraction/submit_all_batches.sh")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())