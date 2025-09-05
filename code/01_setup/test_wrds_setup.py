#!/usr/bin/env python3
"""
Test WRDS Setup for CEO LinkedIn Extraction
Verifies directories, permissions, and WRDS connection
"""

import os
import sys
import socket

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

def test_environment():
    """Test if we're on WRDS server"""
    hostname = socket.gethostname()
    print(f"🖥️  Hostname: {hostname}")
    
    if 'wrds-cloud' in hostname:
        print("✅ Running on WRDS server")
        return True
    else:
        print("⚠️  Not on WRDS server (local development mode)")
        return False

def test_directories():
    """Test if directories exist and are writable"""
    print("\n📁 Testing directories:")
    
    # Test home directory
    if os.path.exists(HOME_DIR):
        print(f"✅ Home directory exists: {HOME_DIR}")
    else:
        print(f"❌ Home directory not found: {HOME_DIR}")
    
    # Test project directory
    if os.path.exists(PROJECT_DIR):
        print(f"✅ Project directory exists: {PROJECT_DIR}")
    else:
        print(f"❌ Project directory not found: {PROJECT_DIR}")
    
    # Create scratch subdirectories
    directories_to_create = [
        SCRATCH_DIR,
        DATA_DIR,
        BATCH_DIR,
        EXTRACTED_DIR,
        COMBINED_DIR,
        LOGS_DIR,
        UNMATCHED_DIR
    ]
    
    print(f"\n📂 Creating scratch directories:")
    for directory in directories_to_create:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✅ Created/verified: {directory}")
        except Exception as e:
            print(f"❌ Failed to create {directory}: {e}")

def test_wrds_connection():
    """Test WRDS database connection"""
    print("\n🔌 Testing WRDS connection:")
    
    try:
        import wrds
        print("✅ WRDS module imported successfully")
        
        # Try to establish connection
        try:
            db = wrds.Connection(wrds_username=WRDS_USERNAME)
            print(f"✅ Connected to WRDS as user: {WRDS_USERNAME}")
            
            # Test query to Revelio database
            test_query = "SELECT COUNT(*) as count FROM revelio.individual_user LIMIT 1"
            result = db.raw_sql(test_query)
            
            if result is not None:
                print(f"✅ Successfully queried Revelio database")
                print(f"   Individual_user table accessible")
            
            db.close()
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            print("   You may need to run: wrds-pgpass-create")
            
    except ImportError:
        print("❌ WRDS module not available")
        print("   This is expected in local development")

def test_data_files():
    """Test if input data files are present"""
    print("\n📊 Testing data files:")
    
    # Check for batch parameter files
    batch_params_dir = os.path.join(PROJECT_DIR, "batch_params")
    
    if os.path.exists(batch_params_dir):
        batch_files = [f for f in os.listdir(batch_params_dir) if f.endswith('.csv')]
        print(f"✅ Found {len(batch_files)} batch files in {batch_params_dir}")
        
        for batch_file in sorted(batch_files)[:3]:  # Show first 3
            print(f"   - {batch_file}")
    else:
        print(f"⚠️  Batch parameters directory not found: {batch_params_dir}")
        print("   Run generate_batch_params.py locally first")

def check_disk_space():
    """Check available disk space"""
    print("\n💾 Checking disk space:")
    
    try:
        import shutil
        
        # Check scratch space
        if os.path.exists(SCRATCH_BASE):
            stat = shutil.disk_usage(SCRATCH_BASE)
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            used_pct = (stat.used / stat.total) * 100
            
            print(f"   Scratch directory: {SCRATCH_BASE}")
            print(f"   Free space: {free_gb:.1f} GB / {total_gb:.1f} GB")
            print(f"   Used: {used_pct:.1f}%")
            
            if free_gb < 10:
                print("⚠️  Low disk space! May need to clean up old files")
            else:
                print("✅ Sufficient disk space available")
    except Exception as e:
        print(f"   Could not check disk space: {e}")

def main():
    """Run all tests"""
    print("🧪 WRDS Setup Test for CEO LinkedIn Extraction")
    print("=" * 60)
    
    # Run tests
    is_wrds = test_environment()
    test_directories()
    
    if is_wrds:
        test_wrds_connection()
        check_disk_space()
    
    test_data_files()
    
    print("\n" + "=" * 60)
    print("📝 Setup test complete!")
    
    if is_wrds:
        print("\nNext steps on WRDS:")
        print("1. Ensure batch_params files are synced from local")
        print("2. Review code/02_extraction scripts")
        print("3. Submit test job: qsub -N test_job code/02_extraction/batch_01.sh")
    else:
        print("\nNext steps locally:")
        print("1. Push code to GitHub")
        print("2. Pull on WRDS server")
        print("3. Run this test script on WRDS")

if __name__ == "__main__":
    main()