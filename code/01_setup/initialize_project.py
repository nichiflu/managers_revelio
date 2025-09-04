#!/usr/bin/env python3
"""
Initialize CEO LinkedIn Extraction Project
Sets up all necessary directories, configuration, and batch parameters
"""

import os
import sys
import subprocess
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

def check_environment():
    """Check if running on WRDS or local development"""
    
    print("🔍 Checking environment...")
    
    hostname = os.environ.get('HOSTNAME', 'unknown')
    if 'wrds-cloud' in hostname:
        print(f"✓ Running on WRDS server: {hostname}")
        return 'wrds'
    else:
        print(f"✓ Running in local development mode: {hostname}")
        return 'local'

def setup_local_directories():
    """Create local project directories"""
    
    print("\n📁 Creating local project directories...")
    
    local_dirs = [
        'code/01_setup',
        'code/02_extraction', 
        'code/03_processing',
        'code/04_results',
        'data',
        'batch_params',
        'logs',
        'results'
    ]
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    for dir_path in local_dirs:
        full_path = os.path.join(base_dir, dir_path)
        os.makedirs(full_path, exist_ok=True)
        print(f"   ✓ {dir_path}")
    
    return True

def setup_wrds_directories():
    """Create WRDS scratch directories"""
    
    print("\n📁 Creating WRDS scratch directories...")
    
    try:
        create_directories()  # From config.py
        return True
    except Exception as e:
        print(f"❌ Error creating directories: {e}")
        return False

def check_data_file():
    """Verify CEO data file exists"""
    
    print("\n📊 Checking CEO data file...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_file = os.path.join(base_dir, "data", "ceo_names_companies.csv")
    
    if os.path.exists(data_file):
        # Get file info
        import pandas as pd
        try:
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(data_file, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Could not decode CSV with any standard encoding")
            
            print(f"✓ Found CEO data file: {data_file}")
            print(f"   Records: {len(df)}")
            print(f"   Columns: {', '.join(df.columns.tolist())}")
            return True
        except Exception as e:
            print(f"❌ Error reading data file: {e}")
            return False
    else:
        print(f"❌ CEO data file not found: {data_file}")
        print("   Please ensure data/ceo_names_companies.csv exists")
        return False

def setup_git_ignore():
    """Create .gitignore file for sensitive data"""
    
    print("\n🔒 Setting up .gitignore...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    gitignore_file = os.path.join(base_dir, ".gitignore")
    
    gitignore_content = """# Sensitive data
data/*.csv
!data/ceo_names_companies.csv

# Results and logs
results/
logs/
*.log

# Batch outputs
batch_params/*.csv
extracted/
combined/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python

# Environment
.env
venv/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db
"""
    
    with open(gitignore_file, 'w') as f:
        f.write(gitignore_content)
    
    print(f"✓ Created .gitignore file")
    return True

def create_readme():
    """Create README file with project documentation"""
    
    print("\n📚 Creating README...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    readme_file = os.path.join(base_dir, "README_CEO_EXTRACTION.md")
    
    readme_content = f"""# CEO LinkedIn Data Extraction from WRDS Revelio

## Project Overview
Extract detailed LinkedIn profile data for {TOTAL_CEOS} CEOs from the WRDS Revelio database.

## Directory Structure
```
managers_revelio/
├── code/
│   ├── 01_setup/       # Configuration and setup scripts
│   ├── 02_extraction/  # Data extraction scripts
│   ├── 03_processing/  # Data processing and consolidation
│   └── 04_results/     # Final output generation
├── data/
│   └── ceo_names_companies.csv  # Input CEO data
├── batch_params/       # Batch parameter files
├── logs/              # Execution logs
└── results/           # Output data files
```

## Setup Instructions

### Local Development
1. Initialize project:
   ```bash
   python3 code/01_setup/initialize_project.py
   ```

2. Generate batch parameters:
   ```bash
   python3 code/01_setup/generate_batch_params.py
   ```

3. Push to GitHub:
   ```bash
   git add .
   git commit -m "Initialize CEO extraction project"
   git push
   ```

### WRDS Execution
1. SSH to WRDS:
   ```bash
   ssh [username]@wrds-cloud.wharton.upenn.edu
   ```

2. Clone/pull repository:
   ```bash
   cd ~
   git clone [repository-url] managers_revelio
   # or if already cloned:
   cd ~/managers_revelio
   git pull
   ```

3. Initialize WRDS environment:
   ```bash
   python3 code/01_setup/initialize_project.py
   ```

4. Submit batch jobs:
   ```bash
   bash code/02_extraction/submit_all_batches.sh
   ```

5. Monitor jobs:
   ```bash
   qstat -u $USER
   ```

## Configuration
- **Total CEOs**: {TOTAL_CEOS}
- **Batch Size**: {NUM_BATCHES} parallel batches
- **CEOs per Batch**: ~{CEOS_PER_BATCH}
- **Resource Allocation**: {BATCH_RESOURCES['cores']} cores, {BATCH_RESOURCES['memory']} RAM, {BATCH_RESOURCES['time']} runtime

## Data Flow
1. **Input**: CEO names and companies from CSV
2. **Matching**: Link CEOs to LinkedIn profiles in Revelio
3. **Extraction**: Pull comprehensive professional data
4. **Consolidation**: Combine batch results
5. **Output**: Enriched dataset with LinkedIn information

## Key Scripts
- `config.py`: Central configuration
- `generate_batch_params.py`: Create batch assignments
- `extract_ceo_batch.py`: Extract data for one batch
- `combine_results.py`: Merge all batch outputs
- `generate_report.py`: Create extraction summary

## Monitoring
Check extraction progress:
```bash
tail -f logs/ceo_batch_*.log
```

## Output Files
- `results/ceo_linkedin_enriched_[timestamp].csv`: Final dataset
- `results/unmatched_ceos.csv`: CEOs not found in Revelio
- `results/extraction_report.txt`: Summary statistics

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    with open(readme_file, 'w') as f:
        f.write(readme_content)
    
    print(f"✓ Created README_CEO_EXTRACTION.md")
    return True

def main():
    """Main initialization function"""
    
    print("🚀 CEO LinkedIn Extraction Project Initializer")
    print("=" * 60)
    
    # Check environment
    env_type = check_environment()
    
    # Setup directories based on environment
    if env_type == 'local':
        if not setup_local_directories():
            return 1
    else:
        if not setup_wrds_directories():
            return 1
    
    # Check data file
    if not check_data_file():
        print("\n⚠️  Warning: CEO data file not found")
        print("   Project structure created, but you need to add the data file")
    
    # Setup git ignore
    setup_git_ignore()
    
    # Create documentation
    create_readme()
    
    # Generate batch parameters if data exists
    if check_data_file():
        print("\n📋 Generating batch parameters...")
        try:
            import generate_batch_params
            generate_batch_params.main()
        except Exception as e:
            print(f"⚠️  Could not generate batch parameters: {e}")
            print("   Run manually: python3 code/01_setup/generate_batch_params.py")
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ Project initialization complete!")
    
    if env_type == 'local':
        print("\n📝 Next steps for local development:")
        print("   1. Review generated configuration in code/01_setup/config.py")
        print("   2. Add CEO data to data/ceo_names_companies.csv if not present")
        print("   3. Generate batch parameters: python3 code/01_setup/generate_batch_params.py")
        print("   4. Commit and push to GitHub")
        print("   5. Pull on WRDS server and run extraction")
    else:
        print("\n📝 Next steps on WRDS:")
        print("   1. Review batch parameters in batch_params/")
        print("   2. Submit extraction jobs: bash code/02_extraction/submit_all_batches.sh")
        print("   3. Monitor progress: qstat -u $USER")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())