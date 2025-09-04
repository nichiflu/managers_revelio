# CEO LinkedIn Data Extraction from WRDS Revelio

## Project Overview
Extract detailed LinkedIn profile data for 1243 CEOs from the WRDS Revelio database.

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
- **Total CEOs**: 1243
- **Batch Size**: 5 parallel batches
- **CEOs per Batch**: ~248
- **Resource Allocation**: 2 cores, 16G RAM, 02:00:00 runtime

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

Generated: 2025-09-04 15:32:46
