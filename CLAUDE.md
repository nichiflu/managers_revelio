# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a python based research project focused on analyzing CEO compensation data using Revelio workforce intelligence data. The project tries to extend the BPVR paper (JEEA) in `/Users/nichi/Dropbox/teaching/ECON6901/students/fall2025/DanielStone/managers_revelio/dev-docs/bpvr_paper_JEEA` (which usess Stata) by using detailed individual level LinkedIn data from Reelio (WRDS). The main purpose of this repo is to write code that extracts individual level data from WRDS for the CEOs listed in `data/ceo_names_companies.csv`

## Key Data Files

- `data/ceo_names_companies.csv` - Contains CEO surnames, first names, company names, and log pay (lnpay)


## Development Commands

### Running Stata Code
```bash
stata -b do filename.do
```

### Key Stata Files
- `extract_ceo_info.do` - Extracts CEO names and company information from pay data

## Project Structure

- `/code/` - Main code directory (currently empty, awaiting development)
- `/data/` - Data files including extracted CEO information
- `/dev-docs/` - Development documentation including:
  - `bpvr_paper_JEEA/` - Original paper replication code and data
  - `wrds_revelio/` - WRDS Revelio database documentation
  - `wrds_cloud/` - WRDS cloud access examples
  - `wrds_cloud_revelio_extraction_example_code/` - Sample code for extracting Revelio data

## Working with Revelio Data

The project integrates WRDS Revelio workforce intelligence data. Key documentation is available in:
- `/Users/nichi/Dropbox/teaching/ECON6901/students/fall2025/DanielStone/managers_revelio/dev-docs/wrds_cloud_revelio_extraction_example_code` - complete code example of how to extract data from the Revelio database
- `dev-docs/wrds_revelio/DATABASE_OVERVIEW.md` - Overview of Revelio database structure
- `dev-docs/wrds_revelio/ACCESS_METHODS.md` - Methods for accessing Revelio data
- `dev-docs/wrds_revelio/PERFORMANCE_GUIDE.md` - Performance optimization for large queries

## Notes

- The project uses python for data analysis
- The repository includes replication materials from the BPVR JEEA paper as reference (using Stata)
- The repository also includes example code from a different project that extracts Revelio data from WRDS

## Task Master AI Instructions
**Import Task Master's development workflow commands and guidelines, treat as if import is in the main CLAUDE.md file.**
@./.taskmaster/CLAUDE.md
