#!/usr/bin/env python3
"""
Configuration settings for CEO LinkedIn Data Extraction from WRDS Revelio
Shared configuration across all extraction scripts
"""

import os
from datetime import datetime

# ============================================================================
# WRDS CONNECTION SETTINGS
# ============================================================================

# WRDS connection parameters
WRDS_USERNAME = 'nichiflu'  # Your WRDS username
WRDS_CONNECTION_TIMEOUT = 30  # seconds

# ============================================================================
# DIRECTORY PATHS
# ============================================================================

# WRDS Server paths
HOME_DIR = "/home/uncc/nichiflu"
PROJECT_DIR = f"{HOME_DIR}/managers_revelio"
SCRATCH_BASE = "/scratch/uncc"
SCRATCH_DIR = f"{SCRATCH_BASE}/nichiflu_managers_revelio"

# Data directories - all data stays in the data folder
DATA_DIR = f"{PROJECT_DIR}/data"
BATCH_DIR = f"{DATA_DIR}/batches"
EXTRACTED_DIR = f"{SCRATCH_DIR}/extracted"  # Large files go to scratch
COMBINED_DIR = f"{SCRATCH_DIR}/combined"    # Large files go to scratch
LOGS_DIR = f"{SCRATCH_DIR}/logs"
UNMATCHED_DIR = f"{DATA_DIR}/unmatched"

# Input data file
CEO_DATA_FILE = f"{DATA_DIR}/ceo_names_companies.csv"

# ============================================================================
# EXTRACTION PARAMETERS
# ============================================================================

# CEO dataset parameters
TOTAL_CEOS = 1243
NUM_BATCHES = 5  # Maximum concurrent processes allowed by WRDS
CEOS_PER_BATCH = TOTAL_CEOS // NUM_BATCHES  # ~249 CEOs per batch

# ============================================================================
# PERFORMANCE PARAMETERS
# ============================================================================

# Grid Engine resource allocation for each batch job
BATCH_RESOURCES = {
    'cores': 2,
    'memory': '16G',  # Increased for large Revelio queries
    'time': '02:00:00'  # 2 hours per batch
}

# Query chunk size (to avoid memory issues)
QUERY_CHUNK_SIZE = 50  # Process 50 CEOs at a time within each batch

# ============================================================================
# REVELIO TABLE REFERENCES
# ============================================================================

# Main Revelio tables to query
REVELIO_TABLES = {
    'individual_user': 'revelio.individual_user',
    'individual_positions': 'revelio.individual_positions',
    'individual_user_education': 'revelio.individual_user_education',
    'individual_user_skills': 'revelio.individual_user_skills',
    'company_mapping': 'revelio.company_mapping',
    'company': 'revelio.company'
}

# ============================================================================
# SQL QUERY TEMPLATES
# ============================================================================

# CEO profile matching query template
CEO_MATCH_QUERY_TEMPLATE = """
SELECT DISTINCT
    iu.user_id,
    iu.fullname,
    iu.first_name,
    iu.last_name,
    iu.predicted_gender,
    iu.predicted_ethnicity,
    iu.location,
    iu.n_connection,
    iu.prestige_score
FROM {individual_user} iu
WHERE (
    -- Exact match on full name
    LOWER(iu.fullname) = LOWER('{fullname}')
    OR 
    -- Match on first and last name separately
    (LOWER(iu.first_name) = LOWER('{firstname}') 
     AND LOWER(iu.last_name) = LOWER('{surname}'))
)
"""

# Position extraction query template
POSITION_QUERY_TEMPLATE = """
SELECT 
    ip.user_id,
    ip.position_id,
    ip.rcid,
    ip.company_name,
    ip.title,
    ip.role,
    ip.start_date,
    ip.end_date,
    ip.is_current,
    ip.location
FROM {individual_positions} ip
WHERE ip.user_id = '{user_id}'
  AND (
    LOWER(ip.title) LIKE '%ceo%'
    OR LOWER(ip.title) LIKE '%chief executive%'
    OR LOWER(ip.title) LIKE '%president%'
    OR LOWER(ip.role) LIKE '%executive%'
  )
ORDER BY ip.start_date DESC
"""

# Education extraction query template
EDUCATION_QUERY_TEMPLATE = """
SELECT 
    iue.user_id,
    iue.school,
    iue.degree,
    iue.field_of_study,
    iue.start_year,
    iue.end_year
FROM {individual_user_education} iue
WHERE iue.user_id = '{user_id}'
ORDER BY iue.end_year DESC
"""

# Skills extraction query template
SKILLS_QUERY_TEMPLATE = """
SELECT 
    ius.user_id,
    ius.skill,
    ius.skill_category
FROM {individual_user_skills} ius
WHERE ius.user_id = '{user_id}'
"""

# Company validation query template
COMPANY_VALIDATION_QUERY = """
SELECT 
    cm.rcid,
    cm.company_name,
    cm.company_name_clean,
    c.ultimate_parent_rcid,
    c.ultimate_parent_name
FROM {company_mapping} cm
LEFT JOIN {company} c ON cm.rcid = c.rcid
WHERE LOWER(cm.company_name_clean) LIKE LOWER('%{company_name}%')
LIMIT 10
"""

# ============================================================================
# FILE NAMING CONVENTIONS
# ============================================================================

def get_batch_filename(batch_number):
    """Generate filename for batch CEO list"""
    return f"batch_{batch_number:02d}_ceos.csv"

def get_extracted_filename(batch_number):
    """Generate filename for extracted data"""
    return f"batch_{batch_number:02d}_extracted.csv"

def get_unmatched_filename(batch_number):
    """Generate filename for unmatched CEOs"""
    return f"batch_{batch_number:02d}_unmatched.csv"

def get_combined_filename():
    """Generate filename for final combined dataset"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"ceo_linkedin_enriched_{timestamp}.csv"

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging(script_name, batch_number=None):
    """Setup logging for extraction scripts"""
    import logging
    
    if batch_number:
        log_filename = f"{LOGS_DIR}/{script_name}_batch_{batch_number:02d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    else:
        log_filename = f"{LOGS_DIR}/{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create logs directory if it doesn't exist
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(script_name)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_directories():
    """Create all necessary directories"""
    directories = [
        SCRATCH_DIR,
        DATA_DIR,
        BATCH_DIR,
        EXTRACTED_DIR,
        COMBINED_DIR,
        LOGS_DIR,
        UNMATCHED_DIR
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    print(f"✓ Created {len(directories)} directories")

def get_storage_usage():
    """Get current storage usage information"""
    import subprocess
    
    try:
        result = subprocess.run(['quota'], capture_output=True, text=True)
        return result.stdout
    except Exception as e:
        return f"Could not get storage info: {e}"

def validate_environment():
    """Validate that we're running in the correct environment"""
    import socket
    
    hostname = socket.gethostname()
    
    # Check if we're on WRDS or local development
    if 'wrds-cloud' not in hostname:
        print(f"⚠️  Local development mode. Hostname: {hostname}")
        local_mode = True
    else:
        print(f"✓ Running on WRDS server. Hostname: {hostname}")
        local_mode = False
    
    # Check WRDS module availability
    try:
        import wrds
        print("✓ WRDS module available")
        return True
    except ImportError:
        if local_mode:
            print("⚠️  WRDS module not available (expected in local mode)")
            return True
        else:
            print("❌ WRDS module not available on WRDS server")
            return False

def clean_name(name):
    """Clean and standardize CEO names for matching"""
    if not name:
        return ""
    
    # Remove extra spaces and strip
    name = ' '.join(name.split())
    
    # Remove common suffixes
    suffixes = ['Jr.', 'Jr', 'Sr.', 'Sr', 'III', 'II', 'IV']
    for suffix in suffixes:
        name = name.replace(f' {suffix}', '')
    
    return name.strip()

def generate_name_variations(firstname, surname):
    """Generate common name variations for matching"""
    variations = []
    
    # Clean names first
    firstname = clean_name(firstname)
    surname = clean_name(surname)
    
    # Standard variations
    variations.append(f"{firstname} {surname}")  # John Smith
    variations.append(f"{surname}, {firstname}")  # Smith, John
    
    # Handle middle initials/names
    if ' ' in firstname:
        first_parts = firstname.split()
        variations.append(f"{first_parts[0]} {surname}")  # John Smith (without middle)
        variations.append(f"{surname}, {first_parts[0]}")  # Smith, John (without middle)
    
    return variations

if __name__ == "__main__":
    print("CEO LinkedIn Data Extraction Configuration")
    print("=" * 50)
    print(f"Username: {WRDS_USERNAME}")
    print(f"Home directory: {HOME_DIR}")
    print(f"Project directory: {PROJECT_DIR}")
    print(f"Scratch directory: {SCRATCH_DIR}")
    print(f"Total CEOs: {TOTAL_CEOS}")
    print(f"Number of batches: {NUM_BATCHES}")
    print(f"CEOs per batch: {CEOS_PER_BATCH}")
    print("")
    
    print("Environment validation:")
    if validate_environment():
        print("✅ Environment ready")
    else:
        print("❌ Environment issues detected")
    
    print("")
    print("Creating directories:")
    create_directories()
    
    if 'wrds-cloud' in os.environ.get('HOSTNAME', ''):
        print("")
        print("Storage usage:")
        print(get_storage_usage())