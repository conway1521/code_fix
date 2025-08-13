"""
HEALTHCARE WORKFORCE ANALYSIS TEMPLATE
=====================================

This template provides a standardized framework for analyzing healthcare workforce data
across multiple health systems. It emphasizes validation, transparency, and analyst control.

DESIGN PRINCIPLES:
- No silent failures - every step validates its work
- User controls all mappings and parameters
- Consistent variable naming for downstream use
- Heavy validation with hard stops when data quality is insufficient
- Modular sections that can be validated step-by-step

WORKFLOW SECTIONS:
1. Data Import & Schema Setup
2. Initial Data Exploration & Validation  
3. Geographic Filtering
4. Column Standardization & Mapping
5. Job Title Standardization
6. Employment Status Standardization
7. Location/Facility Standardization & Classification
8. Core Metrics Calculation
9. Facility-Level Analysis Setup
10. Output Generation Framework
"""

import pandas as pd
import numpy as np
import re
from math import gcd
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# SECTION 1: DATA IMPORT & SCHEMA SETUP
# ==========================================

print("=" * 80)
print("SECTION 1: DATA IMPORT & SCHEMA SETUP")
print("=" * 80)

# USER CONFIGURATION - MODIFY THESE SETTINGS
# ==========================================

# File paths and data sources
DATA_CONFIG = {
    'base_folder': '/path/to/your/data',  # Base folder containing data files
    'headcount_source': {
        'file': 'System2024.xlsx',
        'sheet': 'Headcount',
        'type': 'excel'  # 'excel' or 'csv'
    },
    'hires_source': {
        'file': 'System2024.xlsx', 
        'sheet': 'Hires',
        'type': 'excel'
    },
    'terminations_source': {
        'file': 'System2024.xlsx',
        'sheet': 'Terms', 
        'type': 'excel'
    },
    'requisitions_source': {
        'file': 'System2024.xlsx',
        'sheet': 'Requisitions',
        'type': 'excel'
    },
    'contractors_source': {
        'file': 'System2024.xlsx',
        'sheet': 'Contractor',
        'type': 'excel'
    }
}

# Column mapping - USER MUST SPECIFY THESE
COLUMN_MAPPING = {
    'job_title_col': 'Job Title',
    'location_col': 'Location', 
    'state_col': 'Work State',
    'full_part_time_col': 'Full/Part Time',
    'employment_category_col': 'Employment Category',
    'hire_date_col': 'Hire Date',  # For hires sheet
    'term_date_col': 'Termination Date',  # For terminations sheet
    'req_status_col': 'Req Current State',  # For requisitions sheet
}

# Analysis parameters
ANALYSIS_CONFIG = {
    'target_state': 'NC',  # State to filter for
    'analysis_date': '2024-12-31',  # Date for analysis
    'target_job_titles': ['Registered Nurse', 'Licensed Practical Nurse', 'Medical Assistant', 'Certified Nursing Assistant'],
    'fte_values': ['FULL_TIME'],  # Values that indicate FTE status
    'prn_values': ['PRN'],  # Values that indicate PRN status
    'open_req_statuses': ['Not Posted', 'Posted', 'Unposted', 'In Progress'],
    'closed_req_statuses': ['Suspended', 'Expired']
}

print("Configuration loaded:")
print(f"Target state: {ANALYSIS_CONFIG['target_state']}")
print(f"Analysis date: {ANALYSIS_CONFIG['analysis_date']}")
print(f"Target job titles: {ANALYSIS_CONFIG['target_job_titles']}")

def load_dataset(source_config, dataset_name):
    """Load a dataset based on configuration"""
    try:
        filepath = f"{DATA_CONFIG['base_folder']}/{source_config['file']}"
        
        if source_config['type'] == 'excel':
            df = pd.read_excel(filepath, sheet_name=source_config['sheet'])
            print(f"✓ Loaded {dataset_name}: {len(df)} rows from {source_config['file']}/{source_config['sheet']}")
        elif source_config['type'] == 'csv':
            df = pd.read_csv(filepath)
            print(f"✓ Loaded {dataset_name}: {len(df)} rows from {source_config['file']}")
        else:
            raise ValueError(f"Unsupported file type: {source_config['type']}")
            
        return df
        
    except Exception as e:
        print(f"✗ CRITICAL ERROR loading {dataset_name}: {str(e)}")
        raise

# Load all datasets
print("\nLoading datasets...")
try:
    headcount_df = load_dataset(DATA_CONFIG['headcount_source'], 'headcount')
    hires_df = load_dataset(DATA_CONFIG['hires_source'], 'hires')
    terminations_df = load_dataset(DATA_CONFIG['terminations_source'], 'terminations')
    requisitions_df = load_dataset(DATA_CONFIG['requisitions_source'], 'requisitions')
    
    # Contractors is optional
    try:
        contractors_df = load_dataset(DATA_CONFIG['contractors_source'], 'contractors')
        has_contractors = True
    except:
        print("! No contractors data found - proceeding without it")
        contractors_df = pd.DataFrame()
        has_contractors = False
        
except Exception as e:
    print(f"\n✗ CRITICAL: Data loading failed. Cannot proceed.")
    print(f"Error: {str(e)}")
    raise

print(f"\n✓ Data loading complete. {4 + int(has_contractors)} datasets loaded.")

# ==========================================
# SECTION 2: INITIAL DATA EXPLORATION & VALIDATION
# ==========================================

print("\n" + "=" * 80)
print("SECTION 2: INITIAL DATA EXPLORATION & VALIDATION")
print("=" * 80)

def validate_dataset_structure(df, dataset_name, required_columns):
    """Validate that dataset has required structure"""
    print(f"\nValidating {dataset_name} structure:")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    
    missing_cols = []
    for col in required_columns:
        if col not in df.columns:
            missing_cols.append(col)
            
    if missing_cols:
        print(f"✗ CRITICAL: Missing required columns in {dataset_name}: {missing_cols}")
        return False
    else:
        print(f"✓ All required columns present in {dataset_name}")
        return True

# Define required columns for each dataset
REQUIRED_COLUMNS = {
    'headcount': [COLUMN_MAPPING['job_title_col'], COLUMN_MAPPING['location_col'], 
                  COLUMN_MAPPING['state_col'], COLUMN_MAPPING['full_part_time_col'], 
                  COLUMN_MAPPING['employment_category_col']],
    'hires': [COLUMN_MAPPING['job_title_col'], COLUMN_MAPPING['location_col'], 
              COLUMN_MAPPING['state_col'], COLUMN_MAPPING['hire_date_col']],
    'terminations': [COLUMN_MAPPING['job_title_col'], COLUMN_MAPPING['location_col'], 
                     COLUMN_MAPPING['state_col'], COLUMN_MAPPING['term_date_col']],
    'requisitions': [COLUMN_MAPPING['job_title_col'], COLUMN_MAPPING['location_col'], 
                     COLUMN_MAPPING['req_status_col']]
}

# Validate all datasets
validation_passed = True
datasets_to_validate = [
    (headcount_df, 'headcount'),
    (hires_df, 'hires'), 
    (terminations_df, 'terminations'),
    (requisitions_df, 'requisitions')
]

if has_contractors:
    datasets_to_validate.append((contractors_df, 'contractors'))

for df, name in datasets_to_validate:
    if name in REQUIRED_COLUMNS:
        if not validate_dataset_structure(df, name, REQUIRED_COLUMNS[name]):
            validation_passed = False

if not validation_passed:
    print("\n✗ CRITICAL: Dataset validation failed. Check column mappings and try again.")
    raise ValueError("Dataset validation failed")

print("\n✓ All datasets passed structure validation")

# Show data previews
print("\nData previews:")
for df, name in datasets_to_validate:
    if len(df) > 0:
        print(f"\n{name.upper()} - First 3 rows:")
        print(df.head(3).to_string())
        
        # Show unique values for key columns
        if COLUMN_MAPPING['state_col'] in df.columns:
            states = df[COLUMN_MAPPING['state_col']].unique()
            print(f"  Unique states: {states}")
            
        if COLUMN_MAPPING['job_title_col'] in df.columns:
            job_titles = df[COLUMN_MAPPING['job_title_col']].value_counts().head(5)
            print(f"  Top 5 job titles: {dict(job_titles)}")

# ==========================================
# SECTION 3: GEOGRAPHIC FILTERING  
# ==========================================

print("\n" + "=" * 80)
print("SECTION 3: GEOGRAPHIC FILTERING")
print("=" * 80)

def filter_by_state(df, dataset_name, state_col, target_state):
    """Filter dataset by state and validate results"""
    original_count = len(df)
    
    if state_col not in df.columns:
        print(f"✗ CRITICAL: State column '{state_col}' not found in {dataset_name}")
        raise ValueError(f"State column missing in {dataset_name}")
    
    # Show unique states before filtering
    unique_states = df[state_col].unique()
    print(f"\n{dataset_name} - States before filtering: {unique_states}")
    
    # Filter for target state
    df_filtered = df[df[state_col] == target_state].copy()
    filtered_count = len(df_filtered)
    
    print(f"{dataset_name} filtering: {original_count} → {filtered_count} rows")
    
    if filtered_count == 0:
        print(f"✗ CRITICAL: No data remains after filtering {dataset_name} for {target_state}")
        raise ValueError(f"No {target_state} data in {dataset_name}")
    
    # Validation - check that filtering worked
    remaining_states = df_filtered[state_col].unique()
    if len(remaining_states) != 1 or remaining_states[0] != target_state:
        print(f"✗ CRITICAL: Filtering failed for {dataset_name}. Remaining states: {remaining_states}")
        raise ValueError(f"State filtering failed for {dataset_name}")
    
    print(f"✓ {dataset_name} successfully filtered to {target_state}")
    return df_filtered

# Filter all datasets
target_state = ANALYSIS_CONFIG['target_state']
state_col = COLUMN_MAPPING['state_col']

print(f"Filtering all datasets for state: {target_state}")

headcount_df = filter_by_state(headcount_df, 'headcount', state_col, target_state)
hires_df = filter_by_state(hires_df, 'hires', state_col, target_state)
terminations_df = filter_by_state(terminations_df, 'terminations', state_col, target_state)
requisitions_df = filter_by_state(requisitions_df, 'requisitions', state_col, target_state)

if has_contractors:
    contractors_df = filter_by_state(contractors_df, 'contractors', state_col, target_state)

print(f"\n✓ Geographic filtering complete. All datasets filtered to {target_state}")

# ==========================================
# SECTION 4: COLUMN STANDARDIZATION & MAPPING
# ==========================================

print("\n" + "=" * 80)
print("SECTION 4: COLUMN STANDARDIZATION & MAPPING")
print("=" * 80)

def standardize_columns(df, dataset_name):
    """Apply column standardization to ensure consistent naming"""
    df = df.copy()
    
    # Ensure all mapped columns exist and create standard names
    column_renames = {}
    
    # Standard columns all datasets should have
    if COLUMN_MAPPING['job_title_col'] in df.columns:
        column_renames[COLUMN_MAPPING['job_title_col']] = 'Job_Title'
    if COLUMN_MAPPING['location_col'] in df.columns:
        column_renames[COLUMN_MAPPING['location_col']] = 'Location'
    if COLUMN_MAPPING['state_col'] in df.columns:
        column_renames[COLUMN_MAPPING['state_col']] = 'State'
        
    # Dataset-specific columns
    if dataset_name in ['headcount', 'hires', 'terminations']:
        if COLUMN_MAPPING['full_part_time_col'] in df.columns:
            column_renames[COLUMN_MAPPING['full_part_time_col']] = 'Full_Part_Time'
        if COLUMN_MAPPING['employment_category_col'] in df.columns:
            column_renames[COLUMN_MAPPING['employment_category_col']] = 'Employment_Category'
    
    if dataset_name == 'hires' and COLUMN_MAPPING['hire_date_col'] in df.columns:
        column_renames[COLUMN_MAPPING['hire_date_col']] = 'Date'
    if dataset_name == 'terminations' and COLUMN_MAPPING['term_date_col'] in df.columns:
        column_renames[COLUMN_MAPPING['term_date_col']] = 'Date'
    if dataset_name == 'requisitions' and COLUMN_MAPPING['req_status_col'] in df.columns:
        column_renames[COLUMN_MAPPING['req_status_col']] = 'Req_Status'
    
    # Apply renames
    df = df.rename(columns=column_renames)
    
    print(f"✓ {dataset_name} columns standardized. Renamed: {column_renames}")
    return df

# Standardize all datasets
print("Standardizing column names across all datasets...")

headcount_df = standardize_columns(headcount_df, 'headcount')
hires_df = standardize_columns(hires_df, 'hires')
terminations_df = standardize_columns(terminations_df, 'terminations')
requisitions_df = standardize_columns(requisitions_df, 'requisitions')

if has_contractors:
    contractors_df = standardize_columns(contractors_df, 'contractors')

print("✓ All datasets have standardized column names")

# Validation - ensure key columns are present
for df, name in [(headcount_df, 'headcount'), (hires_df, 'hires'), 
                 (terminations_df, 'terminations'), (requisitions_df, 'requisitions')]:
    if 'Job_Title' not in df.columns or 'Location' not in df.columns:
        print(f"✗ CRITICAL: Missing standardized columns in {name}")
        raise ValueError(f"Column standardization failed for {name}")

print("✓ Column standardization validation passed")

# ==========================================
# SECTION 5: JOB TITLE STANDARDIZATION
# ==========================================

print("\n" + "=" * 80)
print("SECTION 5: JOB TITLE STANDARDIZATION")
print("=" * 80)

def standardize_job_title(title):
    """
    Standardize job titles to target categories:
    - Registered Nurse
    - Licensed Practical Nurse  
    - Medical Assistant
    - Certified Nursing Assistant
    """
    if pd.isna(title):
        return None
    
    title_lower = str(title).lower().strip()
    
    # Registered Nurse variations
    if 'rn' in title_lower or 'registered nurse' in title_lower:
        # Exclude non-employee entries
        if '(nonee)' in title_lower or 'traveler' in title_lower:
            return None
        return 'Registered Nurse'
    
    # Licensed Practical Nurse variations
    elif 'lpn' in title_lower or 'licensed practical nurse' in title_lower:
        return 'Licensed Practical Nurse'
    
    # Medical Assistant variations
    elif 'medical assistant' in title_lower or 'certified medical assistant' in title_lower:
        return 'Medical Assistant'
    
    # Certified Nursing Assistant variations
    elif ('cna' in title_lower or 'certified nursing assistant' in title_lower or 
          'certified nurse assistant' in title_lower or 'nurse assistant' in title_lower or 
          'nursing assistant' in title_lower):
        return 'Certified Nursing Assistant'
    
    # If none match, return None (will be filtered out)
    else:
        return None

def apply_job_title_standardization(df, dataset_name):
    """Apply job title standardization and validate results"""
    original_count = len(df)
    
    # Apply standardization
    df = df.copy()
    df['Standardized_Job_Title'] = df['Job_Title'].apply(standardize_job_title)
    
    # Filter for target roles only
    df_filtered = df[df['Standardized_Job_Title'].notna()].copy()
    filtered_count = len(df_filtered)
    
    # Calculate capture rate
    capture_rate = (filtered_count / original_count * 100) if original_count > 0 else 0
    
    print(f"\n{dataset_name} job title standardization:")
    print(f"  Original rows: {original_count}")
    print(f"  Rows with target job titles: {filtered_count}")
    print(f"  Capture rate: {capture_rate:.1f}%")
    
    # Show distribution of standardized titles
    title_dist = df_filtered['Standardized_Job_Title'].value_counts()
    print(f"  Distribution: {dict(title_dist)}")
    
    # Validation - ensure we have reasonable capture rate
    if capture_rate < 10:
        print(f"✗ CRITICAL: Very low capture rate ({capture_rate:.1f}%) for {dataset_name}")
        print("This suggests job title standardization is not working properly.")
        # Show some examples of unmapped titles
        unmapped = df[df['Standardized_Job_Title'].isna()]['Job_Title'].value_counts().head(10)
        print(f"Top unmapped titles: {dict(unmapped)}")
        raise ValueError(f"Job title standardization failed for {dataset_name}")
    
    if filtered_count == 0:
        print(f"✗ CRITICAL: No target job titles found in {dataset_name}")
        raise ValueError(f"No target job titles in {dataset_name}")
    
    print(f"✓ {dataset_name} job title standardization successful")
    return df_filtered

# Apply job title standardization to all datasets
print("Applying job title standardization...")
print(f"Target job titles: {ANALYSIS_CONFIG['target_job_titles']}")

headcount_filtered = apply_job_title_standardization(headcount_df, 'headcount')
hires_filtered = apply_job_title_standardization(hires_df, 'hires')
terminations_filtered = apply_job_title_standardization(terminations_df, 'terminations')
requisitions_filtered = apply_job_title_standardization(requisitions_df, 'requisitions')

if has_contractors:
    contractors_filtered = apply_job_title_standardization(contractors_df, 'contractors')

print("\n✓ Job title standardization complete for all datasets")

# Cross-dataset validation - ensure we have consistent job titles
all_job_titles = set()
for df, name in [(headcount_filtered, 'headcount'), (hires_filtered, 'hires'),
                 (terminations_filtered, 'terminations'), (requisitions_filtered, 'requisitions')]:
    titles = set(df['Standardized_Job_Title'].unique())
    all_job_titles.update(titles)
    
expected_titles = set(ANALYSIS_CONFIG['target_job_titles'])
if not all_job_titles.issubset(expected_titles):
    print(f"✗ WARNING: Unexpected job titles found: {all_job_titles - expected_titles}")

print(f"✓ Job title validation complete. Found titles: {sorted(all_job_titles)}")

# ==========================================
# SECTION 6: EMPLOYMENT STATUS STANDARDIZATION
# ==========================================

print("\n" + "=" * 80)
print("SECTION 6: EMPLOYMENT STATUS STANDARDIZATION")  
print("=" * 80)

def create_standardized_employment_status(df, dataset_name):
    """Create standardized FTE/PRN employment status"""
    df = df.copy()
    
    # Check required columns exist
    if 'Full_Part_Time' not in df.columns or 'Employment_Category' not in df.columns:
        print(f"✗ CRITICAL: Missing employment status columns in {dataset_name}")
        raise ValueError(f"Employment status columns missing in {dataset_name}")
    
    # Show original distributions
    print(f"\n{dataset_name} employment status standardization:")
    ft_dist = df['Full_Part_Time'].value_counts()
    emp_dist = df['Employment_Category'].value_counts()
    print(f"  Full_Part_Time distribution: {dict(ft_dist)}")
    print(f"  Employment_Category distribution: {dict(emp_dist)}")
    
    # Create standardized status
    df['Standardized_Employment_Status'] = ''
    
    # Set FTE for full-time employees
    fte_values = ANALYSIS_CONFIG['fte_values']
    fte_mask = df['Full_Part_Time'].isin(fte_values)
    df.loc[fte_mask, 'Standardized_Employment_Status'] = 'FTE'
    
    # Override with PRN where applicable  
    prn_values = ANALYSIS_CONFIG['prn_values']
    prn_mask = df['Employment_Category'].isin(prn_values)
    df.loc[prn_mask, 'Standardized_Employment_Status'] = 'PRN'
    
    # Validation
    fte_count = (df['Standardized_Employment_Status'] == 'FTE').sum()
    prn_count = (df['Standardized_Employment_Status'] == 'PRN').sum()
    other_count = len(df) - fte_count - prn_count
    
    print(f"  Standardized employment status:")
    print(f"    FTE: {fte_count}")
    print(f"    PRN: {prn_count}")
    print(f"    Other/Missing: {other_count}")
    
    if other_count > 0:
        print(f"✗ WARNING: {other_count} records with unclear employment status in {dataset_name}")
        # Show examples
        other_examples = df[~df['Standardized_Employment_Status'].isin(['FTE', 'PRN'])][
            ['Full_Part_Time', 'Employment_Category']].drop_duplicates().head(5)
        print(f"Examples of unclear status:\n{other_examples}")
    
    if fte_count == 0 and prn_count == 0:
        print(f"✗ CRITICAL: No valid employment status found in {dataset_name}")
        raise ValueError(f"Employment status standardization failed for {dataset_name}")
    
    print(f"✓ {dataset_name} employment status standardization complete")
    return df

# Apply employment status standardization
print("Creating standardized employment status...")
print(f"FTE values: {ANALYSIS_CONFIG['fte_values']}")
print(f"PRN values: {ANALYSIS_CONFIG['prn_values']}")

headcount_filtered = create_standardized_employment_status(headcount_filtered, 'headcount')
hires_filtered = create_standardized_employment_status(hires_filtered, 'hires')
terminations_filtered = create_standardized_employment_status(terminations_filtered, 'terminations')

if has_contractors:
    contractors_filtered = create_standardized_employment_status(contractors_filtered, 'contractors')

# Create FTE and PRN subsets for analysis
print("\nCreating employment status subsets...")

# Headcount subsets
headcount_filtered_fte = headcount_filtered[headcount_filtered['Standardized_Employment_Status'] == 'FTE'].copy()
headcount_filtered_prn = headcount_filtered[headcount_filtered['Standardized_Employment_Status'] == 'PRN'].copy()

# Hires subsets  
hires_filtered_fte = hires_filtered[hires_filtered['Standardized_Employment_Status'] == 'FTE'].copy()
hires_filtered_prn = hires_filtered[hires_filtered['Standardized_Employment_Status'] == 'PRN'].copy()

# Terminations subsets
terminations_filtered_fte = terminations_filtered[terminations_filtered['Standardized_Employment_Status'] == 'FTE'].copy()
terminations_filtered_prn = terminations_filtered[terminations_filtered['Standardized_Employment_Status'] == 'PRN'].copy()

print("Employment status subsets created:")
print(f"  Headcount - FTE: {len(headcount_filtered_fte)}, PRN: {len(headcount_filtered_prn)}")
print(f"  Hires - FTE: {len(hires_filtered_fte)}, PRN: {len(hires_filtered_prn)}")
print(f"  Terminations - FTE: {len(terminations_filtered_fte)}, PRN: {len(terminations_filtered_prn)}")

print("\n✓ Employment status standardization complete")

# ==========================================
# SECTION 7: LOCATION/FACILITY STANDARDIZATION & CLASSIFICATION
# ==========================================

print("\n" + "=" * 80)
print("SECTION 7: LOCATION/FACILITY STANDARDIZATION & CLASSIFICATION")
print("=" * 80)

def standardize_location(location):
    """Clean and standardize location names"""
    if pd.isna(location):
        return location
    
    # Convert to string and basic cleaning
    loc = str(location).strip()
    
    # Remove extra spaces
    loc = re.sub(r'\s+', ' ', loc)
    
    # Remove trailing punctuation and symbols
    loc = re.sub(r'[,.\-\s]+
, '', loc)
    
    # Standardize common abbreviations
    loc = re.sub(r'\bSt\b', 'St', loc)  # Standardize Street
    loc = re.sub(r'\bDr\b', 'Dr', loc)  # Standardize Drive
    loc = re.sub(r'\bSte\b', 'Suite', loc)  # Standardize Suite
    
    # Fix state abbreviations (add comma before state)
    loc = re.sub(r'\s+(NC|SC|VA|TN|GA)\s*
, r', \1', loc)
    
    return loc.strip()

def classify_facility_type_detailed(facility_name):
    """
    Classify facility type and sub-type based on keywords in facility name
    Returns tuple: (primary_type, sub_type)
    """
    if pd.isna(facility_name):
        return 'Unknown', 'Unknown'
    
    name_lower = str(facility_name).lower()
    
    # Hospital classification
    if any(keyword in name_lower for keyword in [
        'hospital', 'medical center', 'medical centre', 'regional medical', 
        'health system', 'emergency', 'trauma', 'intensive care', 'icu',
        'surgery center', 'surgical center', 'ambulatory surgery'
    ]):
        # Sub-type classification for hospitals
        if any(keyword in name_lower for keyword in ['children', 'pediatric', 'peds']):
            sub_type = 'Pediatric Hospital'
        elif any(keyword in name_lower for keyword in ['regional', 'main', 'flagship']):
            sub_type = 'Regional Hospital'
        elif any(keyword in name_lower for keyword in ['community', 'local']):
            sub_type = 'Community Hospital'
        elif any(keyword in name_lower for keyword in ['specialty', 'surgical', 'surgery']):
            sub_type = 'Specialty Hospital'
        else:
            sub_type = 'General Hospital'
        
        return 'Hospital', sub_type
    
    # Clinic classification
    elif any(keyword in name_lower for keyword in [
        'clinic', 'medical group', 'family medicine', 'primary care', 'urgent care',
        'outpatient', 'ambulatory', 'practice', 'office', 'center', 'specialty',
        'cardiology', 'orthopedic', 'oncology', 'cancer', 'heart', 'diabetes',
        'rehabilitation', 'physical therapy', 'occupational health', 'employee health',
        'fracture', 'ent', 'bariatrics', 'imaging', 'gynecology', 'endoscopy',
        'physician', 'pulmonary', 'endocrine', 'surgery', 'surg', 'weight', 'pain',
        'walk-in', 'womancare', 'pulmonology', 'rheumatology', 'arthritis', 'spine',
        'vascular', 'hyperbarics', 'healthcare', 'critical care', 'infectious',
        'neuroscience', 'women\'s', 'sports', 'mobile', 'neuro'
    ]):
        # Sub-type classification for clinics
        if any(keyword in name_lower for keyword in ['dental', 'dentist']):
            sub_type = 'Dental'
        elif any(keyword in name_lower for keyword in ['eye', 'vision', 'ophthalmology', 'optometry']):
            sub_type = 'Eye Care'
        elif any(keyword in name_lower for keyword in ['urgent care', 'walk-in']):
            sub_type = 'Urgent Care'
        elif any(keyword in name_lower for keyword in ['primary care', 'family medicine']):
            sub_type = 'Primary Care'
        else:
            sub_type = 'Specialty Clinic'
        
        return 'Clinic', sub_type
    
    # Default fallback
    else:
        return 'Other', 'Other'

def apply_location_standardization(datasets_dict):
    """Apply location standardization and facility classification to all datasets"""
    print("Standardizing locations across all datasets...")
    
    # Collect all unique locations
    all_locations = set()
    for name, df in datasets_dict.items():
        if 'Location' in df.columns:
            locations = df['Location'].dropna().unique()
            all_locations.update(locations)
    
    original_location_count = len(all_locations)
    print(f"Found {original_location_count} unique locations across all datasets")
    
    # Apply location standardization to each dataset
    for name, df in datasets_dict.items():
        if 'Location' in df.columns:
            print(f"  Standardizing locations in {name}...")
            df['Location'] = df['Location'].apply(standardize_location)
    
    # Re-collect locations after standardization
    all_locations_clean = set()
    for name, df in datasets_dict.items():
        if 'Location' in df.columns:
            locations = df['Location'].dropna().unique()
            all_locations_clean.update(locations)
    
    cleaned_location_count = len(all_locations_clean)
    print(f"After standardization: {cleaned_location_count} unique locations")
    print(f"Consolidation: {original_location_count - cleaned_location_count} duplicates removed")
    
    # Apply facility type classification
    print("\nApplying facility type classification...")
    
    # Create facility type lookup
    unique_facilities = pd.DataFrame({'Facility_Name': sorted(all_locations_clean)})
    facility_classifications = unique_facilities['Facility_Name'].apply(classify_facility_type_detailed)
    unique_facilities['Facility_Type'] = [classification[0] for classification in facility_classifications]
    unique_facilities['Facility_Sub_Type'] = [classification[1] for classification in facility_classifications]
    
    # Show facility type distribution
    print("\nFacility type distribution:")
    type_dist = unique_facilities['Facility_Type'].value_counts()
    for facility_type, count in type_dist.items():
        print(f"  {facility_type}: {count} facilities")
        
        # Show sub-types for each primary type
        sub_types = unique_facilities[unique_facilities['Facility_Type'] == facility_type]['Facility_Sub_Type'].value_counts()
        for sub_type, sub_count in sub_types.items():
            print(f"    {sub_type}: {sub_count}")
    
    # Add facility types to all datasets
    facility_lookup = unique_facilities.set_index('Facility_Name')[['Facility_Type', 'Facility_Sub_Type']]
    
    for name, df in datasets_dict.items():
        if 'Location' in df.columns:
            print(f"  Adding facility types to {name}...")
            df['Facility_Type'] = df['Location'].map(facility_lookup['Facility_Type'])
            df['Facility_Sub_Type'] = df['Location'].map(facility_lookup['Facility_Sub_Type'])
            
            # Validation - ensure all locations got classified
            unclassified = df['Facility_Type'].isna().sum()
            if unclassified > 0:
                print(f"✗ WARNING: {unclassified} locations in {name} could not be classified")
                unclassified_locs = df[df['Facility_Type'].isna()]['Location'].unique()[:5]
                print(f"Examples: {list(unclassified_locs)}")
    
    print("✓ Location standardization and facility classification complete")
    return datasets_dict

# Apply location standardization
datasets_for_location = {
    'headcount_filtered': headcount_filtered,
    'hires_filtered': hires_filtered,
    'terminations_filtered': terminations_filtered,
    'requisitions_filtered': requisitions_filtered
}

if has_contractors:
    datasets_for_location['contractors_filtered'] = contractors_filtered

datasets_for_location = apply_location_standardization(datasets_for_location)

# Update dataset variables
headcount_filtered = datasets_for_location['headcount_filtered']
hires_filtered = datasets_for_location['hires_filtered']
terminations_filtered = datasets_for_location['terminations_filtered']
requisitions_filtered = datasets_for_location['requisitions_filtered']

if has_contractors:
    contractors_filtered = datasets_for_location['contractors_filtered']

# ==========================================
# SECTION 8: CORE METRICS CALCULATION
# ==========================================

print("\n" + "=" * 80)
print("SECTION 8: CORE METRICS CALCULATION")
print("=" * 80)

def calculate_system_wide_metrics():
    """Calculate system-wide workforce metrics"""
    print("Calculating system-wide metrics...")
    
    # Basic counts (FTE only for primary metrics)
    total_headcount_fte = len(headcount_filtered_fte)
    total_hires_fte = len(hires_filtered_fte)
    total_terminations_fte = len(terminations_filtered_fte)
    
    # Contingent workforce counts
    total_headcount_prn = len(headcount_filtered_prn)
    total_hires_prn = len(hires_filtered_prn)
    total_terminations_prn = len(terminations_filtered_prn)
    
    # All workforce counts
    total_headcount_all = len(headcount_filtered)
    total_hires_all = len(hires_filtered)
    total_terminations_all = len(terminations_filtered)
    
    # Facility counts
    all_facilities = pd.concat([
        headcount_filtered['Location'],
        hires_filtered['Location'], 
        terminations_filtered['Location']
    ]).drop_duplicates()
    num_facilities = all_facilities.nunique()
    
    # Requisitions analysis
    open_requisitions = 0
    closed_requisitions = 0
    total_requisitions = len(requisitions_filtered)
    
    if 'Req_Status' in requisitions_filtered.columns:
        open_statuses = ANALYSIS_CONFIG['open_req_statuses']
        closed_statuses = ANALYSIS_CONFIG['closed_req_statuses']
        
        open_requisitions = requisitions_filtered['Req_Status'].isin(open_statuses).sum()
        closed_requisitions = requisitions_filtered['Req_Status'].isin(closed_statuses).sum()
    
    # Calculate derived metrics
    net_change_fte = total_hires_fte - total_terminations_fte
    churn_rate_fte = (total_terminations_fte / total_headcount_fte * 100) if total_headcount_fte > 0 else 0
    vacancy_rate = (open_requisitions / total_headcount_all * 100) if total_headcount_all > 0 else 0
    
    # Contingent to FTE ratio
    if total_headcount_fte > 0:
        ratio_prn = total_headcount_prn
        ratio_fte = total_headcount_fte
        common_divisor = gcd(ratio_prn, ratio_fte) if ratio_prn > 0 else 1
        ratio_prn_simplified = ratio_prn // common_divisor if ratio_prn > 0 else 0
        ratio_fte_simplified = ratio_fte // common_divisor
        contingent_to_fte_ratio = f"{ratio_prn_simplified}:{ratio_fte_simplified}"
    else:
        contingent_to_fte_ratio = "N/A"
    
    # Create system overview
    system_overview = {
        'Analysis_Date': ANALYSIS_CONFIG['analysis_date'],
        'Target_State': ANALYSIS_CONFIG['target_state'],
        'Total_Headcount_FTE': total_headcount_fte,
        'Total_Headcount_PRN': total_headcount_prn,
        'Total_Headcount_All': total_headcount_all,
        'Total_Hires_FTE': total_hires_fte,
        'Total_Hires_PRN': total_hires_prn,
        'Total_Hires_All': total_hires_all,
        'Total_Terminations_FTE': total_terminations_fte,
        'Total_Terminations_PRN': total_terminations_prn,
        'Total_Terminations_All': total_terminations_all,
        'Net_Change_FTE': net_change_fte,
        'Open_Requisitions': open_requisitions,
        'Closed_Requisitions': closed_requisitions,
        'Total_Requisitions': total_requisitions,
        'Vacancy_Rate_Percent': round(vacancy_rate, 1),
        'Churn_Rate_FTE_Percent': round(churn_rate_fte, 1),
        'Contingent_to_FTE_Ratio': contingent_to_fte_ratio,
        'Number_of_Facilities': num_facilities
    }
    
    print("SYSTEM-WIDE METRICS:")
    for key, value in system_overview.items():
        print(f"  {key}: {value}")
    
    # Validation checks
    print(f"\nVALIDATION CHECKS:")
    print(f"  Total headcount matches: {total_headcount_fte + total_headcount_prn == total_headcount_all}")
    print(f"  Total hires matches: {total_hires_fte + total_hires_prn == total_hires_all}")
    print(f"  Total terminations matches: {total_terminations_fte + total_terminations_prn == total_terminations_all}")
    print(f"  Requisitions categorized: {open_requisitions + closed_requisitions} of {total_requisitions}")
    
    return system_overview

def calculate_job_title_metrics():
    """Calculate metrics by standardized job title"""
    print("\nCalculating metrics by job title...")
    
    # Headcount by job title
    headcount_by_role = headcount_filtered.groupby('Standardized_Job_Title').size().rename('Headcount_All')
    headcount_fte_by_role = headcount_filtered_fte.groupby('Standardized_Job_Title').size().rename('Headcount_FTE')
    headcount_prn_by_role = headcount_filtered_prn.groupby('Standardized_Job_Title').size().rename('Headcount_PRN')
    
    # Hires by job title
    hires_by_role = hires_filtered.groupby('Standardized_Job_Title').size().rename('Hires_All')
    hires_fte_by_role = hires_filtered_fte.groupby('Standardized_Job_Title').size().rename('Hires_FTE')
    hires_prn_by_role = hires_filtered_prn.groupby('Standardized_Job_Title').size().rename('Hires_PRN')
    
    # Terminations by job title
    terminations_by_role = terminations_filtered.groupby('Standardized_Job_Title').size().rename('Terminations_All')
    terminations_fte_by_role = terminations_filtered_fte.groupby('Standardized_Job_Title').size().rename('Terminations_FTE')
    terminations_prn_by_role = terminations_filtered_prn.groupby('Standardized_Job_Title').size().rename('Terminations_PRN')
    
    # Requisitions by job title
    if 'Req_Status' in requisitions_filtered.columns:
        open_statuses = ANALYSIS_CONFIG['open_req_statuses']
        closed_statuses = ANALYSIS_CONFIG['closed_req_statuses']
        
        requisitions_by_role = requisitions_filtered.groupby('Standardized_Job_Title').agg(
            Open_Requisitions=('Req_Status', lambda x: x.isin(open_statuses).sum()),
            Closed_Requisitions=('Req_Status', lambda x: x.isin(closed_statuses).sum()),
            Total_Requisitions=('Req_Status', 'count')
        )
    else:
        # If no status column, just count total requisitions
        requisitions_by_role = requisitions_filtered.groupby('Standardized_Job_Title').size().to_frame('Total_Requisitions')
        requisitions_by_role['Open_Requisitions'] = 0
        requisitions_by_role['Closed_Requisitions'] = 0
    
    # Facilities by job title
    facilities_by_role = headcount_filtered.groupby('Standardized_Job_Title')['Location'].nunique().rename('Number_of_Facilities')
    
    # Combine all metrics
    job_title_summary = pd.DataFrame({
        'Headcount_All': headcount_by_role,
        'Headcount_FTE': headcount_fte_by_role,
        'Headcount_PRN': headcount_prn_by_role,
        'Hires_All': hires_by_role,
        'Hires_FTE': hires_fte_by_role,
        'Hires_PRN': hires_prn_by_role,
        'Terminations_All': terminations_by_role,
        'Terminations_FTE': terminations_fte_by_role,
        'Terminations_PRN': terminations_prn_by_role,
        'Open_Requisitions': requisitions_by_role['Open_Requisitions'],
        'Closed_Requisitions': requisitions_by_role['Closed_Requisitions'],
        'Total_Requisitions': requisitions_by_role['Total_Requisitions'],
        'Number_of_Facilities': facilities_by_role
    }).fillna(0)
    
    # Calculate derived metrics
    job_title_summary['Net_Change_FTE'] = job_title_summary['Hires_FTE'] - job_title_summary['Terminations_FTE']
    job_title_summary['Turnover_Rate_FTE_Percent'] = (
        job_title_summary['Terminations_FTE'] / job_title_summary['Headcount_FTE'] * 100
    ).round(1)
    job_title_summary['Vacancy_Rate_Percent'] = (
        job_title_summary['Open_Requisitions'] / job_title_summary['Headcount_All'] * 100
    ).round(1)
    
    # Calculate contingent ratios
    def calculate_contingent_ratio(row):
        prn_count = int(row['Headcount_PRN'])
        fte_count = int(row['Headcount_FTE'])
        
        if fte_count == 0 and prn_count > 0:
            return f"{prn_count}:0"
        elif prn_count == 0:
            return "0:1"
        else:
            common_divisor = gcd(prn_count, fte_count)
            prn_simplified = prn_count // common_divisor
            fte_simplified = fte_count // common_divisor
            return f"{prn_simplified}:{fte_simplified}"
    
    job_title_summary['Contingent_to_FTE_Ratio'] = job_title_summary.apply(calculate_contingent_ratio, axis=1)
    
    print("JOB TITLE METRICS:")
    print(job_title_summary.to_string())
    
    # Validation - check totals match system-wide
    total_headcount_check = job_title_summary['Headcount_All'].sum()
    total_hires_check = job_title_summary['Hires_All'].sum()
    total_terms_check = job_title_summary['Terminations_All'].sum()
    
    print(f"\nVALIDATION - Job title totals vs system totals:")
    print(f"  Headcount: {total_headcount_check} (should equal {len(headcount_filtered)})")
    print(f"  Hires: {total_hires_check} (should equal {len(hires_filtered)})")
    print(f"  Terminations: {total_terms_check} (should equal {len(terminations_filtered)})")
    
    if (total_headcount_check != len(headcount_filtered) or 
        total_hires_check != len(hires_filtered) or 
        total_terms_check != len(terminations_filtered)):
        print("✗ CRITICAL: Job title totals don't match system totals!")
        raise ValueError("Job title aggregation failed validation")
    
    print("✓ Job title metrics validation passed")
    return job_title_summary

# Calculate metrics
system_overview = calculate_system_wide_metrics()
job_title_summary = calculate_job_title_metrics()

print("\n✓ Core metrics calculation complete")

# ==========================================
# SECTION 9: FACILITY-LEVEL ANALYSIS SETUP
# ==========================================

print("\n" + "=" * 80)
print("SECTION 9: FACILITY-LEVEL ANALYSIS SETUP")
print("=" * 80)

def create_facility_level_metrics():
    """Create comprehensive facility-level workforce metrics"""
    print("Creating facility-level metrics by job title...")
    
    # Calculate metrics by facility and job title
    headcount_by_facility_role = headcount_filtered.groupby(['Location', 'Standardized_Job_Title']).size().rename('Headcount_All')
    headcount_fte_by_facility_role = headcount_filtered_fte.groupby(['Location', 'Standardized_Job_Title']).size().rename('Headcount_FTE')
    headcount_prn_by_facility_role = headcount_filtered_prn.groupby(['Location', 'Standardized_Job_Title']).size().rename('Headcount_PRN')
    
    hires_by_facility_role = hires_filtered.groupby(['Location', 'Standardized_Job_Title']).size().rename('Hires_All')
    hires_fte_by_facility_role = hires_filtered_fte.groupby(['Location', 'Standardized_Job_Title']).size().rename('Hires_FTE')
    hires_prn_by_facility_role = hires_filtered_prn.groupby(['Location', 'Standardized_Job_Title']).size().rename('Hires_PRN')
    
    terminations_by_facility_role = terminations_filtered.groupby(['Location', 'Standardized_Job_Title']).size().rename('Terminations_All')
    terminations_fte_by_facility_role = terminations_filtered_fte.groupby(['Location', 'Standardized_Job_Title']).size().rename('Terminations_FTE')
    terminations_prn_by_facility_role = terminations_filtered_prn.groupby(['Location', 'Standardized_Job_Title']).size().rename('Terminations_PRN')
    
    # Requisitions by facility and job title
    if 'Req_Status' in requisitions_filtered.columns:
        open_statuses = ANALYSIS_CONFIG['open_req_statuses']
        closed_statuses = ANALYSIS_CONFIG['closed_req_statuses']
        
        requisitions_by_facility_role = requisitions_filtered.groupby(['Location', 'Standardized_Job_Title']).agg(
            Open_Requisitions=('Req_Status', lambda x: x.isin(open_statuses).sum()),
            Closed_Requisitions=('Req_Status', lambda x: x.isin(closed_statuses).sum()),
            Total_Requisitions=('Req_Status', 'count')
        )
    else:
        requisitions_by_facility_role = requisitions_filtered.groupby(['Location', 'Standardized_Job_Title']).size().to_frame('Total_Requisitions')
        requisitions_by_facility_role['Open_Requisitions'] = 0
        requisitions_by_facility_role['Closed_Requisitions'] = 0
    
    # Combine all facility-level metrics
    facility_job_metrics = pd.DataFrame({
        'Headcount_All': headcount_by_facility_role,
        'Headcount_FTE': headcount_fte_by_facility_role,
        'Headcount_PRN': headcount_prn_by_facility_role,
        'Hires_All': hires_by_facility_role,
        'Hires_FTE': hires_fte_by_facility_role,
        'Hires_PRN': hires_prn_by_facility_role,
        'Terminations_All': terminations_by_facility_role,
        'Terminations_FTE': terminations_fte_by_facility_role,
        'Terminations_PRN': terminations_prn_by_facility_role,
        'Open_Requisitions': requisitions_by_facility_role['Open_Requisitions'],
        'Closed_Requisitions': requisitions_by_facility_role['Closed_Requisitions'],
        'Total_Requisitions': requisitions_by_facility_role['Total_Requisitions']
    }).fillna(0)
    
    # Calculate derived metrics
    facility_job_metrics['Net_Change_FTE'] = facility_job_metrics['Hires_FTE'] - facility_job_metrics['Terminations_FTE']
    
    facility_job_metrics['Turnover_Rate_FTE_Percent'] = facility_job_metrics.apply(
        lambda row: (row['Terminations_FTE'] / row['Headcount_FTE'] * 100) if row['Headcount_FTE'] > 0 else 0,
        axis=1
    ).round(1)
    
    facility_job_metrics['Vacancy_Rate_Percent'] = facility_job_metrics.apply(
        lambda row: (row['Open_Requisitions'] / row['Headcount_All'] * 100) if row['Headcount_All'] > 0 else 0,
        axis=1
    ).round(1)
    
    # Reset index to make facility and job title regular columns
    facility_job_metrics = facility_job_metrics.reset_index()
    
    # Add facility type information
    facility_type_lookup = headcount_filtered[['Location', 'Facility_Type', 'Facility_Sub_Type']].drop_duplicates()
    facility_job_metrics = facility_job_metrics.merge(
        facility_type_lookup,
        on='Location',
        how='left'
    )
    
    # Reorder columns
    column_order = [
        'Location', 'Facility_Type', 'Facility_Sub_Type', 'Standardized_Job_Title',
        'Headcount_All', 'Headcount_FTE', 'Headcount_PRN',
        'Hires_All', 'Hires_FTE', 'Hires_PRN',
        'Terminations_All', 'Terminations_FTE', 'Terminations_PRN',
        'Net_Change_FTE', 'Open_Requisitions', 'Closed_Requisitions', 'Total_Requisitions',
        'Turnover_Rate_FTE_Percent', 'Vacancy_Rate_Percent'
    ]
    
    facility_job_metrics = facility_job_metrics[column_order]
    
    # Sort by facility type, facility name, then job title
    facility_job_metrics = facility_job_metrics.sort_values(['Facility_Type', 'Location', 'Standardized_Job_Title'])
    
    print(f"Facility-level analysis complete:")
    print(f"  Total facility-job title combinations: {len(facility_job_metrics)}")
    print(f"  Unique facilities: {facility_job_metrics['Location'].nunique()}")
    print(f"  Job titles: {facility_job_metrics['Standardized_Job_Title'].nunique()}")
    print(f"  Facility types: {facility_job_metrics['Facility_Type'].nunique()}")
    
    # Show sample
    print(f"\nSample facility metrics (first 10 rows):")
    sample_cols = ['Location', 'Facility_Type', 'Standardized_Job_Title', 'Headcount_All', 'Hires_All', 'Terminations_All']
    print(facility_job_metrics[sample_cols].head(10).to_string())
    
    # Validation - ensure totals still match
    facility_headcount_total = facility_job_metrics['Headcount_All'].sum()
    facility_hires_total = facility_job_metrics['Hires_All'].sum()
    facility_terms_total = facility_job_metrics['Terminations_All'].sum()
    
    print(f"\nVALIDATION - Facility totals vs system totals:")
    print(f"  Headcount: {facility_headcount_total} (should equal {len(headcount_filtered)})")
    print(f"  Hires: {facility_hires_total} (should equal {len(hires_filtered)})")
    print(f"  Terminations: {facility_terms_total} (should equal {len(terminations_filtered)})")
    
    if (facility_headcount_total != len(headcount_filtered) or
        facility_hires_total != len(hires_filtered) or
        facility_terms_total != len(terminations_filtered)):
        print("✗ CRITICAL: Facility totals don't match system totals!")
        raise ValueError("Facility-level aggregation failed validation")
    
    print("✓ Facility-level metrics validation passed")
    return facility_job_metrics

# Create facility-level metrics
facility_job_metrics = create_facility_level_metrics()

print("\n✓ Facility-level analysis setup complete")

print("\n" + "=" * 80)
print("DATA PREPARATION COMPLETE")
print("=" * 80)
print("All datasets have been loaded, validated, filtered, and standardized.")
print("All metrics have been calculated and validated.")
print("Ready for output generation.")
print(f"""
FINAL DATASET SUMMARY:
- headcount_filtered: {len(headcount_filtered)} rows
- hires_filtered: {len(hires_filtered)} rows  
- terminations_filtered: {len(terminations_filtered)} rows
- requisitions_filtered: {len(requisitions_filtered)} rows
- Target job titles: {sorted(headcount_filtered['
