import pandas as pd
import numpy as np
from datetime import datetime

# -----------------------------
# Config / mappings
# -----------------------------
INPUT_FILE = "Book1.xlsx"
OUTPUT_FILE = "Book2.xlsx" 
UNCLASSIFIED_FILE = "unclassified_words.txt"

# Keywords found in "Nature Goods" (nature has priority)
CATEGORY_KEYWORDS = {
    "MEAT": ["meat", "beef", "goat", "mutton", "pork", "chicken", "nyama", "frozen meat", "chilled meat", "sheep", "goat carcass"],
    "FISH": ["fish", "samaki", "tilapia", "sardines", "dagaa"],
    "CRABS/LOBSTER": ["lobster", "crab", "kamba"],
    "FLOWERS": ["flower", "rose", "maua", "carnation", "tulip"],
    "VEGETABLES": ["vegetable", "vegetables", "veg", "mboga"],
    "AVOCADO": ["avocado", "parachichi"],
    "VALUABLES": ["valuable", "valuables", "jewelry", "cash", "money", "gold"],
    "COURIER": ["courier", "parcel", "express", "ems"],
    "P.O.MAIL": ["mail", "postal", "posta"],
    "PER/COL": ["perishable", "perishables", "chilled", "frozen", "fresh", "col"],
}

# SHC mapping (codes -> category). If a code maps to multiple categories in practice,
# choose a default here (nature will override it when present).
SHC_MAP = {
    "PEM": "MEAT",
    "PES": "FISH",       # PES => default FISH (nature can override to CRABS/LOBSTER)
    "PEF": "FLOWERS",
    "FLW": "FLOWERS",
    "AVI": "AVOCADO",
    "COL": "PER/COL",
    "PER": "PER/COL",
    "MAL": "P.O.MAIL",
    "COU": "COURIER",
    "VAL": "VALUABLES",
    "DG": "DG",
    "GEN": "G. CARGO",
    "GCR": "G. CARGO",
    "NWP": "G. CARGO",
    "RCM": "DG",
    "RRY": "DG",
    "RCL": "DG",
    "RMD": "DG",
    "FRO": "PER/COL",
    "RFL": "DG",
    "HUM": "G. CARGO",
    "RNG": "DG", 
    "RIS": "DG",
}

# Output column layout (sector will be left empty)
OUTPUT_COLUMNS = [
    "DATE", "AIRLINE", "FLIGHT No", "SECTOR", "F/CATEGORY",
    "G. CARGO", "VEGETABLES", "AVOCADO", "FISH", "MEAT", "VALUABLES",
    "FLOWERS", "PER/COL", "DG", "CRABS/LOBSTER", "P.O.MAIL", "COURIER",
    "G. AWBs", "VAL AWBs", "VEGETABLES AWBs", "AVOCADO AWBs", "FISH AWBs",
    "MEAT AWBs", "COURIER AWBs", "CRAB/LOBSTER AWBs", "FLOWERS AWBs",
    "PER/COL AWBs", "DG AWBs", "P.O.MAIL AWBs", "TOTAL AWBs", "TOTAL WEIGHT"
]

# AWB counters mapping (category -> AWB column)
AWB_COL_MAP = {
    "G. CARGO": "G. AWBs",
    "VALUABLES": "VAL AWBs",
    "VEGETABLES": "VEGETABLES AWBs",
    "AVOCADO": "AVOCADO AWBs",
    "FISH": "FISH AWBs",
    "MEAT": "MEAT AWBs",
    "COURIER": "COURIER AWBs",
    "CRABS/LOBSTER": "CRAB/LOBSTER AWBs",
    "FLOWERS": "FLOWERS AWBs",
    "PER/COL": "PER/COL AWBs",
    "DG": "DG AWBs",
    "P.O.MAIL": "P.O.MAIL AWBs",
}

GENERIC_WORDS = {"perishable", "perishables", "chilled", "frozen", "fresh", "col", "per"}

# -----------------------------
# Utilities
# -----------------------------
def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_lower(x):
    return normalize_text(x).lower()

def ensure_log_header():
    """Ensure each run writes a timestamp header at top of log (append if exists)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"\n==== Run on {ts} ====\n"
    # Create file with header if doesn't exist, else append header
    with open(UNCLASSIFIED_FILE, "a", encoding="utf-8") as f:
        f.write(header)

def log_unclassified(kind, awb, nature, shcs):
    """Append a single unclassified/conflict entry (assumes run header already written)."""
    awb_s = normalize_text(awb)
    nature_s = normalize_text(nature)
    shcs_s = normalize_text(shcs)
    with open(UNCLASSIFIED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{kind} | AWB:{awb_s} | Nature:{nature_s} | SHC:{shcs_s}\n")

# -----------------------------
# Flight category logic
# -----------------------------
def classify_flight_category(carrier, flight_no):
    """
    Classify flight category based on carrier and flight number.
    """
    carrier = str(carrier).upper() if pd.notna(carrier) else ''
    flight_no = str(flight_no).upper() if pd.notna(flight_no) else ''
    
    # TC flights
    if carrier == 'TC':
        if flight_no.startswith('TC1'):
            return 'DOMESTIC'
        elif flight_no.startswith('TC2') or flight_no.startswith('TC4'):
            return 'TC-FOREIGN'
        else:
            return 'FOREIGN'
    
    # PW flights
    elif carrier == 'PW':
        if flight_no in ['PW717', 'PW721']:
            return 'PW-FOREIGN'
        else:
            return 'DOMESTIC'
    
    # All other carriers
    else:
        return 'FOREIGN'

def transform_data(input_file='Book1.xlsx', output_file='Book2.xlsx'):
    """
    Main transformation function that processes Book1 format to Book2 format.
    """
    print(f"Reading {input_file}...")
    
    # Read Book1 with header at row 1 (index 1)
    df_book1 = pd.read_excel(input_file, header=0)
    
    # Clean column names
    df_book1.columns = df_book1.columns.str.strip()
    
    print(f"Total rows in Book1: {len(df_book1)}")
    
    # Initialize list for unclassified items
    unclassified_log = []
    
    # Group by Flight date, Carrier, and Flight No
    grouped = df_book1.groupby(['Flight date', 'Carrier', 'Flight No.'])
    
    # Initialize Book2 structure
    book2_data = []
    
    # Define all category columns
    category_columns = ['G. CARGO', 'VEGETABLES', 'AVOCADO', 'FISH', 'MEAT', 
                       'VALUABLES', 'FLOWERS', 'PER/COL', 'DG', 'CRABS/LOBSTER', 
                       'P.O.MAIL', 'COURIER']
    
    awb_columns = ['G. AWBs', 'VAL AWBs', 'VEGETABLES AWBs', 'AVOCADO AWBs', 
                   'FISH AWBs', 'MEAT AWBs', 'COURIER AWBs', 'CRAB/LOBSTER AWBs', 
                   'FLOWERS AWBs', 'PER/COL AWBs', 'DG AWBs']
    
    total_awb_count = 0
    
    for (flight_date, carrier, flight_no), group in grouped:
        # Initialize row for Book2
        row_data = {
            'DATE': flight_date,
            'AIRLINE': carrier,
            'FLIGHT No': flight_no,
            'SECTOR': 0,  # Leave empty/0 as specified
            'F/CATEGORY': classify_flight_category(carrier, flight_no)
        }
        
        # Initialize all weight and AWB count columns to 0
        for col in category_columns:
            row_data[col] = 0
        for col in awb_columns:
            row_data[col] = 0
        
        # Process each AWB in the group
        for _, awb_row in group.iterrows():
            category, weight = classify_cargo(awb_row, unclassified_log)
            
            # Add weight to the appropriate category
            if category in row_data:
                row_data[category] += weight
            
            # Increment AWB count for the category
            awb_col_mapping = {
                'G. CARGO': 'G. AWBs',
                'VALUABLES': 'VAL AWBs',
                'VEGETABLES': 'VEGETABLES AWBs',
                'AVOCADO': 'AVOCADO AWBs',
                'FISH': 'FISH AWBs',
                'MEAT': 'MEAT AWBs',
                'COURIER': 'COURIER AWBs',
                'CRABS/LOBSTER': 'CRAB/LOBSTER AWBs',
                'FLOWERS': 'FLOWERS AWBs',
                'PER/COL': 'PER/COL AWBs',
                'DG': 'DG AWBs',
                'P.O.MAIL': 'G. AWBs'  # P.O.MAIL counted in G. AWBs
            }
            
            if category in awb_col_mapping:
                row_data[awb_col_mapping[category]] += 1
            
            total_awb_count += 1
        
        # Calculate totals
        row_data['TOTAL AWBs'] = len(group)  # Total AWBs for this flight
        row_data['TOTAL WEIGHT'] = sum(row_data[col] for col in category_columns)
        
        book2_data.append(row_data)
    
    # Create DataFrame for Book2
    df_book2 = pd.DataFrame(book2_data)
    
    # Ensure all columns are in the correct order
    column_order = ['DATE', 'AIRLINE', 'FLIGHT No', 'SECTOR', 'F/CATEGORY'] + \
                   category_columns + awb_columns + ['TOTAL AWBs', 'TOTAL WEIGHT']
    
    # Add any missing columns with 0 values
    for col in column_order:
        if col not in df_book2.columns:
            df_book2[col] = 0
    
    df_book2 = df_book2[column_order]
    
    # Verify AWB count
    total_awbs_book2 = df_book2['TOTAL AWBs'].sum()
    print(f"\nVerification:")
    print(f"Total AWBs in Book1: {len(df_book1)}")
    print(f"Total AWBs in Book2: {total_awbs_book2}")
    print(f"Match: {'✓' if total_awbs_book2 == len(df_book1) else '✗'}")
    
    # Save Book2
    df_book2.to_excel(output_file, index=False)
    print(f"\nBook2 saved to {output_file}")
    
    # Save unclassified log
    if unclassified_log:
        log_filename = 'unclassified_words.txt'
        with open(log_filename, 'a') as f:
            f.write(f"\n{'='*50}\n")
            f.write(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*50}\n")
            for item in unclassified_log:
                f.write(f"AWB: {item['AWB']}\n")
                f.write(f"Nature Goods: {item['Nature Goods']}\n")
                f.write(f"SHCs: {item['SHCs']}\n")
                f.write(f"Timestamp: {item['Timestamp']}\n")
                f.write(f"{'-'*30}\n")
        print(f"Unclassified items logged to {log_filename} ({len(unclassified_log)} items)")
    else:
        print("No unclassified items found.")
    
    # Print summary statistics
    print(f"\nSummary Statistics:")
    print(f"Total flights processed: {len(df_book2)}")
    print(f"Total weight processed: {df_book2['TOTAL WEIGHT'].sum():,.2f} kg")
    
    # Category breakdown
    print(f"\nCategory Breakdown (Weight):")
    for col in category_columns:
        count = df_book2[col].sum()
        if count > 0:
            print(f"  {col}: {count} kg")
    

    print(f"\nCategory Breakdown (AWBs):")
    for col in awb_columns:
        count = df_book2[col].sum()
        if count > 0:
            print(f"  {col}: {count}")
    
    return df_book2
    

if __name__ == "__main__":
    # Run the transformation
    result = transform_data('Book1.xlsx', 'Book2.xlsx')
    print("\nTransformation complete!")
    print("Files created:")
    print("  - Book2.xlsx (transformed data)")
    print("  - unclassified_words.txt (log file, if any unclassified items)")