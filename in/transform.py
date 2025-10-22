import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def classify_cargo(row, unclassified_log, transit_conflict_log):
    """
    Classify cargo based on STRICT PRIORITY ORDER:
    1. TRANSIT (highest priority)
    2. P.O MAIL
    3. COURIER
    4. PER/COL
    5. DG
    6. GENCARGO (lowest priority/default)
    
    Each AWB is classified into ONLY ONE category.
    Returns the category and weight for that category.
    """
    nature_goods = str(row['Nature Goods']).lower() if pd.notna(row['Nature Goods']) else ''
    shcs = str(row['SHCs']).upper() if pd.notna(row['SHCs']) else ''
    weight = float(row['Weight']) if pd.notna(row['Weight']) else 0
    awb = str(row['AWB']) if pd.notna(row['AWB']) else ''
    import_status = str(row['Import Status']).upper() if pd.notna(row['Import Status']) else ''
    awb_dest = str(row['AWB Dest']).upper().strip() if pd.notna(row['AWB Dest']) else ''
    
    # Skip AWBs with zero weight
    if weight == 0:
        return None, 0
    
    # PRIORITY 1: TRANSIT (HIGHEST PRIORITY)
    # Transit = Import Status contains "CKD" AND AWB Dest is NOT "DAR"
    # Log if only one condition is true (transit conflict)
    has_ckd = 'CKD' in import_status
    dest_not_dar = awb_dest != 'DAR' and awb_dest != ''
    
    if has_ckd and dest_not_dar:
        # Both conditions true - it's TRANSIT
        return 'TRANSIT', weight
    elif has_ckd or dest_not_dar:
        # Only one condition true - log as transit conflict
        transit_conflict_log.append({
            'AWB': awb,
            'Import Status': row['Import Status'],
            'AWB Dest': row['AWB Dest'],
            'Has CKD': has_ckd,
            'Dest Not DAR': dest_not_dar,
            'Weight': weight,
            'Nature Goods': row['Nature Goods'],
            'SHCs': row['SHCs'],
            'Reason': 'CKD without non-DAR destination' if has_ckd else 'Non-DAR destination without CKD',
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # PRIORITY 2: P.O MAIL
    # Check AWB prefix for P.O.MAIL
    if awb.startswith('MAL'):
        return 'P.O MAIL', weight
    
    # PRIORITY 3: COURIER
    # If COU SHC or courier in nature goods, classify as COURIER
    # This overrides PER/COL, DG, and GENCARGO
    if 'COU' in shcs or 'courier' in nature_goods:
        return 'COURIER', weight
    
    # PRIORITY 4: PER/COL (Perishables/Cold Chain)
    # Check for perishable SHCs
    if any(term in shcs for term in ['COL', 'FRO', 'CRT', 'ICE', 'ERT', 'PER', 'PEF', 'PES', 'PEM']):
        return 'PER/COL', weight
    
    # Check for perishable terms in nature goods
    perishable_terms = ['perishable', 'fresh', 'chilled', 'frozen', 'cool', 'cold', 
                       'flower', 'fish', 'meat', 'vegetable', 'fruit', 'avocado']
    if any(term in nature_goods for term in perishable_terms):
        return 'PER/COL', weight
    
    # PRIORITY 5: DG (Dangerous Goods)
    # Check for dangerous goods SHCs
    if any(term in shcs for term in ['DGR', 'RRY', 'RMD', 'RPB', 'RFL', 'RCG', 'RNG', 'RIS', 'RDS']):
        return 'DG', weight
    
    # Check for dangerous in nature goods
    if 'dangerous' in nature_goods:
        return 'DG', weight
    
    # PRIORITY 6: GENCARGO (Default)
    # Check for general cargo SHCs
    if any(term in shcs for term in ['GEN', 'GCR']):
        return 'GENCARGO', weight
    
    # If we can't classify with confidence, log it
    if nature_goods and nature_goods not in ['general cargo', 'cargo', 'general', 'gen', '']:
        unclassified_log.append({
            'AWB': awb,
            'Nature Goods': row['Nature Goods'],
            'SHCs': row['SHCs'],
            'Import Status': row['Import Status'],
            'AWB Dest': row['AWB Dest'],
            'Weight': weight,
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Default to general cargo
    return 'GENCARGO', weight

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
        elif flight_no.startswith('TC2') or flight_no.startswith('TC4') or flight_no.startswith('TC5') or flight_no.startswith('TC05'):
            return 'TC-FOREIGN'
        else:
            return 'FOREIGN'
    
    # PW flights
    elif carrier == 'PW':
        return 'DOMESTIC'
    
    # All other carriers
    else:
        return 'FOREIGN'
    
def classify_flight_route(origin, dest):
    """
    Create route string from origin and destination.
    """
    origin = str(origin).upper() if pd.notna(origin) else ''
    dest = str(dest).upper() if pd.notna(dest) else ''
    return f"{origin}-{dest}"
    

def transform_data(input_file='Book1.xlsx', output_file='Book2.xlsx'):
    """
    Main transformation function that processes Book1 format to Book2 format.
    """
    print(f"Reading {input_file}...")
    
    # Read Book1 with header at row 0
    df_book1 = pd.read_excel(input_file, header=0)
    
    # Clean column names - remove extra spaces
    df_book1.columns = df_book1.columns.str.strip()
    
    print(f"Total rows in Book1: {len(df_book1)}")
    print(f"\nColumns found in Book1:")
    for i, col in enumerate(df_book1.columns):
        print(f"  {i}: '{col}'")
    print()
    
    # Try to identify the correct column names (case-insensitive matching)
    # IMPORTANT: Check more specific conditions first to avoid conflicts
    column_mapping = {}
    for col in df_book1.columns:
        col_lower = col.lower()
        if 'flight' in col_lower and 'date' in col_lower:
            column_mapping['Flight date'] = col
        elif col_lower in ['carrier', 'airlines', 'airline']:
            column_mapping['Carrier'] = col
        elif 'flight' in col_lower and ('no' in col_lower or 'number' in col_lower):
            column_mapping['Flight No.'] = col
        elif 'origin' in col_lower and 'awb' not in col_lower:
            column_mapping['Origin'] = col
        elif 'awb' in col_lower and 'dest' in col_lower:  # Check AWB Dest FIRST (more specific)
            column_mapping['AWB Dest'] = col
        elif col_lower in ['dest', 'destination']:  # Then check Dest (more general)
            column_mapping['Dest'] = col
        elif col_lower == 'awb':
            column_mapping['AWB'] = col
        elif 'piece' in col_lower:
            column_mapping['Pieces'] = col
        elif 'uld' in col_lower and 'number' in col_lower:
            column_mapping['ULD Number'] = col
        elif 'nature' in col_lower and 'goods' in col_lower:
            column_mapping['Nature Goods'] = col
        elif 'import' in col_lower and 'status' in col_lower:
            column_mapping['Import Status'] = col
        elif 'weight' in col_lower and 'total' not in col_lower:
            column_mapping['Weight'] = col
        elif 'shc' in col_lower:
            column_mapping['SHCs'] = col
    
    # Rename columns to expected names
    df_book1 = df_book1.rename(columns={v: k for k, v in column_mapping.items()})
    
    print(f"\nColumn mapping applied:")
    for k, v in column_mapping.items():
        print(f"  '{v}' -> '{k}'")
    print()
    
    # Check if we have all required columns
    required_columns = ['Flight date', 'Carrier', 'Flight No.', 'Origin', 'Dest', 
                       'AWB', 'Pieces', 'Weight', 'ULD Number', 'Import Status', 'AWB Dest', 'Nature Goods', 'SHCs']
    missing_columns = [col for col in required_columns if col not in df_book1.columns]
    
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df_book1.columns)}")
        return None
    
    # Initialize lists for logging
    unclassified_log = []
    transit_conflict_log = []
    
    # Count rows before filtering
    total_rows_before = len(df_book1)
    
    # STEP 1: Filter out unwanted Import Status (MIS, ACC, NOT)
    excluded_statuses = ['MIS', 'ACC', 'NOT']
    df_book1['Import Status Clean'] = df_book1['Import Status'].astype(str).str.upper().str.strip()
    rows_before_status_filter = len(df_book1)
    df_book1 = df_book1[~df_book1['Import Status Clean'].isin(excluded_statuses)].copy()
    status_filtered = rows_before_status_filter - len(df_book1)
    
    if status_filtered > 0:
        print(f"Filtered out {status_filtered} rows with Import Status: {excluded_statuses}")
    
    # STEP 2: Filter out AWBs with zero weight
    rows_before_weight_filter = len(df_book1)
    df_book1 = df_book1[df_book1['Weight'] != 0].copy()
    weight_filtered = rows_before_weight_filter - len(df_book1)
    
    if weight_filtered > 0:
        print(f"Filtered out {weight_filtered} rows with zero weight")
    
    # STEP 3: Remove system-generated duplicates with STRICT matching
    # Duplicates are identified as same: Flight Date, Flight No., AWB, Pieces, Weight, ULD Number, Nature Goods, SHCs
    print(f"\nChecking for duplicate entries (strict matching)...")
    rows_before_dedup = len(df_book1)
    
    # Clean ULD Number - strip whitespace
    df_book1['ULD Number'] = df_book1['ULD Number'].astype(str).str.strip()
    
    # Clean Nature Goods and SHCs for comparison
    df_book1['Nature Goods Clean'] = df_book1['Nature Goods'].astype(str).str.strip().str.lower()
    df_book1['SHCs Clean'] = df_book1['SHCs'].astype(str).str.strip().str.upper()
    
    # Identify duplicates based on the combination of ALL key fields (STRICT)
    duplicate_check_cols = ['Flight date', 'Flight No.', 'AWB', 'Pieces', 'Weight', 'ULD Number', 'Nature Goods Clean', 'SHCs Clean']
    
    # Find duplicates before removing them (for logging)
    duplicates_df = df_book1[df_book1.duplicated(subset=duplicate_check_cols, keep=False)]
    
    # Keep only the first occurrence of each duplicate group
    df_book1 = df_book1.drop_duplicates(subset=duplicate_check_cols, keep='first')
    
    duplicates_removed = rows_before_dedup - len(df_book1)
    
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate rows (strict matching: Flight Date, Flight No., AWB, Pieces, Weight, ULD, Nature Goods, SHCs)")
        
        # Log duplicates to file
        if len(duplicates_df) > 0:
            duplicate_log_filename = 'duplicate_entries.txt'
            with open(duplicate_log_filename, 'w') as f:
                f.write(f"Duplicate Entries Report (Strict Matching)\n")
                f.write(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'='*100}\n\n")
                f.write(f"Total duplicate rows found: {len(duplicates_df)}\n")
                f.write(f"Duplicate groups removed: {duplicates_removed}\n\n")
                f.write(f"Matching criteria: Flight Date, Flight No., AWB, Pieces, Weight, ULD Number, Nature Goods, SHCs\n")
                f.write(f"{'='*100}\n\n")
                
                # Group duplicates by the key fields
                for _, group in duplicates_df.groupby(duplicate_check_cols):
                    if len(group) > 1:
                        f.write(f"Duplicate Group (appeared {len(group)} times):\n")
                        f.write(f"  Flight Date: {group.iloc[0]['Flight date']}\n")
                        f.write(f"  Flight No.: {group.iloc[0]['Flight No.']}\n")
                        f.write(f"  AWB: {group.iloc[0]['AWB']}\n")
                        f.write(f"  Pieces: {group.iloc[0]['Pieces']}\n")
                        f.write(f"  Weight: {group.iloc[0]['Weight']}\n")
                        f.write(f"  ULD Number: {group.iloc[0]['ULD Number']}\n")
                        f.write(f"  Nature Goods: {group.iloc[0]['Nature Goods']}\n")
                        f.write(f"  SHCs: {group.iloc[0]['SHCs']}\n")
                        f.write(f"  Import Status: {group.iloc[0]['Import Status']}\n")
                        f.write(f"  AWB Dest: {group.iloc[0]['AWB Dest']}\n")
                        f.write(f"{'-'*100}\n")
            
            print(f"Duplicate details logged to {duplicate_log_filename}")
    
    print(f"\nTotal rows filtered: {total_rows_before - len(df_book1)}")
    print(f"  - Import Status (MIS/ACC/NOT): {status_filtered}")
    print(f"  - Zero weight: {weight_filtered}")
    print(f"  - Duplicates: {duplicates_removed}")
    print(f"Remaining rows to process: {len(df_book1)}\n")
    
    # Group by Flight date, Carrier, Flight No., Origin, and Dest
    grouped = df_book1.groupby(['Flight date', 'Carrier', 'Flight No.', 'Origin', 'Dest'])
    
    # Initialize Book2 structure
    book2_data = []
    
    # Define all category columns (weight columns)
    category_columns = ['GENCARGO', 'PER/COL', 'DG', 'TRANSIT', 'P.O MAIL', 'COURIER']
    
    # Define AWB count columns (P.O MAIL is NOT counted separately)
    awb_columns = ['GEN(awb)', 'COL(awb)', 'DG(awb)', 'TNST(awb)', 'COU(awb)']
    
    for (flight_date, carrier, flight_no, origin, dest), group in grouped:
        # Convert flight_date to date only (remove time component)
        if pd.notna(flight_date):
            if isinstance(flight_date, pd.Timestamp):
                flight_date = flight_date.date()
            elif isinstance(flight_date, datetime):
                flight_date = flight_date.date()
        
        # Initialize row for Book2
        row_data = {
            'DATE': flight_date,
            'AIRLINE': carrier,
            'FLIGHT NO': flight_no,
            'ROUTE': classify_flight_route(origin, dest),
            'R/CATEGORY': classify_flight_category(carrier, flight_no)
        }
        
        # Initialize all weight and AWB count columns to 0
        for col in category_columns:
            row_data[col] = 0
        for col in awb_columns:
            row_data[col] = 0
        
        # Track unique AWBs per category for this flight
        unique_awbs_by_category = {
            'GENCARGO': set(),
            'PER/COL': set(),
            'DG': set(),
            'TRANSIT': set(),
            'COURIER': set(),
            'P.O MAIL': set()  # Tracked for weight but not counted in AWB columns
        }
        
        # Process each AWB in the group
        for _, awb_row in group.iterrows():
            category, weight = classify_cargo(awb_row, unclassified_log, transit_conflict_log)
            
            # Skip if category is None (zero weight AWBs)
            if category is None:
                continue
            
            # Add weight to the appropriate category
            if category in row_data:
                row_data[category] += weight
            
            # Track unique AWB for this category
            awb_number = str(awb_row['AWB']) if pd.notna(awb_row['AWB']) else ''
            if awb_number and category in unique_awbs_by_category:
                unique_awbs_by_category[category].add(awb_number)
        
        # Count unique AWBs per category
        # P.O MAIL is NOT counted in any AWB column as requested
        awb_col_mapping = {
            'GENCARGO': 'GEN(awb)',
            'PER/COL': 'COL(awb)',
            'DG': 'DG(awb)',
            'TRANSIT': 'TNST(awb)',
            'COURIER': 'COU(awb)'
            # P.O MAIL intentionally excluded from counting
        }
        
        for category, awb_col in awb_col_mapping.items():
            row_data[awb_col] = len(unique_awbs_by_category[category])
        
        # Calculate totals - count unique AWBs across all categories (excluding P.O MAIL)
        all_unique_awbs = set()
        for category in ['GENCARGO', 'PER/COL', 'DG', 'TRANSIT', 'COURIER']:
            all_unique_awbs.update(unique_awbs_by_category[category])
        
        row_data['AWB TOTAL'] = len(all_unique_awbs)  # Total unique AWBs for this flight (excluding P.O MAIL)
        row_data['TOTAL WEIGHT'] = sum(row_data[col] for col in category_columns)
        
        book2_data.append(row_data)
    
    # Create DataFrame for Book2
    df_book2 = pd.DataFrame(book2_data)
    
    # Ensure all columns are in the correct order
    column_order = ['DATE', 'AIRLINE', 'FLIGHT NO', 'ROUTE', 'R/CATEGORY'] + \
                   category_columns + awb_columns + ['AWB TOTAL', 'TOTAL WEIGHT']
    
    # Add any missing columns with 0 values
    for col in column_order:
        if col not in df_book2.columns:
            df_book2[col] = 0
    
    df_book2 = df_book2[column_order]
    
    # Verify AWB count (excluding P.O MAIL)
    total_awbs_book2 = df_book2['AWB TOTAL'].sum()
    print(f"\nVerification:")
    print(f"Total rows processed (after all filters): {len(df_book1)}")
    print(f"Total unique AWBs in Book2 (excluding P.O MAIL): {total_awbs_book2}")
    
    # Save Book2
    df_book2.to_excel(output_file, index=False)
    print(f"\nBook2 saved to {output_file}")
    
    # Save transit conflict log
    if transit_conflict_log:
        transit_log_filename = 'transit_conflicts.txt'
        with open(transit_log_filename, 'w') as f:
            f.write(f"Transit Conflict Report\n")
            f.write(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*100}\n\n")
            f.write(f"Total transit conflicts found: {len(transit_conflict_log)}\n")
            f.write(f"These AWBs have only ONE of the two required conditions for TRANSIT classification:\n")
            f.write(f"  1. Import Status contains 'CKD'\n")
            f.write(f"  2. AWB Dest is NOT 'DAR'\n\n")
            f.write(f"{'='*100}\n\n")
            
            for item in transit_conflict_log:
                f.write(f"AWB: {item['AWB']}\n")
                f.write(f"  Import Status: {item['Import Status']} (Has CKD: {item['Has CKD']})\n")
                f.write(f"  AWB Dest: {item['AWB Dest']} (Not DAR: {item['Dest Not DAR']})\n")
                f.write(f"  Conflict Reason: {item['Reason']}\n")
                f.write(f"  Weight: {item['Weight']}\n")
                f.write(f"  Nature Goods: {item['Nature Goods']}\n")
                f.write(f"  SHCs: {item['SHCs']}\n")
                f.write(f"  Timestamp: {item['Timestamp']}\n")
                f.write(f"{'-'*100}\n")
        
        print(f"Transit conflicts logged to {transit_log_filename} ({len(transit_conflict_log)} items)")
    else:
        print("No transit conflicts found.")
    
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
                f.write(f"Import Status: {item['Import Status']}\n")
                f.write(f"AWB Dest: {item['AWB Dest']}\n")
                f.write(f"Weight: {item['Weight']}\n")
                f.write(f"Timestamp: {item['Timestamp']}\n")
                f.write(f"{'-'*30}\n")
        print(f"Unclassified items logged to {log_filename} ({len(unclassified_log)} items)")
    else:
        print("No unclassified items found.")
    
    # Print summary statistics
    print(f"\nSummary Statistics:")
    print(f"Total flights processed: {len(df_book2)}")
    print(f"Total weight processed: {df_book2['TOTAL WEIGHT'].sum():,.2f} kg")
    
    # Category breakdown by weight
    print(f"\nCategory Breakdown (Weight in kg):")
    for col in category_columns:
        weight = df_book2[col].sum()
        if weight > 0:
            print(f"  {col}: {weight:,.2f}")
    
    # Category breakdown by unique AWB count
    print(f"\nCategory Breakdown (Unique AWB Count):")
    for col in awb_columns:
        count = df_book2[col].sum()
        if count > 0:
            print(f"  {col}: {count}")
    
    # Check if there are any P.O MAIL items
    po_mail_weight = df_book2['P.O MAIL'].sum()
    if po_mail_weight > 0:
        print(f"\nNote: P.O MAIL weight: {po_mail_weight:,.2f} kg (not counted in AWB totals)")
    
    return df_book2

if __name__ == "__main__":
    # Run the transformation
    result = transform_data('Book1.xlsx', 'Book2.xlsx')
    if result is not None:
        print("\nTransformation complete!")
        print("Files created:")
        print("  - Book2.xlsx (transformed data)")
        print("  - unclassified_words.txt (log file, if any unclassified items)")
        print("  - duplicate_entries.txt (log file, if any duplicates found)")
        print("  - transit_conflicts.txt (log file, if any transit conflicts found)")
    else:
        print("\nTransformation failed - check error messages above.")