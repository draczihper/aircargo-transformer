import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def classify_cargo(row, unclassified_log):
    """
    Classify cargo based on Nature Goods and SHCs with comprehensive rules.
    Returns the category and weight for that category.
    """
    nature_goods = str(row['Nature Goods']).lower() if pd.notna(row['Nature Goods']) else ''
    shcs = str(row['SHCs']).upper() if pd.notna(row['SHCs']) else ''
    weight = float(row['Weight']) if pd.notna(row['Weight']) else 0
    awb = str(row['AWB']) if pd.notna(row['AWB']) else ''
    awb_dest = str(row['AWB Dest']).lower() if pd.notna(row['AWB Dest']) else ''
    import_status = str(row['Import Status']).lower() if pd.notna(row['Import Status']) else ''
    
    # Check AWB prefix for P.O.MAIL
    if awb.startswith('MAL'):
        return 'P.O.MAIL', weight
    
    # Priority 1: Specific items in Import Status and AWB Dest
    if "CKD" in import_status and awb_dest != 'DAR':
        return 'TRANSIT', weight

    if any(term in shcs for term in ['COL', 'FRO', 'CRT', 'ICE', 'ERT', 'PER']):
        return 'PER/COL', weight
     
    if any(term in shcs for term in ['DGR','RRY', 'RMD', 'RPB', 'RFL', 'RCG', 'RNG', 'RIS', 'RDS']) or 'dangerous' in nature_goods:
        return 'DG', weight
    
    if 'courier' in nature_goods or 'COU' in shcs:
        return 'COURIER', weight
    
    
    if any(term in shcs for term in ['GEN', 'GCR']):
        return 'G. CARGO', weight
    
    # Priority 2: Generic perishables
    """ perishable_terms = ['perishable', 'fresh', 'chilled', 'frozen', 'cool', 'cold']
    if any(term in nature_goods for term in perishable_terms) or 'COL' in shcs or 'PER' in shcs or 'FRO' in shcs or 'ICE' in shcs:
        return 'PER/COL', weight """
    
    # If we can't classify with confidence, log it
    if nature_goods and nature_goods not in ['general cargo', 'cargo', 'general', '']:
        unclassified_log.append({
            'AWB': awb,
            'Nature Goods': row['Nature Goods'],
            'SHCs': row['SHCs'],
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Default to general cargo
    return 'G. CARGO', weight

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
        if flight_no in ['717', '721']:
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
    
    # Read Book1 with header at row 2 (index 1)
    df_book1 = pd.read_excel(input_file, header=0)
    
    # Clean column names - remove extra spaces
    df_book1.columns = df_book1.columns.str.strip()
    
    print(f"Total rows in Book1: {len(df_book1)}")
    print(f"\nColumns found in Book1:")
    for i, col in enumerate(df_book1.columns):
        print(f"  {i}: '{col}'")
    print()
    
    # Try to identify the correct column names (case-insensitive matching)
    column_mapping = {}
    for col in df_book1.columns:
        col_lower = col.lower()
        if 'flight' in col_lower and 'date' in col_lower:
            column_mapping['Flight date'] = col
        elif col_lower == 'carrier' or col_lower == 'airlines' or col_lower == 'airline':
            column_mapping['Carrier'] = col
        elif 'flight' in col_lower and ('no' in col_lower or 'number' in col_lower):
            column_mapping['Flight No.'] = col
        elif col_lower == 'awb' or 'awb' in col_lower:
            column_mapping['AWB'] = col
        elif 'nature' in col_lower and 'goods' in col_lower:
            column_mapping['Nature Goods'] = col
        elif 'rcv' in col_lower or 'weight' in col_lower:
            column_mapping['Weight'] = col
        elif 'shc' in col_lower:
            column_mapping['SHCs'] = col
    
    # Rename columns to expected names
    df_book1 = df_book1.rename(columns={v: k for k, v in column_mapping.items()})
    
    # Check if we have all required columns
    required_columns = ['Flight date', 'Carrier', 'Flight No.', 'AWB', 'Weight', 'Nature Goods', 'SHCs']
    missing_columns = [col for col in required_columns if col not in df_book1.columns]
    
    if missing_columns:
        print(f"Warning: Missing columns: {missing_columns}")
        print("Attempting to proceed with available columns...")
    
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