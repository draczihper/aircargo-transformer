import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def is_po_mail(row):
    """
    Check if a row is P.O MAIL based on AWB, Nature Goods, or SHCs.
    P.O MAIL detection is row-level only.
    """
    awb = str(row['AWB']).strip().upper() if pd.notna(row['AWB']) else ''
    nature_goods = str(row['Nature Goods']).upper() if pd.notna(row['Nature Goods']) else ''
    shcs = str(row['SHCs']).upper() if pd.notna(row['SHCs']) else ''
    
    # Check AWB prefix
    if awb.startswith('MAL'):
        return True
    
    # Check Nature Goods (exclude DIPLOMATIC)
    if 'MAIL' in nature_goods and 'DIPLOMATIC' not in nature_goods:
        return True
    
    # Check SHCs for MAL (space-separated)
    shc_list = shcs.split()
    if 'MAL' in shc_list:
        return True
    
    return False

def classify_awb_group(rows, transit_conflicts, unclassified_entries):
    """
    Classify a group of rows (same AWB on same flight) into ONE category.
    Returns category and total weight for this AWB group.
    
    Priority Order:
    1. TRANSIT
    2. COURIER
    3. PER/COL
    4. DG
    5. GENCARGO (default)
    """
    # Use first row for classification (all rows in group should have same AWB)
    if len(rows) == 0:
        return None, 0
    
    row = rows.iloc[0]
    total_weight = rows['Weight'].sum()  # Sum weight across all rows in group
    
    nature_goods = str(row['Nature Goods']).lower() if pd.notna(row['Nature Goods']) else ''
    shcs = str(row['SHCs']).upper() if pd.notna(row['SHCs']) else ''
    awb = str(row['AWB']) if pd.notna(row['AWB']) else ''
    import_status = str(row['Import Status']).upper() if pd.notna(row['Import Status']) else ''
    awb_dest = str(row['AWB Dest']).upper().strip() if pd.notna(row['AWB Dest']) else ''
    
    # PRIORITY 1: TRANSIT
    has_ckd = 'CKD' in import_status
    dest_not_dar = awb_dest != 'DAR' and awb_dest != ''
    
    if has_ckd and dest_not_dar:
        return 'TRANSIT', total_weight
    elif has_ckd or dest_not_dar:
        # Log transit conflict
        transit_conflicts.append({
            'AWB': awb,
            'Import Status': row['Import Status'],
            'AWB Dest': row['AWB Dest'],
            'Has CKD': has_ckd,
            'Dest Not DAR': dest_not_dar,
            'Weight': total_weight,
            'Nature Goods': row['Nature Goods'],
            'SHCs': row['SHCs'],
            'Reason': 'CKD without non-DAR destination' if has_ckd else 'Non-DAR destination without CKD',
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # PRIORITY 2: COURIER
    if 'COU' in shcs or 'courier' in nature_goods:
        return 'COURIER', total_weight
    
    # PRIORITY 3: PER/COL
    perishable_shcs = ['COL', 'FRO', 'CRT', 'ICE', 'ERT', 'PER', 'PEF', 'PES', 'PEM']
    if any(term in shcs for term in perishable_shcs):
        return 'PER/COL', total_weight
    
    perishable_terms = ['perishable', 'fresh', 'chilled', 'frozen', 'cool', 'cold', 
                       'flower', 'fish', 'meat', 'vegetable', 'fruit', 'avocado']
    if any(term in nature_goods for term in perishable_terms):
        return 'PER/COL', total_weight
    
    # PRIORITY 4: DG
    dg_shcs = ['DGR', 'RRY', 'RMD', 'RPB', 'RFL', 'RCG', 'RNG', 'RIS', 'RDS']
    if any(term in shcs for term in dg_shcs):
        return 'DG', total_weight
    
    if 'dangerous' in nature_goods:
        return 'DG', total_weight
    
    # PRIORITY 5: GENCARGO (Default)
    if any(term in shcs for term in ['GEN', 'GCR']):
        return 'GENCARGO', total_weight
    
    # Log unclassified if not generic
    if nature_goods and nature_goods not in ['general cargo', 'cargo', 'general', 'gen', '']:
        unclassified_entries.append({
            'AWB': awb,
            'Nature Goods': row['Nature Goods'],
            'SHCs': row['SHCs'],
            'Import Status': row['Import Status'],
            'AWB Dest': row['AWB Dest'],
            'Weight': total_weight,
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return 'GENCARGO', total_weight

def classify_flight_category(carrier, flight_no):
    """Classify flight route category."""
    carrier = str(carrier).upper() if pd.notna(carrier) else ''
    flight_no = str(flight_no).upper() if pd.notna(flight_no) else ''
    
    if carrier == 'PW':
        return 'DOMESTIC'
    
    if carrier == 'TC':
        if flight_no.startswith('TC1'):
            return 'DOMESTIC'
        elif any(flight_no.startswith(prefix) for prefix in ['TC2', 'TC4', 'TC5', 'TC05']):
            return 'TC-FOREIGN'
        else:
            return 'FOREIGN'
    
    return 'FOREIGN'

def classify_flight_route(origin, dest):
    """Create route string."""
    origin = str(origin).upper() if pd.notna(origin) else ''
    dest = str(dest).upper() if pd.notna(dest) else ''
    return f"{origin}-{dest}"

def transform_data(input_file='Book1.xlsx', output_file='Book2.xlsx'):
    """Main transformation function."""
    print(f"Reading {input_file}...")
    
    # Read Excel file
    df = pd.read_excel(input_file, header=0)
    df.columns = df.columns.str.strip()
    
    total_rows_initial = len(df)
    total_weight_initial = df['Weight'].sum()
    
    print(f"Total rows read: {total_rows_initial}")
    print(f"Initial total weight: {total_weight_initial:,.2f} kg\n")
    
    # Column mapping
    column_mapping = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'flight' in col_lower and 'date' in col_lower:
            column_mapping['Flight date'] = col
        elif col_lower in ['carrier', 'airlines', 'airline']:
            column_mapping['Carrier'] = col
        elif 'flight' in col_lower and ('no' in col_lower or 'number' in col_lower):
            column_mapping['Flight No.'] = col
        elif 'origin' in col_lower and 'awb' not in col_lower:
            column_mapping['Origin'] = col
        elif 'awb' in col_lower and 'dest' in col_lower:
            column_mapping['AWB Dest'] = col
        elif col_lower in ['dest', 'destination']:
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
    
    df = df.rename(columns={v: k for k, v in column_mapping.items()})
    
    # Check required columns
    required_cols = ['Flight date', 'Carrier', 'Flight No.', 'Origin', 'Dest', 
                     'AWB', 'Pieces', 'Weight', 'ULD Number', 'Import Status', 
                     'AWB Dest', 'Nature Goods', 'SHCs']
    missing = [col for col in required_cols if col not in df.columns]
    
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        return None
    
    # === FILTERING PHASE ===
    
    # Filter 1: Import Status
    excluded_statuses = ['MIS', 'ACC', 'NOT']
    df['Import Status Clean'] = df['Import Status'].astype(str).str.upper().str.strip()
    before = len(df)
    df = df[~df['Import Status Clean'].isin(excluded_statuses)].copy()
    status_removed = before - len(df)
    print(f"Rows removed (status filter): {status_removed}")
    
    # Filter 2: Zero weight
    before = len(df)
    df = df[df['Weight'] != 0].copy()
    zero_removed = before - len(df)
    print(f"Rows removed (zero weight): {zero_removed}")
    
    # Filter 3: Empty AWB
    df['AWB'] = df['AWB'].astype(str).str.strip()
    before = len(df)
    df = df[df['AWB'] != ''].copy()
    df = df[df['AWB'] != 'nan'].copy()
    empty_awb_removed = before - len(df)
    print(f"Rows removed (empty AWB): {empty_awb_removed}")
    
    # Filter 4: HWB AWBs
    before = len(df)
    df = df[~df['AWB'].str.upper().str.startswith('HWB')].copy()
    hwb_removed = before - len(df)
    print(f"Rows removed (HWB AWBs): {hwb_removed}")
    
    # Filter 5: Duplicates
    df['ULD Number'] = df['ULD Number'].astype(str).str.strip()
    df['Nature Goods Clean'] = df['Nature Goods'].astype(str).str.strip().str.lower()
    df['SHCs Clean'] = df['SHCs'].astype(str).str.strip().str.upper()
    
    dup_cols = ['Flight date', 'Carrier', 'Flight No.', 'Pieces', 'Weight', 
                'ULD Number', 'Nature Goods Clean', 'SHCs Clean']
    before = len(df)
    df = df.drop_duplicates(subset=dup_cols, keep='first')
    dup_removed = before - len(df)
    print(f"Rows removed (duplicates): {dup_removed}")
    
    # === P.O MAIL DETECTION (ROW-LEVEL) ===
    df['Is_PO_Mail'] = df.apply(is_po_mail, axis=1)
    po_mail_rows = df[df['Is_PO_Mail']].copy()
    po_mail_weight_total = po_mail_rows['Weight'].sum()
    
    print(f"\nDetected P.O MAIL total weight (row-level): {po_mail_weight_total:,.2f} kg")
    print(f"P.O MAIL rows: {len(po_mail_rows)}")
    
    # Separate P.O MAIL from regular cargo
    df_cargo = df[~df['Is_PO_Mail']].copy()
    
    # === PROCESS REGULAR CARGO ===
    transit_conflicts = []
    unclassified_entries = []
    
    # Group by flight
    flight_groups = df_cargo.groupby(['Flight date', 'Carrier', 'Flight No.', 'Origin', 'Dest'])
    
    book2_data = []
    category_columns = ['GENCARGO', 'PER/COL', 'DG', 'TRANSIT', 'P.O MAIL', 'COURIER']
    awb_columns = ['GEN(awb)', 'COL(awb)', 'DG(awb)', 'TNST(awb)', 'COU(awb)']
    
    for flight_key, flight_data in flight_groups:
        flight_date, carrier, flight_no, origin, dest = flight_key
        
        # Convert date
        if pd.notna(flight_date):
            if isinstance(flight_date, pd.Timestamp):
                flight_date = flight_date.date()
            elif isinstance(flight_date, datetime):
                flight_date = flight_date.date()
        
        row_data = {
            'DATE': flight_date,
            'AIRLINE': carrier,
            'FLIGHT NO': flight_no,
            'ROUTE': classify_flight_route(origin, dest),
            'R/CATEGORY': classify_flight_category(carrier, flight_no)
        }
        
        # Initialize weights and counts
        for col in category_columns:
            row_data[col] = 0
        for col in awb_columns:
            row_data[col] = 0
        
        # Track unique AWBs per category
        unique_awbs = {
            'GENCARGO': set(),
            'PER/COL': set(),
            'DG': set(),
            'TRANSIT': set(),
            'COURIER': set()
        }
        
        # Group by AWB within this flight
        for awb, awb_rows in flight_data.groupby('AWB'):
            category, weight = classify_awb_group(awb_rows, transit_conflicts, unclassified_entries)
            
            if category and category in row_data:
                row_data[category] += weight
                if category in unique_awbs:
                    unique_awbs[category].add(awb)
        
        # Add P.O MAIL weight for this flight
        po_mail_flight = po_mail_rows[
            (po_mail_rows['Flight date'] == flight_key[0]) &
            (po_mail_rows['Carrier'] == carrier) &
            (po_mail_rows['Flight No.'] == flight_no) &
            (po_mail_rows['Origin'] == origin) &
            (po_mail_rows['Dest'] == dest)
        ]
        row_data['P.O MAIL'] = po_mail_flight['Weight'].sum()
        
        # Count unique AWBs per category
        awb_mapping = {
            'GENCARGO': 'GEN(awb)',
            'PER/COL': 'COL(awb)',
            'DG': 'DG(awb)',
            'TRANSIT': 'TNST(awb)',
            'COURIER': 'COU(awb)'
        }
        
        for cat, col in awb_mapping.items():
            row_data[col] = len(unique_awbs[cat])
        
        # Calculate totals
        all_unique = set()
        for awbs in unique_awbs.values():
            all_unique.update(awbs)
        
        row_data['AWB TOTAL'] = len(all_unique)
        row_data['TOTAL WEIGHT'] = sum(row_data[col] for col in category_columns)
        
        book2_data.append(row_data)
    
    # Create Book2 DataFrame
    df_book2 = pd.DataFrame(book2_data)
    
    column_order = ['DATE', 'AIRLINE', 'FLIGHT NO', 'ROUTE', 'R/CATEGORY'] + \
                   category_columns + awb_columns + ['AWB TOTAL', 'TOTAL WEIGHT']
    
    for col in column_order:
        if col not in df_book2.columns:
            df_book2[col] = 0
    
    df_book2 = df_book2[column_order]
    
    # Save output
    df_book2.to_excel(output_file, index=False)
    
    # === CONSOLE OUTPUT ===
    print(f"\nFlights processed: {len(df_book2)}")
    print(f"Total unique AWBs (excluding mail): {df_book2['AWB TOTAL'].sum()}")
    print(f"Grand total weight: {df_book2['TOTAL WEIGHT'].sum():,.2f} kg")
    print(f"P.O MAIL total weight: {df_book2['P.O MAIL'].sum():,.2f} kg")
    print(f"\nOutput saved to {output_file}")
    
    # Category breakdown
    print(f"\nCategory Breakdown (Weight in kg):")
    for col in category_columns:
        weight = df_book2[col].sum()
        if weight > 0:
            print(f"  {col}: {weight:,.2f}")
    
    print(f"\nCategory Breakdown (Unique AWB Count):")
    for col in awb_columns:
        count = df_book2[col].sum()
        if count > 0:
            print(f"  {col}: {count}")
    
    # Save logs
    if transit_conflicts:
        with open('transit_conflicts.txt', 'w') as f:
            f.write(f"Transit Conflicts: {len(transit_conflicts)}\n\n")
            for item in transit_conflicts:
                f.write(f"AWB: {item['AWB']}\n")
                f.write(f"  Reason: {item['Reason']}\n")
                f.write(f"  Import Status: {item['Import Status']}\n")
                f.write(f"  AWB Dest: {item['AWB Dest']}\n")
                f.write(f"  Weight: {item['Weight']}\n\n")
        print(f"\nTransit conflicts logged: {len(transit_conflicts)}")
    
    if unclassified_entries:
        with open('unclassified_words.txt', 'w') as f:
            f.write(f"Unclassified Entries: {len(unclassified_entries)}\n\n")
            for item in unclassified_entries:
                f.write(f"AWB: {item['AWB']}\n")
                f.write(f"  Nature Goods: {item['Nature Goods']}\n")
                f.write(f"  SHCs: {item['SHCs']}\n")
                f.write(f"  Weight: {item['Weight']}\n\n")
        print(f"Unclassified entries logged: {len(unclassified_entries)}")
    
    return df_book2

if __name__ == "__main__":
    result = transform_data('Book1.xlsx', 'Book2.xlsx')
    if result is not None:
        print("\n✓ Transformation complete!")
    else:
        print("\n✗ Transformation failed.")