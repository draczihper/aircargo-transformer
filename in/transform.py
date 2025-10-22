"""
transform.py

Full Air Cargo Import Data Transformation script.
Run: python transform.py
Input:  Book1.xlsx (must be in same directory)
Output: Book2.xlsx, duplicate_entries.txt, transit_conflicts.txt, unclassified_words.txt
"""

import sys
import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ----------------- Configuration / Keyword Sets -----------------
GENERIC_TERMS = set(["general cargo","cargo","general","gen"])

PER_COL_SHCS = set(['COL','FRO','CRT','ICE','ERT','PER','PEF','PES','PEM'])
PER_COL_KEYWORDS = set(['perishable','fresh','chilled','frozen','cool','cold','flower','fish','meat','vegetable','fruit','avocado'])

DG_SHCS = set(['DGR','RRY','RMD','RPB','RFL','RCG','RNG','RIS','RDS'])

COURIER_SHCS = set(['COU'])
COURIER_KEYWORDS = set(['courier'])

GEN_SHCS = set(['GEN','GCR'])

# ----------------- Helpers -----------------
def shc_tokens(shc_field):
    """Normalize SHCs string into a set of uppercase tokens."""
    if pd.isna(shc_field) or str(shc_field).strip() == '':
        return set()
    # split by whitespace or common separators
    parts = re.split(r'[\s,;|/]+', str(shc_field).upper())
    parts = [p.strip() for p in parts if p.strip()!='']
    return set(parts)

# ----------------- Column mapping -----------------
def map_columns(df):
    """Map a variety of possible input column names to the canonical names used by the script."""
    cols = {c.lower().strip(): c for c in df.columns}
    def find(preferred_names):
        for name in preferred_names:
            key = name.lower()
            if key in cols:
                return cols[key]
        return None

    mapping = {
        'flight_date': find(['Flight Date','flight date','flight_date','date']),
        'carrier': find(['Carrier','carrier','airline']),
        'flight_no': find(['Flight No','Flight No.','flight no','flight_no','flight']),
        'origin': find(['Origin','origin','org']),
        'dest': find(['Dest','Destination','dest','destination']),
        'awb': find(['AWB','Awb','awb_number','awb']),
        'pieces': find(['Pieces','pieces','pcs']),
        'weight': find(['Weight','weight','kg']),
        'uld': find(['ULD Number','ULD','uld number','uld_number','uld_no']),
        'import_status': find(['Import Status','Import_Status','import status','status']),
        'awb_dest': find(['AWB Dest','AWB Destination','awb dest','awb_dest','final destination']),
        'nature_goods': find(['Nature Goods','nature goods','nature_goods','description']),
        'shcs': find(['SHCs','SHC','shcs','shc','special handling codes'])
    }

    missing = [k for k,v in mapping.items() if v is None]
    if missing:
        # If some mapping keys missing, raise informative error
        raise ValueError(f"Missing required columns (couldn't map): {missing}. Available columns: {list(df.columns)}")

    # rename the DataFrame columns to canonical lowercase names used later
    df = df.rename(columns={v:k for k,v in mapping.items() if v is not None})
    return df

# ----------------- Flight route/category -----------------
def classify_flight_route(origin, dest):
    if pd.isna(origin): origin = ''
    if pd.isna(dest): dest = ''
    return f"{str(origin).strip().upper()}-{str(dest).strip().upper()}"

def classify_flight_category(carrier, flight_no):
    c = '' if pd.isna(carrier) else str(carrier).strip().upper()
    fn = '' if pd.isna(flight_no) else str(flight_no).strip().upper()
    if c == 'PW':
        return 'DOMESTIC'
    if c == 'TC':
        if fn.startswith('TC1'):
            return 'DOMESTIC'
        if fn.startswith('TC2') or fn.startswith('TC4') or fn.startswith('TC5'):
            return 'TC-FOREIGN'
        return 'FOREIGN'
    return 'FOREIGN'

# ----------------- AWB classification -----------------
def classify_awb_group(rows, transit_conflicts, unclassified_entries):
    """
    Classify an AWB (group of rows) into exactly one category and return (category, weight_sum).
    Follows priority:
    1) TRANSIT (CKD in Import Status AND AWB Dest != 'DAR')
    2) P.O MAIL (AWB starts with MAL)
    3) COURIER (SHC contains COU or Nature Goods contains 'courier')
    4) PER/COL (SHC in per-col list or nature goods keywords)
    5) DG (dangerous goods SHC or keyword)
    6) GENCARGO (default)
    Also logs transit conflicts and unclassified entries.
    """
    awb_value = None
    try:
        # rows.name might be AWB when grouped
        awb_value = rows.name
    except Exception:
        awb_value = None

    has_ckd = False
    dests = set()
    any_shcs = set()
    any_nature = []
    weight_sum = rows['weight'].fillna(0).astype(float).sum()

    for _, r in rows.iterrows():
        imp = '' if pd.isna(r.get('import_status','')) else str(r.get('import_status','')).upper()
        if 'CKD' in imp:
            has_ckd = True
        awb_dest = '' if pd.isna(r.get('awb_dest','')) else str(r.get('awb_dest','')).strip().upper()
        dests.add(awb_dest)
        any_shcs |= shc_tokens(r.get('shcs',''))
        ng = '' if pd.isna(r.get('nature_goods','')) else str(r.get('nature_goods',''))
        any_nature.append(ng)

    # Transit logic
    dest_not_dar = any([d!='DAR' and d!='' for d in dests])
    if has_ckd and dest_not_dar:
        return 'TRANSIT', weight_sum

    # If only one of the transit conditions true -> log conflict
    if has_ckd ^ dest_not_dar:
        r0 = rows.iloc[0]
        transit_conflicts.append({
            'AWB': awb_value,
            'Has_CKD': bool(has_ckd),
            'Dest_Not_DAR': bool(dest_not_dar),
            'Import_Status': r0.get('import_status',''),
            'AWB_Dests': list(dests),
            'Weight': weight_sum,
            'Nature_Goods': '; '.join([n for n in any_nature if str(n).strip()!='']),
            'SHCs': ' '.join(sorted(list(any_shcs)))
        })
        # Continue classification normally (transit not assigned)

    # P.O MAIL check (AWB starts with MAL)
    if isinstance(awb_value, str) and awb_value.strip().upper().startswith('MAL') or "MAIL" in any_nature or "MAL" in any_shcs:
        return 'P.O MAIL', weight_sum

    # COURIER
    if (len(any_shcs & COURIER_SHCS) > 0) or any([('courier' in str(n).lower()) for n in any_nature]):
        return 'COURIER', weight_sum

    # PER/COL
    if (len(any_shcs & PER_COL_SHCS) > 0) or any([any(k in str(n).lower() for k in PER_COL_KEYWORDS) for n in any_nature]):
        return 'PER/COL', weight_sum

    # DG
    if (len(any_shcs & DG_SHCS) > 0) or any([('dangerous' in str(n).lower()) for n in any_nature]):
        return 'DG', weight_sum

    # GENCARGO default
    nature_combined = ' '.join([str(n) for n in any_nature if str(n).strip()!='']).strip().lower()
    # If both SHCs empty and nature goods generic/empty, log as unclassified
    if (len(any_shcs) == 0) and (nature_combined == '' or any(term in nature_combined for term in GENERIC_TERMS)):
        r0 = rows.iloc[0]
        unclassified_entries.append({
            'AWB': awb_value,
            'Nature_Goods': r0.get('nature_goods',''),
            'SHCs': ' '.join(sorted(list(any_shcs))),
            'Import_Status': r0.get('import_status',''),
            'AWB_Dest': ';'.join(list(dests)),
            'Weight': weight_sum
        })

    return 'GENCARGO', weight_sum

# ----------------- Main transform function -----------------
def transform_data(input_file, output_file='Book2.xlsx'):
    # Read
    try:
        df = pd.read_excel(input_file, dtype=str)
    except Exception as e:
        print(f"ERROR reading '{input_file}': {e}")
        return None

    total_rows = len(df)
    print(f"Total rows read: {total_rows}")

    # Map columns
    try:
        df = map_columns(df)
    except Exception as e:
        print(f"ERROR: {e}")
        return None

    # Normalize / strip whitespace for certain fields
    for c in ['awb','uld','import_status','awb_dest','nature_goods','shcs','carrier','origin','dest','flight_no']:
        if c in df.columns:
            df[c] = df[c].astype(str).replace('nan','').fillna('').apply(lambda x: x.strip())

    # parse numeric fields
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0).astype(float)
    df['pieces'] = pd.to_numeric(df['pieces'], errors='coerce').fillna(0).astype(int)

    # Flight date -> date-only
    df['flight_date'] = pd.to_datetime(df['flight_date'], errors='coerce')
    df['flight_date_only'] = df['flight_date'].dt.date

    # Filter out excluded import statuses BEFORE processing
    status_exclude = set(['MIS','ACC','NOT'])
    before_status = len(df)
    df['import_status_clean'] = df['import_status'].str.upper().fillna('')
    df = df[~df['import_status_clean'].isin(status_exclude)].copy()
    after_status = len(df)
    print(f"Rows removed by status filter: {before_status - after_status}")

    # Filter out zero weight rows
    before_w = len(df)
    df = df[df['weight'] != 0].copy()
    after_w = len(df)
    print(f"Rows removed by zero weight: {before_w - after_w}")

    # Remove strict duplicates (keep first)
    dup_subset = ['flight_date_only','flight_no','awb','pieces','weight','uld','nature_goods','shcs']
    for col in dup_subset:
        if col not in df.columns:
            df[col] = ''
    duplicated_mask = df.duplicated(subset=dup_subset, keep='first')
    duplicates = df[duplicated_mask].copy()
    df = df[~duplicated_mask].copy()
    print(f"Duplicate rows found (strict): {len(duplicates)}")

    # Prepare duplicate log entries
    dup_log_entries = []
    for _, r in duplicates.iterrows():
        dup_log_entries.append({
            'Flight Date': str(r['flight_date_only']),
            'Flight No': r.get('flight_no',''),
            'AWB': r.get('awb',''),
            'Pieces': r.get('pieces',''),
            'Weight': r.get('weight',''),
            'ULD': r.get('uld',''),
            'Nature Goods': r.get('nature_goods',''),
            'SHCs': r.get('shcs',''),
            'Import Status': r.get('import_status',''),
            'AWB Dest': r.get('awb_dest','')
        })

    # Prepare for grouping
    group_cols = ['flight_date_only','carrier','flight_no','origin','dest']
    for c in group_cols:
        if c not in df.columns:
            df[c] = ''

    flights = []
    transit_conflicts = []
    unclassified_entries = []

    flights_processed = 0
    total_weight_sum = 0.0
    unique_awbs_global = set()

    # Group by flight
    gb = df.groupby(group_cols, dropna=False)
    for flight_id, sub in gb:
        flights_processed += 1
        flight_date_only, carrier, flight_no, origin, dest = flight_id
        route = classify_flight_route(origin, dest)
        r_category = classify_flight_category(carrier, flight_no)

        # counters for this flight
        weights = {'GENCARGO':0.0,'PER/COL':0.0,'DG':0.0,'TRANSIT':0.0,'P.O MAIL':0.0,'COURIER':0.0}
        awb_sets = {'GENCARGO':set(),'PER/COL':set(),'DG':set(),'TRANSIT':set(),'COURIER':set()}

        # Group by AWB within this flight
        sub_awb_groups = sub.groupby('awb', dropna=False)
        for awb, awb_rows in sub_awb_groups:
            cat, w = classify_awb_group(awb_rows, transit_conflicts, unclassified_entries)
            if pd.isna(w):
                w = 0.0
            # --- IMPORTANT: Always add weight to category (including P.O MAIL) ---
            weights[cat] = weights.get(cat, 0.0) + float(w)

            # --- AWB counting: exclude P.O MAIL AWBs from any AWB sets/counts ---
            if cat != 'P.O MAIL':
                if awb is not None and str(awb).strip() != '':
                    awb_sets[cat].add(str(awb).strip())
                    unique_awbs_global.add(str(awb).strip())

            # track total weight across all categories for summary
            total_weight_sum += float(w)

        # compute AWB counts for this flight
        awb_counts = {k: len(v) for k,v in awb_sets.items()}
        awb_total = sum(awb_counts.values())  # excludes P.O MAIL by design

        total_weight = sum(weights.values())

        flights.append({
            'DATE': flight_date_only,
            'AIRLINE': carrier,
            'FLIGHT NO': flight_no,
            'ROUTE': route,
            'R/CATEGORY': r_category,
            'GENCARGO': weights['GENCARGO'],
            'PER/COL': weights['PER/COL'],
            'DG': weights['DG'],
            'TRANSIT': weights['TRANSIT'],
            'P.O MAIL': weights['P.O MAIL'],
            'COURIER': weights['COURIER'],
            'GEN(awb)': awb_counts['GENCARGO'],
            'COL(awb)': awb_counts['PER/COL'],
            'DG(awb)': awb_counts['DG'],
            'TNST(awb)': awb_counts['TRANSIT'],
            'COU(awb)': awb_counts['COURIER'],
            'AWB TOTAL': awb_total,
            'TOTAL WEIGHT': total_weight
        })

    # assemble Book2 dataframe
    book2 = pd.DataFrame(flights)
    col_order = ['DATE','AIRLINE','FLIGHT NO','ROUTE','R/CATEGORY',
                 'GENCARGO','PER/COL','DG','TRANSIT','P.O MAIL','COURIER',
                 'GEN(awb)','COL(awb)','DG(awb)','TNST(awb)','COU(awb)','AWB TOTAL','TOTAL WEIGHT']
    for c in col_order:
        if c not in book2.columns:
            book2[c] = 0
    book2 = book2[col_order]

    # Write Book2.xlsx
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            book2.to_excel(writer, index=False, sheet_name='Flights')
    except Exception as e:
        print(f"ERROR writing '{output_file}': {e}")
        return None

    # Write duplicate_entries.txt
    with open('duplicate_entries.txt','w',encoding='utf-8') as f:
        if len(dup_log_entries) == 0:
            f.write('No strict duplicates found.\n')
        else:
            for d in dup_log_entries:
                line = '\t'.join([str(d.get(k,'')) for k in ['Flight Date','Flight No','AWB','Pieces','Weight','ULD','Nature Goods','SHCs','Import Status','AWB Dest']])
                f.write(line + '\n')

    # Write transit_conflicts.txt
    with open('transit_conflicts.txt','w',encoding='utf-8') as f:
        if len(transit_conflicts) == 0:
            f.write('No transit conflicts found.\n')
        else:
            f.write('AWB\tHas_CKD\tDest_Not_DAR\tImport_Status\tAWB_Dests\tWeight\tNature_Goods\tSHCs\n')
            for d in transit_conflicts:
                f.write(f"{d['AWB']}\t{d['Has_CKD']}\t{d['Dest_Not_DAR']}\t{d['Import_Status']}\t{','.join(d['AWB_Dests'])}\t{d['Weight']}\t{d['Nature_Goods']}\t{d['SHCs']}\n")

    # Write unclassified_words.txt
    with open('unclassified_words.txt','w',encoding='utf-8') as f:
        if len(unclassified_entries) == 0:
            f.write('No unclassified AWBs found.\n')
        else:
            f.write('AWB\tNature_Goods\tSHCs\tImport_Status\tAWB_Dest\tWeight\n')
            for d in unclassified_entries:
                f.write(f"{d['AWB']}\t{d['Nature_Goods']}\t{d['SHCs']}\t{d['Import_Status']}\t{d['AWB_Dest']}\t{d['Weight']}\n")

    # Console summary & verification
    print(f"Rows remaining to process (after filtering & dedup): {len(df)}")
    print(f"Flights processed: {flights_processed}")
    print(f"Total unique AWBs (excluding P.O MAIL): {len([a for a in unique_awbs_global if not str(a).upper().startswith('MAL')])}")
    print(f"Total weight across all categories: {total_weight_sum:.2f} kg")

    if not book2.empty:
        wcols = ['GENCARGO','PER/COL','DG','TRANSIT','P.O MAIL','COURIER']
        print('\nWeight by category (kg):')
        for c in wcols:
            print(f"  {c}: {book2[c].sum():.2f}")
        print('\nAWB counts by category:')
        count_cols = ['GEN(awb)','COL(awb)','DG(awb)','TNST(awb)','COU(awb)']
        for c in count_cols:
            print(f"  {c}: {book2[c].sum():.0f}")
        if book2['P.O MAIL'].sum() > 0:
            print(f"\nP.O MAIL weight (excluded from AWB totals): {book2['P.O MAIL'].sum():.2f} kg")

    print(f"Outputs written: {output_file}, duplicate_entries.txt, transit_conflicts.txt, unclassified_words.txt")
    return book2

# ----------------- Entry point -----------------
if __name__ == '__main__':
    input_file = 'Book1.xlsx'
    output_file = 'Book2.xlsx'
    print(f"Running transformation on {input_file} ...")
    transform_data(input_file, output_file)
