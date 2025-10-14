import pandas as pd
import numpy as np

# =========================
#  1️⃣ INITIAL SETUP
# =========================

input_file = "Book1.xlsx"
output_file = "Book2.xlsx"
transit_conflict_file = "transit_conflict.txt"

# Clear previous transit conflict log
open(transit_conflict_file, "w").close()

# Read Excel file
df = pd.read_excel(input_file)
df.columns = df.columns.str.strip()  # Clean up header whitespace

# Filter out zero weight rows
df = df[df['Weight'] > 0]

# Build ROUTE column
df['ROUTE'] = df['Origin'].str.upper() + "-" + df['Dest'].str.upper()

# Ensure SHCs uppercase and string
df['SHCs'] = df['SHCs'].astype(str).str.upper()

# =========================
#  2️⃣ CLASSIFICATION LOGIC
# =========================

def categorize_route(flight_no):
    """Categorize route based on flight number"""
    flight_no = str(flight_no).upper()
    if flight_no.startswith('TC1') or flight_no.startswith('PW'):
        return 'DOMESTIC'
    elif flight_no.startswith('TC2') or flight_no.startswith('TC4') or flight_no.startswith('TC5'):
        return 'TC-FOREIGN'

    return 'FOREIGN'

def classify(row):
    shc = row['SHCs']
    awb = str(row['AWB']).upper()
    import_status = str(row['Import Status']).upper()
    awb_dest = str(row['AWB Dest']).upper()

    # COURIER
    if 'COU' in shc:
        return 'COURIER'

    # P.O MAIL
    if awb.startswith('MAL'):
        return 'P.O MAIL'

    # DG
    dg_codes = ['DGR', 'RRY', 'RMD', 'RPB', 'RFL', 'RCG', 'RNG', 'RIS', 'RDS', 'RCL']
    if any(code in shc for code in dg_codes):
        return 'DG'

    # PER/COL
    per_codes = ['COL', 'FRO', 'CRT', 'ICE', 'ERT', 'PER']
    if any(code in shc for code in per_codes):
        return 'PER/COL'

    # TRANSIT strict check
    if import_status == 'CKD' and awb_dest != 'DAR':
        return 'TRANSIT'
    elif import_status == 'CKD' or awb_dest != 'DAR':
        # Log conflicting potential transit records
        with open(transit_conflict_file, "a", encoding="utf-8") as f:
            f.write(f"{row['AWB']}, Import Status: {import_status}, AWB Dest: {awb_dest}\n")

    # Default: General Cargo
    return 'GENCARGO'

df['CATEGORY'] = df.apply(classify, axis=1)

# =========================
#  3️⃣ SETUP COLUMNS
# =========================

weight_cols = [
    'GENCARGO',
    'PER/COL',
    'DG',
    'TRANSIT',
    'P.O MAIL',
    'COURIER'
]

awb_cols = [
    'GEN(awb)',
    'COL(awb)',
    'DG(awb)',
    'TRANSIT(awb)',
    'COU(awb)'
]

# Initialize columns with proper dtypes
for col in weight_cols:
    df[col] = 0.0

for col in awb_cols:
    df[col] = 0

# =========================
#  4️⃣ ASSIGN WEIGHT & AWB COUNTS
# =========================

for i, row in df.iterrows():
    cat = row['CATEGORY']
    weight = row['Weight']

    if cat in weight_cols:
        df.at[i, cat] = weight

    # AWB counting by category
    if cat == 'GENCARGO':
        df.at[i, 'GEN(awb)'] = 1
    elif cat == 'PER/COL':
        df.at[i, 'COL(awb)'] = 1
    elif cat == 'DG':
        df.at[i, 'DG(awb)'] = 1
    elif cat == 'TRANSIT':
        df.at[i, 'TRANSIT(awb)'] = 1
    elif cat == 'COURIER':
        df.at[i, 'COU(awb)'] = 1
    # 'P.O MAIL' doesn't have separate awb column

# =========================
#  5️⃣ GROUPING & AGGREGATION
# =========================

group_cols = ['Flight Date', 'Carrier', 'Flight Number', 'ROUTE']
agg_dict = {col: 'sum' for col in weight_cols + awb_cols}

grouped = df.groupby(group_cols).agg(agg_dict).reset_index()

# =========================
#  6️⃣ R/CATEGORY LOGIC
# =========================

def categorize_route(airline):
    airline = str(airline).upper()
    if airline.startswith('TC1') or airline.startswith('PW'):
        return 'DOMESTIC'
    elif airline.startswith('TC2') or airline.startswith('TC4') or airline.startswith('TC5'):
        return 'TC-FOREIGN'
    else:
        return 'FOREIGN'

grouped['R/CATEGORY'] = grouped['Carrier'].apply(categorize_route)

# =========================
#  7️⃣ TOTALS & COLUMN ORDER
# =========================

grouped['AWB TOTAL'] = (
    grouped['GEN(awb)'] +
    grouped['COL(awb)'] +
    grouped['DG(awb)'] +
    grouped['TRANSIT(awb)'] +
    grouped['COU(awb)']
)

grouped['TOTAL WEIGHT'] = grouped[weight_cols].sum(axis=1)

# Rename columns to match final format
grouped = grouped.rename(columns={
    'Flight Date': 'Date',
    'Carrier': 'AIRLINE',
    'Flight Number': 'FLGHTNO'
})

final_cols = [
    'Date', 'AIRLINE', 'FLGHTNO', 'ROUTE', 'R/CATEGORY',
    'GENCARGO', 'PER/COL', 'DG', 'TRANSIT', 'P.O MAIL', 'COURIER',
    'GEN(awb)', 'COL(awb)', 'DG(awb)', 'TRANSIT(awb)', 'COU(awb)',
    'AWB TOTAL', 'TOTAL WEIGHT'
]

grouped = grouped[final_cols]

# =========================
#  8️⃣ OUTPUT
# =========================

grouped.to_excel(output_file, index=False)
print(f"✅ Transformation complete. File saved as: {output_file}")
print(f"📝 Transit conflicts logged to: {transit_conflict_file}")

# =========================
#  9️⃣ TERMINAL SUMMARY
# =========================

print("\n===== 📊 TRANSFORMATION SUMMARY =====")
print(f"Total unique flights processed: {len(grouped)}")
print(f"Total AWBs processed: {int(grouped['AWB TOTAL'].sum())}")
print(f"Total weight processed: {grouped['TOTAL WEIGHT'].sum():,.2f} kg")

print("\nBreakdown by Category (Weight):")
weight_sums = grouped[weight_cols].sum()
print(weight_sums.to_string())

print("\nBreakdown by Category (AWBs):")
awb_sums = grouped[awb_cols].sum()
print(awb_sums.to_string())
print("======================================\n")
