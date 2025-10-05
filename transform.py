# transform.py
import pandas as pd
import re
import os
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
    Carrier-based rules:
      - PW: default DOMESTIC, but PW717 & PW721 -> PW-FOREIGN
      - TC: TC100* -> DOMESTIC, TC2*/TC4* -> TC-FOREIGN
      - others -> FOREIGN
    Notes:
      - flight_no may include letters or not, so we normalize both alphabetic and numeric forms.
    """
    c = normalize_text(carrier).upper()
    fn_raw = normalize_text(flight_no).upper()

    # numeric part of flight no (remove non-digits)
    fn_digits = re.sub(r"\D", "", fn_raw)

    if c == "PW":
        # check both possible representations
        if fn_raw.startswith("PW717") or fn_raw.startswith("PW721") or fn_digits.startswith("717") or fn_digits.startswith("721"):
            return "PW-FOREIGN"
        return "DOMESTIC"

    if c == "TC":
        if fn_raw.startswith("TC100") or fn_digits.startswith("100"):
            return "DOMESTIC"
        if fn_raw.startswith("TC2") or fn_digits.startswith("2") or fn_raw.startswith("TC4") or fn_digits.startswith("4"):
            return "TC-FOREIGN"
        # fallback
        return "FOREIGN"

    return "FOREIGN"

# -----------------------------
# Classification logic
# -----------------------------
def classify_goods(nature, shc_field, awb):
    """
    Priority:
      1. Nature-of-goods specific keywords -> choose that category (MEAT, FISH, etc).
      2. If nature is generic (perishable/chilled/...), use SHC to decide.
      3. If no nature, use SHC (choose from SHC_MAP tokens using a small priority list).
      4. If AWB contains 'MAL' treat as P.O.MAIL.
      5. Else log and assign G. CARGO.
    """
    nature_l = normalize_lower(nature)
    shc_raw = normalize_text(shc_field).upper()
    shc_tokens = [t for t in re.split(r"[\s,/;]+", shc_raw) if t]

    # 1) Nature-of-goods priority
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in nature_l:
                # edge: if nature says lobster/crab but SHC is PES (fish) -> keep CRABS/LOBSTER
                if cat == "FISH" and ("lobster" in nature_l or "crab" in nature_l):
                    return "CRABS/LOBSTER"
                return cat

    # 2) SHC tokens -> map to categories (collect all candidates)
    shc_cands = []
    for token in shc_tokens:
        if token in SHC_MAP:
            shc_cands.append(SHC_MAP[token])

    shc_cands = list(dict.fromkeys(shc_cands))  # unique preserve order

    # AWB rule for mail (if AWB contains MAL)
    if "MAL" in str(awb).upper():
        return "P.O.MAIL"

    # 3) If nature contains generic words (chilled/frozen/perishable...), use SHC when available
    if any(g in nature_l for g in GENERIC_WORDS) and shc_cands:
        # If SHC contains multiple categories choose sensible priority:
        priority = ["MEAT", "CRABS/LOBSTER", "FISH", "FLOWERS", "AVOCADO", "VALUABLES", "COURIER", "P.O.MAIL", "PER/COL", "DG"]
        for p in priority:
            if p in shc_cands:
                return p
        # otherwise pick first
        return shc_cands[0]

    # 4) If no nature but SHC present -> pick from shc_cands with priority
    if shc_cands:
        priority = ["MEAT", "CRABS/LOBSTER", "FISH", "FLOWERS", "AVOCADO", "VALUABLES", "COURIER", "P.O.MAIL", "PER/COL", "DG"]
        for p in priority:
            if p in shc_cands:
                return p
        return shc_cands[0]

    # 5) Nothing matched => log and default to General Cargo
    log_unclassified("UNCLASSIFIED", awb, nature, shc_field)
    return "G. CARGO"

# -----------------------------
# Input reader (robust header handling)
# -----------------------------
def read_input(input_file):
    """
    Attempt to read the Excel with sensible header row.
    Prefer header=1 (your usual export), fallback to header=0.
    Normalize/rename key columns to standardized names.
    """
    df = None
    for hdr in (1, 0, 2):
        try:
            tmp = pd.read_excel(input_file, header=hdr)
        except Exception:
            continue
        cols_lower = [str(c).strip().lower() for c in tmp.columns]
        # We expect at least 'flight' and 'awb' columns to be present
        if any("flight" in c and "date" in c for c in cols_lower) or any("flight no" in c for c in cols_lower):
            df = tmp
            break
    if df is None:
        # final fallback - try default read
        df = pd.read_excel(input_file, header=0)

    # build a mapping from current column names to standardized keys
    col_map = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if "flight" in lc and "date" in lc:
            col_map[c] = "flight_date"
        elif lc == "carrier" or "carrier" in lc or "airline" in lc:
            col_map[c] = "carrier"
        elif "flight" in lc and ("no" in lc or "number" in lc):
            col_map[c] = "flight_no"
        elif "origin" in lc:
            col_map[c] = "origin"
        elif "dest" in lc or "destination" in lc:
            col_map[c] = "dest"
        elif "awb" in lc:
            col_map[c] = "awb"
        elif "rcv" in lc and "weight" in lc or lc == "weight" or "rcv weight" in lc:
            col_map[c] = "rcv_weight"
        elif "nature" in lc and "good" in lc:
            col_map[c] = "nature_goods"
        elif "shc" in lc:
            col_map[c] = "shcs"
        else:
            col_map[c] = c  # keep original if unknown

    df = df.rename(columns=col_map)

    # Ensure required cols exist (create if missing)
    for required in ("flight_date", "carrier", "flight_no", "awb", "rcv_weight", "nature_goods", "shcs"):
        if required not in df.columns:
            df[required] = ""

    # force rcv_weight numeric
    df["rcv_weight"] = pd.to_numeric(df["rcv_weight"], errors="coerce").fillna(0.0)

    # Make sure flight_no and carrier are strings
    df["flight_no"] = df["flight_no"].astype(str)
    df["carrier"] = df["carrier"].astype(str)

    return df

# -----------------------------
# Main transform
# -----------------------------
def transform(input_file=INPUT_FILE, output_file=OUTPUT_FILE):
    # Read input
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        return

    df = read_input(input_file)

    # Prepare logging header
    ensure_log_header()

    # Compute flight category (F/CATEGORY) per row
    df["f_category"] = df.apply(lambda r: classify_flight_category(r.get("carrier", ""), r.get("flight_no", "")), axis=1)

    # Compute "category" per AWB row
    df["category"] = df.apply(lambda r: classify_goods(r.get("nature_goods", ""), r.get("shcs", ""), r.get("awb", "")), axis=1)

    # We IGNORE origin/dest when grouping — the user requested sector be empty.
    # Group by: flight_date, carrier, flight_no, f_category
    group_cols = ["flight_date", "carrier", "flight_no", "f_category"]

    aggregated = []
    for keys, grp in df.groupby(group_cols, dropna=False):
        flight_date, carrier, flight_no, f_category = keys
        # Prepare row skeleton
        row = {col: 0 for col in OUTPUT_COLUMNS}
        row["DATE"] = flight_date
        row["AIRLINE"] = carrier
        row["FLIGHT No"] = flight_no
        row["SECTOR"] = ""          # intentionally left empty
        row["F/CATEGORY"] = f_category

        # Sum weights & count unique AWBs per category
        total_awbs = 0
        total_weight = 0.0

        # track seen AWBs to count unique awbs per category reliably
        # (we'll count per-category using nunique below)
        for cat in [c for c in CATEGORY_KEYWORDS.keys()] + list({"PER/COL","DG"}):
            pass  # we won't use this loop; we'll use fixed category list next

        # Use fixed categories order for sums (matching OUTPUT_COLUMNS)
        category_list = ["G. CARGO", "VEGETABLES", "AVOCADO", "FISH", "MEAT", "VALUABLES",
                         "FLOWERS", "PER/COL", "DG", "CRABS/LOBSTER", "P.O.MAIL", "COURIER"]

        for cat in category_list:
            sub = grp[grp["category"] == cat]
            wsum = float(sub["rcv_weight"].sum()) if not sub.empty else 0.0
            awb_count = int(sub["awb"].nunique()) if not sub.empty else 0
            # Put weight in category column
            row[cat] = wsum
            # Put AWB count in AWB column (if mapping exists)
            awb_col = AWB_COL_MAP.get(cat)
            if awb_col:
                row[awb_col] = awb_count
            total_weight += wsum
            total_awbs += awb_count

        row["TOTAL WEIGHT"] = total_weight
        row["TOTAL AWBs"] = total_awbs

        aggregated.append(row)

    out_df = pd.DataFrame(aggregated, columns=OUTPUT_COLUMNS)

    # Ensure numeric columns are numeric (replace NaN with 0)
    num_cols = [c for c in OUTPUT_COLUMNS if c not in ("DATE","AIRLINE","FLIGHT No","SECTOR","F/CATEGORY")]
    out_df[num_cols] = out_df[num_cols].fillna(0)

    out_df.to_excel(output_file, index=False)
    print(f"✅ Transformation complete — saved to '{output_file}' (SECTOR intentionally empty).")
    print(f"Unclassified/conflict log: '{UNCLASSIFIED_FILE}' (timestamped).")

if __name__ == "__main__":
    transform()
