import pandas as pd
import os

# ================================
# Configuration
# ================================
INPUT_FILE = "Book1.xlsx"   # Raw system data
OUTPUT_FILE = "Book2.xlsx"  # Clean formatted report
UNCLASSIFIED_FILE = "unclassified_words.txt"

# Mapping dictionary (extend as needed)
CATEGORY_MAPPING = {
    "meat": "MEAT",
    "nyama": "MEAT",
    "beef": "MEAT",
    "goat": "MEAT",
    "fish": "FISH",
    "samaki": "FISH",
    "crab": "CRABS/LOBSTER",
    "lobster": "CRABS/LOBSTER",
    "vegetable": "VEGETABLES",
    "mboga": "VEGETABLES",
    "avocado": "AVOCADO",
    "flowers": "FLOWERS",
    "maua": "FLOWERS",
    "valuable": "VALUABLES",
    "valuables": "VALUABLES",
    "courier": "COURIER",
    "mail": "P.O.MAIL",
    "posta": "P.O.MAIL",
    "mal": "P.O.MAIL",  # AWB containing "MAL"
    "per": "PER/COL",
    "col": "PER/COL",
    "dg": "DG",
    "hazard": "DG",
    "general": "G. CARGO"
}

# SHC code mapping
SHC_MAPPING = {
    "PEM": "MEAT",
    "PER": "PER/COL",
    "COL": "PER/COL",
    "DG": "DG",
    "MAL": "P.O.MAIL",
    "AVI": "AVOCADO",
    "FLW": "FLOWERS",
    "VAL": "VALUABLES"
}


# ================================
# Functions
# ================================
def normalize_text(text):
    """Lowercase and strip spaces for robust matching."""
    if pd.isna(text):
        return ""
    return str(text).strip().lower()


def classify_goods(nature_goods, shcs, awb):
    """Classify a record into a cargo category with double-checking."""

    nature = normalize_text(nature_goods)
    shc = normalize_text(shcs)
    awb = normalize_text(awb)

    nature_category = None
    shc_category = None

    # Try SHC mapping
    for code, category in SHC_MAPPING.items():
        if shc.startswith(code.lower()):
            shc_category = category
            break

    # Try Nature of Goods mapping
    for key, category in CATEGORY_MAPPING.items():
        if key in nature:
            nature_category = category
            break

    # Try AWB check for mail
    if "mal" in awb:
        nature_category = "P.O.MAIL"

    # Decision logic
    if shc_category and nature_category:
        if shc_category == nature_category:
            return shc_category
        else:
            # Conflict → log and default to G. CARGO
            with open(UNCLASSIFIED_FILE, "a", encoding="utf-8") as f:
                f.write(f"CONFLICT | AWB:{awb} | Nature:{nature_goods} | SHC:{shcs}\n")
            return "G. CARGO"

    if shc_category:
        return shc_category
    if nature_category:
        return nature_category

    # If nothing matches → log and fallback
    with open(UNCLASSIFIED_FILE, "a", encoding="utf-8") as f:
        f.write(f"UNCLASSIFIED | AWB:{awb} | Nature:{nature_goods} | SHC:{shcs}\n")
    return "G. CARGO"


def transform():
    # Read input
    df = pd.read_excel(INPUT_FILE)

    # Ensure SECTOR column is created
    if "Origin" in df.columns and "Dest" in df.columns:
        df["SECTOR"] = df["Origin"].astype(str) + "-" + df["Dest"].astype(str)
    else:
        df["SECTOR"] = ""  # fallback if not available

    # Classify each record
    df["CATEGORY"] = df.apply(
        lambda row: classify_goods(row.get("Nature Goods", ""), row.get("SHCs", ""), row.get("AWB", "")),
        axis=1
    )

    # Prepare output columns
    categories = [
        "G. CARGO", "VEGETABLES", "AVOCADO", "FISH", "MEAT", "VALUABLES",
        "FLOWERS", "PER/COL", "DG", "CRABS/LOBSTER", "P.O.MAIL", "COURIER"
    ]

    output_columns = [
        "DATE", "AIRLINE", "FLIGHT No", "SECTOR", "F/CATEGORY"
    ] + categories + \
    [c + " AWBs" for c in categories] + ["TOTAL AWBs", "TOTAL WEIGHT"]

    # Aggregate
    grouped = []
    for keys, group in df.groupby(["Flight date", "Carrier", "Flight No.", "SECTOR"]):
        record = {
            "DATE": keys[0],
            "AIRLINE": keys[1],
            "FLIGHT No": keys[2],
            "SECTOR": keys[3],
            "F/CATEGORY": ""  # not implemented yet
        }

        total_awbs = 0
        total_weight = 0

        for cat in categories:
            cat_group = group[group["CATEGORY"] == cat]
            weight_sum = cat_group["weight"].sum()
            awb_count = cat_group["AWB"].nunique()

            record[cat] = weight_sum
            record[cat + " AWBs"] = awb_count

            total_awbs += awb_count
            total_weight += weight_sum

        record["TOTAL AWBs"] = total_awbs
        record["TOTAL WEIGHT"] = total_weight
        grouped.append(record)

    # Save output
    output_df = pd.DataFrame(grouped, columns=output_columns)
    output_df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Transformation complete! Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found.")
    else:
        transform()
