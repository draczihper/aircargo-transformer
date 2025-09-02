import pandas as pd

# -----------------------------
# STEP 1: Define category map
# -----------------------------
category_map = {
    "MEAT": ["meat", "nyama", "goat meat", "beef", "fresh meat", "chilled meat", "frozen meat", "pem"],
    "FISH": ["fish", "samaki"],
    "VEGETABLES": ["vegetable", "mboga"],
    "AVOCADO": ["avocado", "parachichi"],
    "FLOWERS": ["flower", "maua"],
    "VALUABLES": ["valuable", "valu", "val"],
    "COURIER": ["courier", "ems", "express"],
    "CRABS/LOBSTER": ["crab", "lobster", "kamba"],
    "PER/COL": ["perishable", "per", "col"],
    "DG": ["dangerous", "dg", "hazard"],
    "G. CARGO": ["general", "cargo", "gc"]  # fallback
}

# -----------------------------
# STEP 2: Classify function
# -----------------------------
def classify_goods(nature, shc):
    text = str(nature).lower() + " " + str(shc).lower()
    for category, keywords in category_map.items():
        for kw in keywords:
            if kw in text:
                return category
    return "G. CARGO"  # default if no match

# -----------------------------
# STEP 3: Transform Book1 -> Book2
# -----------------------------
def transform_book1_to_book2(input_file, output_file):
    # Load Book1
    df = pd.read_excel(input_file)

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Create helper columns
    df["CATEGORY"] = df.apply(lambda row: classify_goods(row.get("nature goods", ""), row.get("shcs", "")), axis=1)
    df["SECTOR"] = df["origin"].astype(str) + "-" + df["dest"].astype(str)

    # -----------------------------
    # Aggregation logic
    # -----------------------------
    categories = list(category_map.keys())

    grouped = df.groupby(["flight date", "carrier", "flight no."])

    results = []
    for (date, airline, flight_no), group in grouped:
        row = {
            "DATE": date,
            "AIRLINE": airline,
            "FLIGHT No": flight_no,
            #"SECTOR": sector,
            "F/CATEGORY": "",  # leave empty for now
        }

        # Initialize numeric fields
        for cat in categories:
            row[cat] = 0
            row[f"{cat} AWBs"] = 0

        # Process AWBs
        for _, g in group.iterrows():
            cat = g["CATEGORY"]
            weight = g.get("weight", 0)
            row[cat] += weight
            row[f"{cat} AWBs"] += 1

        # Totals
        row["TOTAL AWBs"] = group.shape[0]
        row["TOTAL WEIGHT"] = group["weight"].sum()

        results.append(row)

    # -----------------------------
    # Create final DataFrame
    # -----------------------------
    final_df = pd.DataFrame(results)

    # Reorder columns (Book2 format)
    book2_columns = [
        "DATE", "AIRLINE", "FLIGHT No", "SECTOR", "F/CATEGORY",
        "G. CARGO", "VEGETABLES", "AVOCADO", "FISH", "MEAT", "VALUABLES",
        "FLOWERS", "PER/COL", "DG", "CRABS/LOBSTER", "P.O.MAIL", "COURIER",
        "G. CARGO AWBs", "VALUABLES AWBs", "VEGETABLES AWBs", "AVOCADO AWBs",
        "FISH AWBs", "MEAT AWBs", "COURIER AWBs", "CRABS/LOBSTER AWBs",
        "FLOWERS AWBs", "PER/COL AWBs", "DG AWBs", "TOTAL AWBs", "TOTAL WEIGHT"
    ]

    # Add missing columns if not present
    for col in book2_columns:
        if col not in final_df.columns:
            final_df[col] = 0

    final_df = final_df[book2_columns]

    # Save Book2
    final_df.to_excel(output_file, index=False)
    print(f"✅ Transformation complete! Saved to {output_file}")

# -----------------------------
# STEP 4: Run
# -----------------------------
if __name__ == "__main__":
    transform_book1_to_book2("Book1.xlsx", "Book2.xlsx")
