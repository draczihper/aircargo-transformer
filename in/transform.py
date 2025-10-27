"""
Air Cargo Import Data Transformation
------------------------------------
Run with: python transform.py
Input:  Book1.xlsx  (must be in same directory)
Output: Book2.xlsx, plus logs printed in console
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------
# 1. Helper Functions
# ---------------------------------------------------------------------
def shc_tokens(shc_field):
    if pd.isna(shc_field) or str(shc_field).strip() == "":
        return set()
    parts = re.split(r"[\s,;|/]+", str(shc_field).upper())
    return set(p.strip() for p in parts if p.strip() != "")


def classify_flight_route(origin, dest):
    return f"{str(origin).strip().upper()}-{str(dest).strip().upper()}"


def classify_flight_category(carrier, flight_no):
    c = str(carrier).strip().upper()
    fn = str(flight_no).strip().upper()
    if c == "PW":
        return "DOMESTIC"
    if c == "TC":
        if fn.startswith("TC1"):
            return "DOMESTIC"
        if fn.startswith(("TC2", "TC4", "TC5")):
            return "TC-FOREIGN"
        return "FOREIGN"
    return "FOREIGN"


# ---------------------------------------------------------------------
# 2. Classification Logic
# ---------------------------------------------------------------------
def classify_awb_group(rows, transit_conflicts, unclassified_entries):
    # Safe extraction of AWB value (fixes AttributeError)
    if hasattr(rows, "name"):
        awb_value = rows.name
    else:
        awb_value = str(rows["awb"].iloc[0]) if "awb" in rows.columns and len(rows) > 0 else None

    weight_sum = rows["weight"].fillna(0).astype(float).sum()
    has_ckd = any("CKD" in str(s).upper() for s in rows["import_status"])
    dests = set(str(d).strip().upper() for d in rows["awb_dest"].fillna(""))
    dest_not_dar = any(d != "DAR" and d != "" for d in dests)

    if has_ckd and dest_not_dar:
        return "TRANSIT", weight_sum
    if has_ckd ^ dest_not_dar:
        r0 = rows.iloc[0]
        transit_conflicts.append({
            "AWB": awb_value,
            "Has_CKD": has_ckd,
            "Dest_Not_DAR": dest_not_dar,
            "Import_Status": r0.get("import_status", ""),
            "AWB_Dests": list(dests),
            "Weight": weight_sum
        })

    # COURIER
    if any("COU" in shc for shc in rows["shcs"].astype(str).str.upper()) or \
       any("COURIER" in str(n).upper() for n in rows["nature_goods"]):
        return "COURIER", weight_sum

    # PER/COL
    per_shcs = {"COL", "FRO", "CRT", "ICE", "ERT", "PER", "PEF", "PES", "PEM"}
    per_kw = ["PERISHABLE", "FRESH", "CHILLED", "FROZEN", "COOL", "COLD",
              "FLOWER", "FISH", "MEAT", "VEGETABLE", "FRUIT", "AVOCADO"]
    if any(s in per_shcs for s in shc_tokens(" ".join(rows["shcs"].astype(str)))) or \
       any(any(k in str(n).upper() for k in per_kw) for n in rows["nature_goods"]):
        return "PER/COL", weight_sum

    # DG
    dg_shcs = {"DGR", "RRY", "RMD", "RPB", "RFL", "RCG", "RNG", "RIS", "RDS"}
    if any(s in dg_shcs for s in shc_tokens(" ".join(rows["shcs"].astype(str)))) or \
       any("DANGEROUS" in str(n).upper() for n in rows["nature_goods"]):
        return "DG", weight_sum

    # Default
    return "GENCARGO", weight_sum


# ---------------------------------------------------------------------
# 3. Main Transformation
# ---------------------------------------------------------------------
def transform_data(input_file, output_file="Book2.xlsx"):
    df = pd.read_excel(input_file, dtype=str)
    total_rows = len(df)
    print(f"Total rows read: {total_rows}")

    # --- Normalize Columns ---
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # --- Clean & Type Conversion ---
    for c in ["awb", "uld_number", "import_status", "awb_dest", "nature_goods", "shcs",
              "carrier", "origin", "dest", "flight_no"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()

    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0)
    df["pieces"] = pd.to_numeric(df["pieces"], errors="coerce").fillna(0).astype(int)
    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce")
    df["flight_date_only"] = df["flight_date"].dt.date

    # --- Filter by Status ---
    before = len(df)
    df = df[~df["import_status"].str.upper().isin(["MIS", "ACC", "NOT"])]
    print(f"Rows removed (status filter): {before - len(df)}")

    # --- Remove zero-weight rows ---
    before = len(df)
    df = df[df["weight"] > 0]
    print(f"Rows removed (zero weight): {before - len(df)}")

    # --- Remove empty AWBs ---
    before = len(df)
    df = df[df["awb"].str.strip() != ""]
    print(f"Rows removed (empty AWB): {before - len(df)}")

    # --- Remove AWBs starting with HWB ---
    before = len(df)
    df = df[~df["awb"].str.upper().str.startswith("HWB", na=False)]
    print(f"Rows removed (HWB AWBs): {before - len(df)}")

    # --- Remove strict duplicates ---
    dup_cols = ["flight_date_only", "flight_no", "carrier",
                "pieces", "weight", "uld_number", "nature_goods", "shcs"]
    before = len(df)
    df = df.drop_duplicates(subset=dup_cols, keep="first")
    print(f"Rows removed (duplicates): {before - len(df)}")

    # -----------------------------------------------------------------
    # Row-level P.O MAIL detection
    # -----------------------------------------------------------------
    df["AWB_norm"] = df["awb"].str.upper()
    df["NATURE_up"] = df["nature_goods"].str.upper()
    df["SHC_up"] = df["shcs"].str.upper()

    cond_awb_mal = df["AWB_norm"].str.startswith("MAL", na=False)
    cond_nature_mail = df["NATURE_up"].str.contains("MAIL", na=False)
    cond_nature_dipl = df["NATURE_up"].str.contains("DIPLOMATIC", na=False)
    cond_nature_mail_eff = cond_nature_mail & (~cond_nature_dipl)

    def shc_has_mal(s):
        return "MAL" in re.split(r"[\s,;|/]+", str(s).upper())

    cond_shc_mal = df["SHC_up"].apply(shc_has_mal)
    df["is_po_mail_row"] = cond_awb_mal | cond_nature_mail_eff | cond_shc_mal

    group_cols = ["flight_date_only", "carrier", "flight_no", "origin", "dest"]
    po_mail_by_flight = (
        df[df["is_po_mail_row"]]
        .groupby(group_cols, dropna=False)["weight"]
        .sum()
        .rename("P.O MAIL_weight")
        .reset_index()
    )
    po_mail_rows_weight = df.loc[df["is_po_mail_row"], "weight"].sum()
    print(f"Detected P.O MAIL total weight (row-level): {po_mail_rows_weight:.2f} kg")

    # Remove P.O MAIL rows before AWB-level grouping
    df = df[~df["is_po_mail_row"]].copy()

    # -----------------------------------------------------------------
    # Flight-level aggregation
    # -----------------------------------------------------------------
    flights = []
    transit_conflicts, unclassified_entries = [], []
    unique_awbs_global = set()
    total_weight_sum = 0

    for (fdate, carrier, fno, orig, dest), sub in df.groupby(group_cols):
        route = classify_flight_route(orig, dest)
        rcat = classify_flight_category(carrier, fno)
        weights = {"GENCARGO": 0, "PER/COL": 0, "DG": 0, "TRANSIT": 0, "P.O MAIL": 0, "COURIER": 0}
        awb_sets = {k: set() for k in ["GENCARGO", "PER/COL", "DG", "TRANSIT", "COURIER"]}

        for awb, awb_rows in sub.groupby("awb", dropna=False):
            cat, w = classify_awb_group(awb_rows, transit_conflicts, unclassified_entries)
            weights[cat] = weights.get(cat, 0) + float(w)
            if cat != "P.O MAIL" and awb.strip() != "":
                awb_sets[cat].add(awb)
                unique_awbs_global.add(awb)
            total_weight_sum += float(w)

        # Add mail weight for this flight
        po_match = po_mail_by_flight[
            (po_mail_by_flight["flight_date_only"] == fdate)
            & (po_mail_by_flight["carrier"] == carrier)
            & (po_mail_by_flight["flight_no"] == fno)
            & (po_mail_by_flight["origin"] == orig)
            & (po_mail_by_flight["dest"] == dest)
        ]
        po_w = 0 if po_match.empty else float(po_match["P.O MAIL_weight"].iloc[0])
        weights["P.O MAIL"] = po_w
        total_weight_sum += po_w

        awb_counts = {k: len(v) for k, v in awb_sets.items()}
        awb_total = sum(awb_counts.values())
        total_weight = sum(weights.values())

        flights.append({
            "DATE": fdate, "AIRLINE": carrier, "FLIGHT NO": fno, "ROUTE": route, "R/CATEGORY": rcat,
            "GENCARGO": weights["GENCARGO"], "PER/COL": weights["PER/COL"], "DG": weights["DG"],
            "TRANSIT": weights["TRANSIT"], "P.O MAIL": weights["P.O MAIL"], "COURIER": weights["COURIER"],
            "GEN(awb)": awb_counts["GENCARGO"], "COL(awb)": awb_counts["PER/COL"],
            "DG(awb)": awb_counts["DG"], "TNST(awb)": awb_counts["TRANSIT"], "COU(awb)": awb_counts["COURIER"],
            "AWB TOTAL": awb_total, "TOTAL WEIGHT": total_weight
        })

    # -----------------------------------------------------------------
    # Write Outputs
    # -----------------------------------------------------------------
    book2 = pd.DataFrame(flights)
    cols = ["DATE", "AIRLINE", "FLIGHT NO", "ROUTE", "R/CATEGORY",
            "GENCARGO", "PER/COL", "DG", "TRANSIT", "P.O MAIL", "COURIER",
            "GEN(awb)", "COL(awb)", "DG(awb)", "TNST(awb)", "COU(awb)",
            "AWB TOTAL", "TOTAL WEIGHT"]
    book2 = book2[cols]
    book2.to_excel(output_file, index=False)

    print(f"\nFlights processed: {len(book2)}")
    print(f"Total unique AWBs (excluding mail): {len(unique_awbs_global)}")
    print(f"Grand total weight: {book2['TOTAL WEIGHT'].sum():.2f} kg")
    print(f"P.O MAIL total weight: {book2['P.O MAIL'].sum():.2f} kg")
    print(f"\nOutput saved to {output_file}")


# ---------------------------------------------------------------------
# 4. Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    transform_data("Book1.xlsx", "Book2.xlsx")
