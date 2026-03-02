"""
data_cleaning_pipeline.py
==========================
Complete Data Cleaning Pipeline for Krishna Dairy — ALL_YEARS_COMBINED Dataset

Steps performed:
  1. Load CSV (auto-detects encoding)
  2. Translate PRODDESC from Akruti garbled text → English
  3. Drop unnecessary columns: DC_NO, SUB_POS_NAME, GSTIN, HSN
  4. Parse & standardize DC_DATE → datetime (YYYY-MM-DD) for time series
  5. Sort data by DC_DATE ascending
  6. Strip whitespace from all string columns
  7. Drop fully duplicate rows
  8. Report summary stats & any unmapped PRODDESC values
  9. Save cleaned output as ALL_YEARS_CLEANED.csv

Usage:
    python data_cleaning_pipeline.py
    python data_cleaning_pipeline.py --input ALL_YEARS_COMBINED.csv --output ALL_YEARS_CLEANED.csv
"""

import pandas as pd
import re
import argparse
import os
import sys

# ============================================================
# PRODUCT MAP: Garbled Akruti Text → English Product Names
# ============================================================

PRODUCT_MAP = {
    # Skimmed Milk Powder
    "òç¨îÙÀ òÙðâ¨î ÑððãðÀÜ (±ððÚð)":                       "Skimmed Milk Powder (Cow)",
    "òç¨îÙÀ òÙðâ¨î ÑððãðÀÜ (Ùèøçð)":                      "Skimmed Milk Powder (Buffalo)",

    # Butter
    "±ððÚð ×ð¾Ü  (Ç÷äðó âðð÷Âðó )":                        "Cow Butter (Desi Loni)",
    "±ððÚð ×ð¾Ü (Ç÷äðó âðð÷Âðó) ¦¨çðÑðð÷¾á":              "Cow Butter (Desi Loni) Export",
    "Ùèøçð ×ð¾Ü (Ç÷äðó âðð÷Âðó )":                         "Buffalo Butter (Desi Loni)",

    # Milk
    "Ñððäµð.Òôîâð òªîÙð òÙðâ¨î":                           "Past. Full Cream Milk",
    "òÑßÙðóÚðÙð (Ñððäµð.±ððÚð ÇõÏð)":                     "Premium (Past. Cow Milk)",
    "Ãðð¸ð±ðó (Ñððäµð.¾ð÷ÐÀ ÇõÏð)":                       "Taazgi (Past. Toned Milk)",
    "ÑððäµðÜðýá¸À ç¾ùÂÀÀá ÇõÏð":                           "Pasteurized Standard Milk",
    "èúÇóÚðô¨Ãð ¨îðÁð ç¾òÜâððÚð»ðÀ ÇõÏð":                "Homogenized Sterilized Milk",
    "Úðô¦µð¾ó ¾ð÷ÐÀ ÇõÏð ( ¾÷¾àð Ñðù¨î )":                "UHT Toned Milk (Tetra Pack)",

    # Cream
    "±ððÚð òªîÙð  (¨îµµð÷ ) î":                            "Cow Cream (Raw)",
    "Ùèøçð òªîÙð (¨îµµð÷ )":                                "Buffalo Cream (Raw)",
    "±ððÚð òªîÙð  (ÑððäµðÜðýá¸À )":                        "Cow Cream (Pasteurized)",

    # Ghee
    "çðð¸ðô¨î ( Ùèøçð ) ÃðõÑð":                            "Pure Ghee (Buffalo)",
    "±ððýáµð÷ ÃðõÑð":                                       "Cow Ghee",

    # Lassi
    "âðççðó":                                                "Lassi",
    "¨öîæÂðð âðççðó Ùðû±ðð÷":                               "Krishna Lassi Mango",

    # Paneer
    "ÑðÐðóÜ":                                                "Paneer",
    "ÑðÐðóÜ (LOW FAT)":                                     "Paneer (Low Fat)",

    # Curd / Buttermilk
    "Çèó ( ¨öîæÂðð )":                                      "Curd (Krishna)",
    "Ãðð¨î":                                                 "Buttermilk (Tak)",
    "ò¸ðÜð Ãðð¨î":                                          "Jeera Buttermilk (Tak)",

    # Shrikhand & Chakka
    "åó®ðüÀ  (×ðÇðÙð - òÑðçÃðð )":                         "Shrikhand (Badam-Pista)",
    "¡ðü×ðð   åóï®ðüÀ":                                     "Amba Shrikhand (Mango)",
    "åó®ðüÀ (×ð¾Üç¨îðùµð)":                                "Shrikhand (Butterscotch)",
    "µð¨¨îð":                                                "Chakka (Shrikhand Base)",

    # Basundi
    "×ððçðôüÇó":                                             "Basundi",
    "¨öîæÂðð ×ððçðôüÇó (òçðÃððÒîú Ü×ðÀó)":                "Krishna Basundi (Sitafal)",

    # Krishna Flavoured Milk (Thanda)
    "¨îöæÂðð Òâð÷ãðÀá òÙðâ¨":                              "Krishna Flavoured Milk",
    "ç¾àðù×ð÷Üó  ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨":          "Krishna Thanda Strawberry (Flavoured Milk)",
    "×ð¾Üç¨îðùµð   ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨":         "Krishna Thanda Butterscotch (Flavoured Milk)",
    "Ùðùû±ðð÷ ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨":              "Krishna Thanda Mango (Flavoured Milk)",
    "òÑðçÃðð ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨":               "Krishna Thanda Pista (Flavoured Milk)",
    "µððù¨îâð÷¾ ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨î )":         "Krishna Thanda Chocolate (Flavoured Milk)",
    "¨÷îçðÜ ýáâððÚðµðó ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨î )": "Krishna Thanda Kesar Elaichi (Flavoured Milk)",
    "ÑððÚðÐððÑðâð ¨îöæÂðð  ¿üÀð (Òâð÷ãðÀá òÙðâ¨î )":      "Krishna Thanda Pineapple (Flavoured Milk)",
    "¨îðùÒîó ¨îöæÂðð ¿üÀð (Òâð÷ãðÀá òÙðâ¨î )":            "Krishna Thanda Coffee (Flavoured Milk)",

    # Sweets & Desserts
    "¨öîæÂðð ®ðãðð":                                        "Krishna Khawa (Mawa)",
    "¨öîæÂðð ±ðôâðð×ð ¸ððÙðôÐð":                            "Krishna Gulab Jamun",
    "¨öîæÂðð ýÐçð¾ü¾ ±ðôâðð×ð ¸ððÙðôÐð òÙð¨çð":            "Krishna Instant Gulab Jamun Mix",
    "¨öîæÂðð ¨ôîâÑðó":                                      "Krishna Kulfi",
    "Ñð÷Áð":                                                 "Peda",
    "Ñð÷Áð (±ðôú)":                                         "Peda (Jaggery)",

    # Milk Powder
    "èð÷âð òÙðâ¨î ÑððãðÀÜ ( WMP )":                        "Whole Milk Powder (WMP)",

    # Other
    "SWEEP POWDER":                                          "Sweep Powder",
}

# ============================================================
# COLUMNS TO DROP
# ============================================================

COLUMNS_TO_DROP = ["DC_NO", "SUB_POS_NAME", "GSTIN", "HSN"]

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def normalize_whitespace(text):
    """Collapse multiple spaces/tabs into a single space and strip."""
    if not isinstance(text, str):
        return text
    return re.sub(r'\s+', ' ', text).strip()


def translate_proddesc(value, mapping):
    """
    Translate a single PRODDESC value using the mapping.
    Tries: exact match → whitespace-normalized match → stripped match → partial match.
    Returns original value if no match found.
    """
    if not isinstance(value, str) or not value.strip():
        return value

    # Try 1: exact
    if value in mapping:
        return mapping[value]

    # Try 2: normalized whitespace
    normalized = normalize_whitespace(value)
    if normalized in mapping:
        return mapping[normalized]

    # Try 3: stripped only
    stripped = value.strip()
    if stripped in mapping:
        return mapping[stripped]

    # Try 4: partial match (key is substring of value)
    for key, translation in mapping.items():
        if key.strip() in value:
            return translation

    return value  # unchanged — log as unmapped


def is_garbled(value):
    """
    Detect if a string still contains Akruti garbled characters.
    These are high-codepoint Latin chars that appear when Akruti
    font data is read as Windows-1252 / Latin-1.
    """
    if not isinstance(value, str):
        return False
    garbled_chars = set(
        'òçÙðâÑãÀÜ±×ÇäÒôîÚÃàèåæéêëøùúûü÷ýþßÉÊËÌÍÎÏÐÓÔÕÖ'
        '¨öîæÂðçðôüÇó¸¡åï®ðüÀ¾ÜÑãñÝÙèøçµ³'
    )
    return any(c in garbled_chars for c in value)


def load_csv(filepath):
    """Load CSV with auto-encoding detection (UTF-8 → Latin-1 fallback)."""
    print(f"  Loading '{filepath}'...")
    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
        print(f"  Encoding: UTF-8")
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='latin-1', low_memory=False)
        print(f"  Encoding: Latin-1 (fallback)")
    return df


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline(input_file, output_file):

    print(f"\n{'='*65}")
    print(f"  Krishna Dairy — Data Cleaning Pipeline")
    print(f"{'='*65}")
    print(f"  Input  : {input_file}")
    print(f"  Output : {output_file}")
    print(f"{'='*65}\n")

    # ----------------------------------------------------------
    # STEP 1: LOAD
    # ----------------------------------------------------------
    print("[ STEP 1 ] Loading data...")
    if not os.path.exists(input_file):
        print(f"\n  ERROR: File '{input_file}' not found.")
        print("  Place ALL_YEARS_COMBINED.csv in the same folder and re-run.\n")
        sys.exit(1)

    df = load_csv(input_file)
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
    print(f"  Columns: {df.columns.tolist()}\n")

    original_rows = len(df)

    # ----------------------------------------------------------
    # STEP 2: STRIP WHITESPACE FROM ALL STRING COLUMNS
    # ----------------------------------------------------------
    print("[ STEP 2 ] Stripping whitespace from all string columns...")
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    print(f"  Cleaned {len(str_cols)} text columns.\n")

    # ----------------------------------------------------------
    # STEP 3: DROP UNNECESSARY COLUMNS
    # ----------------------------------------------------------
    print("[ STEP 3 ] Dropping unnecessary columns...")
    existing_drops = [c for c in COLUMNS_TO_DROP if c in df.columns]
    missing_drops  = [c for c in COLUMNS_TO_DROP if c not in df.columns]
    df.drop(columns=existing_drops, inplace=True)
    print(f"  Dropped  : {existing_drops}")
    if missing_drops:
        print(f"  Not found (skipped): {missing_drops}")
    print(f"  Remaining columns: {df.columns.tolist()}\n")

    # ----------------------------------------------------------
    # STEP 4: TRANSLATE PRODDESC → ENGLISH
    # ----------------------------------------------------------
    print("[ STEP 4 ] Translating PRODDESC to English...")
    if 'PRODDESC' not in df.columns:
        print("  WARNING: 'PRODDESC' column not found. Skipping.\n")
    else:
        original_proddesc = df['PRODDESC'].copy()
        df['PRODDESC'] = df['PRODDESC'].apply(lambda x: translate_proddesc(x, PRODUCT_MAP))

        translated = (original_proddesc != df['PRODDESC']).sum()
        print(f"  Translated: {translated:,} cells")

        # Report unmapped garbled values
        unmapped_mask = df['PRODDESC'].apply(is_garbled)
        if unmapped_mask.any():
            unmapped_vals = df.loc[unmapped_mask, 'PRODDESC'].value_counts()
            print(f"\n  [WARNING] {unmapped_mask.sum():,} rows still have garbled PRODDESC.")
            print("  Add these to PRODUCT_MAP and re-run:\n")
            for val, cnt in unmapped_vals.items():
                print(f"    {cnt:6,}x  {repr(val)}")
        else:
            print("  All PRODDESC values successfully translated!")
        print()

    # ----------------------------------------------------------
    # STEP 5: FIX DC_DATE — PARSE & FORMAT FOR TIME SERIES
    # ----------------------------------------------------------
    print("[ STEP 5 ] Parsing and formatting DC_DATE for time series...")
    if 'DC_DATE' not in df.columns:
        print("  WARNING: 'DC_DATE' column not found. Skipping.\n")
    else:
        pre_nulls = df['DC_DATE'].isna().sum()

        # Parse flexibly — handles M/D/YYYY, YYYY-MM-DD, DD-MM-YYYY, etc.
        df['DC_DATE'] = pd.to_datetime(df['DC_DATE'], infer_datetime_format=True, errors='coerce')

        post_nulls = df['DC_DATE'].isna().sum()
        failed = post_nulls - pre_nulls

        print(f"  Date range : {df['DC_DATE'].min().date()} → {df['DC_DATE'].max().date()}")
        print(f"  Parsed OK  : {df['DC_DATE'].notna().sum():,} rows")
        if failed > 0:
            print(f"  Failed (NaT): {failed:,} rows — these had unparseable date formats")
        print()

    # ----------------------------------------------------------
    # STEP 6: SORT BY DC_DATE (TIME SERIES ORDER)
    # ----------------------------------------------------------
    print("[ STEP 6 ] Sorting data by DC_DATE (ascending)...")
    if 'DC_DATE' in df.columns:
        df.sort_values('DC_DATE', ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        print(f"  Sorted {len(df):,} rows chronologically.\n")
    else:
        print("  Skipped (DC_DATE not available).\n")

    # ----------------------------------------------------------
    # STEP 7: DROP FULLY DUPLICATE ROWS
    # ----------------------------------------------------------
    print("[ STEP 7 ] Dropping duplicate rows...")
    before = len(df)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    dropped_dupes = before - len(df)
    print(f"  Removed: {dropped_dupes:,} duplicate rows")
    print(f"  Remaining: {len(df):,} rows\n")

    # ----------------------------------------------------------
    # STEP 8: FINAL COLUMN DTYPES REPORT
    # ----------------------------------------------------------
    print("[ STEP 8 ] Final column overview:")
    print(f"  {'Column':<25} {'Dtype':<15} {'Non-Null':<12} {'Unique'}")
    print(f"  {'-'*65}")
    for col in df.columns:
        non_null = df[col].notna().sum()
        unique   = df[col].nunique()
        dtype    = str(df[col].dtype)
        print(f"  {col:<25} {dtype:<15} {non_null:<12,} {unique:,}")
    print()

    # ----------------------------------------------------------
    # STEP 9: SAVE OUTPUT
    # ----------------------------------------------------------
    print("[ STEP 9 ] Saving cleaned data...")

    # Save DC_DATE as YYYY-MM-DD string in CSV (ideal for time series)
    df_save = df.copy()
    if 'DC_DATE' in df_save.columns:
        df_save['DC_DATE'] = df_save['DC_DATE'].dt.strftime('%Y-%m-%d')

    df_save.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  Saved: '{output_file}'")

    # ----------------------------------------------------------
    # PIPELINE SUMMARY
    # ----------------------------------------------------------
    print(f"\n{'='*65}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'='*65}")
    print(f"  Original rows  : {original_rows:,}")
    print(f"  Cleaned rows   : {len(df):,}")
    print(f"  Rows removed   : {original_rows - len(df):,} (duplicates + failed dates)")
    print(f"  Output file    : {output_file}")
    print(f"{'='*65}\n")

    # Sample of cleaned data
    print("Sample of cleaned data (first 5 rows):")
    print(df.head().to_string(index=False))
    print()

    return df


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Complete data cleaning pipeline for Krishna Dairy dataset'
    )
    parser.add_argument(
        '--input', default='ALL_YEARS_COMBINED.csv',
        help='Input CSV filename (default: ALL_YEARS_COMBINED.csv)'
    )
    parser.add_argument(
        '--output', default='ALL_YEARS_CLEANED.csv',
        help='Output CSV filename (default: ALL_YEARS_CLEANED.csv)'
    )
    args = parser.parse_args()

    run_pipeline(args.input, args.output)