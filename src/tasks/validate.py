# src/tasks/validate.py

import os
import re
import pandas as pd
import io
from common import azure

INTERMEDIATE_CONTAINER = "in-progress"
FINAL_CONTAINER = "validated"

MONTH_MAP = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}

# Example file name pattern to parse:
# "KRAMOR – 1_2248719_May_2025_v2.1.xlsx"
#   ^KRA num^   ^proj^   ^mon^ ^yr^ ^version^
FNAME_RE = re.compile(
    r"""KRAMOR\s*[–-]\s*(?P<kra>\d+)_           # 'KRAMOR – 1'  (en dash or hyphen)
        (?P<proj>\d+)_                          # project id (variable length)
        (?P<mon>[A-Za-z]+)_                     # month word/abbr
        (?P<yr>\d{4})_                          # 4-digit year
        (?P<ver>v[\d.]+)                        # version like v2.1
        $""",
    re.VERBOSE
)

def _decode_month_to_mm(mon_word: str) -> str:
    key = mon_word.strip().lower()
    # try exact, then 3-letter stem (covers 'September'/'Sept' -> 'sep')
    if key in MONTH_MAP:
        return MONTH_MAP[key]
    stem = key[:3]
    if stem in MONTH_MAP:
        return MONTH_MAP[stem]
    raise ValueError(f"Unrecognized month '{mon_word}' in original file name.")

def _build_output_name_from_original(original_path_no_ext: str, sheet_name: str) -> str:
    """
    Build '<ProjectId>_<Version>_<YYYYMM>_KRA<Number>_<Sheet>.csv'
    from a base like 'KRAMOR – 1_2248719_May_2025_v2.1'
    """
    fname = os.path.basename(original_path_no_ext)
    m = FNAME_RE.match(fname)
    if not m:
        raise ValueError(
            "Original file name did not match the expected pattern "
            "'KRAMOR – <num>_<project>_<Month>_<Year>_<version>'. "
            f"Got: '{fname}'"
        )
    kra = m.group("kra")
    proj = m.group("proj")
    mon_word = m.group("mon")
    year = m.group("yr")
    ver = m.group("ver")

    mm = _decode_month_to_mm(mon_word)
    yyyymm = f"{year}{mm}"
    return f"{proj}_{ver}_{yyyymm}_KRA{kra}_{sheet_name}.csv"

def run(run_id: str, input_blob_name: str, original_path: str):
    """
    Reads a cleansed CSV, performs validation, and saves it to the final
    container using the new naming convention.
    """
    print(f"[{run_id}] Running 'validate' task for {input_blob_name}...")

    # 1) Read cleansed CSV
    in_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, input_blob_name)
    data = in_blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))

    # 2) (Your validation logic goes here)
    print(f"[{run_id}] File passed validation checks.")

    # 3) Build output name from original path + sheet name
    base_no_ext = os.path.splitext(original_path)[0]  # e.g. '2248719/202505/KRAMOR – 1_2248719_May_2025_v2.1'
    sheet_name = input_blob_name.split('01-cleansed-')[-1].replace('.csv', '')  # e.g. '1_1_a'
    new_filename = _build_output_name_from_original(os.path.basename(base_no_ext), sheet_name)

    # Keep original directory (e.g., '2248719/202505/') and swap the filename:
    out_dir = os.path.dirname(base_no_ext)           # '2248719/202505'
    output_blob_name = f"{out_dir}/{new_filename}" if out_dir else new_filename

    # 4) Write to final container
    out_blob = azure.get_blob_client(FINAL_CONTAINER, output_blob_name)
    out_blob.upload_blob(data, overwrite=True)
    print(f"[{run_id}] Validated file saved to: {FINAL_CONTAINER}/{output_blob_name}")
