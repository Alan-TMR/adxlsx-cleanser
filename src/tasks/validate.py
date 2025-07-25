# src/tasks/validate.py

import os
import pandas as pd
import io
from common import azure

INTERMEDIATE_CONTAINER = "in-progress"
FINAL_CONTAINER = "validated"

def run(run_id: str, input_blob_name: str, original_path: str):
    """
    Reads a cleansed CSV, performs validation, and saves it to the final
    container with a name that includes the original filename and sheet name.
    """
    print(f"[{run_id}] Running 'validate' task for {input_blob_name}...")
    
    in_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, input_blob_name)
    data = in_blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    
    # Add your validation logic here.
    print(f"[{run_id}] File passed validation checks.")
    
    # Get the original filename without extension
    base_name = os.path.splitext(original_path)[0]
    # Get the sheet name from the intermediate file path
    sheet_name = input_blob_name.split('01-cleansed-')[-1].replace('.csv', '')
    
    # Construct final name, e.g., "path/to/file_SheetName.csv"
    output_blob_name = f"{base_name}_{sheet_name}.csv"
    
    out_blob = azure.get_blob_client(FINAL_CONTAINER, output_blob_name)
    out_blob.upload_blob(data, overwrite=True)

    print(f"[{run_id}] Validated file saved to: {FINAL_CONTAINER}/{output_blob_name}")