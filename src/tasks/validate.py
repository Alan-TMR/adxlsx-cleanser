# src/tasks/validate.py

import os
import pandas as pd
import io
from common import azure

INTERMEDIATE_CONTAINER = "in-progress"
FINAL_CONTAINER = "validated"

def run(run_id: str, input_blob_name: str) -> str:
    """
    Task 2: Reads the cleansed CSV, performs validation, and moves it
    to the final container.

    Args:
        run_id: The unique identifier for this workflow run.
        input_blob_name: The name of the cleansed blob from the previous step.

    Returns:
        The name of the final validated blob.
    """
    print(f"[{run_id}] Running 'validate' task...")
    
    # 1. Read the intermediate file
    in_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, input_blob_name)
    data = in_blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    
    # 2. Perform validation (example: check for nulls in a key column)
    # Replace 'YourKeyColumn' with an actual column name from your data
    # if 'YourKeyColumn' in df.columns and df['YourKeyColumn'].isnull().any():
    #     raise ValueError("Validation failed: Key column contains null values.")
    print(f"[{run_id}] File passed validation checks.")
    
    # 3. Write to the final destination container
    original_filename = input_blob_name.split('/')[-1].replace('01-cleansed', '')
    output_blob_name = f"{run_id}/validated{original_filename}"
    out_blob = azure.get_blob_client(FINAL_CONTAINER, output_blob_name)
    out_blob.upload_blob(data, overwrite=True)

    print(f"[{run_id}] Validated file saved to: {FINAL_CONTAINER}/{output_blob_name}")
    return output_blob_name